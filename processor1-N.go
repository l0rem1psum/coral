package processor

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/samber/lo"
)

// Generic1InNOutSyncProcessor[In, Out] broadcasts single input to multiple outputs.
// • Input: In (raw data type from upstream)
// • Output: []Out (slice of raw data type produced by processor)
// • Synchronous: Process(In) blocks until output ready
type Generic1InNOutSyncProcessor[In, Out any] interface {
	NumOutputs() int
	Init() error
	Process(In) ([]Out, error)
	Close() error
}

// Generic1InNOutSyncProcessorIO[I, O, In, Out] adapts sync broadcaster input/output.
// • I: adapted input type from upstream channel
// • O: adapted output type for downstream consumers
// • In: raw input type from processor
// • Out: raw output type from processor
type Generic1InNOutSyncProcessorIO[I, O, In, Out any] interface {
	AsInput(I) In
	FromOutput(I, Out) O

	ReleaseInput(I)
	ReleaseOutput(O)
}

// InitializeGeneric1InNOutSyncProcessor[IO, I, O, In, Out] creates processor setup closure.
// • IO: adapter implementing Generic1InNOutSyncProcessorIO[I, O, In, Out]
// • I: adapted input type from upstream channel
// • O: adapted output type for downstream consumers
// • In: raw input type from processor
// • Out: raw output type from processor
// Returns closure that spawns processor goroutine and produces (*Controller, []chan O, error).
func InitializeGeneric1InNOutSyncProcessor[IO Generic1InNOutSyncProcessorIO[I, O, In, Out], I, O, In, Out any](processor Generic1InNOutSyncProcessor[In, Out], opts ...Option) func(<-chan I) (*Controller, []chan O, error) {
	var config config
	for _, opt := range opts {
		opt(&config)
	}

	logger := slog.Default()
	if config.logger != nil {
		logger = config.logger
	}

	if config.label != nil {
		logger = logger.With("label", *config.label)
	}

	return func(input <-chan I) (*Controller, []chan O, error) {
		var io IO

		outputChs := make([]chan O, processor.NumOutputs())
		for i := range outputChs {
			outputChs[i] = make(chan O)
		}

		closeCh := make(chan struct{})
		doneCh := make(chan struct{})

		initErrCh := make(chan error)
		closeErrCh := make(chan error)
		startCh := make(chan struct{})
		stopAfterInit := make(chan struct{})

		controlReqCh := make(chan *wrappedRequest)

		supportsControl := false
		if _, ok := processor.(Controllable); ok {
			supportsControl = true
		}

		loopStarted := atomic.Bool{}
		loopEnded := atomic.Bool{}

		loop := func() {
			initErr := processor.Init()
			if initErr != nil {
				initErrCh <- initErr
				return
			}
			initErrCh <- nil

			select {
			case <-startCh:
			case <-stopAfterInit:
				logger.Info("Closing processor after initialization and before start")
				closeErrCh <- processor.Close()
				return
			}

			paused := config.startPaused

			logger.Info("Processor started")
		LOOP:
			for {
				select {
				case i, ok := <-input:
					if !ok {
						loopEnded.Swap(true)
						logger.Info("Input channel closed, stopping")
						break LOOP
					}

					if paused {
						io.ReleaseInput(i)
						continue
					}

					in := io.AsInput(i)
					outs, err := processor.Process(in)
					if err != nil {
						logger.With("error", err).Error("Error encountered during processing, continuing")
						continue
					}
					uos := lo.Map(outs, func(out Out, _ int) O {
						return io.FromOutput(i, out)
					})

					var wg sync.WaitGroup
					wg.Add(len(uos))
					for i, uo := range uos {
						go func(i int, uo O) {
							defer wg.Done()

							if config.blockOnOutput {
								outputChs[i] <- uo
							} else {
								select {
								case outputChs[i] <- uo:
								default:
									select {
									case oldOutput := <-outputChs[i]:
										logger.With("output_index", i).Warn("Output channel full, dropping the frontmost/oldest output")
										io.ReleaseOutput(oldOutput)
										outputChs[i] <- uo
									default:
										logger.With("output_index", i).Warn("Output channel full, dropping current output")
										io.ReleaseOutput(uo)
									}
								}
							}
						}(i, uo)
					}
					wg.Wait()
				case ctlReq := <-controlReqCh:
					switch ctlReq.req.(type) {
					case pause:
						if !paused {
							paused = true
							ctlReq.res <- nil
							logger.Info("Processor paused")
						} else {
							ctlReq.res <- ErrAlreadyPaused
						}
						continue
					case resume:
						if paused {
							paused = false
							ctlReq.res <- nil
							logger.Info("Processor resumed")
						} else {
							ctlReq.res <- ErrAlreadyRunning
						}
						continue
					default:
						if !supportsControl {
							ctlReq.res <- ErrControlNotSupported
							continue
						}

						ctlReq.res <- processor.(Controllable).OnControl(ctlReq.req)
					}
				case <-closeCh:
					loopEnded.Swap(true)
					logger.Info("Close signal received, stopping")
					break LOOP
				}
			}
			close(doneCh)
			closeMultipleChans(outputChs...)

			for _, output := range outputChs {
				for o := range output {
					io.ReleaseOutput(o)
				}
			}

			closeErrCh <- processor.Close()
			logger.Info("Processor stopped")
		}

		go loop()

		err := <-initErrCh
		close(initErrCh)
		if err != nil { // Error during init
			return &Controller{
				starter: &starter{
					f: func() error {
						// Since init failed
						return ErrUnableToStart
					},
				},
				stopper: &stopper{
					f: func() error {
						// Init failed, close everything
						closeMultipleChans(outputChs...)
						close(closeCh)
						close(doneCh)
						close(startCh)
						// Since init failed, we don't need to call Close
						return nil
					},
				},
				loopStarted: &loopStarted,
				loopEnded:   &loopEnded,
				reqCh:       controlReqCh,
			}, nil, err
		}

		return &Controller{
			starter: &starter{
				f: func() error {
					// Init succeeded, start the loop normally
					_ = loopStarted.Swap(true)
					close(startCh)
					// No error generated after init and before start
					return nil
				},
			},
			stopper: &stopper{
				f: func() error {
					if loopStarted.Load() {
						// Loop already started, wait for close signal, or input channel closed
						close(closeCh)
						<-doneCh
					} else {
						// Inited but loop not started
						close(stopAfterInit)
						closeMultipleChans(outputChs...)
						close(closeCh)
						close(doneCh)
					}
					// In both cases, Close are called
					return <-closeErrCh
				},
			},
			loopStarted: &loopStarted,
			loopEnded:   &loopEnded,
			reqCh:       controlReqCh,
		}, outputChs, nil
	}
}
