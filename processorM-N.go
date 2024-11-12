package processor

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/samber/lo"
)

type GenericMInNOutSyncProcessor[In, Out any] interface {
	NumOutputs() int
	Init() error
	Process([]In) ([]Out, error)
	Close() error
}

type GenericMInNOutSyncProcessorIO[I, O, In, Out any] interface {
	AsInput(I) In
	FromOutput(Out) O

	ReleaseInput(I)
	ReleaseOutput(O)
}

func InitializeGenericMInNOutSyncProcessor[IO GenericMInNOutSyncProcessorIO[I, O, In, Out], I, O, In, Out any](processor GenericMInNOutSyncProcessor[In, Out], opts ...Option) func(<-chan []I) (*Controller, []chan O, error) {
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

	return func(input <-chan []I) (*Controller, []chan O, error) {
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
				case is, ok := <-input:
					if !ok {
						loopEnded.Swap(true)
						logger.Info("Input channel closed, stopping")
						break LOOP
					}

					if paused {
						for _, i := range is {
							io.ReleaseInput(i)
						}
						continue
					}

					ins := lo.Map(is, func(i I, _ int) In { return io.AsInput(i) })
					out, err := processor.Process(ins)
					if err != nil {
						logger.With("error", err).Error("Error encountered during processing, continuing")
						continue
					}
					outs := lo.Map(out, func(out Out, _ int) O { return io.FromOutput(out) })

					var wg sync.WaitGroup
					wg.Add(len(outs))
					for i, out := range outs {
						go func(i int, out O) {
							defer wg.Done()

							if config.blockOnOutput {
								outputChs[i] <- out
							} else {
								select {
								case outputChs[i] <- out:
								default:
									select {
									case oldOutput := <-outputChs[i]:
										logger.With("output_index", i).Warn("Output channel full, dropping the frontmost/oldest output")
										io.ReleaseOutput(oldOutput)
										outputChs[i] <- out
									default:
										logger.With("output_index", i).Warn("Output channel full, dropping current output")
										io.ReleaseOutput(out)
									}
								}
							}
						}(i, out)
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
