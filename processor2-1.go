package processor

import (
	"log/slog"
	"sync/atomic"
)

type Generic2In1OutAsyncProcessor[In1, In2, Out any] interface {
	Init() error
	Process1(In1) error
	Process2(In2) error
	Output() <-chan Out
	Close() error
}

type Generic2In1OutAsyncProcessorIO[I1, I2, O, In1, In2, Out any] interface {
	AsInput1(I1) In1
	AsInput2(I2) In2
	FromOutput(Out) O

	ReleaseInput1(I1)
	ReleaseInput2(I2)
	ReleaseOutput(O)
}

func InitializeGeneric2In1OutAsyncProcessor[IO Generic2In1OutAsyncProcessorIO[I1, I2, O, In1, In2, Out], I1, I2, O, In1, In2, Out any](processor Generic2In1OutAsyncProcessor[In1, In2, Out], opts ...Option) func(<-chan I1, <-chan I2) (*Controller, chan O, error) {
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

	return func(input1 <-chan I1, input2 <-chan I2) (*Controller, chan O, error) {
		var io IO

		outputCh := make(chan O)
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
				case i, ok := <-input1:
					if !ok {
						loopEnded.Swap(true)
						logger.With("input_index", 1).Info("Input channel closed, stopping")
						break LOOP
					}

					if paused {
						io.ReleaseInput1(i)
						continue
					}

					in := io.AsInput1(i)
					if err := processor.Process1(in); err != nil {
						logger.With("error", err, "input_index", 1).Error("Error encountered during processing, continuing")
						continue
					}
				case i, ok := <-input2:
					if !ok {
						loopEnded.Swap(true)
						logger.With("input_index", 2).Info("Input channel closed, stopping")
						break LOOP
					}

					if paused {
						io.ReleaseInput2(i)
						continue
					}

					in := io.AsInput2(i)
					if err := processor.Process2(in); err != nil {
						logger.With("error", err, "input_index", 2).Error("Error encountered during processing, continuing")
						continue
					}
				case out, ok := <-processor.Output():
					if !ok {
						loopEnded.Swap(true)
						break LOOP
					}
					uo := io.FromOutput(out)

					if config.blockOnOutput {
						outputCh <- uo
					} else {
						select {
						case outputCh <- uo:
						default:
							select {
							case oldOutput := <-outputCh:
								logger.Warn("Output channel full, dropping the frontmost/oldest output")
								io.ReleaseOutput(oldOutput)
								outputCh <- uo
							default:
								logger.Warn("Output channel full, dropping current output")
								io.ReleaseOutput(uo)
							}
						}
					}
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
			close(outputCh)

			for o := range outputCh {
				io.ReleaseOutput(o)
			}

			go func() {
				for o := range processor.Output() {
					io.ReleaseOutput(io.FromOutput(o))
				}
			}()
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
						close(outputCh)
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
						close(outputCh)
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
		}, outputCh, nil
	}
}
