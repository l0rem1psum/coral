package processor

import (
	"log/slog"
	"sync/atomic"
)

type GenericNIn0OutAsyncProcessor[In any] interface {
	Init() error
	Process(int, In) error
	Close() error
}

type GenericNIn0OutAsyncProcessorIO[I, In any] interface {
	AsInput(I) In

	ReleaseInput(I)
}

func InitializeGenericNIn0OutAsyncProcessor[IO GenericNIn0OutAsyncProcessorIO[I, In], I, In any](processor GenericNIn0OutAsyncProcessor[In], opts ...Option) func([]<-chan I) (*Controller, error) {
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

	return func(inputs []<-chan I) (*Controller, error) {
		var io IO

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

			var input <-chan fannedInResult[I]
			if config.useRoundRobinFanIn {
				input = roundRobinFanIn(0, inputs...)
			} else {
				input = parallelFanIn(0, inputs...)
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
						io.ReleaseInput(i.t)
						continue
					}

					in := io.AsInput(i.t)
					if err := processor.Process(i.index, in); err != nil {
						logger.With("error", err).Error("Error encountered during processing, continuing")
						continue
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
			}, err
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
		}, nil
	}
}
