package processor

import (
	"log/slog"
	"sync/atomic"
)

// Generic0In1OutSyncProcessor[Out] generates data without input.
//   - Input: None
//   - Output: Out (raw data type produced by processor)
//   - Synchronous: Process() blocks until output ready
type Generic0In1OutSyncProcessor[Out any] interface {
	Init() error
	Process() (Out, error)
	Close() error
}

// Generic0In1OutSyncProcessorIO[O, Out] adapts processor output for consumption.
//   - O: adapted output type for downstream consumers
//   - Out: raw output type from processor
//   - Converts Out → O and manages O lifecycle
type Generic0In1OutSyncProcessorIO[O, Out any] interface {
	FromOutput(Out) O

	ReleaseOutput(O)
}

// InitializeGeneric0In1OutSyncProcessor[IO, O, Out] creates processor setup closure.
//   - IO: adapter implementing Generic0In1OutSyncProcessorIO[O, Out]
//   - O: adapted output type for downstream consumers
//   - Out: raw processor output type
//
// Returns closure that spawns processor goroutine and produces (*Controller, chan O, error).
func InitializeGeneric0In1OutSyncProcessor[IO Generic0In1OutSyncProcessorIO[O, Out], O, Out any](processor Generic0In1OutSyncProcessor[Out], opts ...Option) func() (*Controller, chan O, error) {
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

	return func() (*Controller, chan O, error) {
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
				default:
					if paused {
						continue
					}

					out, err := processor.Process()
					if err != nil {
						logger.With("error", err).Error("Error encountered during processing, continuing")
						continue
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

// Generic0In1OutAsyncProcessor[Out] generates data asynchronously without input.
//   - Input: None
//   - Output: Out (raw data type) via self-managed channel
//   - Asynchronous: controls own production timing
type Generic0In1OutAsyncProcessor[Out any] interface {
	Init() error
	Start() error
	Output() <-chan Out
	Close() error
}

// Generic0In1OutAsyncProcessorIO[O, Out] adapts async processor output for consumption.
//   - O: adapted output type for downstream consumers
//   - Out: raw output type from async processor
//   - Converts Out → O and manages O lifecycle
type Generic0In1OutAsyncProcessorIO[O, Out any] interface {
	FromOutput(Out) O

	ReleaseOutput(O)
}

// InitializeGeneric0In1OutAsyncProcessor[IO, O, Out] creates async processor setup closure.
//   - IO: adapter implementing Generic0In1OutAsyncProcessorIO[O, Out]
//   - O: adapted output type for downstream consumers
//   - Out: raw async processor output type
//
// Returns closure that spawns processor goroutine and produces (*Controller, chan O, error).
func InitializeGeneric0In1OutAsyncProcessor[IO Generic0In1OutAsyncProcessorIO[O, Out], O, Out any](processor Generic0In1OutAsyncProcessor[Out], opts ...Option) func() (*Controller, chan O, error) {
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

	return func() (*Controller, chan O, error) {
		var io IO

		outputCh := make(chan O)
		closeCh := make(chan struct{})
		doneCh := make(chan struct{})

		initErrCh := make(chan error)
		startErrCh := make(chan error)
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

			startErr := processor.Start()
			if startErr != nil {
				startErrCh <- startErr
				return
			}
			startErrCh <- nil

			paused := config.startPaused

			logger.Info("Processor started")
		LOOP:
			for {
				select {
				case out, ok := <-processor.Output():
					if !ok {
						loopEnded.Swap(true)
						break LOOP
					}
					uo := io.FromOutput(out)

					if paused {
						io.ReleaseOutput(uo)
						continue
					}

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
					close(startCh)
					startErr := <-startErrCh
					if startErr != nil {
						return startErr
					}
					// Init succeeded, start the loop normally
					_ = loopStarted.Swap(true)
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
