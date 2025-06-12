package processor

import (
	"log/slog"
	"sync/atomic"
)

// Generic1In1OutSyncProcessor[In, Out] transforms data synchronously with 1:1 mapping.
// • Input: In (raw data type from upstream)
// • Output: Out (raw data type produced by processor)
// • Synchronous: Process(In) blocks until output ready
type Generic1In1OutSyncProcessor[In, Out any] interface {
	Init() error
	Process(In) (Out, error)
	Close() error
}

// Generic1In1OutSyncProcessorIO[I, O, In, Out] adapts sync transformer input/output.
// • I: adapted input type from upstream channel
// • O: adapted output type for downstream consumers
// • In: raw input type from processor
// • Out: raw output type from processor
type Generic1In1OutSyncProcessorIO[I, O, In, Out any] interface {
	AsInput(I) In
	FromOutput(I, Out) O

	ReleaseInput(I)
	ReleaseOutput(O)
}

// InitializeGeneric1In1OutSyncProcessor[IO, I, O, In, Out] creates processor setup closure.
// • IO: adapter implementing Generic1In1OutSyncProcessorIO[I, O, In, Out]
// • I: adapted input type from upstream channel
// • O: adapted output type for downstream consumers
// • In: raw input type from processor
// • Out: raw output type from processor
// Returns closure that spawns processor goroutine and produces (*Controller, chan O, error).
func InitializeGeneric1In1OutSyncProcessor[IO Generic1In1OutSyncProcessorIO[I, O, In, Out], I, O, In, Out any](processor Generic1In1OutSyncProcessor[In, Out], opts ...Option) func(<-chan I) (*Controller, chan O, error) {
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

	return func(input <-chan I) (*Controller, chan O, error) {
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
					out, err := processor.Process(in)
					if err != nil {
						logger.With("error", err).Error("Error encountered during processing, continuing")
						continue
					}
					uo := io.FromOutput(i, out)

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

// Generic1In1OutAsyncProcessor[In, Out] transforms data asynchronously with separate input/output handling.
// • Input: In (raw data type) via Process(In)
// • Output: Out (raw data type produced by processor) via self-managed Output() channel
// • Asynchronous: input processing and output generation decoupled
type Generic1In1OutAsyncProcessor[In, Out any] interface {
	Init() error
	Process(In) error
	Output() <-chan Out
	Close() error
}

// Generic1In1OutAsyncProcessorIO[I, O, In, Out] adapts async transformer input/output.
// • I: adapted input type from upstream channel
// • O: adapted output type for downstream consumers
// • In: raw input type from processor
// • Out: raw output type from processor
type Generic1In1OutAsyncProcessorIO[I, O, In, Out any] interface {
	AsInput(I) In
	FromOutput(Out) O

	ReleaseInput(I)
	ReleaseOutput(O)
}

// InitializeGeneric1In1OutAsyncProcessor[IO, I, O, In, Out] creates async processor setup closure.
// • IO: adapter implementing Generic1In1OutAsyncProcessorIO[I, O, In, Out]
// • I: adapted input type from upstream channel
// • O: adapted output type for downstream consumers
// • In: raw input type from processor
// • Out: raw output type from processor
// Returns closure that spawns processor goroutine and produces (*Controller, chan O, error).
func InitializeGeneric1In1OutAsyncProcessor[IO Generic1In1OutAsyncProcessorIO[I, O, In, Out], I, O, In, Out any](processor Generic1In1OutAsyncProcessor[In, Out], opts ...Option) func(<-chan I) (*Controller, chan O, error) {
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

	return func(input <-chan I) (*Controller, chan O, error) {
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
					if err := processor.Process(in); err != nil {
						logger.With("error", err).Error("Error encountered during processing, continuing")
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
