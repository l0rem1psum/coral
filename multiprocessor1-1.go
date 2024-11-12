package processor

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/go-multierror"
)

func InitializeGeneric1In1OutSyncMultiProcessor[IO Generic1In1OutSyncProcessorIO[I, O, In, Out], I, O, In, Out any, P Generic1In1OutSyncProcessor[In, Out]](processors []P, opts ...Option) func(inputs <-chan []I) (*Controller, chan []O, []error) {
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

	return func(inputs <-chan []I) (*Controller, chan []O, []error) {
		inputChs := make([]chan I, len(processors))
		outputChs := make([]chan O, len(processors))
		closeChs := make([]chan struct{}, len(processors))

		initErrChs := make([]chan error, len(processors))
		closeErrChs := make([]chan error, len(processors))
		startChs := make([]chan struct{}, len(processors))
		stopAfterInits := make([]chan struct{}, len(processors))

		controlReqChs := make([]chan *wrappedRequest, len(processors))

		for i := range processors {
			inputChs[i] = make(chan I)
			outputChs[i] = make(chan O)
			closeChs[i] = make(chan struct{})

			initErrChs[i] = make(chan error)
			closeErrChs[i] = make(chan error)
			startChs[i] = make(chan struct{})
			stopAfterInits[i] = make(chan struct{})

			controlReqChs[i] = make(chan *wrappedRequest)
		}

		// Start each processor in its own goroutine
		for i, processor := range processors {
			go singleGeneric1In1OutSyncProcessorLoop[IO](
				processor,
				initErrChs[i],
				startChs[i],
				stopAfterInits[i],
				closeErrChs[i],
				inputChs[i],
				outputChs[i],
				controlReqChs[i],
				closeChs[i],
				logger.With("multiproc_index", i),
			)
		}

		var wg sync.WaitGroup
		initErrs := make([]error, len(processors))
		errDuringInit := false
		wg.Add(len(processors))
		for i := range processors {
			go func(i int) {
				defer wg.Done()
				initErr := <-initErrChs[i]
				initErrs[i] = initErr
				if initErr != nil {
					errDuringInit = true
				}
			}(i)
		}
		wg.Wait()

		if errDuringInit {
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
						closeMultipleChans(closeChs...)
						closeMultipleChans(startChs...)
						// Since init failed, we don't need to call Close
						return nil
					},
				},
				loopStarted: &atomic.Bool{},
				loopEnded:   &atomic.Bool{},
				reqCh:       nil,
			}, nil, initErrs
		}

		// Start controlling goroutine
		var io IO

		outputCh := make(chan []O)
		closeCh := make(chan struct{})

		closeErrCh := make(chan error)
		startCh := make(chan struct{})
		stopAfterInit := make(chan struct{})

		controlReqCh := make(chan *wrappedRequest)

		allSupportControl := true
		for _, p := range processors {
			if _, ok := any(p).(Controllable); !ok {
				allSupportControl = false
				break
			}
		}

		loopStarted := atomic.Bool{}
		loopEnded := atomic.Bool{}

		go func() {
			select {
			case <-startCh:
				var wg sync.WaitGroup
				wg.Add(len(processors))
				for _, startCh := range startChs {
					go func(startCh chan struct{}) {
						defer wg.Done()
						close(startCh)
					}(startCh)
				}
				wg.Wait()
			case <-stopAfterInit:
				logger.Info("Closing multiprocessor after initialization and before start")
				var wg sync.WaitGroup
				wg.Add(len(processors))
				closeErrs := make([]error, len(processors))
				for i, stopAfterInit := range stopAfterInits {
					go func(i int, stopAfterInit chan struct{}) {
						defer wg.Done()
						close(stopAfterInit)
						closeErrs[i] = <-closeErrChs[i]
					}(i, stopAfterInit)
				}
				wg.Wait()

				var multierr error
				for _, err := range closeErrs {
					if err != nil {
						multierr = multierror.Append(multierr, err)
					}
				}

				closeErrCh <- multierr
				return
			}

			paused := config.startPaused

			logger.Info("Multiprocessor started")
		LOOP:
			for {
				select {
				case is, ok := <-inputs:
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

					if len(is) == 0 {
						logger.Warn("Input is empty, dropping the input")
						continue
					}

					if len(is) != len(processors) {
						logger.With("input_length", len(is), "processor_length", len(processors)).Warn("Input length mismatch, dropping the input")
						for _, i := range is {
							io.ReleaseInput(i)
						}
						continue
					}

					var wg sync.WaitGroup
					wg.Add(len(is))
					os := make([]O, len(is))
					for j, i := range is {
						go func(j int, i I) {
							defer wg.Done()
							inputChs[j] <- i
							os[j] = <-outputChs[j]
						}(j, i)
					}
					wg.Wait()

					// Right now, it crashes if any one of the sub-processors failed
					// TODO: Consider adding error handling

					if config.blockOnOutput {
						outputCh <- os
					} else {
						select {
						case outputCh <- os:
						default:
							select {
							case oldOs := <-outputCh:
								logger.Warn("Output channel full, dropping the frontmost/oldest output")
								for _, o := range oldOs {
									io.ReleaseOutput(o)
								}
								outputCh <- os
							default:
								logger.Warn("Output channel full, dropping current output")
								for _, o := range os {
									io.ReleaseOutput(o)
								}
							}
						}
					}
				case ctlReq := <-controlReqCh:
					switch ctlReq.req.(type) {
					case pause:
						if !paused {
							paused = true
							ctlReq.res <- nil
							logger.Info("Multiprocessor paused")
						} else {
							ctlReq.res <- ErrAlreadyPaused
						}
						continue
					case resume:
						if paused {
							paused = false
							ctlReq.res <- nil
							logger.Info("Multiprocessor resumed")
						} else {
							ctlReq.res <- ErrAlreadyRunning
						}
						continue
					case *MultiProcessorRequest:
						if !allSupportControl {
							ctlReq.res <- ErrControlNotSupported
							continue
						}

						multiReq := ctlReq.req.(*MultiProcessorRequest)
						ctlReq.res <- any(processors[multiReq.I]).(Controllable).OnControl(multiReq.Req)
						continue
					default:
						if !allSupportControl {
							ctlReq.res <- ErrControlNotSupported
							continue
						}

						var wg sync.WaitGroup
						wg.Add(len(processors))
						ctlErrs := make([]error, len(processors))
						for i, processor := range processors {
							go func(i int, processor P) {
								defer wg.Done()
								ctlErrs[i] = any(processor).(Controllable).OnControl(ctlReq.req)
							}(i, processor)
						}
						wg.Wait()

						var multierr error
						for _, err := range ctlErrs {
							if err != nil {
								multierr = multierror.Append(multierr, err)
							}
						}
						ctlReq.res <- multierr
					}
				case <-closeCh:
					loopEnded.Swap(true)
					logger.Info("Close signal received, stopping")
					break LOOP
				}
			}

			var wg sync.WaitGroup
			wg.Add(len(processors))
			closeErrs := make([]error, len(processors))
			for i, closeCh := range closeChs {
				go func(i int, closeCh chan struct{}) {
					defer wg.Done()
					close(closeCh)
					closeErrs[i] = <-closeErrChs[i]
				}(i, closeCh)
			}
			wg.Wait()

			var multierr error
			for _, err := range closeErrs {
				if err != nil {
					multierr = multierror.Append(multierr, err)
				}
			}
			closeErrCh <- multierr
			logger.Info("Multiprocessor stopped")
		}()

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
					} else {
						// Inited but loop not started
						close(stopAfterInit)
						close(outputCh)
						close(closeCh)
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

func singleGeneric1In1OutSyncProcessorLoop[IO Generic1In1OutSyncProcessorIO[I, O, In, Out], I, O, In, Out any, P Generic1In1OutSyncProcessor[In, Out]](
	processor P,
	initErrCh chan error,
	startCh chan struct{},
	stopAfterInit chan struct{},
	closeErrCh chan error,
	input chan I,
	output chan O,
	controlReqCh chan *wrappedRequest,
	closeCh chan struct{},
	logger *slog.Logger,
) {
	var io IO

	initErr := processor.Init()
	if initErr != nil {
		initErrCh <- initErr
		return
	}
	initErrCh <- nil

	select {
	case <-startCh:
	case <-stopAfterInit:
		logger.Info("Closing sub-processor after initialization and before start")
		closeErrCh <- processor.Close()
		return
	}

	logger.Info("Sub-processor started")
LOOP:
	for {
		select {
		case i := <-input:
			in := io.AsInput(i)
			out, err := processor.Process(in)
			if err != nil {
				panic(err)
			}
			output <- io.FromOutput(i, out)
		case ctlReq := <-controlReqCh:
			ctlReq.res <- any(processor).(Controllable).OnControl(ctlReq.req)
		case <-closeCh:
			break LOOP
		}
	}

	close(output)

	for o := range output {
		io.ReleaseOutput(o)
	}

	closeErrCh <- processor.Close()
	logger.Info("Sub-processor stopped")
}
