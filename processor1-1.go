package processor

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Generic1In1OutSyncProcessor[In, Out] transforms data synchronously with 1:1 mapping.
//   - Input: In (raw data type from upstream)
//   - Output: Out (raw data type produced by processor)
//   - Synchronous: Process(In) blocks until output ready
type Generic1In1OutSyncProcessor[In, Out any] interface {
	Init() error
	Process(In) (Out, error)
	Close() error
}

// Generic1In1OutSyncProcessorIO[I, O, In, Out] adapts sync transformer input/output.
//   - I: adapted input type from upstream channel
//   - O: adapted output type for downstream consumers
//   - In: raw input type from processor
//   - Out: raw output type from processor
type Generic1In1OutSyncProcessorIO[I, O, In, Out any] interface {
	AsInput(I) In
	FromOutput(I, Out) O

	ReleaseInput(I)
	ReleaseOutput(O)
}

// InitializeGeneric1In1OutSyncProcessor[IO, I, O, In, Out] creates processor setup closure.
//   - IO: adapter implementing Generic1In1OutSyncProcessorIO[I, O, In, Out]
//   - I: adapted input type from upstream channel
//   - O: adapted output type for downstream consumers
//   - In: raw input type from processor
//   - Out: raw output type from processor
//
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

	if config.hooks == nil {
		config.hooks = noopHooks
	}
	if config.hooks.BeforeProcessing == nil {
		config.hooks.BeforeProcessing = noopHooksBeforeProcessing
	}
	if config.hooks.AfterProcessing == nil {
		config.hooks.AfterProcessing = noopHooksAfterProcessing
	}

	return func(input <-chan I) (*Controller, chan O, error) {
		return newFSM1In1OutSync[IO](processor, config, logger, input).start()
	}
}

type fsm1In1OutSync[IO Generic1In1OutSyncProcessorIO[I, O, In, Out], I, O, In, Out any] struct {
	*fsm

	processor             Generic1In1OutSyncProcessor[In, Out]
	controllableProcessor Controllable

	config  config
	logger  *slog.Logger
	metrics *metricsRecorder

	inputCh       <-chan I
	outputCh      chan O
	closeCh       chan struct{}
	doneCh        chan struct{}
	initErrCh     chan error
	closeErrCh    chan error
	startCh       chan struct{}
	startDoneCh   chan struct{}
	stopAfterInit chan struct{}
	controlReqCh  chan *wrappedRequest
}

func newFSM1In1OutSync[
	IO Generic1In1OutSyncProcessorIO[I, O, In, Out],
	I, O, In, Out any,
](
	processor Generic1In1OutSyncProcessor[In, Out],
	config config,
	logger *slog.Logger,
	inputCh <-chan I,
) *fsm1In1OutSync[IO, I, O, In, Out] {
	fsm := &fsm1In1OutSync[IO, I, O, In, Out]{
		fsm:           &fsm{},
		processor:     processor,
		config:        config,
		logger:        logger,
		inputCh:       inputCh,
		outputCh:      make(chan O, config.outputChannelSize),
		closeCh:       make(chan struct{}),
		doneCh:        make(chan struct{}),
		initErrCh:     make(chan error),
		closeErrCh:    make(chan error),
		startCh:       make(chan struct{}),
		startDoneCh:   make(chan struct{}),
		stopAfterInit: make(chan struct{}),
		controlReqCh:  make(chan *wrappedRequest),
	}

	if config.meterProvider != nil && config.label != nil {
		if metrics, err := newMetricsRecorder(config.meterProvider, *config.label); err != nil {
			logger.With("error", err).Warn("Failed to initialize processor metrics")
		} else {
			fsm.metrics = metrics
		}
	}

	if controllableProcessor, ok := processor.(Controllable); ok {
		fsm.controllableProcessor = controllableProcessor
	}

	fsm.setState(StateCreated)
	return fsm
}

func (fsm *fsm1In1OutSync[_, _, O, _, _]) start() (*Controller, chan O, error) {
	go fsm.run()

	err := <-fsm.initErrCh
	close(fsm.initErrCh)

	if err != nil { // Initialization failed
		close(fsm.outputCh)
		close(fsm.closeCh)
		close(fsm.doneCh)
		close(fsm.closeErrCh)
		close(fsm.startCh)
		close(fsm.startDoneCh)
		close(fsm.stopAfterInit)
		close(fsm.controlReqCh)
		return &Controller{
			starter: &starter{
				f: func() error {
					return ErrUnableToStart
				},
			},
			stopper: &stopper{
				f: func() error {
					return nil
				},
			},
			reqCh:    fsm.controlReqCh,
			fsmState: &fsm.state,
		}, nil, err
	}

	// Initialization succeeded
	return &Controller{
		starter: &starter{
			f: func() error {
				// Init succeeded, start the loop normally
				close(fsm.startCh)
				<-fsm.startDoneCh
				// No error generated after init and before start
				return nil
			},
		},
		stopper: &stopper{
			f: func() error {
				if fsm.getState() == StateRunning ||
					fsm.getState() == StatePaused ||
					fsm.getState() == StateTerminating {
					// Loop already started, signal close and wait
					close(fsm.closeCh)
					<-fsm.doneCh
				} else if fsm.getState() == StateWaitingToStart {
					// Loop not started, stop before start
					close(fsm.stopAfterInit)
					close(fsm.outputCh)
					close(fsm.closeCh)
					close(fsm.doneCh)
				} else {
					panic("impossible state: " + fsm.getState().String())
				}
				return <-fsm.closeErrCh
			},
		},
		reqCh:    fsm.controlReqCh,
		fsmState: &fsm.state,
	}, fsm.outputCh, nil
}

// run is the main goroutine that drives the FSM
func (fsm *fsm1In1OutSync[_, _, _, _, _]) run() {
	fsm.transitionTo(StateInitializing)

	initErr := fsm.processor.Init()
	if initErr != nil {
		fsm.initErrCh <- initErr
		fsm.transitionTo(StateTerminated)
		return
	}
	fsm.initErrCh <- nil
	fsm.transitionTo(StateWaitingToStart)

	// Wait for start signal or early stop
	select {
	case <-fsm.startCh:
		if fsm.config.startPaused {
			fsm.transitionTo(StatePaused)
			fsm.logger.Info(logProcessorStartedPaused)
		} else {
			fsm.transitionTo(StateRunning)
			fsm.logger.Info(logProcessorStarted)
		}
		close(fsm.startDoneCh)
		close(fsm.stopAfterInit)
	case <-fsm.stopAfterInit:
		fsm.logger.Info(logProcessorClosingAfterInit)
		fsm.transitionTo(StateTerminating)
		fsm.closeErrCh <- fsm.processor.Close()
		fsm.transitionTo(StateTerminated)
		return
	}

	fsm.processingLoop()

	fsm.cleanup()
}

func (fsm *fsm1In1OutSync[_, _, _, _, _]) transitionTo(newState ProcessorState) {
	oldState := fsm.getState()
	fsm.setState(newState)
	fsm.logger.Debug(logStateTransition, "from", oldState.String(), "to", newState.String())
}

func (fsm *fsm1In1OutSync[_, _, _, _, _]) processingLoop() {
LOOP:
	for {
		select {
		case i, ok := <-fsm.inputCh:
			if !ok {
				fsm.transitionTo(StateTerminating)
				fsm.logger.Info(logInputChannelClosed)
				break LOOP
			}
			fsm.handleInput(i)
		case ctlReq := <-fsm.controlReqCh:
			fsm.handleControlRequest(ctlReq)
		case <-fsm.closeCh:
			fsm.transitionTo(StateTerminating)
			fsm.logger.Info(logCloseSignalReceived)
			break LOOP
		}
	}
}

func (fsm *fsm1In1OutSync[IO, I, _, _, _]) handleInput(i I) {
	var io IO

	switch fsm.getState() {
	case StateRunning:
		fsm.processInput(i)
	case StatePaused:
		io.ReleaseInput(i)
		fsm.metrics.recordInputReleased(context.Background(), 0)
	default:
		panic("impossible state: " + fsm.getState().String())
	}
}

func (fsm *fsm1In1OutSync[IO, I, _, _, _]) processInput(i I) {
	var io IO

	in := io.AsInput(i)

	start := time.Now()
	fsm.config.hooks.BeforeProcessing(*fsm.config.label, 0)
	out, err := fsm.processor.Process(in)
	fsm.config.hooks.AfterProcessing(*fsm.config.label, 0)
	if errors.Is(err, SkipResult) {
		return
	}
	if err != nil {
		fsm.logger.With("error", err).Error(logProcessingError)
		fsm.metrics.recordInputProcessedFailure(context.Background(), 0)
		return
	}
	fsm.metrics.recordProcessDuration(context.Background(), time.Since(start))
	fsm.metrics.recordInputProcessedSuccess(context.Background(), 0)

	uo := io.FromOutput(i, out)
	fsm.handleOutput(uo)
}

func (fsm *fsm1In1OutSync[IO, _, O, _, _]) handleOutput(output O) {
	var io IO

	start := time.Now()
	if fsm.config.blockOnOutput {
		fsm.outputCh <- output
	} else {
		select {
		case fsm.outputCh <- output:
		default:
			select {
			case oldOutput := <-fsm.outputCh:
				fsm.logger.Warn(logOutputChannelFullDropOldest)
				io.ReleaseOutput(oldOutput)
				fsm.metrics.recordOutputReleased(context.Background(), 0)
				fsm.outputCh <- output
			default:
				fsm.logger.Warn(logOutputChannelFullDropCurrent)
				io.ReleaseOutput(output)
				fsm.metrics.recordOutputReleased(context.Background(), 0)
			}
		}
	}
	fsm.metrics.recordOutputDuration(context.Background(), 0, time.Since(start))
}

func (fsm *fsm1In1OutSync[_, _, _, _, _]) handleControlRequest(ctlReq *wrappedRequest) {
	switch ctlReq.req.(type) {
	case pause:
		if fsm.getState() == StateRunning {
			fsm.transitionTo(StatePaused)
			ctlReq.res <- nil
			fsm.logger.Info(logProcessorPaused)
		} else if fsm.getState() == StatePaused {
			ctlReq.res <- ErrAlreadyPaused
		} else {
			panic("impossible state: " + fsm.getState().String())
		}
	case resume:
		if fsm.getState() == StatePaused {
			fsm.transitionTo(StateRunning)
			ctlReq.res <- nil
			fsm.logger.Info(logProcessorResumed)
		} else if fsm.getState() == StateRunning {
			ctlReq.res <- ErrAlreadyRunning
		} else {
			panic("impossible state: " + fsm.getState().String())
		}
	default:
		if fsm.controllableProcessor == nil {
			ctlReq.res <- ErrControlNotSupported
			return
		}
		ctlReq.res <- fsm.controllableProcessor.OnControl(ctlReq.req)
	}
}

func (fsm *fsm1In1OutSync[IO, _, _, _, _]) cleanup() {
	var io IO

	close(fsm.doneCh)
	close(fsm.outputCh)
	close(fsm.controlReqCh)

	// Drain remaining outputs and release resources
	for o := range fsm.outputCh {
		io.ReleaseOutput(o)
		fsm.metrics.recordOutputReleased(context.Background(), 0)
	}

	// Close the processor and report any error
	fsm.closeErrCh <- fsm.processor.Close()
	fsm.logger.Info(logProcessorStopped)

	fsm.transitionTo(StateTerminated)
}

// Generic1In1OutAsyncProcessor[In, Out] transforms data asynchronously with separate input/output handling.
//   - Input: In (raw data type) via Process(In)
//   - Output: Out (raw data type produced by processor) via self-managed Output() channel
//   - Asynchronous: input processing and output generation decoupled
type Generic1In1OutAsyncProcessor[In, Out any] interface {
	Init() error
	Process(In) error
	Output() <-chan Out
	Close() error
}

// Generic1In1OutAsyncProcessorIO[I, O, In, Out] adapts async transformer input/output.
//   - I: adapted input type from upstream channel
//   - O: adapted output type for downstream consumers
//   - In: raw input type from processor
//   - Out: raw output type from processor
type Generic1In1OutAsyncProcessorIO[I, O, In, Out any] interface {
	AsInput(I) In
	FromOutput(Out) O

	ReleaseInput(I)
	ReleaseOutput(O)
}

// InitializeGeneric1In1OutAsyncProcessor[IO, I, O, In, Out] creates async processor setup closure.
//   - IO: adapter implementing Generic1In1OutAsyncProcessorIO[I, O, In, Out]
//   - I: adapted input type from upstream channel
//   - O: adapted output type for downstream consumers
//   - In: raw input type from processor
//   - Out: raw output type from processor
//
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

	if config.hooks == nil {
		config.hooks = noopHooks
	}
	if config.hooks.BeforeProcessing == nil {
		config.hooks.BeforeProcessing = noopHooksBeforeProcessing
	}
	if config.hooks.AfterProcessing == nil {
		config.hooks.AfterProcessing = noopHooksAfterProcessing
	}

	return func(input <-chan I) (*Controller, chan O, error) {
		return newFSM1In1OutAsync[IO](processor, config, logger, input).start()
	}
}

type fsm1In1OutAsync[IO Generic1In1OutAsyncProcessorIO[I, O, In, Out], I, O, In, Out any] struct {
	*fsm

	processor             Generic1In1OutAsyncProcessor[In, Out]
	controllableProcessor Controllable

	config  config
	logger  *slog.Logger
	metrics *metricsRecorder

	inputCh       <-chan I
	outputCh      chan O
	closeCh       chan struct{}
	doneCh        chan struct{}
	initErrCh     chan error
	closeErrCh    chan error
	startCh       chan struct{}
	startDoneCh   chan struct{}
	stopAfterInit chan struct{}
	controlReqCh  chan *wrappedRequest
}

func newFSM1In1OutAsync[
	IO Generic1In1OutAsyncProcessorIO[I, O, In, Out],
	I, O, In, Out any,
](
	processor Generic1In1OutAsyncProcessor[In, Out],
	config config,
	logger *slog.Logger,
	inputCh <-chan I,
) *fsm1In1OutAsync[IO, I, O, In, Out] {
	fsm := &fsm1In1OutAsync[IO, I, O, In, Out]{
		fsm:           &fsm{},
		processor:     processor,
		config:        config,
		logger:        logger,
		inputCh:       inputCh,
		outputCh:      make(chan O, config.outputChannelSize),
		closeCh:       make(chan struct{}),
		doneCh:        make(chan struct{}),
		initErrCh:     make(chan error),
		closeErrCh:    make(chan error),
		startCh:       make(chan struct{}),
		startDoneCh:   make(chan struct{}),
		stopAfterInit: make(chan struct{}),
		controlReqCh:  make(chan *wrappedRequest),
	}

	if config.meterProvider != nil && config.label != nil {
		if metrics, err := newMetricsRecorder(config.meterProvider, *config.label); err != nil {
			logger.With("error", err).Warn("Failed to initialize processor metrics")
		} else {
			fsm.metrics = metrics
		}
	}

	if controllableProcessor, ok := processor.(Controllable); ok {
		fsm.controllableProcessor = controllableProcessor
	}

	fsm.setState(StateCreated)
	return fsm
}

func (fsm *fsm1In1OutAsync[_, _, O, _, _]) start() (*Controller, chan O, error) {
	go fsm.run()

	err := <-fsm.initErrCh
	close(fsm.initErrCh)

	if err != nil { // Initialization failed
		close(fsm.outputCh)
		close(fsm.closeCh)
		close(fsm.doneCh)
		close(fsm.closeErrCh)
		close(fsm.startCh)
		close(fsm.startDoneCh)
		close(fsm.stopAfterInit)
		close(fsm.controlReqCh)
		return &Controller{
			starter: &starter{
				f: func() error {
					return ErrUnableToStart
				},
			},
			stopper: &stopper{
				f: func() error {
					return nil
				},
			},
			reqCh:    fsm.controlReqCh,
			fsmState: &fsm.state,
		}, nil, err
	}

	// Initialization succeeded
	return &Controller{
		starter: &starter{
			f: func() error {
				// Init succeeded, start the loop normally
				close(fsm.startCh)
				<-fsm.startDoneCh
				// No error generated after init and before start
				return nil
			},
		},
		stopper: &stopper{
			f: func() error {
				if fsm.getState() == StateRunning ||
					fsm.getState() == StatePaused ||
					fsm.getState() == StateTerminating {
					// Loop already started, signal close and wait
					close(fsm.closeCh)
					<-fsm.doneCh
				} else if fsm.getState() == StateWaitingToStart {
					// Loop not started, stop before start
					close(fsm.stopAfterInit)
					close(fsm.outputCh)
					close(fsm.closeCh)
					close(fsm.doneCh)
				} else {
					panic("impossible state: " + fsm.getState().String())
				}
				return <-fsm.closeErrCh
			},
		},
		reqCh:    fsm.controlReqCh,
		fsmState: &fsm.state,
	}, fsm.outputCh, nil
}

// run is the main goroutine that drives the FSM
func (fsm *fsm1In1OutAsync[_, _, _, _, _]) run() {
	fsm.transitionTo(StateInitializing)

	initErr := fsm.processor.Init()
	if initErr != nil {
		fsm.initErrCh <- initErr
		fsm.transitionTo(StateTerminated)
		return
	}
	fsm.initErrCh <- nil
	fsm.transitionTo(StateWaitingToStart)

	// Wait for start signal or early stop
	select {
	case <-fsm.startCh:
		if fsm.config.startPaused {
			fsm.transitionTo(StatePaused)
			fsm.logger.Info(logProcessorStartedPaused)
		} else {
			fsm.transitionTo(StateRunning)
			fsm.logger.Info(logProcessorStarted)
		}
		close(fsm.startDoneCh)
		close(fsm.stopAfterInit)
	case <-fsm.stopAfterInit:
		fsm.logger.Info(logProcessorClosingAfterInit)
		fsm.transitionTo(StateTerminating)
		fsm.closeErrCh <- fsm.processor.Close()
		fsm.transitionTo(StateTerminated)
		return
	}

	fsm.processingLoop()

	fsm.cleanup()
}

func (fsm *fsm1In1OutAsync[_, _, _, _, _]) transitionTo(newState ProcessorState) {
	oldState := fsm.getState()
	fsm.setState(newState)
	fsm.logger.Debug(logStateTransition, "from", oldState.String(), "to", newState.String())
}

func (fsm *fsm1In1OutAsync[_, _, _, _, _]) processingLoop() {
LOOP:
	for {
		select {
		case i, ok := <-fsm.inputCh:
			if !ok {
				fsm.transitionTo(StateTerminating)
				fsm.logger.Info(logInputChannelClosed)
				break LOOP
			}
			fsm.handleInput(i)
		case out, ok := <-fsm.processor.Output():
			if !ok {
				fsm.transitionTo(StateTerminating)
				fsm.logger.Info(logOutputChannelClosed)
				break LOOP
			}
			fsm.handleProcessorOutput(out)
		case ctlReq := <-fsm.controlReqCh:
			fsm.handleControlRequest(ctlReq)
		case <-fsm.closeCh:
			fsm.transitionTo(StateTerminating)
			fsm.logger.Info(logCloseSignalReceived)
			break LOOP
		}
	}
}

func (fsm *fsm1In1OutAsync[IO, I, _, _, _]) handleInput(i I) {
	var io IO

	switch fsm.getState() {
	case StateRunning:
		fsm.processInput(i)
	case StatePaused:
		io.ReleaseInput(i)
		fsm.metrics.recordInputReleased(context.Background(), 0)
	default:
		panic("impossible state: " + fsm.getState().String())
	}
}

func (fsm *fsm1In1OutAsync[IO, I, _, _, _]) processInput(i I) {
	var io IO

	in := io.AsInput(i)

	start := time.Now()
	fsm.config.hooks.BeforeProcessing(*fsm.config.label, 0)
	if err := fsm.processor.Process(in); errors.Is(err, SkipResult) {
		fsm.logger.Warn(logSkipResultMisuseWarning)
		return
	} else if err != nil {
		fsm.logger.With("error", err).Error(logProcessingError)
		fsm.metrics.recordInputProcessedFailure(context.Background(), 0)
		return
	}
	fsm.metrics.recordProcessDuration(context.Background(), time.Since(start))
	fsm.metrics.recordInputProcessedSuccess(context.Background(), 0)
}

func (fsm *fsm1In1OutAsync[IO, _, O, _, Out]) handleProcessorOutput(out Out) {
	var io IO

	switch fsm.getState() {
	case StateRunning:
		uo := io.FromOutput(out)
		fsm.handleOutput(uo)
	case StatePaused:
		// Still forward output even when paused, as processor may have buffered outputs
		uo := io.FromOutput(out)
		fsm.handleOutput(uo)
	default:
		panic("impossible state: " + fsm.getState().String())
	}
}

func (fsm *fsm1In1OutAsync[IO, _, O, _, _]) handleOutput(output O) {
	var io IO

	start := time.Now()
	if fsm.config.blockOnOutput {
		fsm.outputCh <- output
	} else {
		select {
		case fsm.outputCh <- output:
		default:
			select {
			case oldOutput := <-fsm.outputCh:
				fsm.logger.Warn(logOutputChannelFullDropOldest)
				io.ReleaseOutput(oldOutput)
				fsm.metrics.recordOutputReleased(context.Background(), 0)
				fsm.outputCh <- output
			default:
				fsm.logger.Warn(logOutputChannelFullDropCurrent)
				io.ReleaseOutput(output)
				fsm.metrics.recordOutputReleased(context.Background(), 0)
			}
		}
	}
	fsm.metrics.recordOutputDuration(context.Background(), 0, time.Since(start))
}

func (fsm *fsm1In1OutAsync[_, _, _, _, _]) handleControlRequest(ctlReq *wrappedRequest) {
	switch ctlReq.req.(type) {
	case pause:
		if fsm.getState() == StateRunning {
			fsm.transitionTo(StatePaused)
			ctlReq.res <- nil
			fsm.logger.Info(logProcessorPaused)
		} else if fsm.getState() == StatePaused {
			ctlReq.res <- ErrAlreadyPaused
		} else {
			panic("impossible state: " + fsm.getState().String())
		}
	case resume:
		if fsm.getState() == StatePaused {
			fsm.transitionTo(StateRunning)
			ctlReq.res <- nil
			fsm.logger.Info(logProcessorResumed)
		} else if fsm.getState() == StateRunning {
			ctlReq.res <- ErrAlreadyRunning
		} else {
			panic("impossible state: " + fsm.getState().String())
		}
	default:
		if fsm.controllableProcessor == nil {
			ctlReq.res <- ErrControlNotSupported
			return
		}
		ctlReq.res <- fsm.controllableProcessor.OnControl(ctlReq.req)
	}
}

func (fsm *fsm1In1OutAsync[IO, _, _, _, _]) cleanup() {
	var io IO

	close(fsm.doneCh)
	close(fsm.outputCh)
	close(fsm.controlReqCh)

	// Drain remaining outputs and release resources
	for o := range fsm.outputCh {
		io.ReleaseOutput(o)
		fsm.metrics.recordOutputReleased(context.Background(), 0)
	}

	// Drain remaining processor outputs and release resources
	go func() {
		for o := range fsm.processor.Output() {
			io.ReleaseOutput(io.FromOutput(o))
			fsm.metrics.recordOutputReleased(context.Background(), 0)
		}
	}()

	// Close the processor and report any error
	fsm.closeErrCh <- fsm.processor.Close()
	fsm.logger.Info(logProcessorStopped)

	fsm.transitionTo(StateTerminated)
}
