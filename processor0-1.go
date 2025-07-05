package processor

import (
	"log/slog"
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
		return newFSM0In1OutSync[IO](processor, config, logger).start()
	}
}

type fsm0In1OutSync[IO Generic0In1OutSyncProcessorIO[O, Out], O, Out any] struct {
	*fsm

	processor             Generic0In1OutSyncProcessor[Out]
	controllableProcessor Controllable

	config config
	logger *slog.Logger

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

func newFSM0In1OutSync[
	IO Generic0In1OutSyncProcessorIO[O, Out],
	O, Out any,
](
	processor Generic0In1OutSyncProcessor[Out],
	config config,
	logger *slog.Logger,
) *fsm0In1OutSync[IO, O, Out] {
	fsm := &fsm0In1OutSync[IO, O, Out]{
		fsm:           &fsm{},
		processor:     processor,
		config:        config,
		logger:        logger,
		outputCh:      make(chan O),
		closeCh:       make(chan struct{}),
		doneCh:        make(chan struct{}),
		initErrCh:     make(chan error),
		closeErrCh:    make(chan error),
		startCh:       make(chan struct{}),
		startDoneCh:   make(chan struct{}),
		stopAfterInit: make(chan struct{}),
		controlReqCh:  make(chan *wrappedRequest),
	}

	if controllableProcessor, ok := processor.(Controllable); ok {
		fsm.controllableProcessor = controllableProcessor
	}

	fsm.setState(StateCreated)
	return fsm
}

func (fsm *fsm0In1OutSync[_, O, _]) start() (*Controller, chan O, error) {
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
func (fsm *fsm0In1OutSync[_, _, _]) run() {
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
			fsm.logger.Info("Processor started in paused state")
		} else {
			fsm.transitionTo(StateRunning)
			fsm.logger.Info("Processor started")
		}
		close(fsm.startDoneCh)
		close(fsm.stopAfterInit)
	case <-fsm.stopAfterInit:
		fsm.logger.Info("Closing processor after initialization and before start")
		fsm.transitionTo(StateTerminating)
		fsm.closeErrCh <- fsm.processor.Close()
		fsm.transitionTo(StateTerminated)
		return
	}

	fsm.processingLoop()

	fsm.cleanup()
}

func (fsm *fsm0In1OutSync[_, _, _]) transitionTo(newState ProcessorState) {
	oldState := fsm.getState()
	fsm.setState(newState)
	fsm.logger.Debug("State transition", "from", oldState.String(), "to", newState.String())
}

func (fsm *fsm0In1OutSync[_, _, _]) processingLoop() {
LOOP:
	for {
		select {
		default:
			if fsm.getState() == StateRunning {
				fsm.generateOutput()
			}
		case ctlReq := <-fsm.controlReqCh:
			fsm.handleControlRequest(ctlReq)
		case <-fsm.closeCh:
			fsm.transitionTo(StateTerminating)
			fsm.logger.Info("Close signal received, stopping")
			break LOOP
		}
	}
}

func (fsm *fsm0In1OutSync[IO, _, _]) generateOutput() {
	var io IO

	out, err := fsm.processor.Process()
	if err != nil {
		fsm.logger.With("error", err).Error("Error encountered during processing, continuing")
		return
	}

	uo := io.FromOutput(out)
	fsm.handleOutput(uo)
}

func (fsm *fsm0In1OutSync[IO, O, _]) handleOutput(output O) {
	var io IO

	if fsm.config.blockOnOutput {
		fsm.outputCh <- output
	} else {
		select {
		case fsm.outputCh <- output:
		default:
			select {
			case oldOutput := <-fsm.outputCh:
				fsm.logger.Warn("Output channel full, dropping the frontmost/oldest output")
				io.ReleaseOutput(oldOutput)
				fsm.outputCh <- output
			default:
				fsm.logger.Warn("Output channel full, dropping current output")
				io.ReleaseOutput(output)
			}
		}
	}
}

func (fsm *fsm0In1OutSync[_, _, _]) handleControlRequest(ctlReq *wrappedRequest) {
	switch ctlReq.req.(type) {
	case pause:
		if fsm.getState() == StateRunning {
			fsm.transitionTo(StatePaused)
			ctlReq.res <- nil
			fsm.logger.Info("Processor paused")
		} else if fsm.getState() == StatePaused {
			ctlReq.res <- ErrAlreadyPaused
		} else {
			panic("impossible state: " + fsm.getState().String())
		}
	case resume:
		if fsm.getState() == StatePaused {
			fsm.transitionTo(StateRunning)
			ctlReq.res <- nil
			fsm.logger.Info("Processor resumed")
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

func (fsm *fsm0In1OutSync[IO, _, _]) cleanup() {
	var io IO

	close(fsm.doneCh)
	close(fsm.outputCh)
	close(fsm.controlReqCh)

	// Drain remaining outputs and release resources
	for o := range fsm.outputCh {
		io.ReleaseOutput(o)
	}

	// Close the processor and report any error
	fsm.closeErrCh <- fsm.processor.Close()
	fsm.logger.Info("Processor stopped")

	fsm.transitionTo(StateTerminated)
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
		return newFSM0In1OutAsync[IO](processor, config, logger).start()
	}
}

type fsm0In1OutAsync[IO Generic0In1OutAsyncProcessorIO[O, Out], O, Out any] struct {
	*fsm

	processor             Generic0In1OutAsyncProcessor[Out]
	controllableProcessor Controllable

	config config
	logger *slog.Logger

	outputCh      chan O
	closeCh       chan struct{}
	doneCh        chan struct{}
	initErrCh     chan error
	startErrCh    chan error
	closeErrCh    chan error
	startCh       chan struct{}
	startDoneCh   chan struct{}
	stopAfterInit chan struct{}
	controlReqCh  chan *wrappedRequest
}

func newFSM0In1OutAsync[
	IO Generic0In1OutAsyncProcessorIO[O, Out],
	O, Out any,
](
	processor Generic0In1OutAsyncProcessor[Out],
	config config,
	logger *slog.Logger,
) *fsm0In1OutAsync[IO, O, Out] {
	fsm := &fsm0In1OutAsync[IO, O, Out]{
		fsm:           &fsm{},
		processor:     processor,
		config:        config,
		logger:        logger,
		outputCh:      make(chan O),
		closeCh:       make(chan struct{}),
		doneCh:        make(chan struct{}),
		initErrCh:     make(chan error),
		startErrCh:    make(chan error),
		closeErrCh:    make(chan error),
		startCh:       make(chan struct{}),
		startDoneCh:   make(chan struct{}),
		stopAfterInit: make(chan struct{}),
		controlReqCh:  make(chan *wrappedRequest),
	}

	if controllableProcessor, ok := processor.(Controllable); ok {
		fsm.controllableProcessor = controllableProcessor
	}

	fsm.setState(StateCreated)
	return fsm
}

func (fsm *fsm0In1OutAsync[_, O, _]) start() (*Controller, chan O, error) {
	go fsm.run()

	err := <-fsm.initErrCh
	close(fsm.initErrCh)

	if err != nil { // Initialization failed
		close(fsm.outputCh)
		close(fsm.closeCh)
		close(fsm.doneCh)
		close(fsm.startErrCh)
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
				// Start the processor and begin processing
				close(fsm.startCh)
				<-fsm.startDoneCh
				return <-fsm.startErrCh
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
func (fsm *fsm0In1OutAsync[_, _, _]) run() {
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
		startErr := fsm.processor.Start()
		if startErr != nil {
			fsm.startErrCh <- startErr
			fsm.transitionTo(StateTerminated)
			return
		}

		if fsm.config.startPaused {
			fsm.transitionTo(StatePaused)
			fsm.logger.Info("Processor started in paused state")
		} else {
			fsm.transitionTo(StateRunning)
			fsm.logger.Info("Processor started")
		}
		close(fsm.startDoneCh)
		close(fsm.stopAfterInit)
		fsm.startErrCh <- nil
	case <-fsm.stopAfterInit:
		fsm.logger.Info("Closing processor after initialization and before start")
		fsm.transitionTo(StateTerminating)
		fsm.closeErrCh <- fsm.processor.Close()
		fsm.transitionTo(StateTerminated)
		return
	}

	fsm.processingLoop()

	fsm.cleanup()
}

func (fsm *fsm0In1OutAsync[_, _, _]) transitionTo(newState ProcessorState) {
	oldState := fsm.getState()
	fsm.setState(newState)
	fsm.logger.Debug("State transition", "from", oldState.String(), "to", newState.String())
}

func (fsm *fsm0In1OutAsync[_, _, _]) processingLoop() {
LOOP:
	for {
		select {
		case out, ok := <-fsm.processor.Output():
			if !ok {
				fsm.transitionTo(StateTerminating)
				fsm.logger.Info("Processor output channel closed, stopping")
				break LOOP
			}
			fsm.handleProcessorOutput(out)
		case ctlReq := <-fsm.controlReqCh:
			fsm.handleControlRequest(ctlReq)
		case <-fsm.closeCh:
			fsm.transitionTo(StateTerminating)
			fsm.logger.Info("Close signal received, stopping")
			break LOOP
		}
	}
}

func (fsm *fsm0In1OutAsync[IO, _, Out]) handleProcessorOutput(out Out) {
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

func (fsm *fsm0In1OutAsync[IO, O, _]) handleOutput(output O) {
	var io IO

	if fsm.config.blockOnOutput {
		fsm.outputCh <- output
	} else {
		select {
		case fsm.outputCh <- output:
		default:
			select {
			case oldOutput := <-fsm.outputCh:
				fsm.logger.Warn("Output channel full, dropping the frontmost/oldest output")
				io.ReleaseOutput(oldOutput)
				fsm.outputCh <- output
			default:
				fsm.logger.Warn("Output channel full, dropping current output")
				io.ReleaseOutput(output)
			}
		}
	}
}

func (fsm *fsm0In1OutAsync[_, _, _]) handleControlRequest(ctlReq *wrappedRequest) {
	switch ctlReq.req.(type) {
	case pause:
		if fsm.getState() == StateRunning {
			fsm.transitionTo(StatePaused)
			ctlReq.res <- nil
			fsm.logger.Info("Processor paused")
		} else if fsm.getState() == StatePaused {
			ctlReq.res <- ErrAlreadyPaused
		} else {
			panic("impossible state: " + fsm.getState().String())
		}
	case resume:
		if fsm.getState() == StatePaused {
			fsm.transitionTo(StateRunning)
			ctlReq.res <- nil
			fsm.logger.Info("Processor resumed")
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

func (fsm *fsm0In1OutAsync[IO, _, _]) cleanup() {
	var io IO

	close(fsm.doneCh)
	close(fsm.outputCh)
	close(fsm.controlReqCh)

	// Drain remaining outputs and release resources
	for o := range fsm.outputCh {
		io.ReleaseOutput(o)
	}

	// Drain remaining processor outputs and release resources
	go func() {
		for o := range fsm.processor.Output() {
			io.ReleaseOutput(io.FromOutput(o))
		}
	}()

	// Close the processor and report any error
	fsm.closeErrCh <- fsm.processor.Close()
	fsm.logger.Info("Processor stopped")

	fsm.transitionTo(StateTerminated)
}
