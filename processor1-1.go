package processor

import (
	"log/slog"
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

	return func(input <-chan I) (*Controller, chan O, error) {
		fsm := newFSM1In1OutSync[IO](processor, config, logger, input)
		return fsm.Initialize()
	}
}

type fsm1In1OutSync[IO Generic1In1OutSyncProcessorIO[I, O, In, Out], I, O, In, Out any] struct {
	*fsm

	processor Generic1In1OutSyncProcessor[In, Out]

	config config
	logger *slog.Logger

	supportsControl bool
	controllable    Controllable

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
	controllable, supportsControl := processor.(Controllable)

	fsm := &fsm1In1OutSync[IO, I, O, In, Out]{
		fsm:             &fsm{},
		processor:       processor,
		config:          config,
		logger:          logger,
		inputCh:         inputCh,
		supportsControl: supportsControl,
		controllable:    controllable,
		outputCh:        make(chan O),
		closeCh:         make(chan struct{}),
		doneCh:          make(chan struct{}),
		initErrCh:       make(chan error),
		closeErrCh:      make(chan error),
		startCh:         make(chan struct{}),
		startDoneCh:     make(chan struct{}),
		stopAfterInit:   make(chan struct{}),
		controlReqCh:    make(chan *wrappedRequest),
	}

	fsm.setState(StateCreated)
	return fsm
}

// Initialize starts the FSM and returns the Controller and output channel
func (fsm *fsm1In1OutSync[_, _, O, _, _]) Initialize() (*Controller, chan O, error) {
	// Start the processor goroutine
	go fsm.run()

	// Wait for initialization to complete
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

// transitionTo changes the FSM state atomically and logs the transition
func (fsm *fsm1In1OutSync[_, _, _, _, _]) transitionTo(newState ProcessorState) {
	oldState := fsm.getState()
	fsm.setState(newState)
	fsm.logger.Debug("State transition", "from", oldState.String(), "to", newState.String())
}

// processingLoop handles the main processing logic
func (fsm *fsm1In1OutSync[_, _, _, _, _]) processingLoop() {
LOOP:
	for {
		select {
		case i, ok := <-fsm.inputCh:
			if !ok {
				fsm.transitionTo(StateTerminating)
				fsm.logger.Info("Input channel closed, stopping")
				break LOOP
			}
			fsm.handleInput(i)
		case ctlReq := <-fsm.controlReqCh:
			fsm.handleControlRequest(ctlReq)
		case <-fsm.closeCh:
			fsm.transitionTo(StateTerminating)
			fsm.logger.Info("Close signal received, stopping")
			break LOOP
		}
	}
}

// handleInput processes a single input item based on current state
func (fsm *fsm1In1OutSync[IO, I, _, _, _]) handleInput(i I) {
	var io IO

	switch fsm.getState() {
	case StateRunning:
		fsm.processInput(i)
	case StatePaused:
		io.ReleaseInput(i)
	default:
		panic("impossible state: " + fsm.getState().String())
	}
}

// processInput handles the actual data transformation
func (fsm *fsm1In1OutSync[IO, I, _, _, _]) processInput(i I) {
	var io IO

	in := io.AsInput(i)
	out, err := fsm.processor.Process(in)
	if err != nil {
		fsm.logger.With("error", err).Error("Error encountered during processing, continuing")
		return
	}

	uo := io.FromOutput(i, out)
	fsm.handleOutput(uo)
}

// handleOutput manages backpressure and output delivery
func (fsm *fsm1In1OutSync[IO, _, O, _, _]) handleOutput(output O) {
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

// handleControlRequest processes control messages (pause, resume, custom)
func (fsm *fsm1In1OutSync[_, _, _, _, _]) handleControlRequest(ctlReq *wrappedRequest) {
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
		if !fsm.supportsControl {
			ctlReq.res <- ErrControlNotSupported
			return
		}
		ctlReq.res <- fsm.controllable.OnControl(ctlReq.req)
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
	}

	// Close the processor and report any error
	fsm.closeErrCh <- fsm.processor.Close()
	fsm.logger.Info("Processor stopped")

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

	return func(input <-chan I) (*Controller, chan O, error) {
		fsm := newFSM1In1OutAsync[IO](processor, config, logger, input)
		return fsm.Initialize()
	}
}

type fsm1In1OutAsync[IO Generic1In1OutAsyncProcessorIO[I, O, In, Out], I, O, In, Out any] struct {
	*fsm

	processor Generic1In1OutAsyncProcessor[In, Out]

	config config
	logger *slog.Logger

	supportsControl bool
	controllable    Controllable

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
	controllable, supportsControl := processor.(Controllable)

	fsm := &fsm1In1OutAsync[IO, I, O, In, Out]{
		fsm:             &fsm{},
		processor:       processor,
		config:          config,
		logger:          logger,
		inputCh:         inputCh,
		supportsControl: supportsControl,
		controllable:    controllable,
		outputCh:        make(chan O),
		closeCh:         make(chan struct{}),
		doneCh:          make(chan struct{}),
		initErrCh:       make(chan error),
		closeErrCh:      make(chan error),
		startCh:         make(chan struct{}),
		startDoneCh:     make(chan struct{}),
		stopAfterInit:   make(chan struct{}),
		controlReqCh:    make(chan *wrappedRequest),
	}

	fsm.setState(StateCreated)
	return fsm
}

// Initialize starts the FSM and returns the Controller and output channel
func (fsm *fsm1In1OutAsync[_, _, O, _, _]) Initialize() (*Controller, chan O, error) {
	// Start the processor goroutine
	go fsm.run()

	// Wait for initialization to complete
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

// transitionTo changes the FSM state atomically and logs the transition
func (fsm *fsm1In1OutAsync[_, _, _, _, _]) transitionTo(newState ProcessorState) {
	oldState := fsm.getState()
	fsm.setState(newState)
	fsm.logger.Debug("State transition", "from", oldState.String(), "to", newState.String())
}

// processingLoop handles the main processing logic with dual input sources
func (fsm *fsm1In1OutAsync[_, _, _, _, _]) processingLoop() {
LOOP:
	for {
		select {
		case i, ok := <-fsm.inputCh:
			if !ok {
				fsm.transitionTo(StateTerminating)
				fsm.logger.Info("Input channel closed, stopping")
				break LOOP
			}
			fsm.handleInput(i)
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

// handleInput processes a single input item based on current state
func (fsm *fsm1In1OutAsync[IO, I, _, _, _]) handleInput(i I) {
	var io IO

	switch fsm.getState() {
	case StateRunning:
		fsm.processInput(i)
	case StatePaused:
		io.ReleaseInput(i)
	default:
		panic("impossible state: " + fsm.getState().String())
	}
}

// processInput handles the actual input processing (async style)
func (fsm *fsm1In1OutAsync[IO, I, _, _, _]) processInput(i I) {
	var io IO

	in := io.AsInput(i)
	if err := fsm.processor.Process(in); err != nil {
		fsm.logger.With("error", err).Error("Error encountered during processing, continuing")
	}
}

// handleProcessorOutput processes output from the processor's Output() channel
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

// handleOutput manages backpressure and output delivery
func (fsm *fsm1In1OutAsync[IO, _, O, _, _]) handleOutput(output O) {
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

// handleControlRequest processes control messages (pause, resume, custom)
func (fsm *fsm1In1OutAsync[_, _, _, _, _]) handleControlRequest(ctlReq *wrappedRequest) {
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
		if !fsm.supportsControl {
			ctlReq.res <- ErrControlNotSupported
			return
		}
		ctlReq.res <- fsm.controllable.OnControl(ctlReq.req)
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
