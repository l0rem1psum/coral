package processor

import (
	"log/slog"
)

// Generic1In0OutSyncProcessor[In] consumes data without producing output.
//   - Input: In (raw data type from upstream)
//   - Output: None (side effects only)
//   - Synchronous: Process(In) blocks until output ready
type Generic1In0OutSyncProcessor[In any] interface {
	Init() error
	Process(In) error
	Close() error
}

// Generic1In0OutSyncProcessorIO[I, In] adapts sync sink input.
//   - I: adapted input type from upstream channel
//   - In: raw input type from processor
//   - Converts I → In and manages I lifecycle
type Generic1In0OutSyncProcessorIO[I, In any] interface {
	AsInput(I) In

	ReleaseInput(I)
}

// InitializeGeneric1In0OutSyncProcessor[IO, I, In] creates processor setup closure.
//   - IO: adapter implementing Generic1In0OutSyncProcessorIO[I, In]
//   - I: adapted input type from upstream channel
//   - In: raw input type from processor
//
// Returns closure that spawns processor goroutine and produces (*Controller, error).
func InitializeGeneric1In0OutSyncProcessor[IO Generic1In0OutSyncProcessorIO[I, In], I, In any](processor Generic1In0OutSyncProcessor[In], opts ...Option) func(<-chan I) (*Controller, error) {
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

	return func(input <-chan I) (*Controller, error) {
		return newFSM1In0OutSync[IO](processor, config, logger, input).start()
	}
}

type fsm1In0OutSync[IO Generic1In0OutSyncProcessorIO[I, In], I, In any] struct {
	*fsm

	processor             Generic1In0OutSyncProcessor[In]
	controllableProcessor Controllable

	config config
	logger *slog.Logger

	inputCh       <-chan I
	closeCh       chan struct{}
	doneCh        chan struct{}
	initErrCh     chan error
	closeErrCh    chan error
	startCh       chan struct{}
	startDoneCh   chan struct{}
	stopAfterInit chan struct{}
	controlReqCh  chan *wrappedRequest
}

func newFSM1In0OutSync[
	IO Generic1In0OutSyncProcessorIO[I, In],
	I, In any,
](
	processor Generic1In0OutSyncProcessor[In],
	config config,
	logger *slog.Logger,
	inputCh <-chan I,
) *fsm1In0OutSync[IO, I, In] {
	fsm := &fsm1In0OutSync[IO, I, In]{
		fsm:           &fsm{},
		processor:     processor,
		config:        config,
		logger:        logger,
		inputCh:       inputCh,
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

func (fsm *fsm1In0OutSync[_, _, _]) start() (*Controller, error) {
	go fsm.run()

	err := <-fsm.initErrCh
	close(fsm.initErrCh)

	if err != nil { // Initialization failed
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
		}, err
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
	}, nil
}

// run is the main goroutine that drives the FSM
func (fsm *fsm1In0OutSync[_, _, _]) run() {
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

func (fsm *fsm1In0OutSync[_, _, _]) transitionTo(newState ProcessorState) {
	oldState := fsm.getState()
	fsm.setState(newState)
	fsm.logger.Debug(logStateTransition, "from", oldState.String(), "to", newState.String())
}

func (fsm *fsm1In0OutSync[_, _, _]) processingLoop() {
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

func (fsm *fsm1In0OutSync[IO, I, _]) handleInput(i I) {
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

func (fsm *fsm1In0OutSync[IO, I, _]) processInput(i I) {
	var io IO

	in := io.AsInput(i)
	if err := fsm.processor.Process(in); err != nil {
		fsm.logger.With("error", err).Error(logProcessingError)
	}
}

func (fsm *fsm1In0OutSync[_, _, _]) handleControlRequest(ctlReq *wrappedRequest) {
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

func (fsm *fsm1In0OutSync[_, _, _]) cleanup() {
	close(fsm.doneCh)
	close(fsm.controlReqCh)

	// Close the processor and report any error
	fsm.closeErrCh <- fsm.processor.Close()
	fsm.logger.Info(logProcessorStopped)

	fsm.transitionTo(StateTerminated)
}
