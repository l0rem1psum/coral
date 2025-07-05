package processor

import (
	"log/slog"
)

// GenericNIn0OutAsyncProcessor[In] consumes data from multiple input streams without output.
//   - Input: In (raw data type from multiple upstream channels)
//   - Output: None (side effects only)
//   - Asynchronous: Process(index, In) called per input channel, index identifies source channel
type GenericNIn0OutAsyncProcessor[In any] interface {
	Init() error
	Process(int, In) error
	Close() error
}

// GenericNIn0OutAsyncProcessorIO[I, In] adapts multiple input sink.
//   - I: adapted input type from upstream channels
//   - In: raw input type from processor
//   - Converts I → In and manages I lifecycle
type GenericNIn0OutAsyncProcessorIO[I, In any] interface {
	AsInput(I) In

	ReleaseInput(I)
}

// InitializeGenericNIn0OutAsyncProcessor[IO, I, In] creates processor setup closure.
//   - IO: adapter implementing GenericNIn0OutAsyncProcessorIO[I, In]
//   - I: adapted input type from upstream channels
//   - In: raw input type from processor
//
// Returns closure that spawns processor goroutine and produces (*Controller, error).
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
		return newFSMNIn0OutAsync[IO](processor, config, logger, inputs).start()
	}
}

type fsmNIn0OutAsync[IO GenericNIn0OutAsyncProcessorIO[I, In], I, In any] struct {
	*fsm

	processor             GenericNIn0OutAsyncProcessor[In]
	controllableProcessor Controllable

	config config
	logger *slog.Logger

	inputChs      []<-chan I
	fannedInputCh <-chan fannedInResult[I]
	closeCh       chan struct{}
	doneCh        chan struct{}
	initErrCh     chan error
	closeErrCh    chan error
	startCh       chan struct{}
	startDoneCh   chan struct{}
	stopAfterInit chan struct{}
	controlReqCh  chan *wrappedRequest
}

func newFSMNIn0OutAsync[
	IO GenericNIn0OutAsyncProcessorIO[I, In],
	I, In any,
](
	processor GenericNIn0OutAsyncProcessor[In],
	config config,
	logger *slog.Logger,
	inputChs []<-chan I,
) *fsmNIn0OutAsync[IO, I, In] {
	fsm := &fsmNIn0OutAsync[IO, I, In]{
		fsm:           &fsm{},
		processor:     processor,
		config:        config,
		logger:        logger,
		inputChs:      inputChs,
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

func (fsm *fsmNIn0OutAsync[_, _, _]) start() (*Controller, error) {
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
func (fsm *fsmNIn0OutAsync[_, _, _]) run() {
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
		// Set up fan-in after successful start
		if fsm.config.useRoundRobinFanIn {
			fsm.fannedInputCh = roundRobinFanIn(0, fsm.inputChs...)
		} else {
			fsm.fannedInputCh = parallelFanIn(0, fsm.inputChs...)
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

func (fsm *fsmNIn0OutAsync[_, _, _]) transitionTo(newState ProcessorState) {
	oldState := fsm.getState()
	fsm.setState(newState)
	fsm.logger.Debug("State transition", "from", oldState.String(), "to", newState.String())
}

func (fsm *fsmNIn0OutAsync[_, _, _]) processingLoop() {
LOOP:
	for {
		select {
		case i, ok := <-fsm.fannedInputCh:
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

func (fsm *fsmNIn0OutAsync[IO, I, _]) handleInput(input fannedInResult[I]) {
	var io IO

	switch fsm.getState() {
	case StateRunning:
		fsm.processInput(input)
	case StatePaused:
		io.ReleaseInput(input.t)
	default:
		panic("impossible state: " + fsm.getState().String())
	}
}

func (fsm *fsmNIn0OutAsync[IO, I, _]) processInput(input fannedInResult[I]) {
	var io IO

	in := io.AsInput(input.t)
	if err := fsm.processor.Process(input.index, in); err != nil {
		fsm.logger.With("error", err).Error("Error encountered during processing, continuing")
	}
}

func (fsm *fsmNIn0OutAsync[_, _, _]) handleControlRequest(ctlReq *wrappedRequest) {
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

func (fsm *fsmNIn0OutAsync[_, _, _]) cleanup() {
	close(fsm.doneCh)
	close(fsm.controlReqCh)

	// Close the processor and report any error
	fsm.closeErrCh <- fsm.processor.Close()
	fsm.logger.Info("Processor stopped")

	fsm.transitionTo(StateTerminated)
}
