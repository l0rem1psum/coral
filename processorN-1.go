package processor

import (
	"log/slog"
)

// GenericNIn1OutAsyncProcessor[In, Out] aggregates multiple input streams into single output.
//   - Input: In (raw data type from multiple upstream channels)
//   - Output: Out (raw data type produced by processor) via self-managed Output() channel
//   - Asynchronous: Process(index, In) called per input channel, index identifies source channel, output generation decoupled
type GenericNIn1OutAsyncProcessor[In, Out any] interface {
	Init() error
	Process(int, In) error
	Output() <-chan Out
	Close() error
}

// GenericNIn1OutAsyncProcessorIO[I, O, In, Out] adapts multiple input aggregator input/output.
//   - I: adapted input type from upstream channels
//   - O: adapted output type for downstream consumers
//   - In: raw input type from processor
//   - Out: raw output type from processor
type GenericNIn1OutAsyncProcessorIO[I, O, In, Out any] interface {
	AsInput(I) In
	FromOutput(Out) O

	ReleaseInput(I)
	ReleaseOutput(O)
}

// InitializeGenericNIn1OutAsyncProcessor[IO, I, O, In, Out] creates async processor setup closure.
//   - IO: adapter implementing GenericNIn1OutAsyncProcessorIO[I, O, In, Out]
//   - I: adapted input type from upstream channels
//   - O: adapted output type for downstream consumers
//   - In: raw input type from processor
//   - Out: raw output type from processor
//
// Returns closure that spawns processor goroutine and produces (*Controller, chan O, error).
func InitializeGenericNIn1OutAsyncProcessor[IO GenericNIn1OutAsyncProcessorIO[I, O, In, Out], I, O, In, Out any](processor GenericNIn1OutAsyncProcessor[In, Out], opts ...Option) func([]<-chan I) (*Controller, chan O, error) {
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

	return func(inputs []<-chan I) (*Controller, chan O, error) {
		return newFSMNIn1OutAsync[IO](processor, config, logger, inputs).start()
	}
}

type fsmNIn1OutAsync[IO GenericNIn1OutAsyncProcessorIO[I, O, In, Out], I, O, In, Out any] struct {
	*fsm

	processor             GenericNIn1OutAsyncProcessor[In, Out]
	controllableProcessor Controllable

	config config
	logger *slog.Logger

	inputChs      []<-chan I
	fannedInputCh <-chan fannedInResult[I]
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

func newFSMNIn1OutAsync[
	IO GenericNIn1OutAsyncProcessorIO[I, O, In, Out],
	I, O, In, Out any,
](
	processor GenericNIn1OutAsyncProcessor[In, Out],
	config config,
	logger *slog.Logger,
	inputChs []<-chan I,
) *fsmNIn1OutAsync[IO, I, O, In, Out] {
	fsm := &fsmNIn1OutAsync[IO, I, O, In, Out]{
		fsm:           &fsm{},
		processor:     processor,
		config:        config,
		logger:        logger,
		inputChs:      inputChs,
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

func (fsm *fsmNIn1OutAsync[_, _, O, _, _]) start() (*Controller, chan O, error) {
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
func (fsm *fsmNIn1OutAsync[_, _, _, _, _]) run() {
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

func (fsm *fsmNIn1OutAsync[_, _, _, _, _]) transitionTo(newState ProcessorState) {
	oldState := fsm.getState()
	fsm.setState(newState)
	fsm.logger.Debug("State transition", "from", oldState.String(), "to", newState.String())
}

func (fsm *fsmNIn1OutAsync[_, _, _, _, _]) processingLoop() {
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

func (fsm *fsmNIn1OutAsync[IO, I, _, _, _]) handleInput(input fannedInResult[I]) {
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

func (fsm *fsmNIn1OutAsync[IO, I, _, _, _]) processInput(input fannedInResult[I]) {
	var io IO

	in := io.AsInput(input.t)
	if err := fsm.processor.Process(input.index, in); err != nil {
		fsm.logger.With("error", err).Error("Error encountered during processing, continuing")
	}
}

func (fsm *fsmNIn1OutAsync[IO, _, O, _, Out]) handleProcessorOutput(out Out) {
	var io IO

	uo := io.FromOutput(out)
	fsm.handleOutput(uo)
}

func (fsm *fsmNIn1OutAsync[IO, _, O, _, _]) handleOutput(output O) {
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

func (fsm *fsmNIn1OutAsync[_, _, _, _, _]) handleControlRequest(ctlReq *wrappedRequest) {
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

func (fsm *fsmNIn1OutAsync[IO, _, _, _, _]) cleanup() {
	var io IO

	close(fsm.doneCh)
	close(fsm.outputCh)
	close(fsm.controlReqCh)

	// Drain remaining outputs and release resources
	for o := range fsm.outputCh {
		io.ReleaseOutput(o)
	}

	// Drain any remaining processor outputs asynchronously
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
