package processor

import (
	"log/slog"
	"sync"

	"github.com/samber/lo"
)

// GenericMInNOutSyncProcessor[In, Out] processes batched inputs and distributes to multiple outputs.
//   - Input: []In (slice of raw data type from batched upstream sources)
//   - Output: []Out (slice of raw data type produced by processor)
//   - Synchronous: Process([]In) blocks until all outputs ready
type GenericMInNOutSyncProcessor[In, Out any] interface {
	// NumOutputs returns the number of output channels this processor will use.
	// Must remain constant throughout processor lifetime for proper channel allocation.
	NumOutputs() int
	Init() error
	Process([]In) ([]Out, error)
	Close() error
}

// GenericMInNOutSyncProcessorIO[I, O, In, Out] adapts batched input/distributed output processor.
//   - I: adapted input type from upstream channel
//   - O: adapted output type for downstream consumers
//   - In: raw input type from processor
//   - Out: raw output type from processor
type GenericMInNOutSyncProcessorIO[I, O, In, Out any] interface {
	AsInput(I) In
	FromOutput(Out) O

	ReleaseInput(I)
	ReleaseOutput(O)
}

// InitializeGenericMInNOutSyncProcessor[IO, I, O, In, Out] creates processor setup closure.
//   - IO: adapter implementing GenericMInNOutSyncProcessorIO[I, O, In, Out]
//   - I: adapted input type from upstream channel
//   - O: adapted output type for downstream consumers
//   - In: raw input type from processor
//   - Out: raw output type from processor
//
// Returns closure that spawns processor goroutine and produces (*Controller, []chan O, error).
func InitializeGenericMInNOutSyncProcessor[IO GenericMInNOutSyncProcessorIO[I, O, In, Out], I, O, In, Out any](processor GenericMInNOutSyncProcessor[In, Out], opts ...Option) func(<-chan []I) (*Controller, []chan O, error) {
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

	return func(input <-chan []I) (*Controller, []chan O, error) {
		return newFSMMInNOutSync[IO](processor, config, logger, input).start()
	}
}

type fsmMInNOutSync[IO GenericMInNOutSyncProcessorIO[I, O, In, Out], I, O, In, Out any] struct {
	*fsm

	processor GenericMInNOutSyncProcessor[In, Out]

	config config
	logger *slog.Logger

	supportsControl bool
	controllable    Controllable

	inputCh       <-chan []I
	outputChs     []chan O
	closeCh       chan struct{}
	doneCh        chan struct{}
	initErrCh     chan error
	closeErrCh    chan error
	startCh       chan struct{}
	startDoneCh   chan struct{}
	stopAfterInit chan struct{}
	controlReqCh  chan *wrappedRequest
}

func newFSMMInNOutSync[
	IO GenericMInNOutSyncProcessorIO[I, O, In, Out],
	I, O, In, Out any,
](
	processor GenericMInNOutSyncProcessor[In, Out],
	config config,
	logger *slog.Logger,
	inputCh <-chan []I,
) *fsmMInNOutSync[IO, I, O, In, Out] {
	controllable, supportsControl := processor.(Controllable)

	outputChs := make([]chan O, processor.NumOutputs())
	for i := range outputChs {
		outputChs[i] = make(chan O)
	}

	fsm := &fsmMInNOutSync[IO, I, O, In, Out]{
		fsm:             &fsm{},
		processor:       processor,
		config:          config,
		logger:          logger,
		inputCh:         inputCh,
		supportsControl: supportsControl,
		controllable:    controllable,
		outputChs:       outputChs,
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

func (fsm *fsmMInNOutSync[_, _, O, _, _]) start() (*Controller, []chan O, error) {
	go fsm.run()

	err := <-fsm.initErrCh
	close(fsm.initErrCh)

	if err != nil { // Initialization failed
		closeMultipleChans(fsm.outputChs...)
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
					closeMultipleChans(fsm.outputChs...)
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
	}, fsm.outputChs, nil
}

// run is the main goroutine that drives the FSM
func (fsm *fsmMInNOutSync[_, _, _, _, _]) run() {
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

func (fsm *fsmMInNOutSync[_, _, _, _, _]) transitionTo(newState ProcessorState) {
	oldState := fsm.getState()
	fsm.setState(newState)
	fsm.logger.Debug("State transition", "from", oldState.String(), "to", newState.String())
}

func (fsm *fsmMInNOutSync[_, _, _, _, _]) processingLoop() {
LOOP:
	for {
		select {
		case inputBatch, ok := <-fsm.inputCh:
			if !ok {
				fsm.transitionTo(StateTerminating)
				fsm.logger.Info("Input channel closed, stopping")
				break LOOP
			}
			fsm.handleInputBatch(inputBatch)
		case ctlReq := <-fsm.controlReqCh:
			fsm.handleControlRequest(ctlReq)
		case <-fsm.closeCh:
			fsm.transitionTo(StateTerminating)
			fsm.logger.Info("Close signal received, stopping")
			break LOOP
		}
	}
}

func (fsm *fsmMInNOutSync[IO, I, _, _, _]) handleInputBatch(inputBatch []I) {
	var io IO

	switch fsm.getState() {
	case StateRunning:
		fsm.processBatch(inputBatch)
	case StatePaused:
		for _, input := range inputBatch {
			io.ReleaseInput(input)
		}
	default:
		panic("impossible state: " + fsm.getState().String())
	}
}

func (fsm *fsmMInNOutSync[IO, I, O, In, Out]) processBatch(inputBatch []I) {
	var io IO

	ins := lo.Map(inputBatch, func(i I, _ int) In { return io.AsInput(i) })

	outputs, err := fsm.processor.Process(ins)
	if err != nil {
		fsm.logger.With("error", err).Error("Error encountered during processing, continuing")
		return
	}

	outputsAdapted := lo.Map(outputs, func(out Out, _ int) O { return io.FromOutput(out) })

	fsm.handleOutputs(outputsAdapted)
}

func (fsm *fsmMInNOutSync[IO, _, O, _, _]) handleOutputs(outputs []O) {
	var wg sync.WaitGroup
	wg.Add(len(outputs))

	for i, output := range outputs {
		go func(channelIndex int, output O) {
			defer wg.Done()
			fsm.deliverToChannel(channelIndex, output)
		}(i, output)
	}

	wg.Wait()
}

func (fsm *fsmMInNOutSync[IO, _, O, _, _]) deliverToChannel(channelIndex int, output O) {
	var io IO

	if fsm.config.blockOnOutput {
		fsm.outputChs[channelIndex] <- output
	} else {
		select {
		case fsm.outputChs[channelIndex] <- output:
		default:
			select {
			case oldOutput := <-fsm.outputChs[channelIndex]:
				fsm.logger.With("output_index", channelIndex).Warn("Output channel full, dropping the frontmost/oldest output")
				io.ReleaseOutput(oldOutput)
				fsm.outputChs[channelIndex] <- output
			default:
				fsm.logger.With("output_index", channelIndex).Warn("Output channel full, dropping current output")
				io.ReleaseOutput(output)
			}
		}
	}
}

func (fsm *fsmMInNOutSync[_, _, _, _, _]) handleControlRequest(ctlReq *wrappedRequest) {
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

func (fsm *fsmMInNOutSync[IO, _, _, _, _]) cleanup() {
	var io IO

	close(fsm.doneCh)
	closeMultipleChans(fsm.outputChs...)
	close(fsm.controlReqCh)

	// Drain remaining outputs and release resources
	for _, outputCh := range fsm.outputChs {
		for o := range outputCh {
			io.ReleaseOutput(o)
		}
	}

	// Close the processor and report any error
	fsm.closeErrCh <- fsm.processor.Close()
	fsm.logger.Info("Processor stopped")

	fsm.transitionTo(StateTerminated)
}
