package processor

import (
	"context"
	"log/slog"
	"time"
)

// Generic2In1OutAsyncProcessor[In1, In2, Out] aggregates two input streams into single output.
//   - Input: In1 (raw data type from first upstream), In2 (raw data type from second upstream)
//   - Output: Out (raw data type produced by processor) via self-managed Output() channel
//   - Asynchronous: input processing and output generation decoupled
type Generic2In1OutAsyncProcessor[In1, In2, Out any] interface {
	Init() error
	Process1(In1) error
	Process2(In2) error
	Output() <-chan Out
	Close() error
}

// Generic2In1OutAsyncProcessorIO[I1, I2, O, In1, In2, Out] adapts async aggregator input/output.
//   - I1: adapted input type from first upstream channel
//   - I2: adapted input type from second upstream channel
//   - O: adapted output type for downstream consumers
//   - In1: raw input type from processor for first input
//   - In2: raw input type from processor for second input
//   - Out: raw output type from processor
type Generic2In1OutAsyncProcessorIO[I1, I2, O, In1, In2, Out any] interface {
	AsInput1(I1) In1
	AsInput2(I2) In2
	FromOutput(Out) O

	ReleaseInput1(I1)
	ReleaseInput2(I2)
	ReleaseOutput(O)
}

// InitializeGeneric2In1OutAsyncProcessor[IO, I1, I2, O, In1, In2, Out] creates async processor setup closure.
//   - IO: adapter implementing Generic2In1OutAsyncProcessorIO[I1, I2, O, In1, In2, Out]
//   - I1: adapted input type from first upstream channel
//   - I2: adapted input type from second upstream channel
//   - O: adapted output type for downstream consumers
//   - In1: raw input type from processor for first input
//   - In2: raw input type from processor for second input
//   - Out: raw output type from processor
//
// Returns closure that spawns processor goroutine and produces (*Controller, chan O, error).
func InitializeGeneric2In1OutAsyncProcessor[IO Generic2In1OutAsyncProcessorIO[I1, I2, O, In1, In2, Out], I1, I2, O, In1, In2, Out any](processor Generic2In1OutAsyncProcessor[In1, In2, Out], opts ...Option) func(<-chan I1, <-chan I2) (*Controller, chan O, error) {
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

	return func(input1 <-chan I1, input2 <-chan I2) (*Controller, chan O, error) {
		return newFSM2In1OutAsync[IO](processor, config, logger, input1, input2).start()
	}
}

type fsm2In1OutAsync[IO Generic2In1OutAsyncProcessorIO[I1, I2, O, In1, In2, Out], I1, I2, O, In1, In2, Out any] struct {
	*fsm

	processor             Generic2In1OutAsyncProcessor[In1, In2, Out]
	controllableProcessor Controllable

	config  config
	logger  *slog.Logger
	metrics *metricsRecorder

	input1Ch      <-chan I1
	input2Ch      <-chan I2
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

func newFSM2In1OutAsync[
	IO Generic2In1OutAsyncProcessorIO[I1, I2, O, In1, In2, Out],
	I1, I2, O, In1, In2, Out any,
](
	processor Generic2In1OutAsyncProcessor[In1, In2, Out],
	config config,
	logger *slog.Logger,
	input1Ch <-chan I1,
	input2Ch <-chan I2,
) *fsm2In1OutAsync[IO, I1, I2, O, In1, In2, Out] {
	fsm := &fsm2In1OutAsync[IO, I1, I2, O, In1, In2, Out]{
		fsm:           &fsm{},
		processor:     processor,
		config:        config,
		logger:        logger,
		input1Ch:      input1Ch,
		input2Ch:      input2Ch,
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

func (fsm *fsm2In1OutAsync[_, _, _, O, _, _, _]) start() (*Controller, chan O, error) {
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
func (fsm *fsm2In1OutAsync[_, _, _, _, _, _, _]) run() {
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

func (fsm *fsm2In1OutAsync[_, _, _, _, _, _, _]) transitionTo(newState ProcessorState) {
	oldState := fsm.getState()
	fsm.setState(newState)
	fsm.logger.Debug(logStateTransition, "from", oldState.String(), "to", newState.String())
}

func (fsm *fsm2In1OutAsync[_, _, _, _, _, _, _]) processingLoop() {
LOOP:
	for {
		select {
		case i1, ok := <-fsm.input1Ch:
			if !ok {
				fsm.transitionTo(StateTerminating)
				fsm.logger.With("input_index", 1).Info(logInputChannelClosed)
				break LOOP
			}
			fsm.handleInput1(i1)
		case i2, ok := <-fsm.input2Ch:
			if !ok {
				fsm.transitionTo(StateTerminating)
				fsm.logger.With("input_index", 2).Info(logInputChannelClosed)
				break LOOP
			}
			fsm.handleInput2(i2)
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

func (fsm *fsm2In1OutAsync[IO, I1, _, _, _, _, _]) handleInput1(input I1) {
	var io IO

	switch fsm.getState() {
	case StateRunning:
		fsm.processInput1(input)
	case StatePaused:
		io.ReleaseInput1(input)
		fsm.metrics.recordInputReleased(context.Background(), 0)
	default:
		panic("impossible state: " + fsm.getState().String())
	}
}

func (fsm *fsm2In1OutAsync[IO, _, I2, _, _, _, _]) handleInput2(input I2) {
	var io IO

	switch fsm.getState() {
	case StateRunning:
		fsm.processInput2(input)
	case StatePaused:
		io.ReleaseInput2(input)
		fsm.metrics.recordInputReleased(context.Background(), 1)
	default:
		panic("impossible state: " + fsm.getState().String())
	}
}

func (fsm *fsm2In1OutAsync[IO, I1, _, _, _, _, _]) processInput1(input I1) {
	var io IO

	in := io.AsInput1(input)

	start := time.Now()
	if err := fsm.processor.Process1(in); err != nil {
		fsm.logger.With("error", err, "input_index", 0).Error(logProcessingError)
		fsm.metrics.recordInputProcessedFailure(context.Background(), 0)
		return
	}
	fsm.metrics.recordProcessDuration(context.Background(), time.Since(start))
	fsm.metrics.recordInputProcessedSuccess(context.Background(), 0)
}

func (fsm *fsm2In1OutAsync[IO, _, I2, _, _, _, _]) processInput2(input I2) {
	var io IO

	in := io.AsInput2(input)

	start := time.Now()
	if err := fsm.processor.Process2(in); err != nil {
		fsm.logger.With("error", err, "input_index", 1).Error(logProcessingError)
		fsm.metrics.recordInputProcessedFailure(context.Background(), 1)
		return
	}
	fsm.metrics.recordProcessDuration(context.Background(), time.Since(start))
	fsm.metrics.recordInputProcessedSuccess(context.Background(), 1)
}

func (fsm *fsm2In1OutAsync[IO, _, _, O, _, _, Out]) handleProcessorOutput(out Out) {
	var io IO

	uo := io.FromOutput(out)
	fsm.handleOutput(uo)
}

func (fsm *fsm2In1OutAsync[IO, _, _, O, _, _, _]) handleOutput(output O) {
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

func (fsm *fsm2In1OutAsync[_, _, _, _, _, _, _]) handleControlRequest(ctlReq *wrappedRequest) {
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

func (fsm *fsm2In1OutAsync[IO, _, _, _, _, _, _]) cleanup() {
	var io IO

	close(fsm.doneCh)
	close(fsm.outputCh)
	close(fsm.controlReqCh)

	// Drain remaining outputs and release resources
	for o := range fsm.outputCh {
		io.ReleaseOutput(o)
		fsm.metrics.recordOutputReleased(context.Background(), 0)
	}

	// Drain any remaining processor outputs asynchronously
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
