package processor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/samber/lo"
)

type wrappedGenericNIn1OutSyncProcessorOutput[O any] struct {
	o   O
	err error
}

var _ GenericNIn1OutSyncProcessor[any, *wrappedGenericNIn1OutSyncProcessorOutput[any]] = (*wrappedGenericNIn1OutSyncProcessor[any, any])(nil)

type wrappedGenericNIn1OutSyncProcessor[In any, Out any] struct {
	processor GenericNIn1OutSyncProcessor[In, Out]
}

func (p *wrappedGenericNIn1OutSyncProcessor[In, Out]) Init() error {
	return p.processor.Init()
}

func (p *wrappedGenericNIn1OutSyncProcessor[In, Out]) Close() error {
	return p.processor.Close()
}

func (p *wrappedGenericNIn1OutSyncProcessor[In, Out]) Process(index int, input In) (*wrappedGenericNIn1OutSyncProcessorOutput[Out], error) {
	output, err := p.processor.Process(index, input)
	return &wrappedGenericNIn1OutSyncProcessorOutput[Out]{o: output, err: err}, nil
}

var _ GenericNIn1OutSyncProcessor[any, *wrappedGenericNIn1OutSyncProcessorOutput[any]] = (*wrappedControllableGenericNIn1OutSyncProcessor[any, any])(nil)

type wrappedControllableGenericNIn1OutSyncProcessor[In any, Out any] struct {
	processor GenericNIn1OutSyncProcessor[In, Out]
}

func (p *wrappedControllableGenericNIn1OutSyncProcessor[In, Out]) Init() error {
	return p.processor.Init()
}

func (p *wrappedControllableGenericNIn1OutSyncProcessor[In, Out]) Close() error {
	return p.processor.Close()
}

func (p *wrappedControllableGenericNIn1OutSyncProcessor[In, Out]) Process(index int, input In) (*wrappedGenericNIn1OutSyncProcessorOutput[Out], error) {
	output, err := p.processor.Process(index, input)
	return &wrappedGenericNIn1OutSyncProcessorOutput[Out]{o: output, err: err}, nil
}

func (p *wrappedControllableGenericNIn1OutSyncProcessor[In, Out]) OnControl(req any) error {
	if controllableProcessor, ok := p.processor.(Controllable); ok {
		return controllableProcessor.OnControl(req)
	}
	panic("processor does not implement Controllable interface")
}

type wrappedGenericNIn1OutSyncProcessorIO[IO GenericNIn1OutSyncProcessorIO[I, O, In, Out], I, O, In, Out any] struct{}

var _ GenericNIn1OutSyncProcessorIO[
	any,
	*wrappedGenericNIn1OutSyncProcessorOutput[any],
	any,
	*wrappedGenericNIn1OutSyncProcessorOutput[any],
] = (*wrappedGenericNIn1OutSyncProcessorIO[GenericNIn1OutSyncProcessorIO[any, any, any, any], any, any, any, any])(nil)

func (io *wrappedGenericNIn1OutSyncProcessorIO[IO, I, _, In, _]) AsInput(i I) In {
	var wrappedIO IO
	return wrappedIO.AsInput(i)
}

func (io *wrappedGenericNIn1OutSyncProcessorIO[IO, I, O, _, Out]) FromOutput(i I, out *wrappedGenericNIn1OutSyncProcessorOutput[Out]) *wrappedGenericNIn1OutSyncProcessorOutput[O] {
	var wrappedIO IO
	return &wrappedGenericNIn1OutSyncProcessorOutput[O]{o: wrappedIO.FromOutput(i, out.o), err: out.err}
}

func (io *wrappedGenericNIn1OutSyncProcessorIO[IO, I, _, _, _]) ReleaseInput(i I) {
	var wrappedIO IO
	wrappedIO.ReleaseInput(i)
}

func (io *wrappedGenericNIn1OutSyncProcessorIO[IO, _, O, _, _]) ReleaseOutput(o *wrappedGenericNIn1OutSyncProcessorOutput[O]) {
	var wrappedIO IO
	wrappedIO.ReleaseOutput(o.o)
}

// InitializeGenericNIn1OutSyncMultiProcessor[IO, I, O, In, Out, P] creates multi-processor setup closure for parallel N-1 transformation.
//   - IO: adapter implementing GenericNIn1OutSyncProcessorIO[I, O, In, Out]
//   - I: adapted input type from upstream channels
//   - O: adapted output type for downstream consumers
//   - In: raw input type from processor
//   - Out: raw output type from processor
//   - P: processor type implementing GenericNIn1OutSyncProcessor[In, Out]
//
// Returns closure that spawns processor goroutines and produces (*Controller, chan []O, []error).
// Input slices are transposed and distributed to processor instances, outputs collected into synchronized batches.
func InitializeGenericNIn1OutSyncMultiProcessor[IO GenericNIn1OutSyncProcessorIO[I, O, In, Out], I, O, In, Out any, P GenericNIn1OutSyncProcessor[In, Out]](processors []P, opts ...Option) func(inputs []<-chan []I) (*Controller, chan []O, []error) {
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

	wrappedProcessors := make([]GenericNIn1OutSyncProcessor[In, *wrappedGenericNIn1OutSyncProcessorOutput[Out]], len(processors))
	for i, processor := range processors {
		if _, ok := any(processor).(Controllable); ok {
			wrappedProcessors[i] = &wrappedControllableGenericNIn1OutSyncProcessor[In, Out]{processor: processor}
		} else {
			wrappedProcessors[i] = &wrappedGenericNIn1OutSyncProcessor[In, Out]{processor: processor}
		}
	}

	return func(inputs []<-chan []I) (*Controller, chan []O, []error) {
		return newFSMMultiProcessorNIn1OutSync[IO](wrappedProcessors, config, logger, inputs).start()
	}
}

type fsmMultiProcessorNIn1OutSync[IO GenericNIn1OutSyncProcessorIO[I, O, In, Out], I, O, In, Out any, P GenericNIn1OutSyncProcessor[In, *wrappedGenericNIn1OutSyncProcessorOutput[Out]]] struct {
	*fsm

	processors             []P
	controllableProcessors []Controllable
	subProcessorFSMs       []*fsmNIn1OutSync[
		*wrappedGenericNIn1OutSyncProcessorIO[IO, I, O, In, Out],
		I, *wrappedGenericNIn1OutSyncProcessorOutput[O], In, *wrappedGenericNIn1OutSyncProcessorOutput[Out],
	]

	config  config
	logger  *slog.Logger
	metrics *metricsRecorder

	inputsChs     []<-chan []I
	fannedInputCh <-chan fannedInResult[[]I]
	outputCh      chan []O
	closeCh       chan struct{}
	doneCh        chan struct{}

	initErrsCh    chan []error
	closeErrCh    chan error
	startCh       chan struct{}
	startDoneCh   chan struct{}
	startErrsCh   chan []error
	stopAfterInit chan struct{}
	controlReqCh  chan *wrappedRequest

	subProcessorInputChss [][]chan I

	subControllers []*Controller
	subOutputChans []chan *wrappedGenericNIn1OutSyncProcessorOutput[O]
}

func newFSMMultiProcessorNIn1OutSync[IO GenericNIn1OutSyncProcessorIO[I, O, In, Out], I, O, In, Out any, P GenericNIn1OutSyncProcessor[In, *wrappedGenericNIn1OutSyncProcessorOutput[Out]]](
	processors []P,
	cfg config,
	logger *slog.Logger,
	inputsChs []<-chan []I,
) *fsmMultiProcessorNIn1OutSync[IO, I, O, In, Out, P] {
	subProcessorFSMs := make([]*fsmNIn1OutSync[*wrappedGenericNIn1OutSyncProcessorIO[IO, I, O, In, Out],
		I, *wrappedGenericNIn1OutSyncProcessorOutput[O], In, *wrappedGenericNIn1OutSyncProcessorOutput[Out]], len(processors))
	subProcessorInputChss := make([][]chan I, len(processors))
	controllableProcessors := make([]Controllable, len(processors))

	for i, processor := range processors {
		subProcessorInputChss[i] = make([]chan I, len(inputsChs))
		for j := range inputsChs {
			subProcessorInputChss[i][j] = make(chan I)
		}

		// Always block on output channel to ensure all outputs are processed before next input batch
		subConfig := config{
			blockOnOutput:      true,
			meterProvider:      cfg.meterProvider,
			useRoundRobinFanIn: cfg.useRoundRobinFanIn,
		}
		if cfg.label != nil {
			subLabel := fmt.Sprintf("%s_%d", *cfg.label, i)
			subConfig.label = &subLabel
		}

		subProcessorFSMs[i] = newFSMNIn1OutSync[*wrappedGenericNIn1OutSyncProcessorIO[IO, I, O, In, Out]](processor, subConfig, logger.With("multiproc_index", i), bidirectionalChanSliceToDirectional(subProcessorInputChss[i]))
		controllableProcessor, ok := any(processor).(Controllable)
		if ok {
			controllableProcessors[i] = controllableProcessor
		} else {
			controllableProcessors[i] = nil
		}
	}

	fsm := &fsmMultiProcessorNIn1OutSync[IO, I, O, In, Out, P]{
		fsm:                    &fsm{},
		processors:             processors,
		controllableProcessors: controllableProcessors,
		subProcessorFSMs:       subProcessorFSMs,
		config:                 cfg,
		logger:                 logger,
		inputsChs:              inputsChs,
		outputCh:               make(chan []O, cfg.outputChannelSize),
		closeCh:                make(chan struct{}),
		doneCh:                 make(chan struct{}),
		initErrsCh:             make(chan []error),
		closeErrCh:             make(chan error),
		startCh:                make(chan struct{}),
		startDoneCh:            make(chan struct{}),
		startErrsCh:            make(chan []error),
		stopAfterInit:          make(chan struct{}),
		controlReqCh:           make(chan *wrappedRequest),
		subProcessorInputChss:  subProcessorInputChss,
	}

	if cfg.meterProvider != nil && cfg.label != nil {
		if metrics, err := newMetricsRecorder(cfg.meterProvider, *cfg.label); err != nil {
			logger.With("error", err).Warn("Failed to initialize multiprocessor metrics")
		} else {
			fsm.metrics = metrics
		}
	}

	fsm.setState(StateCreated)
	return fsm
}

func (fsm *fsmMultiProcessorNIn1OutSync[_, _, O, _, _, _]) start() (*Controller, chan []O, []error) {
	go fsm.run()

	initErrs := <-fsm.initErrsCh
	close(fsm.initErrsCh)

	errorDuringInit := false
	for _, err := range initErrs {
		if err != nil {
			errorDuringInit = true
			break
		}
	}

	if errorDuringInit {
		// Initialization failed
		close(fsm.outputCh)
		close(fsm.closeCh)
		close(fsm.doneCh)
		close(fsm.closeErrCh)
		close(fsm.startCh)
		close(fsm.startDoneCh)
		close(fsm.startErrsCh)
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
		}, nil, initErrs
	}

	// Initialization succeeded
	return &Controller{
		starter: &starter{
			f: func() error {
				close(fsm.startCh)
				<-fsm.startDoneCh
				// Wait for start errors
				startErrs := <-fsm.startErrsCh
				close(fsm.startErrsCh)

				var multierr error
				for _, err := range startErrs {
					if err != nil {
						multierr = multierror.Append(multierr, err)
					}
				}
				return multierr
			},
		},
		stopper: &stopper{
			f: func() error {
				if fsm.getState() == StateRunning ||
					fsm.getState() == StatePaused ||
					fsm.getState() == StateTerminating {
					close(fsm.closeCh)
					<-fsm.doneCh
				} else if fsm.getState() == StateWaitingToStart {
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

func (fsm *fsmMultiProcessorNIn1OutSync[_, _, O, _, _, _]) run() {
	fsm.transitionTo(StateInitializing)

	subControllers := make([]*Controller, len(fsm.processors))
	subOutputChans := make([]chan *wrappedGenericNIn1OutSyncProcessorOutput[O], len(fsm.processors))
	initErrs := make([]error, len(fsm.processors))

	var wg sync.WaitGroup
	var errorDuringInit atomic.Bool
	wg.Add(len(fsm.processors))

	for i := range fsm.subProcessorFSMs {
		go func(i int) {
			defer wg.Done()
			controller, outputCh, err := fsm.subProcessorFSMs[i].start()
			subControllers[i] = controller
			subOutputChans[i] = outputCh
			initErrs[i] = err
			if err != nil {
				errorDuringInit.Store(true)
			}
		}(i)
	}
	wg.Wait()

	fsm.subControllers = subControllers
	fsm.subOutputChans = subOutputChans

	fsm.initErrsCh <- initErrs
	if errorDuringInit.Load() {
		fsm.transitionTo(StateTerminated)
		return
	}
	fsm.transitionTo(StateWaitingToStart)

	// Wait for start signal or early stop
	select {
	case <-fsm.startCh:
		// Start all sub-processors in parallel
		var wg sync.WaitGroup
		var errorDuringStart atomic.Bool
		startErrs := make([]error, len(fsm.subControllers))
		wg.Add(len(fsm.subControllers))

		for i, controller := range fsm.subControllers {
			go func(i int, controller *Controller) {
				defer wg.Done()
				startErrs[i] = controller.Start()
				if startErrs[i] != nil {
					errorDuringStart.Store(true)
				}
			}(i, controller)
		}
		wg.Wait()

		if errorDuringStart.Load() {
			for i, err := range startErrs {
				if err != nil {
					fsm.logger.With("multiproc_index", i, "error", err).Error(logSubProcessorStartFailed)
				}
			}
			fsm.startErrsCh <- startErrs
			fsm.transitionTo(StateTerminated)
			close(fsm.startDoneCh)
			close(fsm.stopAfterInit)
			return
		}

		// Set up fan-in after successful start
		if fsm.config.useRoundRobinFanIn {
			fsm.fannedInputCh = roundRobinFanIn(0, fsm.inputsChs...)
		} else {
			fsm.fannedInputCh = parallelFanIn(0, fsm.inputsChs...)
		}

		if fsm.config.startPaused {
			fsm.transitionTo(StatePaused)
			fsm.logger.Info(logMultiprocessorStartedPaused)
		} else {
			fsm.transitionTo(StateRunning)
			fsm.logger.Info(logMultiprocessorStarted)
		}
		close(fsm.startDoneCh)
		close(fsm.stopAfterInit)
		fsm.startErrsCh <- startErrs
	case <-fsm.stopAfterInit:
		fsm.logger.Info(logMultiprocessorClosingAfterInit)
		fsm.transitionTo(StateTerminating)
		fsm.coordinatedShutdown(fsm.subControllers)
		return
	}

	fsm.processingLoop()
	fsm.cleanup(fsm.subControllers)
}

func (fsm *fsmMultiProcessorNIn1OutSync[_, _, _, _, _, _]) transitionTo(newState ProcessorState) {
	oldState := fsm.getState()
	fsm.setState(newState)
	fsm.logger.Debug(logMultiStateTransition, "from", oldState.String(), "to", newState.String())
}

func (fsm *fsmMultiProcessorNIn1OutSync[_, _, O, _, _, _]) processingLoop() {
LOOP:
	for {
		select {
		case isWithIndex, ok := <-fsm.fannedInputCh:
			if !ok {
				fsm.transitionTo(StateTerminating)
				fsm.logger.Info(logInputChannelClosed)
				break LOOP
			}
			fsm.handleInputBatch(isWithIndex)
		case ctlReq := <-fsm.controlReqCh:
			fsm.handleControlRequest(ctlReq)
		case <-fsm.closeCh:
			fsm.transitionTo(StateTerminating)
			fsm.logger.Info(logCloseSignalReceived)
			break LOOP
		}
	}
}

func (fsm *fsmMultiProcessorNIn1OutSync[IO, I, O, _, _, _]) handleInputBatch(inputSliceWithIndex fannedInResult[[]I]) {
	var io IO

	switch fsm.getState() {
	case StateRunning:
		fsm.processBatch(inputSliceWithIndex)
	case StatePaused:
		for _, item := range inputSliceWithIndex.t {
			io.ReleaseInput(item)
			fsm.metrics.recordInputReleased(context.Background(), inputSliceWithIndex.index)
		}
	default:
		panic("impossible state: " + fsm.getState().String())
	}
}

func (fsm *fsmMultiProcessorNIn1OutSync[IO, I, O, _, _, _]) processBatch(inputSliceWithIndex fannedInResult[[]I]) {
	var io IO

	inputs := inputSliceWithIndex.t
	channelIndex := inputSliceWithIndex.index

	if len(inputs) == 0 {
		fsm.logger.Warn(logInputBatchEmpty)
		return
	}

	if len(inputs) != len(fsm.processors) {
		fsm.logger.With("input_length", len(inputs), "processor_length", len(fsm.processors)).Warn(logInputLengthMismatch)
		for _, i := range inputs {
			io.ReleaseInput(i)
			fsm.metrics.recordInputReleased(context.Background(), channelIndex)
		}
		return
	}

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(len(inputs))
	outputs := make([]*wrappedGenericNIn1OutSyncProcessorOutput[O], len(inputs))

	for j, input := range inputs {
		go func(j int, input I) {
			defer wg.Done()
			fsm.subProcessorInputChss[j][channelIndex] <- input
			outputs[j] = <-fsm.subOutputChans[j]
		}(j, input)
	}

	wg.Wait()
	fsm.metrics.recordProcessDuration(context.Background(), time.Since(start))
	fsm.metrics.recordInputProcessedSuccess(context.Background(), channelIndex)

	fsm.handleOutputBatch(outputs)
}

func (fsm *fsmMultiProcessorNIn1OutSync[IO, _, O, _, _, _]) handleOutputBatch(outputs []*wrappedGenericNIn1OutSyncProcessorOutput[O]) {
	var io IO

	allSkip := true
	for _, output := range outputs {
		if !errors.Is(output.err, SkipResult) {
			allSkip = false
		}
	}
	if allSkip {
		return
	}

	unwrappedOutputs := make([]O, len(outputs))
	allErrors := true
	for i, output := range outputs {
		unwrappedOutputs[i] = output.o
		if output.err != nil {
			if errors.Is(output.err, SkipResult) {
				continue
			}
			fsm.subProcessorFSMs[i].logger.With("error", output.err).Error(logProcessingError)
			fsm.subProcessorFSMs[i].metrics.revertInputProcessedSuccess(context.Background(), 0)
			fsm.subProcessorFSMs[i].metrics.recordInputProcessedFailure(context.Background(), 0)
		} else {
			allErrors = false
		}
	}
	if allErrors {
		return
	}

	start := time.Now()
	if fsm.config.blockOnOutput {
		fsm.outputCh <- unwrappedOutputs
	} else {
		select {
		case fsm.outputCh <- unwrappedOutputs:
		default:
			select {
			case oldOutputs := <-fsm.outputCh:
				fsm.logger.Warn(logOutputChannelFullDropOldestBatch)
				for _, o := range oldOutputs {
					io.ReleaseOutput(o)
					fsm.metrics.recordOutputReleased(context.Background(), 0)
				}
				fsm.outputCh <- unwrappedOutputs
			default:
				fsm.logger.Warn(logOutputChannelFullDropCurrentBatch)
				for _, o := range outputs {
					io.ReleaseOutput(o.o)
					fsm.metrics.recordOutputReleased(context.Background(), 0)
				}
			}
		}
	}
	fsm.metrics.recordOutputDuration(context.Background(), 0, time.Since(start))
}

func (fsm *fsmMultiProcessorNIn1OutSync[_, _, _, _, _, P]) handleControlRequest(ctlReq *wrappedRequest) {
	switch ctlReq.req.(type) {
	case pause:
		if fsm.getState() == StateRunning {
			fsm.transitionTo(StatePaused)
			ctlReq.res <- nil
			fsm.logger.Info(logMultiprocessorPaused)
		} else if fsm.getState() == StatePaused {
			ctlReq.res <- ErrAlreadyPaused
		} else {
			panic("impossible state: " + fsm.getState().String())
		}
	case resume:
		if fsm.getState() == StatePaused {
			fsm.transitionTo(StateRunning)
			ctlReq.res <- nil
			fsm.logger.Info(logMultiprocessorResumed)
		} else if fsm.getState() == StateRunning {
			ctlReq.res <- ErrAlreadyRunning
		} else {
			panic("impossible state: " + fsm.getState().String())
		}
	case *MultiProcessorRequest:
		multiReq := ctlReq.req.(*MultiProcessorRequest)
		if controllableProcessor := fsm.controllableProcessors[multiReq.I]; controllableProcessor == nil {
			ctlReq.res <- ErrControlNotSupported
			return
		} else {
			ctlReq.res <- controllableProcessor.OnControl(multiReq.Req)
		}
	default:
		if lo.SomeBy(fsm.controllableProcessors, func(c Controllable) bool {
			return c == nil
		}) {
			ctlReq.res <- ErrControlNotSupported
			return
		}

		// Broadcast control request to all sub-processors
		var wg sync.WaitGroup
		wg.Add(len(fsm.controllableProcessors))
		ctlErrs := make([]error, len(fsm.controllableProcessors))
		for i, controllableProcessor := range fsm.controllableProcessors {
			go func(i int, controllableProcessor Controllable) {
				defer wg.Done()
				ctlErrs[i] = controllableProcessor.OnControl(ctlReq.req)
			}(i, controllableProcessor)
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
}

func (fsm *fsmMultiProcessorNIn1OutSync[_, _, _, _, _, _]) coordinatedShutdown(subControllers []*Controller) {
	var wg sync.WaitGroup
	wg.Add(len(subControllers))
	closeErrs := make([]error, len(subControllers))

	for i, controller := range subControllers {
		go func(i int, controller *Controller) {
			defer wg.Done()
			closeErrs[i] = controller.Stop()
		}(i, controller)
	}
	wg.Wait()

	var multierr error
	for _, err := range closeErrs {
		if err != nil {
			multierr = multierror.Append(multierr, err)
		}
	}
	fsm.closeErrCh <- multierr
	fsm.transitionTo(StateTerminated)
}

func (fsm *fsmMultiProcessorNIn1OutSync[_, _, _, _, _, _]) cleanup(subControllers []*Controller) {
	close(fsm.doneCh)
	close(fsm.outputCh)
	close(fsm.controlReqCh)

	fsm.coordinatedShutdown(subControllers)
	fsm.logger.Info(logMultiprocessorStopped)
}
