package processor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/samber/lo"
)

// InitializeGeneric1InNOutSyncMultiProcessor[IO, I, O, In, Out, P] creates multi-processor setup closure for parallel broadcasting.
//   - IO: adapter implementing Generic1InNOutSyncProcessorIO[I, O, In, Out]
//   - I: adapted input type from upstream channel
//   - O: adapted output type for downstream consumers
//   - In: raw input type from processor
//   - Out: raw output type from processor
//   - P: processor type implementing Generic1InNOutSyncProcessor[In, Out]
//
// Returns closure that spawns processor goroutines and produces (*Controller, []chan []O, []error).
// Input slices are distributed 1:1 across processor instances, outputs transposed across N channels.
func InitializeGeneric1InNOutSyncMultiProcessor[IO Generic1InNOutSyncProcessorIO[I, O, In, Out], I, O, In, Out any, P Generic1InNOutSyncProcessor[In, Out]](processors []P, opts ...Option) func(inputs <-chan []I) (*Controller, []chan []O, []error) {
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

	return func(inputs <-chan []I) (*Controller, []chan []O, []error) {
		return newFSMMultiProcessor1InNOutSync[IO](processors, config, logger, inputs).start()
	}
}

type fsmMultiProcessor1InNOutSync[IO Generic1InNOutSyncProcessorIO[I, O, In, Out], I, O, In, Out any, P Generic1InNOutSyncProcessor[In, Out]] struct {
	*fsm

	processors             []P
	controllableProcessors []Controllable
	subProcessorFSMs       []*fsm1InNOutSync[IO, I, O, In, Out]

	config  config
	logger  *slog.Logger
	metrics *metricsRecorder

	inputsCh  <-chan []I
	outputChs []chan []O
	closeCh   chan struct{}
	doneCh    chan struct{}

	initErrsCh    chan []error
	closeErrCh    chan error
	startCh       chan struct{}
	startDoneCh   chan struct{}
	startErrsCh   chan []error
	stopAfterInit chan struct{}
	controlReqCh  chan *wrappedRequest

	subProcessorInputChs []chan I

	subControllers []*Controller
	subOutputChans [][]chan O
}

func newFSMMultiProcessor1InNOutSync[IO Generic1InNOutSyncProcessorIO[I, O, In, Out], I, O, In, Out any, P Generic1InNOutSyncProcessor[In, Out]](
	processors []P,
	cfg config,
	logger *slog.Logger,
	inputsCh <-chan []I,
) *fsmMultiProcessor1InNOutSync[IO, I, O, In, Out, P] {
	subProcessorFSMs := make([]*fsm1InNOutSync[IO, I, O, In, Out], len(processors))
	subProcessorInputChs := make([]chan I, len(processors))
	controllableProcessors := make([]Controllable, len(processors))

	for i, processor := range processors {
		subProcessorInputChs[i] = make(chan I)
		// Always block on output channel to ensure all outputs are processed before next input batch
		subConfig := config{
			blockOnOutput: true,
			meterProvider: cfg.meterProvider,
		}
		if cfg.label != nil {
			subLabel := fmt.Sprintf("%s_%d", *cfg.label, i)
			subConfig.label = &subLabel
		}
		subProcessorFSMs[i] = newFSM1InNOutSync[IO](processor, subConfig, logger.With("multiproc_index", i), subProcessorInputChs[i])
		controllableProcessor, ok := any(processor).(Controllable)
		if ok {
			controllableProcessors[i] = controllableProcessor
		} else {
			controllableProcessors[i] = nil
		}
	}

	// Create output channels based on first processor's NumOutputs
	// TODO: validate all processors have same NumOutputs
	numOutputs := processors[0].NumOutputs()
	outputChs := make([]chan []O, numOutputs)
	for i := range outputChs {
		outputChs[i] = make(chan []O, cfg.outputChannelSize)
	}

	fsm := &fsmMultiProcessor1InNOutSync[IO, I, O, In, Out, P]{
		fsm:                    &fsm{},
		processors:             processors,
		controllableProcessors: controllableProcessors,
		subProcessorFSMs:       subProcessorFSMs,
		config:                 cfg,
		logger:                 logger,
		inputsCh:               inputsCh,
		outputChs:              outputChs,
		closeCh:                make(chan struct{}),
		doneCh:                 make(chan struct{}),
		initErrsCh:             make(chan []error),
		closeErrCh:             make(chan error),
		startCh:                make(chan struct{}),
		startDoneCh:            make(chan struct{}),
		startErrsCh:            make(chan []error),
		stopAfterInit:          make(chan struct{}),
		controlReqCh:           make(chan *wrappedRequest),
		subProcessorInputChs:   subProcessorInputChs,
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

func (fsm *fsmMultiProcessor1InNOutSync[_, _, O, _, _, _]) start() (*Controller, []chan []O, []error) {
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
		closeMultipleChans(fsm.outputChs...)
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

func (fsm *fsmMultiProcessor1InNOutSync[_, _, O, _, _, _]) run() {
	fsm.transitionTo(StateInitializing)

	subControllers := make([]*Controller, len(fsm.processors))
	subOutputChans := make([][]chan O, len(fsm.processors))
	initErrs := make([]error, len(fsm.processors))

	var wg sync.WaitGroup
	var errorDuringInit atomic.Bool
	wg.Add(len(fsm.processors))

	for i := range fsm.subProcessorFSMs {
		go func(i int) {
			defer wg.Done()
			controller, outputChans, err := fsm.subProcessorFSMs[i].start()
			subControllers[i] = controller
			subOutputChans[i] = outputChans
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

func (fsm *fsmMultiProcessor1InNOutSync[_, _, _, _, _, _]) transitionTo(newState ProcessorState) {
	oldState := fsm.getState()
	fsm.setState(newState)
	fsm.logger.Debug(logMultiStateTransition, "from", oldState.String(), "to", newState.String())
}

func (fsm *fsmMultiProcessor1InNOutSync[_, _, _, _, _, _]) processingLoop() {
LOOP:
	for {
		select {
		case is, ok := <-fsm.inputsCh:
			if !ok {
				fsm.transitionTo(StateTerminating)
				fsm.logger.Info(logInputChannelClosed)
				break LOOP
			}
			fsm.handleInputBatch(is)
		case ctlReq := <-fsm.controlReqCh:
			fsm.handleControlRequest(ctlReq)
		case <-fsm.closeCh:
			fsm.transitionTo(StateTerminating)
			fsm.logger.Info(logCloseSignalReceived)
			break LOOP
		}
	}
}

func (fsm *fsmMultiProcessor1InNOutSync[IO, I, O, _, _, _]) handleInputBatch(inputs []I) {
	var io IO

	switch fsm.getState() {
	case StateRunning:
		fsm.processBatch(inputs)
	case StatePaused:
		for _, i := range inputs {
			io.ReleaseInput(i)
			fsm.metrics.recordInputReleased(context.Background(), 0)
		}
	default:
		panic("impossible state: " + fsm.getState().String())
	}
}

func (fsm *fsmMultiProcessor1InNOutSync[IO, I, O, _, _, _]) processBatch(inputs []I) {
	var io IO

	if len(inputs) == 0 {
		fsm.logger.Warn(logInputBatchEmpty)
		return
	}

	if len(inputs) != len(fsm.processors) {
		fsm.logger.With("input_length", len(inputs), "processor_length", len(fsm.processors)).Warn(logInputLengthMismatch)
		for _, i := range inputs {
			io.ReleaseInput(i)
			fsm.metrics.recordInputReleased(context.Background(), 0)
		}
		return
	}

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(len(inputs))
	outputs := make([][]O, len(inputs))

	for j, input := range inputs {
		go func(j int, input I) {
			defer wg.Done()
			fsm.subProcessorInputChs[j] <- input

			// Collect outputs from all output channels of this processor
			numOutputs := len(fsm.subOutputChans[j])
			processorOutputs := make([]O, numOutputs)
			var outputWg sync.WaitGroup
			outputWg.Add(numOutputs)

			for k := 0; k < numOutputs; k++ {
				go func(k int) {
					defer outputWg.Done()
					processorOutputs[k] = <-fsm.subOutputChans[j][k]
				}(k)
			}
			outputWg.Wait()

			outputs[j] = processorOutputs
		}(j, input)
	}
	wg.Wait()
	fsm.metrics.recordProcessDuration(context.Background(), time.Since(start))
	fsm.metrics.recordInputProcessedSuccess(context.Background(), 0)

	transposedOutputs := transpose(outputs)

	fsm.handleOutputBatches(transposedOutputs)
}

func (fsm *fsmMultiProcessor1InNOutSync[IO, _, O, _, _, _]) handleOutputBatches(outputBatches [][]O) {
	var io IO

	var wg sync.WaitGroup
	wg.Add(len(outputBatches))

	for i, outputBatch := range outputBatches {
		go func(i int, outputBatch []O) {
			defer wg.Done()

			start := time.Now()
			if fsm.config.blockOnOutput {
				fsm.outputChs[i] <- outputBatch
			} else {
				select {
				case fsm.outputChs[i] <- outputBatch:
				default:
					select {
					case oldOutputBatch := <-fsm.outputChs[i]:
						fsm.logger.With("output_index", i).Warn(logOutputChannelFullDropOldestBatch)
						for _, o := range oldOutputBatch {
							io.ReleaseOutput(o)
							fsm.metrics.recordOutputReleased(context.Background(), i)
						}
						fsm.outputChs[i] <- outputBatch
					default:
						fsm.logger.With("output_index", i).Warn(logOutputChannelFullDropCurrentBatch)
						for _, o := range outputBatch {
							io.ReleaseOutput(o)
							fsm.metrics.recordOutputReleased(context.Background(), i)
						}
					}
				}
			}
			fsm.metrics.recordOutputDuration(context.Background(), i, time.Since(start))
		}(i, outputBatch)
	}
	wg.Wait()
}

func (fsm *fsmMultiProcessor1InNOutSync[_, _, _, _, _, P]) handleControlRequest(ctlReq *wrappedRequest) {
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

func (fsm *fsmMultiProcessor1InNOutSync[_, _, _, _, _, _]) coordinatedShutdown(subControllers []*Controller) {
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

func (fsm *fsmMultiProcessor1InNOutSync[_, _, _, _, _, _]) cleanup(subControllers []*Controller) {
	close(fsm.doneCh)
	closeMultipleChans(fsm.outputChs...)
	close(fsm.controlReqCh)

	fsm.coordinatedShutdown(subControllers)
	fsm.logger.Info(logMultiprocessorStopped)
}

func transpose[T any](slice [][]T) [][]T {
	xl := len(slice[0])
	yl := len(slice)
	result := make([][]T, xl)
	for i := range result {
		result[i] = make([]T, yl)
	}
	for i := 0; i < xl; i++ {
		for j := 0; j < yl; j++ {
			result[i][j] = slice[j][i]
		}
	}
	return result
}
