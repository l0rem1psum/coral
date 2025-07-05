package processor

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/go-multierror"
)

// InitializeGeneric1In1OutSyncMultiProcessor[IO, I, O, In, Out, P] creates multi-processor setup closure for parallel transformation.
//   - IO: adapter implementing Generic1In1OutSyncProcessorIO[I, O, In, Out]
//   - I: adapted input type from upstream channel
//   - O: adapted output type for downstream consumers
//   - In: raw input type from processor
//   - Out: raw output type from processor
//   - P: processor type implementing Generic1In1OutSyncProcessor[In, Out]
//
// Returns closure that spawns processor goroutines and produces (*Controller, chan []O, []error).
// Input slices are distributed 1:1 across processor instances, outputs collected into slices.
func InitializeGeneric1In1OutSyncMultiProcessor[IO Generic1In1OutSyncProcessorIO[I, O, In, Out], I, O, In, Out any, P Generic1In1OutSyncProcessor[In, Out]](processors []P, opts ...Option) func(inputs <-chan []I) (*Controller, chan []O, []error) {
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

	return func(inputs <-chan []I) (*Controller, chan []O, []error) {
		fsm := newFSMMultiProcessor1In1OutSync[IO](processors, config, logger, inputs)
		return fsm.Initialize()
	}
}

type fsmMultiProcessor1In1OutSync[IO Generic1In1OutSyncProcessorIO[I, O, In, Out], I, O, In, Out any, P Generic1In1OutSyncProcessor[In, Out]] struct {
	*fsm

	processors       []P
	subProcessorFSMs []*fsm1In1OutSync[IO, I, O, In, Out]

	config config
	logger *slog.Logger

	allSupportControl bool

	inputsCh <-chan []I
	outputCh chan []O
	closeCh  chan struct{}
	doneCh   chan struct{}

	initErrsCh    chan []error
	closeErrCh    chan error
	startCh       chan struct{}
	startDoneCh   chan struct{}
	startErrsCh   chan []error
	stopAfterInit chan struct{}
	controlReqCh  chan *wrappedRequest

	subProcessorInputChs []chan I

	subControllers []*Controller
	subOutputChans []chan O
}

func newFSMMultiProcessor1In1OutSync[IO Generic1In1OutSyncProcessorIO[I, O, In, Out], I, O, In, Out any, P Generic1In1OutSyncProcessor[In, Out]](
	processors []P,
	config config,
	logger *slog.Logger,
	inputsCh <-chan []I,
) *fsmMultiProcessor1In1OutSync[IO, I, O, In, Out, P] {
	allSupportControl := true
	for _, p := range processors {
		if _, ok := any(p).(Controllable); !ok {
			allSupportControl = false
			break
		}
	}

	subProcessorFSMs := make([]*fsm1In1OutSync[IO, I, O, In, Out], len(processors))
	subProcessorInputChs := make([]chan I, len(processors))

	// Always block on output channel to ensure all outputs are processed before next input batch
	processorConfig := config
	processorConfig.blockOnOutput = true
	for i, processor := range processors {
		subProcessorInputChs[i] = make(chan I)
		subProcessorFSMs[i] = newFSM1In1OutSync[IO](processor, processorConfig, logger.With("multiproc_index", i), subProcessorInputChs[i])
	}

	fsm := &fsmMultiProcessor1In1OutSync[IO, I, O, In, Out, P]{
		fsm:                  &fsm{},
		processors:           processors,
		subProcessorFSMs:     subProcessorFSMs,
		config:               config,
		logger:               logger,
		allSupportControl:    allSupportControl,
		inputsCh:             inputsCh,
		outputCh:             make(chan []O),
		closeCh:              make(chan struct{}),
		doneCh:               make(chan struct{}),
		initErrsCh:           make(chan []error),
		closeErrCh:           make(chan error),
		startCh:              make(chan struct{}),
		startDoneCh:          make(chan struct{}),
		startErrsCh:          make(chan []error),
		stopAfterInit:        make(chan struct{}),
		controlReqCh:         make(chan *wrappedRequest),
		subProcessorInputChs: subProcessorInputChs,
	}

	fsm.setState(StateCreated)
	return fsm
}

func (fsm *fsmMultiProcessor1In1OutSync[_, _, O, _, _, _]) Initialize() (*Controller, chan []O, []error) {
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

func (fsm *fsmMultiProcessor1In1OutSync[_, _, O, _, _, _]) run() {
	fsm.transitionTo(StateInitializing)

	subControllers := make([]*Controller, len(fsm.processors))
	subOutputChans := make([]chan O, len(fsm.processors))
	initErrs := make([]error, len(fsm.processors))

	var wg sync.WaitGroup
	var errorDuringInit atomic.Bool
	wg.Add(len(fsm.processors))

	for i := range fsm.subProcessorFSMs {
		go func(i int) {
			defer wg.Done()
			controller, outputCh, err := fsm.subProcessorFSMs[i].Initialize()
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
					fsm.logger.With("multiproc_index", i, "error", err).Error("Failed to start sub-processor")
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
			fsm.logger.Info("Multiprocessor started in paused state")
		} else {
			fsm.transitionTo(StateRunning)
			fsm.logger.Info("Multiprocessor started")
		}
		close(fsm.startDoneCh)
		close(fsm.stopAfterInit)
		fsm.startErrsCh <- startErrs
	case <-fsm.stopAfterInit:
		fsm.logger.Info("Closing multiprocessor after initialization and before start")
		fsm.transitionTo(StateTerminating)
		fsm.coordinatedShutdown(fsm.subControllers)
		return
	}

	fsm.processingLoop()
	fsm.cleanup(fsm.subControllers)
}

func (fsm *fsmMultiProcessor1In1OutSync[_, _, _, _, _, _]) transitionTo(newState ProcessorState) {
	oldState := fsm.getState()
	fsm.setState(newState)
	fsm.logger.Debug("MultiProcessor state transition", "from", oldState.String(), "to", newState.String())
}

func (fsm *fsmMultiProcessor1In1OutSync[_, _, O, _, _, _]) processingLoop() {
LOOP:
	for {
		select {
		case is, ok := <-fsm.inputsCh:
			if !ok {
				fsm.transitionTo(StateTerminating)
				fsm.logger.Info("Input channel closed, stopping")
				break LOOP
			}
			fsm.handleInputBatch(is)
		case ctlReq := <-fsm.controlReqCh:
			fsm.handleControlRequest(ctlReq)
		case <-fsm.closeCh:
			fsm.transitionTo(StateTerminating)
			fsm.logger.Info("Close signal received, stopping")
			break LOOP
		}
	}
}

func (fsm *fsmMultiProcessor1In1OutSync[IO, I, O, _, _, _]) handleInputBatch(inputs []I) {
	var io IO

	switch fsm.getState() {
	case StateRunning:
		fsm.processBatch(inputs)
	case StatePaused:
		for _, i := range inputs {
			io.ReleaseInput(i)
		}
	default:
		panic("impossible state: " + fsm.getState().String())
	}
}

func (fsm *fsmMultiProcessor1In1OutSync[IO, I, O, _, _, _]) processBatch(inputs []I) {
	var io IO

	if len(inputs) == 0 {
		fsm.logger.Warn("Input batch is empty, dropping")
		return
	}

	if len(inputs) != len(fsm.processors) {
		fsm.logger.With("input_length", len(inputs), "processor_length", len(fsm.processors)).Warn("Input length mismatch, dropping")
		for _, i := range inputs {
			io.ReleaseInput(i)
		}
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(inputs))
	outputs := make([]O, len(inputs))

	for j, input := range inputs {
		go func(j int, input I) {
			defer wg.Done()
			fsm.subProcessorInputChs[j] <- input
			outputs[j] = <-fsm.subOutputChans[j]
		}(j, input)
	}
	wg.Wait()

	fsm.handleOutputBatch(outputs)
}

func (fsm *fsmMultiProcessor1In1OutSync[IO, _, O, _, _, _]) handleOutputBatch(outputs []O) {
	var io IO

	if fsm.config.blockOnOutput {
		fsm.outputCh <- outputs
	} else {
		select {
		case fsm.outputCh <- outputs:
		default:
			select {
			case oldOutputs := <-fsm.outputCh:
				fsm.logger.Warn("Output channel full, dropping oldest batch")
				for _, o := range oldOutputs {
					io.ReleaseOutput(o)
				}
				fsm.outputCh <- outputs
			default:
				fsm.logger.Warn("Output channel full, dropping current batch")
				for _, o := range outputs {
					io.ReleaseOutput(o)
				}
			}
		}
	}
}

func (fsm *fsmMultiProcessor1In1OutSync[_, _, _, _, _, P]) handleControlRequest(ctlReq *wrappedRequest) {
	switch ctlReq.req.(type) {
	case pause:
		if fsm.getState() == StateRunning {
			fsm.transitionTo(StatePaused)
			ctlReq.res <- nil
			fsm.logger.Info("Multiprocessor paused")
		} else if fsm.getState() == StatePaused {
			ctlReq.res <- ErrAlreadyPaused
		} else {
			panic("impossible state: " + fsm.getState().String())
		}
	case resume:
		if fsm.getState() == StatePaused {
			fsm.transitionTo(StateRunning)
			ctlReq.res <- nil
			fsm.logger.Info("Multiprocessor resumed")
		} else if fsm.getState() == StateRunning {
			ctlReq.res <- ErrAlreadyRunning
		} else {
			panic("impossible state: " + fsm.getState().String())
		}
	case *MultiProcessorRequest:
		if !fsm.allSupportControl {
			ctlReq.res <- ErrControlNotSupported
			return
		}
		multiReq := ctlReq.req.(*MultiProcessorRequest)
		ctlReq.res <- any(fsm.processors[multiReq.I]).(Controllable).OnControl(multiReq.Req)
	default:
		if !fsm.allSupportControl {
			ctlReq.res <- ErrControlNotSupported
			return
		}

		// Broadcast control request to all sub-processors
		var wg sync.WaitGroup
		wg.Add(len(fsm.processors))
		ctlErrs := make([]error, len(fsm.processors))
		for i, processor := range fsm.processors {
			go func(i int, processor P) {
				defer wg.Done()
				ctlErrs[i] = any(processor).(Controllable).OnControl(ctlReq.req)
			}(i, processor)
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

func (fsm *fsmMultiProcessor1In1OutSync[_, _, _, _, _, _]) coordinatedShutdown(subControllers []*Controller) {
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

func (fsm *fsmMultiProcessor1In1OutSync[_, _, _, _, _, _]) cleanup(subControllers []*Controller) {
	close(fsm.doneCh)
	close(fsm.outputCh)
	close(fsm.controlReqCh)

	fsm.coordinatedShutdown(subControllers)
	fsm.logger.Info("Multiprocessor stopped")
}
