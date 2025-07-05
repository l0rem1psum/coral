package processor

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/go-multierror"
	"github.com/samber/lo"
)

// InitializeGeneric1In0OutSyncMultiProcessor[IO, I, In, P] creates multi-processor setup closure for parallel sink processing.
//   - IO: adapter implementing Generic1In0OutSyncProcessorIO[I, In]
//   - I: adapted input type from upstream channel
//   - In: raw input type from processor
//   - P: processor type implementing Generic1In0OutSyncProcessor[In]
//
// Returns closure that spawns processor goroutines and produces (*Controller, []error).
// Input slices are distributed 1:1 across processor instances for parallel execution.
func InitializeGeneric1In0OutSyncMultiProcessor[IO Generic1In0OutSyncProcessorIO[I, In], I, In any, P Generic1In0OutSyncProcessor[In]](processors []P, opts ...Option) func(inputs <-chan []I) (*Controller, []error) {
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

	return func(inputs <-chan []I) (*Controller, []error) {
		fsm := newFSMMultiProcessor1In0OutSync[IO](processors, config, logger, inputs)
		return fsm.Initialize()
	}
}

type fsmMultiProcessor1In0OutSync[IO Generic1In0OutSyncProcessorIO[I, In], I, In any, P Generic1In0OutSyncProcessor[In]] struct {
	*fsm

	processors             []P
	controllableProcessors []Controllable
	subProcessorFSMs       []*fsm1In0OutSync[IO, I, In]

	config config
	logger *slog.Logger

	inputsCh <-chan []I
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
}

func newFSMMultiProcessor1In0OutSync[IO Generic1In0OutSyncProcessorIO[I, In], I, In any, P Generic1In0OutSyncProcessor[In]](
	processors []P,
	config config,
	logger *slog.Logger,
	inputsCh <-chan []I,
) *fsmMultiProcessor1In0OutSync[IO, I, In, P] {
	subProcessorFSMs := make([]*fsm1In0OutSync[IO, I, In], len(processors))
	subProcessorInputChs := make([]chan I, len(processors))
	controllableProcessor := make([]Controllable, len(processors))

	for i, processor := range processors {
		subProcessorInputChs[i] = make(chan I)
		subProcessorFSMs[i] = newFSM1In0OutSync[IO](processor, config, logger.With("multiproc_index", i), subProcessorInputChs[i])
		controllableProcessor[i] = any(processor).(Controllable)
	}

	fsm := &fsmMultiProcessor1In0OutSync[IO, I, In, P]{
		fsm:                    &fsm{},
		processors:             processors,
		controllableProcessors: controllableProcessor,
		subProcessorFSMs:       subProcessorFSMs,
		config:                 config,
		logger:                 logger,
		inputsCh:               inputsCh,
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

	fsm.setState(StateCreated)
	return fsm
}

func (fsm *fsmMultiProcessor1In0OutSync[_, _, _, _]) Initialize() (*Controller, []error) {
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
		}, initErrs
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

func (fsm *fsmMultiProcessor1In0OutSync[_, _, _, _]) run() {
	fsm.transitionTo(StateInitializing)

	subControllers := make([]*Controller, len(fsm.processors))
	initErrs := make([]error, len(fsm.processors))

	var wg sync.WaitGroup
	var errorDuringInit atomic.Bool
	wg.Add(len(fsm.processors))

	for i := range fsm.subProcessorFSMs {
		go func(i int) {
			defer wg.Done()
			controller, err := fsm.subProcessorFSMs[i].Initialize()
			subControllers[i] = controller
			initErrs[i] = err
			if err != nil {
				errorDuringInit.Store(true)
			}
		}(i)
	}
	wg.Wait()

	fsm.subControllers = subControllers

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

func (fsm *fsmMultiProcessor1In0OutSync[_, _, _, _]) transitionTo(newState ProcessorState) {
	oldState := fsm.getState()
	fsm.setState(newState)
	fsm.logger.Debug("MultiProcessor state transition", "from", oldState.String(), "to", newState.String())
}

func (fsm *fsmMultiProcessor1In0OutSync[_, _, _, _]) processingLoop() {
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

func (fsm *fsmMultiProcessor1In0OutSync[IO, I, _, _]) handleInputBatch(inputs []I) {
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

func (fsm *fsmMultiProcessor1In0OutSync[IO, I, _, _]) processBatch(inputs []I) {
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

	for j, input := range inputs {
		go func(j int, input I) {
			defer wg.Done()
			fsm.subProcessorInputChs[j] <- input
		}(j, input)
	}
	wg.Wait()
}

func (fsm *fsmMultiProcessor1In0OutSync[_, _, _, P]) handleControlRequest(ctlReq *wrappedRequest) {
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

func (fsm *fsmMultiProcessor1In0OutSync[_, _, _, _]) coordinatedShutdown(subControllers []*Controller) {
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

func (fsm *fsmMultiProcessor1In0OutSync[_, _, _, _]) cleanup(subControllers []*Controller) {
	close(fsm.doneCh)
	close(fsm.controlReqCh)

	fsm.coordinatedShutdown(subControllers)
	fsm.logger.Info("Multiprocessor stopped")
}
