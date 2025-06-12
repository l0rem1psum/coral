package processor

import (
	"sync"
	"sync/atomic"
	"time"
)

// Controllable allows processors to handle custom control messages.
// Processors implementing this interface can receive application-specific
// commands via Controller.Control() for runtime configuration or state changes.
type Controllable interface {
	OnControl(any) error
}

type starter struct {
	val  atomic.Int32
	once sync.Once
	err  error
	f    func() error
}

func (p *starter) Start() error {
	p.once.Do(func() {
		p.err = p.f()
	})
	if val := p.val.Load(); val >= 1 {
		return ErrMultipleStart
	}
	p.val.Add(1)
	return p.err
}

type stopper struct {
	val  atomic.Int32
	once sync.Once
	err  error
	f    func() error
}

func (p *stopper) Stop() error {
	p.once.Do(func() {
		p.err = p.f()
	})
	if val := p.val.Load(); val >= 1 {
		return ErrMultipleStop
	}
	p.val.Add(1)
	return p.err
}

var requestTimeout = time.Second

type (
	pause                 struct{}
	resume                struct{}
	// MultiProcessorRequest enables targeted control of specific processor instances within multi-processors.
	// I specifies the processor index, Req contains the control message for that specific processor.
	MultiProcessorRequest struct {
		I   int
		Req any
	}
)

type wrappedRequest struct {
	req any
	res chan error
}

// Controller provides thread-safe lifecycle management for processor goroutines.
// Each processor is managed by exactly one Controller that handles start/stop operations,
// pause/resume functionality, and custom control message delivery.
type Controller struct {
	// Start
	starter *starter

	// Stop
	stopper *stopper

	// Pause/resume & other control operations
	loopStarted *atomic.Bool
	loopEnded   *atomic.Bool
	reqCh       chan *wrappedRequest
}

func (c *Controller) sendRequest(req any) error {
	wreq := &wrappedRequest{
		req: req,
		res: make(chan error),
	}

	if !c.loopStarted.Load() {
		return ErrProcessorNotRunning
	}

	if c.loopEnded.Load() {
		return ErrProcessorNotRunning
	}

	select {
	case c.reqCh <- wreq:
	case <-time.After(requestTimeout):
		return ErrProcessorBusy
	}

	return <-wreq.res
}

// Start begins processor execution.
// Can only be called once per Controller. Returns ErrMultipleStart on subsequent calls.
func (c *Controller) Start() error {
	return c.starter.Start()
}

// Stop gracefully shuts down the processor and cleans up resources.
// Can only be called once per Controller. Returns ErrMultipleStop on subsequent calls.
func (c *Controller) Stop() error {
	return c.stopper.Stop()
}

// Pause temporarily halts processor execution while keeping the goroutine alive.
// Returns ErrAlreadyPaused if already paused, ErrProcessorNotRunning if not started.
func (c *Controller) Pause() error {
	return c.sendRequest(pause{})
}

// Resume restarts processor execution after being paused.
// Returns ErrAlreadyRunning if not paused, ErrProcessorNotRunning if not started.
func (c *Controller) Resume() error {
	return c.sendRequest(resume{})
}

// Control sends custom control messages to processors implementing Controllable.
// Returns ErrControlNotSupported if processor doesn't implement Controllable,
// ErrProcessorNotRunning if processor not started, or ErrProcessorBusy on timeout.
func (c *Controller) Control(req any) error {
	return c.sendRequest(req)
}
