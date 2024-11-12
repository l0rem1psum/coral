package processor

import (
	"sync"
	"sync/atomic"
	"time"
)

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
	MultiProcessorRequest struct {
		I   int
		Req any
	}
)

type wrappedRequest struct {
	req any
	res chan error
}

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

func (c *Controller) Start() error {
	return c.starter.Start()
}

func (c *Controller) Stop() error {
	return c.stopper.Stop()
}

func (c *Controller) Pause() error {
	return c.sendRequest(pause{})
}

func (c *Controller) Resume() error {
	return c.sendRequest(resume{})
}

func (c *Controller) Control(req any) error {
	return c.sendRequest(req)
}
