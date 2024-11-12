package processorutils

import (
	"time"

	processor "github.com/l0rem1psum/coral"
)

var _ processor.Generic1In1OutAsyncProcessor[any, any] = &RateLimiting1In1OutAsyncProcessor[processor.Generic1In1OutAsyncProcessorIO[any, any, any, any], any]{}

type RateLimiting1In1OutAsyncProcessor[IO processor.Generic1In1OutAsyncProcessorIO[T, T, T, T], T any] struct {
	rate   float64
	ticker *time.Ticker

	inputCh  chan T
	outputCh chan T
	closeCh  chan struct{}
	doneCh   chan struct{}
}

func NewRateLimiting1In1OutAsyncProcessor[IO processor.Generic1In1OutAsyncProcessorIO[T, T, T, T], T any](ratePerSecond float64) *RateLimiting1In1OutAsyncProcessor[IO, T] {
	// TODO: check ratePerSecond > 0
	return &RateLimiting1In1OutAsyncProcessor[IO, T]{
		rate:     ratePerSecond,
		inputCh:  make(chan T),
		outputCh: make(chan T),
		closeCh:  make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

func (p *RateLimiting1In1OutAsyncProcessor[IO, T]) Init() error {
	p.ticker = time.NewTicker(time.Duration(1e9 / p.rate))
	go p.ratelimit()

	return nil
}

func (p *RateLimiting1In1OutAsyncProcessor[IO, T]) Process(t T) error {
	var io IO

	select {
	case p.inputCh <- t:
	default:
		io.ReleaseInput(t)
	}

	return nil
}

func (p *RateLimiting1In1OutAsyncProcessor[IO, T]) Output() <-chan T {
	return p.outputCh
}

func (p *RateLimiting1In1OutAsyncProcessor[IO, T]) Close() error {
	close(p.closeCh)
	<-p.doneCh

	return nil
}

func (p *RateLimiting1In1OutAsyncProcessor[IO, T]) ratelimit() {
	var io IO
LOOP:
	for {
		select {
		case <-p.closeCh:
			break LOOP
		case input := <-p.inputCh:
			select {
			case <-p.ticker.C:
				select {
				case p.outputCh <- input:
				default:
					io.ReleaseInput(input)
				}
			default:
				io.ReleaseInput(input)
			}
		}
	}
	close(p.outputCh)
	close(p.doneCh)
}
