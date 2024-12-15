package processorutils

import processor "github.com/l0rem1psum/coral"

var _ processor.Generic1In1OutSyncProcessor[any, any] = &Func1In1OutSyncProcessor[any, any]{}

type Func1In1OutSyncProcessor[In, Out any] struct {
	fn func(In) (Out, error)
}

func NewGenericMergeProcessor[In, Out any](fn func(In) (Out, error)) *Func1In1OutSyncProcessor[In, Out] {
	return &Func1In1OutSyncProcessor[In, Out]{
		fn: fn,
	}
}

func (p *Func1In1OutSyncProcessor[In, Out]) Init() error {
	return nil
}

func (p *Func1In1OutSyncProcessor[In, Out]) Process(in In) (Out, error) {
	return p.fn(in)
}

func (p *Func1In1OutSyncProcessor[In, Out]) Close() error {
	return nil
}
