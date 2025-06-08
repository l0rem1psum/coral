package processorutils

import processor "github.com/l0rem1psum/coral"

var _ processor.GenericMInNOutSyncProcessor[any, any] = &PassthroughNInNOutSyncProcessor[any]{}

type PassthroughNInNOutSyncProcessor[T any] struct {
	numOutputs int
}

func NewPassthrough1InNOutSyncProcessor[T any](numOutputs int) *PassthroughNInNOutSyncProcessor[T] {
	return &PassthroughNInNOutSyncProcessor[T]{numOutputs: numOutputs}
}

func (p *PassthroughNInNOutSyncProcessor[T]) NumOutputs() int {
	return p.numOutputs
}

func (p *PassthroughNInNOutSyncProcessor[T]) Init() error {
	return nil
}

func (p *PassthroughNInNOutSyncProcessor[T]) Process(ins []T) ([]T, error) {
	return ins, nil
}

func (p *PassthroughNInNOutSyncProcessor[T]) Close() error {
	return nil
}
