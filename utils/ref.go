package processorutils

import (
	"sync"
	"sync/atomic"

	processor "github.com/l0rem1psum/coral"
)

var _ processor.Generic1InNOutSyncProcessor[*ReferenceInput, *ReferenceOutput] = &ReferenceProcessor{}

type (
	ReferenceProcessor struct {
		numRefs int
	}

	ReferenceInput struct {
		ReleaseFunc func()
	}
	ReferenceOutput struct {
		ReleaseFunc func()
	}
)

func NewReferenceProcessor(numRefs int) *ReferenceProcessor {
	return &ReferenceProcessor{
		numRefs: numRefs,
	}
}

func (rp *ReferenceProcessor) NumOutputs() int {
	return rp.numRefs
}

func (rp *ReferenceProcessor) Init() error {
	return nil
}

func (rp *ReferenceProcessor) Process(ri *ReferenceInput) ([]*ReferenceOutput, error) {
	var atomicCounter atomic.Int32
	atomicCounter.Store(int32(rp.numRefs))

	outputs := make([]*ReferenceOutput, rp.numRefs)
	for i := 0; i < rp.numRefs; i++ {
		outputs[i] = &ReferenceOutput{
			ReleaseFunc: sync.OnceFunc(func() {
				refCount := atomicCounter.Add(-1)
				if refCount == 0 {
					ri.ReleaseFunc()
				}
			}),
		}
	}
	return outputs, nil
}

func (rp *ReferenceProcessor) Close() error {
	return nil
}
