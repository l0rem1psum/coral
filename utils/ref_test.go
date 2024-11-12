package processorutils_test

import (
	"testing"

	processorutils "github.com/l0rem1psum/coral/utils"
	"github.com/stretchr/testify/assert"
)

func TestRefProcessor(t *testing.T) {
	refCount := 10
	rp := processorutils.NewReferenceProcessor(refCount)

	flag := 1
	refInput := &processorutils.ReferenceInput{
		ReleaseFunc: func() {
			flag--
		},
	}

	outputs, err := rp.Process(refInput)
	assert.NoError(t, err)
	assert.Len(t, outputs, refCount)

	for _, output := range outputs {
		output.ReleaseFunc()
	}
	assert.Equal(t, 0, flag)
}

func TestRefProcessor_MultipleRefs(t *testing.T) {
	refCount := 10
	rp := processorutils.NewReferenceProcessor(refCount)

	flag1 := 1
	refInput1 := &processorutils.ReferenceInput{
		ReleaseFunc: func() {
			flag1--
		},
	}

	flag2 := 1
	refInput2 := &processorutils.ReferenceInput{
		ReleaseFunc: func() {
			flag2--
		},
	}

	outputs1, err := rp.Process(refInput1)
	assert.NoError(t, err)
	assert.Len(t, outputs1, refCount)

	for _, output := range outputs1 {
		output.ReleaseFunc()
	}
	assert.Equal(t, 0, flag1)
	assert.Equal(t, 1, flag2)

	outputs2, err := rp.Process(refInput2)
	assert.NoError(t, err)
	assert.Len(t, outputs2, refCount)

	for _, output := range outputs2 {
		output.ReleaseFunc()
	}
	assert.Equal(t, 0, flag1)
	assert.Equal(t, 0, flag2)
}
