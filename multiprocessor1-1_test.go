package processor_test

import (
	"fmt"
	"testing"
	"time"

	processor "github.com/l0rem1psum/coral"
	"github.com/stretchr/testify/assert"
)

func TestGeneric1In1OutSyncMultiProcessor_NormalFlow_CloseByInput(t *testing.T) {
	inputCh := make(chan []string)

	initErr := false

	numMultiProcessors := 10
	mockProcessors := make([]*MockGeneric1In1OutSyncProcessor, numMultiProcessors)
	for i := 0; i < numMultiProcessors; i++ {
		mockProcessors[i] = NewMockGeneric1In1OutSyncProcessor(initErr)
	}

	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncMultiProcessor[*mock1In1OutSyncProcessorIO](mockProcessors)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for i := 0; i < 10; i++ {
			inputs := make([]string, numMultiProcessors)
			for j := 0; j < numMultiProcessors; j++ {
				inputs[j] = "hello"
			}
			inputCh <- inputs
		}
		close(inputCh)
	}()

	go func() {
		for s := range outputCh {
			fmt.Println(s)
		}
	}()

	time.Sleep(200 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)
}

func TestGeneric1In1OutSyncMultiProcessor_NormalFlow_CloseByStop(t *testing.T) {
	inputCh := make(chan []string)

	initErr := false

	numMultiProcessors := 10
	mockProcessors := make([]*MockGeneric1In1OutSyncProcessor, numMultiProcessors)
	for i := 0; i < numMultiProcessors; i++ {
		mockProcessors[i] = NewMockGeneric1In1OutSyncProcessor(initErr)
	}

	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncMultiProcessor[*mock1In1OutSyncProcessorIO](mockProcessors)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputs := make([]string, numMultiProcessors)
			for j := 0; j < numMultiProcessors; j++ {
				inputs[j] = "hello"
			}
			inputCh <- inputs
			time.Sleep(10 * time.Millisecond)
		}
	}()

	go func() {
		for s := range outputCh {
			fmt.Println(s)
		}
	}()

	time.Sleep(200 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)
}

func TestGeneric1In1OutSyncMultiProcessor_CloseWhenNotStarted(t *testing.T) {
	inputCh := make(chan []string)

	initErr := false

	numMultiProcessors := 10
	mockProcessors := make([]*MockGeneric1In1OutSyncProcessor, numMultiProcessors)
	for i := 0; i < numMultiProcessors; i++ {
		mockProcessors[i] = NewMockGeneric1In1OutSyncProcessor(initErr)
	}

	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncMultiProcessor[*mock1In1OutSyncProcessorIO](mockProcessors)(inputCh)
	assert.Nil(t, err)

	go func() {
		for i := 0; i < 100; i++ {
			inputs := make([]string, numMultiProcessors)
			for j := 0; j < numMultiProcessors; j++ {
				inputs[j] = "hello"
			}
			inputCh <- inputs
		}
	}()

	go func() {
		for s := range outputCh {
			fmt.Println(s)
		}
	}()

	time.Sleep(200 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)
}

func TestGeneric1In1OutSyncMultiProcessor_StopWhenInitFailed(t *testing.T) {
	inputCh := make(chan []string)

	initErr := true

	numMultiProcessors := 10
	mockProcessors := make([]*MockGeneric1In1OutSyncProcessor, numMultiProcessors)
	for i := 0; i < numMultiProcessors; i++ {
		mockProcessors[i] = NewMockGeneric1In1OutSyncProcessor(initErr)
	}

	controller, outputCh, errs := processor.InitializeGeneric1In1OutSyncMultiProcessor[*mock1In1OutSyncProcessorIO](mockProcessors)(inputCh)
	assert.Equal(t, numMultiProcessors, len(errs)) // Should stop init-ing when the first processor fails
	for _, err := range errs {
		assert.Error(t, err)
	}

	_ = outputCh

	startErr := controller.Start()
	assert.ErrorIs(t, startErr, processor.ErrUnableToStart)

	time.Sleep(200 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)
}

func TestGeneric1In1OutSyncMultiProcessor_MultipleStartAndStop(t *testing.T) {
	inputCh := make(chan []string)

	initErr := false

	numMultiProcessors := 10
	mockProcessors := make([]*MockGeneric1In1OutSyncProcessor, numMultiProcessors)
	for i := 0; i < numMultiProcessors; i++ {
		mockProcessors[i] = NewMockGeneric1In1OutSyncProcessor(initErr)
	}

	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncMultiProcessor[*mock1In1OutSyncProcessorIO](mockProcessors)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	startErr = controller.Start()
	assert.ErrorIs(t, startErr, processor.ErrMultipleStart)

	go func() {
		for i := 0; i < 10; i++ {
			inputs := make([]string, numMultiProcessors)
			for j := 0; j < numMultiProcessors; j++ {
				inputs[j] = "hello"
			}
			inputCh <- inputs
		}
		close(inputCh)
	}()

	go func() {
		for s := range outputCh {
			fmt.Println(s)
		}
	}()

	time.Sleep(200 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)

	stopErr = controller.Stop()
	assert.ErrorIs(t, stopErr, processor.ErrMultipleStop)
}

func TestGeneric1In1OutSyncMultiProcessor_StopWhenNoProducer(t *testing.T) {
	inputCh := make(chan []string)

	initErr := false

	numMultiProcessors := 10
	mockProcessors := make([]*MockGeneric1In1OutSyncProcessor, numMultiProcessors)
	for i := 0; i < numMultiProcessors; i++ {
		mockProcessors[i] = NewMockGeneric1In1OutSyncProcessor(initErr)
	}

	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncMultiProcessor[*mock1In1OutSyncProcessorIO](mockProcessors)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for s := range outputCh {
			fmt.Println(s)
		}
	}()

	time.Sleep(200 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)
}

func TestGeneric1In1OutSyncMultiProcessor_StopWhenNoConsumer(t *testing.T) {
	inputCh := make(chan []string)

	initErr := false

	numMultiProcessors := 10
	mockProcessors := make([]*MockGeneric1In1OutSyncProcessor, numMultiProcessors)
	for i := 0; i < numMultiProcessors; i++ {
		mockProcessors[i] = NewMockGeneric1In1OutSyncProcessor(initErr)
	}

	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncMultiProcessor[*mock1In1OutSyncProcessorIO](mockProcessors)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for i := 0; i < 10; i++ {
			inputs := make([]string, numMultiProcessors)
			for j := 0; j < numMultiProcessors; j++ {
				inputs[j] = "hello"
			}
			inputCh <- inputs
		}
		close(inputCh)
	}()

	_ = outputCh

	time.Sleep(200 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)
}

func TestGeneric1In1OutSyncMultiProcessor_PauseAndResume_ControlNotSupported(t *testing.T) {
	inputCh := make(chan []string)

	initErr := false

	numMultiProcessors := 10
	mockProcessors := make([]*MockGeneric1In1OutSyncProcessorWithoutControl, numMultiProcessors)
	for i := 0; i < numMultiProcessors; i++ {
		mockProcessors[i] = NewMockGeneric1In1OutSyncProcessorWithoutControl(initErr)
	}
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncMultiProcessor[*mock1In1OutSyncProcessorIO](mockProcessors)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputs := make([]string, numMultiProcessors)
			for j := 0; j < numMultiProcessors; j++ {
				inputs[j] = "hello"
			}
			inputCh <- inputs
			time.Sleep(10 * time.Millisecond)
		}
	}()

	go func() {
		for s := range outputCh {
			fmt.Println(s)
		}
	}()

	pauseErr := controller.Pause()
	assert.Nil(t, pauseErr)

	time.Sleep(200 * time.Millisecond)

	resumeErr := controller.Resume()
	assert.Nil(t, resumeErr)

	time.Sleep(200 * time.Millisecond)

	controlErr := controller.Control("hello")
	assert.ErrorIs(t, controlErr, processor.ErrControlNotSupported)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)
}

func TestGeneric1In1OutSyncMultiProcessor_PauseAndResume_ControlSupported(t *testing.T) {
	inputCh := make(chan []string)

	initErr := false

	numMultiProcessors := 10
	mockProcessors := make([]*MockGeneric1In1OutSyncProcessor, numMultiProcessors)
	for i := 0; i < numMultiProcessors; i++ {
		mockProcessors[i] = NewMockGeneric1In1OutSyncProcessor(initErr)
	}
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncMultiProcessor[*mock1In1OutSyncProcessorIO](mockProcessors)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputs := make([]string, numMultiProcessors)
			for j := 0; j < numMultiProcessors; j++ {
				inputs[j] = "hello"
			}
			inputCh <- inputs
			time.Sleep(10 * time.Millisecond)
		}
	}()

	go func() {
		for s := range outputCh {
			fmt.Println(s)
		}
	}()

	pauseErr := controller.Pause()
	assert.Nil(t, pauseErr)

	time.Sleep(200 * time.Millisecond)

	resumeErr := controller.Resume()
	assert.Nil(t, resumeErr)

	time.Sleep(200 * time.Millisecond)

	controlErr := controller.Control("hello")
	assert.Nil(t, controlErr)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)
}

func TestGeneric1In1OutSyncMultiProcessor_DoublePause(t *testing.T) {
	inputCh := make(chan []string)

	initErr := false

	numMultiProcessors := 10
	mockProcessors := make([]*MockGeneric1In1OutSyncProcessor, numMultiProcessors)
	for i := 0; i < numMultiProcessors; i++ {
		mockProcessors[i] = NewMockGeneric1In1OutSyncProcessor(initErr)
	}
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncMultiProcessor[*mock1In1OutSyncProcessorIO](mockProcessors)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputs := make([]string, numMultiProcessors)
			for j := 0; j < numMultiProcessors; j++ {
				inputs[j] = "hello"
			}
			inputCh <- inputs
			time.Sleep(10 * time.Millisecond)
		}
	}()

	go func() {
		for s := range outputCh {
			fmt.Println(s)
		}
	}()

	pauseErr := controller.Pause()
	assert.Nil(t, pauseErr)

	time.Sleep(200 * time.Millisecond)

	pauseErr = controller.Pause()
	assert.ErrorIs(t, pauseErr, processor.ErrAlreadyPaused)

	resumeErr := controller.Resume()
	assert.Nil(t, resumeErr)

	time.Sleep(200 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)
}

func TestGeneric1In1OutSyncMultiProcessor_DoubleResume(t *testing.T) {
	inputCh := make(chan []string)

	initErr := false

	numMultiProcessors := 10
	mockProcessors := make([]*MockGeneric1In1OutSyncProcessor, numMultiProcessors)
	for i := 0; i < numMultiProcessors; i++ {
		mockProcessors[i] = NewMockGeneric1In1OutSyncProcessor(initErr)
	}
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncMultiProcessor[*mock1In1OutSyncProcessorIO](mockProcessors)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputs := make([]string, numMultiProcessors)
			for j := 0; j < numMultiProcessors; j++ {
				inputs[j] = "hello"
			}
			inputCh <- inputs
			time.Sleep(10 * time.Millisecond)
		}
	}()

	go func() {
		for s := range outputCh {
			fmt.Println(s)
		}
	}()

	pauseErr := controller.Pause()
	assert.Nil(t, pauseErr)

	time.Sleep(200 * time.Millisecond)

	resumeErr := controller.Resume()
	assert.Nil(t, resumeErr)

	time.Sleep(200 * time.Millisecond)

	resumeErr = controller.Resume()
	assert.ErrorIs(t, resumeErr, processor.ErrAlreadyRunning)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)
}

func TestGeneric1In1OutSyncMultiProcessor_PauseAndResumeWhenNotStarted(t *testing.T) {
	inputCh := make(chan []string)

	initErr := false

	numMultiProcessors := 10
	mockProcessors := make([]*MockGeneric1In1OutSyncProcessor, numMultiProcessors)
	for i := 0; i < numMultiProcessors; i++ {
		mockProcessors[i] = NewMockGeneric1In1OutSyncProcessor(initErr)
	}
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncMultiProcessor[*mock1In1OutSyncProcessorIO](mockProcessors)(inputCh)
	assert.Nil(t, err)

	pauseErr := controller.Pause()
	assert.ErrorIs(t, pauseErr, processor.ErrProcessorNotRunning)

	time.Sleep(200 * time.Millisecond)

	resumeErr := controller.Resume()
	assert.ErrorIs(t, resumeErr, processor.ErrProcessorNotRunning)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputs := make([]string, numMultiProcessors)
			for j := 0; j < numMultiProcessors; j++ {
				inputs[j] = "hello"
			}
			inputCh <- inputs
			time.Sleep(10 * time.Millisecond)
		}
	}()

	go func() {
		for s := range outputCh {
			fmt.Println(s)
		}
	}()

	time.Sleep(200 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)
}

func TestGeneric1In1OutSyncMultiProcessor_PauseAndResumeAfterStopped(t *testing.T) {
	inputCh := make(chan []string)

	initErr := false

	numMultiProcessors := 10
	mockProcessors := make([]*MockGeneric1In1OutSyncProcessor, numMultiProcessors)
	for i := 0; i < numMultiProcessors; i++ {
		mockProcessors[i] = NewMockGeneric1In1OutSyncProcessor(initErr)
	}
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncMultiProcessor[*mock1In1OutSyncProcessorIO](mockProcessors)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputs := make([]string, numMultiProcessors)
			for j := 0; j < numMultiProcessors; j++ {
				inputs[j] = "hello"
			}
			inputCh <- inputs
			time.Sleep(10 * time.Millisecond)
		}
	}()

	go func() {
		for s := range outputCh {
			fmt.Println(s)
		}
	}()

	time.Sleep(200 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)

	pauseErr := controller.Pause()
	assert.ErrorIs(t, pauseErr, processor.ErrProcessorNotRunning)

	time.Sleep(200 * time.Millisecond)

	resumeErr := controller.Resume()
	assert.ErrorIs(t, resumeErr, processor.ErrProcessorNotRunning)
}

func TestGeneric1In1OutSyncMultiProcessor_StopWhenPaused(t *testing.T) {
	inputCh := make(chan []string)

	initErr := false

	numMultiProcessors := 10
	mockProcessors := make([]*MockGeneric1In1OutSyncProcessor, numMultiProcessors)
	for i := 0; i < numMultiProcessors; i++ {
		mockProcessors[i] = NewMockGeneric1In1OutSyncProcessor(initErr)
	}
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncMultiProcessor[*mock1In1OutSyncProcessorIO](mockProcessors)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputs := make([]string, numMultiProcessors)
			for j := 0; j < numMultiProcessors; j++ {
				inputs[j] = "hello"
			}
			inputCh <- inputs
			time.Sleep(10 * time.Millisecond)
		}
	}()

	go func() {
		for s := range outputCh {
			fmt.Println(s)
		}
	}()

	pauseErr := controller.Pause()
	assert.Nil(t, pauseErr)

	time.Sleep(200 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)
}

func TestGeneric1In1OutSyncMultiProcessor_ControlAfterInitFailed(t *testing.T) {
	inputCh := make(chan []string)

	initErr := true

	numMultiProcessors := 10
	mockProcessors := make([]*MockGeneric1In1OutSyncProcessor, numMultiProcessors)
	for i := 0; i < numMultiProcessors; i++ {
		mockProcessors[i] = NewMockGeneric1In1OutSyncProcessor(initErr)
	}
	controller, outputCh, errs := processor.InitializeGeneric1In1OutSyncMultiProcessor[*mock1In1OutSyncProcessorIO](mockProcessors)(inputCh)
	assert.Equal(t, numMultiProcessors, len(errs)) // Should stop init-ing when the first processor fails
	for _, err := range errs {
		assert.Error(t, err)
	}

	startErr := controller.Start()
	assert.Error(t, startErr)

	go func() {
		for {
			inputs := make([]string, numMultiProcessors)
			for j := 0; j < numMultiProcessors; j++ {
				inputs[j] = "hello"
			}
			inputCh <- inputs
			time.Sleep(10 * time.Millisecond)
		}
	}()

	go func() {
		for s := range outputCh {
			fmt.Println(s)
		}
	}()

	pauseErr := controller.Pause()
	assert.ErrorIs(t, pauseErr, processor.ErrProcessorNotRunning)

	time.Sleep(200 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)
}
