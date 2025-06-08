package processor_test

import (
	"fmt"
	"testing"
	"time"

	processor "github.com/l0rem1psum/coral"
	"github.com/stretchr/testify/assert"
)

var (
	_ processor.Generic1In1OutSyncProcessor[*MockProcessorInput, *MockProcessorOutput] = &MockGeneric1In1OutSyncProcessor{}
	_ processor.Controllable                                                           = &MockGeneric1In1OutSyncProcessor{}
)

type (
	MockGeneric1In1OutSyncProcessor struct {
		// for testing
		initErr bool
	}
	MockGeneric1In1OutSyncProcessorWithoutControl struct {
		// for testing
		initErr bool
	}
	MockProcessorInput struct {
		str string
	}
	MockProcessorOutput struct {
		str string
	}
)

func NewMockGeneric1In1OutSyncProcessor(initErr bool) *MockGeneric1In1OutSyncProcessor {
	return &MockGeneric1In1OutSyncProcessor{
		initErr: initErr,
	}
}

func (mp *MockGeneric1In1OutSyncProcessor) Init() error {
	if mp.initErr {
		return fmt.Errorf("init error")
	}
	return nil
}

func (mp *MockGeneric1In1OutSyncProcessor) Process(ui *MockProcessorInput) (*MockProcessorOutput, error) {
	return &MockProcessorOutput{str: ui.str}, nil
}

func (mp *MockGeneric1In1OutSyncProcessor) Close() error {
	return nil
}

func (mp *MockGeneric1In1OutSyncProcessor) OnControl(a any) error {
	return nil
}

func NewMockGeneric1In1OutSyncProcessorWithoutControl(initErr bool) *MockGeneric1In1OutSyncProcessorWithoutControl {
	return &MockGeneric1In1OutSyncProcessorWithoutControl{
		initErr: initErr,
	}
}

func (mp *MockGeneric1In1OutSyncProcessorWithoutControl) Init() error {
	if mp.initErr {
		return fmt.Errorf("init error")
	}
	return nil
}

func (mp *MockGeneric1In1OutSyncProcessorWithoutControl) Process(ui *MockProcessorInput) (*MockProcessorOutput, error) {
	return &MockProcessorOutput{str: ui.str}, nil
}

func (mp *MockGeneric1In1OutSyncProcessorWithoutControl) Close() error {
	return nil
}

var _ processor.Generic1In1OutSyncProcessorIO[string, string, *MockProcessorInput, *MockProcessorOutput] = &mock1In1OutSyncProcessorIO{}

type mock1In1OutSyncProcessorIO struct{}

func (m *mock1In1OutSyncProcessorIO) AsInput(s string) *MockProcessorInput {
	return &MockProcessorInput{str: s}
}

func (m *mock1In1OutSyncProcessorIO) FromOutput(_ string, o *MockProcessorOutput) string {
	return o.str
}

func (m *mock1In1OutSyncProcessorIO) ReleaseOutput(o string) {}

func (m *mock1In1OutSyncProcessorIO) ReleaseInput(i string) {}

func TestGeneric1In1OutSyncProcessor_NormalFlow_CloseByInput(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutSyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for i := 0; i < 10; i++ {
			inputCh <- "hello"
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

func TestGeneric1In1OutSyncProcessor_NormalFlow_CloseByStop(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutSyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputCh <- "hello"
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

func TestGeneric1In1OutSyncProcessor_CloseWhenNotStarted(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutSyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	go func() {
		for i := 0; i < 10; i++ {
			inputCh <- "hello"
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

func TestGeneric1In1OutSyncProcessor_StopWhenInitFailed(t *testing.T) {
	inputCh := make(chan string)

	initErr := true
	mockProcessor := NewMockGeneric1In1OutSyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
	assert.Error(t, err)
	_ = outputCh

	startErr := controller.Start()
	assert.ErrorIs(t, startErr, processor.ErrUnableToStart)

	time.Sleep(200 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)
}

func TestGeneric1In1OutSyncProcessor_MultipleStartAndStop(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutSyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	startErr = controller.Start()
	assert.ErrorIs(t, startErr, processor.ErrMultipleStart)

	go func() {
		for i := 0; i < 10; i++ {
			inputCh <- "hello"
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

func TestGeneric1In1OutSyncProcessor_StopWhenNoProducer(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutSyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
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

func TestGeneric1In1OutSyncProcessor_StopWhenNoConsumer(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutSyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for i := 0; i < 10; i++ {
			inputCh <- "hello"
		}
		close(inputCh)
	}()

	_ = outputCh

	time.Sleep(200 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)
}

func TestGeneric1In1OutSyncProcessor_PauseAndResume_ControlNotSupported(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutSyncProcessorWithoutControl(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputCh <- "hello"
			time.Sleep(50 * time.Millisecond)
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

func TestGeneric1In1OutSyncProcessor_PauseAndResume_ControlSupported(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutSyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputCh <- "hello"
			time.Sleep(50 * time.Millisecond)
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

func TestGeneric1In1OutSyncProcessor_DoublePause(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutSyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputCh <- "hello"
			time.Sleep(50 * time.Millisecond)
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

func TestGeneric1In1OutSyncProcessor_DoubleResume(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutSyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputCh <- "hello"
			time.Sleep(50 * time.Millisecond)
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

func TestGeneric1In1OutSyncProcessor_PauseAndResumeWhenNotStarted(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutSyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
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
			inputCh <- "hello"
			time.Sleep(50 * time.Millisecond)
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

func TestGeneric1In1OutSyncProcessor_PauseAndResumeAfterStopped(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutSyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputCh <- "hello"
			time.Sleep(50 * time.Millisecond)
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

func TestGeneric1In1OutSyncProcessor_StopWhenPaused(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutSyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputCh <- "hello"
			time.Sleep(50 * time.Millisecond)
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

func TestGeneric1In1OutSyncProcessor_ControlAfterInitFailed(t *testing.T) {
	inputCh := make(chan string)

	initErr := true
	mockProcessor := NewMockGeneric1In1OutSyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
	assert.Error(t, err)

	startErr := controller.Start()
	assert.Error(t, startErr)

	go func() {
		for {
			inputCh <- "hello"
			time.Sleep(50 * time.Millisecond)
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

var (
	_ processor.Generic1In1OutAsyncProcessor[*MockProcessorInput, *MockProcessorOutput] = &MockGeneric1In1OutAsyncProcessor{}
	_ processor.Controllable                                                            = &MockGeneric1In1OutAsyncProcessor{}
)

type (
	MockGeneric1In1OutAsyncProcessor struct {
		inputCh  chan *MockProcessorInput
		outputCh chan *MockProcessorOutput
		closeCh  chan struct{}
		doneCh   chan struct{}

		// for testing
		initErr bool
	}
	MockGeneric1In1OutAsyncProcessorWithoutControl struct {
		inputCh  chan *MockProcessorInput
		outputCh chan *MockProcessorOutput
		closeCh  chan struct{}
		doneCh   chan struct{}

		// for testing
		initErr bool
	}
)

func NewMockGeneric1In1OutAsyncProcessor(initErr bool) *MockGeneric1In1OutAsyncProcessor {
	return &MockGeneric1In1OutAsyncProcessor{
		inputCh:  make(chan *MockProcessorInput),
		outputCh: make(chan *MockProcessorOutput),
		closeCh:  make(chan struct{}),
		doneCh:   make(chan struct{}),
		initErr:  initErr,
	}
}

func (mp *MockGeneric1In1OutAsyncProcessor) Init() error {
	if mp.initErr {
		return fmt.Errorf("init error")
	}
	go func() {
	LOOP:
		for {
			select {
			case i, ok := <-mp.inputCh:
				if !ok {
					break LOOP
				}
				select {
				case mp.outputCh <- &MockProcessorOutput{str: i.str}:
				default:
					fmt.Println("dropped")
				}
			case <-mp.closeCh:
				break LOOP
			}
		}
		close(mp.outputCh)
		close(mp.doneCh)
	}()
	return nil
}

func (mp *MockGeneric1In1OutAsyncProcessor) Process(ui *MockProcessorInput) error {
	select {
	case mp.inputCh <- ui:
	default:
		fmt.Println("dropped")
	}
	return nil
}

func (mp *MockGeneric1In1OutAsyncProcessor) Output() <-chan *MockProcessorOutput {
	return mp.outputCh
}

func (mp *MockGeneric1In1OutAsyncProcessor) Close() error {
	close(mp.closeCh)
	<-mp.doneCh
	return nil
}

func (mp *MockGeneric1In1OutAsyncProcessor) OnControl(a any) error {
	return nil
}

func NewMockGeneric1In1OutAsyncProcessorWithoutControl(initErr bool) *MockGeneric1In1OutAsyncProcessorWithoutControl {
	return &MockGeneric1In1OutAsyncProcessorWithoutControl{
		inputCh:  make(chan *MockProcessorInput),
		outputCh: make(chan *MockProcessorOutput),
		closeCh:  make(chan struct{}),
		doneCh:   make(chan struct{}),
		initErr:  initErr,
	}
}

func (mp *MockGeneric1In1OutAsyncProcessorWithoutControl) Init() error {
	if mp.initErr {
		return fmt.Errorf("init error")
	}
	go func() {
	LOOP:
		for {
			select {
			case i, ok := <-mp.inputCh:
				if !ok {
					break LOOP
				}
				select {
				case mp.outputCh <- &MockProcessorOutput{str: i.str}:
				default:
					fmt.Println("dropped")
				}
			case <-mp.closeCh:
				break LOOP
			}
		}
		close(mp.outputCh)
		close(mp.doneCh)
	}()
	return nil
}

func (mp *MockGeneric1In1OutAsyncProcessorWithoutControl) Process(ui *MockProcessorInput) error {
	select {
	case mp.inputCh <- ui:
	default:
		fmt.Println("dropped")
	}
	return nil
}

func (mp *MockGeneric1In1OutAsyncProcessorWithoutControl) Output() <-chan *MockProcessorOutput {
	return mp.outputCh
}

func (mp *MockGeneric1In1OutAsyncProcessorWithoutControl) Close() error {
	close(mp.closeCh)
	<-mp.doneCh
	return nil
}

var _ processor.Generic1In1OutAsyncProcessorIO[string, string, *MockProcessorInput, *MockProcessorOutput] = &mock1In1OutAsyncProcessorIO{}

type mock1In1OutAsyncProcessorIO struct{}

func (m *mock1In1OutAsyncProcessorIO) AsInput(s string) *MockProcessorInput {
	return &MockProcessorInput{str: s}
}

func (m *mock1In1OutAsyncProcessorIO) FromOutput(o *MockProcessorOutput) string {
	return o.str
}

func (m *mock1In1OutAsyncProcessorIO) ReleaseOutput(o string) {}

func (m *mock1In1OutAsyncProcessorIO) ReleaseInput(i string) {}

func TestGeneric1In1OutAsyncProcessor_NormalFlow_CloseByInput(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutAsyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutAsyncProcessor[*mock1In1OutAsyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for i := 0; i < 10; i++ {
			inputCh <- "hello"
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

func TestGeneric1In1OutAsyncProcessor_NormalFlow_CloseByStop(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutAsyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutAsyncProcessor[*mock1In1OutAsyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputCh <- "hello"
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

func TestGeneric1In1OutAsyncProcessor_CloseWhenNotStarted(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutAsyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutAsyncProcessor[*mock1In1OutAsyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	go func() {
		for i := 0; i < 10; i++ {
			inputCh <- "hello"
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

func TestGeneric1In1OutAsyncProcessor_StopWhenInitFailed(t *testing.T) {
	inputCh := make(chan string)

	initErr := true
	mockProcessor := NewMockGeneric1In1OutAsyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutAsyncProcessor[*mock1In1OutAsyncProcessorIO](mockProcessor)(inputCh)
	assert.Error(t, err)
	_ = outputCh

	startErr := controller.Start()
	assert.ErrorIs(t, startErr, processor.ErrUnableToStart)

	time.Sleep(200 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)
}

func TestGeneric1In1OutAsyncProcessor_MultipleStartAndStop(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutAsyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutAsyncProcessor[*mock1In1OutAsyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	startErr = controller.Start()
	assert.ErrorIs(t, startErr, processor.ErrMultipleStart)

	go func() {
		for i := 0; i < 10; i++ {
			inputCh <- "hello"
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

func TestGeneric1In1OutAsyncProcessor_StopWhenNoProducer(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutAsyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutAsyncProcessor[*mock1In1OutAsyncProcessorIO](mockProcessor)(inputCh)
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

func TestGeneric1In1OutAsyncProcessor_StopWhenNoConsumer(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutAsyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutAsyncProcessor[*mock1In1OutAsyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for i := 0; i < 10; i++ {
			inputCh <- "hello"
		}
		close(inputCh)
	}()

	_ = outputCh

	time.Sleep(200 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)
}

func TestGeneric1In1OutAsyncProcessor_PauseAndResume_ControlNotSupported(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutAsyncProcessorWithoutControl(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutAsyncProcessor[*mock1In1OutAsyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputCh <- "hello"
			time.Sleep(50 * time.Millisecond)
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

func TestGeneric1In1OutAsyncProcessor_PauseAndResume_ControlSupported(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutAsyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutAsyncProcessor[*mock1In1OutAsyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputCh <- "hello"
			time.Sleep(50 * time.Millisecond)
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

func TestGeneric1In1OutAsyncProcessor_DoublePause(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutAsyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutAsyncProcessor[*mock1In1OutAsyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputCh <- "hello"
			time.Sleep(50 * time.Millisecond)
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

func TestGeneric1In1OutAsyncProcessor_DoubleResume(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutAsyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutAsyncProcessor[*mock1In1OutAsyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputCh <- "hello"
			time.Sleep(50 * time.Millisecond)
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

func TestGeneric1In1OutAsyncProcessor_PauseAndResumeWhenNotStarted(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutAsyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutAsyncProcessor[*mock1In1OutAsyncProcessorIO](mockProcessor)(inputCh)
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
			inputCh <- "hello"
			time.Sleep(50 * time.Millisecond)
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

func TestGeneric1In1OutAsyncProcessor_PauseAndResumeAfterStopped(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutAsyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutAsyncProcessor[*mock1In1OutAsyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputCh <- "hello"
			time.Sleep(50 * time.Millisecond)
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

func TestGeneric1In1OutAsyncProcessor_StopWhenPaused(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutAsyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutAsyncProcessor[*mock1In1OutAsyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for {
			inputCh <- "hello"
			time.Sleep(50 * time.Millisecond)
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

func TestGeneric1In1OutAsyncProcessor_ControlAfterInitFailed(t *testing.T) {
	inputCh := make(chan string)

	initErr := true
	mockProcessor := NewMockGeneric1In1OutAsyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutAsyncProcessor[*mock1In1OutAsyncProcessorIO](mockProcessor)(inputCh)
	assert.Error(t, err)

	startErr := controller.Start()
	assert.Error(t, startErr)

	go func() {
		for {
			inputCh <- "hello"
			time.Sleep(50 * time.Millisecond)
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
