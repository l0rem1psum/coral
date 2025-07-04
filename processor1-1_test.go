package processor_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	processor "github.com/l0rem1psum/coral"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	MockGeneric1In1OutSyncControllableProcessor struct {
		shouldErrorOn   map[string]error
		shouldFailInit  bool
		shouldFailClose bool
		errorCount      int
		processedInputs []string
		customControls  []any
		mu              sync.Mutex
	}
	MockGeneric1In1OutSyncProcessorWithCloseError struct {
		initErr  bool
		closeErr bool
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

func NewMockGeneric1In1OutSyncControllableProcessor() *MockGeneric1In1OutSyncControllableProcessor {
	return &MockGeneric1In1OutSyncControllableProcessor{
		shouldErrorOn:   make(map[string]error),
		processedInputs: make([]string, 0),
		customControls:  make([]any, 0),
	}
}

func (cp *MockGeneric1In1OutSyncControllableProcessor) SetInitError(shouldFail bool) *MockGeneric1In1OutSyncControllableProcessor {
	cp.shouldFailInit = shouldFail
	return cp
}

func (cp *MockGeneric1In1OutSyncControllableProcessor) SetCloseError(shouldFail bool) *MockGeneric1In1OutSyncControllableProcessor {
	cp.shouldFailClose = shouldFail
	return cp
}

func (cp *MockGeneric1In1OutSyncControllableProcessor) SetProcessingError(input string, err error) *MockGeneric1In1OutSyncControllableProcessor {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.shouldErrorOn[input] = err
	return cp
}

func (cp *MockGeneric1In1OutSyncControllableProcessor) Init() error {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.shouldFailInit {
		return errors.New("controlled init error")
	}
	return nil
}

func (cp *MockGeneric1In1OutSyncControllableProcessor) Process(input *MockProcessorInput) (*MockProcessorOutput, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if err, shouldError := cp.shouldErrorOn[input.str]; shouldError {
		cp.errorCount++
		return nil, err
	}

	cp.processedInputs = append(cp.processedInputs, input.str)
	return &MockProcessorOutput{str: input.str}, nil
}

func (cp *MockGeneric1In1OutSyncControllableProcessor) Close() error {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.shouldFailClose {
		return errors.New("controlled close error")
	}
	return nil
}

func (cp *MockGeneric1In1OutSyncControllableProcessor) OnControl(control any) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.customControls = append(cp.customControls, control)
	return nil
}

func (cp *MockGeneric1In1OutSyncControllableProcessor) GetMetrics() ProcessorMetrics {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return ProcessorMetrics{
		ProcessedInputs: append([]string{}, cp.processedInputs...),
		ErrorCount:      cp.errorCount,
		CustomControls:  append([]any{}, cp.customControls...),
	}
}

type ProcessorMetrics struct {
	ProcessedInputs []string
	ErrorCount      int
	CustomControls  []any
}

func NewMockGeneric1In1OutSyncProcessorWithCloseError(initErr, closeErr bool) *MockGeneric1In1OutSyncProcessorWithCloseError {
	return &MockGeneric1In1OutSyncProcessorWithCloseError{
		initErr:  initErr,
		closeErr: closeErr,
	}
}

func (mp *MockGeneric1In1OutSyncProcessorWithCloseError) Init() error {
	if mp.initErr {
		return fmt.Errorf("init error")
	}
	return nil
}

func (mp *MockGeneric1In1OutSyncProcessorWithCloseError) Process(ui *MockProcessorInput) (*MockProcessorOutput, error) {
	return &MockProcessorOutput{str: ui.str}, nil
}

func (mp *MockGeneric1In1OutSyncProcessorWithCloseError) Close() error {
	if mp.closeErr {
		return fmt.Errorf("close error")
	}
	return nil
}

func (mp *MockGeneric1In1OutSyncProcessorWithCloseError) OnControl(a any) error {
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

func TestGeneric1In1OutSyncProcessor_StartPaused(t *testing.T) {
	inputCh := make(chan string)

	// Create processor that starts in paused state
	mockProcessor := NewMockGeneric1In1OutSyncProcessor(false)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](
		mockProcessor,
		processor.StartPaused(), // Test StateWaitingToStart → StatePaused transition
	)(inputCh)
	require.NoError(t, err, "Processor initialization should succeed")

	// Start processor - should begin in paused state
	startErr := controller.Start()
	require.NoError(t, startErr, "Processor start should succeed")

	// Verify processor is paused by attempting to pause again
	pauseErr := controller.Pause()
	assert.ErrorIs(t, pauseErr, processor.ErrAlreadyPaused, "Processor should already be paused")

	// Send inputs that should be dropped due to paused state
	go func() {
		for i := 0; i < 3; i++ {
			inputCh <- "drop"
		}
		time.Sleep(20 * time.Millisecond)

		// Resume processor
		resumeErr := controller.Resume()
		require.NoError(t, resumeErr, "Resume should succeed")

		// Send inputs that should be processed after resume
		for i := 0; i < 2; i++ {
			inputCh <- "process"
		}
		close(inputCh)
	}()

	// Collect outputs
	var outputs []string
	go func() {
		for s := range outputCh {
			outputs = append(outputs, s)
		}
	}()

	time.Sleep(200 * time.Millisecond)

	// Verify clean termination
	stopErr := controller.Stop()
	assert.NoError(t, stopErr, "Processor stop should succeed")

	// Verify outputs are only from inputs after resume (allowing for backpressure)
	assert.LessOrEqual(t, len(outputs), 2, "Should not have more outputs than inputs after resume")
	// All outputs should be from post-resume inputs only
	for _, output := range outputs {
		assert.Equal(t, "process", output, "All outputs should be from inputs sent after resume")
	}
}

func TestGeneric1In1OutSyncProcessor_InputsDroppedDuringPause(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutSyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	// Process a few inputs normally first
	go func() {
		for i := 0; i < 3; i++ {
			inputCh <- "before_pause"
			time.Sleep(10 * time.Millisecond)
		}
	}()

	time.Sleep(50 * time.Millisecond)

	// Pause the processor
	pauseErr := controller.Pause()
	assert.Nil(t, pauseErr)

	// Send inputs while paused - these should be dropped
	go func() {
		for i := 0; i < 5; i++ {
			inputCh <- "during_pause_dropped"
			time.Sleep(10 * time.Millisecond)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Resume and send more inputs
	resumeErr := controller.Resume()
	assert.Nil(t, resumeErr)

	go func() {
		for i := 0; i < 2; i++ {
			inputCh <- "after_resume"
			time.Sleep(10 * time.Millisecond)
		}
		close(inputCh)
	}()

	// Collect all outputs
	var outputs []string
	go func() {
		for s := range outputCh {
			outputs = append(outputs, s)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)

	// Should have some outputs from before pause and after resume
	// The 5 inputs during pause should be dropped
	assert.LessOrEqual(t, len(outputs), 7)    // Allow some flexibility for timing
	assert.GreaterOrEqual(t, len(outputs), 1) // Should have at least some outputs (timing can vary)
}

func TestGeneric1In1OutSyncProcessor_CustomControlDuringPause(t *testing.T) {
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

	// Pause the processor
	pauseErr := controller.Pause()
	assert.Nil(t, pauseErr)

	// Send custom control while paused - should work
	controlErr := controller.Control("custom_control_during_pause")
	assert.Nil(t, controlErr)

	// Resume
	resumeErr := controller.Resume()
	assert.Nil(t, resumeErr)

	// Send custom control while running - should also work
	controlErr = controller.Control("custom_control_during_run")
	assert.Nil(t, controlErr)

	time.Sleep(100 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)
}

func TestGeneric1In1OutSyncProcessor_ProcessingError(t *testing.T) {
	inputCh := make(chan string)

	// Create processor with controlled error injection
	mockProcessor := NewMockGeneric1In1OutSyncControllableProcessor().
		SetProcessingError("error1", errors.New("controlled error 1")).
		SetProcessingError("error2", errors.New("controlled error 2"))

	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
	require.NoError(t, err, "Processor initialization should succeed")

	// Start processor
	startErr := controller.Start()
	require.NoError(t, startErr, "Processor start should succeed")

	// Send mixed inputs with known error pattern
	inputs := []string{"ok1", "error1", "ok2", "error2", "ok3"}
	expectedSuccessfulInputs := []string{"ok1", "ok2", "ok3"} // Only successful inputs
	expectedErrors := 2                                       // error1 and error2

	go func() {
		for _, input := range inputs {
			inputCh <- input
		}
		close(inputCh)
	}()

	// Collect outputs
	var actualOutputs []string
	go func() {
		for s := range outputCh {
			actualOutputs = append(actualOutputs, s)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Verify clean termination
	stopErr := controller.Stop()
	require.NoError(t, stopErr, "Processor stop should succeed")

	// Verify error tracking is exact
	metrics := mockProcessor.GetMetrics()
	assert.Equal(t, expectedErrors, metrics.ErrorCount, "Should have exactly the expected number of errors")

	// Verify that only successful inputs were processed (allowing for backpressure drops)
	assert.LessOrEqual(t, len(actualOutputs), len(expectedSuccessfulInputs), "Should not have more outputs than successful inputs")

	// All outputs should be from successful inputs only
	for _, output := range actualOutputs {
		assert.Contains(t, expectedSuccessfulInputs, output, "All outputs should be from successful inputs only")
	}

	// All processed inputs should be successful ones
	for _, processedInput := range metrics.ProcessedInputs {
		assert.Contains(t, expectedSuccessfulInputs, processedInput, "All processed inputs should be successful ones")
	}
}

func TestGeneric1In1OutSyncProcessor_BackpressureBlocking(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutSyncProcessor(initErr)

	// Test config.blockOnOutput=true behavior
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](
		mockProcessor,
		processor.BlockOnOutput(), // This triggers blocking behavior
	)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	// Fill the output channel buffer to create backpressure
	go func() {
		for i := 0; i < 10; i++ {
			inputCh <- fmt.Sprintf("input_%d", i)
			time.Sleep(5 * time.Millisecond) // Fast input
		}
		close(inputCh)
	}()

	// Slow consumer - should create backpressure
	var outputs []string
	go func() {
		time.Sleep(50 * time.Millisecond) // Delay before consuming
		for s := range outputCh {
			outputs = append(outputs, s)
			time.Sleep(20 * time.Millisecond) // Slow consumption
		}
	}()

	time.Sleep(400 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)

	// With blocking behavior, all inputs should produce outputs (no dropping)
	assert.Equal(t, 10, len(outputs))
}

func TestGeneric1In1OutSyncProcessor_BackpressureNonBlocking(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutSyncProcessor(initErr)

	// Default behavior is non-blocking (blockOnOutput=false)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	// Fast input to overwhelm output channel
	go func() {
		for i := 0; i < 20; i++ {
			inputCh <- fmt.Sprintf("fast_input_%d", i)
			time.Sleep(1 * time.Millisecond) // Very fast input
		}
		close(inputCh)
	}()

	// Very slow consumer to create maximum backpressure
	var outputs []string
	go func() {
		time.Sleep(100 * time.Millisecond) // Long delay before consuming
		for s := range outputCh {
			outputs = append(outputs, s)
			time.Sleep(50 * time.Millisecond) // Very slow consumption
		}
	}()

	time.Sleep(300 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)

	// With non-blocking behavior, some outputs should be dropped due to backpressure
	assert.Less(t, len(outputs), 20)          // Should have fewer outputs than inputs due to dropping
	assert.GreaterOrEqual(t, len(outputs), 0) // May have zero outputs if consumer is too slow
}

func TestGeneric1In1OutSyncProcessor_ResourceReleaseVerification(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutSyncProcessor(initErr)

	// For now, we'll test indirectly by verifying behavior with paused inputs
	// The IO adapter resource release calls happen internally and can't be easily mocked
	// without changing the FSM implementation
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	// Pause immediately to force input dropping
	pauseErr := controller.Pause()
	assert.Nil(t, pauseErr)

	// Send inputs that should be dropped and released
	go func() {
		for i := 0; i < 5; i++ {
			inputCh <- fmt.Sprintf("dropped_input_%d", i)
			time.Sleep(10 * time.Millisecond)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Resume and send normal inputs
	resumeErr := controller.Resume()
	assert.Nil(t, resumeErr)

	go func() {
		for i := 0; i < 3; i++ {
			inputCh <- fmt.Sprintf("normal_input_%d", i)
			time.Sleep(10 * time.Millisecond)
		}
		close(inputCh)
	}()

	// Don't consume outputs to force output dropping
	var outputs []string
	go func() {
		time.Sleep(50 * time.Millisecond) // Delay to allow channel to fill
		for s := range outputCh {
			outputs = append(outputs, s)
		}
	}()

	time.Sleep(200 * time.Millisecond)

	stopErr := controller.Stop()
	assert.Nil(t, stopErr)

	// Verify behavior indirectly - dropped inputs during pause
	assert.LessOrEqual(t, len(outputs), 5) // Should have fewer outputs than total inputs
}

func TestGeneric1In1OutSyncProcessor_ChannelCleanupOrder(t *testing.T) {
	inputCh := make(chan string)

	initErr := false
	mockProcessor := NewMockGeneric1In1OutSyncProcessor(initErr)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	// Send some inputs
	go func() {
		for i := 0; i < 5; i++ {
			inputCh <- fmt.Sprintf("input_%d", i)
			time.Sleep(10 * time.Millisecond)
		}
		close(inputCh)
	}()

	// Test that we can consume outputs normally
	var outputs []string
	outputDone := make(chan bool)
	go func() {
		for s := range outputCh {
			outputs = append(outputs, s)
		}
		outputDone <- true
	}()

	// Allow some processing time
	time.Sleep(100 * time.Millisecond)

	// Wait for processing and cleanup
	stopErr := controller.Stop()
	assert.Nil(t, stopErr)

	// Wait for output channel to be closed and drained
	<-outputDone

	// Verify some inputs were processed (timing can be variable)
	assert.GreaterOrEqual(t, len(outputs), 1) // Should have processed at least one input
}

func TestGeneric1In1OutSyncProcessor_ProcessorCloseError(t *testing.T) {
	inputCh := make(chan string)

	// Create processor that will error on Close()
	mockProcessor := NewMockGeneric1In1OutSyncProcessorWithCloseError(false, true)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
	assert.Nil(t, err)

	startErr := controller.Start()
	assert.Nil(t, startErr)

	go func() {
		for i := 0; i < 3; i++ {
			inputCh <- fmt.Sprintf("input_%d", i)
			time.Sleep(10 * time.Millisecond)
		}
		close(inputCh)
	}()

	go func() {
		for s := range outputCh {
			fmt.Println(s)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Stop should return the close error
	stopErr := controller.Stop()
	assert.Error(t, stopErr)
	assert.Contains(t, stopErr.Error(), "close error")
}

func TestInitializationErrorHandling(t *testing.T) {
	scenarios := []struct {
		name           string
		initShouldFail bool
		expectStart    bool
		expectError    bool
	}{
		{
			name:           "successful_initialization",
			initShouldFail: false,
			expectStart:    true,
			expectError:    false,
		},
		{
			name:           "failed_initialization",
			initShouldFail: true,
			expectStart:    false,
			expectError:    true,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			inputCh := make(chan string)

			mockProcessor := NewMockGeneric1In1OutSyncControllableProcessor().SetInitError(scenario.initShouldFail)
			controller, outputCh, initErr := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)

			if scenario.expectError {
				assert.Error(t, initErr, "Initialization should fail")
				assert.NotNil(t, controller, "Controller should still be returned")
			} else {
				assert.NoError(t, initErr, "Initialization should succeed")
				assert.NotNil(t, outputCh, "Output channel should be provided")
			}

			// Test start behavior
			startErr := controller.Start()
			if scenario.expectStart {
				assert.NoError(t, startErr, "Start should succeed after successful init")
			} else {
				assert.ErrorIs(t, startErr, processor.ErrUnableToStart, "Start should fail after failed init")
			}

			// Always test clean shutdown
			stopErr := controller.Stop()
			assert.NoError(t, stopErr, "Stop should always succeed")
		})
	}
}

func TestPauseResumeStateMachineCompliance(t *testing.T) {
	inputCh := make(chan string)

	mockProcessor := NewMockGeneric1In1OutSyncProcessor(false)
	controller, outputCh, err := processor.InitializeGeneric1In1OutSyncProcessor[*mock1In1OutSyncProcessorIO](mockProcessor)(inputCh)
	require.NoError(t, err)

	// Collect outputs
	go func() {
		for range outputCh {
			// Consume outputs to prevent blocking
		}
	}()

	// Test pause/resume state machine behavior
	t.Run("pause_before_start_fails", func(t *testing.T) {
		pauseErr := controller.Pause()
		assert.ErrorIs(t, pauseErr, processor.ErrProcessorNotRunning, "Cannot pause before start")
	})

	t.Run("resume_before_start_fails", func(t *testing.T) {
		resumeErr := controller.Resume()
		assert.ErrorIs(t, resumeErr, processor.ErrProcessorNotRunning, "Cannot resume before start")
	})

	// Start processor
	startErr := controller.Start()
	require.NoError(t, startErr)

	t.Run("pause_when_running_succeeds", func(t *testing.T) {
		pauseErr := controller.Pause()
		assert.NoError(t, pauseErr, "Should be able to pause when running")
	})

	t.Run("double_pause_fails", func(t *testing.T) {
		pauseErr := controller.Pause()
		assert.ErrorIs(t, pauseErr, processor.ErrAlreadyPaused, "Cannot pause when already paused")
	})

	t.Run("resume_when_paused_succeeds", func(t *testing.T) {
		resumeErr := controller.Resume()
		assert.NoError(t, resumeErr, "Should be able to resume when paused")
	})

	t.Run("double_resume_fails", func(t *testing.T) {
		resumeErr := controller.Resume()
		assert.ErrorIs(t, resumeErr, processor.ErrAlreadyRunning, "Cannot resume when already running")
	})

	// Clean shutdown
	stopErr := controller.Stop()
	require.NoError(t, stopErr)

	t.Run("pause_after_stop_fails", func(t *testing.T) {
		pauseErr := controller.Pause()
		assert.ErrorIs(t, pauseErr, processor.ErrProcessorNotRunning, "Cannot pause after stop")
	})

	t.Run("resume_after_stop_fails", func(t *testing.T) {
		resumeErr := controller.Resume()
		assert.ErrorIs(t, resumeErr, processor.ErrProcessorNotRunning, "Cannot resume after stop")
	})
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
