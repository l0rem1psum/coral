package processor

// Lifecycle Messages
const (
	// Processor lifecycle messages
	logProcessorStarted          = "Processor started"
	logProcessorStartedPaused    = "Processor started in paused state"
	logProcessorStopped          = "Processor stopped"
	logProcessorClosingAfterInit = "Closing processor after initialization and before start"

	// Multiprocessor lifecycle messages
	logMultiprocessorStarted          = "Multiprocessor started"
	logMultiprocessorStartedPaused    = "Multiprocessor started in paused state"
	logMultiprocessorStopped          = "Multiprocessor stopped"
	logMultiprocessorClosingAfterInit = "Closing multiprocessor after initialization and before start"
)

// Control Messages
const (
	// Processor control messages
	logProcessorPaused  = "Processor paused"
	logProcessorResumed = "Processor resumed"

	// Multiprocessor control messages
	logMultiprocessorPaused  = "Multiprocessor paused"
	logMultiprocessorResumed = "Multiprocessor resumed"
)

// Channel Messages
const (
	// Input/output channel messages
	logInputChannelClosed  = "Input channel closed, stopping"
	logOutputChannelClosed = "Processor output channel closed, stopping"
	logCloseSignalReceived = "Close signal received, stopping"
)

// Error Messages
const (
	// Processing error messages
	logProcessingError = "Error encountered during processing, continuing"
)

// Warning Messages
const (
	// Output channel management warnings
	logOutputChannelFullDropOldest  = "Output channel full, dropping the frontmost/oldest output"
	logOutputChannelFullDropCurrent = "Output channel full, dropping current output"

	// Multiprocessor-specific warnings
	logOutputChannelFullDropOldestBatch  = "Output channel full, dropping oldest batch"
	logOutputChannelFullDropCurrentBatch = "Output channel full, dropping current batch"

	// Skip result misuse warning
	logSkipResultMisuseWarning = "SkipResult received, possible misues"
)

// Debug Messages
const (
	// State transition debug messages
	logStateTransition      = "State transition"
	logMultiStateTransition = "MultiProcessor state transition"
)

// Multiprocessor-Specific Messages
const (
	// Multiprocessor error/warning messages
	logSubProcessorStartFailed = "Failed to start sub-processor"
	logInputBatchEmpty         = "Input batch is empty, dropping"
	logInputLengthMismatch     = "Input length mismatch, dropping"
)
