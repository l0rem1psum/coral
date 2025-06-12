# Coral

**Type-safe, concurrent data processing pipelines for Go**

Coral is a high-performance Go library for building concurrent data processing pipelines with compile-time type safety. It provides a composable framework for creating complex data flows using a simple, functional programming approach while handling all the complexity of goroutine management, backpressure, and resource cleanup.

[![Go Reference](https://pkg.go.dev/badge/github.com/l0rem1psum/coral.svg)](https://pkg.go.dev/github.com/l0rem1psum/coral)
[![Go Report Card](https://goreportcard.com/badge/github.com/l0rem1psum/coral)](https://goreportcard.com/report/github.com/l0rem1psum/coral)

## Key Features

- **🔒 Compile-Time Type Safety**: Generic type system ensures incompatible processors cannot be connected, eliminating entire classes of runtime errors common in data processing systems

- **🧵 Goroutine-Per-Processor**: Each processor runs in its own dedicated goroutine, enabling safe integration with C libraries, thread-local storage, and blocking operations without affecting other processors

- **📈 Horizontal Scaling**: Multi-processor support for distributing work across multiple processor instances within pipeline stages

- **🎛️ Runtime Controllability**: Pause, resume, or send custom control messages to running processors without stopping the entire pipeline

- **🔧 Composable Architecture**: Build complex data flows from simple, reusable processor components. Each processor focuses on a single responsibility and can be tested in isolation

- **⚡ Zero-Configuration Concurrency**: Write sequential processing logic while Coral handles goroutines, channels, synchronization, and backpressure automatically

- **📊 Production-Grade Reliability**: Built-in structured logging, graceful shutdown, resource cleanup, and error isolation ensure pipelines run reliably in production environments

## Requirements

- Go 1.21 or later

## Installation

```bash
go get github.com/l0rem1psum/coral
```

## Examples

See the complete working examples in the [`example/`](./example/) directory:

- **[Pipeline Example](./example/simple/)**: A 3-stage pipeline using the recommended Pipeline approach with automatic lifecycle management and channel wiring
- **[Controller Example](./example/controllers/)**: The same workflow implemented using Controllers directly for manual control and custom orchestration  
- **[MultiProcessor Example](./example/multiprocessor/)**: Horizontal scaling with 3 parallel compute processors for enhanced throughput

## Key Concepts

### 1. Processor

A **Processor** is a concurrent data processing unit that transforms data streams with a managed lifecycle. Each processor runs in its own dedicated goroutine for complete isolation.

```
       ┌─────────────────┐
Input ─│   Processor A   │─ Output
       └─────────────────┘
```

**Core Responsibilities:**
- **Data Transformation**: Process input data according to business logic and produce output
- **Lifecycle Management**: Handle initialization, processing loop, and cleanup phases
- **Resource Isolation**: Run in dedicated goroutine to prevent interference with other processors
- **Error Handling**: Manage processing errors and maintain system stability

### 2. MultiProcessor

A **MultiProcessor** enables horizontal scaling by running multiple processor instances in parallel. Input slices are distributed 1:1 across instances for concurrent processing.

```
Single Processor:
       ┌─────────────────┐
Input ─│   Processor 1   │─ Output
       └─────────────────┘

MultiProcessor:
        ┌─────────────────┐
Input ──│   Processor 1   │── Output
Slice   ├─────────────────┤   Slice
[A,B,C] │   Processor 2   │   [A',B',C']
        ├─────────────────┤
        │   Processor 3   │
        └─────────────────┘
```

**Core Responsibilities:**
- **Parallel Execution**: Coordinate multiple processor instances for increased throughput
- **Load Distribution**: Distribute input slices evenly across processor instances
- **Output Aggregation**: Collect and structure outputs from all processor instances
- **Unified Control**: Present single Controller interface for managing all instances

### 3. Controller

A **Controller** provides thread-safe lifecycle management for processor goroutines. Each processor has exactly one Controller that handles start/stop operations and custom control messages.

```
┌─────────────┐                    ┌─────────────┐
│             │───── Commands ────▶│             │
│ Controller  │                    │ Processor   │
│             │◀──── Responses ────│ Goroutine   │
└─────────────┘                    └─────────────┘
```

**Core Responsibilities:**
- **Lifecycle Control**: Manage processor startup, shutdown, pause, and resume operations
- **Thread-Safe Communication**: Provide safe interface for controlling processor goroutines
- **State Management**: Track processor state and prevent invalid operations
- **Custom Control**: Enable application-specific runtime control and configuration changes

### 4. IO Adapter

An **IO Adapter** handles type conversions and resource management for a specific processor, enabling loose coupling and reusability. Each processor is wrapped by its own IO adapter that converts between channel types and the processor's business logic types.

```
         ┌──────────────────────────────────┐
         │         IO Adapter               │
         │  ┌─────────────────────────┐     │
Channel  │  │     Processor A         │     │ Channel
Type I ──│─▶│ (Raw Types: In→Out)     │─▶───│── Type O
         │  └─────────────────────────┘     │
         │ AsInput(I)→In  Out→FromOutput(O) │
         └──────────────────────────────────┘
```

**Core Responsibilities:**
- **Type Conversion**: Transform channel types to/from processor's raw business logic types
- **Decoupling**: Separate processor business logic from pipeline-specific data formats  
- **Resource Management**: Handle acquisition and cleanup of resources via `ReleaseInput`/`ReleaseOutput`
- **Reusability**: Enable processors to work with different data types across various pipelines

### 5. Pipeline

A **Pipeline** orchestrates complex data processing workflows by connecting multiple processors in a Directed Acyclic Graph (DAG). It handles automatic channel wiring, topological execution order, and type safety.

Simple Pipeline:
```
┌─────────────┐
│ Processor A │
└─────┬───────┘
      │
      ▼
┌─────────────┐
│ Processor B │
└─────┬───────┘
      │
      ▼
┌─────────────┐
│ Processor C │
└─────────────┘
```

Complex Pipeline:
```
                ┌─────────────┐
          ┌────▶│ Processor B │────┐
          │     └─────────────┘    │
          │                        ▼
┌─────────────┐              ┌─────────────┐
│ Processor A │              │ Processor E │
└─────┬───────┘              └─────┬───────┘
      │                            │
      │     ┌─────────────┐        │
      └────▶│ Processor C │────────┘
            └─────┬───────┘
                  │
                  ▼
            ┌─────────────┐
            │ Processor D │
            └─────────────┘
```

**Core Responsibilities:**
- **DAG Orchestration**: Define and validate processor connections in directed acyclic graphs
- **Automatic Wiring**: Connect processor outputs to downstream inputs via typed channels
- **Execution Ordering**: Start and stop processors in topologically correct dependency order
- **Type Safety**: Ensure compile-time compatibility between connected processor interfaces
- **Centralized Management**: Coordinate initialization, startup, and shutdown across all processors

## Processor Types

Coral provides different processor patterns based on input/output cardinality (M→N pattern):

| Type | Pattern | Interface | Sync/Async | Description | Use Cases |
|------|---------|-----------|------------|-------------|-----------|
| **Generator** | 0→1 | [`Generic0In1OutSyncProcessor`](./processor0-1.go) | Sync | Creates data from external sources | File readers, API clients, sensor data |
| **Generator** | 0→1 | [`Generic0In1OutAsyncProcessor`](./processor0-1.go) | Async | Creates data with decoupled output | Event streams, async data sources |
| **Transformer** | 1→1 | [`Generic1In1OutSyncProcessor`](./processor1-1.go) | Sync | Transforms data with 1:1 mapping | Data validation, format conversion |
| **Transformer** | 1→1 | [`Generic1In1OutAsyncProcessor`](./processor1-1.go) | Async | Transforms with decoupled input/output | Complex processing pipelines |
| **Sink** | 1→0 | [`Generic1In0OutSyncProcessor`](./processor1-0.go) | Sync | Consumes data and performs side effects | Database writers, file outputs |
| **Broadcaster** | 1→N | [`Generic1InNOutSyncProcessor`](./processor1-N.go) | Sync | Distributes single input to multiple outputs | Data routing, replication |
| **2-Input Aggregator** | 2→1 | [`Generic2In1OutAsyncProcessor`](./processor2-1.go) | Async | Combines two input streams | Data joining, correlation |
| **N-Input Aggregator** | N→1 | [`GenericNIn1OutAsyncProcessor`](./processorN-1.go) | Async | Combines multiple input streams | Stream merging, fan-in operations |
| **N-Input Sink** | N→0 | [`GenericNIn0OutAsyncProcessor`](./processorN-0.go) | Async | Consumes from multiple input streams | Multi-stream logging, aggregated outputs |
| **Multi-I/O** | M→N | [`GenericMInNOutSyncProcessor`](./processorM-N.go) | Sync | Complex processing with multiple inputs/outputs | Complex transformations, routing |

### MultiProcessor Variants

| Type | Pattern | Function | Description | Use Cases |
|------|---------|-----------|-------------|-----------|
| **Multi-Transformer** | 1→1 | [`InitializeGeneric1In1OutSyncMultiProcessor`](./multiprocessor1-1.go) | Parallel 1:1 processing across multiple instances | High-throughput data transformation |
| **Multi-Sink** | 1→0 | [`InitializeGeneric1In0OutSyncMultiProcessor`](./multiprocessor1-0.go) | Parallel sink processing across multiple instances | High-throughput data consumption |
| **Multi-Broadcaster** | 1→N | [`InitializeGeneric1InNOutSyncMultiProcessor`](./multiprocessor1-N.go) | Parallel broadcasting across multiple instances | High-throughput data distribution |

### Processing Modes

Coral supports both **synchronous** and **asynchronous** processing modes:

- **Synchronous**: `Process(input)` blocks until output is ready - simpler for 1:1 transformations
- **Asynchronous**: Input processing and output generation are decoupled via self-managed channels - better for complex timing requirements

```
Synchronous Flow:
Input → Process() → Output (blocking)

Asynchronous Flow:
Input → Process() → Internal Queue
              ↓
         Output() ← Channel (non-blocking)
```

## Pipeline vs Controller Usage Patterns

Coral offers two approaches for building data processing workflows: **Pipelines** (recommended) and **Controllers** (manual).

### Pipeline Approach (Recommended)

Pipelines provide automatic lifecycle management and channel wiring for complex data flows:

```go
// Declarative topology definition
vertices := []pipeline.ProcessorVertex{
    {Label: "generator", Outputs: []pipeline.EdgeLabels{{"random_numbers"}}},
    {Label: "computer", Inputs: []pipeline.EdgeLabels{{"random_numbers"}}, 
     Outputs: []pipeline.EdgeLabels{{"computed_results"}}},
    {Label: "printer", Inputs: []pipeline.EdgeLabels{{"computed_results"}}},
}

// Automatic management
ppl, _ := pipeline.NewPipeline(vertices, logger)
pipeline.AddGeneric0In1OutSyncProcessor[*GeneratorToComputerIO](ppl, generator, "generator")
pipeline.AddGeneric1In1OutSyncProcessor[*ComputerToPrinterIO](ppl, computer, "computer")  
pipeline.AddGeneric1In0OutSyncProcessor[*PrintProcessorIO](ppl, printer, "printer")

ppl.Initialize()
ppl.Start()
defer ppl.Stop()
```

### Controller Approach (Manual)

Controllers provide fine-grained control for custom orchestration scenarios:

```go
// Manual initialization and channel wiring
genController, genOutputCh, _ := processor.InitializeGeneric0In1OutSyncProcessor[*GeneratorToComputerIO](generator)()
compController, compOutputCh, _ := processor.InitializeGeneric1In1OutSyncProcessor[*ComputerToPrinterIO](computer)(genOutputCh)
printController, _ := processor.InitializeGeneric1In0OutSyncProcessor[*PrintProcessorIO](printer)(compOutputCh)

// Manual lifecycle management (order matters)
genController.Start()
compController.Start() 
printController.Start()
```

### When to Use Each Approach

| Use Pipeline When | Use Controllers When |
|-------------------|---------------------|
| Building standard DAG workflows | Custom orchestration patterns |
| Want automatic lifecycle management | Need fine-grained control |
| Prefer declarative configuration | Require dynamic processor management |
| Need centralized error handling | Have non-standard startup sequences |

**See working examples:** [`example/simple/`](./example/simple/) (Pipeline) vs [`example/controllers/`](./example/controllers/) (Controllers)

## Advanced Usage

### Custom Control Messages

Implement dynamic runtime configuration by sending custom control messages to processors.

#### Processor Implementation

```go
type ConfigurableProcessor struct {
    rateLimit int
    logger    *slog.Logger
}

// Implement Controllable interface
func (p *ConfigurableProcessor) OnControl(msg any) error {
    switch ctrl := msg.(type) {
    case *RateLimitUpdate:
        p.rateLimit = ctrl.NewLimit
        p.logger.Info("Rate limit updated", "new_limit", ctrl.NewLimit)
        return nil
    case *LogLevelUpdate:
        // Update log level dynamically
        return p.updateLogLevel(ctrl.Level)
    default:
        return processor.ErrControlNotSupported
    }
}
```

#### Sending Control Messages

```go
// Define control message types
type RateLimitUpdate struct {
    NewLimit int
}

// Send control messages at runtime
controller.Control(&RateLimitUpdate{NewLimit: 1000})
controller.Pause()  // Built-in control
controller.Resume() // Built-in control
```

### Multi-Processor Scaling

Multi-processors enable horizontal scaling by running multiple processor instances in parallel for enhanced throughput.

#### Basic Multi-Processor Setup

```go
// Create multiple processor instances
computeProcessors := []*ComputeNumberProcessor{
    NewComputeNumberProcessor(logger, 1),
    NewComputeNumberProcessor(logger, 2), 
    NewComputeNumberProcessor(logger, 3),
}

// Add as multi-processor - same interface as single processor
err = pipeline.AddGeneric1In1OutSyncMultiProcessor[*MultiProcessorComputeIO](
    ppl,
    computeProcessors,
    "multi_computer",
)
```

#### Advanced Control Features

Multi-processors support sophisticated control operations:

```go
// Broadcast control to all instances
controller.Control(configUpdate)

// Target specific processor instance
controller.Control(&processor.MultiProcessorRequest{
    I:   1,                    // Target processor instance 1
    Req: customControlMessage, // Send specific control message
})
```

**See working example:** [`example/multiprocessor/`](./example/multiprocessor/)

### Backpressure Handling

Configure how processors handle output channel congestion to prevent memory issues and maintain system stability.

#### Configuration Options

```go
// Block until output channel has space (default)
processor.InitializeGeneric1In1OutSyncProcessor[*MyIO](
    processor,
    processor.BlockOnOutput(), // Will block processing if output is full
)

// Drop messages when output channel is full
processor.InitializeGeneric1In1OutSyncProcessor[*MyIO](
    processor,
    // No BlockOnOutput() - will drop oldest or current messages
)
```

#### Backpressure Strategies

- **Blocking**: Processor waits until downstream consumer reads from output channel
- **Dropping**: When output buffer is full, either oldest or current message is dropped
- **Logging**: Dropped messages are logged for monitoring and debugging

### Resource Management

IO Adapters provide hooks for proper resource lifecycle management, preventing memory leaks and ensuring cleanup.

#### Resource Cleanup Implementation

```go
type FileProcessorIO struct {
    filePool *sync.Pool
}

func (io *FileProcessorIO) AsInput(filePath string) *os.File {
    file, _ := os.Open(filePath)
    return file
}

func (io *FileProcessorIO) FromOutput(file *os.File, result *ProcessedData) *OutputData {
    return &OutputData{
        Content: result.Content,
        Source:  file.Name(),
    }
}

// Critical: Release resources to prevent leaks
func (io *FileProcessorIO) ReleaseInput(file *os.File) {
    if file != nil {
        file.Close()
    }
}

func (io *FileProcessorIO) ReleaseOutput(output *OutputData) {
    // Return to pool, close connections, etc.
    io.filePool.Put(output.buffer)
}
```

### Integration with C Libraries

Coral's goroutine-per-processor architecture enables safe integration with C libraries that require thread-local storage.

#### Thread-Local Storage Pattern

```go
import "C"
import "runtime"

type CLibraryProcessor struct {
    initialized bool
}

func (p *CLibraryProcessor) Init() error {
    // Lock this goroutine to current OS thread
    runtime.LockOSThread()
    
    // Initialize C library - will use thread-local storage
    C.initialize_library()
    p.initialized = true
    return nil
}

func (p *CLibraryProcessor) Process(input *Data) (*Result, error) {
    // Guaranteed to run on same OS thread as Init()
    // C library can safely access thread-local state
    result := C.process_data((*C.char)(input.buffer))
    return &Result{Data: C.GoString(result)}, nil
}

func (p *CLibraryProcessor) Close() error {
    // Cleanup on same OS thread
    C.cleanup_library()
    runtime.UnlockOSThread()
    return nil
}
```

#### Benefits for C Integration

- **Thread Consistency**: Initialization, processing, and cleanup on same OS thread
- **Thread-Local Storage**: Safe access to C library thread-local variables  
- **Blocking Operations**: C library blocking calls don't affect other processors
- **Resource Isolation**: Each processor has independent C library state

## Contributing

Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
