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

## Quick Start

See the complete working examples in the [`example/`](./example/) directory:

- **[Simple Pipeline](./example/simple/)**: A basic 3-stage pipeline demonstrating core concepts including generators, transformers, sinks, and IO adapters

To run the simple example:
```bash
cd example/simple
go run .
```


## Key Concepts

### Processors

Processors are the fundamental building blocks of Coral pipelines. Each processor implements a simple interface for initialization, data processing, and cleanup:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Generator     │───▶│   Transformer   │───▶│      Sink       │
│  (0→1 output)   │    │   (1→1 data)    │    │   (1→0 input)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### IO Adapters

IO Adapters handle type conversions between processors, enabling loose coupling and processor reusability:

```
Processor A                    IO Adapter                    Processor B
┌─────────────┐              ┌─────────────┐              ┌─────────────┐
│             │─── TypeX ───▶│  TypeX →    │─── TypeY ───▶│             │
│   Output    │              │  TypeY      │              │    Input    │
│             │              │ Conversion  │              │             │
└─────────────┘              └─────────────┘              └─────────────┘
```

### Pipeline Topology

Pipelines are defined as Directed Acyclic Graphs (DAGs) where vertices represent processors and edges represent data channels:

```
       ┌─────────────┐
       │  Generator  │
       └─────┬───────┘
             │ numbers
             ▼
       ┌─────────────┐
       │ Transformer │
       └─────┬───────┘
             │ results
             ▼
       ┌─────────────┐
       │    Sink     │
       └─────────────┘
```

Complex topologies support branching, merging, and parallel processing:

```
                    ┌──────────────┐
              ┌────▶│ Processor B  │────┐
              │     └──────────────┘    │
              │                         ▼
┌─────────────┐                   ┌──────────────┐
│ Processor A │                   │ Processor D  │
└─────────────┘                   └──────────────┘
              │                         ▲
              │     ┌──────────────┐    │
              └────▶│ Processor C  │────┘
                    └──────────────┘
```

### Controllers vs Pipelines

While Controllers can be used directly for fine-grained processor lifecycle management, **Pipelines are the recommended approach** for building data processing workflows.

#### Direct Controller Usage

Each processor returns a Controller that provides thread-safe lifecycle management:

```go
controller, outputCh, err := InitializeProcessor(processor)(inputCh)

// Lifecycle control
controller.Start()     // Begin processing
controller.Pause()     // Temporarily halt processing
controller.Resume()    // Resume processing
controller.Stop()      // Graceful shutdown
controller.Control(msg) // Send custom control message
```

#### Why Use Pipelines Instead

**Pipelines provide significant advantages over using Controllers directly:**

1. **Automatic Lifecycle Management**: Pipelines handle the start/stop sequence of all processors in topological order, ensuring proper initialization and shutdown without manual coordination.

2. **Reduced Boilerplate**: No need to manually wire channels between processors or handle complex initialization sequences - the pipeline handles all channel creation and connection.

3. **Error Handling**: Centralized error handling during pipeline initialization and startup, with automatic cleanup on failure.

4. **Type Safety**: Compile-time verification that processor input/output types match across the entire pipeline topology.

5. **Simplified Code**: Compare direct controller usage vs pipeline:

```go
// Direct Controllers (manual, error-prone)
gen := NewGenerator()
proc := NewProcessor()
sink := NewSink()

genController, genOut, _ := InitializeGenerator(gen)()
procController, procOut, _ := InitializeProcessor(proc)(genOut)
sinkController, _, _ := InitializeSink(sink)(procOut)

// Manual start sequence
genController.Start()
procController.Start()
sinkController.Start()

// Manual stop sequence (reverse order)
sinkController.Stop()
procController.Stop()
genController.Stop()

// vs Pipeline (automatic, safe)
ppl.AddGenerator(gen, "gen")
ppl.AddProcessor(proc, "proc") 
ppl.AddSink(sink, "sink")
ppl.Initialize()
ppl.Start()
defer ppl.Stop() // Handles proper shutdown order
```

**Use Controllers directly only when you need:**
- Custom processor orchestration outside of DAG topology
- Fine-grained control over individual processor lifecycle
- Dynamic processor management at runtime

### Goroutine-Per-Processor Architecture

**One of Coral's key design advantages is running each processor in its own dedicated goroutine.** This architecture provides several critical benefits:

#### C Library Integration & Thread-Local Storage

Many external libraries, especially C libraries via CGO, rely on thread-local storage and require that initialization, processing, and cleanup all happen on the same OS thread. Coral's goroutine-per-processor design makes this seamless:

```go
type CLibraryProcessor struct {
    initialized bool
}

func (p *CLibraryProcessor) Init() error {
    // Lock this goroutine to the current OS thread
    runtime.LockOSThread()
    
    // Initialize C library - will use thread-local storage
    C.initialize_library()
    p.initialized = true
    return nil
}

func (p *CLibraryProcessor) Process(input *Data) (*Result, error) {
    // This guaranteed to run on the same OS thread as Init()
    // C library can safely access its thread-local state
    return C.process_data(input), nil
}

func (p *CLibraryProcessor) Close() error {
    // Cleanup also happens on the same OS thread
    C.cleanup_library()
    return nil
}
```

#### Why This Matters

- **Thread Safety**: Each processor runs in isolation, eliminating shared state issues
- **C Library Compatibility**: Perfect for integrating legacy C/C++ libraries that weren't designed for Go's goroutine model  
- **Predictable Resource Management**: Thread-local resources are properly managed within processor lifecycle
- **Blocking Operations**: Processors can safely perform blocking I/O or call blocking C functions without affecting other processors

#### Processor Initialization and Goroutine Management

Coral's processor initialization follows a consistent pattern across all processor types through `InitializeXXXProcessor` functions. These functions implement a **factory closure pattern** that separates processor setup from execution:

```go
// Step 1: Create the setup closure
setupFn := InitializeGeneric1In1OutSyncProcessor[*MyIO](processor, options...)

// Step 2: Call the closure to spawn processor goroutine  
controller, outputCh, err := setupFn(inputCh)
```

**How the Pattern Works:**

1. **Setup Phase**: `InitializeXXXProcessor` configures logging, options, and returns a closure
2. **Execution Phase**: The closure spawns a dedicated goroutine for the processor and returns:
   - `*Controller`: Interface for sending control messages (start/stop/pause/resume) to the processor goroutine
   - `chan O`: Output channel for receiving processed data (if processor produces output)
   - `error`: Indicates processor initialization failure

**Processor Lifecycle in Goroutine:**

```
Goroutine Spawned → processor.Init() → Wait for Start Signal → Processing Loop → processor.Close()
                                    ↑                                      ↓
                              Controller.Start()                    Controller.Stop()
```

**Consistent Signatures Across Processor Types:**

- **Generators** (`0→1`): `func() (*Controller, chan O, error)` - No input required
- **Transformers** (`1→1`): `func(<-chan I) (*Controller, chan O, error)` - Input and output channels  
- **Sinks** (`1→0`): `func(<-chan I) (*Controller, error)` - Input channel only
- **Aggregators** (`N→1`): `func([]<-chan I) (*Controller, chan O, error)` - Multiple inputs, one output

This design ensures that:
- Each processor runs in complete isolation within its own goroutine
- Controller provides thread-safe communication with the processor goroutine
- Initialization errors are caught before goroutine execution begins
- Resource cleanup happens automatically when the goroutine terminates

## Processor Types

Coral provides several processor patterns to handle different data flow scenarios:

### **Generator Processors** ([`Generic0In1OutSyncProcessor`](./processor0-1.go#L8-L12))
Create data from external sources without input dependencies.

**Interface**: See [`processor0-1.go`](./processor0-1.go) for the complete interface definition.

**Use cases**: File readers, API clients, sensor data collectors, test data generators

### **Transformer Processors** ([`Generic1In1OutSyncProcessor`](./processor1-1.go#L8-L12))
Transform data in a 1:1 mapping relationship.

**Interface**: See [`processor1-1.go`](./processor1-1.go) for the complete interface definition.

**Use cases**: Data validation, format conversion, business logic application, enrichment

### **Aggregator Processors** ([`GenericNIn1OutAsyncProcessor`](./processorN-1.go#L8-L13))
Combine multiple input streams into a single output stream.

**Interface**: See [`processorN-1.go`](./processorN-1.go) for the complete interface definition.

**Use cases**: Data joining, stream merging, fan-in operations, correlation

### **Broadcaster Processors** ([`Generic1InNOutSyncProcessor`](./processor1-N.go#L8-L12))
Distribute single inputs to multiple output streams.

**Interface**: See [`processor1-N.go`](./processor1-N.go) for the complete interface definition.

**Use cases**: Data routing, replication, fan-out operations, parallel processing

### **Sink Processors** ([`Generic1In0OutSyncProcessor`](./processor1-0.go#L8-L12))
Consume data and perform side effects without producing outputs.

**Interface**: See [`processor1-0.go`](./processor1-0.go) for the complete interface definition.

**Use cases**: Database writers, file outputs, logging, notifications, metrics collection

## Multi-Processor Scaling

Coral supports horizontal scaling within pipeline stages using multi-processors:

```
Single Processor:
┌─────────┐    ┌─────────────┐    ┌─────────┐
│ Input   │───▶│ Processor   │───▶│ Output  │
└─────────┘    └─────────────┘    └─────────┘

Multi-Processor:
┌─────────┐    ┌─────────────┐    ┌─────────┐
│         │───▶│ Processor 1 │───▶│         │
│ Input   │    ├─────────────┤    │ Output  │
│ Dist.   │───▶│ Processor 2 │───▶│ Agg.    │
│         │    ├─────────────┤    │         │
│         │───▶│ Processor 3 │───▶│         │
└─────────┘    └─────────────┘    └─────────┘
```

Multi-processors automatically distribute work across multiple processor instances:

```go
// Create multiple processor instances
processors := []*MyProcessor{
    NewMyProcessor(),
    NewMyProcessor(), 
    NewMyProcessor(),
}

// Add as multi-processor for parallel execution
pipeline.AddGeneric1In1OutSyncMultiProcessor[*MyIO](
    ppl, processors, "parallel-stage")
```

## Advanced Features

### **Backpressure Handling**
Configure how processors handle output channel congestion:

```go
processor.Option{
    BlockOnOutput: true,  // Block until output is consumed
    // OR
    BlockOnOutput: false, // Drop messages when output buffer is full
}
```

### **Custom Control Messages**
Send application-specific control messages to running processors:

```go
type CustomControl struct {
    ConfigUpdate map[string]interface{}
}

// Send control message
controller.Control(&CustomControl{
    ConfigUpdate: map[string]interface{}{
        "rate_limit": 1000,
    },
})

// Handle in processor
func (p *MyProcessor) OnControl(msg interface{}) error {
    if ctrl, ok := msg.(*CustomControl); ok {
        // Handle configuration update
        return p.updateConfig(ctrl.ConfigUpdate)
    }
    return nil
}
```

### **Resource Management**
IO Adapters provide hooks for resource acquisition and cleanup:

```go
func (io *MyIO) ReleaseInput(input *InputType) {
    // Clean up input resources (close files, return to pools, etc.)
}

func (io *MyIO) ReleaseOutput(output *OutputType) {
    // Clean up output resources
}
```

## Examples

Explore complete examples in the [`example/`](./example/) directory:

- **[Simple Pipeline](./example/simple/)**: Basic 3-stage pipeline demonstrating core concepts
- More examples coming soon!

## Installation

```bash
go get github.com/l0rem1psum/coral
```

## Requirements

- Go 1.21 or later
- No external dependencies beyond the Go standard library

## Contributing

Contributions are welcome! Please feel free to submit issues, feature requests, or pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Related Projects

Coral draws inspiration from functional programming and stream processing frameworks while focusing on Go's strengths in concurrent programming and type safety.

---

**Keywords**: Go pipeline, data processing, concurrent programming, type-safe, stream processing, ETL, data flow, goroutines, channels, functional programming, DAG processing