# Controllers Example

This example demonstrates using Coral processors with **Controllers directly** instead of Pipelines. It implements the same data processing workflow as the `simple` example but shows the manual approach.

## Workflow

The example creates a 3-stage data processing workflow:

1. **Generator** (0→1): Generates random numbers every second
2. **Computer** (1→1): Squares the random numbers  
3. **Printer** (1→0): Prints the results

## Key Differences from Pipeline Approach

### Manual Channel Wiring
```go
// Each processor must be manually connected
genController, genOutputCh, err := genSetup()
compController, compOutputCh, err := compSetup(genOutputCh)  // Connect to generator
printController, err := printSetup(compOutputCh)             // Connect to computer
```

### Manual Startup Sequence
```go
// Must start in dependency order
genController.Start()   // Start generator first
compController.Start()  // Then computer
printController.Start() // Finally printer
```

### Manual Shutdown Sequence  
```go
// Must stop in reverse dependency order
printController.Stop() // Stop printer first
compController.Stop()   // Then computer
genController.Stop()    // Finally generator
```

### Error Handling Complexity
```go
if err := compController.Start(); err != nil {
    // Must manually cleanup already-started processors
    genController.Stop()
    os.Exit(1)
}
```

## Running the Example

```bash
cd example/controllers
go run .
```

## When to Use This Approach

Use Controllers directly when you need:
- Custom processor orchestration outside DAG topology
- Fine-grained control over individual processor lifecycle
- Dynamic processor management at runtime
- Non-standard startup/shutdown sequences

For most use cases, the Pipeline approach (see `../simple/`) is recommended as it handles all the complexity automatically.