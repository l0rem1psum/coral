# MultiProcessor Example

This example demonstrates Coral's **multiprocessor capabilities** for horizontal scaling within pipeline stages. It builds on the simple pipeline example but uses multiple processor instances for parallel processing.

## Workflow

The example creates a 3-stage data processing workflow with multiprocessor scaling:

1. **Batch Generator** (0→1): Generates batches of 3 random numbers every 2 seconds
2. **Multi-Computer** (1→1): **3 parallel instances** process numbers simultaneously  
3. **Batch Printer** (1→0): Prints the aggregated results from all processors

## Key MultiProcessor Features Demonstrated

### Parallel Processing
```go
// Create 3 processor instances for parallel execution
computeProcessors := []*ComputeNumberProcessor{
    NewComputeNumberProcessor(logger, 1),
    NewComputeNumberProcessor(logger, 2), 
    NewComputeNumberProcessor(logger, 3),
}

// Add as multiprocessor - same interface as single processor
err = pipeline.AddGeneric1In1OutSyncMultiProcessor[*MultiProcessorComputeIO](
    ppl,
    computeProcessors,
    "multi_computer",
)
```

### Batch Input Distribution
- **Input**: `[]*ComputeInput` slice with 3 numbers
- **Distribution**: Each number goes to a different processor instance (1:1 mapping)
- **Processing**: All 3 numbers processed simultaneously in parallel
- **Output**: `[]*ComputeResult` slice with results in same order

### Load Balancing
```
Batch Input: [23, 47, 89]
     ↓
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│Processor 1  │  │Processor 2  │  │Processor 3  │
│   23 → 529  │  │   47 → 2209 │  │   89 → 7921 │
└─────────────┘  └─────────────┘  └─────────────┘
     ↓                ↓                ↓
     └────────────────┼────────────────┘
                      ↓
              [529, 2209, 7921]
```

## Performance Benefits

- **3x Throughput**: Process 3 numbers simultaneously instead of sequentially
- **Consistent Interface**: MultiProcessor uses same Controller interface as single processors
- **Automatic Coordination**: Framework handles synchronization and result aggregation
- **Type Safety**: Compile-time verification ensures processor compatibility

## Running the Example

```bash
cd example/multiprocessor
go run .
```

## Expected Output

You should see logs showing:
- Batch generation with 3 numbers
- Parallel processing across 3 instances (note different instance IDs)
- Coordinated result aggregation
- Higher overall throughput compared to single processor

## Comparison with Simple Example

| Aspect | Simple Pipeline | MultiProcessor Pipeline |
|--------|----------------|------------------------|
| **Input** | Single numbers | Batches of 3 numbers |
| **Processing** | Sequential (1 processor) | Parallel (3 processors) |
| **Throughput** | 1 number per cycle | 3 numbers per cycle |
| **Coordination** | Single goroutine | 3 coordinated goroutines |
| **Interface** | Same Pipeline API | Same Pipeline API |

## When to Use MultiProcessors

Use multiprocessors when you need:
- **High throughput** for CPU-intensive operations
- **Load distribution** across multiple processor instances  
- **Parallel processing** within pipeline stages
- **Scalability** without changing pipeline topology