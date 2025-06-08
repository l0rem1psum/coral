# Coral Pipeline Example

This example demonstrates how to create a simple 3-node pipeline using the Coral library for concurrent data processing.

## Pipeline Structure

The pipeline implements the following data flow:

```
[Generator] → [Computer] → [Printer]
```

### Node 1: Random Number Generator (`Generic0In1OutSyncProcessor`)
- **Type**: `Generic0In1OutSyncProcessor` (0 inputs, 1 output, synchronous)
- **Function**: Generates random integers between 1-100 every second
- **Output**: `RandomNumberResult` containing the generated value

### Node 2: Math Computation (`Generic1In1OutSyncProcessor`)
- **Type**: `Generic1In1OutSyncProcessor` (1 input, 1 output, synchronous)
- **Function**: 
  - Receives random numbers from the generator
  - Computes the square of each number
  - Passes the computation result to the printer
- **Input**: `ComputeInput` (converted from `RandomNumberResult`)
- **Output**: `ComputeResult` containing both original and squared values

### Node 3: Print Processor (`Generic1In0OutSyncProcessor`)
- **Type**: `Generic1In0OutSyncProcessor` (1 input, 0 outputs, synchronous)
- **Function**: Receives computation results and prints them to console
- **Input**: `PrintInput` (converted from `ComputeResult`)

## Key Components

### Processor Types Used

1. **`Generic0In1OutSyncProcessor`**: For generating data without inputs
2. **`Generic1In1OutSyncProcessor`**: For transforming data 1:1
3. **`Generic1In0OutSyncProcessor`**: For consuming data as a terminal sink

### IO Adapters

The pipeline uses IO adapter structs to convert data between processor types:

- **`GeneratorToComputerIO`**: Converts `RandomNumberResult` → `ComputeInput`
- **`ComputerToPrinterIO`**: Converts `ComputeResult` → `PrintInput`
- **`PrintProcessorIO`**: Manages input conversion for the print processor

These adapters implement the required interfaces:
- `Generic0In1OutSyncProcessorIO`
- `Generic1In1OutSyncProcessorIO`  
- `Generic1In0OutSyncProcessorIO`

### Pipeline Topology

The pipeline topology is defined using `ProcessorVertex` structs that specify:
- Processor labels
- Input edge labels (channel connections)
- Output edge labels (channel connections)

## Running the Example

```bash
cd example/simple
go run .
```
