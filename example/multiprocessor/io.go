package main

// IO Adapters for Multiprocessor Example
//
// These adapters handle the type transformations required for the multiprocessor pipeline,
// including batch processing and slice distribution.

// IO adapter from Batch Generator to MultiProcessor
// Implements Generic0In1OutSyncProcessorIO for generating slices
type BatchGeneratorToMultiProcessorIO struct{}

func (io *BatchGeneratorToMultiProcessorIO) FromOutput(result *BatchGeneratorResult) []*ComputeInput {
	// Convert batch of values to slice of ComputeInput for multiprocessor distribution
	inputs := make([]*ComputeInput, len(result.Values))
	for i, value := range result.Values {
		inputs[i] = &ComputeInput{Number: value}
	}
	return inputs
}

func (io *BatchGeneratorToMultiProcessorIO) ReleaseOutput(inputs []*ComputeInput) {
	// No resources to release in this simple example
}

// IO adapter for MultiProcessor - handles slice processing
// Implements Generic1In1OutSyncProcessorIO for multiprocessor
type MultiProcessorComputeIO struct{}

func (io *MultiProcessorComputeIO) AsInput(input *ComputeInput) *ComputeInput {
	return input
}

func (io *MultiProcessorComputeIO) FromOutput(input *ComputeInput, result *ComputeResult) *ComputeResult {
	return result
}

func (io *MultiProcessorComputeIO) ReleaseInput(input *ComputeInput) {
	// No resources to release in this simple example
}

func (io *MultiProcessorComputeIO) ReleaseOutput(result *ComputeResult) {
	// No resources to release in this simple example
}

// IO adapter from MultiProcessor output to Batch Print Processor
// Implements Generic1In0OutSyncProcessorIO for processing result slices
type MultiProcessorToBatchPrinterIO struct{}

func (io *MultiProcessorToBatchPrinterIO) AsInput(results []*ComputeResult) []*ComputeResult {
	return results
}

func (io *MultiProcessorToBatchPrinterIO) ReleaseInput(results []*ComputeResult) {
	// No resources to release in this simple example
}