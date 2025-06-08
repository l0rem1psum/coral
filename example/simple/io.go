package main

// IO Adapters
//
// These adapters implement the processor IO interfaces to convert data between 
// different processor types. Each adapter handles the type transformations
// required for the pipeline connections.

// IO adapter from Generator to Computer
// Implements Generic0In1OutSyncProcessorIO
type GeneratorToComputerIO struct{}

func (io *GeneratorToComputerIO) FromOutput(result *RandomNumberResult) *ComputeInput {
	return &ComputeInput{Number: result.Value}
}

func (io *GeneratorToComputerIO) ReleaseOutput(input *ComputeInput) {
	// No resources to release in this simple example
}

// IO adapter from Computer to Printer
// Implements Generic1In1OutSyncProcessorIO
type ComputerToPrinterIO struct{}

func (io *ComputerToPrinterIO) AsInput(input *ComputeInput) *ComputeInput {
	return input
}

func (io *ComputerToPrinterIO) FromOutput(input *ComputeInput, result *ComputeResult) *PrintInput {
	return &PrintInput{
		Original: result.Original,
		Squared:  result.Squared,
	}
}

func (io *ComputerToPrinterIO) ReleaseInput(input *ComputeInput) {
	// No resources to release in this simple example
}

func (io *ComputerToPrinterIO) ReleaseOutput(output *PrintInput) {
	// No resources to release in this simple example
}

// IO adapter for the Print Processor
// Implements Generic1In0OutSyncProcessorIO
type PrintProcessorIO struct{}

func (io *PrintProcessorIO) AsInput(input *PrintInput) *PrintInput {
	return input
}

func (io *PrintProcessorIO) ReleaseInput(input *PrintInput) {
	// No resources to release in this simple example
}