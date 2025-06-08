package main

// Data structures for passing data between processors

// RandomNumberResult represents the output from the random number generator
type RandomNumberResult struct {
	Value int
}

// ComputeInput represents the input to the math computation processor
type ComputeInput struct {
	Number int
}

// ComputeResult represents the output from the math computation processor
type ComputeResult struct {
	Original int
	Squared  int
}

// PrintInput represents the input to the print processor
type PrintInput struct {
	Original int
	Squared  int
}

// BatchGeneratorResult represents a batch of random numbers for multiprocessor
type BatchGeneratorResult struct {
	Values []int
	BatchID int
}