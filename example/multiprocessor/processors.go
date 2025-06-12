package main

import (
	"log/slog"
	"math/rand"
	"time"
)

// Step 1: Batch Random Number Generator Processor (0-input, 1-output)
// Generates batches of random numbers for multiprocessor consumption
type BatchGenerateRandomNumberProcessor struct {
	logger  *slog.Logger
	batchID int
}

func NewBatchGenerateRandomNumberProcessor(logger *slog.Logger) *BatchGenerateRandomNumberProcessor {
	return &BatchGenerateRandomNumberProcessor{
		logger:  logger,
		batchID: 0,
	}
}

func (p *BatchGenerateRandomNumberProcessor) Init() error {
	p.logger.Info("Initializing batch random number generator")
	return nil
}

func (p *BatchGenerateRandomNumberProcessor) Process() (*BatchGeneratorResult, error) {
	// Generate a batch of 3 random numbers
	values := make([]int, 3)
	for i := range values {
		values[i] = rand.Intn(100) + 1 // Generate random number between 1-100
	}
	
	p.batchID++
	p.logger.Info("Generated batch of random numbers", "values", values, "batchID", p.batchID)
	time.Sleep(2 * time.Second) // Slow down generation
	
	return &BatchGeneratorResult{
		Values:  values,
		BatchID: p.batchID,
	}, nil
}

func (p *BatchGenerateRandomNumberProcessor) Close() error {
	p.logger.Info("Closing batch random number generator")
	return nil
}

// Step 2: Math Computation Processor (1-input, 1-output) 
// This will be used in a multiprocessor setup for parallel processing
type ComputeNumberProcessor struct {
	logger     *slog.Logger
	instanceID int
}

func NewComputeNumberProcessor(logger *slog.Logger, instanceID int) *ComputeNumberProcessor {
	return &ComputeNumberProcessor{
		logger:     logger.With("instance", instanceID),
		instanceID: instanceID,
	}
}

func (p *ComputeNumberProcessor) Init() error {
	p.logger.Info("Initializing math computation processor")
	return nil
}

func (p *ComputeNumberProcessor) Process(input *ComputeInput) (*ComputeResult, error) {
	squared := input.Number * input.Number
	p.logger.Info("Computing square", "input", input.Number, "result", squared, "processor", p.instanceID)
	
	// Simulate some processing time
	time.Sleep(100 * time.Millisecond)
	
	return &ComputeResult{
		Original: input.Number,
		Squared:  squared,
	}, nil
}

func (p *ComputeNumberProcessor) Close() error {
	p.logger.Info("Closing math computation processor")
	return nil
}

// Step 3: Batch Print Processor (1-input, 0-output)
// Prints batches of results from the multiprocessor
type BatchPrintProcessor struct {
	logger *slog.Logger
}

func NewBatchPrintProcessor(logger *slog.Logger) *BatchPrintProcessor {
	return &BatchPrintProcessor{
		logger: logger,
	}
}

func (p *BatchPrintProcessor) Init() error {
	p.logger.Info("Initializing batch print processor")
	return nil
}

func (p *BatchPrintProcessor) Process(results []*ComputeResult) error {
	p.logger.Info("BATCH RESULTS", "count", len(results))
	for i, result := range results {
		p.logger.Info("RESULT", "index", i, "original", result.Original, "squared", result.Squared)
	}
	return nil
}

func (p *BatchPrintProcessor) Close() error {
	p.logger.Info("Closing batch print processor")
	return nil
}