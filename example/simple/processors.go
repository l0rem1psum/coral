package main

import (
	"log/slog"
	"math/rand"
	"time"
)

// Step 1: Random Number Generator Processor (0-input, 1-output)
type GenerateRandomNumberProcessor struct {
	logger *slog.Logger
}

func NewGenerateRandomNumberProcessor(logger *slog.Logger) *GenerateRandomNumberProcessor {
	return &GenerateRandomNumberProcessor{
		logger: logger,
	}
}

func (p *GenerateRandomNumberProcessor) Init() error {
	p.logger.Info("Initializing random number generator")
	return nil
}

func (p *GenerateRandomNumberProcessor) Process() (*RandomNumberResult, error) {
	value := rand.Intn(100) + 1 // Generate random number between 1-100
	p.logger.Info("Generated random number", "value", value)
	time.Sleep(1 * time.Second) // Slow down generation
	return &RandomNumberResult{Value: value}, nil
}

func (p *GenerateRandomNumberProcessor) Close() error {
	p.logger.Info("Closing random number generator")
	return nil
}

// Step 2: Math Computation Processor (1-input, 1-output)
type ComputeNumberProcessor struct {
	logger *slog.Logger
}

func NewComputeNumberProcessor(logger *slog.Logger) *ComputeNumberProcessor {
	return &ComputeNumberProcessor{
		logger: logger,
	}
}

func (p *ComputeNumberProcessor) Init() error {
	p.logger.Info("Initializing math computation processor")
	return nil
}

func (p *ComputeNumberProcessor) Process(input *ComputeInput) (*ComputeResult, error) {
	squared := input.Number * input.Number
	p.logger.Info("Computing square", "input", input.Number, "result", squared)
	return &ComputeResult{
		Original: input.Number,
		Squared:  squared,
	}, nil
}

func (p *ComputeNumberProcessor) Close() error {
	p.logger.Info("Closing math computation processor")
	return nil
}

// Step 3: Print Processor (1-input, 0-output)
type PrintProcessor struct {
	logger *slog.Logger
}

func NewPrintProcessor(logger *slog.Logger) *PrintProcessor {
	return &PrintProcessor{
		logger: logger,
	}
}

func (p *PrintProcessor) Init() error {
	p.logger.Info("Initializing print processor")
	return nil
}

func (p *PrintProcessor) Process(input *PrintInput) error {
	p.logger.Info("RESULT", "original", input.Original, "squared", input.Squared)
	return nil
}

func (p *PrintProcessor) Close() error {
	p.logger.Info("Closing print processor")
	return nil
}