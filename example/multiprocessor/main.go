package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/l0rem1psum/coral/pipeline"
)

func main() {
	// Initialize logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Define the pipeline topology for multiprocessor example
	vertices := []pipeline.ProcessorVertex{
		{
			Label:   "batch_generator",
			Inputs:  []pipeline.EdgeLabels{}, // No inputs for generator
			Outputs: []pipeline.EdgeLabels{{"batch_numbers"}},
		},
		{
			Label:   "multi_computer",
			Inputs:  []pipeline.EdgeLabels{{"batch_numbers"}},
			Outputs: []pipeline.EdgeLabels{{"batch_results"}},
		},
		{
			Label:   "batch_printer",
			Inputs:  []pipeline.EdgeLabels{{"batch_results"}},
			Outputs: []pipeline.EdgeLabels{}, // No outputs for printer
		},
	}

	// Create the pipeline
	ppl, err := pipeline.NewPipeline(vertices, logger)
	if err != nil {
		logger.Error("Failed to create pipeline", "error", err)
		os.Exit(1)
	}

	// Create and add processors to the pipeline

	// 1. Add batch random number generator (Generic0In1OutSyncProcessor)
	batchGenerator := NewBatchGenerateRandomNumberProcessor(logger.With("component", "batch_generator"))
	err = pipeline.AddGeneric0In1OutSyncProcessor[*BatchGeneratorToMultiProcessorIO](
		ppl,
		batchGenerator,
		"batch_generator",
	)
	if err != nil {
		logger.Error("Failed to add batch generator processor", "error", err)
		os.Exit(1)
	}

	// 2. Add multiprocessor for parallel computation (3 instances)
	// Create multiple instances of the compute processor
	computeProcessors := []*ComputeNumberProcessor{
		NewComputeNumberProcessor(logger.With("component", "multi_computer"), 1),
		NewComputeNumberProcessor(logger.With("component", "multi_computer"), 2),
		NewComputeNumberProcessor(logger.With("component", "multi_computer"), 3),
	}
	
	err = pipeline.AddGeneric1In1OutSyncMultiProcessor[*MultiProcessorComputeIO](
		ppl,
		computeProcessors,
		"multi_computer",
	)
	if err != nil {
		logger.Error("Failed to add multiprocessor", "error", err)
		os.Exit(1)
	}

	// 3. Add batch print processor (Generic1In0OutSyncProcessor for slices)
	batchPrinter := NewBatchPrintProcessor(logger.With("component", "batch_printer"))
	err = pipeline.AddGeneric1In0OutSyncProcessor[*MultiProcessorToBatchPrinterIO](
		ppl,
		batchPrinter,
		"batch_printer",
	)
	if err != nil {
		logger.Error("Failed to add batch printer processor", "error", err)
		os.Exit(1)
	}

	// Initialize the pipeline
	err = ppl.Initialize()
	if err != nil {
		logger.Error("Failed to initialize pipeline", "error", err)
		os.Exit(1)
	}

	// Start the pipeline
	err = ppl.Start()
	if err != nil {
		logger.Error("Failed to start pipeline", "error", err)
		os.Exit(1)
	}

	logger.Info("Multiprocessor pipeline started successfully")
	logger.Info("Using 3 parallel compute processors for enhanced throughput")

	// Let the pipeline run for a few seconds to generate some batches
	time.Sleep(10 * time.Second)

	// Stop the pipeline
	err = ppl.Stop()
	if err != nil {
		logger.Error("Failed to stop pipeline", "error", err)
		os.Exit(1)
	}

	logger.Info("Multiprocessor pipeline stopped successfully")
}