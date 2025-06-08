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

	// Define the pipeline topology
	vertices := []pipeline.ProcessorVertex{
		{
			Label:   "generator",
			Inputs:  []pipeline.EdgeLabels{}, // No inputs for generator
			Outputs: []pipeline.EdgeLabels{{"random_numbers"}},
		},
		{
			Label:   "computer",
			Inputs:  []pipeline.EdgeLabels{{"random_numbers"}},
			Outputs: []pipeline.EdgeLabels{{"computed_results"}},
		},
		{
			Label:   "printer",
			Inputs:  []pipeline.EdgeLabels{{"computed_results"}},
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

	// 1. Add random number generator (Generic0In1OutSyncProcessor)
	generator := NewGenerateRandomNumberProcessor(logger.With("component", "generator"))
	err = pipeline.AddGeneric0In1OutSyncProcessor[*GeneratorToComputerIO](
		ppl,
		generator,
		"generator",
	)
	if err != nil {
		logger.Error("Failed to add generator processor", "error", err)
		os.Exit(1)
	}

	// 2. Add math computation processor (Generic1In1OutSyncProcessor)
	computer := NewComputeNumberProcessor(logger.With("component", "computer"))
	err = pipeline.AddGeneric1In1OutSyncProcessor[*ComputerToPrinterIO](
		ppl,
		computer,
		"computer",
	)
	if err != nil {
		logger.Error("Failed to add computer processor", "error", err)
		os.Exit(1)
	}

	// 3. Add print processor (Generic1In0OutSyncProcessor)
	printer := NewPrintProcessor(logger.With("component", "printer"))
	err = pipeline.AddGeneric1In0OutSyncProcessor[*PrintProcessorIO](
		ppl,
		printer,
		"printer",
	)
	if err != nil {
		logger.Error("Failed to add printer processor", "error", err)
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

	logger.Info("Pipeline started successfully")

	// Let the pipeline run for a few seconds to generate some numbers
	time.Sleep(5 * time.Second)

	// Stop the pipeline
	err = ppl.Stop()
	if err != nil {
		logger.Error("Failed to stop pipeline", "error", err)
		os.Exit(1)
	}

	logger.Info("Pipeline stopped successfully")
}
