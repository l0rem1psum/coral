package main

import (
	"log/slog"
	"os"
	"time"

	processor "github.com/l0rem1psum/coral"
)

func main() {
	// Initialize logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Create processors
	generator := NewGenerateRandomNumberProcessor(logger.With("component", "generator"))
	computer := NewComputeNumberProcessor(logger.With("component", "computer"))
	printer := NewPrintProcessor(logger.With("component", "printer"))

	// Manual processor initialization using Controllers
	
	// 1. Initialize Generator (0→1)
	genSetup := processor.InitializeGeneric0In1OutSyncProcessor[*GeneratorToComputerIO](
		generator,
		processor.WithLogger(logger.With("processor", "generator")),
		processor.WithLabel("generator"),
	)
	genController, genOutputCh, err := genSetup()
	if err != nil {
		logger.Error("Failed to initialize generator", "error", err)
		os.Exit(1)
	}

	// 2. Initialize Computer (1→1) - connect to generator output
	compSetup := processor.InitializeGeneric1In1OutSyncProcessor[*ComputerToPrinterIO](
		computer,
		processor.WithLogger(logger.With("processor", "computer")),
		processor.WithLabel("computer"),
	)
	compController, compOutputCh, err := compSetup(genOutputCh)
	if err != nil {
		logger.Error("Failed to initialize computer", "error", err)
		os.Exit(1)
	}

	// 3. Initialize Printer (1→0) - connect to computer output
	printSetup := processor.InitializeGeneric1In0OutSyncProcessor[*PrintProcessorIO](
		printer,
		processor.WithLogger(logger.With("processor", "printer")),
		processor.WithLabel("printer"),
	)
	printController, err := printSetup(compOutputCh)
	if err != nil {
		logger.Error("Failed to initialize printer", "error", err)
		os.Exit(1)
	}

	// Manual startup sequence - order matters!
	logger.Info("Starting processors in dependency order...")
	
	if err := genController.Start(); err != nil {
		logger.Error("Failed to start generator", "error", err)
		os.Exit(1)
	}
	
	if err := compController.Start(); err != nil {
		logger.Error("Failed to start computer", "error", err)
		// Cleanup on failure
		genController.Stop()
		os.Exit(1)
	}
	
	if err := printController.Start(); err != nil {
		logger.Error("Failed to start printer", "error", err)
		// Cleanup on failure
		compController.Stop()
		genController.Stop()
		os.Exit(1)
	}

	logger.Info("All processors started successfully")

	// Let the processors run for a few seconds
	time.Sleep(5 * time.Second)

	// Manual shutdown sequence - reverse order!
	logger.Info("Stopping processors in reverse dependency order...")
	
	if err := printController.Stop(); err != nil {
		logger.Error("Failed to stop printer", "error", err)
	}
	
	if err := compController.Stop(); err != nil {
		logger.Error("Failed to stop computer", "error", err)
	}
	
	if err := genController.Stop(); err != nil {
		logger.Error("Failed to stop generator", "error", err)
	}

	logger.Info("All processors stopped successfully")
}