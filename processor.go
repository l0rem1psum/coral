package processor

import (
	"errors"
	"log/slog"
)

var (
	ErrControlNotSupported = errors.New("control not supported")
	ErrProcessorNotRunning = errors.New("processor not running")
	ErrAlreadyRunning      = errors.New("already running")
	ErrAlreadyPaused       = errors.New("already paused")
	ErrProcessorBusy       = errors.New("processor busy")
	ErrUnableToStart       = errors.New("unable to start")
	ErrMultipleStart       = errors.New("multiple start")
	ErrMultipleStop        = errors.New("multiple stop")
)

type config struct {
	label              *string
	logger             *slog.Logger
	startPaused        bool
	blockOnOutput      bool
	useRoundRobinFanIn bool // TODO: warn if set on non-N processor
	outputChannelSize  int
}

type Option func(*config)

func WithLogger(logger *slog.Logger) Option {
	return func(c *config) {
		c.logger = logger
	}
}

func WithLabel(label string) Option {
	return func(c *config) {
		c.label = &label
	}
}

func StartPaused() Option {
	return func(c *config) {
		c.startPaused = true
	}
}

func BlockOnOutput() Option {
	return func(c *config) {
		c.blockOnOutput = true
	}
}

func UseRoundRobinFanIn() Option {
	return func(c *config) {
		c.useRoundRobinFanIn = true
	}
}

func WithOutputChannelSize(size int) Option {
	return func(c *config) {
		c.outputChannelSize = max(0, size)
	}
}
