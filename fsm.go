package processor

import "sync/atomic"

// ProcessorState represents the current state of a processor in its lifecycle
type ProcessorState int32

const (
	StateCreated ProcessorState = iota
	StateInitializing
	StateWaitingToStart
	StateRunning
	StatePaused
	StateTerminating
	StateTerminated
)

// String returns a human-readable representation of the processor state
func (s ProcessorState) String() string {
	switch s {
	case StateCreated:
		return "Created"
	case StateInitializing:
		return "Initializing"
	case StateWaitingToStart:
		return "WaitingToStart"
	case StateRunning:
		return "Running"
	case StatePaused:
		return "Paused"
	case StateTerminating:
		return "Terminating"
	case StateTerminated:
		return "Terminated"
	default:
		return "Unknown"
	}
}

type fsm struct {
	state atomic.Int32
}

func (fsm *fsm) getState() ProcessorState {
	return ProcessorState(fsm.state.Load())
}

func (fsm *fsm) setState(newState ProcessorState) {
	fsm.state.Store(int32(newState))
}
