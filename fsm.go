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

// // ProcessorEvent represents events that can trigger state transitions
// type ProcessorEvent int

// const (
// 	EventInitSuccess ProcessorEvent = iota
// 	EventInitFailed
// 	EventStartRequested
// 	EventStopBeforeStart
// 	EventInputReceived
// 	EventInputClosed
// 	EventPauseRequested
// 	EventResumeRequested
// 	EventCustomControl
// 	EventCloseRequested
// )

// // String returns a human-readable representation of the processor event
// func (e ProcessorEvent) String() string {
// 	switch e {
// 	case EventInitSuccess:
// 		return "InitSuccess"
// 	case EventInitFailed:
// 		return "InitFailed"
// 	case EventStartRequested:
// 		return "StartRequested"
// 	case EventStopBeforeStart:
// 		return "StopBeforeStart"
// 	case EventInputReceived:
// 		return "InputReceived"
// 	case EventInputClosed:
// 		return "InputClosed"
// 	case EventPauseRequested:
// 		return "PauseRequested"
// 	case EventResumeRequested:
// 		return "ResumeRequested"
// 	case EventCustomControl:
// 		return "CustomControl"
// 	case EventCloseRequested:
// 		return "CloseRequested"
// 	default:
// 		return "Unknown"
// 	}
// }

type fsm struct {
	state atomic.Int32
}

func (fsm *fsm1In1OutSync[IO, I, O, In, Out]) getState() ProcessorState {
	return ProcessorState(fsm.state.Load())
}

func (fsm *fsm1In1OutSync[IO, I, O, In, Out]) setState(newState ProcessorState) {
	fsm.state.Store(int32(newState))
}
