package livecanary

import (
	"fmt"
	"sync"
)

type RuntimeGateState string

const (
	RuntimeGateStarting        RuntimeGateState = "starting"
	RuntimeGateIdle            RuntimeGateState = "idle"
	RuntimeGateExecuting       RuntimeGateState = "executing"
	RuntimeGateRecovering      RuntimeGateState = "recovering"
	RuntimeGateRefueling       RuntimeGateState = "refueling"
	RuntimeGateRecoveryBlocked RuntimeGateState = "recovery_blocked"
	RuntimeGateStopping        RuntimeGateState = "stopping"
)

// RuntimeGate serializes execution, recovery and maintenance while allowing
// feeds to remain connected. Changes is edge-triggered; consumers must always
// re-read State after receiving from it.
type RuntimeGate struct {
	mu      sync.RWMutex
	state   RuntimeGateState
	changes chan struct{}
}

func NewRuntimeGate() *RuntimeGate {
	return &RuntimeGate{
		state: RuntimeGateStarting, changes: make(chan struct{}, 1),
	}
}

func (g *RuntimeGate) State() RuntimeGateState {
	if g == nil {
		return RuntimeGateIdle
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.state
}

func (g *RuntimeGate) EvaluationAllowed() bool {
	return g.State() == RuntimeGateIdle
}

func (g *RuntimeGate) EvaluationChanges() <-chan struct{} {
	if g == nil {
		return nil
	}
	return g.changes
}

func (g *RuntimeGate) Transition(
	from RuntimeGateState,
	to RuntimeGateState,
) error {
	if g == nil {
		return fmt.Errorf("runtime gate is unavailable")
	}
	if !validRuntimeGateTransition(from, to) {
		return fmt.Errorf("invalid runtime gate transition %s -> %s", from, to)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state != from {
		return fmt.Errorf(
			"runtime gate is %s, expected %s",
			g.state,
			from,
		)
	}
	g.state = to
	select {
	case g.changes <- struct{}{}:
	default:
	}
	return nil
}

func validRuntimeGateTransition(from, to RuntimeGateState) bool {
	if from == to {
		return false
	}
	switch from {
	case RuntimeGateStarting:
		return to == RuntimeGateIdle ||
			to == RuntimeGateRecovering ||
			to == RuntimeGateRecoveryBlocked ||
			to == RuntimeGateStopping
	case RuntimeGateIdle:
		return to == RuntimeGateExecuting ||
			to == RuntimeGateRecovering ||
			to == RuntimeGateRefueling ||
			to == RuntimeGateRecoveryBlocked ||
			to == RuntimeGateStopping
	case RuntimeGateExecuting:
		return to == RuntimeGateIdle ||
			to == RuntimeGateRecovering ||
			to == RuntimeGateRecoveryBlocked ||
			to == RuntimeGateStopping
	case RuntimeGateRecovering:
		return to == RuntimeGateIdle ||
			to == RuntimeGateRefueling ||
			to == RuntimeGateRecoveryBlocked ||
			to == RuntimeGateStopping
	case RuntimeGateRefueling:
		return to == RuntimeGateIdle ||
			to == RuntimeGateRecovering ||
			to == RuntimeGateRecoveryBlocked ||
			to == RuntimeGateStopping
	case RuntimeGateRecoveryBlocked:
		return to == RuntimeGateStopping
	default:
		return false
	}
}
