package saga

import (
	"sync"

	"github.com/VarozXYZ/vernier/domain/execution"
)

// OperationGate enforces the initial one-active-operation policy across the
// complete lifecycle, not merely across preparation and broadcast.
type OperationGate struct {
	mu     sync.Mutex
	active execution.OperationID
}

func NewOperationGate() *OperationGate { return &OperationGate{} }

func (g *OperationGate) TryBegin(operation execution.OperationID) bool {
	if g == nil || operation == "" {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active != "" {
		return false
	}
	g.active = operation
	return true
}

func (g *OperationGate) Restore(operation execution.OperationID) bool {
	return g.TryBegin(operation)
}

func (g *OperationGate) Complete(operation execution.OperationID) bool {
	if g == nil || operation == "" {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active != operation {
		return false
	}
	g.active = ""
	return true
}

func (g *OperationGate) Active() (execution.OperationID, bool) {
	if g == nil {
		return "", false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.active, g.active != ""
}
