package livecanary

import (
	"context"
	"fmt"
	"sync"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

type Executor interface {
	Execute(
		context.Context,
		execution.OperationID,
		execution.SequentialPlan,
	) (executionport.SequentialResult, error)
}

type Event struct {
	Operation execution.OperationID
	Result    executionport.SequentialResult
	Err       error
	// RetryEvaluation is true only when no stage settled and the failure is
	// known not to have produced its intended economic effect. This includes
	// a pre-broadcast rejection and an atomically confirmed first-stage revert.
	RetryEvaluation bool
	// FromRetryEvaluation prevents a permanently rejected opportunity from
	// creating a tight synthetic retry loop. A real event or idle evaluation
	// may still offer it again.
	FromRetryEvaluation bool
}

// Manager keeps execution off the websocket/evaluation loop. Offer never
// blocks and accepts at most one operation at a time.
type Manager struct {
	ctx      context.Context
	planner  Planner
	executor Executor
	events   chan Event

	mu            sync.Mutex
	active        bool
	closed        bool
	accepted      int
	maxOperations int
	wg            sync.WaitGroup
}

func NewManager(
	ctx context.Context,
	planner Planner,
	executor Executor,
) (*Manager, error) {
	return NewManagerWithLimit(ctx, planner, executor, 1)
}

func NewManagerWithLimit(
	ctx context.Context,
	planner Planner,
	executor Executor,
	maxOperations int,
) (*Manager, error) {
	if ctx == nil || executor == nil {
		return nil, fmt.Errorf("live manager context and executor are required")
	}
	if planner.ExecutionUnits == nil || planner.ExecutionUnits.Sign() <= 0 {
		return nil, fmt.Errorf("live manager requires positive execution units")
	}
	if maxOperations < 0 {
		return nil, fmt.Errorf("live manager operation limit cannot be negative")
	}
	return &Manager{
		ctx: ctx, planner: planner, executor: executor,
		events: make(chan Event, 1), maxOperations: maxOperations,
	}, nil
}

// Offer returns false when another operation is active. It deliberately does
// not queue stale opportunities. A zero maxOperations keeps the manager armed
// indefinitely while preserving the single-active-operation barrier.
func (m *Manager) Offer(opportunity arbitrage.Opportunity) (bool, error) {
	plan, err := m.planner.Plan(opportunity)
	if err != nil {
		return false, err
	}
	operationID, err := newOperationID()
	if err != nil {
		return false, err
	}
	m.mu.Lock()
	if m.closed || m.active ||
		(m.maxOperations > 0 && m.accepted >= m.maxOperations) {
		m.mu.Unlock()
		return false, nil
	}
	m.active = true
	if m.maxOperations > 0 {
		m.accepted++
	}
	m.wg.Add(1)
	m.mu.Unlock()
	go func() {
		defer m.wg.Done()
		result, executeErr := m.executor.Execute(m.ctx, operationID, plan)
		retryEvaluation := executeErr != nil &&
			len(result.Settlements) == 0 &&
			executionport.IsDefinitiveFailure(executeErr)
		m.mu.Lock()
		m.active = false
		if retryEvaluation && m.maxOperations > 0 && m.accepted > 0 {
			m.accepted--
		}
		closed := m.closed
		m.mu.Unlock()
		if closed {
			return
		}
		select {
		case m.events <- Event{
			Operation:       operationID,
			Result:          result,
			Err:             executeErr,
			RetryEvaluation: retryEvaluation,
			FromRetryEvaluation: plan.Opportunity.HasTrigger &&
				plan.Opportunity.Trigger.Source == "live/retry",
		}:
		case <-m.ctx.Done():
		}
	}()
	return true, nil
}

func (m *Manager) Events() <-chan Event { return m.events }

func (m *Manager) Close() {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	m.wg.Wait()
	close(m.events)
}
