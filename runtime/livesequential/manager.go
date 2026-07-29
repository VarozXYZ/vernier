package livesequential

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
	// known to have happened before a transaction could change balances.
	RetryEvaluation bool
	// FromRetryEvaluation prevents a permanently rejected opportunity from
	// creating a tight synthetic retry loop. A real trigger may still offer it
	// again.
	FromRetryEvaluation bool
}

// Manager keeps execution off the feed/evaluation loop. Offer never blocks,
// never queues stale opportunities, and accepts at most one active operation.
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
		return nil, fmt.Errorf(
			"sequential manager context and executor are required",
		)
	}
	if planner.ExecutionUnits == nil || planner.ExecutionUnits.Sign() <= 0 {
		return nil, fmt.Errorf(
			"sequential manager requires positive execution units",
		)
	}
	if maxOperations <= 0 {
		return nil, fmt.Errorf(
			"sequential manager requires a positive operation limit",
		)
	}
	return &Manager{
		ctx: ctx, planner: planner, executor: executor,
		events: make(chan Event, maxOperations), maxOperations: maxOperations,
	}, nil
}

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
	if m.closed || m.active || m.accepted >= m.maxOperations {
		m.mu.Unlock()
		return false, nil
	}
	m.active = true
	m.accepted++
	m.wg.Add(1)
	m.mu.Unlock()

	go func() {
		defer m.wg.Done()
		result, executeErr := m.executor.Execute(m.ctx, operationID, plan)
		retryEvaluation := executeErr != nil &&
			len(result.Settlements) == 0 &&
			executionport.ErrorDisposition(executeErr) ==
				executionport.DispositionRejected

		m.mu.Lock()
		m.active = false
		if retryEvaluation && m.accepted > 0 {
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
