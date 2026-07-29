package livesequential_test

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	"github.com/VarozXYZ/vernier/runtime/livesequential"
)

type blockingExecutor struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (e *blockingExecutor) Execute(
	ctx context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
) (executionport.SequentialResult, error) {
	e.once.Do(func() { close(e.started) })
	select {
	case <-e.release:
	case <-ctx.Done():
		return executionport.SequentialResult{}, ctx.Err()
	}
	return executionport.SequentialResult{
		Operation:   operation,
		FinalAmount: plan.InitialInput,
	}, nil
}

type retryingExecutor struct {
	mu          sync.Mutex
	calls       int
	disposition executionport.BroadcastDisposition
}

func (e *retryingExecutor) Execute(
	_ context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
) (executionport.SequentialResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	result := executionport.SequentialResult{Operation: operation}
	if e.calls == 1 {
		disposition := e.disposition
		if disposition == "" {
			disposition = executionport.DispositionRejected
		}
		return result, executionport.NewStageError(
			disposition,
			errors.New("first stage simulation rejected"),
		)
	}
	result.FinalAmount = plan.InitialInput
	return result, nil
}

func syntheticPlanner() livesequential.Planner {
	return livesequential.Planner{
		MarketChains: map[market.MarketID]market.ChainID{
			"market-a": "chain-a",
			"market-b": "chain-b",
		},
		ExecutionUnits: big.NewInt(1_000_000),
	}
}

func TestManagerDoesNotBlockOrQueueWhileOperationIsActive(
	t *testing.T,
) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	executor := &blockingExecutor{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager, err := livesequential.NewManager(
		ctx,
		syntheticPlanner(),
		executor,
	)
	if err != nil {
		t.Fatal(err)
	}
	first, err := manager.Offer(syntheticOpportunity(t))
	if err != nil || !first {
		t.Fatalf("first offer accepted=%t error=%v", first, err)
	}
	select {
	case <-executor.started:
	case <-time.After(time.Second):
		t.Fatal("execution did not start")
	}
	started := time.Now()
	second, err := manager.Offer(syntheticOpportunity(t))
	if err != nil {
		t.Fatal(err)
	}
	if second {
		t.Fatal("second offer was unexpectedly queued")
	}
	if time.Since(started) > 50*time.Millisecond {
		t.Fatal("Offer blocked the evaluation loop")
	}
	close(executor.release)
	select {
	case event := <-manager.Events():
		if event.Err != nil {
			t.Fatal(event.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("execution result was not emitted")
	}
	third, err := manager.Offer(syntheticOpportunity(t))
	if err != nil {
		t.Fatal(err)
	}
	if third {
		t.Fatal("manager exceeded its run limit")
	}
	manager.Close()
}

func TestManagerReleasesLimitAfterDefinitivePreBroadcastRejection(
	t *testing.T,
) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	executor := &retryingExecutor{}
	manager, err := livesequential.NewManagerWithLimit(
		ctx,
		syntheticPlanner(),
		executor,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := manager.Offer(syntheticOpportunity(t))
	if err != nil || !accepted {
		t.Fatalf("first offer accepted=%t error=%v", accepted, err)
	}
	select {
	case event := <-manager.Events():
		if !event.RetryEvaluation || event.Err == nil ||
			len(event.Result.Settlements) != 0 {
			t.Fatalf("unexpected retry event: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("first rejection was not emitted")
	}
	accepted, err = manager.Offer(syntheticOpportunity(t))
	if err != nil || !accepted {
		t.Fatalf("retry offer accepted=%t error=%v", accepted, err)
	}
	select {
	case event := <-manager.Events():
		if event.RetryEvaluation || event.Err != nil {
			t.Fatalf("unexpected completion event: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("retry result was not emitted")
	}
	manager.Close()
}

func TestManagerDoesNotRetryUncertainFirstBroadcast(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	executor := &retryingExecutor{
		disposition: executionport.DispositionPossible,
	}
	manager, err := livesequential.NewManagerWithLimit(
		ctx,
		syntheticPlanner(),
		executor,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := manager.Offer(syntheticOpportunity(t))
	if err != nil || !accepted {
		t.Fatalf("first offer accepted=%t error=%v", accepted, err)
	}
	select {
	case event := <-manager.Events():
		if event.RetryEvaluation || event.Err == nil {
			t.Fatalf("uncertain outcome became retryable: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("uncertain result was not emitted")
	}
	accepted, err = manager.Offer(syntheticOpportunity(t))
	if err != nil {
		t.Fatal(err)
	}
	if accepted {
		t.Fatal("manager reused the limit after an uncertain broadcast")
	}
	manager.Close()
}
