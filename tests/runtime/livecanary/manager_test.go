package livecanary_test

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	"github.com/VarozXYZ/vernier/runtime/livecanary"
)

type blockingSequentialExecutor struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type retryingSequentialExecutor struct {
	mu          sync.Mutex
	calls       int
	disposition executionport.BroadcastDisposition
}

func (e *retryingSequentialExecutor) Execute(
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
			errors.New("buy simulation rejected"),
		)
	}
	result.FinalAmount = plan.InitialInput
	return result, nil
}

func (e *blockingSequentialExecutor) Execute(
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
		Operation: operation, FinalAmount: plan.InitialInput,
	}, nil
}

func liveCanaryOpportunity(t *testing.T) arbitrage.Opportunity {
	t.Helper()
	amount := func(token market.TokenID, units int64) market.TokenAmount {
		value, err := market.NewTokenAmount(token, big.NewInt(units))
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	return arbitrage.Opportunity{
		Evaluation: "evaluation", ConfigHash: "config",
		Classification: arbitrage.ClassificationPolicyQualified,
		Direction: arbitrage.Direction{
			BuyMarket: "market-a", SellMarket: "market-b",
		},
		Candidates: []arbitrage.Candidate{{
			BuyQuote: market.Quote{
				AmountIn:  amount("quote-a", 750_000_000),
				AmountOut: amount("base-a", 3_000_000_000),
			},
			SellQuote: market.Quote{
				AmountIn:  amount("base-b", 3_000_000_000_000_000_000),
				AmountOut: amount("quote-b", 752_000_000),
			},
		}},
		SelectedIndex: 0,
	}
}

func TestPlannerConvertsHumanExecutionAmountPerBuyTokenDecimals(t *testing.T) {
	opportunity := liveCanaryOpportunity(t)
	for _, test := range []struct {
		decimals uint8
		want     string
	}{{6, "1000000"}, {18, "1000000000000000000"}} {
		planner := livecanary.Planner{MarketChains: map[market.MarketID]market.ChainID{"market-a": "chain-a", "market-b": "chain-b"},
			ExecutionAmount: big.NewRat(1, 1), TokenDecimals: map[market.TokenID]uint8{"quote-a": test.decimals}}
		plan, err := planner.Plan(opportunity)
		if err != nil {
			t.Fatal(err)
		}
		if got := plan.InitialInput.Units().String(); got != test.want {
			t.Fatalf("decimals=%d units=%s want=%s", test.decimals, got, test.want)
		}
	}
}

func TestPlannerPreservesSelectedDiscreteCandidateInput(t *testing.T) {
	opportunity := liveCanaryOpportunity(t)
	input, _ := market.NewAssetQuantity("quote", big.NewRat(750, 1))
	opportunity.Candidates[0].Input = input
	planner := livecanary.Planner{
		MarketChains:            map[market.MarketID]market.ChainID{"market-a": "chain-a", "market-b": "chain-b"},
		ExecutionAmount:         big.NewRat(1000, 1),
		AllowedExecutionAmounts: []*big.Rat{big.NewRat(250, 1), big.NewRat(500, 1), big.NewRat(750, 1), big.NewRat(1000, 1)},
	}
	plan, err := planner.Plan(opportunity)
	if err != nil {
		t.Fatal(err)
	}
	if got := plan.InitialInput.Units().String(); got != "750000000" {
		t.Fatalf("initial input=%s want selected candidate units", got)
	}

	opportunity.Candidates[0].Input, _ = market.NewAssetQuantity("quote", big.NewRat(600, 1))
	if _, err := planner.Plan(opportunity); err == nil {
		t.Fatal("unconfigured execution size was accepted")
	}
}

func TestManagerDoesNotBlockOrQueueWhileAnOperationIsActive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	executor := &blockingSequentialExecutor{
		started: make(chan struct{}), release: make(chan struct{}),
	}
	manager, err := livecanary.NewManager(
		ctx,
		livecanary.Planner{
			MarketChains: map[market.MarketID]market.ChainID{
				"market-a": "chain-a", "market-b": "chain-b",
			},
			ExecutionUnits: big.NewInt(1_000_000),
		},
		executor,
	)
	if err != nil {
		t.Fatal(err)
	}
	first, err := manager.Offer(liveCanaryOpportunity(t))
	if err != nil || !first {
		t.Fatalf("first offer was not accepted: accepted=%t err=%v", first, err)
	}
	select {
	case <-executor.started:
	case <-time.After(time.Second):
		t.Fatal("execution did not start")
	}
	started := time.Now()
	second, err := manager.Offer(liveCanaryOpportunity(t))
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
	third, err := manager.Offer(liveCanaryOpportunity(t))
	if err != nil {
		t.Fatal(err)
	}
	if third {
		t.Fatal("manager accepted more operations than its run limit")
	}
	manager.Close()
}

func TestManagerReleasesRunLimitAfterDefinitiveFirstStageRejection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	executor := &retryingSequentialExecutor{}
	manager, err := livecanary.NewManagerWithLimit(
		ctx,
		livecanary.Planner{
			MarketChains: map[market.MarketID]market.ChainID{
				"market-a": "chain-a", "market-b": "chain-b",
			},
			ExecutionUnits: big.NewInt(1_000_000),
		},
		executor,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := manager.Offer(liveCanaryOpportunity(t))
	if err != nil || !accepted {
		t.Fatalf("first offer: accepted=%t err=%v", accepted, err)
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
	accepted, err = manager.Offer(liveCanaryOpportunity(t))
	if err != nil || !accepted {
		t.Fatalf("retry offer: accepted=%t err=%v", accepted, err)
	}
	select {
	case event := <-manager.Events():
		if event.RetryEvaluation || event.Err != nil {
			t.Fatalf("unexpected completion event: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("second result was not emitted")
	}
	manager.Close()
}

func TestManagerReevaluatesAfterConfirmedFirstStageRevert(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	executor := &retryingSequentialExecutor{
		disposition: executionport.DispositionConfirmedFailure,
	}
	manager, err := livecanary.NewManagerWithLimit(
		ctx,
		livecanary.Planner{
			MarketChains: map[market.MarketID]market.ChainID{
				"market-a": "chain-a", "market-b": "chain-b",
			},
			ExecutionUnits: big.NewInt(1_000_000),
		},
		executor,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := manager.Offer(liveCanaryOpportunity(t))
	if err != nil || !accepted {
		t.Fatalf("first offer: accepted=%t err=%v", accepted, err)
	}
	select {
	case event := <-manager.Events():
		if !event.RetryEvaluation || event.Err == nil ||
			executionport.ErrorDisposition(event.Err) !=
				executionport.DispositionConfirmedFailure {
			t.Fatalf("unexpected confirmed-revert event: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("confirmed first-stage revert was not emitted")
	}
	accepted, err = manager.Offer(liveCanaryOpportunity(t))
	if err != nil || !accepted {
		t.Fatalf("retry offer: accepted=%t err=%v", accepted, err)
	}
	select {
	case event := <-manager.Events():
		if event.RetryEvaluation || event.Err != nil {
			t.Fatalf("unexpected completion event: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("second result was not emitted")
	}
	manager.Close()
}

func TestManagerDoesNotRetryUncertainFirstBroadcast(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	executor := &retryingSequentialExecutor{
		disposition: executionport.DispositionPossible,
	}
	manager, err := livecanary.NewManagerWithLimit(
		ctx,
		livecanary.Planner{
			MarketChains: map[market.MarketID]market.ChainID{
				"market-a": "chain-a", "market-b": "chain-b",
			},
			ExecutionUnits: big.NewInt(1_000_000),
		},
		executor,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := manager.Offer(liveCanaryOpportunity(t))
	if err != nil || !accepted {
		t.Fatalf("first offer: accepted=%t err=%v", accepted, err)
	}
	select {
	case event := <-manager.Events():
		if event.RetryEvaluation || event.Err == nil {
			t.Fatalf("uncertain outcome became retryable: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("uncertain result was not emitted")
	}
	accepted, err = manager.Offer(liveCanaryOpportunity(t))
	if err != nil {
		t.Fatal(err)
	}
	if accepted {
		t.Fatal("manager reused the run limit after an uncertain broadcast")
	}
	manager.Close()
}

func TestUnlimitedManagerAcceptsAnotherOperationAfterCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	executor := &retryingSequentialExecutor{calls: 1}
	manager, err := livecanary.NewManagerWithLimit(
		ctx,
		livecanary.Planner{
			MarketChains: map[market.MarketID]market.ChainID{
				"market-a": "chain-a", "market-b": "chain-b",
			},
			ExecutionUnits: big.NewInt(1_000_000),
		},
		executor,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		accepted, offerErr := manager.Offer(liveCanaryOpportunity(t))
		if offerErr != nil || !accepted {
			t.Fatalf(
				"offer %d: accepted=%t err=%v",
				index+1,
				accepted,
				offerErr,
			)
		}
		select {
		case event := <-manager.Events():
			if event.Err != nil {
				t.Fatalf("operation %d: %v", index+1, event.Err)
			}
		case <-time.After(time.Second):
			t.Fatalf("operation %d did not complete", index+1)
		}
	}
	manager.Close()
}
