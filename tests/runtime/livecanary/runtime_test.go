package livecanary_test

import (
	"context"
	"errors"
	"io"
	"math/big"
	"path/filepath"
	"testing"
	"time"

	sqlitestore "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	"github.com/VarozXYZ/vernier/runtime/livecanary"
	"github.com/VarozXYZ/vernier/runtime/livecompare"
)

type continuingStream struct {
	started    chan struct{}
	reevaluate chan struct{}
}

func (s *continuingStream) RunStream(
	ctx context.Context,
	options livecompare.StreamOptions,
) error {
	close(s.started)
	select {
	case <-ctx.Done():
		return nil
	case <-options.ReevaluationRequests:
		close(s.reevaluate)
	}
	<-ctx.Done()
	return nil
}

type successfulExecutor struct{}

func (successfulExecutor) Execute(
	_ context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
) (executionport.SequentialResult, error) {
	return executionport.SequentialResult{
		Operation: operation, FinalAmount: plan.InitialInput,
	}, nil
}

func TestProductionRuntimeContinuesAndReevaluatesAfterCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager, err := livecanary.NewManagerWithLimit(
		ctx,
		livecanary.Planner{
			MarketChains: map[market.MarketID]market.ChainID{
				"market-a": "chain-a", "market-b": "chain-b",
			},
			ExecutionUnits: big.NewInt(1_000_000),
		},
		successfulExecutor{},
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	store, err := sqlitestore.Open(
		filepath.Join(t.TempDir(), "opportunities.sqlite"),
	)
	if err != nil {
		t.Fatal(err)
	}
	stream := &continuingStream{
		started: make(chan struct{}), reevaluate: make(chan struct{}),
	}
	reevaluate := make(chan time.Time, 1)
	runtime, err := livecanary.NewRuntime(
		stream,
		manager,
		store,
		nil,
		io.Discard,
		nil,
		reevaluate,
		true,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	runResult := make(chan error, 1)
	go func() { runResult <- runtime.Run(ctx) }()
	select {
	case <-stream.started:
	case <-time.After(time.Second):
		t.Fatal("research stream did not start")
	}
	accepted, err := manager.Offer(runtimeOpportunity(t))
	if err != nil || !accepted {
		t.Fatalf("offer: accepted=%t err=%v", accepted, err)
	}
	select {
	case <-stream.reevaluate:
	case err := <-runResult:
		t.Fatalf("runtime exited after successful operation: %v", err)
	case <-time.After(time.Second):
		t.Fatal("runtime did not request a post-settlement reevaluation")
	}
	select {
	case err := <-runResult:
		t.Fatalf("runtime exited after successful operation: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-runResult:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("runtime cancellation error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not stop after cancellation")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func runtimeOpportunity(t *testing.T) arbitrage.Opportunity {
	t.Helper()
	amount := func(token market.TokenID, units int64) market.TokenAmount {
		value, err := market.NewTokenAmount(token, big.NewInt(units))
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	return arbitrage.Opportunity{
		Evaluation:     "evaluation",
		ConfigHash:     "config",
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
