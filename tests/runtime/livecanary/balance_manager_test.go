package livecanary_test

import (
	"context"
	"errors"
	"io"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/inventory"
	"github.com/VarozXYZ/vernier/domain/market"
	"github.com/VarozXYZ/vernier/runtime/configuration"
	"github.com/VarozXYZ/vernier/runtime/livecanary"
)

func TestBalanceManagerRejectsPrefundedOpportunityBeforeExecutionIO(t *testing.T) {
	manager, gate, _ := newTestBalanceManager(t, big.NewInt(3_000_000_000_000_000))
	if err := gate.Transition(livecanary.RuntimeGateStarting, livecanary.RuntimeGateIdle); err != nil {
		t.Fatal(err)
	}
	planner := livecanary.Planner{
		MarketChains: map[market.MarketID]market.ChainID{
			"market-a": "chain-a", "market-b": "chain-b",
		},
		ExecutionUnits:  big.NewInt(1_000_000),
		ExecutionPolicy: execution.PolicyPrefundedSequential,
	}
	plan, err := planner.Plan(liveCanaryOpportunity(t))
	if err != nil {
		t.Fatal(err)
	}
	err = manager.Admit(plan)
	var insufficient *livecanary.InsufficientLocalBalanceError
	if !errors.As(err, &insufficient) {
		t.Fatalf("expected local balance rejection, got %v", err)
	}
	// The 750 USDC discovery quote is scaled to the 1 USDC execution input:
	// 4 base units at 9 decimals become 4e18 units at 18 decimals.
	if insufficient.Required.Cmp(big.NewInt(4_000_000_000_000_000)) != 0 {
		t.Fatalf("required=%s", insufficient.Required)
	}
}

func TestBalanceManagerRejectsIncompletePrefundedParallelPlans(t *testing.T) {
	manager, _, _ := newTestBalanceManager(t, big.NewInt(9_000_000_000_000_000))
	planner := livecanary.Planner{
		MarketChains: map[market.MarketID]market.ChainID{
			"market-a": "chain-a", "market-b": "chain-b",
		},
		ExecutionUnits:  big.NewInt(1_000_000),
		ExecutionPolicy: execution.PolicyPrefundedParallel,
		BaseAsset:       "base",
		QuoteAsset:      "quote",
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9, "quote-b": 6, "base-b": 18,
		},
	}
	plan, err := planner.Plan(liveCanaryOpportunity(t))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("missing sell stage", func(t *testing.T) {
		partial := plan
		partial.Stages = partial.Stages[:1]
		if err := manager.Admit(partial); err == nil {
			t.Fatal("expected incomplete plan rejection")
		}
	})

	t.Run("missing sell input", func(t *testing.T) {
		incomplete := plan
		incomplete.Opportunity.Candidates = append(
			[]arbitrage.Candidate(nil), plan.Opportunity.Candidates...,
		)
		selected := incomplete.Opportunity.SelectedIndex
		incomplete.Opportunity.Candidates[selected].SellQuote.AmountIn = market.TokenAmount{}
		if err := manager.Admit(incomplete); err == nil {
			t.Fatal("expected incomplete sell quote rejection")
		}
	})
}

func TestBalanceManagerAppliesConfirmedSettlementLocally(t *testing.T) {
	manager, _, _ := newTestBalanceManager(t, big.NewInt(9_000_000_000_000_000))
	input, _ := market.NewTokenAmount("quote-a", big.NewInt(1_000_000))
	output, _ := market.NewTokenAmount("base-a", big.NewInt(4_000_000))
	settlement := execution.SequentialStageSettlement{
		Request: execution.SequentialStageRequest{
			Operation: "operation", Plan: "plan",
			Stage: execution.SequentialStagePlan{
				Ordinal: 1, Stage: execution.StageBuy, SourceChain: "chain-a",
				InputToken: "quote-a", OutputToken: "base-a", Market: "market-a",
			},
			Input: input,
		},
		ActualInput: input, ActualOutput: output,
		SourceIdentity: execution.TransactionIdentity{Chain: "chain-a", Account: "account-a", Hash: "tx"},
		ObservedAt:     time.Now().UTC(), Evidence: "receipt",
	}
	if err := manager.ObserveSettlement(settlement); err != nil {
		t.Fatal(err)
	}
	if err := manager.ObserveSettlement(settlement); err != nil {
		t.Fatal(err)
	}
	quote, _ := manager.Available("chain-a", "quote-a")
	base, _ := manager.Available("chain-a", "base-a")
	if quote.Cmp(big.NewInt(9_000_000)) != 0 || base.Cmp(big.NewInt(4_000_000)) != 0 {
		t.Fatalf("quote=%s base=%s", quote, base)
	}
}

func TestBalancePollingSkipsEveryTickWhileGateIsBusy(t *testing.T) {
	manager, gate, reads := newTestBalanceManager(t, big.NewInt(9_000_000_000_000_000))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go manager.Run(ctx)
	initial := reads.Load()
	time.Sleep(35 * time.Millisecond)
	if got := reads.Load(); got != initial {
		t.Fatalf("busy gate performed balance RPCs: initial=%d got=%d", initial, got)
	}
	if err := gate.Transition(livecanary.RuntimeGateStarting, livecanary.RuntimeGateIdle); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for reads.Load() == initial && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if reads.Load() == initial {
		t.Fatal("idle balance reconciliation did not run")
	}
}

func newTestBalanceManager(
	t *testing.T,
	prefunded *big.Int,
) (*livecanary.BalanceManager, *livecanary.RuntimeGate, *atomic.Int32) {
	t.Helper()
	token := func(id market.TokenID, chain market.ChainID, decimals uint8) market.Token {
		return market.Token{ID: id, Asset: market.AssetID(id), Chain: chain, Decimals: decimals, Symbol: string(id)}
	}
	balances := []configuration.ResolvedInventoryBalance{
		{Chain: "chain-a", Account: "account-a", Token: token("quote-a", "chain-a", 6), AllocationCap: big.NewRat(10, 1), Target: big.NewRat(10, 1), Buffer: new(big.Rat)},
		{Chain: "chain-a", Account: "account-a", Token: token("base-a", "chain-a", 9), AllocationCap: big.NewRat(100, 1), Target: big.NewRat(100, 1), Buffer: new(big.Rat)},
		{Chain: "chain-b", Account: "account-b", Token: token("quote-b", "chain-b", 6), AllocationCap: big.NewRat(10, 1), Target: big.NewRat(10, 1), Buffer: new(big.Rat)},
		{Chain: "chain-b", Account: "account-b", Token: token("base-b", "chain-b", 18), AllocationCap: big.NewRat(100, 1), Target: big.NewRat(100, 1), Buffer: new(big.Rat)},
	}
	values := map[inventory.Key]*big.Int{
		{Chain: "chain-a", Account: "account-a", Token: "quote-a"}: big.NewInt(10_000_000),
		{Chain: "chain-a", Account: "account-a", Token: "base-a"}:  new(big.Int),
		{Chain: "chain-b", Account: "account-b", Token: "quote-b"}: big.NewInt(10_000_000),
		{Chain: "chain-b", Account: "account-b", Token: "base-b"}:  new(big.Int).Set(prefunded),
	}
	var reads atomic.Int32
	readers := make(map[inventory.Key]livecanary.PhysicalBalanceReader)
	for key, value := range values {
		value := new(big.Int).Set(value)
		readers[key] = func(context.Context) (*big.Int, error) {
			reads.Add(1)
			return new(big.Int).Set(value), nil
		}
	}
	gate := livecanary.NewRuntimeGate()
	manager, err := livecanary.NewBalanceManager(livecanary.BalanceManagerConfig{
		Balances: balances, Readers: readers,
		Accounts: map[market.ChainID]execution.AccountID{"chain-a": "account-a", "chain-b": "account-b"},
		Gate:     gate, Output: io.Discard, PollInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	return manager, gate, &reads
}
