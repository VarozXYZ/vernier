package livecanary_test

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	"github.com/VarozXYZ/vernier/runtime/livecanary"
)

func TestFlowCostOraclePublishesDirectionalCompleteFlowCosts(t *testing.T) {
	now := time.Date(2026, 7, 28, 8, 0, 0, 0, time.UTC)
	left := arbitrage.Direction{BuyMarket: "solana", SellMarket: "polygon"}
	right := arbitrage.Direction{BuyMarket: "polygon", SellMarket: "solana"}
	ready := 0
	oracle, err := livecanary.NewFlowCostOracle(livecanary.FlowCostOracleConfig{
		Directions: []arbitrage.Direction{left, right}, QuoteAsset: "usdc",
		RefreshInterval: time.Second, TTL: 3 * time.Second, Clock: func() time.Time { return now },
		OnReady: func() { ready++ },
		Refresh: func(context.Context, []arbitrage.Opportunity) ([]livecanary.FlowCostEstimate, error) {
			return []livecanary.FlowCostEstimate{
				{Direction: left, Components: []livecanary.FlowCostComponent{
					component(t, "buy_swap", "0.10", now),
					component(t, "base_bridge", "0.20", now),
					component(t, "sell_swap", "0.30", now),
					component(t, "quote_bridge", "0.40", now),
				}},
				{Direction: right, Components: []livecanary.FlowCostComponent{
					component(t, "buy_swap", "0.25", now),
					component(t, "base_bridge", "0.25", now),
					component(t, "sell_swap", "0.25", now),
					component(t, "quote_bridge", "0.25", now),
				}},
			}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := oracle.Snapshot(left, now); ok {
		t.Fatal("cold cache unexpectedly usable")
	}
	if err := oracle.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ready != 1 {
		t.Fatalf("ready transitions=%d, want 1", ready)
	}
	if err := oracle.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ready != 1 {
		t.Fatalf("ready callback repeated while cache remained healthy: %d", ready)
	}
	for _, direction := range []arbitrage.Direction{left, right} {
		snapshot, ok := oracle.Snapshot(direction, now.Add(2*time.Second))
		if !ok {
			t.Fatalf("direction %v unavailable", direction)
		}
		if got := snapshot.Amount.Rat().RatString(); got != "1" {
			t.Fatalf("cost = %s, want 1", got)
		}
		components, ok := oracle.Components(direction, now)
		if !ok || len(components) != 4 {
			t.Fatalf("components = %d, ok=%v", len(components), ok)
		}
	}
	if _, ok := oracle.Snapshot(left, now.Add(4*time.Second)); ok {
		t.Fatal("stale cache unexpectedly usable")
	}
}

func TestFlowCostOracleKeepsLastGoodSnapshotUntilTTL(t *testing.T) {
	now := time.Date(2026, 7, 28, 8, 0, 0, 0, time.UTC)
	direction := arbitrage.Direction{BuyMarket: "solana", SellMarket: "polygon"}
	fail := false
	oracle, err := livecanary.NewFlowCostOracle(livecanary.FlowCostOracleConfig{
		Directions: []arbitrage.Direction{direction}, QuoteAsset: "usdc",
		RefreshInterval: time.Second, TTL: 3 * time.Second, Clock: func() time.Time { return now },
		Refresh: func(context.Context, []arbitrage.Opportunity) ([]livecanary.FlowCostEstimate, error) {
			if fail {
				return nil, errors.New("probe unavailable")
			}
			return []livecanary.FlowCostEstimate{{
				Direction: direction,
				Components: []livecanary.FlowCostComponent{
					component(t, "complete_flow", "0.5", now),
				},
			}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := oracle.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	fail = true
	if err := oracle.Warm(context.Background()); err == nil {
		t.Fatal("failed refresh unexpectedly succeeded")
	}
	if _, ok := oracle.Snapshot(direction, now.Add(2*time.Second)); !ok {
		t.Fatal("last good snapshot should remain usable before TTL")
	}
	if oracle.LastError() == nil {
		t.Fatal("refresh error was not retained")
	}
}

func TestFlowCostOracleSelectsOnlyPendingExitCosts(t *testing.T) {
	now := time.Date(2026, 7, 28, 8, 0, 0, 0, time.UTC)
	forward := arbitrage.Direction{
		BuyMarket: "solana", SellMarket: "polygon",
	}
	reverse := arbitrage.Direction{
		BuyMarket: "polygon", SellMarket: "solana",
	}
	oracle, err := livecanary.NewFlowCostOracle(
		livecanary.FlowCostOracleConfig{
			Directions: []arbitrage.Direction{forward, reverse},
			QuoteAsset: "usdc", RefreshInterval: time.Second,
			TTL: 3 * time.Second, Clock: func() time.Time { return now },
			Refresh: func(
				context.Context,
				[]arbitrage.Opportunity,
			) ([]livecanary.FlowCostEstimate, error) {
				return []livecanary.FlowCostEstimate{
					{
						Direction: forward,
						Components: []livecanary.FlowCostComponent{
							component(t, "swap_buy", "0.01", now),
							component(t, "base_bridge_evm", "0.02", now),
							component(t, "swap_sell", "0.03", now),
							component(t, "quote_bridge_spread", "0.04", now),
							component(t, "quote_bridge_source", "0.05", now),
						},
					},
					{
						Direction: reverse,
						Components: []livecanary.FlowCostComponent{
							component(t, "swap_buy", "0.10", now),
							component(t, "base_bridge_evm", "0.20", now),
							component(t, "base_bridge_solana", "0.30", now),
							component(t, "swap_sell", "0.40", now),
							component(t, "quote_bridge_spread", "0.50", now),
						},
					},
				}, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := oracle.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	destination, ok := oracle.ExitCost(
		forward, execution.ExitSellAtDestination, now,
	)
	if !ok || destination.Rat().Cmp(big.NewRat(12, 100)) != 0 {
		t.Fatalf("destination cost=%s ok=%t", destination, ok)
	}
	returned, ok := oracle.ExitCost(
		forward, execution.ExitReturnToOrigin, now,
	)
	if !ok || returned.Rat().Cmp(big.NewRat(9, 10)) != 0 {
		t.Fatalf("return cost=%s ok=%t", returned, ok)
	}
	originSale, ok := oracle.ExitCost(
		forward, execution.ExitSellAtOrigin, now,
	)
	if !ok || originSale.Rat().Cmp(big.NewRat(4, 10)) != 0 {
		t.Fatalf("origin sale cost=%s ok=%t", originSale, ok)
	}
	prefundedDestination, ok := oracle.PrefundedExitCost(
		forward, execution.ExitSellAtDestination, now,
	)
	if !ok || prefundedDestination.Rat().Cmp(big.NewRat(14, 100)) != 0 {
		t.Fatalf("prefunded destination cost=%s ok=%t", prefundedDestination, ok)
	}
	prefundedOrigin, ok := oracle.PrefundedExitCost(
		forward, execution.ExitSellAtOrigin, now,
	)
	if !ok || prefundedOrigin.Rat().Cmp(big.NewRat(4, 10)) != 0 {
		t.Fatalf("prefunded origin cost=%s ok=%t", prefundedOrigin, ok)
	}
}

func component(t *testing.T, kind, amount string, at time.Time) livecanary.FlowCostComponent {
	t.Helper()
	value, ok := new(big.Rat).SetString(amount)
	if !ok {
		t.Fatal("invalid amount")
	}
	quantity, err := market.NewAssetQuantity("usdc", value)
	if err != nil {
		t.Fatal(err)
	}
	return livecanary.FlowCostComponent{
		Kind: kind, Amount: quantity, Evidence: "test", CapturedAt: at,
	}
}
