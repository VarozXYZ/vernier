package livecanary_test

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync/atomic"
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

func TestFlowCostOracleKeepsAmountSpecificCosts(t *testing.T) {
	now := time.Date(2026, 8, 16, 14, 0, 0, 0, time.UTC)
	left := arbitrage.Direction{BuyMarket: "a", SellMarket: "b"}
	right := arbitrage.Direction{BuyMarket: "b", SellMarket: "a"}
	input250, _ := market.NewAssetQuantity("usdc", big.NewRat(250, 1))
	input1000, _ := market.NewAssetQuantity("usdc", big.NewRat(1000, 1))
	oracle, err := livecanary.NewFlowCostOracle(livecanary.FlowCostOracleConfig{
		Directions: []arbitrage.Direction{left, right}, QuoteAsset: "usdc",
		RefreshInterval: time.Second, TTL: 3 * time.Second, Clock: func() time.Time { return now },
		Refresh: func(context.Context, []arbitrage.Opportunity) ([]livecanary.FlowCostEstimate, error) {
			return []livecanary.FlowCostEstimate{
				{Direction: left, Input: input250, Components: []livecanary.FlowCostComponent{component(t, "flow", "0.20", now)}},
				{Direction: left, Input: input1000, Components: []livecanary.FlowCostComponent{component(t, "flow", "0.50", now)}},
				{Direction: right, Input: input250, Components: []livecanary.FlowCostComponent{component(t, "flow", "0.25", now)}},
				{Direction: right, Input: input1000, Components: []livecanary.FlowCostComponent{component(t, "flow", "0.55", now)}},
			}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := oracle.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		direction arbitrage.Direction
		input     market.AssetQuantity
		want      string
	}{{left, input250, "1/5"}, {left, input1000, "1/2"}, {right, input250, "1/4"}, {right, input1000, "11/20"}} {
		snapshot, ok := oracle.SnapshotFor(test.direction, test.input, now)
		if !ok || snapshot.Amount.Rat().RatString() != test.want {
			t.Fatalf("direction=%v input=%s cost=%v ok=%t want=%s", test.direction, test.input, snapshot.Amount, ok, test.want)
		}
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

func TestFlowCostOracleRedactsProviderURLsFromRetainedErrors(t *testing.T) {
	now := time.Date(2026, 8, 16, 14, 0, 0, 0, time.UTC)
	direction := arbitrage.Direction{BuyMarket: "a", SellMarket: "b"}
	oracle, err := livecanary.NewFlowCostOracle(livecanary.FlowCostOracleConfig{
		Directions: []arbitrage.Direction{direction}, QuoteAsset: "usdc",
		RefreshInterval: time.Second, TTL: 3 * time.Second, Clock: func() time.Time { return now },
		Refresh: func(context.Context, []arbitrage.Opportunity) ([]livecanary.FlowCostEstimate, error) {
			return nil, errors.New(`Post "https://provider.example/private-key": unavailable`)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = oracle.Warm(context.Background()); err == nil {
		t.Fatal("provider failure unexpectedly succeeded")
	}
	retained := oracle.LastError()
	if retained == nil || strings.Contains(retained.Error(), "private-key") ||
		!strings.Contains(retained.Error(), "[provider-url]") {
		t.Fatalf("provider URL was not redacted: %v", retained)
	}
}

func TestFlowCostOracleUsesExplicitStaleFallbackWithoutBlockingAdmission(t *testing.T) {
	now := time.Date(2026, 8, 14, 10, 0, 0, 0, time.UTC)
	direction := arbitrage.Direction{BuyMarket: "market_a", SellMarket: "market_b"}
	reverse := arbitrage.Direction{BuyMarket: "market_b", SellMarket: "market_a"}
	fallback, err := market.NewAssetQuantity("usdc", big.NewRat(15, 100))
	if err != nil {
		t.Fatal(err)
	}
	stale := make(chan error, 1)
	oracle, err := livecanary.NewFlowCostOracle(livecanary.FlowCostOracleConfig{
		Directions: []arbitrage.Direction{direction, reverse}, QuoteAsset: "usdc",
		RefreshInterval: 10 * time.Millisecond, TTL: 20 * time.Millisecond,
		Clock: func() time.Time { return now }, StaleFallback: fallback,
		StaleAlertAfter: 10 * time.Millisecond,
		OnStale:         func(err error) { stale <- err },
		Refresh: func(context.Context, []arbitrage.Opportunity) ([]livecanary.FlowCostEstimate, error) {
			return nil, errors.New("provider unavailable")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, ok := oracle.Snapshot(direction, now)
	if !ok || snapshot.Amount.Rat().Cmp(big.NewRat(15, 100)) != 0 ||
		!strings.Contains(snapshot.ID, "fixed-stale-fallback") {
		t.Fatalf("fallback snapshot=%+v ok=%t", snapshot, ok)
	}
	components, ok := oracle.Components(direction, now)
	if !ok || len(components) != 1 ||
		components[0].Evidence != "configured_stale_cost_fallback" {
		t.Fatalf("fallback components=%+v ok=%t", components, ok)
	}
	for _, route := range []execution.SequentialExitRoute{
		execution.ExitSellAtDestination,
		execution.ExitReturnToOrigin,
		execution.ExitSellAtOrigin,
	} {
		cost, available := oracle.ExitCost(direction, route, now)
		if !available || cost.Rat().Cmp(big.NewRat(15, 100)) != 0 {
			t.Fatalf("route=%s fallback cost=%s available=%t", route, cost, available)
		}
	}
	prefunded, available := oracle.PrefundedExitCost(
		direction,
		execution.ExitSellAtDestination,
		now,
	)
	if !available || prefunded.Rat().Cmp(big.NewRat(15, 100)) != 0 {
		t.Fatalf("prefunded fallback cost=%s available=%t", prefunded, available)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go oracle.Run(ctx)
	time.Sleep(35 * time.Millisecond)
	select {
	case alert := <-stale:
		t.Fatalf("usable fallback incorrectly reported an admission block: %v", alert)
	default:
	}
}

func TestFlowCostOracleRejectsInvalidStaleFallback(t *testing.T) {
	direction := arbitrage.Direction{BuyMarket: "market_a", SellMarket: "market_b"}
	fallback, err := market.NewAssetQuantity("other", big.NewRat(15, 100))
	if err != nil {
		t.Fatal(err)
	}
	_, err = livecanary.NewFlowCostOracle(livecanary.FlowCostOracleConfig{
		Directions: []arbitrage.Direction{direction}, QuoteAsset: "usdc",
		RefreshInterval: time.Second, TTL: 2 * time.Second,
		StaleFallback: fallback,
		Refresh: func(context.Context, []arbitrage.Opportunity) ([]livecanary.FlowCostEstimate, error) {
			return nil, nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "stale cost fallback is invalid") {
		t.Fatalf("error=%v", err)
	}
}

func TestFlowCostOracleMergesIndependentlyObservedDirections(t *testing.T) {
	now := time.Date(2026, 8, 13, 8, 0, 0, 0, time.UTC)
	left := arbitrage.Direction{BuyMarket: "market-a", SellMarket: "market-b"}
	right := arbitrage.Direction{BuyMarket: "market-b", SellMarket: "market-a"}
	observed := 0
	oracle, err := livecanary.NewFlowCostOracle(livecanary.FlowCostOracleConfig{
		Directions: []arbitrage.Direction{left, right}, QuoteAsset: "usdc",
		RefreshInterval: time.Second, TTL: 3 * time.Second, Clock: func() time.Time { return now },
		Refresh: func(_ context.Context, opportunities []arbitrage.Opportunity) ([]livecanary.FlowCostEstimate, error) {
			observed = len(opportunities)
			return []livecanary.FlowCostEstimate{
				{Direction: left, Components: []livecanary.FlowCostComponent{component(t, "flow", "1", now)}},
				{Direction: right, Components: []livecanary.FlowCostComponent{component(t, "flow", "1", now)}},
			}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	oracle.Observe([]arbitrage.Opportunity{observedOpportunity(left)})
	oracle.Observe([]arbitrage.Opportunity{observedOpportunity(right)})
	if err := oracle.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	if observed != 2 {
		t.Fatalf("observed directions=%d, want 2", observed)
	}
}

func TestFlowCostOracleIncompleteObservationDoesNotReplaceUsableDirection(t *testing.T) {
	now := time.Date(2026, 8, 13, 8, 0, 0, 0, time.UTC)
	direction := arbitrage.Direction{BuyMarket: "market-a", SellMarket: "market-b"}
	observed := 0
	oracle, err := livecanary.NewFlowCostOracle(livecanary.FlowCostOracleConfig{
		Directions: []arbitrage.Direction{direction}, QuoteAsset: "usdc",
		RefreshInterval: time.Second, TTL: 3 * time.Second, Clock: func() time.Time { return now },
		Refresh: func(_ context.Context, opportunities []arbitrage.Opportunity) ([]livecanary.FlowCostEstimate, error) {
			observed = len(opportunities)
			return []livecanary.FlowCostEstimate{{Direction: direction,
				Components: []livecanary.FlowCostComponent{component(t, "flow", "1", now)}}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	oracle.Observe([]arbitrage.Opportunity{observedOpportunity(direction)})
	oracle.Observe([]arbitrage.Opportunity{{Direction: direction}})
	if err := oracle.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	if observed != 1 {
		t.Fatalf("usable observations=%d, want 1", observed)
	}
}

func TestFlowCostOracleObservationNeverSchedulesProviderRefresh(t *testing.T) {
	direction := arbitrage.Direction{BuyMarket: "market-a", SellMarket: "market-b"}
	var refreshes atomic.Int64
	oracle, err := livecanary.NewFlowCostOracle(livecanary.FlowCostOracleConfig{
		Directions: []arbitrage.Direction{direction}, QuoteAsset: "usdc",
		RefreshInterval: time.Second, TTL: 2 * time.Second, Clock: time.Now,
		Refresh: func(_ context.Context, _ []arbitrage.Opportunity) ([]livecanary.FlowCostEstimate, error) {
			refreshes.Add(1)
			return nil, errors.New("provider call must remain periodic")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go oracle.Run(ctx)
	oracle.Observe([]arbitrage.Opportunity{{Direction: direction}})
	time.Sleep(75 * time.Millisecond)
	if got := refreshes.Load(); got != 0 {
		t.Fatalf("provider refreshes after market observation=%d, want 0", got)
	}
}

func TestFlowCostOracleAlertsAfterStaleCacheBlocksAdmissionAndOnRecovery(t *testing.T) {
	direction := arbitrage.Direction{BuyMarket: "market-a", SellMarket: "market-b"}
	var fail atomic.Bool
	fail.Store(true)
	stale := make(chan error, 1)
	recovered := make(chan struct{}, 1)
	oracle, err := livecanary.NewFlowCostOracle(livecanary.FlowCostOracleConfig{
		Directions: []arbitrage.Direction{direction}, QuoteAsset: "usdc",
		RefreshInterval: 10 * time.Millisecond, TTL: 20 * time.Millisecond,
		StaleAlertAfter: 25 * time.Millisecond, Clock: time.Now,
		OnStale:     func(err error) { stale <- err },
		OnRecovered: func() { recovered <- struct{}{} },
		Refresh: func(_ context.Context, _ []arbitrage.Opportunity) ([]livecanary.FlowCostEstimate, error) {
			if fail.Load() {
				return nil, errors.New("provider unavailable")
			}
			now := time.Now().UTC()
			return []livecanary.FlowCostEstimate{{Direction: direction,
				Components: []livecanary.FlowCostComponent{component(t, "flow", "0.1", now)}}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go oracle.Run(ctx)
	select {
	case alert := <-stale:
		t.Fatalf("cache health alone alerted before blocking admission: %v", alert)
	case <-time.After(60 * time.Millisecond):
	}
	if _, ok := oracle.Snapshot(direction, time.Now().UTC()); ok {
		t.Fatal("unavailable cache unexpectedly returned a snapshot")
	}
	select {
	case alert := <-stale:
		if alert == nil || !strings.Contains(alert.Error(), "provider unavailable") {
			t.Fatalf("stale alert=%v", alert)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("stale cache did not alert")
	}
	fail.Store(false)
	select {
	case <-recovered:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("recovered cache did not alert")
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

func observedOpportunity(direction arbitrage.Direction) arbitrage.Opportunity {
	output, _ := market.NewTokenAmount("quote", big.NewInt(1))
	return arbitrage.Opportunity{Direction: direction, Candidates: []arbitrage.Candidate{{
		SellQuote: market.Quote{AmountOut: output},
	}}}
}
