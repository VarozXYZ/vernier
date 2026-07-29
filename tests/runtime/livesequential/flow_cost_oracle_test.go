package livesequential_test

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	"github.com/VarozXYZ/vernier/runtime/livesequential"
)

func TestFlowCostOraclePublishesDirectionalCompleteFlowCosts(
	t *testing.T,
) {
	now := time.Date(2026, 1, 2, 8, 0, 0, 0, time.UTC)
	left := arbitrage.Direction{
		BuyMarket:  "market-a",
		SellMarket: "market-b",
	}
	right := arbitrage.Direction{
		BuyMarket:  "market-b",
		SellMarket: "market-a",
	}
	ready := 0
	oracle, err := livesequential.NewFlowCostOracle(
		livesequential.FlowCostOracleConfig{
			Directions:      []arbitrage.Direction{left, right},
			QuoteAsset:      "quote",
			RefreshInterval: time.Second,
			TTL:             3 * time.Second,
			Clock:           func() time.Time { return now },
			OnReady:         func() { ready++ },
			Refresh: func(
				context.Context,
				[]arbitrage.Opportunity,
			) ([]livesequential.FlowCostEstimate, error) {
				return []livesequential.FlowCostEstimate{
					{
						Direction: left,
						Components: completeFlowComponents(
							t,
							now,
							"0.10",
							"0.20",
							"0.30",
							"0.40",
						),
					},
					{
						Direction: right,
						Components: completeFlowComponents(
							t,
							now,
							"0.25",
							"0.25",
							"0.25",
							"0.25",
						),
					},
				}, nil
			},
		},
	)
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
		t.Fatalf("ready callback repeated: %d", ready)
	}
	for _, direction := range []arbitrage.Direction{left, right} {
		snapshot, ok := oracle.Snapshot(
			direction,
			now.Add(2*time.Second),
		)
		if !ok {
			t.Fatalf("direction %v unavailable", direction)
		}
		if got := snapshot.Amount.Rat().RatString(); got != "1" {
			t.Fatalf("cost=%s, want 1", got)
		}
		components, ok := oracle.Components(direction, now)
		if !ok || len(components) != 4 {
			t.Fatalf(
				"components=%d available=%t",
				len(components),
				ok,
			)
		}
	}
	if _, ok := oracle.Snapshot(
		left,
		now.Add(4*time.Second),
	); ok {
		t.Fatal("stale cache unexpectedly usable")
	}
}

func TestFlowCostOracleKeepsLastGoodSnapshotUntilTTL(
	t *testing.T,
) {
	now := time.Date(2026, 1, 2, 8, 0, 0, 0, time.UTC)
	direction := arbitrage.Direction{
		BuyMarket:  "market-a",
		SellMarket: "market-b",
	}
	fail := false
	oracle, err := livesequential.NewFlowCostOracle(
		livesequential.FlowCostOracleConfig{
			Directions:      []arbitrage.Direction{direction},
			QuoteAsset:      "quote",
			RefreshInterval: time.Second,
			TTL:             3 * time.Second,
			Clock:           func() time.Time { return now },
			Refresh: func(
				context.Context,
				[]arbitrage.Opportunity,
			) ([]livesequential.FlowCostEstimate, error) {
				if fail {
					return nil, errors.New("probe unavailable")
				}
				return []livesequential.FlowCostEstimate{{
					Direction: direction,
					Components: []livesequential.FlowCostComponent{
						flowComponent(
							t,
							execution.StageBuy,
							"network",
							"0.5",
							now,
						),
					},
				}}, nil
			},
		},
	)
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
	if _, ok := oracle.Snapshot(
		direction,
		now.Add(2*time.Second),
	); !ok {
		t.Fatal("last good snapshot should remain usable before TTL")
	}
	if oracle.LastError() == nil {
		t.Fatal("refresh error was not retained")
	}
}

func TestFlowCostOracleSelectsPendingStagesWithoutChainNames(
	t *testing.T,
) {
	now := time.Date(2026, 1, 2, 8, 0, 0, 0, time.UTC)
	forward := arbitrage.Direction{
		BuyMarket:  "market-a",
		SellMarket: "market-b",
	}
	reverse := arbitrage.Direction{
		BuyMarket:  "market-b",
		SellMarket: "market-a",
	}
	oracle, err := livesequential.NewFlowCostOracle(
		livesequential.FlowCostOracleConfig{
			Directions:      []arbitrage.Direction{forward, reverse},
			QuoteAsset:      "quote",
			RefreshInterval: time.Second,
			TTL:             3 * time.Second,
			Clock:           func() time.Time { return now },
			Refresh: func(
				context.Context,
				[]arbitrage.Opportunity,
			) ([]livesequential.FlowCostEstimate, error) {
				return []livesequential.FlowCostEstimate{
					{
						Direction: forward,
						Components: []livesequential.FlowCostComponent{
							flowComponent(t, execution.StageBuy, "network", "0.01", now),
							flowComponent(t, execution.StageBridgeBase, "source", "0.02", now),
							flowComponent(t, execution.StageSell, "network", "0.03", now),
							flowComponent(t, execution.StageBridgeQuoteReturn, "spread", "0.04", now),
							flowComponent(t, execution.StageBridgeQuoteReturn, "source", "0.05", now),
						},
					},
					{
						Direction: reverse,
						Components: []livesequential.FlowCostComponent{
							flowComponent(t, execution.StageBuy, "network", "0.10", now),
							flowComponent(t, execution.StageBridgeBase, "source", "0.20", now),
							flowComponent(t, execution.StageBridgeBase, "destination", "0.30", now),
							flowComponent(t, execution.StageSell, "network", "0.40", now),
							flowComponent(t, execution.StageBridgeQuoteReturn, "spread", "0.50", now),
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
		forward,
		execution.ExitSellAtDestination,
		now,
	)
	if !ok || destination.Rat().Cmp(big.NewRat(12, 100)) != 0 {
		t.Fatalf("destination cost=%s available=%t", destination, ok)
	}
	returned, ok := oracle.ExitCost(
		forward,
		execution.ExitReturnToOrigin,
		now,
	)
	if !ok || returned.Rat().Cmp(big.NewRat(9, 10)) != 0 {
		t.Fatalf("return cost=%s available=%t", returned, ok)
	}
}

func completeFlowComponents(
	t *testing.T,
	at time.Time,
	amounts ...string,
) []livesequential.FlowCostComponent {
	t.Helper()
	stages := []execution.SequentialStage{
		execution.StageBuy,
		execution.StageBridgeBase,
		execution.StageSell,
		execution.StageBridgeQuoteReturn,
	}
	result := make([]livesequential.FlowCostComponent, len(stages))
	for index, stage := range stages {
		result[index] = flowComponent(
			t,
			stage,
			"synthetic",
			amounts[index],
			at,
		)
	}
	return result
}

func flowComponent(
	t *testing.T,
	stage execution.SequentialStage,
	kind string,
	amount string,
	at time.Time,
) livesequential.FlowCostComponent {
	t.Helper()
	value, ok := new(big.Rat).SetString(amount)
	if !ok {
		t.Fatal("invalid amount")
	}
	quantity, err := market.NewAssetQuantity("quote", value)
	if err != nil {
		t.Fatal(err)
	}
	return livesequential.FlowCostComponent{
		Stage:      stage,
		Kind:       kind,
		Amount:     quantity,
		Evidence:   "synthetic",
		CapturedAt: at,
	}
}
