package arbitrage_test

import (
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
)

type snapshotData struct{}

func (snapshotData) SnapshotKind() string { return "test" }

func TestArbitrageSetupBuildsBothDirections(t *testing.T) {
	registry := testRegistry(t)
	setup, err := arbitrage.NewArbitrageSetup("setup", "pair", []market.MarketID{"market-a", "market-b"}, registry)
	if err != nil {
		t.Fatal(err)
	}
	directions := setup.Directions()
	if len(directions) != 2 {
		t.Fatalf("expected two directions, got %d", len(directions))
	}
	directions[0].BuyMarket = "mutated"
	if setup.Directions()[0].BuyMarket == "mutated" {
		t.Fatal("setup exposed its internal direction slice")
	}
}

func TestEvaluationFixesUniqueSnapshots(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snapshots := []market.MarketSnapshot{testSnapshot(t, "market-a", now), testSnapshot(t, "market-b", now)}
	cost, _ := market.ParseAssetQuantity("quote", "1")
	evaluation, err := arbitrage.NewEvaluation(
		"evaluation", "run", "strategy", "hash", snapshots,
		arbitrage.CostSnapshot{ID: "cost", Amount: cost, CapturedAt: now}, now, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	snapshots[0] = testSnapshot(t, "market-b", now)
	if got, ok := evaluation.Snapshot("market-a"); !ok || got.Metadata().Market != "market-a" {
		t.Fatal("evaluation did not retain its fixed snapshot set")
	}

	duplicate := []market.MarketSnapshot{testSnapshot(t, "market-a", now), testSnapshot(t, "market-a", now)}
	if _, err := arbitrage.NewEvaluation("e", "r", "s", "h", duplicate, arbitrage.CostSnapshot{ID: "cost", Amount: cost, CapturedAt: now}, now, now); err == nil {
		t.Fatal("expected duplicate market snapshots to fail")
	}
}

func testSnapshot(t *testing.T, id market.MarketID, now time.Time) market.MarketSnapshot {
	t.Helper()
	snapshot, err := market.NewMarketSnapshot(market.SnapshotMetadata{
		Market: id, Source: "source", Version: 1,
		EventPosition: market.SourcePosition{Kind: "block", Value: 1},
		Finality:      market.FinalityConfirmed, ReceivedAt: now, AppliedAt: now,
		Health: market.HealthHealthy, HealthChangedAt: now,
	}, snapshotData{})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func testRegistry(t *testing.T) *market.Registry {
	t.Helper()
	registry, err := market.NewRegistry(market.Catalog{
		Chains: []market.Chain{{ID: "a"}, {ID: "b"}},
		Assets: []market.Asset{{ID: "base", Symbol: "B"}, {ID: "quote", Symbol: "Q"}},
		Tokens: []market.Token{
			{ID: "base-a", Asset: "base", Chain: "a", Symbol: "B"}, {ID: "quote-a", Asset: "quote", Chain: "a", Symbol: "Q"},
			{ID: "base-b", Asset: "base", Chain: "b", Symbol: "B"}, {ID: "quote-b", Asset: "quote", Chain: "b", Symbol: "Q"},
		},
		Venues: []market.Venue{{ID: "venue"}},
		Pairs:  []market.Pair{{ID: "pair", BaseAsset: "base", QuoteAsset: "quote"}},
		Pools: []market.Pool{
			{ID: "pool-a", Venue: "venue", Chain: "a", Tokens: []market.TokenID{"base-a", "quote-a"}, Adapter: "test"},
			{ID: "pool-b", Venue: "venue", Chain: "b", Tokens: []market.TokenID{"base-b", "quote-b"}, Adapter: "test"},
		},
		Paths: []market.Path{
			{ID: "path-a", Chain: "a", Hops: []market.Hop{{Pool: "pool-a", TokenIn: "base-a", TokenOut: "quote-a"}}},
			{ID: "path-b", Chain: "b", Hops: []market.Hop{{Pool: "pool-b", TokenIn: "base-b", TokenOut: "quote-b"}}},
		},
		Markets: []market.Market{
			{ID: "market-a", Pair: "pair", Chain: "a", Path: "path-a", BaseToken: "base-a", QuoteToken: "quote-a"},
			{ID: "market-b", Pair: "pair", Chain: "b", Path: "path-b", BaseToken: "base-b", QuoteToken: "quote-b"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}
