package strategy_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/market/constantproduct"
	"github.com/VarozXYZ/vernier/core/sizing"
	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

func TestTwoMarketEvaluatesBothDirectionsWithExactDecimals(t *testing.T) {
	fixture := newStrategyFixture(t, "1", "0.5")
	opportunities, err := fixture.strategy.Evaluate(context.Background(), fixture.evaluation(t, fixture.snapshots, fixture.now, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(opportunities) != 2 {
		t.Fatalf("expected two directions, got %d", len(opportunities))
	}
	if opportunities[0].Classification != arbitrage.ClassificationPolicyQualified {
		t.Fatalf("expected qualified A->B opportunity, got %s", opportunities[0].Classification)
	}
	if opportunities[1].Classification != arbitrage.ClassificationNoSpread {
		t.Fatalf("expected no-spread B->A opportunity, got %s", opportunities[1].Classification)
	}
	selected := opportunities[0].Candidates[opportunities[0].SelectedIndex]
	converted := new(big.Int).Mul(selected.BuyQuote.AmountOut.Units(), big.NewInt(100))
	if selected.SellQuote.AmountIn.Units().Cmp(converted) != 0 {
		t.Fatalf("cross-decimal conversion lost value: buy=%s sell=%s", selected.BuyQuote.AmountOut, selected.SellQuote.AmountIn)
	}
	if selected.BuyQuote.SnapshotVersion != 1 || selected.SellQuote.SnapshotVersion != 1 {
		t.Fatal("candidate did not preserve fixed snapshot versions")
	}
	for _, opportunity := range opportunities {
		if opportunity.Classification == arbitrage.ClassificationExecutable || opportunity.Classification == arbitrage.ClassificationModeledCandidate {
			t.Fatalf("research kernel claimed unsupported execution classification %q", opportunity.Classification)
		}
	}
}

func TestTwoMarketSeparatesEconomicClassifications(t *testing.T) {
	tests := []struct {
		name      string
		threshold string
		cost      string
		want      arbitrage.Classification
	}{
		{name: "cost consumes profit", threshold: "1", cost: "5", want: arbitrage.ClassificationObservedSpread},
		{name: "below threshold", threshold: "4", cost: "0.5", want: arbitrage.ClassificationEconomic},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newStrategyFixture(t, test.threshold, test.cost)
			opportunities, err := fixture.strategy.Evaluate(context.Background(), fixture.evaluation(t, fixture.snapshots, fixture.now, time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if got := opportunities[0].Classification; got != test.want {
				t.Fatalf("classification: got %s, want %s", got, test.want)
			}
		})
	}
}

func TestTwoMarketMarksStaleAndDegradedSnapshotsUnclassifiable(t *testing.T) {
	fixture := newStrategyFixture(t, "1", "0.5")
	staleEvaluation := fixture.evaluation(t, fixture.snapshots, fixture.now.Add(10*time.Second), time.Second)
	stale, err := fixture.strategy.Evaluate(context.Background(), staleEvaluation)
	if err != nil {
		t.Fatal(err)
	}
	if stale[0].Classification != arbitrage.ClassificationUnclassifiable || stale[0].Reasons[0] != "stale_market_snapshot" {
		t.Fatalf("unexpected stale result: %+v", stale[0])
	}

	degraded := fixture.snapshots[0].Metadata()
	degraded.Health = market.HealthDegraded
	degradedSnapshot, err := market.NewMarketSnapshot(degraded, fixture.snapshots[0].Data())
	if err != nil {
		t.Fatal(err)
	}
	snapshots := []market.MarketSnapshot{degradedSnapshot, fixture.snapshots[1]}
	results, err := fixture.strategy.Evaluate(context.Background(), fixture.evaluation(t, snapshots, fixture.now, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Classification != arbitrage.ClassificationUnclassifiable || results[0].Reasons[0] != "degraded_market_snapshot" {
		t.Fatalf("unexpected degraded result: %+v", results[0])
	}
}

type strategyFixture struct {
	registry  *market.Registry
	strategy  *strategy.TwoMarketCrossChainArbitrage
	snapshots []market.MarketSnapshot
	cost      arbitrage.CostSnapshot
	now       time.Time
}

func newStrategyFixture(t *testing.T, thresholdText, costText string) strategyFixture {
	t.Helper()
	now := time.Date(2026, 1, 1, 0, 0, 10, 0, time.UTC)
	registry := strategyRegistry(t)
	setup, err := arbitrage.NewArbitrageSetup("setup", "pair", []market.MarketID{"market-a", "market-b"}, registry)
	if err != nil {
		t.Fatal(err)
	}
	gridValues := []market.AssetQuantity{quantity(t, "10"), quantity(t, "20")}
	grid, err := sizing.NewGrid(gridValues)
	if err != nil {
		t.Fatal(err)
	}
	threshold := quantity(t, thresholdText)
	cost := quantity(t, costText)
	marketA, _ := registry.Market("market-a")
	marketB, _ := registry.Market("market-b")
	snapshotA := strategySnapshot(t, marketA.ID, "1000000000", "1800000000", now)
	snapshotB := strategySnapshot(t, marketB.ID, "100000000000", "2200000000", now)
	quoterA, _ := constantproduct.NewQuoter("local-a", marketA)
	quoterB, _ := constantproduct.NewQuoter("local-b", marketB)
	candidate, err := strategy.NewTwoMarket(strategy.TwoMarketConfig{
		ID: "strategy", Setup: setup, Registry: registry,
		Sources: map[market.MarketID]quoteport.Source{"market-a": quoterA, "market-b": quoterB},
		Grid:    grid, Threshold: threshold, Clock: func() time.Time { return now.Add(5 * time.Millisecond) },
	})
	if err != nil {
		t.Fatal(err)
	}
	return strategyFixture{
		registry: registry, strategy: candidate, snapshots: []market.MarketSnapshot{snapshotA, snapshotB},
		cost: arbitrage.CostSnapshot{ID: "fixed", Amount: cost, CapturedAt: now}, now: now,
	}
}

func (f strategyFixture) evaluation(t *testing.T, snapshots []market.MarketSnapshot, started time.Time, maxAge time.Duration) arbitrage.Evaluation {
	t.Helper()
	evaluation, err := arbitrage.NewEvaluation(
		"evaluation", "run", "strategy", "config-hash", snapshots, f.cost,
		f.now, started, maxAge,
	)
	if err != nil {
		t.Fatal(err)
	}
	return evaluation
}

func quantity(t *testing.T, text string) market.AssetQuantity {
	t.Helper()
	value, err := market.ParseAssetQuantity("quote", text)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func strategySnapshot(t *testing.T, marketID market.MarketID, base, quote string, now time.Time) market.MarketSnapshot {
	t.Helper()
	baseReserve, _ := new(big.Int).SetString(base, 10)
	quoteReserve, _ := new(big.Int).SetString(quote, 10)
	update, err := constantproduct.NewReserveUpdate(baseReserve, quoteReserve, 30)
	if err != nil {
		t.Fatal(err)
	}
	mirror, err := constantproduct.NewMirror(marketID, "feed", func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	event, err := market.NewMarketEvent(market.MarketEvent{
		Market: marketID, Source: "feed", Sequence: 1, Finality: market.FinalityConfirmed,
		SourceTime: now.Add(-time.Millisecond), SourceTimeKnown: true, ReceivedAt: now, Data: update,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := mirror.Apply(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func strategyRegistry(t *testing.T) *market.Registry {
	t.Helper()
	registry, err := market.NewRegistry(market.Catalog{
		Chains: []market.Chain{{ID: "chain-a"}, {ID: "chain-b"}},
		Assets: []market.Asset{{ID: "base", Symbol: "BASE"}, {ID: "quote", Symbol: "QUOTE"}},
		Tokens: []market.Token{
			{ID: "base-a", Asset: "base", Chain: "chain-a", Decimals: 6, Symbol: "BASE"},
			{ID: "quote-a", Asset: "quote", Chain: "chain-a", Decimals: 6, Symbol: "QUOTE"},
			{ID: "base-b", Asset: "base", Chain: "chain-b", Decimals: 8, Symbol: "BASE"},
			{ID: "quote-b", Asset: "quote", Chain: "chain-b", Decimals: 6, Symbol: "QUOTE"},
		},
		Venues: []market.Venue{{ID: "venue"}},
		Pairs:  []market.Pair{{ID: "pair", BaseAsset: "base", QuoteAsset: "quote"}},
		Pools: []market.Pool{
			{ID: "pool-a", Venue: "venue", Chain: "chain-a", Tokens: []market.TokenID{"base-a", "quote-a"}, Adapter: "constant_product"},
			{ID: "pool-b", Venue: "venue", Chain: "chain-b", Tokens: []market.TokenID{"base-b", "quote-b"}, Adapter: "constant_product"},
		},
		Paths: []market.Path{
			{ID: "path-a", Chain: "chain-a", Hops: []market.Hop{{Pool: "pool-a", TokenIn: "base-a", TokenOut: "quote-a"}}},
			{ID: "path-b", Chain: "chain-b", Hops: []market.Hop{{Pool: "pool-b", TokenIn: "base-b", TokenOut: "quote-b"}}},
		},
		Markets: []market.Market{
			{ID: "market-a", Pair: "pair", Chain: "chain-a", Path: "path-a", BaseToken: "base-a", QuoteToken: "quote-a"},
			{ID: "market-b", Pair: "pair", Chain: "chain-b", Path: "path-b", BaseToken: "base-b", QuoteToken: "quote-b"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}
