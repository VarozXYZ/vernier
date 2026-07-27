package strategy_test

import (
	"context"
	"crypto/sha256"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type snapshotData struct{}

func (snapshotData) SnapshotKind() string { return "live-test/v1" }

type blockingSource struct {
	id      market.SourceID
	started chan quoteport.Input
	release <-chan struct{}
	output  *big.Int
}

func (s *blockingSource) ID() market.SourceID { return s.id }
func (s *blockingSource) Quote(ctx context.Context, input quoteport.Input) (market.Quote, error) {
	select {
	case s.started <- input:
	case <-ctx.Done():
		return market.Quote{}, ctx.Err()
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		return market.Quote{}, ctx.Err()
	}
	output, _ := market.NewTokenAmount(input.TokenOut, s.output)
	return market.NewQuote(market.Quote{
		Source: s.id, Market: input.Snapshot.Metadata().Market,
		SnapshotVersion: input.Snapshot.Metadata().Version, SnapshotHash: input.Snapshot.Metadata().StateHash,
		Purpose: input.Purpose, Mode: market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
		AmountIn: input.AmountIn, AmountOut: output, QuotedAt: input.QuotedAt,
	})
}

func TestLiveEvaluatorFixesIndependentInputsAndQuotesConcurrently(t *testing.T) {
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	registry, setup, snapshots := liveRegistry(t, now)
	release := make(chan struct{})
	started := make(chan quoteport.Input, 2)
	buySource := &blockingSource{id: "buy-source", started: started, release: release, output: big.NewInt(14_550)}
	sellSource := &blockingSource{id: "sell-source", started: started, release: release, output: big.NewInt(100_100_000)}
	evaluator, err := strategy.NewLiveEvaluator(strategy.LiveConfig{
		Setup: setup, Registry: registry,
		Sources: map[market.MarketID]quoteport.Source{"market-a": buySource, "market-b": sellSource},
		Clock:   func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	valuation, err := arbitrage.NewValuationSnapshot(1, "base", "quote", big.NewRat(100, 14_500), 2, now)
	if err != nil {
		t.Fatal(err)
	}
	notional, _ := market.NewAssetQuantity("quote", big.NewRat(100, 1))
	cost, _ := market.NewAssetQuantity("quote", big.NewRat(1, 100))
	threshold, _ := market.NewAssetQuantity("quote", big.NewRat(1, 5))
	maximumCost, _ := market.NewAssetQuantity("quote", big.NewRat(1, 1))
	maximumExposure, _ := market.NewAssetQuantity("base", big.NewRat(100, 1))
	request := strategy.LiveEvaluationRequest{
		ID: "evaluation-1", Direction: arbitrage.Direction{BuyMarket: "market-a", SellMarket: "market-b"},
		Snapshots: snapshots, Notional: notional, Valuation: valuation, Cost: cost,
		Threshold: threshold, MaximumCost: maximumCost,
		MaximumBaseExposure: maximumExposure, TriggeredAt: now,
	}
	result := make(chan arbitrage.LiveOpportunity, 1)
	failures := make(chan error, 1)
	go func() {
		opportunity, evaluateErr := evaluator.Evaluate(context.Background(), request)
		if evaluateErr != nil {
			failures <- evaluateErr
			return
		}
		result <- opportunity
	}()
	first := <-started
	second := <-started
	if first.TokenIn == second.TokenIn {
		t.Fatalf("expected buy and sell to start independently, got duplicate token %q", first.TokenIn)
	}
	var buyInput, sellInput market.TokenAmount
	for _, input := range []quoteport.Input{first, second} {
		if input.TokenIn == "quote-a" {
			buyInput = input.AmountIn
		} else {
			sellInput = input.AmountIn
		}
	}
	if buyInput.String() != "100000000" {
		t.Fatalf("buy input = %s", buyInput.String())
	}
	if sellInput.String() != "14500" {
		t.Fatalf("sell input = %s; it must come from cached valuation, not buy output", sellInput.String())
	}
	close(release)
	select {
	case err := <-failures:
		t.Fatal(err)
	case opportunity := <-result:
		if opportunity.SellQuote.AmountIn.String() != "14500" || opportunity.BuyQuote.AmountOut.String() != "14550" {
			t.Fatalf("dependent sell regression: sell=%s buy_out=%s", opportunity.SellQuote.AmountIn, opportunity.BuyQuote.AmountOut)
		}
		if got := opportunity.QuoteDelta.Decimal(6); got != "0.100000" {
			t.Fatalf("quote delta = %s", got)
		}
		if got := opportunity.BaseDelta.Decimal(0); got != "50" {
			t.Fatalf("base delta = %s", got)
		}
		if got := opportunity.GrossPnL.Decimal(6); got != "0.444828" {
			t.Fatalf("gross PnL = %s", got)
		}
		if !opportunity.Profitable() {
			t.Fatal("expected profitable opportunity")
		}
		request.MaximumBaseExposure, _ = market.NewAssetQuantity("base", big.NewRat(10, 1))
		blocked, err := evaluator.Value(request, opportunity.BuyQuote, opportunity.SellQuote, now)
		if err != nil {
			t.Fatal(err)
		}
		if blocked.RiskBlock != "base_exposure_limit" || blocked.Profitable() {
			t.Fatalf("exposure limit did not block execution: %+v", blocked)
		}
	}
}

func TestBaseValuationCacheAveragesLatestObservationsWithoutTTL(t *testing.T) {
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	cache, err := strategy.NewBaseValuationCache("base", "quote")
	if err != nil {
		t.Fatal(err)
	}
	baseA := market.Token{ID: "base-a", Asset: "base", Chain: "a", Decimals: 0, Symbol: "BASE"}
	quoteA := market.Token{ID: "quote-a", Asset: "quote", Chain: "a", Decimals: 0, Symbol: "QUOTE"}
	baseB := market.Token{ID: "base-b", Asset: "base", Chain: "b", Decimals: 0, Symbol: "BASE"}
	quoteB := market.Token{ID: "quote-b", Asset: "quote", Chain: "b", Decimals: 0, Symbol: "QUOTE"}
	observeQuote(t, cache, "a/buy", quoteA, baseA, 100, 20, now)
	observeQuote(t, cache, "b/sell", baseB, quoteB, 10, 60, now)
	snapshot, err := cache.Snapshot(now.Add(24 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	// Prices are 5 and 6 quote/base; the mean remains valid regardless of age.
	if snapshot.Price().Cmp(big.NewRat(11, 2)) != 0 {
		t.Fatalf("price = %s", snapshot.Price())
	}
	if snapshot.Observations() != 2 {
		t.Fatalf("observations = %d", snapshot.Observations())
	}
}

func observeQuote(t *testing.T, cache *strategy.BaseValuationCache, key string, tokenIn, tokenOut market.Token, in, out int64, at time.Time) {
	t.Helper()
	amountIn, _ := market.NewTokenAmount(tokenIn.ID, big.NewInt(in))
	amountOut, _ := market.NewTokenAmount(tokenOut.ID, big.NewInt(out))
	quote, err := market.NewQuote(market.Quote{
		Source: market.SourceID(key), Market: market.MarketID(key), SnapshotVersion: 1,
		Purpose: market.QuotePurposeLiveDiscovery, Mode: market.QuoteModeExactInput,
		Quality: market.QuoteQualityExact, AmountIn: amountIn, AmountOut: amountOut, QuotedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Observe(key, quote, tokenIn, tokenOut, at); err != nil {
		t.Fatal(err)
	}
}

func liveRegistry(t *testing.T, now time.Time) (*market.Registry, arbitrage.ArbitrageSetup, map[market.MarketID]market.MarketSnapshot) {
	t.Helper()
	catalog := market.Catalog{
		Chains: []market.Chain{{ID: "a"}, {ID: "b"}},
		Assets: []market.Asset{{ID: "base", Symbol: "BASE"}, {ID: "quote", Symbol: "QUOTE"}},
		Tokens: []market.Token{
			{ID: "base-a", Asset: "base", Chain: "a", Decimals: 0, Symbol: "BASE"},
			{ID: "quote-a", Asset: "quote", Chain: "a", Decimals: 6, Symbol: "QUOTE"},
			{ID: "base-b", Asset: "base", Chain: "b", Decimals: 0, Symbol: "BASE"},
			{ID: "quote-b", Asset: "quote", Chain: "b", Decimals: 6, Symbol: "QUOTE"},
		},
		Venues: []market.Venue{{ID: "venue-a"}, {ID: "venue-b"}},
		Pairs:  []market.Pair{{ID: "pair", BaseAsset: "base", QuoteAsset: "quote"}},
		Pools: []market.Pool{
			{ID: "pool-a", Venue: "venue-a", Chain: "a", Tokens: []market.TokenID{"base-a", "quote-a"}, Adapter: "test"},
			{ID: "pool-b", Venue: "venue-b", Chain: "b", Tokens: []market.TokenID{"base-b", "quote-b"}, Adapter: "test"},
		},
		Paths: []market.Path{
			{ID: "path-a", Chain: "a", Hops: []market.Hop{{Pool: "pool-a", TokenIn: "base-a", TokenOut: "quote-a"}}},
			{ID: "path-b", Chain: "b", Hops: []market.Hop{{Pool: "pool-b", TokenIn: "base-b", TokenOut: "quote-b"}}},
		},
		Markets: []market.Market{
			{ID: "market-a", Pair: "pair", Chain: "a", Path: "path-a", BaseToken: "base-a", QuoteToken: "quote-a"},
			{ID: "market-b", Pair: "pair", Chain: "b", Path: "path-b", BaseToken: "base-b", QuoteToken: "quote-b"},
		},
	}
	registry, err := market.NewRegistry(catalog)
	if err != nil {
		t.Fatal(err)
	}
	setup, err := arbitrage.NewArbitrageSetup("setup", "pair", []market.MarketID{"market-a", "market-b"}, registry)
	if err != nil {
		t.Fatal(err)
	}
	snapshots := make(map[market.MarketID]market.MarketSnapshot)
	for index, id := range []market.MarketID{"market-a", "market-b"} {
		hash := sha256.Sum256([]byte(id))
		snapshot, snapshotErr := market.NewMarketSnapshot(market.SnapshotMetadata{
			Market: id, Source: market.SourceID(id + "/source"), Version: uint64(index + 1),
			ReceivedAt: now, AppliedAt: now, Health: market.HealthHealthy, HealthChangedAt: now, StateHash: hash,
		}, snapshotData{})
		if snapshotErr != nil {
			t.Fatal(snapshotErr)
		}
		snapshots[id] = snapshot
	}
	return registry, setup, snapshots
}

var _ quoteport.Source = (*blockingSource)(nil)
