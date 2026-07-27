package strategy_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/feed/sourceorder"
	"github.com/VarozXYZ/vernier/adapters/market/constantproduct"
	"github.com/VarozXYZ/vernier/core/marketstate"
	"github.com/VarozXYZ/vernier/core/sizing"
	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

func TestTwoMarketCachesUnchangedPoolQuotesAndInvalidatesChangedPool(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 10, 0, time.UTC)
	registry := strategyRegistry(t)
	setup, err := arbitrage.NewArbitrageSetup("setup", "pair", []market.MarketID{"market-a", "market-b"}, registry)
	if err != nil {
		t.Fatal(err)
	}
	grid, err := sizing.NewGrid([]market.AssetQuantity{quantity(t, "10"), quantity(t, "20")})
	if err != nil {
		t.Fatal(err)
	}
	threshold := quantity(t, "1")
	marketA, _ := registry.Market("market-a")
	marketB, _ := registry.Market("market-b")
	quoterA, _ := constantproduct.NewQuoter("local-a", marketA)
	quoterB, _ := constantproduct.NewQuoter("local-b", marketB)
	sourceA := &countingSource{delegate: quoterA, exact: quoterA}
	sourceB := &countingSource{delegate: quoterB, exact: quoterB}
	candidate, err := strategy.NewTwoMarket(strategy.TwoMarketConfig{
		ID: "strategy", Setup: setup, Registry: registry,
		Sources: map[market.MarketID]quoteport.Source{"market-a": sourceA, "market-b": sourceB},
		Grid:    grid, Threshold: threshold, Clock: func() time.Time { return now.Add(5 * time.Millisecond) }, SizingAsset: strategy.SizingAssetQuote,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshotA := strategySnapshot(t, "market-a", "1000000000", "1800000000", now)
	snapshotB := strategySnapshot(t, "market-b", "100000000000", "2200000000", now)
	cost := arbitrage.CostSnapshot{ID: "fixed", Amount: quantity(t, "0.5"), CapturedAt: now}
	evaluate := func(id string, snapshots []market.MarketSnapshot) []arbitrage.Opportunity {
		evaluation, evalErr := arbitrage.NewEvaluation(arbitrage.EvaluationID(id), "run", "strategy", "config-hash", snapshots, cost, now, now)
		if evalErr != nil {
			t.Fatal(evalErr)
		}
		result, evalErr := candidate.Evaluate(context.Background(), evaluation)
		if evalErr != nil {
			t.Fatal(evalErr)
		}
		return result
	}

	evaluate("first", []market.MarketSnapshot{snapshotA, snapshotB})
	firstA, firstB := sourceA.totalCalls(), sourceB.totalCalls()
	if firstA == 0 || firstB == 0 {
		t.Fatal("initial evaluation did not quote both markets")
	}
	initial := evaluate("quote-sized", []market.MarketSnapshot{snapshotA, snapshotB})
	for _, opportunity := range initial {
		for _, candidate := range opportunity.Candidates {
			if candidate.Size.Asset() != "quote" || candidate.BuyQuote.Mode != market.QuoteModeExactInput {
				t.Fatalf("quote sizing did not use quote budget and exact-input buy: size=%s mode=%s", candidate.Size, candidate.BuyQuote.Mode)
			}
		}
	}
	evaluate("same", []market.MarketSnapshot{snapshotA, snapshotB})
	if sourceA.totalCalls() != firstA || sourceB.totalCalls() != firstB {
		t.Fatal("unchanged snapshots caused quote recomputation")
	}

	nextVersion := sameStateNextVersion(t, snapshotA, now)
	results := evaluate("same-state-new-version", []market.MarketSnapshot{nextVersion, snapshotB})
	if sourceA.totalCalls() != firstA || sourceB.totalCalls() != firstB {
		t.Fatal("same economic state caused quote recomputation")
	}
	for _, opportunity := range results {
		for _, candidate := range opportunity.Candidates {
			if candidate.BuyQuote.Market == "market-a" && candidate.BuyQuote.SnapshotVersion != nextVersion.Metadata().Version {
				t.Fatalf("cached quote was not rebound to current snapshot version: got %d want %d", candidate.BuyQuote.SnapshotVersion, nextVersion.Metadata().Version)
			}
		}
	}

	changed := strategySnapshot(t, "market-a", "1100000000", "1800000000", now)
	evaluate("changed", []market.MarketSnapshot{changed, snapshotB})
	currentA := sourceA.totalCalls()
	currentB := sourceB.totalCalls()
	if currentA <= firstA || currentB <= firstB || currentB >= firstB*2 {
		t.Fatalf("cache did not reuse the unchanged market's fixed-budget leg: A=%d B=%d firstA=%d firstB=%d", currentA, currentB, firstA, firstB)
	}
}

func TestTwoMarketDoesNotCacheOptedOutRemoteQuotes(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 10, 0, time.UTC)
	registry := strategyRegistry(t)
	setup, err := arbitrage.NewArbitrageSetup("setup", "pair", []market.MarketID{"market-a", "market-b"}, registry)
	if err != nil {
		t.Fatal(err)
	}
	grid, err := sizing.NewGrid([]market.AssetQuantity{quantity(t, "100")})
	if err != nil {
		t.Fatal(err)
	}
	marketA, _ := registry.Market("market-a")
	marketB, _ := registry.Market("market-b")
	quoterA, _ := constantproduct.NewQuoter("local-a", marketA)
	quoterB, _ := constantproduct.NewQuoter("remote-b", marketB)
	local := &countingSource{delegate: quoterA, exact: quoterA}
	remoteCounter := &countingSource{delegate: quoterB, exact: quoterB}
	remote := &uncachedCountingSource{countingSource: remoteCounter}
	candidate, err := strategy.NewTwoMarket(strategy.TwoMarketConfig{
		ID: "strategy", Setup: setup, Registry: registry,
		Sources: map[market.MarketID]quoteport.Source{"market-a": local, "market-b": remote},
		Grid:    grid, Threshold: quantity(t, "0"), Clock: func() time.Time { return now },
		SizingAsset: strategy.SizingAssetQuote,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshots := []market.MarketSnapshot{
		strategySnapshot(t, "market-a", "1000000000", "1800000000", now),
		strategySnapshot(t, "market-b", "100000000000", "2200000000", now),
	}
	evaluate := func(id arbitrage.EvaluationID) {
		evaluation, evalErr := arbitrage.NewEvaluation(id, "run", "strategy", "config-hash", snapshots,
			arbitrage.CostSnapshot{ID: "zero", Amount: quantity(t, "0"), CapturedAt: now}, now, now)
		if evalErr != nil {
			t.Fatal(evalErr)
		}
		if _, evalErr = candidate.Evaluate(context.Background(), evaluation); evalErr != nil {
			t.Fatal(evalErr)
		}
	}
	evaluate("first")
	firstRemoteCalls := remoteCounter.totalCalls()
	if firstRemoteCalls == 0 {
		t.Fatal("initial evaluation did not invoke remote source")
	}
	evaluate("second")
	if calls := remoteCounter.totalCalls(); calls != firstRemoteCalls*2 {
		t.Fatalf("unchanged evaluation reused opted-out remote quotes: first=%d total=%d", firstRemoteCalls, calls)
	}
}

type countingSource struct {
	delegate    quoteport.Source
	exact       quoteport.ExactOutputSource
	inputCalls  atomic.Int64
	outputCalls atomic.Int64
}

func (s *countingSource) ID() market.SourceID { return s.delegate.ID() }

func (s *countingSource) Quote(ctx context.Context, input quoteport.Input) (market.Quote, error) {
	s.inputCalls.Add(1)
	return s.delegate.Quote(ctx, input)
}

func (s *countingSource) QuoteExactOutput(ctx context.Context, input quoteport.ExactOutputInput) (market.Quote, error) {
	s.outputCalls.Add(1)
	return s.exact.QuoteExactOutput(ctx, input)
}

func (s *countingSource) totalCalls() int64 {
	return s.inputCalls.Load() + s.outputCalls.Load()
}

type uncachedCountingSource struct {
	*countingSource
}

func (*uncachedCountingSource) CacheQuotes() bool { return false }

func sameStateNextVersion(t *testing.T, current market.MarketSnapshot, now time.Time) market.MarketSnapshot {
	t.Helper()
	state := current.Data().(constantproduct.Snapshot)
	update, err := constantproduct.NewReserveUpdate(state.BaseReserve(), state.QuoteReserve(), state.FeeBPS())
	if err != nil {
		t.Fatal(err)
	}
	mirror, err := marketstate.NewMirror(current.Metadata().Market, current.Metadata().Source, constantproduct.Reducer{}, sourceorder.NewMonotonic(sourceorder.BlockPositionKind, false), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	for block := uint64(1); block <= 2; block++ {
		event, eventErr := market.NewMarketEvent(market.MarketEvent{
			Market: current.Metadata().Market, Source: current.Metadata().Source,
			Position: market.SourcePosition{Kind: sourceorder.BlockPositionKind, Value: block},
			Finality: market.FinalityConfirmed, ReceivedAt: now, Data: update,
		})
		if eventErr != nil {
			t.Fatal(eventErr)
		}
		result, applyErr := mirror.Apply(context.Background(), event)
		if applyErr != nil {
			t.Fatal(applyErr)
		}
		if block == 2 {
			return result.Snapshot
		}
	}
	panic("unreachable")
}

var _ quoteport.ExactOutputSource = (*countingSource)(nil)
var _ quoteport.CachePolicy = (*uncachedCountingSource)(nil)
