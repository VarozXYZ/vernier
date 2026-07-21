package strategy_test

import (
	"context"
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
	grid, err := sizing.NewGrid([]market.AssetQuantity{baseQuantity(t, "10"), baseQuantity(t, "20")})
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
		Grid:    grid, Threshold: threshold, Clock: func() time.Time { return now.Add(5 * time.Millisecond) },
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
	firstA, firstB := sourceA.inputCalls+sourceA.outputCalls, sourceB.inputCalls+sourceB.outputCalls
	if firstA == 0 || firstB == 0 {
		t.Fatal("initial evaluation did not quote both markets")
	}
	evaluate("same", []market.MarketSnapshot{snapshotA, snapshotB})
	if sourceA.inputCalls+sourceA.outputCalls != firstA || sourceB.inputCalls+sourceB.outputCalls != firstB {
		t.Fatal("unchanged snapshots caused quote recomputation")
	}

	nextVersion := sameStateNextVersion(t, snapshotA, now)
	results := evaluate("same-state-new-version", []market.MarketSnapshot{nextVersion, snapshotB})
	if sourceA.inputCalls+sourceA.outputCalls != firstA || sourceB.inputCalls+sourceB.outputCalls != firstB {
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
	if sourceA.inputCalls+sourceA.outputCalls <= firstA || sourceB.inputCalls+sourceB.outputCalls != firstB {
		t.Fatal("cache did not invalidate only the changed market")
	}
}

type countingSource struct {
	delegate    quoteport.Source
	exact       quoteport.ExactOutputSource
	inputCalls  int
	outputCalls int
}

func (s *countingSource) ID() market.SourceID { return s.delegate.ID() }

func (s *countingSource) Quote(ctx context.Context, input quoteport.Input) (market.Quote, error) {
	s.inputCalls++
	return s.delegate.Quote(ctx, input)
}

func (s *countingSource) QuoteExactOutput(ctx context.Context, input quoteport.ExactOutputInput) (market.Quote, error) {
	s.outputCalls++
	return s.exact.QuoteExactOutput(ctx, input)
}

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
