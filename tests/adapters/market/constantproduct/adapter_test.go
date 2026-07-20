package constantproduct_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/feed/sourceorder"
	"github.com/VarozXYZ/vernier/adapters/market/constantproduct"
	"github.com/VarozXYZ/vernier/core/marketstate"
	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

func TestMirrorPublishesImmutableSnapshots(t *testing.T) {
	appliedAt := time.Date(2026, 1, 1, 0, 0, 2, 0, time.UTC)
	mirror := newMirror(t, "market", func() time.Time { return appliedAt })
	first := apply(t, mirror, event(t, 1, 1_000_000, 2_000_000))
	second := apply(t, mirror, event(t, 2, 2_000_000, 3_000_000))

	firstState := first.Data().(constantproduct.Snapshot)
	firstReserve := firstState.BaseReserve()
	firstReserve.SetInt64(99)
	if got := firstState.BaseReserve().String(); got != "1000000" {
		t.Fatalf("snapshot state mutated through returned integer: %s", got)
	}
	if first.Metadata().Version != 1 || second.Metadata().Version != 2 {
		t.Fatalf("unexpected versions: %d, %d", first.Metadata().Version, second.Metadata().Version)
	}
}

func TestMirrorIgnoresOlderBlocksWithoutDegrading(t *testing.T) {
	mirror := newMirror(t, "market", func() time.Time { return time.Now().UTC() })
	first := apply(t, mirror, event(t, 20, 1_000_000, 2_000_000))
	result, err := mirror.Apply(context.Background(), event(t, 19, 9_000_000, 9_000_000))
	if err != nil {
		t.Fatal(err)
	}
	if result.Disposition != feedport.ApplyDispositionIgnoredStale {
		t.Fatalf("older block disposition = %q", result.Disposition)
	}
	current, ok := mirror.Current()
	if !ok || current.Metadata().Health != market.HealthHealthy || mirror.Health() != market.HealthHealthy {
		t.Fatal("stale event degraded the mirror")
	}
	if current.Metadata().Version != first.Metadata().Version || current.Metadata().StateHash != first.Metadata().StateHash {
		t.Fatal("stale event changed market state")
	}
}

func TestMirrorAcceptsSameAndNonContiguousLaterBlocks(t *testing.T) {
	mirror := newMirror(t, "market", func() time.Time { return time.Now().UTC() })
	apply(t, mirror, event(t, 20, 1_000_000, 2_000_000))
	sameBlock := apply(t, mirror, event(t, 20, 2_000_000, 3_000_000))
	laterBlock := apply(t, mirror, event(t, 900, 3_000_000, 4_000_000))
	if sameBlock.Metadata().Version != 2 || laterBlock.Metadata().Version != 3 {
		t.Fatalf("accepted events have versions %d and %d", sameBlock.Metadata().Version, laterBlock.Metadata().Version)
	}
}

func TestMirrorUsesArrivalOrderWithoutComparableSourceEvidence(t *testing.T) {
	mirror := newMirror(t, "market", func() time.Time { return time.Now().UTC() })
	first := event(t, 20, 1_000_000, 2_000_000)
	first.Position = market.SourcePosition{}
	apply(t, mirror, first)
	second := event(t, 19, 2_000_000, 3_000_000)
	second.Position = market.SourcePosition{}
	second.SourceTimeKnown = false
	second.SourceTime = time.Time{}
	if got := apply(t, mirror, second).Metadata().Version; got != 2 {
		t.Fatalf("arrival-order update version = %d", got)
	}
}

func TestMirrorIgnoresOlderKnownTimestampWhenPositionsAreUnknown(t *testing.T) {
	mirror := newMirror(t, "market", func() time.Time { return time.Now().UTC() })
	current := event(t, 20, 1_000_000, 2_000_000)
	current.Position = market.SourcePosition{}
	apply(t, mirror, current)
	older := event(t, 21, 9_000_000, 9_000_000)
	older.Position = market.SourcePosition{}
	older.SourceTime = current.SourceTime.Add(-time.Second)
	result, err := mirror.Apply(context.Background(), older)
	if err != nil {
		t.Fatal(err)
	}
	if result.Disposition != feedport.ApplyDispositionIgnoredStale || mirror.Health() != market.HealthHealthy {
		t.Fatalf("older timestamp result = %+v, health = %q", result, mirror.Health())
	}
}

func TestMirrorDisconnectIsExplicitAndFreshDataRecoversHealth(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	mirror := newMirror(t, "market", func() time.Time { return now })
	healthy := apply(t, mirror, event(t, 20, 1_000_000, 2_000_000))
	disconnectedAt := now.Add(time.Second)
	if err := mirror.SetHealth(context.Background(), feedport.HealthUpdate{
		Health: market.HealthDegraded, Reason: "websocket_disconnected", ObservedAt: disconnectedAt,
	}); err != nil {
		t.Fatal(err)
	}
	degraded, _ := mirror.Current()
	if mirror.Health() != market.HealthDegraded || degraded.Metadata().HealthReason != "websocket_disconnected" {
		t.Fatal("disconnect was not reflected in mirror health")
	}
	if degraded.Metadata().Version != healthy.Metadata().Version || degraded.Metadata().StateHash != healthy.Metadata().StateHash {
		t.Fatal("health transition changed market state")
	}
	if healthy.Metadata().Health != market.HealthHealthy {
		t.Fatal("previous immutable snapshot was mutated")
	}
	now = disconnectedAt.Add(time.Second)
	recovered := apply(t, mirror, event(t, 21, 2_000_000, 3_000_000))
	if recovered.Metadata().Health != market.HealthHealthy || recovered.Metadata().HealthReason != "" {
		t.Fatal("fresh event did not restore healthy state")
	}
}

func TestQuoterMatchesConstantProductGoldenVector(t *testing.T) {
	mirror := newMirror(t, "market", func() time.Time {
		return time.Date(2026, 1, 1, 0, 0, 2, 0, time.UTC)
	})
	snapshot := apply(t, mirror, event(t, 1, 1_000_000, 2_000_000))
	quoter, err := constantproduct.NewQuoter("local", market.Market{ID: "market", BaseToken: "base", QuoteToken: "quote"})
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := market.ParseTokenAmount("quote", "100000")
	quote, err := quoter.Quote(context.Background(), quoteport.Input{
		Snapshot: snapshot, TokenIn: "quote", TokenOut: "base", AmountIn: amount,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Date(2026, 1, 1, 0, 0, 3, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := quote.AmountOut.String(); got != "47482" {
		t.Fatalf("unexpected output: got %s, want 47482", got)
	}
	if got := quote.Fees()[0].Amount().String(); got != "300" {
		t.Fatalf("unexpected fee: got %s, want 300", got)
	}
}

func TestQuoterRejectsWrongMarketSnapshot(t *testing.T) {
	mirror := newMirror(t, "other", func() time.Time { return time.Now().UTC() })
	snapshot := apply(t, mirror, eventForMarket(t, "other", 1, 1_000_000, 2_000_000))
	quoter, _ := constantproduct.NewQuoter("local", market.Market{ID: "market", BaseToken: "base", QuoteToken: "quote"})
	amount, _ := market.ParseTokenAmount("quote", "100")
	_, err := quoter.Quote(context.Background(), quoteport.Input{
		Snapshot: snapshot, TokenIn: "quote", TokenOut: "base", AmountIn: amount,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now().UTC(),
	})
	if err == nil {
		t.Fatal("expected wrong-market snapshot to fail")
	}
}

func TestMirrorRejectsZeroReserveUpdateWithoutPanicking(t *testing.T) {
	mirror := newMirror(t, "market", func() time.Time { return time.Now().UTC() })
	event, err := market.NewMarketEvent(market.MarketEvent{
		Market: "market", Source: "feed", Finality: market.FinalityConfirmed,
		ReceivedAt: time.Now().UTC(), Data: constantproduct.ReserveUpdate{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mirror.Apply(context.Background(), event); err == nil {
		t.Fatal("expected zero reserve update to fail")
	}
}

func event(t *testing.T, block uint64, base, quote int64) market.MarketEvent {
	t.Helper()
	return eventForMarket(t, "market", block, base, quote)
}

func eventForMarket(t *testing.T, marketID market.MarketID, block uint64, base, quote int64) market.MarketEvent {
	t.Helper()
	update, err := constantproduct.NewReserveUpdate(big.NewInt(base), big.NewInt(quote), 30)
	if err != nil {
		t.Fatal(err)
	}
	event, err := market.NewMarketEvent(market.MarketEvent{
		Market: marketID, Source: "feed", Position: market.SourcePosition{Kind: sourceorder.BlockPositionKind, Value: block},
		Finality:   market.FinalityConfirmed,
		SourceTime: time.Date(2026, 1, 1, 0, 0, int(block%60), 0, time.UTC), SourceTimeKnown: true,
		ReceivedAt: time.Date(2026, 1, 1, 0, 1, int(block%60), 0, time.UTC), Data: update,
	})
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func newMirror(t *testing.T, marketID market.MarketID, clock marketstate.Clock) *marketstate.Mirror {
	t.Helper()
	mirror, err := marketstate.NewMirror(
		marketID, "feed", constantproduct.Reducer{},
		sourceorder.NewMonotonic(sourceorder.BlockPositionKind, true), clock,
	)
	if err != nil {
		t.Fatal(err)
	}
	return mirror
}

func apply(t *testing.T, mirror *marketstate.Mirror, event market.MarketEvent) market.MarketSnapshot {
	t.Helper()
	result, err := mirror.Apply(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	return result.Snapshot
}
