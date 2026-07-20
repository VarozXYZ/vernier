package constantproduct_test

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/market/constantproduct"
	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

func TestMirrorPublishesImmutableSnapshots(t *testing.T) {
	appliedAt := time.Date(2026, 1, 1, 0, 0, 2, 0, time.UTC)
	mirror, err := constantproduct.NewMirror("market", "feed", func() time.Time { return appliedAt })
	if err != nil {
		t.Fatal(err)
	}
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

func TestMirrorMarksCurrentSnapshotDegradedOnSequenceViolation(t *testing.T) {
	mirror, _ := constantproduct.NewMirror("market", "feed", func() time.Time { return time.Now().UTC() })
	apply(t, mirror, event(t, 1, 1_000_000, 2_000_000))
	_, err := mirror.Apply(context.Background(), event(t, 3, 1_000_000, 2_000_000))
	var violation feedport.SequenceViolation
	if !errors.As(err, &violation) || !violation.IsGap() {
		t.Fatalf("expected sequence gap, got %v", err)
	}
	current, ok := mirror.Current()
	if !ok || current.Metadata().Health != market.HealthDegraded || mirror.Health() != market.HealthDegraded {
		t.Fatal("mirror did not expose degraded health")
	}
	if current.Metadata().Version != 1 {
		t.Fatal("sequence violation changed market-state version")
	}
}

func TestQuoterMatchesConstantProductGoldenVector(t *testing.T) {
	mirror, _ := constantproduct.NewMirror("market", "feed", func() time.Time {
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
	if got := quote.Fee.String(); got != "300" {
		t.Fatalf("unexpected fee: got %s, want 300", got)
	}
}

func TestQuoterRejectsWrongMarketSnapshot(t *testing.T) {
	mirror, _ := constantproduct.NewMirror("other", "feed", func() time.Time { return time.Now().UTC() })
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
	mirror, _ := constantproduct.NewMirror("market", "feed", func() time.Time { return time.Now().UTC() })
	event, err := market.NewMarketEvent(market.MarketEvent{
		Market: "market", Source: "feed", Sequence: 1, Finality: market.FinalityConfirmed,
		ReceivedAt: time.Now().UTC(), Data: constantproduct.ReserveUpdate{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mirror.Apply(context.Background(), event); err == nil {
		t.Fatal("expected zero reserve update to fail")
	}
}

func event(t *testing.T, sequence uint64, base, quote int64) market.MarketEvent {
	t.Helper()
	return eventForMarket(t, "market", sequence, base, quote)
}

func eventForMarket(t *testing.T, marketID market.MarketID, sequence uint64, base, quote int64) market.MarketEvent {
	t.Helper()
	update, err := constantproduct.NewReserveUpdate(big.NewInt(base), big.NewInt(quote), 30)
	if err != nil {
		t.Fatal(err)
	}
	event, err := market.NewMarketEvent(market.MarketEvent{
		Market: marketID, Source: "feed", Sequence: sequence, Finality: market.FinalityConfirmed,
		SourceTime: time.Date(2026, 1, 1, 0, 0, int(sequence), 0, time.UTC), SourceTimeKnown: true,
		ReceivedAt: time.Date(2026, 1, 1, 0, 0, int(sequence), int(time.Millisecond), time.UTC), Data: update,
	})
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func apply(t *testing.T, mirror *constantproduct.Mirror, event market.MarketEvent) market.MarketSnapshot {
	t.Helper()
	snapshot, err := mirror.Apply(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
