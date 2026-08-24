package crosschain_test

import (
	"context"
	"crypto/sha256"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
	"github.com/VarozXYZ/vernier/runtime/crosschain"
)

type splitSnapshotData struct{}

func (splitSnapshotData) SnapshotKind() string { return "split-test" }

type splitQuoteSource struct {
	id         market.SourceID
	multiplier int64
	mu         sync.Mutex
	calls      int
}

func (s *splitQuoteSource) ID() market.SourceID { return s.id }
func (s *splitQuoteSource) Quote(_ context.Context, input quoteport.Input) (market.Quote, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	output, _ := market.NewTokenAmount(input.TokenOut, new(big.Int).Mul(input.AmountIn.Units(), big.NewInt(s.multiplier)))
	return market.NewQuote(market.Quote{Source: s.id, Market: input.Snapshot.Metadata().Market,
		SnapshotVersion: input.Snapshot.Metadata().Version, SnapshotHash: input.Snapshot.Metadata().StateHash,
		Purpose: input.Purpose, Mode: market.QuoteModeExactInput, AmountIn: input.AmountIn,
		AmountOut: output, QuotedAt: input.QuotedAt})
}
func (s *splitQuoteSource) count() int { s.mu.Lock(); defer s.mu.Unlock(); return s.calls }

type splitMirror struct{ id market.MarketID }

func (m splitMirror) MarketID() market.MarketID { return m.id }
func (splitMirror) Apply(context.Context, market.MarketEvent) (feedport.ApplyResult, error) {
	return feedport.ApplyResult{}, nil
}
func (splitMirror) Reset(context.Context, market.MarketEvent) (feedport.ApplyResult, error) {
	return feedport.ApplyResult{}, nil
}
func (splitMirror) SetHealth(context.Context, feedport.HealthUpdate) error { return nil }
func (splitMirror) Current() (market.MarketSnapshot, bool)                 { return market.MarketSnapshot{}, false }
func (splitMirror) Health() market.Health                                  { return market.HealthHealthy }

func TestSplitSourceCachesExactAllocationForOneImmutableSnapshot(t *testing.T) {
	now := time.Date(2026, 8, 16, 1, 0, 0, 0, time.UTC)
	candidate := market.Market{ID: "composite", BaseToken: "base", QuoteToken: "quote"}
	directMarket := market.Market{ID: "direct", BaseToken: "base", QuoteToken: "quote"}
	firstMarket := market.Market{ID: "first", BaseToken: "base", QuoteToken: "middle"}
	secondMarket := market.Market{ID: "second", BaseToken: "middle", QuoteToken: "quote"}
	direct := &splitQuoteSource{id: "direct-source", multiplier: 3}
	first := &splitQuoteSource{id: "first-source", multiplier: 1}
	second := &splitQuoteSource{id: "second-source", multiplier: 1}
	route, err := crosschain.NewSplitRoute(crosschain.SplitRouteConfig{Candidate: candidate, Source: "composite-source",
		Intermediate: "middle", Direct: []crosschain.SplitLeg{{Market: directMarket, Source: direct}},
		First:   []crosschain.SplitLeg{{Market: firstMarket, Source: first}},
		Second:  []crosschain.SplitLeg{{Market: secondMarket, Source: second}},
		Mirrors: []feedport.Mirror{splitMirror{id: "direct"}, splitMirror{id: "first"}, splitMirror{id: "second"}},
		Clock:   func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := splitRouteSnapshot(t, now, 1)
	amount, _ := market.NewTokenAmount("base", big.NewInt(100))
	input := quoteport.Input{Snapshot: snapshot, TokenIn: "base", TokenOut: "quote", AmountIn: amount,
		Purpose: market.QuotePurposeLiveDiscovery, QuotedAt: now}
	firstResult, err := route.Source.QuoteExecutable(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	counts := [3]int{direct.count(), first.count(), second.count()}
	input.QuotedAt = now.Add(time.Millisecond)
	cached, err := route.Source.QuoteExecutable(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if got := [3]int{direct.count(), first.count(), second.count()}; got != counts {
		t.Fatalf("curve calls changed on cache hit: %v -> %v", counts, got)
	}
	if cached.Quote.Quality != market.QuoteQualityCachedExact || cached.Quote.QuotedAt != input.QuotedAt ||
		cached.Quote.AmountOut.Units().Cmp(firstResult.Quote.AmountOut.Units()) != 0 {
		t.Fatalf("unexpected cached quote: %+v", cached.Quote)
	}
	input.Snapshot = splitRouteSnapshot(t, now, 2)
	if _, err := route.Source.QuoteExecutable(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if got := [3]int{direct.count(), first.count(), second.count()}; got == counts {
		t.Fatal("new snapshot reused the previous allocation")
	}
}

func splitRouteSnapshot(t *testing.T, now time.Time, version uint64) market.MarketSnapshot {
	t.Helper()
	children := make([]market.MarketSnapshot, 0, 3)
	for _, id := range []market.MarketID{"direct", "first", "second"} {
		hash := sha256.Sum256([]byte(string(id) + big.NewInt(int64(version)).String()))
		snapshot, err := market.NewMarketSnapshot(market.SnapshotMetadata{Market: id, Source: "child", Version: version,
			ReceivedAt: now, AppliedAt: now, Health: market.HealthHealthy, HealthChangedAt: now, StateHash: hash}, splitSnapshotData{})
		if err != nil {
			t.Fatal(err)
		}
		children = append(children, snapshot)
	}
	bundle, err := market.NewSnapshotBundle("composite", children)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := market.NewMarketSnapshot(market.SnapshotMetadata{Market: "composite", Source: "composite-source", Version: version,
		ReceivedAt: now, AppliedAt: now, Health: market.HealthHealthy, HealthChangedAt: now, StateHash: bundle.Hash()}, bundle)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
