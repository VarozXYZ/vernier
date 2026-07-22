package route_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	routequote "github.com/VarozXYZ/vernier/adapters/quote/route"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type data struct{}

func (data) SnapshotKind() string { return "test" }

type countingSource struct {
	id    market.SourceID
	calls atomic.Int32
}

func (s *countingSource) ID() market.SourceID { return s.id }
func (s *countingSource) Quote(_ context.Context, input quoteport.Input) (market.Quote, error) {
	s.calls.Add(1)
	out, _ := market.NewTokenAmount(input.TokenOut, new(big.Int).Add(input.AmountIn.Units(), big.NewInt(1)))
	return market.NewQuote(market.Quote{Source: s.id, Market: input.Snapshot.Metadata().Market, SnapshotVersion: input.Snapshot.Metadata().Version, SnapshotHash: input.Snapshot.Metadata().StateHash, Purpose: input.Purpose, Mode: market.QuoteModeExactInput, AmountIn: input.AmountIn, AmountOut: out, QuotedAt: input.QuotedAt})
}

func (s *countingSource) QuoteExactOutput(_ context.Context, input quoteport.ExactOutputInput) (market.Quote, error) {
	s.calls.Add(1)
	in, _ := market.NewTokenAmount(input.TokenIn, new(big.Int).Add(input.AmountOut.Units(), big.NewInt(1)))
	return market.NewQuote(market.Quote{Source: s.id, Market: input.Snapshot.Metadata().Market, SnapshotVersion: input.Snapshot.Metadata().Version, SnapshotHash: input.Snapshot.Metadata().StateHash, Purpose: input.Purpose, Mode: market.QuoteModeExactOutput, AmountIn: in, AmountOut: input.AmountOut, QuotedAt: input.QuotedAt})
}

func TestRouteCacheReusesUnchangedHop(t *testing.T) {
	first := &countingSource{id: "first"}
	second := &countingSource{id: "second"}
	source, err := routequote.New("route-local", market.Market{ID: "route", BaseToken: "base", QuoteToken: "quote"}, []routequote.Hop{{Market: "hop1", In: "base", Out: "mid", Source: first}, {Market: "hop2", In: "mid", Out: "quote", Source: second}})
	if err != nil {
		t.Fatal(err)
	}
	firstSnapshot := snapshot(t, "hop1", 1, 1)
	secondSnapshot := snapshot(t, "hop2", 1, 2)
	input := quoteport.Input{Snapshot: routeSnapshot(t, firstSnapshot, secondSnapshot), TokenIn: "base", TokenOut: "quote", AmountIn: mustAmount("base", 10), Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()}
	if _, err := source.Quote(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Quote(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if first.calls.Load() != 1 || second.calls.Load() != 1 {
		t.Fatalf("cache calls first=%d second=%d", first.calls.Load(), second.calls.Load())
	}
	trace := source.LastTiming()
	if !trace.Cached || len(trace.Hops) != 2 || !trace.Hops[0].Cached || !trace.Hops[1].Cached {
		t.Fatalf("cached route did not preserve hop trace: %+v", trace)
	}
	changedSecond := snapshot(t, "hop2", 2, 3)
	input.Snapshot = routeSnapshot(t, firstSnapshot, changedSecond)
	if _, err := source.Quote(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if first.calls.Load() != 1 || second.calls.Load() != 2 {
		t.Fatalf("per-hop invalidation calls first=%d second=%d", first.calls.Load(), second.calls.Load())
	}
	trace = source.LastTiming()
	if len(trace.Hops) != 2 || !trace.Hops[0].Cached || trace.Hops[1].Cached || trace.Duration < 0 {
		t.Fatalf("unexpected per-hop timing after invalidation: %+v", trace)
	}
}

func TestRouteQuotesReverseDirection(t *testing.T) {
	first := &countingSource{id: "first"}
	second := &countingSource{id: "second"}
	source, err := routequote.New("route-local", market.Market{ID: "route", BaseToken: "base", QuoteToken: "quote"}, []routequote.Hop{{Market: "hop1", In: "base", Out: "mid", Source: first}, {Market: "hop2", In: "mid", Out: "quote", Source: second}})
	if err != nil {
		t.Fatal(err)
	}
	input := quoteport.Input{Snapshot: routeSnapshot(t, snapshot(t, "hop1", 1, 1), snapshot(t, "hop2", 1, 2)), TokenIn: "quote", TokenOut: "base", AmountIn: mustAmount("quote", 10), Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()}
	result, err := source.Quote(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if result.AmountOut.Token() != "base" || result.AmountOut.Units().Cmp(big.NewInt(12)) != 0 {
		t.Fatalf("unexpected reverse output: %s", result.AmountOut)
	}
	if first.calls.Load() != 1 || second.calls.Load() != 1 {
		t.Fatalf("reverse route did not quote both hops first=%d second=%d", first.calls.Load(), second.calls.Load())
	}

	exactOutput, err := source.QuoteExactOutput(context.Background(), quoteport.ExactOutputInput{Snapshot: input.Snapshot, TokenIn: "quote", TokenOut: "base", AmountOut: mustAmount("base", 10), Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if exactOutput.AmountIn.Token() != "quote" || exactOutput.AmountIn.Units().Cmp(big.NewInt(12)) != 0 || exactOutput.AmountOut.Units().Cmp(big.NewInt(10)) != 0 {
		t.Fatalf("unexpected reverse exact-output quote: in=%s out=%s", exactOutput.AmountIn, exactOutput.AmountOut)
	}
}

func mustAmount(token market.TokenID, units int64) market.TokenAmount {
	amount, _ := market.NewTokenAmount(token, big.NewInt(units))
	return amount
}

func snapshot(t *testing.T, id market.MarketID, version, marker uint64) market.MarketSnapshot {
	now := time.Date(2026, 1, 1, 0, 0, 0, int(marker), time.UTC)
	snapshot, err := market.NewMarketSnapshot(market.SnapshotMetadata{Market: id, Source: "feed", Version: version, ReceivedAt: now, AppliedAt: now, Health: market.HealthHealthy, HealthChangedAt: now, StateHash: sha256.Sum256([]byte(fmt.Sprintf("%s-%d", id, marker)))}, data{})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func routeSnapshot(t *testing.T, snapshots ...market.MarketSnapshot) market.MarketSnapshot {
	bundle, err := market.NewSnapshotBundle("route", snapshots)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snapshot, err := market.NewMarketSnapshot(market.SnapshotMetadata{Market: "route", Source: "route", Version: bundle.Version(), ReceivedAt: now, AppliedAt: now, Health: market.HealthHealthy, HealthChangedAt: now, StateHash: bundle.Hash()}, bundle)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
