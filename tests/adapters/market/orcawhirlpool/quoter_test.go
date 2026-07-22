package orcawhirlpool_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/market/orcawhirlpool"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

func TestWhirlpoolVirtualReserveQuoteIsIntegerAndImmutable(t *testing.T) {
	price := new(big.Int).Lsh(big.NewInt(1), 64)
	update, err := orcawhirlpool.NewStateUpdate(price, 0, big.NewInt(1000), 30, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, hash, err := (orcawhirlpool.Reducer{}).Reduce(context.Background(), nil, update)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := testSnapshot(t, "pool", data, hash)
	quoter, err := orcawhirlpool.NewQuoter("orca", market.Market{ID: "pool", BaseToken: "a", QuoteToken: "b"}, "a", "b")
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewTokenAmount("a", big.NewInt(100))
	quote, err := quoter.Quote(context.Background(), quoteport.Input{Snapshot: snapshot, TokenIn: "a", TokenOut: "b", AmountIn: amount, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if quote.AmountOut.Units().Cmp(big.NewInt(90)) != 0 {
		t.Fatalf("amount out = %s, want constant-product output 90", quote.AmountOut.Units())
	}
	target, _ := market.NewTokenAmount("b", big.NewInt(90))
	exact, err := quoter.QuoteExactOutput(context.Background(), quoteport.ExactOutputInput{Snapshot: snapshot, TokenIn: "a", TokenOut: "b", AmountOut: target, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if exact.AmountIn.Units().Cmp(big.NewInt(100)) != 0 {
		t.Fatalf("exact-output input = %s, want 100", exact.AmountIn.Units())
	}
	state := snapshot.Data().(orcawhirlpool.Snapshot)
	price.SetInt64(1)
	if state.SqrtPriceX64().Cmp(new(big.Int).Lsh(big.NewInt(1), 64)) != 0 {
		t.Fatal("snapshot price was aliased")
	}
}

func TestWhirlpoolVirtualReserveUsesQ64Scale(t *testing.T) {
	// sqrtPriceX64 = 2^65 means price = 4, so the virtual B/A reserve ratio
	// is 4. Using 2^6 here would make the result many orders of magnitude off.
	price := new(big.Int).Lsh(big.NewInt(1), 65)
	update, err := orcawhirlpool.NewStateUpdate(price, 0, big.NewInt(1_000), 0, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, hash, err := (orcawhirlpool.Reducer{}).Reduce(context.Background(), nil, update)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := testSnapshot(t, "pool", data, hash)
	quoter, err := orcawhirlpool.NewQuoter("orca", market.Market{ID: "pool", BaseToken: "a", QuoteToken: "b"}, "a", "b")
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewTokenAmount("a", big.NewInt(100))
	quote, err := quoter.Quote(context.Background(), quoteport.Input{Snapshot: snapshot, TokenIn: "a", TokenOut: "b", AmountIn: amount, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if quote.AmountOut.Units().Cmp(big.NewInt(333)) != 0 {
		t.Fatalf("amount out = %s, want 333", quote.AmountOut.Units())
	}
}

func TestWhirlpoolQuoteUsesCoveredInitializedTicks(t *testing.T) {
	price := new(big.Int).Lsh(big.NewInt(1), 64)
	tick, err := orcawhirlpool.NewTick(-1, big.NewInt(0))
	if err != nil {
		t.Fatal(err)
	}
	update, err := orcawhirlpool.NewCoveredStateUpdate(price, 0, big.NewInt(1_000_000_000_000_000_000), 0, 1, []orcawhirlpool.Tick{tick}, false, -1, 0)
	if err != nil {
		t.Fatal(err)
	}
	data, hash, err := (orcawhirlpool.Reducer{}).Reduce(context.Background(), nil, update)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := testSnapshot(t, "pool", data, hash)
	quoter, err := orcawhirlpool.NewQuoter("orca", market.Market{ID: "pool", BaseToken: "a", QuoteToken: "b"}, "a", "b")
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewTokenAmount("a", big.NewInt(1_000_000_000_000))
	quote, err := quoter.Quote(context.Background(), quoteport.Input{Snapshot: snapshot, TokenIn: "a", TokenOut: "b", AmountIn: amount, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if quote.AmountOut.Units().Sign() <= 0 {
		t.Fatal("covered tick quote produced no output")
	}
}

func TestWhirlpoolQuoterUsesCanonicalMintOrder(t *testing.T) {
	price := new(big.Int).Lsh(big.NewInt(1), 64)
	tick, err := orcawhirlpool.NewTick(-1, big.NewInt(0))
	if err != nil {
		t.Fatal(err)
	}
	update, err := orcawhirlpool.NewCoveredStateUpdate(price, 0, big.NewInt(1_000_000_000_000_000_000), 0, 1, []orcawhirlpool.Tick{tick}, false, -1, 0)
	if err != nil {
		t.Fatal(err)
	}
	data, hash, err := (orcawhirlpool.Reducer{}).Reduce(context.Background(), nil, update)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := testSnapshot(t, "pool", data, hash)
	// The first path token is the pool's B mint. The constructor must normalize
	// the IDs using the canonical byte ordering, so input "b" is A->B.
	quoter, err := orcawhirlpool.NewQuoterWithAddresses("orca", market.Market{ID: "pool", BaseToken: "a", QuoteToken: "b"}, "a", "So11111111111111111111111111111111111111112", "b", "11111111111111111111111111111111")
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewTokenAmount("b", big.NewInt(1_000_000_000_000))
	quote, err := quoter.Quote(context.Background(), quoteport.Input{Snapshot: snapshot, TokenIn: "b", TokenOut: "a", AmountIn: amount, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if quote.AmountOut.Units().Sign() <= 0 {
		t.Fatal("canonical-order quote produced no output")
	}
	reverseAmount, _ := market.NewTokenAmount("a", big.NewInt(1_000_000_000_000))
	if _, err := quoter.Quote(context.Background(), quoteport.Input{Snapshot: snapshot, TokenIn: "a", TokenOut: "b", AmountIn: reverseAmount, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()}); err != nil {
		t.Fatalf("reverse quote at upper coverage boundary failed: %v", err)
	}
}

func testSnapshot(t *testing.T, id market.MarketID, data market.SnapshotData, hash [32]byte) market.MarketSnapshot {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snapshot, err := market.NewMarketSnapshot(market.SnapshotMetadata{Market: id, Source: "feed", Version: 1, ReceivedAt: now, AppliedAt: now, Health: market.HealthHealthy, HealthChangedAt: now, StateHash: hash}, data)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
