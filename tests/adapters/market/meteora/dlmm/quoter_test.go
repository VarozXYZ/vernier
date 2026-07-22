package dlmm_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/market/meteora/dlmm"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

func TestMeteoraDLMMGoldenSegmentQuoteAndExactOutput(t *testing.T) {
	bin, err := dlmm.NewBin(0, big.NewInt(1000), big.NewInt(2000))
	if err != nil {
		t.Fatal(err)
	}
	update, err := dlmm.NewStateUpdate(0, 10, 100, []dlmm.Bin{bin})
	if err != nil {
		t.Fatal(err)
	}
	data, hash, err := (dlmm.Reducer{}).Reduce(context.Background(), nil, update)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := testSnapshot(t, "pool", data, hash)
	candidate := market.Market{ID: "pool", BaseToken: "x", QuoteToken: "y"}
	quoter, err := dlmm.NewQuoter("meteora", candidate, "x", "y")
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewTokenAmount("x", big.NewInt(100))
	quote, err := quoter.Quote(context.Background(), quoteport.Input{Snapshot: snapshot, TokenIn: "x", TokenOut: "y", AmountIn: amount, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if quote.AmountOut.Units().Cmp(big.NewInt(198)) != 0 {
		t.Fatalf("amount out = %s", quote.AmountOut.Units())
	}
	target, _ := market.NewTokenAmount("y", big.NewInt(198))
	exact, err := quoter.QuoteExactOutput(context.Background(), quoteport.ExactOutputInput{Snapshot: snapshot, TokenIn: "x", TokenOut: "y", AmountOut: target, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if exact.AmountIn.Units().Cmp(big.NewInt(100)) != 0 {
		t.Fatalf("exact input = %s", exact.AmountIn.Units())
	}
}

func TestMeteoraDLMMUsesBinPriceAndOneSidedLiquidity(t *testing.T) {
	price := new(big.Int).Lsh(big.NewInt(2), 64)
	bin, err := dlmm.NewBinWithPrice(0, big.NewInt(0), big.NewInt(2_000), price)
	if err != nil {
		t.Fatal(err)
	}
	update, err := dlmm.NewStateUpdate(0, 10, 0, []dlmm.Bin{bin})
	if err != nil {
		t.Fatal(err)
	}
	data, hash, err := (dlmm.Reducer{}).Reduce(context.Background(), nil, update)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := testSnapshot(t, "pool", data, hash)
	quoter, err := dlmm.NewQuoter("meteora", market.Market{ID: "pool", BaseToken: "x", QuoteToken: "y"}, "x", "y")
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewTokenAmount("x", big.NewInt(100))
	quote, err := quoter.Quote(context.Background(), quoteport.Input{Snapshot: snapshot, TokenIn: "x", TokenOut: "y", AmountIn: amount, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if quote.AmountOut.Units().Cmp(big.NewInt(200)) != 0 {
		t.Fatalf("amount out = %s, want 200", quote.AmountOut.Units())
	}
}

func TestMeteoraDLMMStartsAtActiveBinInBothDirections(t *testing.T) {
	scale := new(big.Int).Lsh(big.NewInt(1), 64)
	binBelow, err := dlmm.NewBinWithPrice(0, big.NewInt(1_000), big.NewInt(1_000), scale)
	if err != nil {
		t.Fatal(err)
	}
	binActive, err := dlmm.NewBinWithPrice(1, big.NewInt(500), big.NewInt(1_000), new(big.Int).Lsh(big.NewInt(1), 65))
	if err != nil {
		t.Fatal(err)
	}
	binAbove, err := dlmm.NewBinWithPrice(2, big.NewInt(1_000), big.NewInt(1_000), new(big.Int).Lsh(big.NewInt(2), 65))
	if err != nil {
		t.Fatal(err)
	}
	update, err := dlmm.NewStateUpdate(1, 10, 0, []dlmm.Bin{binBelow, binActive, binAbove})
	if err != nil {
		t.Fatal(err)
	}
	data, hash, err := (dlmm.Reducer{}).Reduce(context.Background(), nil, update)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := testSnapshot(t, "pool", data, hash)
	quoter, err := dlmm.NewQuoter("meteora", market.Market{ID: "pool", BaseToken: "x", QuoteToken: "y"}, "x", "y")
	if err != nil {
		t.Fatal(err)
	}
	xIn, _ := market.NewTokenAmount("x", big.NewInt(100))
	forward, err := quoter.Quote(context.Background(), quoteport.Input{Snapshot: snapshot, TokenIn: "x", TokenOut: "y", AmountIn: xIn, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if forward.AmountOut.Units().Cmp(big.NewInt(200)) != 0 {
		t.Fatalf("forward quote = %s, want active-bin output 200", forward.AmountOut.Units())
	}
	yIn, _ := market.NewTokenAmount("y", big.NewInt(100))
	reverse, err := quoter.Quote(context.Background(), quoteport.Input{Snapshot: snapshot, TokenIn: "y", TokenOut: "x", AmountIn: yIn, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if reverse.AmountOut.Units().Cmp(big.NewInt(50)) != 0 {
		t.Fatalf("reverse quote = %s, want active-bin output 50", reverse.AmountOut.Units())
	}
	// Crossing the active bin must follow the protocol direction: X->Y moves
	// to lower IDs, while Y->X moves to higher IDs.
	wideX, _ := market.NewTokenAmount("x", big.NewInt(600))
	wideForward, err := quoter.Quote(context.Background(), quoteport.Input{Snapshot: snapshot, TokenIn: "x", TokenOut: "y", AmountIn: wideX, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if wideForward.AmountOut.Units().Cmp(big.NewInt(1100)) != 0 {
		t.Fatalf("wide forward quote = %s, want 1100", wideForward.AmountOut.Units())
	}
	wideY, _ := market.NewTokenAmount("y", big.NewInt(1500))
	wideReverse, err := quoter.Quote(context.Background(), quoteport.Input{Snapshot: snapshot, TokenIn: "y", TokenOut: "x", AmountIn: wideY, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if wideReverse.AmountOut.Units().Cmp(big.NewInt(625)) != 0 {
		t.Fatalf("wide reverse quote = %s, want 625", wideReverse.AmountOut.Units())
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
