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

func TestMeteoraDLMMQuoteTimeNeverPredatesSnapshot(t *testing.T) {
	bin, err := dlmm.NewBin(0, big.NewInt(10_000), big.NewInt(10_000))
	if err != nil {
		t.Fatal(err)
	}
	update, err := dlmm.NewStateUpdate(0, 1, 0, []dlmm.Bin{bin})
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
	amount, _ := market.NewTokenAmount("x", big.NewInt(1))
	quote, err := quoter.Quote(context.Background(), quoteport.Input{
		Snapshot: snapshot, TokenIn: "x", TokenOut: "y", AmountIn: amount,
		Purpose:  market.QuotePurposeResearchDiscovery,
		QuotedAt: snapshot.Metadata().AppliedAt.Add(-time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !quote.QuotedAt.Equal(snapshot.Metadata().AppliedAt) {
		t.Fatalf("quote time = %s, want snapshot time %s", quote.QuotedAt, snapshot.Metadata().AppliedAt)
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

func TestMeteoraDLMMDynamicFeeRisesAcrossBins(t *testing.T) {
	price := new(big.Int).Lsh(big.NewInt(1), 64)
	active, err := dlmm.NewBinWithPrice(1, big.NewInt(1000), big.NewInt(1000), price)
	if err != nil {
		t.Fatal(err)
	}
	stateUpdate, err := dlmm.NewDynamicStateUpdate(1, 10, 100, 0, 1_000, 100_000, 0, 2, []dlmm.Bin{active})
	if err != nil {
		t.Fatal(err)
	}
	data, _, err := (dlmm.Reducer{}).Reduce(context.Background(), nil, stateUpdate)
	if err != nil {
		t.Fatal(err)
	}
	state := data.(dlmm.Snapshot)
	if state.FeeRateForBin(0) <= state.FeeRateForBin(1) {
		t.Fatalf("dynamic fee did not rise across bins: near=%d next=%d", state.FeeRateForBin(1), state.FeeRateForBin(0))
	}
}

func TestMeteoraDLMMOfficialFeeAndQ64Parity(t *testing.T) {
	// Synthetic fixture evaluated with Meteora's official dlmm-sdk quote
	// algorithm: token Y is sold for X, price is exactly 0.05 in Q64.64,
	// and the pool's canonical 0.03% base fee is collected on input.
	price := new(big.Int).Quo(new(big.Int).Lsh(big.NewInt(1), 64), big.NewInt(20))
	bin, err := dlmm.NewBinWithPrice(0, big.NewInt(10_000_000_000), new(big.Int), price)
	if err != nil {
		t.Fatal(err)
	}
	update, err := dlmm.NewProtocolStateUpdate(0, 16, dlmm.StaticParameters{
		BaseFactor: 1_875, FilterPeriod: 30, DecayPeriod: 600, ReductionFactor: 5_000,
		VariableControl: 30_000, MaxVolatility: 350_000, ProtocolShare: 1_000,
		FunctionType: 2, CollectFeeMode: 1,
	}, dlmm.VariableParameters{IndexReference: 0, LastUpdateTimestamp: 1_767_225_600}, []dlmm.Bin{bin})
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
	amount, _ := market.NewTokenAmount("y", big.NewInt(304_166_666))
	quote, err := quoter.Quote(context.Background(), quoteport.Input{
		Snapshot: snapshot, TokenIn: "y", TokenOut: "x", AmountIn: amount,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := quote.AmountOut.Units(), big.NewInt(6_081_508_320); got.Cmp(want) != 0 {
		t.Fatalf("official-parity output = %s, want %s", got, want)
	}
	fees := quote.Fees()
	if len(fees) != 1 || fees[0].Amount().Token() != "y" || fees[0].Amount().Units().Cmp(big.NewInt(91_250)) != 0 {
		t.Fatalf("official-parity fee = %+v, want 91250 token y", fees)
	}
}

func TestMeteoraDLMMOnlyYChargesReverseFeeOnOutput(t *testing.T) {
	price := new(big.Int).Quo(new(big.Int).Lsh(big.NewInt(1), 64), big.NewInt(20))
	bin, err := dlmm.NewBinWithPrice(0, new(big.Int), big.NewInt(1_000_000_000), price)
	if err != nil {
		t.Fatal(err)
	}
	update, err := dlmm.NewProtocolStateUpdate(0, 16, dlmm.StaticParameters{BaseFactor: 1_875, CollectFeeMode: 1}, dlmm.VariableParameters{}, []dlmm.Bin{bin})
	if err != nil {
		t.Fatal(err)
	}
	data, hash, err := (dlmm.Reducer{}).Reduce(context.Background(), nil, update)
	if err != nil {
		t.Fatal(err)
	}
	quoter, _ := dlmm.NewQuoter("meteora", market.Market{ID: "pool", BaseToken: "x", QuoteToken: "y"}, "x", "y")
	amount, _ := market.NewTokenAmount("x", big.NewInt(6_000_000_000))
	quote, err := quoter.Quote(context.Background(), quoteport.Input{Snapshot: testSnapshot(t, "pool", data, hash), TokenIn: "x", TokenOut: "y", AmountIn: amount, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := quote.AmountOut.Units(), big.NewInt(299_909_999); got.Cmp(want) != 0 {
		t.Fatalf("OnlyY output = %s, want %s", got, want)
	}
	if fee := quote.Fees()[0].Amount(); fee.Token() != "y" || fee.Units().Cmp(big.NewInt(90_000)) != 0 {
		t.Fatalf("OnlyY fee = %s %s, want 90000 y", fee.Units(), fee.Token())
	}
	target, _ := market.NewTokenAmount("y", big.NewInt(299_909_999))
	exact, err := quoter.QuoteExactOutput(context.Background(), quoteport.ExactOutputInput{Snapshot: testSnapshot(t, "pool", data, hash), TokenIn: "x", TokenOut: "y", AmountOut: target, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if got := exact.AmountIn.Units(); got.Cmp(big.NewInt(5_999_999_981)) != 0 {
		t.Fatalf("OnlyY exact-output input = %s, want 5999999981", got)
	}
}

func TestMeteoraDLMMLimitOrderLiquidityIsQuoted(t *testing.T) {
	scale := new(big.Int).Lsh(big.NewInt(1), 64)
	bin, err := dlmm.NewBinWithProtocolLiquidity(0, new(big.Int), new(big.Int), scale, big.NewInt(1_000), big.NewInt(500), true)
	if err != nil {
		t.Fatal(err)
	}
	update, err := dlmm.NewProtocolStateUpdate(0, 10, dlmm.StaticParameters{FunctionType: 2}, dlmm.VariableParameters{}, []dlmm.Bin{bin})
	if err != nil {
		t.Fatal(err)
	}
	data, hash, err := (dlmm.Reducer{}).Reduce(context.Background(), nil, update)
	if err != nil {
		t.Fatal(err)
	}
	quoter, _ := dlmm.NewQuoter("meteora", market.Market{ID: "pool", BaseToken: "x", QuoteToken: "y"}, "x", "y")
	amount, _ := market.NewTokenAmount("y", big.NewInt(1_200))
	quote, err := quoter.Quote(context.Background(), quoteport.Input{Snapshot: testSnapshot(t, "pool", data, hash), TokenIn: "y", TokenOut: "x", AmountIn: amount, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if got := quote.AmountOut.Units(); got.Cmp(big.NewInt(1_200)) != 0 {
		t.Fatalf("limit-order output = %s, want 1200", got)
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
