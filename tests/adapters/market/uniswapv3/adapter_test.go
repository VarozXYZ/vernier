package uniswapv3_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/feed/sourceorder"
	"github.com/VarozXYZ/vernier/adapters/market/uniswapv3"
	"github.com/VarozXYZ/vernier/core/marketstate"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

func TestSqrtRatioAtTickMatchesCanonicalBoundaries(t *testing.T) {
	for _, test := range []struct {
		tick int32
		want string
	}{
		{tick: uniswapv3.MinTick, want: "4295128739"},
		{tick: 0, want: "79228162514264337593543950336"},
		{tick: uniswapv3.MaxTick, want: "1461446703485210103287273052203988822378723970342"},
	} {
		got, err := uniswapv3.SqrtRatioAtTick(test.tick)
		if err != nil {
			t.Fatal(err)
		}
		if got.String() != test.want {
			t.Fatalf("tick %d sqrt ratio = %s, want %s", test.tick, got, test.want)
		}
	}
}

func TestExactInputQuoteMatchesIndependentSingleRangeVector(t *testing.T) {
	snapshot := snapshotForTest(t, big.NewInt(1_000_000_000_000), nil)
	quoter, err := uniswapv3.NewQuoter("local-v3", testMarket(), "token0", "token1")
	if err != nil {
		t.Fatal(err)
	}
	amountIn, _ := market.ParseTokenAmount("token0", "1000000")
	quote, err := quoter.Quote(context.Background(), quoteport.Input{
		Snapshot: snapshot, TokenIn: "token0", TokenOut: "token1", AmountIn: amountIn,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: testTime().Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if quote.AmountOut.String() != "996999" || quote.Fees()[0].Amount().String() != "3000" {
		t.Fatalf("quote output=%s fee=%s, want 996999 and 3000", quote.AmountOut, quote.Fees()[0].Amount())
	}
}

func TestExactInputQuoteSupportsBothTokenDirections(t *testing.T) {
	snapshot := snapshotForTest(t, big.NewInt(1_000_000_000_000), nil)
	quoter, _ := uniswapv3.NewQuoter("local-v3", testMarket(), "token0", "token1")
	amountIn, _ := market.ParseTokenAmount("token1", "1000000")
	quote, err := quoter.Quote(context.Background(), quoteport.Input{
		Snapshot: snapshot, TokenIn: "token1", TokenOut: "token0", AmountIn: amountIn,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: testTime().Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if quote.AmountOut.String() != "996999" || quote.Fees()[0].Amount().String() != "3000" {
		t.Fatalf("reverse quote output=%s fees=%+v", quote.AmountOut, quote.Fees())
	}
}

func TestExactInputQuoteTraversesBitmapWordBoundaries(t *testing.T) {
	snapshot := snapshotForTest(t, big.NewInt(1_000_000_000_000), nil)
	quoter, _ := uniswapv3.NewQuoter("local-v3", testMarket(), "token0", "token1")
	amountIn, _ := market.ParseTokenAmount("token0", "2000000000000")
	quote, err := quoter.Quote(context.Background(), quoteport.Input{
		Snapshot: snapshot, TokenIn: "token0", TokenOut: "token1", AmountIn: amountIn,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: testTime().Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if quote.AmountOut.String() != "665998663994" || quote.Fees()[0].Amount().String() != "6000000001" {
		t.Fatalf("word traversal quote output=%s fee=%s", quote.AmountOut, quote.Fees()[0].Amount())
	}
}

func TestQuoteCrossesInitializedTickAndChangesLiquidity(t *testing.T) {
	inner := big.NewInt(1_000_000_000_000)
	outer := big.NewInt(1_000_000_000_000)
	ticks := []uniswapv3.Tick{
		mustTick(t, -120, outer, outer),
		mustTick(t, -60, inner, inner),
		mustTick(t, 60, inner, new(big.Int).Neg(new(big.Int).Set(inner))),
		mustTick(t, 120, outer, new(big.Int).Neg(new(big.Int).Set(outer))),
	}
	snapshot := snapshotForTest(t, big.NewInt(2_000_000_000_000), ticks)
	quoter, err := uniswapv3.NewQuoter("local-v3", testMarket(), "token0", "token1")
	if err != nil {
		t.Fatal(err)
	}
	amountIn, _ := market.ParseTokenAmount("token0", "7000000000")
	quote, err := quoter.Quote(context.Background(), quoteport.Input{
		Snapshot: snapshot, TokenIn: "token0", TokenOut: "token1", AmountIn: amountIn,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: testTime().Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if quote.AmountOut.String() != "6954265622" || quote.Fees()[0].Amount().String() != "21000001" {
		t.Fatalf("crossing quote output=%s fee=%s", quote.AmountOut, quote.Fees()[0].Amount())
	}
}

func TestReducerPublishesImmutableStateAndAppliesLiquidityEvents(t *testing.T) {
	mirror := newV3Mirror(t)
	initial := applyV3(t, mirror, v3Event(t, 10, stateUpdateForTest(t, big.NewInt(1_000_000_000_000), nil)))
	initialState := initial.Data().(uniswapv3.Snapshot)
	price := initialState.SqrtPriceX96()
	price.SetInt64(1)
	if initialState.SqrtPriceX96().Cmp(q96()) != 0 {
		t.Fatal("snapshot price mutated through returned integer")
	}

	liquidityEvent, err := uniswapv3.NewLiquidityUpdate(-60, 60, big.NewInt(500_000_000_000))
	if err != nil {
		t.Fatal(err)
	}
	updated := applyV3(t, mirror, v3Event(t, 11, liquidityEvent))
	updatedState := updated.Data().(uniswapv3.Snapshot)
	if updatedState.Liquidity().String() != "1500000000000" || len(updatedState.Ticks()) != 2 {
		t.Fatalf("unexpected liquidity state: liquidity=%s ticks=%d", updatedState.Liquidity(), len(updatedState.Ticks()))
	}
	if initial.Metadata().Version != 1 || updated.Metadata().Version != 2 || len(initialState.Ticks()) != 0 {
		t.Fatal("generic mirror did not preserve immutable V3 snapshots")
	}
	burn, _ := uniswapv3.NewLiquidityUpdate(-60, 60, big.NewInt(-500_000_000_000))
	restored := applyV3(t, mirror, v3Event(t, 12, burn)).Data().(uniswapv3.Snapshot)
	if restored.Liquidity().String() != "1000000000000" || len(restored.Ticks()) != 0 {
		t.Fatalf("liquidity burn did not restore state: liquidity=%s ticks=%d", restored.Liquidity(), len(restored.Ticks()))
	}
}

func TestReducerAppliesSwapStateWithoutLosingInitializedTicks(t *testing.T) {
	mirror := newV3Mirror(t)
	outer := big.NewInt(1_000_000_000_000)
	ticks := []uniswapv3.Tick{
		mustTick(t, -60, outer, outer),
		mustTick(t, 60, outer, new(big.Int).Neg(new(big.Int).Set(outer))),
	}
	applyV3(t, mirror, v3Event(t, 1, stateUpdateForTest(t, outer, ticks)))
	nextPrice, _ := uniswapv3.SqrtRatioAtTick(1)
	swap, err := uniswapv3.NewSwapUpdate(nextPrice, 1, outer)
	if err != nil {
		t.Fatal(err)
	}
	updated := applyV3(t, mirror, v3Event(t, 2, swap)).Data().(uniswapv3.Snapshot)
	if updated.Tick() != 1 || updated.SqrtPriceX96().Cmp(nextPrice) != 0 || len(updated.Ticks()) != 2 {
		t.Fatalf("unexpected swap state: tick=%d price=%s ticks=%d", updated.Tick(), updated.SqrtPriceX96(), len(updated.Ticks()))
	}
}

func TestStateRejectsPriceInconsistentWithTick(t *testing.T) {
	if _, err := uniswapv3.NewStateUpdate(q96(), 60, big.NewInt(1), 3000, 60, nil); err == nil {
		t.Fatal("inconsistent tick and sqrt price were accepted")
	}
}

func TestBoundedStateRejectsTicksOutsideDeclaredCoverage(t *testing.T) {
	coverage, err := uniswapv3.NewTickCoverage(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	outside := mustTick(t, -60, big.NewInt(1), big.NewInt(1))
	if _, err := uniswapv3.NewCoveredStateUpdate(
		q96(), 0, big.NewInt(1), 3000, 60, []uniswapv3.Tick{outside}, coverage,
	); err == nil {
		t.Fatal("tick in bitmap word -1 was accepted by coverage 0..0")
	}
}

func snapshotForTest(t *testing.T, liquidity *big.Int, ticks []uniswapv3.Tick) market.MarketSnapshot {
	t.Helper()
	mirror := newV3Mirror(t)
	return applyV3(t, mirror, v3Event(t, 1, stateUpdateForTest(t, liquidity, ticks)))
}

func stateUpdateForTest(t *testing.T, liquidity *big.Int, ticks []uniswapv3.Tick) uniswapv3.StateUpdate {
	t.Helper()
	update, err := uniswapv3.NewStateUpdate(q96(), 0, liquidity, 3000, 60, ticks)
	if err != nil {
		t.Fatal(err)
	}
	return update
}

func newV3Mirror(t *testing.T) *marketstate.Mirror {
	t.Helper()
	mirror, err := marketstate.NewMirror(
		"market", "feed", uniswapv3.Reducer{}, sourceorder.NewMonotonic(sourceorder.BlockPositionKind, true), testTime,
	)
	if err != nil {
		t.Fatal(err)
	}
	return mirror
}

func applyV3(t *testing.T, mirror *marketstate.Mirror, event market.MarketEvent) market.MarketSnapshot {
	t.Helper()
	result, err := mirror.Apply(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	return result.Snapshot
}

func v3Event(t *testing.T, block uint64, data market.EventData) market.MarketEvent {
	t.Helper()
	event, err := market.NewMarketEvent(market.MarketEvent{
		Market: "market", Source: "feed", Position: market.SourcePosition{Kind: sourceorder.BlockPositionKind, Value: block},
		Finality: market.FinalityConfirmed, ReceivedAt: testTime().Add(time.Duration(block) * time.Millisecond), Data: data,
	})
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func mustTick(t *testing.T, index int32, gross, net *big.Int) uniswapv3.Tick {
	t.Helper()
	tick, err := uniswapv3.NewTick(index, gross, net)
	if err != nil {
		t.Fatal(err)
	}
	return tick
}

func testMarket() market.Market {
	return market.Market{ID: "market", BaseToken: "token0", QuoteToken: "token1"}
}

func testTime() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }

func q96() *big.Int {
	value, _ := new(big.Int).SetString("79228162514264337593543950336", 10)
	return value
}
