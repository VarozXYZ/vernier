package uniswapv3

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/feed/sourceorder"
	"github.com/VarozXYZ/vernier/core/marketstate"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

func TestSqrtRatioAtTickMatchesCanonicalBoundaries(t *testing.T) {
	for _, test := range []struct {
		tick int32
		want string
	}{
		{tick: MinTick, want: "4295128739"},
		{tick: 0, want: "79228162514264337593543950336"},
		{tick: MaxTick, want: "1461446703485210103287273052203988822378723970342"},
	} {
		got, err := SqrtRatioAtTick(test.tick)
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
	quoter, err := NewQuoter("local-v3", testMarket(), "token0", "token1")
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
	quoter, _ := NewQuoter("local-v3", testMarket(), "token0", "token1")
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

func TestQuoteCrossesInitializedTickAndChangesLiquidity(t *testing.T) {
	inner := big.NewInt(1_000_000_000_000)
	outer := big.NewInt(1_000_000_000_000)
	ticks := []Tick{
		mustTick(t, -120, outer, outer),
		mustTick(t, -60, inner, inner),
		mustTick(t, 60, inner, new(big.Int).Neg(new(big.Int).Set(inner))),
		mustTick(t, 120, outer, new(big.Int).Neg(new(big.Int).Set(outer))),
	}
	state := snapshotForTest(t, big.NewInt(2_000_000_000_000), ticks).Data().(Snapshot)
	result, err := quoteExactInput(state, true, big.NewInt(7_000_000_000))
	if err != nil {
		t.Fatal(err)
	}
	if result.ticksCrossed != 1 || result.amountOut.String() != "6954265622" || result.fee.String() != "21000001" {
		t.Fatalf("unexpected crossing result: %+v", result)
	}
}

func TestTickTraversalPreservesBitmapWordBoundaries(t *testing.T) {
	if next, initialized := nextInitializedTickWithinOneWord(nil, 0, 60, true); next != 0 || initialized {
		t.Fatalf("zero-for-one boundary = %d initialized=%v", next, initialized)
	}
	if next, initialized := nextInitializedTickWithinOneWord(nil, -1, 60, true); next != -15360 || initialized {
		t.Fatalf("negative word boundary = %d initialized=%v", next, initialized)
	}
	if next, initialized := nextInitializedTickWithinOneWord(nil, 0, 60, false); next != 15300 || initialized {
		t.Fatalf("one-for-zero boundary = %d initialized=%v", next, initialized)
	}
	tick := mustTick(t, -60, big.NewInt(1), big.NewInt(1))
	if next, initialized := nextInitializedTickWithinOneWord([]Tick{tick}, -1, 60, true); next != -60 || !initialized {
		t.Fatalf("initialized boundary = %d initialized=%v", next, initialized)
	}
}

func TestReducerPublishesImmutableStateAndAppliesLiquidityEvents(t *testing.T) {
	mirror := newV3Mirror(t)
	initial := applyV3(t, mirror, v3Event(t, 10, stateUpdateForTest(t, big.NewInt(1_000_000_000_000), nil)))
	initialState := initial.Data().(Snapshot)
	price := initialState.SqrtPriceX96()
	price.SetInt64(1)
	if initialState.SqrtPriceX96().Cmp(q96) != 0 {
		t.Fatal("snapshot price mutated through returned integer")
	}

	liquidityEvent, err := NewLiquidityUpdate(-60, 60, big.NewInt(500_000_000_000))
	if err != nil {
		t.Fatal(err)
	}
	updated := applyV3(t, mirror, v3Event(t, 11, liquidityEvent))
	updatedState := updated.Data().(Snapshot)
	if updatedState.Liquidity().String() != "1500000000000" || len(updatedState.Ticks()) != 2 {
		t.Fatalf("unexpected liquidity state: liquidity=%s ticks=%d", updatedState.Liquidity(), len(updatedState.Ticks()))
	}
	if initial.Metadata().Version != 1 || updated.Metadata().Version != 2 || len(initialState.Ticks()) != 0 {
		t.Fatal("generic mirror did not preserve immutable V3 snapshots")
	}
	burn, _ := NewLiquidityUpdate(-60, 60, big.NewInt(-500_000_000_000))
	restored := applyV3(t, mirror, v3Event(t, 12, burn)).Data().(Snapshot)
	if restored.Liquidity().String() != "1000000000000" || len(restored.Ticks()) != 0 {
		t.Fatalf("liquidity burn did not restore state: liquidity=%s ticks=%d", restored.Liquidity(), len(restored.Ticks()))
	}
}

func TestReducerAppliesSwapStateWithoutLosingInitializedTicks(t *testing.T) {
	mirror := newV3Mirror(t)
	outer := big.NewInt(1_000_000_000_000)
	ticks := []Tick{
		mustTick(t, -60, outer, outer),
		mustTick(t, 60, outer, new(big.Int).Neg(new(big.Int).Set(outer))),
	}
	applyV3(t, mirror, v3Event(t, 1, stateUpdateForTest(t, outer, ticks)))
	nextPrice, _ := SqrtRatioAtTick(1)
	swap, err := NewSwapUpdate(nextPrice, 1, outer)
	if err != nil {
		t.Fatal(err)
	}
	updated := applyV3(t, mirror, v3Event(t, 2, swap)).Data().(Snapshot)
	if updated.Tick() != 1 || updated.SqrtPriceX96().Cmp(nextPrice) != 0 || len(updated.Ticks()) != 2 {
		t.Fatalf("unexpected swap state: tick=%d price=%s ticks=%d", updated.Tick(), updated.SqrtPriceX96(), len(updated.Ticks()))
	}
}

func TestStateRejectsPriceInconsistentWithTick(t *testing.T) {
	if _, err := NewStateUpdate(q96, 60, big.NewInt(1), 3000, 60, nil); err == nil {
		t.Fatal("inconsistent tick and sqrt price were accepted")
	}
}

func snapshotForTest(t *testing.T, liquidity *big.Int, ticks []Tick) market.MarketSnapshot {
	t.Helper()
	mirror := newV3Mirror(t)
	return applyV3(t, mirror, v3Event(t, 1, stateUpdateForTest(t, liquidity, ticks)))
}

func stateUpdateForTest(t *testing.T, liquidity *big.Int, ticks []Tick) StateUpdate {
	t.Helper()
	update, err := NewStateUpdate(q96, 0, liquidity, 3000, 60, ticks)
	if err != nil {
		t.Fatal(err)
	}
	return update
}

func newV3Mirror(t *testing.T) *marketstate.Mirror {
	t.Helper()
	mirror, err := marketstate.NewMirror(
		"market", "feed", Reducer{}, sourceorder.NewMonotonic(sourceorder.BlockPositionKind, true), testTime,
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

func mustTick(t *testing.T, index int32, gross, net *big.Int) Tick {
	t.Helper()
	tick, err := NewTick(index, gross, net)
	if err != nil {
		t.Fatal(err)
	}
	return tick
}

func testMarket() market.Market {
	return market.Market{ID: "market", BaseToken: "token0", QuoteToken: "token1"}
}

func testTime() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
