// Package uniswapv3 provides deterministic local state reduction and quoting
// for the Uniswap V3 concentrated-liquidity model.
package uniswapv3

import (
	"errors"
	"fmt"
	"math/big"
	"sort"

	"github.com/VarozXYZ/vernier/domain/market"
)

const snapshotSchemaVersion uint16 = 2

var ErrInsufficientTickCoverage = errors.New("insufficient Uniswap V3 tick coverage")

type TickCoverage struct {
	full    bool
	minWord int32
	maxWord int32
}

func FullTickCoverage() TickCoverage { return TickCoverage{full: true} }

func NewTickCoverage(minWord, maxWord int32) (TickCoverage, error) {
	if minWord > maxWord {
		return TickCoverage{}, fmt.Errorf("minimum tick word exceeds maximum")
	}
	return TickCoverage{minWord: minWord, maxWord: maxWord}, nil
}

func (c TickCoverage) Full() bool     { return c.full }
func (c TickCoverage) MinWord() int32 { return c.minWord }
func (c TickCoverage) MaxWord() int32 { return c.maxWord }
func (c TickCoverage) Contains(word int32) bool {
	return c.full || word >= c.minWord && word <= c.maxWord
}

type Tick struct {
	index          int32
	liquidityGross *big.Int
	liquidityNet   *big.Int
}

func NewTick(index int32, liquidityGross, liquidityNet *big.Int) (Tick, error) {
	if liquidityGross == nil || liquidityNet == nil || liquidityGross.Sign() <= 0 {
		return Tick{}, fmt.Errorf("tick liquidity gross must be positive and net must be present")
	}
	if new(big.Int).Abs(new(big.Int).Set(liquidityNet)).Cmp(liquidityGross) > 0 {
		return Tick{}, fmt.Errorf("tick liquidity net exceeds gross")
	}
	if liquidityGross.Cmp(maxUint128) > 0 || liquidityNet.Cmp(maxInt128) > 0 || liquidityNet.Cmp(minInt128) < 0 {
		return Tick{}, fmt.Errorf("tick liquidity exceeds Uniswap V3 integer bounds")
	}
	return Tick{
		index: index, liquidityGross: new(big.Int).Set(liquidityGross), liquidityNet: new(big.Int).Set(liquidityNet),
	}, nil
}

func (t Tick) Index() int32             { return t.index }
func (t Tick) LiquidityGross() *big.Int { return cloneInt(t.liquidityGross) }
func (t Tick) LiquidityNet() *big.Int   { return cloneInt(t.liquidityNet) }

type StateUpdate struct {
	sqrtPriceX96 *big.Int
	tick         int32
	liquidity    *big.Int
	feePips      uint32
	tickSpacing  int32
	ticks        []Tick
	coverage     TickCoverage
}

func NewStateUpdate(sqrtPriceX96 *big.Int, tick int32, liquidity *big.Int, feePips uint32, tickSpacing int32, ticks []Tick) (StateUpdate, error) {
	return NewCoveredStateUpdate(sqrtPriceX96, tick, liquidity, feePips, tickSpacing, ticks, FullTickCoverage())
}

func NewCoveredStateUpdate(
	sqrtPriceX96 *big.Int,
	tick int32,
	liquidity *big.Int,
	feePips uint32,
	tickSpacing int32,
	ticks []Tick,
	coverage TickCoverage,
) (StateUpdate, error) {
	state := Snapshot{
		schemaVersion: snapshotSchemaVersion, sqrtPriceX96: cloneInt(sqrtPriceX96), tick: tick,
		liquidity: cloneInt(liquidity), feePips: feePips, tickSpacing: tickSpacing, ticks: normalizedTicks(ticks),
		coverage: coverage,
	}
	if err := state.validate(); err != nil {
		return StateUpdate{}, err
	}
	return StateUpdate{
		sqrtPriceX96: state.SqrtPriceX96(), tick: tick, liquidity: state.Liquidity(),
		feePips: feePips, tickSpacing: tickSpacing, ticks: state.Ticks(), coverage: coverage,
	}, nil
}

func (StateUpdate) EventKind() string { return "uniswap_v3/state/v1" }

type SwapUpdate struct {
	sqrtPriceX96 *big.Int
	tick         int32
	liquidity    *big.Int
}

func NewSwapUpdate(sqrtPriceX96 *big.Int, tick int32, liquidity *big.Int) (SwapUpdate, error) {
	if sqrtPriceX96 == nil || liquidity == nil || liquidity.Sign() < 0 || tick < MinTick || tick >= MaxTick {
		return SwapUpdate{}, fmt.Errorf("invalid Uniswap V3 swap state")
	}
	if sqrtPriceX96.Cmp(minSqrtRatio) <= 0 || sqrtPriceX96.Cmp(maxSqrtRatio) >= 0 {
		return SwapUpdate{}, fmt.Errorf("sqrt price is outside Uniswap V3 bounds")
	}
	if err := validateTickPrice(tick, sqrtPriceX96); err != nil {
		return SwapUpdate{}, err
	}
	return SwapUpdate{sqrtPriceX96: cloneInt(sqrtPriceX96), tick: tick, liquidity: cloneInt(liquidity)}, nil
}

func (SwapUpdate) EventKind() string { return "uniswap_v3/swap/v1" }

type LiquidityUpdate struct {
	lower int32
	upper int32
	delta *big.Int
}

func NewLiquidityUpdate(lower, upper int32, delta *big.Int) (LiquidityUpdate, error) {
	if lower >= upper || lower < MinTick || upper > MaxTick || delta == nil || delta.Sign() == 0 {
		return LiquidityUpdate{}, fmt.Errorf("invalid Uniswap V3 liquidity update")
	}
	return LiquidityUpdate{lower: lower, upper: upper, delta: cloneInt(delta)}, nil
}

func (LiquidityUpdate) EventKind() string { return "uniswap_v3/liquidity/v1" }

type InitializeUpdate struct {
	sqrtPriceX96 *big.Int
	tick         int32
}

func NewInitializeUpdate(sqrtPriceX96 *big.Int, tick int32) (InitializeUpdate, error) {
	if sqrtPriceX96 == nil {
		return InitializeUpdate{}, fmt.Errorf("Uniswap V3 initialization price is required")
	}
	if err := validateTickPrice(tick, sqrtPriceX96); err != nil {
		return InitializeUpdate{}, err
	}
	return InitializeUpdate{sqrtPriceX96: cloneInt(sqrtPriceX96), tick: tick}, nil
}

func (InitializeUpdate) EventKind() string { return "uniswap_v3/initialize/v1" }

type BlockUpdate struct {
	updates []market.EventData
}

func NewBlockUpdate(updates ...market.EventData) (BlockUpdate, error) {
	if len(updates) == 0 {
		return BlockUpdate{}, fmt.Errorf("Uniswap V3 block update requires events")
	}
	result := BlockUpdate{updates: append([]market.EventData(nil), updates...)}
	for _, update := range result.updates {
		switch update.(type) {
		case InitializeUpdate, SwapUpdate, LiquidityUpdate:
		default:
			return BlockUpdate{}, fmt.Errorf("unsupported Uniswap V3 block update %T", update)
		}
	}
	return result, nil
}

func (BlockUpdate) EventKind() string { return "uniswap_v3/block/v1" }

type Snapshot struct {
	schemaVersion uint16
	sqrtPriceX96  *big.Int
	tick          int32
	liquidity     *big.Int
	feePips       uint32
	tickSpacing   int32
	ticks         []Tick
	coverage      TickCoverage
}

func (Snapshot) SnapshotKind() string     { return "uniswap_v3/v1" }
func (s Snapshot) SqrtPriceX96() *big.Int { return cloneInt(s.sqrtPriceX96) }
func (s Snapshot) Tick() int32            { return s.tick }
func (s Snapshot) Liquidity() *big.Int    { return cloneInt(s.liquidity) }
func (s Snapshot) FeePips() uint32        { return s.feePips }
func (s Snapshot) TickSpacing() int32     { return s.tickSpacing }
func (s Snapshot) Ticks() []Tick          { return cloneTicks(s.ticks) }
func (s Snapshot) Coverage() TickCoverage { return s.coverage }

func (s Snapshot) validate() error {
	if s.schemaVersion != snapshotSchemaVersion || s.sqrtPriceX96 == nil || s.liquidity == nil {
		return fmt.Errorf("incomplete Uniswap V3 state")
	}
	if s.tick < MinTick || s.tick >= MaxTick || s.tickSpacing <= 0 || s.tickSpacing > maxTickSpacing || s.feePips >= feeDenominator {
		return fmt.Errorf("invalid Uniswap V3 tick, spacing, or fee")
	}
	if s.sqrtPriceX96.Cmp(minSqrtRatio) <= 0 || s.sqrtPriceX96.Cmp(maxSqrtRatio) >= 0 || s.liquidity.Sign() < 0 {
		return fmt.Errorf("invalid Uniswap V3 price or liquidity")
	}
	if s.liquidity.BitLen() > 128 {
		return fmt.Errorf("active liquidity exceeds uint128")
	}
	if err := validateTickPrice(s.tick, s.sqrtPriceX96); err != nil {
		return err
	}
	if !s.coverage.full && s.coverage.minWord > s.coverage.maxWord {
		return fmt.Errorf("invalid Uniswap V3 tick coverage")
	}
	previous := int32(MinTick - 1)
	for _, initialized := range s.ticks {
		if initialized.index <= previous || initialized.index < MinTick || initialized.index > MaxTick || initialized.index%s.tickSpacing != 0 {
			return fmt.Errorf("invalid or unsorted initialized tick %d", initialized.index)
		}
		if _, err := NewTick(initialized.index, initialized.liquidityGross, initialized.liquidityNet); err != nil {
			return err
		}
		previous = initialized.index
	}
	return nil
}

func validateTickPrice(tick int32, sqrtPriceX96 *big.Int) error {
	lower, err := SqrtRatioAtTick(tick)
	if err != nil {
		return err
	}
	upper, err := SqrtRatioAtTick(tick + 1)
	if err != nil {
		return err
	}
	// Equality with the upper boundary is valid immediately after a
	// zero-for-one crossing, where the stored tick is boundary-1.
	if sqrtPriceX96.Cmp(lower) < 0 || sqrtPriceX96.Cmp(upper) > 0 {
		return fmt.Errorf("sqrt price is inconsistent with tick %d", tick)
	}
	return nil
}

func normalizedTicks(ticks []Tick) []Tick {
	result := cloneTicks(ticks)
	sort.Slice(result, func(i, j int) bool { return result[i].index < result[j].index })
	return result
}

func cloneTicks(ticks []Tick) []Tick {
	result := make([]Tick, len(ticks))
	for index, tick := range ticks {
		result[index] = Tick{index: tick.index, liquidityGross: cloneInt(tick.liquidityGross), liquidityNet: cloneInt(tick.liquidityNet)}
	}
	return result
}

func cloneInt(value *big.Int) *big.Int {
	if value == nil {
		return nil
	}
	return new(big.Int).Set(value)
}

var _ market.EventData = StateUpdate{}
var _ market.EventData = SwapUpdate{}
var _ market.EventData = LiquidityUpdate{}
var _ market.EventData = InitializeUpdate{}
var _ market.EventData = BlockUpdate{}
var _ market.SnapshotData = Snapshot{}
