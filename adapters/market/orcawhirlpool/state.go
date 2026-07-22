// Package orcawhirlpool implements the canonical Orca Whirlpool local state
// model. Account decoding belongs in the adapter bootstrap; the reducer only
// accepts normalized state transitions.
package orcawhirlpool

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/VarozXYZ/vernier/domain/market"
)

const snapshotSchemaVersion uint16 = 1
const feeRatePrecision uint32 = 1_000_000

type Tick struct {
	index        int32
	liquidityNet *big.Int
}

func NewTick(index int32, liquidityNet *big.Int) (Tick, error) {
	if liquidityNet == nil {
		return Tick{}, fmt.Errorf("tick liquidity net is required")
	}
	return Tick{index: index, liquidityNet: new(big.Int).Set(liquidityNet)}, nil
}
func (t Tick) Index() int32           { return t.index }
func (t Tick) LiquidityNet() *big.Int { return clone(t.liquidityNet) }

type StateUpdate struct {
	sqrtPriceX64 *big.Int
	tick         int32
	liquidity    *big.Int
	feeRate      uint32
	tickSpacing  int32
	ticks        []Tick
	fullCoverage bool
	minTick      int32
	maxTick      int32
}

type PoolUpdate struct {
	sqrtPriceX64 *big.Int
	tick         int32
	liquidity    *big.Int
	feeRate      uint32
}

func newPoolUpdate(sqrtPriceX64 *big.Int, tick int32, liquidity *big.Int, feeRate uint32) (PoolUpdate, error) {
	if sqrtPriceX64 == nil || liquidity == nil || sqrtPriceX64.Sign() <= 0 || liquidity.Sign() < 0 || feeRate >= feeRatePrecision {
		return PoolUpdate{}, fmt.Errorf("invalid Whirlpool pool update")
	}
	return PoolUpdate{sqrtPriceX64: clone(sqrtPriceX64), tick: tick, liquidity: clone(liquidity), feeRate: feeRate}, nil
}
func (PoolUpdate) EventKind() string { return "orca_whirlpool/pool/v1" }

type TickArrayUpdate struct {
	startTick int32
	spacing   int32
	ticks     []Tick
}

func newTickArrayUpdate(startTick, spacing int32, ticks []Tick) (TickArrayUpdate, error) {
	if spacing <= 0 {
		return TickArrayUpdate{}, fmt.Errorf("invalid Whirlpool tick array spacing")
	}
	return TickArrayUpdate{startTick: startTick, spacing: spacing, ticks: cloneTicks(ticks)}, nil
}
func (TickArrayUpdate) EventKind() string { return "orca_whirlpool/tick_array/v1" }

func NewStateUpdate(sqrtPriceX64 *big.Int, tick int32, liquidity *big.Int, feeBPS uint16, tickSpacing int32, ticks []Tick) (StateUpdate, error) {
	return NewCoveredStateUpdateWithFeeRate(sqrtPriceX64, tick, liquidity, uint32(feeBPS)*100, tickSpacing, ticks, true, 0, 0)
}
func NewCoveredStateUpdate(sqrtPriceX64 *big.Int, tick int32, liquidity *big.Int, feeBPS uint16, tickSpacing int32, ticks []Tick, full bool, minTick, maxTick int32) (StateUpdate, error) {
	return NewCoveredStateUpdateWithFeeRate(sqrtPriceX64, tick, liquidity, uint32(feeBPS)*100, tickSpacing, ticks, full, minTick, maxTick)
}
func NewCoveredStateUpdateWithFeeRate(sqrtPriceX64 *big.Int, tick int32, liquidity *big.Int, feeRate uint32, tickSpacing int32, ticks []Tick, full bool, minTick, maxTick int32) (StateUpdate, error) {
	state := Snapshot{schemaVersion: snapshotSchemaVersion, sqrtPriceX64: clone(sqrtPriceX64), tick: tick, liquidity: clone(liquidity), feeRate: feeRate, tickSpacing: tickSpacing, ticks: cloneTicks(ticks), fullCoverage: full, minTick: minTick, maxTick: maxTick}
	if err := state.validate(); err != nil {
		return StateUpdate{}, err
	}
	return StateUpdate{sqrtPriceX64: state.SqrtPriceX64(), tick: tick, liquidity: state.Liquidity(), feeRate: feeRate, tickSpacing: tickSpacing, ticks: state.Ticks(), fullCoverage: full, minTick: minTick, maxTick: maxTick}, nil
}
func (StateUpdate) EventKind() string { return "orca_whirlpool/state/v1" }

type SwapUpdate struct {
	sqrtPriceX64 *big.Int
	tick         int32
	liquidity    *big.Int
}

func NewSwapUpdate(sqrtPriceX64 *big.Int, tick int32, liquidity *big.Int) (SwapUpdate, error) {
	if sqrtPriceX64 == nil || liquidity == nil || sqrtPriceX64.Sign() <= 0 || liquidity.Sign() < 0 {
		return SwapUpdate{}, fmt.Errorf("invalid Whirlpool swap state")
	}
	return SwapUpdate{sqrtPriceX64: clone(sqrtPriceX64), tick: tick, liquidity: clone(liquidity)}, nil
}
func (SwapUpdate) EventKind() string { return "orca_whirlpool/swap/v1" }

type LiquidityUpdate struct{ delta *big.Int }

func NewLiquidityUpdate(delta *big.Int) (LiquidityUpdate, error) {
	if delta == nil {
		return LiquidityUpdate{}, fmt.Errorf("liquidity delta is required")
	}
	return LiquidityUpdate{delta: clone(delta)}, nil
}
func (LiquidityUpdate) EventKind() string { return "orca_whirlpool/liquidity/v1" }

type TickUpdate struct{ tick Tick }

func NewTickUpdate(tick Tick) TickUpdate { return TickUpdate{tick: tick} }
func (TickUpdate) EventKind() string     { return "orca_whirlpool/tick/v1" }

type Snapshot struct {
	schemaVersion uint16
	sqrtPriceX64  *big.Int
	tick          int32
	liquidity     *big.Int
	feeRate       uint32
	tickSpacing   int32
	ticks         []Tick
	fullCoverage  bool
	minTick       int32
	maxTick       int32
}

func (Snapshot) SnapshotKind() string     { return "orca_whirlpool/v1" }
func (s Snapshot) SqrtPriceX64() *big.Int { return clone(s.sqrtPriceX64) }
func (s Snapshot) Tick() int32            { return s.tick }
func (s Snapshot) Liquidity() *big.Int    { return clone(s.liquidity) }
func (s Snapshot) FeeRate() uint32        { return s.feeRate }
func (s Snapshot) FeeBPS() uint16 {
	return uint16((uint64(s.feeRate)*10_000 + uint64(feeRatePrecision) - 1) / uint64(feeRatePrecision))
}
func (s Snapshot) TickSpacing() int32 { return s.tickSpacing }
func (s Snapshot) Ticks() []Tick      { return cloneTicks(s.ticks) }
func (s Snapshot) FullCoverage() bool { return s.fullCoverage }
func (s Snapshot) MinTick() int32     { return s.minTick }
func (s Snapshot) MaxTick() int32     { return s.maxTick }

func (s Snapshot) validate() error {
	if s.schemaVersion != snapshotSchemaVersion || s.sqrtPriceX64 == nil || s.sqrtPriceX64.Sign() <= 0 || s.liquidity == nil || s.liquidity.Sign() < 0 || s.feeRate >= feeRatePrecision || s.tickSpacing <= 0 {
		return fmt.Errorf("invalid Orca Whirlpool state")
	}
	if !s.fullCoverage && s.minTick > s.maxTick {
		return fmt.Errorf("invalid Whirlpool tick coverage")
	}
	if !s.fullCoverage && (s.tick < s.minTick || s.tick > s.maxTick) {
		return fmt.Errorf("current Whirlpool tick is outside declared coverage")
	}
	previous := int32(-1 << 31)
	for _, tick := range s.ticks {
		if tick.liquidityNet == nil || tick.index <= previous || tick.index%s.tickSpacing != 0 || !s.fullCoverage && (tick.index < s.minTick || tick.index > s.maxTick) {
			return fmt.Errorf("invalid Whirlpool tick %d", tick.index)
		}
		previous = tick.index
	}
	return nil
}

type Reducer struct{}

func (Reducer) Reduce(ctx context.Context, previous market.SnapshotData, event market.EventData) (market.SnapshotData, [sha256.Size]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	var next Snapshot
	switch update := event.(type) {
	case StateUpdate:
		next = Snapshot{schemaVersion: snapshotSchemaVersion, sqrtPriceX64: clone(update.sqrtPriceX64), tick: update.tick, liquidity: clone(update.liquidity), feeRate: update.feeRate, tickSpacing: update.tickSpacing, ticks: cloneTicks(update.ticks), fullCoverage: update.fullCoverage, minTick: update.minTick, maxTick: update.maxTick}
	case PoolUpdate:
		current, err := require(previous)
		if err != nil {
			return nil, [sha256.Size]byte{}, err
		}
		next = current
		next.sqrtPriceX64 = clone(update.sqrtPriceX64)
		next.tick = update.tick
		next.liquidity = clone(update.liquidity)
		next.feeRate = update.feeRate
	case TickArrayUpdate:
		current, err := require(previous)
		if err != nil {
			return nil, [sha256.Size]byte{}, err
		}
		next = current
		arrayMax := update.startTick + int32(fixedTickArraySize-1)*update.spacing
		filtered := make([]Tick, 0, len(next.ticks)+len(update.ticks))
		for _, tick := range next.ticks {
			if tick.index < update.startTick || tick.index > arrayMax {
				filtered = append(filtered, tick)
			}
		}
		next.ticks = append(filtered, cloneTicks(update.ticks)...)
		sort.Slice(next.ticks, func(i, j int) bool { return next.ticks[i].index < next.ticks[j].index })
	case SwapUpdate:
		current, err := require(previous)
		if err != nil {
			return nil, [sha256.Size]byte{}, err
		}
		next = current
		next.sqrtPriceX64 = clone(update.sqrtPriceX64)
		next.tick = update.tick
		next.liquidity = clone(update.liquidity)
	case LiquidityUpdate:
		current, err := require(previous)
		if err != nil {
			return nil, [sha256.Size]byte{}, err
		}
		next = current
		next.liquidity.Add(next.liquidity, update.delta)
	case TickUpdate:
		current, err := require(previous)
		if err != nil {
			return nil, [sha256.Size]byte{}, err
		}
		next = current
		found := false
		for i := range next.ticks {
			if next.ticks[i].index == update.tick.index {
				next.ticks[i] = update.tick
				found = true
			}
		}
		if !found {
			next.ticks = append(next.ticks, update.tick)
		}
		sort.Slice(next.ticks, func(i, j int) bool { return next.ticks[i].index < next.ticks[j].index })
	default:
		return nil, [sha256.Size]byte{}, fmt.Errorf("unsupported Orca Whirlpool event %T", event)
	}
	if err := next.validate(); err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	return next, hashState(next), nil
}
func require(previous market.SnapshotData) (Snapshot, error) {
	state, ok := previous.(Snapshot)
	if !ok || state.schemaVersion != snapshotSchemaVersion {
		return Snapshot{}, fmt.Errorf("whirlpool update requires a compatible snapshot")
	}
	state.sqrtPriceX64 = clone(state.sqrtPriceX64)
	state.liquidity = clone(state.liquidity)
	state.ticks = cloneTicks(state.ticks)
	return state, nil
}
func cloneTicks(input []Tick) []Tick {
	result := make([]Tick, len(input))
	for i, tick := range input {
		result[i] = Tick{index: tick.index, liquidityNet: clone(tick.liquidityNet)}
	}
	return result
}
func clone(value *big.Int) *big.Int {
	if value == nil {
		return nil
	}
	return new(big.Int).Set(value)
}
func hashState(state Snapshot) [sha256.Size]byte {
	var b strings.Builder
	fmt.Fprintf(&b, "%d|%s|%d|%s|%d|%d|%t|%d|%d", state.schemaVersion, state.sqrtPriceX64, state.tick, state.liquidity, state.feeRate, state.tickSpacing, state.fullCoverage, state.minTick, state.maxTick)
	for _, tick := range state.ticks {
		fmt.Fprintf(&b, "|%d:%s", tick.index, tick.liquidityNet)
	}
	return sha256.Sum256([]byte(b.String()))
}

var _ market.EventData = StateUpdate{}
var _ market.EventData = PoolUpdate{}
var _ market.EventData = TickArrayUpdate{}
var _ market.EventData = SwapUpdate{}
var _ market.EventData = LiquidityUpdate{}
var _ market.EventData = TickUpdate{}
var _ market.SnapshotData = Snapshot{}
