// Package dlmm implements the canonical local state and integer quote model
// for Meteora's Dynamic Liquidity Market Maker.
package dlmm

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

type Bin struct {
	id       int32
	reserveX *big.Int
	reserveY *big.Int
}

func NewBin(id int32, reserveX, reserveY *big.Int) (Bin, error) {
	if reserveX == nil || reserveY == nil || reserveX.Sign() < 0 || reserveY.Sign() < 0 || reserveX.Sign() == 0 && reserveY.Sign() == 0 {
		return Bin{}, fmt.Errorf("bin reserves must be non-negative and not both zero")
	}
	return Bin{id: id, reserveX: clone(reserveX), reserveY: clone(reserveY)}, nil
}
func (b Bin) ID() int32          { return b.id }
func (b Bin) ReserveX() *big.Int { return clone(b.reserveX) }
func (b Bin) ReserveY() *big.Int { return clone(b.reserveY) }

type StateUpdate struct {
	activeID int32
	binStep  uint16
	feeBPS   uint16
	bins     []Bin
}

func NewStateUpdate(activeID int32, binStep, feeBPS uint16, bins []Bin) (StateUpdate, error) {
	state := Snapshot{schemaVersion: snapshotSchemaVersion, activeID: activeID, binStep: binStep, feeBPS: feeBPS, bins: cloneBins(bins)}
	if err := state.validate(); err != nil {
		return StateUpdate{}, err
	}
	return StateUpdate{activeID: activeID, binStep: binStep, feeBPS: feeBPS, bins: cloneBins(bins)}, nil
}
func (StateUpdate) EventKind() string { return "meteora_dlmm/state/v1" }

type SwapUpdate struct {
	activeID int32
	bins     []Bin
}

func NewSwapUpdate(activeID int32, bins []Bin) (SwapUpdate, error) {
	if len(bins) == 0 {
		return SwapUpdate{}, fmt.Errorf("swap update requires changed bins")
	}
	return SwapUpdate{activeID: activeID, bins: cloneBins(bins)}, nil
}
func (SwapUpdate) EventKind() string { return "meteora_dlmm/swap/v1" }

type LiquidityUpdate struct {
	id     int32
	deltaX *big.Int
	deltaY *big.Int
}

func NewLiquidityUpdate(id int32, deltaX, deltaY *big.Int) (LiquidityUpdate, error) {
	if deltaX == nil || deltaY == nil {
		return LiquidityUpdate{}, fmt.Errorf("liquidity deltas are required")
	}
	return LiquidityUpdate{id: id, deltaX: clone(deltaX), deltaY: clone(deltaY)}, nil
}
func (LiquidityUpdate) EventKind() string { return "meteora_dlmm/liquidity/v1" }

type Snapshot struct {
	schemaVersion uint16
	activeID      int32
	binStep       uint16
	feeBPS        uint16
	bins          []Bin
}

func (Snapshot) SnapshotKind() string { return "meteora_dlmm/v1" }
func (s Snapshot) ActiveID() int32    { return s.activeID }
func (s Snapshot) BinStep() uint16    { return s.binStep }
func (s Snapshot) FeeBPS() uint16     { return s.feeBPS }
func (s Snapshot) Bins() []Bin        { return cloneBins(s.bins) }

func (s Snapshot) validate() error {
	if s.schemaVersion != snapshotSchemaVersion || s.binStep == 0 || s.feeBPS >= 10_000 || len(s.bins) == 0 {
		return fmt.Errorf("invalid Meteora DLMM state")
	}
	previous := int32(-1 << 31)
	foundActive := false
	for _, bin := range s.bins {
		if bin.id <= previous || bin.reserveX.Sign() < 0 || bin.reserveY.Sign() < 0 || bin.reserveX.Sign() == 0 && bin.reserveY.Sign() == 0 {
			return fmt.Errorf("invalid or unsorted Meteora bin %d", bin.id)
		}
		if bin.id == s.activeID {
			foundActive = true
		}
		previous = bin.id
	}
	if !foundActive {
		return fmt.Errorf("active Meteora bin %d is not covered", s.activeID)
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
		next = Snapshot{schemaVersion: snapshotSchemaVersion, activeID: update.activeID, binStep: update.binStep, feeBPS: update.feeBPS, bins: cloneBins(update.bins)}
	case SwapUpdate:
		current, err := require(previous)
		if err != nil {
			return nil, [sha256.Size]byte{}, err
		}
		next = current
		next.activeID = update.activeID
		next.bins = mergeBins(current.bins, update.bins)
	case LiquidityUpdate:
		current, err := require(previous)
		if err != nil {
			return nil, [sha256.Size]byte{}, err
		}
		next = current
		found := false
		for i := range next.bins {
			if next.bins[i].id != update.id {
				continue
			}
			next.bins[i].reserveX.Add(next.bins[i].reserveX, update.deltaX)
			next.bins[i].reserveY.Add(next.bins[i].reserveY, update.deltaY)
			found = true
		}
		if !found {
			return nil, [sha256.Size]byte{}, fmt.Errorf("Meteora liquidity update references unknown bin %d", update.id)
		}
	default:
		return nil, [sha256.Size]byte{}, fmt.Errorf("unsupported Meteora DLMM event %T", event)
	}
	if err := next.validate(); err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	return next, hashState(next), nil
}

func require(previous market.SnapshotData) (Snapshot, error) {
	state, ok := previous.(Snapshot)
	if !ok || state.schemaVersion != snapshotSchemaVersion {
		return Snapshot{}, fmt.Errorf("Meteora update requires a compatible snapshot")
	}
	state.bins = cloneBins(state.bins)
	return state, nil
}
func mergeBins(base, updates []Bin) []Bin {
	byID := make(map[int32]Bin, len(base)+len(updates))
	for _, bin := range base {
		byID[bin.id] = bin
	}
	for _, bin := range updates {
		byID[bin.id] = bin
	}
	result := make([]Bin, 0, len(byID))
	for _, bin := range byID {
		result = append(result, bin)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].id < result[j].id })
	return cloneBins(result)
}
func cloneBins(input []Bin) []Bin {
	result := make([]Bin, len(input))
	for i, bin := range input {
		result[i] = Bin{id: bin.id, reserveX: clone(bin.reserveX), reserveY: clone(bin.reserveY)}
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
	var builder strings.Builder
	fmt.Fprintf(&builder, "%d|%d|%d|%d", state.schemaVersion, state.activeID, state.binStep, state.feeBPS)
	for _, bin := range state.bins {
		fmt.Fprintf(&builder, "|%d:%s:%s", bin.id, bin.reserveX, bin.reserveY)
	}
	return sha256.Sum256([]byte(builder.String()))
}

var _ market.EventData = StateUpdate{}
var _ market.EventData = SwapUpdate{}
var _ market.EventData = LiquidityUpdate{}
var _ market.SnapshotData = Snapshot{}
