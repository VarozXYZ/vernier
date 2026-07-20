package uniswapv3

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/big"
	"strings"

	"github.com/VarozXYZ/vernier/domain/market"
)

type Reducer struct{}

func (Reducer) Reduce(ctx context.Context, previous market.SnapshotData, event market.EventData) (market.SnapshotData, [sha256.Size]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	var next Snapshot
	switch update := event.(type) {
	case StateUpdate:
		next = Snapshot{
			schemaVersion: snapshotSchemaVersion, sqrtPriceX96: cloneInt(update.sqrtPriceX96), tick: update.tick,
			liquidity: cloneInt(update.liquidity), feePips: update.feePips,
			tickSpacing: update.tickSpacing, ticks: cloneTicks(update.ticks),
		}
	case SwapUpdate:
		current, err := requireSnapshot(previous)
		if err != nil {
			return nil, [sha256.Size]byte{}, err
		}
		next = current
		next.sqrtPriceX96 = cloneInt(update.sqrtPriceX96)
		next.tick = update.tick
		next.liquidity = cloneInt(update.liquidity)
	case LiquidityUpdate:
		current, err := requireSnapshot(previous)
		if err != nil {
			return nil, [sha256.Size]byte{}, err
		}
		next, err = applyLiquidity(current, update)
		if err != nil {
			return nil, [sha256.Size]byte{}, err
		}
	default:
		return nil, [sha256.Size]byte{}, fmt.Errorf("unsupported Uniswap V3 event payload %T", event)
	}
	if err := next.validate(); err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	return next, hashState(next), nil
}

func requireSnapshot(previous market.SnapshotData) (Snapshot, error) {
	state, ok := previous.(Snapshot)
	if !ok || state.schemaVersion != snapshotSchemaVersion {
		return Snapshot{}, fmt.Errorf("uniswap V3 update requires a compatible prior snapshot")
	}
	return Snapshot{
		schemaVersion: state.schemaVersion, sqrtPriceX96: state.SqrtPriceX96(), tick: state.tick,
		liquidity: state.Liquidity(), feePips: state.feePips,
		tickSpacing: state.tickSpacing, ticks: state.Ticks(),
	}, nil
}

func applyLiquidity(state Snapshot, update LiquidityUpdate) (Snapshot, error) {
	if update.lower%state.tickSpacing != 0 || update.upper%state.tickSpacing != 0 {
		return Snapshot{}, fmt.Errorf("liquidity range is not aligned to tick spacing")
	}
	ticks := make(map[int32]Tick, len(state.ticks)+2)
	for _, initialized := range state.ticks {
		ticks[initialized.index] = initialized
	}
	if err := updateBoundary(ticks, update.lower, update.delta, update.delta); err != nil {
		return Snapshot{}, err
	}
	if err := updateBoundary(ticks, update.upper, update.delta, new(big.Int).Neg(update.delta)); err != nil {
		return Snapshot{}, err
	}
	state.ticks = make([]Tick, 0, len(ticks))
	for _, initialized := range ticks {
		state.ticks = append(state.ticks, initialized)
	}
	state.ticks = normalizedTicks(state.ticks)
	if state.tick >= update.lower && state.tick < update.upper {
		state.liquidity.Add(state.liquidity, update.delta)
		if state.liquidity.Sign() < 0 || state.liquidity.BitLen() > 128 {
			return Snapshot{}, fmt.Errorf("liquidity update produces invalid active liquidity")
		}
	}
	return state, nil
}

func updateBoundary(ticks map[int32]Tick, index int32, grossDelta, netDelta *big.Int) error {
	current, exists := ticks[index]
	gross := new(big.Int)
	net := new(big.Int)
	if exists {
		gross.Set(current.liquidityGross)
		net.Set(current.liquidityNet)
	}
	if grossDelta.Sign() > 0 {
		gross.Add(gross, grossDelta)
	} else {
		gross.Sub(gross, new(big.Int).Abs(grossDelta))
	}
	if gross.Sign() < 0 {
		return fmt.Errorf("liquidity burn exceeds tick %d gross liquidity", index)
	}
	net.Add(net, netDelta)
	if gross.Sign() == 0 {
		if net.Sign() != 0 {
			return fmt.Errorf("cleared tick %d retains net liquidity", index)
		}
		delete(ticks, index)
		return nil
	}
	updated, err := NewTick(index, gross, net)
	if err != nil {
		return err
	}
	ticks[index] = updated
	return nil
}

func hashState(state Snapshot) [sha256.Size]byte {
	var canonical strings.Builder
	fmt.Fprintf(&canonical, "%d|%s|%d|%s|%d|%d", state.schemaVersion, state.sqrtPriceX96, state.tick, state.liquidity, state.feePips, state.tickSpacing)
	for _, initialized := range state.ticks {
		fmt.Fprintf(&canonical, "|%d:%s:%s", initialized.index, initialized.liquidityGross, initialized.liquidityNet)
	}
	return sha256.Sum256([]byte(canonical.String()))
}
