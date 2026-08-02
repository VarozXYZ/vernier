// Package simulation defines read-only execution validation boundaries.
package simulation

import (
	"context"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
)

// LegSimulator validates one concrete transaction against the current chain
// state. Implementations must never broadcast.
type LegSimulator interface {
	Simulate(context.Context, arbitrage.SimulationLeg) (arbitrage.SimulationLeg, error)
}

// PairSimulator starts both legs of an opportunity concurrently. A temporary
// RPC failure is represented in the result and must not be converted into an
// economic window close by callers.
type PairSimulator interface {
	SimulatePair(context.Context, arbitrage.SimulationRequest) (arbitrage.SimulationRound, error)
}
