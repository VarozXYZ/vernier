// Package strategy contains protocol-neutral opportunity evaluation algorithms.
package strategy

import (
	"context"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
)

type Strategy interface {
	ID() arbitrage.StrategyID
	Evaluate(context.Context, arbitrage.Evaluation) ([]arbitrage.Opportunity, error)
}
