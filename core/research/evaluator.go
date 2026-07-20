// Package research orchestrates deterministic Research evaluations.
package research

import (
	"context"
	"fmt"
	"time"

	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
)

type EvaluationRequest struct {
	IDPrefix       string
	Run            arbitrage.ResearchRunID
	ConfigHash     string
	Snapshots      []market.MarketSnapshot
	Cost           arbitrage.CostSnapshot
	TriggeredAt    time.Time
	StartedAt      time.Time
	MaxSnapshotAge time.Duration
}

type Evaluator struct {
	strategies []strategy.Strategy
}

func NewEvaluator(strategies []strategy.Strategy) (*Evaluator, error) {
	if len(strategies) == 0 {
		return nil, fmt.Errorf("at least one strategy is required")
	}
	seen := make(map[arbitrage.StrategyID]struct{}, len(strategies))
	for _, candidate := range strategies {
		if candidate == nil || candidate.ID() == "" {
			return nil, fmt.Errorf("valid strategy is required")
		}
		if _, duplicate := seen[candidate.ID()]; duplicate {
			return nil, fmt.Errorf("duplicate strategy %q", candidate.ID())
		}
		seen[candidate.ID()] = struct{}{}
	}
	return &Evaluator{strategies: append([]strategy.Strategy(nil), strategies...)}, nil
}

func (e *Evaluator) Evaluate(ctx context.Context, request EvaluationRequest) ([]arbitrage.Opportunity, error) {
	if request.IDPrefix == "" {
		return nil, fmt.Errorf("evaluation ID prefix is required")
	}
	var opportunities []arbitrage.Opportunity
	for _, candidate := range e.strategies {
		evaluation, err := arbitrage.NewEvaluation(
			arbitrage.EvaluationID(fmt.Sprintf("%s/%s", request.IDPrefix, candidate.ID())),
			request.Run, candidate.ID(), request.ConfigHash, request.Snapshots, request.Cost,
			request.TriggeredAt, request.StartedAt, request.MaxSnapshotAge,
		)
		if err != nil {
			return nil, err
		}
		result, err := candidate.Evaluate(ctx, evaluation)
		if err != nil {
			return nil, err
		}
		opportunities = append(opportunities, result...)
	}
	return opportunities, nil
}
