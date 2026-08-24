package livecompare

import (
	"context"
	"fmt"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

// SimulationCanaryResult is a read-only smoke-test result. It deliberately
// reports the local quote and the simulation outcome separately, including
// unprofitable quotes, so the canary can validate execution mechanics without
// waiting for a market opportunity.
type SimulationCanaryResult struct {
	Direction arbitrage.Direction
	Candidate arbitrage.Candidate
	Round     arbitrage.SimulationRound
	Class     arbitrage.Classification
	Reasons   []string
	Duration  time.Duration
}

func (r *Runner) RunSimulationCanary(ctx context.Context, input market.AssetQuantity) ([]SimulationCanaryResult, error) {
	if !r.config.SimulationEnabled {
		return nil, fmt.Errorf("research simulation is disabled in configuration")
	}
	blocks, err := r.currentBlocks(ctx)
	if err != nil {
		return nil, err
	}
	slots, err := r.currentSlots(ctx)
	if err != nil {
		return nil, err
	}
	registry, setup, err := r.registry()
	if err != nil {
		return nil, err
	}
	routes := make(map[market.MarketID]routeRuntime, len(r.config.Markets))
	sources := make(map[market.MarketID]quoteport.Source, len(r.config.Markets))
	snapshots := make([]market.MarketSnapshot, 0, len(r.config.Markets))
	now := r.clock().UTC()
	for _, configured := range r.config.Markets {
		route, buildErr := r.buildRoute(ctx, configured, registry, input, blocks, slots, now, true)
		if buildErr != nil {
			return nil, fmt.Errorf("bootstrap canary route %s: %w", configured.ID, buildErr)
		}
		snapshot, ready := route.route.Snapshot()
		if !ready {
			return nil, fmt.Errorf("canary route %s did not publish a snapshot", configured.ID)
		}
		routes[configured.ID] = route
		sources[configured.ID] = route.source
		snapshots = append(snapshots, snapshot)
	}
	_ = routes
	r.rememberSimulationSnapshots(snapshots)
	_, cost, err := r.cost(ctx, blocks, now)
	if err != nil {
		return nil, err
	}
	strategy, err := r.newStrategy(registry, setup, sources)
	if err != nil {
		return nil, err
	}
	pinned, ok := strategy.(pinnedStrategy)
	if !ok {
		return nil, fmt.Errorf("configured strategy does not support simulation canary")
	}
	simulator := newPairSimulator(r)
	results := make([]SimulationCanaryResult, 0, len(setup.Directions()))
	for _, direction := range setup.Directions() {
		started := r.clock().UTC()
		evaluation, err := arbitrage.NewEvaluation(
			arbitrage.EvaluationID(fmt.Sprintf("simulation-canary/%s", direction)),
			arbitrage.ResearchRunID(r.config.RunID), pinned.ID(), r.config.Hash,
			snapshots, cost, started, started,
		)
		if err != nil {
			return nil, err
		}
		opportunity, _, err := pinned.EvaluatePinnedWithTiming(ctx, evaluation, direction, input)
		if err != nil {
			return nil, err
		}
		result := SimulationCanaryResult{Direction: direction, Class: opportunity.Classification, Reasons: opportunity.Reasons, Duration: r.clock().UTC().Sub(started)}
		if opportunity.SelectedIndex < 0 || opportunity.SelectedIndex >= len(opportunity.Candidates) {
			result.Round = arbitrage.SimulationRound{Status: arbitrage.SimulationUnavailable, FailureClass: arbitrage.SimulationFailureModelMismatch, Error: "fresh local quote did not produce a candidate"}
			results = append(results, result)
			continue
		}
		result.Candidate = opportunity.Candidates[opportunity.SelectedIndex]
		request, ok := r.simulationRequestForCandidate(arbitrage.WindowID(fmt.Sprintf("simulation-canary/%s", direction)), 1, result.Candidate, snapshots, opportunity.Classification == arbitrage.ClassificationPolicyQualified)
		if !ok {
			return nil, fmt.Errorf("canary candidate does not map to both route snapshots")
		}
		round, err := simulator.SimulatePair(ctx, request)
		if err != nil {
			return nil, err
		}
		result.Round = round
		result.Duration = r.clock().UTC().Sub(started)
		results = append(results, result)
	}
	return results, nil
}
