package livecompare

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
	"github.com/VarozXYZ/vernier/runtime/marketstream"
)

// WarmDirectionalCosts performs one startup-only remote evaluation and waits
// for the background complete-flow estimator to publish both directions. It
// does not start feeds, persist windows, send alerts, or broadcast.
func (r *Runner) WarmDirectionalCosts(ctx context.Context) error {
	if r.directionalCosts == nil {
		return nil
	}
	observer, ok := r.directionalCosts.(DirectionalCostObserver)
	if !ok || !r.allRemoteMarkets() {
		return fmt.Errorf("directional cost warmup requires remote observed markets")
	}
	registry, setup, err := r.registry()
	if err != nil {
		return err
	}
	sources := make(map[market.MarketID]quoteport.Source, len(r.config.Markets))
	snapshots := make([]market.MarketSnapshot, 0, len(r.config.Markets))
	now := r.clock().UTC()
	for _, configured := range r.config.Markets {
		runtime, buildErr := r.buildEventRefreshedMarket(configured, registry)
		if buildErr != nil {
			return buildErr
		}
		sources[configured.ID] = runtime.source
		hash := sha256.Sum256([]byte("complete-flow-warm/" + string(configured.ID)))
		snapshot, snapshotErr := market.NewMarketSnapshot(
			market.SnapshotMetadata{
				Market:  configured.ID,
				Source:  market.SourceID(configured.QuoteSource + "/cost-warm"),
				Version: 1, Finality: market.FinalityConfirmed,
				ReceivedAt: now, AppliedAt: now,
				Health: market.HealthHealthy, HealthChangedAt: now,
				StateHash: hash,
			},
			marketstream.EventSnapshotData{},
		)
		if snapshotErr != nil {
			return snapshotErr
		}
		snapshots = append(snapshots, snapshot)
	}
	candidate, err := r.newStrategy(registry, setup, sources)
	if err != nil {
		return err
	}
	_, fallback, directional, err := r.unavailableRemoteCosts(setup.Directions())
	if err != nil {
		return err
	}
	research, err := r.evaluateWithDirectionalCosts(
		ctx, candidate, snapshots, fallback, directional,
		"complete-flow-cost-warm/"+r.config.ResearchID,
		now, nil, false,
	)
	if err != nil {
		return fmt.Errorf("startup cost discovery quotes: %w", err)
	}
	observer.Observe(research.Opportunities)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		ready := true
		at := r.clock().UTC()
		for _, direction := range setup.Directions() {
			if _, found := r.directionalCosts.Snapshot(direction, at); !found {
				ready = false
				break
			}
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			if diagnostics, ok := r.directionalCosts.(interface{ LastError() error }); ok {
				if cause := diagnostics.LastError(); cause != nil {
					return fmt.Errorf(
						"warm complete-flow costs: %w: %v", ctx.Err(), cause,
					)
				}
			}
			return fmt.Errorf("warm complete-flow costs: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}
