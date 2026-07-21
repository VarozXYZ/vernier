package livecompare

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/adapters/feed/evmlogs"
	"github.com/VarozXYZ/vernier/adapters/feed/sourceorder"
	"github.com/VarozXYZ/vernier/core/marketstate"
	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

// StreamOptions controls the read-only continuous Research loop. Updates is
// the number of evaluations to emit after both markets have a snapshot; zero
// keeps the stream running until its context is canceled.
type StreamOptions struct {
	Updates  int
	OnReport func(Report) error
}

type streamMarket struct {
	runtime marketRuntime
	mirror  *marketstate.Mirror
	feed    *evmlogs.Feed
}

type streamSignal struct {
	market    market.MarketID
	triggered time.Time
}

// RunStream subscribes to every configured pool and evaluates the shared
// strategy whenever a mirror publishes a new snapshot or becomes degraded.
// It never asks a venue for a quote in the event loop and it does not infer
// gaps from block numbers. Feed reconnects own the full bootstrap lifecycle.
func (r *Runner) RunStream(ctx context.Context, options StreamOptions) error {
	if options.Updates < 0 {
		return fmt.Errorf("stream updates cannot be negative")
	}
	if options.OnReport == nil {
		options.OnReport = func(Report) error { return nil }
	}
	r.logger.Info("continuous research started", "run", r.config.RunID, "markets", len(r.config.Markets), "updates", options.Updates)
	blocks, err := r.currentBlocks(ctx)
	if err != nil {
		return err
	}
	r.logger.Info("stream cost reference blocks read", "blocks", blockSummary(blocks))
	registry, setup, err := r.registry()
	if err != nil {
		return err
	}
	maximum, _ := market.NewAssetQuantity(r.config.Markets[0].Base.Token.Asset, r.config.MaximumSize)
	markets := make(map[market.MarketID]*streamMarket, len(r.config.Markets))
	sources := make(map[market.MarketID]quoteport.Source, len(r.config.Markets))
	now := r.clock().UTC()
	for _, configured := range r.config.Markets {
		candidate, composeErr := r.composeMarket(configured, registry, maximum)
		if composeErr != nil {
			return composeErr
		}
		source := market.SourceID(configured.Venue.Chain + "/pool-logs")
		mirror, mirrorErr := marketstate.NewMirror(
			configured.ID, source, candidate.reducer,
			sourceorder.NewMonotonic(evmlogs.BlockPositionKind, false), r.clock,
		)
		if mirrorErr != nil {
			return mirrorErr
		}
		feed, feedErr := evmlogs.New(evmlogs.Config{
			Market: configured.ID, Source: source, Network: r.networks[configured.Venue.Chain], Venue: candidate.venue,
			Clock: r.clock, Logger: r.logger,
		})
		if feedErr != nil {
			return feedErr
		}
		markets[configured.ID] = &streamMarket{runtime: candidate, mirror: mirror, feed: feed}
		sources[configured.ID] = candidate.source
	}
	r.logger.Info("stream markets composed", "markets", len(markets))
	costEvidence, cost, err := r.cost(ctx, blocks, now)
	if err != nil {
		return err
	}
	r.logger.Info("stream cost snapshot ready", "source", costEvidence.Price.Source(), "asset", costEvidence.Cost.Asset())
	strategy, err := r.newStrategy(registry, setup, sources)
	if err != nil {
		return err
	}
	r.logger.Info("stream strategy ready", "strategy", strategy.ID())

	runCtx, cancel := context.WithCancel(ctx)
	signals := make(chan streamSignal)
	feedErrors := make(chan error, len(markets))
	var feedWG sync.WaitGroup
	for _, state := range markets {
		state := state
		feedWG.Add(1)
		go func() {
			defer feedWG.Done()
			err := state.feed.Run(runCtx, &streamSink{market: state.runtime.config.ID, mirror: state.mirror, signals: signals})
			if err != nil && !errors.Is(err, context.Canceled) {
				feedErrors <- err
			}
		}()
	}
	defer func() {
		cancel()
		feedWG.Wait()
	}()

	evaluations := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-feedErrors:
			r.logger.Error("stream feed failed", "error", err)
			return err
		case signal := <-signals:
			snapshots := make([]market.MarketSnapshot, 0, len(markets))
			ready := true
			for _, configured := range r.config.Markets {
				snapshot, ok := markets[configured.ID].mirror.Current()
				if !ok {
					ready = false
					break
				}
				snapshots = append(snapshots, snapshot)
			}
			if !ready {
				continue
			}
			metadata := make(map[string]any, len(snapshots))
			for _, snapshot := range snapshots {
				value := snapshot.Metadata()
				metadata[string(value.Market)] = map[string]any{"version": value.Version, "block": value.EventPosition.Value, "health": value.Health}
			}
			r.logger.Info("stream evaluation started", "triggered_at", signal.triggered, "snapshots", metadata)
			research, evaluateErr := r.evaluate(
				runCtx, strategy, snapshots, cost,
				fmt.Sprintf("stream-evaluation/%s/%d", r.config.ResearchID, evaluations+1), signal.triggered,
			)
			if evaluateErr != nil {
				return evaluateErr
			}
			research.Evaluations = evaluations + 1
			report := Report{Research: research, Cost: costEvidence}
			if callbackErr := options.OnReport(report); callbackErr != nil {
				return callbackErr
			}
			r.logger.Info("stream evaluation emitted", "evaluation", evaluations+1, "opportunities", len(research.Opportunities), "status", research.Status)
			evaluations++
			if options.Updates > 0 && evaluations >= options.Updates {
				return nil
			}
		}
	}
}

type streamSink struct {
	market  market.MarketID
	mirror  *marketstate.Mirror
	signals chan<- streamSignal
}

func (s *streamSink) Publish(ctx context.Context, event market.MarketEvent) error {
	result, err := s.mirror.Apply(ctx, event)
	if err != nil {
		return err
	}
	if result.Disposition == feedport.ApplyDispositionIgnoredStale {
		return nil
	}
	select {
	case s.signals <- streamSignal{market: s.market, triggered: event.ReceivedAt.UTC()}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *streamSink) SetHealth(ctx context.Context, update feedport.HealthUpdate) error {
	if err := s.mirror.SetHealth(ctx, update); err != nil {
		return err
	}
	if update.Health != market.HealthDegraded {
		return nil
	}
	if _, ok := s.mirror.Current(); !ok {
		return nil
	}
	select {
	case s.signals <- streamSignal{market: s.market, triggered: update.ObservedAt.UTC()}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var _ feedport.Sink = (*streamSink)(nil)
