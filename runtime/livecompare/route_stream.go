package livecompare

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	coreresearch "github.com/VarozXYZ/vernier/core/research"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
	"github.com/VarozXYZ/vernier/runtime/crosschain"
)

func (r *Runner) runRouteStream(ctx context.Context, options StreamOptions) error {
	if options.Updates < 0 {
		return fmt.Errorf("stream updates cannot be negative")
	}
	if options.OnReport == nil {
		options.OnReport = func(Report) error { return nil }
	}
	if options.OnReference == nil {
		options.OnReference = func(ReferenceReport) error { return nil }
	}
	blocks, err := r.currentBlocks(ctx)
	if err != nil {
		return err
	}
	// Route-stream feeds own the current-state bootstrap. Building a route must
	// not perform a second synchronous bootstrap before those feeds start.
	slots := map[string]uint64{}
	registry, setup, err := r.registry()
	if err != nil {
		return err
	}
	maximum, err := market.NewAssetQuantity(r.sizingAsset(), r.config.MaximumSize)
	if err != nil {
		return err
	}
	routes := make(map[market.MarketID]routeRuntime, len(r.config.Markets))
	sources := make(map[market.MarketID]quoteport.Source, len(r.config.Markets))
	now := r.clock().UTC()
	for _, configured := range r.config.Markets {
		route, err := r.buildRoute(ctx, configured, registry, maximum, blocks, slots, now, false)
		if err != nil {
			return err
		}
		routes[configured.ID] = route
		source := quoteport.Source(route.route.Source)
		if configured.ReferenceQuote != "" {
			reference, err := r.externalSource(configured, source)
			if err != nil {
				return err
			}
			if reference != nil {
				source = reference
			}
		}
		sources[configured.ID] = source
	}
	costEvidence, cost, err := r.cost(ctx, blocks, now)
	if err != nil {
		return err
	}
	strategy, err := r.newStrategy(registry, setup, sources)
	if err != nil {
		return err
	}
	var tracker *coreresearch.WindowTracker
	if options.OpportunityStore != nil {
		tracker, err = coreresearch.NewWindowTracker(options.OpportunityStore, r.clock)
		if err != nil {
			return err
		}
		if err := tracker.Start(ctx); err != nil {
			return err
		}
	}
	r.logger.Info("route stream started", "run", r.config.RunID, "markets", len(routes), "hops", routeHopCount(routes), "updates", options.Updates)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	signals := make(chan streamSignal, 64)
	type failure struct {
		market market.MarketID
		err    error
	}
	failures := make(chan failure, len(routes)*2)
	var feeds sync.WaitGroup
	for routeID, route := range routes {
		for _, child := range route.children {
			child := child
			routeID := routeID
			feeds.Add(1)
			go func() {
				defer feeds.Done()
				err := child.feed.Run(runCtx, &routeStreamSink{route: route.route, child: child.market.ID, routeID: routeID, signals: signals})
				if err != nil && !errors.Is(err, context.Canceled) {
					failures <- failure{market: routeID, err: err}
				}
			}()
		}
	}
	defer feeds.Wait()
	var references sync.WaitGroup
	defer references.Wait()
	evaluations := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case result := <-failures:
			if tracker != nil {
				_ = tracker.FailMarket(runCtx, result.market, "feed_failed", r.clock().UTC())
			}
			return result.err
		case signal := <-signals:
			snapshots := make([]market.MarketSnapshot, 0, len(r.config.Markets))
			ready := true
			for _, configured := range r.config.Markets {
				snapshot, ok := routes[configured.ID].route.Snapshot()
				if !ok {
					ready = false
					break
				}
				snapshots = append(snapshots, snapshot)
			}
			if !ready {
				continue
			}
			research, err := r.evaluate(runCtx, strategy, snapshots, cost, fmt.Sprintf("route-stream/%s/%d", r.config.ResearchID, evaluations+1), signal.triggered, triggerPointer(signal))
			if err != nil {
				return err
			}
			research.Evaluations = evaluations + 1
			report := Report{Research: research, Cost: costEvidence}
			if tracker != nil {
				for _, opportunity := range research.Opportunities {
					if err := tracker.Observe(runCtx, opportunity); err != nil {
						return err
					}
				}
			}
			if err := options.OnReport(report); err != nil {
				return err
			}
			if len(r.config.QuoteSources) > 0 || len(r.referenceSources) > 0 {
				referenceEvaluation := evaluations + 1
				referenceSnapshots := append([]market.MarketSnapshot(nil), snapshots...)
				referenceOpportunities := append([]arbitrage.Opportunity(nil), research.Opportunities...)
				available := r.referenceSourcesFor(sources)
				references.Add(1)
				go func() {
					defer references.Done()
					comparisons := validateReferences(ctx, referenceOpportunities, referenceSnapshots, available, research, r.clock)
					if options.OnReference != nil {
						_ = options.OnReference(ReferenceReport{Evaluation: referenceEvaluation, Comparisons: comparisons})
					}
				}()
			}
			evaluations++
			if options.Updates > 0 && evaluations >= options.Updates {
				return nil
			}
		}
	}
}

func routeHopCount(routes map[market.MarketID]routeRuntime) int {
	count := 0
	for _, route := range routes {
		count += len(route.children)
	}
	return count
}

type routeStreamSink struct {
	route   *crosschain.Route
	child   market.MarketID
	routeID market.MarketID
	signals chan<- streamSignal
}

func (s *routeStreamSink) Publish(ctx context.Context, event market.MarketEvent) error {
	result, err := s.route.Apply(ctx, event)
	if err != nil || result.Disposition == feedport.ApplyDispositionIgnoredStale {
		return err
	}
	return s.signal(ctx, event.ReceivedAt, &arbitrage.TriggerMetadata{Market: event.Market, Source: event.Source, Position: event.Position, Reference: event.Reference, At: event.ReceivedAt.UTC()})
}

func (s *routeStreamSink) Reset(ctx context.Context, event market.MarketEvent) error {
	result, err := s.route.Reset(ctx, event)
	if err != nil || result.Disposition == feedport.ApplyDispositionIgnoredStale {
		return err
	}
	return s.signal(ctx, event.ReceivedAt, &arbitrage.TriggerMetadata{Market: event.Market, Source: event.Source, Position: event.Position, Reference: event.Reference, At: event.ReceivedAt.UTC()})
}

func (s *routeStreamSink) SetHealth(ctx context.Context, update feedport.HealthUpdate) error {
	if err := s.route.SetChildHealth(ctx, s.child, update); err != nil {
		return err
	}
	if update.Health != market.HealthDegraded {
		return nil
	}
	if _, ok := s.route.Snapshot(); !ok {
		return nil
	}
	return s.signal(ctx, update.ObservedAt, nil)
}

func (s *routeStreamSink) signal(ctx context.Context, at time.Time, trigger *arbitrage.TriggerMetadata) error {
	signal := streamSignal{market: s.routeID, triggered: at.UTC()}
	if trigger != nil {
		signal.trigger = *trigger
		signal.hasTrigger = true
	}
	select {
	case s.signals <- signal:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var _ feedport.Sink = (*routeStreamSink)(nil)
