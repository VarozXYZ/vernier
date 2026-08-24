package livecompare

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/adapters/feed/evmactivity"
	"github.com/VarozXYZ/vernier/adapters/feed/evmlogs"
	"github.com/VarozXYZ/vernier/adapters/feed/solanalogs"
	"github.com/VarozXYZ/vernier/adapters/quote/jupiter"
	"github.com/VarozXYZ/vernier/adapters/quote/kyberswap"
	coreresearch "github.com/VarozXYZ/vernier/core/research"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
	"github.com/VarozXYZ/vernier/runtime/configuration"
	"github.com/VarozXYZ/vernier/runtime/marketstream"
	runtimeresearch "github.com/VarozXYZ/vernier/runtime/research"
)

type eventRefreshedRuntime struct {
	config      configuration.ResolvedMarket
	store       *marketstream.EventSnapshotStore
	source      quoteport.Source
	feeds       []eventTriggerFeed
	kyberMarket *kyberswap.MarketSource
	kyberClient *kyberswap.Source
}

type eventTriggerFeed struct {
	id   string
	feed feedport.Feed
}

const eventRefreshedRateLimitRetryDelay = 100 * time.Millisecond

func (r *Runner) hasEventRefreshedMarket() bool {
	for _, configured := range r.config.Markets {
		if configured.QuoteSource != "" {
			return true
		}
	}
	return false
}

// runEventRefreshedStream composes local route mirrors with remotely quoted,
// event-refreshed markets. Every accepted trigger produces one Evaluation.
// A Solana trigger advances the remote generation and forces new Jupiter
// quotes. Other-market triggers retain that generation and reuse exact or
// proportionally scaled quotes. Any cached result with positive net PnL is
// replaced by a fresh HTTP confirmation before it may be emitted. A 429
// without a compatible fallback gets one retry after a 100 ms coalescing
// window, using the newest snapshots observed during that window.
func (r *Runner) runEventRefreshedStream(ctx context.Context, options StreamOptions) error {
	if options.Updates < 0 {
		return fmt.Errorf("stream updates cannot be negative")
	}
	if r.config.SizingAsset != "quote" {
		return fmt.Errorf("event-refreshed Jupiter markets require quote-asset sizing")
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
	registry, setup, err := r.registry()
	if err != nil {
		return err
	}
	maximum, err := market.NewAssetQuantity(r.sizingAsset(), r.config.MaximumSize)
	if err != nil {
		return err
	}
	local := make(map[market.MarketID]routeRuntime)
	remote := make(map[market.MarketID]eventRefreshedRuntime)
	sources := make(map[market.MarketID]quoteport.Source, len(r.config.Markets))
	referenceSources := make(map[market.MarketID]quoteport.ExternalReferenceSource)
	for _, configured := range r.config.Markets {
		if configured.QuoteSource != "" {
			candidate, buildErr := r.buildEventRefreshedMarket(configured, registry)
			if buildErr != nil {
				return buildErr
			}
			remote[configured.ID] = candidate
			sources[configured.ID] = candidate.source
			continue
		}
		route, buildErr := r.buildRoute(ctx, configured, registry, maximum, blocks, nil, r.clock().UTC(), false)
		if buildErr != nil {
			return buildErr
		}
		source := route.source
		if configured.ReferenceQuote != "" {
			reference, referenceErr := r.externalSource(configured, source)
			if referenceErr != nil {
				return referenceErr
			}
			if reference != nil {
				source = reference
				referenceSources[configured.ID] = reference
			}
		}
		local[configured.ID] = route
		sources[configured.ID] = source
	}
	if len(local) == 0 || len(remote) == 0 {
		return fmt.Errorf("event-refreshed Research requires at least one local and one remote market")
	}
	now := r.clock().UTC()
	costEvidence, cost, err := r.cost(ctx, blocks, now)
	if err != nil {
		return err
	}
	candidate, err := r.newStrategy(registry, setup, sources)
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

	runCtx, cancel := context.WithCancel(ctx)
	for _, runtime := range remote {
		profile := r.config.QuoteSources[runtime.config.QuoteSource]
		if runtime.kyberMarket != nil && profile.RefreshInterval > 0 {
			// Keep the latest remote routes warm off the event hot path. The
			// timer is measured from quote receipt by MarketSource.
			runtime.kyberMarket.StartRefresh(runCtx, profile.RefreshInterval)
		}
	}
	signals := make(chan streamSignal, 256)
	type feedFailure struct {
		market market.MarketID
		err    error
	}
	failures := make(chan feedFailure, routeFeedCount(local)+eventFeedCount(remote))
	var feeds sync.WaitGroup
	var references sync.WaitGroup
	defer references.Wait()
	defer feeds.Wait()
	defer cancel()
	for routeID, route := range local {
		if route.batchFeed != nil {
			routeID, route := routeID, route
			feeds.Add(1)
			go func() {
				defer feeds.Done()
				runErr := route.batchFeed.Run(runCtx, &routeStreamSink{route: route.route,
					children: routeChildIDs(route), routeID: routeID, signals: signals})
				if runErr != nil && !errors.Is(runErr, context.Canceled) {
					failures <- feedFailure{market: routeID, err: runErr}
				}
			}()
			continue
		}
		for _, child := range route.children {
			child, routeID, route := child, routeID, route
			feeds.Add(1)
			go func() {
				defer feeds.Done()
				runErr := child.feed.Run(runCtx, &routeStreamSink{route: route.route, child: child.market.ID, routeID: routeID, signals: signals})
				if runErr != nil && !errors.Is(runErr, context.Canceled) {
					failures <- feedFailure{market: routeID, err: runErr}
				}
			}()
		}
	}
	for marketID, runtime := range remote {
		for _, configuredFeed := range runtime.feeds {
			configuredFeed, marketID, runtime := configuredFeed, marketID, runtime
			feeds.Add(1)
			go func() {
				defer feeds.Done()
				sink := &eventTriggerSink{market: marketID, feed: configuredFeed.id, store: runtime.store, signals: signals}
				runErr := configuredFeed.feed.Run(runCtx, sink)
				if runErr != nil && !errors.Is(runErr, context.Canceled) {
					failures <- feedFailure{market: marketID, err: runErr}
				}
			}()
		}
	}
	r.logger.Info("event-refreshed research stream started", "local_markets", len(local), "remote_markets", len(remote), "updates", options.Updates)
	evaluations := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case failure := <-failures:
			if tracker != nil {
				_ = tracker.FailMarket(runCtx, failure.market, "feed_failed", r.clock().UTC())
			}
			return failure.err
		case signal := <-signals:
			remoteTriggered := false
			if _, ok := remote[signal.market]; ok {
				remoteTriggered = true
				for _, runtime := range remote {
					if runtime.config.ID == signal.market && runtime.kyberMarket != nil {
						runtime.kyberMarket.Invalidate()
					}
				}
			}
			snapshots, ready := eventStreamSnapshots(r.config.Markets, local, remote)
			if !ready {
				continue
			}
			var research runtimeresearch.Report
			retried := false
			for {
				// Clear any marker left by a prior completed Evaluation before
				// observing this attempt.
				_ = takeEventRateLimitRetry(remote)
				if remoteTriggered {
					research, err = r.evaluateFresh(
						runCtx, candidate, snapshots, cost,
						fmt.Sprintf("event-stream/%s/%d", r.config.ResearchID, evaluations+1),
						signal.triggered, triggerPointer(signal),
					)
				} else {
					research, err = r.evaluate(
						runCtx, candidate, snapshots, cost,
						fmt.Sprintf("event-stream/%s/%d", r.config.ResearchID, evaluations+1),
						signal.triggered, triggerPointer(signal),
					)
				}
				if err != nil {
					return err
				}
				if needsFreshConfirmation(research.Opportunities) {
					snapshots, ready = eventStreamSnapshots(r.config.Markets, local, remote)
					if !ready {
						return fmt.Errorf("event-refreshed snapshots became unavailable during profitability confirmation")
					}
					r.logger.Info("cached profitable result requires fresh Jupiter confirmation",
						"evaluation", evaluations+1)
					research, err = r.evaluateFresh(
						runCtx, candidate, snapshots, cost,
						fmt.Sprintf("event-stream/%s/%d", r.config.ResearchID, evaluations+1),
						signal.triggered, triggerPointer(signal),
					)
					if err != nil {
						return err
					}
				}
				retryRequired := takeEventRateLimitRetry(remote)
				if !retryRequired || retried {
					if retryRequired {
						r.logger.Warn("Jupiter rate-limit retry exhausted", "evaluation", evaluations+1)
					}
					break
				}
				retried = true
				r.logger.Warn("Jupiter rate limit without fresh fallback; coalescing one retry",
					"evaluation", evaluations+1, "delay", eventRefreshedRateLimitRetryDelay)
				latest, found, waitErr := coalesceLatestSignal(
					runCtx, signals, eventRefreshedRateLimitRetryDelay,
					nil,
				)
				if waitErr != nil {
					if runCtx.Err() != nil {
						return nil
					}
					return waitErr
				}
				if found {
					signal = latest
				}
				snapshots, ready = eventStreamSnapshots(r.config.Markets, local, remote)
				if !ready {
					return fmt.Errorf("event-refreshed snapshots became unavailable during rate-limit retry")
				}
			}
			research.Evaluations = evaluations + 1
			report := Report{Research: research, Cost: costEvidence}
			if tracker != nil {
				for _, opportunity := range research.Opportunities {
					if _, err := tracker.Observe(runCtx, opportunity); err != nil {
						return err
					}
				}
			}
			if err := options.OnReport(report); err != nil {
				return err
			}
			if r.referencesEnabled() && len(referenceSources) > 0 {
				evaluationNumber := evaluations + 1
				fixedSnapshots := append([]market.MarketSnapshot(nil), snapshots...)
				fixedOpportunities := append([]arbitrage.Opportunity(nil), research.Opportunities...)
				available := r.referenceSourcesFor(sources)
				for id, source := range referenceSources {
					available[id] = source
				}
				references.Add(1)
				go func() {
					defer references.Done()
					comparisons := validateReferences(ctx, fixedOpportunities, fixedSnapshots, available, research, r.clock)
					_ = options.OnReference(ReferenceReport{Evaluation: evaluationNumber, Comparisons: comparisons})
				}()
			}
			r.logger.Info("event-refreshed evaluation emitted", "evaluation", evaluations+1, "trigger_market", signal.market, "local_duration", research.LocalTiming.Duration)
			evaluations++
			if options.Updates > 0 && evaluations >= options.Updates {
				return nil
			}
		}
	}
}

func (r *Runner) buildEventRefreshedMarket(configured configuration.ResolvedMarket, registry *market.Registry) (eventRefreshedRuntime, error) {
	profile, ok := r.config.QuoteSources[configured.QuoteSource]
	if !ok {
		return eventRefreshedRuntime{}, fmt.Errorf("market %q requires a configured quote source", configured.ID)
	}
	domainMarket, ok := registry.Market(configured.ID)
	if !ok {
		return eventRefreshedRuntime{}, fmt.Errorf("registry is missing market %q", configured.ID)
	}
	var source quoteport.Source
	var kyberDirect *kyberswap.Source
	switch profile.Kind {
	case "jupiter":
		taker := ""
		if profile.TakerEnv != "" {
			value, exists := r.lookup(profile.TakerEnv)
			if !exists || strings.TrimSpace(value) == "" {
				return eventRefreshedRuntime{}, fmt.Errorf(
					"jupiter taker environment %q is unset", profile.TakerEnv,
				)
			}
			taker = strings.TrimSpace(value)
		}
		var keys []string
		if profile.APIKeyEnv != "" {
			if value, exists := r.lookup(profile.APIKeyEnv); exists {
				keys = splitProviderKeys(value)
			}
		}
		if len(keys) == 0 {
			return eventRefreshedRuntime{}, fmt.Errorf("market %q requires Jupiter API keys", configured.ID)
		}
		direct, err := jupiter.NewQuoteSource(jupiter.QuoteConfig{
			ID: market.SourceID(profile.ID + "/http"), BaseURL: profile.BaseURL,
			QuotePath: profile.QuotePath, ExpectedMode: profile.ExpectedMode,
			APIKeys: keys, SlippageBPS: profile.SlippageBPS,
			Taker: taker, SwapMode: profile.SwapMode,
			PriorityFeeLamports: profile.PriorityFeeLamports,
			BroadcastFeeType:    profile.BroadcastFeeType, UseWSOL: profile.UseWSOL,
			ExcludeDexes: profile.ExcludeDexes, ExcludeRouters: profile.ExcludeRouters,
			ClientPlatform: profile.ClientPlatform,
			Limiter:        jupiter.ImmediateLimiter{}, Clock: r.clock,
		})
		if err != nil {
			return eventRefreshedRuntime{}, err
		}
		source, err = jupiter.NewMarketSource(jupiter.MarketSourceConfig{
			ID: market.SourceID(profile.ID), Market: domainMarket, Client: direct,
			Clock: r.clock, FreshOnly: r.config.EvaluationMode == "best_buy_opposite_sell",
			TokenMints: map[market.TokenID]string{
				configured.Base.Token.ID:  configured.Base.AddressText,
				configured.Quote.Token.ID: configured.Quote.AddressText,
			},
		})
		if err != nil {
			return eventRefreshedRuntime{}, err
		}
	case "kyberswap":
		clientID, exists := r.lookup(profile.ClientIDEnv)
		if !exists || strings.TrimSpace(clientID) == "" {
			return eventRefreshedRuntime{}, fmt.Errorf("KyberSwap client ID environment %q is unset", profile.ClientIDEnv)
		}
		direct, err := kyberswap.New(kyberswap.Config{BaseURL: profile.BaseURL, ClientID: clientID})
		if err != nil {
			return eventRefreshedRuntime{}, err
		}
		kyberDirect = direct
		kyberMarket, sourceErr := kyberswap.NewMarketSource(kyberswap.MarketSourceConfig{
			ID: market.SourceID(profile.ID), Market: domainMarket, Client: direct, Chain: profile.ChainSlug,
			CacheEnabled: profile.RefreshInterval > 0,
			TokenAddresses: map[market.TokenID]string{
				configured.Base.Token.ID: configured.Base.AddressText, configured.Quote.Token.ID: configured.Quote.AddressText,
			},
		})
		if sourceErr != nil {
			return eventRefreshedRuntime{}, sourceErr
		}
		source = kyberMarket
	default:
		return eventRefreshedRuntime{}, fmt.Errorf("market %q has unsupported quote source %q", configured.ID, profile.Kind)
	}
	feedIDs := make([]string, len(configured.TriggerPools))
	for index, pool := range configured.TriggerPools {
		feedIDs[index] = pool.ID
	}
	store, err := marketstream.NewEventSnapshotStore(
		configured.ID, market.SourceID(profile.ID+"/events"), feedIDs, r.clock,
	)
	if err != nil {
		return eventRefreshedRuntime{}, err
	}
	feeds := make([]eventTriggerFeed, 0, len(configured.TriggerPools))
	for index, pool := range configured.TriggerPools {
		triggerMarket := market.MarketID(fmt.Sprintf("%s/trigger/%d", configured.ID, index))
		triggerSource := market.SourceID(fmt.Sprintf("%s/pool-activity/%d", pool.Chain, index))
		var feed feedport.Feed
		if r.config.Chains[pool.Chain].Kind == "solana" {
			network := r.solanaNetworks[pool.Chain]
			if network == nil {
				return eventRefreshedRuntime{}, fmt.Errorf("trigger pool %q requires Solana network %q", pool.ID, pool.Chain)
			}
			candidate, feedErr := solanalogs.New(solanalogs.Config{
				Market: triggerMarket, Source: triggerSource, Pool: pool.Address,
				Network: network, Decoder: solanalogs.TriggerDecoder{Kind: pool.Kind}, Clock: r.clock, Logger: r.logger,
			})
			if feedErr != nil {
				return eventRefreshedRuntime{}, feedErr
			}
			feed = candidate
		} else {
			venue, venueErr := evmactivity.NewVenue(pool.ID, pool.Address)
			if venueErr != nil {
				return eventRefreshedRuntime{}, venueErr
			}
			candidate, feedErr := evmlogs.New(evmlogs.Config{
				Market: triggerMarket, Source: triggerSource, Network: r.networks[pool.Chain],
				Venue: venue, Clock: r.clock, Logger: r.logger,
			})
			if feedErr != nil {
				return eventRefreshedRuntime{}, feedErr
			}
			feed = candidate
		}
		feeds = append(feeds, eventTriggerFeed{id: pool.ID, feed: feed})
	}
	result := eventRefreshedRuntime{config: configured, store: store, source: source, feeds: feeds}
	if typed, ok := source.(*kyberswap.MarketSource); ok {
		result.kyberMarket = typed
		result.kyberClient = kyberDirect
	}
	return result, nil
}

func takeEventRateLimitRetry(remote map[market.MarketID]eventRefreshedRuntime) bool {
	required := false
	for _, candidate := range remote {
		if source, ok := candidate.source.(interface{ TakeRateLimitRetry() bool }); ok && source.TakeRateLimitRetry() {
			required = true
		}
	}
	return required
}

func needsFreshConfirmation(opportunities []arbitrage.Opportunity) bool {
	for _, opportunity := range opportunities {
		if opportunity.SelectedIndex < 0 || opportunity.SelectedIndex >= len(opportunity.Candidates) {
			continue
		}
		selected := opportunity.Candidates[opportunity.SelectedIndex]
		if selected.NetPnL.Sign() > 0 &&
			(selected.BuyQuote.Quality.RequiresRefresh() || selected.SellQuote.Quality.RequiresRefresh()) {
			return true
		}
	}
	return false
}

func coalesceLatestSignal(
	ctx context.Context,
	signals <-chan streamSignal,
	delay time.Duration,
	apply func(streamSignal) error,
) (streamSignal, bool, error) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	var (
		latest streamSignal
		found  bool
	)
	accept := func(signal streamSignal) error {
		if apply != nil {
			if err := apply(signal); err != nil {
				return err
			}
		}
		latest, found = signal, true
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return streamSignal{}, false, ctx.Err()
		case signal := <-signals:
			if err := accept(signal); err != nil {
				return streamSignal{}, false, err
			}
		case <-timer.C:
			// Drain events already queued at the deadline so the retry uses
			// the latest snapshot represented by the channel.
			for {
				select {
				case signal := <-signals:
					if err := accept(signal); err != nil {
						return streamSignal{}, false, err
					}
				default:
					return latest, found, nil
				}
			}
		}
	}
}

func eventStreamSnapshots(order [2]configuration.ResolvedMarket, local map[market.MarketID]routeRuntime, remote map[market.MarketID]eventRefreshedRuntime) ([]market.MarketSnapshot, bool) {
	result := make([]market.MarketSnapshot, 0, len(order))
	for _, configured := range order {
		if candidate, ok := remote[configured.ID]; ok {
			snapshot, exists := candidate.store.Current()
			if !exists {
				return nil, false
			}
			result = append(result, snapshot)
			continue
		}
		candidate, ok := local[configured.ID]
		if !ok {
			return nil, false
		}
		snapshot, exists := candidate.route.Snapshot()
		if !exists {
			return nil, false
		}
		result = append(result, snapshot)
	}
	return result, true
}

func splitProviderKeys(value string) []string {
	var result []string
	for _, key := range strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\r'
	}) {
		if key = strings.TrimSpace(key); key != "" {
			result = append(result, key)
		}
	}
	return result
}

func routeFeedCount(routes map[market.MarketID]routeRuntime) int {
	count := 0
	for _, route := range routes {
		if route.batchFeed != nil {
			count++
			continue
		}
		count += len(route.children)
	}
	return count
}

func eventFeedCount(markets map[market.MarketID]eventRefreshedRuntime) int {
	count := 0
	for _, candidate := range markets {
		count += len(candidate.feeds)
	}
	return count
}

type eventTriggerSink struct {
	market  market.MarketID
	feed    string
	store   *marketstream.EventSnapshotStore
	signals chan<- streamSignal
}

func (s *eventTriggerSink) Publish(ctx context.Context, event market.MarketEvent) error {
	changed, err := s.store.Publish(ctx, s.feed, event)
	if err != nil || !changed {
		return err
	}
	return s.signal(ctx, event.ReceivedAt, &arbitrage.TriggerMetadata{
		Market: s.market, Source: event.Source, Position: event.Position,
		Reference: event.Reference, At: event.ReceivedAt.UTC(),
	})
}

func (s *eventTriggerSink) Reset(ctx context.Context, event market.MarketEvent) error {
	_, err := s.store.Reset(ctx, s.feed, event)
	return err
}

func (s *eventTriggerSink) SetHealth(ctx context.Context, update feedport.HealthUpdate) error {
	changed, err := s.store.SetHealth(ctx, s.feed, update)
	if err != nil || !changed {
		return err
	}
	snapshot, ok := s.store.Current()
	if !ok {
		return nil
	}
	metadata := snapshot.Metadata()
	return s.signal(ctx, update.ObservedAt, &arbitrage.TriggerMetadata{
		Market: s.market, Source: metadata.Source, Position: metadata.EventPosition,
		Reference: metadata.EventReference, At: update.ObservedAt.UTC(),
	})
}

func (s *eventTriggerSink) signal(ctx context.Context, at time.Time, trigger *arbitrage.TriggerMetadata) error {
	signal := streamSignal{market: s.market, triggered: at.UTC()}
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

var _ feedport.Sink = (*eventTriggerSink)(nil)
var _ feedport.ResetSink = (*eventTriggerSink)(nil)
