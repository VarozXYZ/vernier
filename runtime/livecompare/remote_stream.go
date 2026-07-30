package livecompare

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	coreresearch "github.com/VarozXYZ/vernier/core/research"
	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	notificationport "github.com/VarozXYZ/vernier/ports/notification"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
	"github.com/VarozXYZ/vernier/runtime/configuration"
	runtimeresearch "github.com/VarozXYZ/vernier/runtime/research"
)

func (r *Runner) allRemoteMarkets() bool {
	for _, configured := range r.config.Markets {
		if configured.QuoteSource == "" {
			return false
		}
	}
	return true
}

// runRemoteAggregatorStream coordinates trigger-only watchers for two remote
// aggregators. Evaluations are serialized; events that arrive while one is
// running collapse into exactly one follow-up using the newest trigger.
func (r *Runner) runRemoteAggregatorStream(ctx context.Context, options StreamOptions) error {
	if options.Updates < 0 {
		return fmt.Errorf("stream updates cannot be negative")
	}
	if r.config.TelegramEnabled && options.OpportunityStore == nil {
		return fmt.Errorf("telegram opening alerts require an opportunity store")
	}
	if options.OnQualifiedOpening != nil && options.OpportunityStore == nil {
		return fmt.Errorf("qualified opening callback requires an opportunity store")
	}
	if options.OnReport == nil {
		options.OnReport = func(Report) error { return nil }
	}
	registry, setup, err := r.registry()
	if err != nil {
		return err
	}
	remote := make(map[market.MarketID]eventRefreshedRuntime, 2)
	sources := make(map[market.MarketID]quoteport.Source, 2)
	for _, configured := range r.config.Markets {
		candidate, buildErr := r.buildEventRefreshedMarket(configured, registry)
		if buildErr != nil {
			return buildErr
		}
		remote[configured.ID] = candidate
		sources[configured.ID] = candidate.source
	}
	now := r.clock().UTC()
	var (
		fixedCostEvidence CostEvidence
		fixedCost         arbitrage.CostSnapshot
	)
	if r.directionalCosts == nil {
		blocks, blockErr := r.currentBlocks(ctx)
		if blockErr != nil {
			return blockErr
		}
		fixedCostEvidence, fixedCost, err = r.cost(ctx, blocks, now)
		if err != nil {
			return err
		}
	}
	candidate, err := r.newStrategy(registry, setup, sources)
	if err != nil {
		return err
	}
	var tracker *coreresearch.WindowTracker
	if options.OpportunityStore != nil {
		tracker, err = coreresearch.NewWindowTrackerWithQualification(
			options.OpportunityStore, r.clock, r.config.WindowQualification,
		)
		if err != nil {
			return err
		}
		if err := tracker.Start(ctx); err != nil {
			return err
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	signals := make(chan streamSignal, 256)
	failures := make(chan error, eventFeedCount(remote))
	var feeds sync.WaitGroup
	for marketID, runtime := range remote {
		for _, configuredFeed := range runtime.feeds {
			marketID, runtime, configuredFeed := marketID, runtime, configuredFeed
			feeds.Add(1)
			go func() {
				defer feeds.Done()
				sink := &eventTriggerSink{market: marketID, feed: configuredFeed.id, store: runtime.store, signals: signals}
				if runErr := configuredFeed.feed.Run(runCtx, sink); runErr != nil && !errors.Is(runErr, context.Canceled) {
					select {
					case failures <- runErr:
					case <-runCtx.Done():
					}
				}
			}()
		}
	}
	defer func() {
		cancel()
		feeds.Wait()
	}()

	idleInterval := r.config.IdleEvaluationInterval
	var idle *time.Timer
	var idleC <-chan time.Time
	if idleInterval > 0 {
		idle = time.NewTimer(idleInterval)
		idleC = idle.C
		defer idle.Stop()
	}
	resetIdle := func() {
		if idle == nil {
			return
		}
		if !idle.Stop() {
			select {
			case <-idle.C:
			default:
			}
		}
		idle.Reset(idleInterval)
	}
	stopIdle := func() {
		if idle == nil {
			return
		}
		if !idle.Stop() {
			select {
			case <-idle.C:
			default:
			}
		}
	}

	gate := runtimeresearch.NewTriggerGate(10_000)
	evaluations := 0
	idleSequence := uint64(0)
	retrySequence := uint64(0)
	var pending *streamSignal
	for {
		if options.EvaluationGate != nil &&
			!options.EvaluationGate.EvaluationAllowed() {
			stopIdle()
			for !options.EvaluationGate.EvaluationAllowed() {
				select {
				case <-ctx.Done():
					return nil
				case failure := <-failures:
					if tracker != nil {
						_ = tracker.FailMarket(
							runCtx, "", "feed_failed", r.clock().UTC(),
						)
					}
					return failure
				case signal := <-signals:
					if gate.Accept(scheduledRemoteTrigger(
						signal,
						r.config.Markets,
					)) {
						pending = &signal
					}
				case at := <-options.ReevaluationRequests:
					retrySequence++
					signal := syntheticRetrySignal(
						r.config.Markets[0].ID,
						at.UTC(),
						retrySequence,
					)
					pending = &signal
				case <-options.EvaluationGate.EvaluationChanges():
				}
			}
			latest := drainLatestRemoteSignal(
				signals,
				r.config.Markets,
				gate,
			)
			if latest != nil {
				pending = latest
			}
			if pending != nil {
				// A real trigger is stronger than a synthetic post-flow
				// request. Drain requests queued before the gate opened so
				// one completed flow produces exactly one evaluation.
				for {
					select {
					case <-options.ReevaluationRequests:
					default:
						goto reevaluationRequestsDrained
					}
				}
			}
		reevaluationRequestsDrained:
			resetIdle()
		}
		var signal streamSignal
		if pending != nil {
			signal = *pending
			pending = nil
		} else {
			select {
			case <-ctx.Done():
				return nil
			case failure := <-failures:
				if tracker != nil {
					_ = tracker.FailMarket(runCtx, "", "feed_failed", r.clock().UTC())
				}
				return failure
			case signal = <-signals:
				if !gate.Accept(scheduledRemoteTrigger(signal, r.config.Markets)) {
					continue
				}
				resetIdle()
			case at := <-idleC:
				idleSequence++
				signal = syntheticIdleSignal(r.config.Markets[0].ID, at.UTC(), idleSequence)
				resetIdle()
			case at := <-options.ReevaluationRequests:
				// Prefer a real event already waiting in the feed queue. When
				// none exists, synthesize one invalidation against the newest
				// snapshots so a rejected first transaction is not silently
				// lost.
				latest := drainLatestRemoteSignal(
					signals,
					r.config.Markets,
					gate,
				)
				if latest != nil {
					signal = *latest
				} else {
					retrySequence++
					signal = syntheticRetrySignal(
						r.config.Markets[0].ID,
						at.UTC(),
						retrySequence,
					)
				}
				resetIdle()
			}
		}

		if err := refreshRemoteSnapshots(runCtx, signal, remote); err != nil {
			return err
		}
		snapshots, ready, healthy := remoteSnapshots(r.config.Markets, remote)
		if !ready || !healthy {
			if tracker != nil && ready {
				_ = tracker.FailMarket(runCtx, signal.market, "websocket_disconnected", r.clock().UTC())
			}
			continue
		}
		costEvidence, cost, directionalCosts, costsReady := r.remoteEvaluationCosts(
			setup.Directions(), fixedCostEvidence, fixedCost,
		)
		if !costsReady {
			r.logger.Warn(
				"complete-flow cost cache is unavailable or stale; evaluation cannot qualify",
				"reason", "cost_cache_stale",
			)
			costEvidence, cost, directionalCosts, err =
				r.unavailableRemoteCosts(setup.Directions())
			if err != nil {
				return err
			}
		}
		research, evaluateErr := r.evaluateWithDirectionalCosts(
			runCtx, candidate, snapshots, cost, directionalCosts,
			fmt.Sprintf("remote-stream/%s/%d", r.config.ResearchID, evaluations+1),
			signal.triggered, triggerPointer(signal), false,
		)
		r.dispatchQuoteWarnings(sources)
		if evaluateErr != nil {
			return evaluateErr
		}
		if observer, ok := r.directionalCosts.(DirectionalCostObserver); ok {
			observer.Observe(research.Opportunities)
		}
		if !costsReady {
			for index := range research.Opportunities {
				research.Opportunities[index].SelectedIndex = -1
				research.Opportunities[index].Classification =
					arbitrage.ClassificationUnclassifiable
				research.Opportunities[index].Reasons =
					[]string{"cost_cache_stale"}
			}
		}
		research.Evaluations = evaluations + 1
		if tracker != nil {
			for _, opportunity := range research.Opportunities {
				transition, observeErr := tracker.Observe(runCtx, opportunity)
				if observeErr != nil {
					return observeErr
				}
				if transition.Kind == coreresearch.WindowTransitionOpened && r.openingAlerts != nil {
					alert, alertErr := buildOpeningAlert(opportunity, research.LocalTiming, registry)
					if alertErr != nil {
						r.logger.Error("build opening alert failed", "error", alertErr)
					} else {
						go r.sendOpeningAlert(alert)
					}
				}
				if transition.Kind == coreresearch.WindowTransitionOpened &&
					opportunity.Classification == arbitrage.ClassificationPolicyQualified &&
					options.OnQualifiedOpening != nil {
					if callbackErr := options.OnQualifiedOpening(opportunity); callbackErr != nil {
						return fmt.Errorf("dispatch qualified opening: %w", callbackErr)
					}
				}
				if opportunity.Classification == arbitrage.ClassificationPolicyQualified &&
					options.OnQualifiedOpportunity != nil {
					if callbackErr := options.OnQualifiedOpportunity(opportunity); callbackErr != nil {
						return fmt.Errorf("dispatch qualified opportunity: %w", callbackErr)
					}
				}
			}
		}
		if callbackErr := options.OnReport(Report{Research: research, Cost: costEvidence}); callbackErr != nil {
			return callbackErr
		}
		evaluations++
		if options.Updates > 0 && evaluations >= options.Updates {
			return nil
		}

		// Collapse all events observed during the completed evaluation into
		// one immediate follow-up carrying the newest trigger.
		pending = drainLatestRemoteSignal(signals, r.config.Markets, gate)
		if pending != nil {
			resetIdle()
		}
	}
}

func (r *Runner) unavailableRemoteCosts(
	directions []arbitrage.Direction,
) (CostEvidence, arbitrage.CostSnapshot, map[arbitrage.Direction]arbitrage.CostSnapshot, error) {
	amount, err := market.NewAssetQuantity(
		r.config.Markets[0].Quote.Token.Asset, new(big.Rat),
	)
	if err != nil {
		return CostEvidence{}, arbitrage.CostSnapshot{}, nil, err
	}
	at := r.clock().UTC()
	fallback := arbitrage.CostSnapshot{
		ID: "complete-flow/unavailable", Amount: amount, CapturedAt: at,
	}
	costs := make(map[arbitrage.Direction]arbitrage.CostSnapshot, len(directions))
	for _, direction := range directions {
		costs[direction] = fallback
	}
	return CostEvidence{
		FixedAmount: new(big.Rat), FixedAsset: amount.Asset(), Cost: amount,
		Model: "complete_flow_cache", Available: false,
	}, fallback, costs, nil
}

func (r *Runner) remoteEvaluationCosts(
	directions []arbitrage.Direction,
	fixedEvidence CostEvidence,
	fixed arbitrage.CostSnapshot,
) (CostEvidence, arbitrage.CostSnapshot, map[arbitrage.Direction]arbitrage.CostSnapshot, bool) {
	if r.directionalCosts == nil {
		return fixedEvidence, fixed, nil, true
	}
	now := r.clock().UTC()
	costs := make(map[arbitrage.Direction]arbitrage.CostSnapshot, len(directions))
	var maximum arbitrage.CostSnapshot
	for _, direction := range directions {
		cost, ok := r.directionalCosts.Snapshot(direction, now)
		if !ok || cost.ID == "" || cost.Amount.Asset() == "" ||
			cost.CapturedAt.IsZero() {
			return CostEvidence{}, arbitrage.CostSnapshot{}, nil, false
		}
		if maximum.ID == "" || cost.Amount.Rat().Cmp(maximum.Amount.Rat()) > 0 {
			maximum = cost
		}
		costs[direction] = cost
	}
	if maximum.ID == "" {
		return CostEvidence{}, arbitrage.CostSnapshot{}, nil, false
	}
	return CostEvidence{
		FixedAmount: maximum.Amount.Rat(),
		FixedAsset:  maximum.Amount.Asset(),
		Cost:        maximum.Amount,
		Model:       "complete_flow_cache",
		Available:   true,
	}, maximum, costs, true
}

func drainLatestRemoteSignal(signals <-chan streamSignal, configured [2]configuration.ResolvedMarket, gate *runtimeresearch.TriggerGate) *streamSignal {
	for {
		select {
		case signal := <-signals:
			gate.OfferPending(scheduledRemoteTrigger(signal, configured))
		default:
			trigger, ok := gate.TakePending()
			if !ok {
				return nil
			}
			signal := streamSignal{
				market: trigger.Market, triggered: trigger.TriggeredAt,
				trigger: trigger.Metadata, hasTrigger: trigger.HasMetadata,
			}
			return &signal
		}
	}
}

func (r *Runner) sendOpeningAlert(alert notificationport.OpportunityOpening) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if err := r.openingAlerts.SendOpening(ctx, alert); err != nil {
		failures := r.alertFailures.Add(1)
		r.logger.Error("Telegram opening alert failed", "error", err, "failures", failures)
		return
	}
	r.logger.Info("Telegram opening alert delivered", "direction", alert.Direction)
}

func (r *Runner) dispatchQuoteWarnings(sources map[market.MarketID]quoteport.Source) {
	for _, source := range sources {
		warningSource, ok := source.(quoteport.WarningSource)
		if !ok {
			continue
		}
		for _, warning := range warningSource.TakeOperationalWarnings() {
			key := fmt.Sprintf("%s|%s|%s|%s|%s", warning.Code, warning.Provider, warning.Market, warning.Expected, warning.Observed)
			r.warningMu.Lock()
			_, delivered := r.deliveredWarnings[key]
			if !delivered {
				r.deliveredWarnings[key] = struct{}{}
			}
			r.warningMu.Unlock()
			if delivered {
				continue
			}
			r.logger.Warn(
				"quote provider configuration mismatch; quote accepted",
				"code", warning.Code, "provider", warning.Provider, "market", warning.Market,
				"expected", warning.Expected, "observed", warning.Observed,
			)
			if r.configurationAlerts == nil {
				continue
			}
			alert := notificationport.ConfigurationWarning{
				Code: warning.Code, Provider: readableProvider(warning.Provider), Market: readableMarket(warning.Market),
				Expected: warning.Expected, Observed: warning.Observed, Details: warning.Details,
				ObservedAt: warning.ObservedAt,
			}
			go r.sendConfigurationWarning(alert)
		}
	}
}

func (r *Runner) sendConfigurationWarning(alert notificationport.ConfigurationWarning) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if err := r.configurationAlerts.SendConfigurationWarning(ctx, alert); err != nil {
		failures := r.alertFailures.Add(1)
		r.logger.Error("Telegram configuration warning failed", "error", err, "failures", failures)
		return
	}
	r.logger.Info(
		"Telegram configuration warning delivered",
		"provider", alert.Provider, "market", alert.Market, "observed", alert.Observed,
	)
}

func buildOpeningAlert(opportunity arbitrage.Opportunity, timing strategy.EvaluationTiming, registry *market.Registry) (notificationport.OpportunityOpening, error) {
	if opportunity.SelectedIndex < 0 || opportunity.SelectedIndex >= len(opportunity.Candidates) {
		return notificationport.OpportunityOpening{}, fmt.Errorf("opened opportunity has no selected candidate")
	}
	candidate := opportunity.Candidates[opportunity.SelectedIndex]
	buyMarket, buyOK := registry.Market(opportunity.Direction.BuyMarket)
	sellMarket, sellOK := registry.Market(opportunity.Direction.SellMarket)
	if !buyOK || !sellOK {
		return notificationport.OpportunityOpening{}, fmt.Errorf("opened opportunity references unknown market")
	}
	buyBase, _ := registry.Token(buyMarket.BaseToken)
	buyQuote, _ := registry.Token(buyMarket.QuoteToken)
	sellQuote, _ := registry.Token(sellMarket.QuoteToken)
	baseBought, err := candidate.BuyQuote.AmountOut.ToAssetQuantity(buyBase)
	if err != nil {
		return notificationport.OpportunityOpening{}, err
	}
	var buyLatency, sellLatency time.Duration
	for _, direction := range timing.Directions {
		if direction.Direction != opportunity.Direction {
			continue
		}
		for _, quote := range direction.Quotes {
			switch quote.Leg {
			case "buy":
				buyLatency += quote.Duration
			case "sell":
				sellLatency += quote.Duration
			}
		}
	}
	trigger := "synthetic"
	if opportunity.HasTrigger {
		trigger = fmt.Sprintf("%s/%s:%s", opportunity.Trigger.Source, opportunity.Trigger.Reference.Kind, opportunity.Trigger.Reference.Value)
	}
	return notificationport.OpportunityOpening{
		Direction: fmt.Sprintf(
			"%s -> %s",
			readableMarket(opportunity.Direction.BuyMarket),
			readableMarket(opportunity.Direction.SellMarket),
		),
		BuyProvider:  readableProvider(candidate.BuyQuote.Source),
		SellProvider: readableProvider(candidate.SellQuote.Source),
		Input:        candidate.Input.Decimal(int(buyQuote.Decimals)) + " " + buyQuote.Symbol,
		BaseBought:   baseBought.Decimal(int(buyBase.Decimals)) + " " + buyBase.Symbol,
		SellOutput:   candidate.Output.Decimal(int(sellQuote.Decimals)) + " " + sellQuote.Symbol,
		GrossPnL:     candidate.GrossPnL.Decimal(int(sellQuote.Decimals)) + " " + sellQuote.Symbol,
		Cost:         candidate.Cost.Amount.Decimal(int(sellQuote.Decimals)) + " " + sellQuote.Symbol,
		NetPnL:       candidate.NetPnL.Decimal(int(sellQuote.Decimals)) + " " + sellQuote.Symbol,
		Threshold:    opportunity.Threshold.Decimal(int(sellQuote.Decimals)) + " " + sellQuote.Symbol,
		BuyLatency:   buyLatency, SellLatency: sellLatency, Trigger: trigger,
		TriggerURL: triggerExplorerURL(opportunity.Trigger), OpenedAt: opportunity.FinishedAt.UTC(),
	}, nil
}

func scheduledRemoteTrigger(signal streamSignal, configured [2]configuration.ResolvedMarket) runtimeresearch.ScheduledTrigger {
	chain := ""
	for _, candidate := range configured {
		if candidate.ID == signal.market {
			chain = candidate.Chain
			break
		}
	}
	return runtimeresearch.ScheduledTrigger{
		Chain: chain, Market: signal.market, TriggeredAt: signal.triggered,
		Metadata: signal.trigger, HasMetadata: signal.hasTrigger,
	}
}

func syntheticIdleSignal(marketID market.MarketID, at time.Time, sequence uint64) streamSignal {
	trigger := arbitrage.TriggerMetadata{
		Market: marketID, Source: "research/idle",
		Position:  market.SourcePosition{Kind: "idle_sequence", Value: sequence},
		Reference: market.SourceReference{Kind: "idle_timer", Value: fmt.Sprintf("%d", sequence)},
		At:        at,
	}
	return streamSignal{market: marketID, triggered: at, trigger: trigger, hasTrigger: true}
}

func syntheticRetrySignal(
	marketID market.MarketID,
	at time.Time,
	sequence uint64,
) streamSignal {
	trigger := arbitrage.TriggerMetadata{
		Market: marketID, Source: "live/retry",
		Position: market.SourcePosition{
			Kind: "retry_sequence", Value: sequence,
		},
		Reference: market.SourceReference{
			Kind: "execution_retry", Value: fmt.Sprintf("%d", sequence),
		},
		At: at,
	}
	return streamSignal{
		market: marketID, triggered: at,
		trigger: trigger, hasTrigger: true,
	}
}

func refreshRemoteSnapshots(ctx context.Context, signal streamSignal, remote map[market.MarketID]eventRefreshedRuntime) error {
	event := market.MarketEvent{
		Market: signal.market, Source: signal.trigger.Source, Position: signal.trigger.Position,
		Reference: signal.trigger.Reference, Finality: market.FinalityPreconfirmed, ReceivedAt: signal.triggered,
	}
	for id, candidate := range remote {
		if id == signal.market && signal.trigger.Source != "research/idle" {
			continue
		}
		if _, err := candidate.store.Refresh(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func remoteSnapshots(order [2]configuration.ResolvedMarket, remote map[market.MarketID]eventRefreshedRuntime) ([]market.MarketSnapshot, bool, bool) {
	result := make([]market.MarketSnapshot, 0, 2)
	healthy := true
	for _, configured := range order {
		snapshot, ok := remote[configured.ID].store.Current()
		if !ok {
			return nil, false, false
		}
		if snapshot.Metadata().Health != market.HealthHealthy {
			healthy = false
		}
		result = append(result, snapshot)
	}
	return result, true, healthy
}
