package livecompare

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
	notificationport "github.com/VarozXYZ/vernier/ports/notification"
	persistenceport "github.com/VarozXYZ/vernier/ports/persistence"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
	"github.com/VarozXYZ/vernier/runtime/crosschain"
	runtimeresearch "github.com/VarozXYZ/vernier/runtime/research"
)

type pinnedStrategy interface {
	ID() arbitrage.StrategyID
	EvaluateWithTiming(context.Context, arbitrage.Evaluation) ([]arbitrage.Opportunity, strategy.EvaluationTiming, error)
	EvaluatePinnedWithTiming(context.Context, arbitrage.Evaluation, arbitrage.Direction, market.AssetQuantity) (arbitrage.Opportunity, strategy.PinnedEvaluationTiming, error)
}

type fixedCandidateSignal struct {
	trigger            arbitrage.TriggerMetadata
	snapshots          []market.MarketSnapshot
	triggerReceivedAt  time.Time
	snapshotCapturedAt time.Time
	queuedAt           time.Time
	degraded           bool
}

type fixedWindowState struct {
	window           arbitrage.TrackingWindow
	previous         arbitrage.Candidate
	current          arbitrage.Candidate
	currentBuyOutput market.AssetQuantity
	initialPnL       market.AssetQuantity
	bestPnL          market.AssetQuantity
	worstPnL         market.AssetQuantity
	sequence         uint64
	changes          uint64
	cumulativeCalc   time.Duration
	cumulativeQueue  time.Duration
	maximumQueue     time.Duration
	history          []notificationport.TrackingHistoryPoint
	latencies        []time.Duration
	previousTrigger  time.Time
}

// These are presentation/statistics buffers only. Every tracking point is
// persisted independently in SQLite, so keeping them bounded cannot lose the
// durable history while preventing an active window from growing without end.
const (
	maxInMemoryTrackingHistory = 256
	maxInMemoryTrackingLatency = 2048
	maxTrackingSignalQueue     = 512
)

func appendBoundedHistory(values []notificationport.TrackingHistoryPoint, value notificationport.TrackingHistoryPoint) []notificationport.TrackingHistoryPoint {
	values = append(values, value)
	if len(values) > maxInMemoryTrackingHistory {
		values = values[len(values)-maxInMemoryTrackingHistory:]
	}
	return values
}

func appendBoundedLatency(values []time.Duration, value time.Duration) []time.Duration {
	values = append(values, value)
	if len(values) > maxInMemoryTrackingLatency {
		values = values[len(values)-maxInMemoryTrackingLatency:]
	}
	return values
}

func (r *Runner) runFixedCandidateRouteStream(ctx context.Context, options StreamOptions) error {
	store, ok := options.OpportunityStore.(persistenceport.TrackingStore)
	if !ok || store == nil {
		return fmt.Errorf("fixed-candidate tracking requires a tracking-capable opportunity store")
	}
	if options.Updates < 0 {
		return fmt.Errorf("stream updates cannot be negative")
	}
	if options.OnReport == nil {
		options.OnReport = func(Report) error { return nil }
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
	routes := make(map[market.MarketID]routeRuntime, len(r.config.Markets))
	sources := make(map[market.MarketID]quoteport.Source, len(r.config.Markets))
	now := r.clock().UTC()
	for _, configured := range r.config.Markets {
		route, buildErr := r.buildRoute(ctx, configured, registry, maximum, blocks, map[string]uint64{}, now, false)
		if buildErr != nil {
			return buildErr
		}
		routes[configured.ID] = route
		sources[configured.ID] = route.route.Source
	}
	costEvidence, cost, err := r.cost(ctx, blocks, now)
	if err != nil {
		return err
	}
	candidate, err := r.newStrategy(registry, setup, sources)
	if err != nil {
		return err
	}
	pinned, ok := candidate.(pinnedStrategy)
	if !ok {
		return fmt.Errorf("configured strategy does not support fixed-candidate tracking")
	}

	runCtx, cancel := context.WithCancel(ctx)
	capacity := r.config.TrackingQueueCapacity
	if capacity > maxTrackingSignalQueue {
		capacity = maxTrackingSignalQueue
		r.logger.Warn("tracking queue capacity capped to protect memory", "configured", r.config.TrackingQueueCapacity, "effective", capacity)
	}
	signals := make(chan fixedCandidateSignal, capacity)
	overflow := make(chan fixedCandidateSignal, 1)
	discoveryWake := make(chan struct{}, 1)
	var trackingActive atomic.Bool
	var discoveryMu sync.Mutex
	var discoveryLatest *fixedCandidateSignal
	type failure struct {
		market market.MarketID
		err    error
	}
	failures := make(chan failure, len(routes)*2)
	capture := func(trigger arbitrage.TriggerMetadata, degraded bool) {
		captured := r.clock().UTC()
		snapshots := make([]market.MarketSnapshot, 0, len(r.config.Markets))
		for _, configured := range r.config.Markets {
			snapshot, ready := routes[configured.ID].route.Snapshot()
			if !ready {
				return
			}
			snapshots = append(snapshots, snapshot)
			if snapshot.Metadata().Health != market.HealthHealthy {
				degraded = true
			}
		}
		signal := fixedCandidateSignal{
			trigger: trigger, snapshots: snapshots, degraded: degraded,
			triggerReceivedAt: trigger.At.UTC(), snapshotCapturedAt: captured, queuedAt: r.clock().UTC(),
		}
		r.rememberSimulationSnapshots(snapshots)
		if !trackingActive.Load() {
			discoveryMu.Lock()
			copyOf := signal
			discoveryLatest = &copyOf
			discoveryMu.Unlock()
			select {
			case discoveryWake <- struct{}{}:
			default:
			}
			return
		}
		select {
		case signals <- signal:
		default:
			select {
			case overflow <- signal:
			default:
			}
		}
	}
	notifier := newTrackingNotifier(r.openingAlerts, store, r.logger)
	notifier.start(runCtx)
	var simulations *simulationCoordinator
	if r.config.SimulationEnabled {
		if simulationStore, ok := any(store).(persistenceport.SimulationStore); ok {
			simulations = newSimulationCoordinator(runCtx, newPairSimulator(r), simulationStore, r.config.SimulationInterval, r.logger, func(update notificationport.TrackingWindowUpdate) {
				if err := notifier.enqueue(runCtx, trackingNotification{window: arbitrage.WindowID(update.WindowID), update: update}); err != nil {
					r.logger.Warn("simulation Telegram update dropped", "window", update.WindowID, "error", err)
				}
			})
		} else {
			r.logger.Warn("simulation disabled for stream: opportunity store has no simulation journal")
		}
	}
	dangling, err := store.FinalizeDanglingTracking(runCtx, r.clock().UTC())
	if err != nil {
		cancel()
		notifier.stop()
		return err
	}
	for _, interrupted := range dangling {
		update := notificationport.TrackingWindowUpdate{
			WindowID: string(interrupted.WindowID), State: "uncertain",
			Direction: fmt.Sprintf("%s -> %s", interrupted.Direction.BuyMarket, interrupted.Direction.SellMarket),
			Input:     formatQuantity(interrupted.Input), BuyOutput: formatQuantity(interrupted.BuyOutput),
			SellOutput: formatQuantity(interrupted.SellOutput), NetPnL: formatQuantity(interrupted.NetPnL),
			Threshold: formatQuantity(interrupted.Threshold), BestPnL: formatQuantity(interrupted.BestPnL),
			WorstPnL: formatQuantity(interrupted.WorstPnL), Reason: "process_interrupted",
			OpenedAt: interrupted.OpenedAt, Points: interrupted.Points, Changes: interrupted.EconomicChanges,
			EconomicDuration:      nonNegativeDuration(interrupted.LastTriggerAt.Sub(interrupted.OpenedAt)),
			ObservedDuration:      nonNegativeDuration(interrupted.ClosedAt.Sub(interrupted.OpenedAt)),
			CumulativeCalculation: interrupted.CumulativeCalculation,
			CumulativeQueue:       interrupted.CumulativeQueue,
		}
		if err := notifier.enqueue(runCtx, trackingNotification{window: interrupted.WindowID, update: update}); err != nil {
			cancel()
			notifier.stop()
			return err
		}
	}

	var feeds sync.WaitGroup
	for routeID, route := range routes {
		for _, child := range route.children {
			child, routeID, route := child, routeID, route
			feeds.Add(1)
			go func() {
				defer feeds.Done()
				sink := &fixedRouteSink{route: route.route, child: child.market.ID, capture: capture}
				if runErr := child.feed.Run(runCtx, sink); runErr != nil && !errors.Is(runErr, context.Canceled) {
					failures <- failure{market: routeID, err: runErr}
				}
			}()
		}
	}
	defer feeds.Wait()
	defer notifier.stop()
	defer cancel()
	r.logger.Info("fixed-candidate route stream started", "run", r.config.RunID, "markets", len(routes), "queue_capacity", capacity)

	var active *fixedWindowState
	evaluations := 0
streamLoop:
	for {
		var signal fixedCandidateSignal
		select {
		case <-ctx.Done():
			return nil
		case failed := <-failures:
			if active != nil {
				_ = r.closeFixedWindow(runCtx, store, notifier, active, fixedCandidateSignal{trigger: syntheticTrackingTrigger(failed.market, "feed_failed", r.clock().UTC()), triggerReceivedAt: r.clock().UTC(), snapshotCapturedAt: r.clock().UTC(), queuedAt: r.clock().UTC()}, "feed_failed", true)
			}
			return failed.err
		case signal = <-overflow:
			if active != nil {
				if closeErr := r.closeFixedWindow(runCtx, store, notifier, active, signal, "observation_gap", true); closeErr != nil {
					return closeErr
				}
				active = nil
				trackingActive.Store(false)
			}
			// Clear the saturated queue: all omitted observations are represented
			// by the durable gap and discovery resumes from the latest snapshot.
			for {
				select {
				case newer := <-signals:
					signal = newer
				default:
					goto process
				}
			}
		case signal = <-signals:
		case <-discoveryWake:
			discoveryMu.Lock()
			if discoveryLatest != nil {
				signal = *discoveryLatest
				discoveryLatest = nil
			}
			discoveryMu.Unlock()
		}
	process:
		if signal.degraded {
			if active != nil {
				if closeErr := r.closeFixedWindow(runCtx, store, notifier, active, signal, "market_degraded", true); closeErr != nil {
					return closeErr
				}
				active = nil
				trackingActive.Store(false)
			}
			continue
		}

		if active == nil {
			opened, report, openErr := r.discoverFixedWindow(runCtx, pinned, registry, store, notifier, simulations, cost, costEvidence, signal, evaluations+1)
			if openErr != nil {
				return openErr
			}
			evaluations++
			if err := options.OnReport(report); err != nil {
				return err
			}
			active = opened
			trackingActive.Store(opened != nil)
			// Discovery deliberately coalesces all events that arrived while the
			// 25-size grid was running. The newest captured snapshot is processed
			// next, either as tracking or as another discovery.
			var latest *fixedCandidateSignal
			discoveryMu.Lock()
			if discoveryLatest != nil {
				copyOf := *discoveryLatest
				latest = &copyOf
				discoveryLatest = nil
			}
			discoveryMu.Unlock()
			select {
			case <-discoveryWake:
			default:
			}
			for {
				select {
				case queued := <-signals:
					copyOf := queued
					latest = &copyOf
				default:
					if latest != nil {
						signal = *latest
						goto process
					}
					if options.Updates > 0 && evaluations >= options.Updates {
						return nil
					}
					continue streamLoop
				}
			}
		}

		closed, pointReport, trackErr := r.trackFixedPoint(runCtx, pinned, registry, store, notifier, simulations, cost, signal, active, evaluations+1)
		if trackErr != nil {
			return trackErr
		}
		evaluations++
		if err := options.OnReport(pointReport); err != nil {
			return err
		}
		if closed {
			active = nil
			trackingActive.Store(false)
			// The exact closing snapshot immediately becomes discovery input.
			goto process
		}
		if options.Updates > 0 && evaluations >= options.Updates {
			return nil
		}
	}
}

func (r *Runner) discoverFixedWindow(
	ctx context.Context,
	pinned pinnedStrategy,
	registry *market.Registry,
	store persistenceport.TrackingStore,
	notifier *trackingNotifier,
	simulations *simulationCoordinator,
	cost arbitrage.CostSnapshot,
	costEvidence CostEvidence,
	signal fixedCandidateSignal,
	evaluationNumber int,
) (*fixedWindowState, Report, error) {
	started := r.clock().UTC()
	research, err := r.evaluate(ctx, pinned, signal.snapshots, cost, fmt.Sprintf("route-tracking/%s/discovery/%d", r.config.ResearchID, evaluationNumber), signal.triggerReceivedAt, &signal.trigger)
	if err != nil {
		return nil, Report{}, err
	}
	research.Evaluations = evaluationNumber
	report := Report{Research: research, Cost: costEvidence}
	var selected *arbitrage.Opportunity
	for index := range research.Opportunities {
		opportunity := &research.Opportunities[index]
		if opportunity.Classification != arbitrage.ClassificationPolicyQualified || opportunity.SelectedIndex < 0 {
			continue
		}
		if selected == nil || quantityGreater(opportunity.Candidates[opportunity.SelectedIndex].NetPnL, selected.Candidates[selected.SelectedIndex].NetPnL) {
			selected = opportunity
		}
	}
	if selected == nil {
		return nil, report, nil
	}
	selectedCandidate := selected.Candidates[selected.SelectedIndex]
	finished := r.clock().UTC()
	windowID := trackingWindowID(r.config.RunID, selected.Direction, signal.trigger, finished)
	baseOutput, err := candidateBuyOutput(registry, selected.Direction, selectedCandidate)
	if err != nil {
		return nil, Report{}, err
	}
	window := arbitrage.TrackingWindow{
		ID: windowID, Run: selected.Run, Strategy: selected.Strategy, ConfigHash: selected.ConfigHash,
		Direction: selected.Direction, Input: selectedCandidate.Input,
		FixedThreshold: selectedCandidate.FixedThreshold, PercentageThreshold: selectedCandidate.PercentageThreshold,
		EffectiveThreshold: selectedCandidate.EffectiveThreshold, Cost: selectedCandidate.Cost.Amount,
		OpeningTrigger: signal.trigger, OpenedAt: signal.triggerReceivedAt, DiscoveryStartedAt: started,
		DiscoveryFinishedAt: finished, Opening: selectedCandidate,
		OpeningBuyOutput: baseOutput, OpeningSnapshots: trackingSnapshots(signal.snapshots), DiscoveryTrace: trackingDiscoveryTrace(research.LocalTiming),
	}
	if err := store.OpenTrackingWindow(ctx, &window); err != nil {
		return nil, Report{}, err
	}
	state := &fixedWindowState{
		window: window, previous: selectedCandidate, current: selectedCandidate,
		currentBuyOutput: baseOutput,
		initialPnL:       selectedCandidate.NetPnL, bestPnL: selectedCandidate.NetPnL, worstPnL: selectedCandidate.NetPnL,
		sequence: 1, changes: 0, cumulativeCalc: research.LocalTiming.Duration,
		previousTrigger: signal.triggerReceivedAt,
		history:         []notificationport.TrackingHistoryPoint{{SinceOpening: 0, SellOutput: formatQuantity(selectedCandidate.Output), NetPnL: formatQuantity(selectedCandidate.NetPnL), Delta: formatSignedZero(selectedCandidate.NetPnL.Asset()), Calculation: research.LocalTiming.Duration, Total: nonNegativeDuration(finished.Sub(signal.triggerReceivedAt))}},
	}
	update := r.fixedWindowUpdate(state, baseOutput, selectedCandidate, signal, "open", "", strategy.PinnedEvaluationTiming{}, arbitrage.TrackingDurations{}, finished)
	update.DiscoveryDuration = research.LocalTiming.Duration
	update.TriggerToOpen = nonNegativeDuration(window.OpeningPersistedAt.Sub(signal.triggerReceivedAt))
	if err := notifier.enqueue(ctx, trackingNotification{window: windowID, update: update}); err != nil {
		return nil, Report{}, err
	}
	if simulations != nil {
		if request, ok := r.simulationRequestForCandidate(windowID, 1, selectedCandidate, signal.snapshots, true); ok {
			simulations.submit(request, update)
		}
	}
	r.logger.Info("fixed opportunity opened", "window", windowID, "direction", selected.Direction, "input", selectedCandidate.Input, "net_pnl", selectedCandidate.NetPnL, "threshold", selectedCandidate.EffectiveThreshold, "discovery_duration", research.LocalTiming.Duration)
	return state, report, nil
}

func (r *Runner) trackFixedPoint(
	ctx context.Context,
	pinned pinnedStrategy,
	registry *market.Registry,
	store persistenceport.TrackingStore,
	notifier *trackingNotifier,
	simulations *simulationCoordinator,
	cost arbitrage.CostSnapshot,
	signal fixedCandidateSignal,
	state *fixedWindowState,
	evaluationNumber int,
) (bool, Report, error) {
	evaluationStarted := r.clock().UTC()
	evaluationID := arbitrage.EvaluationID(fmt.Sprintf("route-tracking/%s/window/%s/%d", r.config.ResearchID, state.window.ID, state.sequence+1))
	evaluation, err := arbitrage.NewEvaluation(evaluationID, arbitrage.ResearchRunID(r.config.RunID), pinned.ID(), r.config.Hash, signal.snapshots, cost, signal.triggerReceivedAt, evaluationStarted)
	if err != nil {
		return false, Report{}, err
	}
	evaluation = evaluation.WithTrigger(signal.trigger)
	opportunity, trace, err := pinned.EvaluatePinnedWithTiming(ctx, evaluation, state.window.Direction, state.window.Input)
	if err != nil {
		return false, Report{}, err
	}
	trace.EvaluationFinishedAt = r.clock().UTC()
	research := researchReportForPinned(r, opportunity, trace, evaluationNumber, signal.snapshots)
	report := Report{Research: research}

	state.sequence++
	point := arbitrage.TrackingPoint{
		ID: fmt.Sprintf("%s/%d", state.window.ID, state.sequence), WindowID: state.window.ID,
		Sequence: state.sequence, Evaluation: evaluationID, Trigger: signal.trigger,
		Timestamps: arbitrage.TrackingTimestamps{
			TriggerReceivedAt: signal.triggerReceivedAt, SnapshotCapturedAt: signal.snapshotCapturedAt,
			QueuedAt: signal.queuedAt, EvaluationStartedAt: evaluationStarted,
			BuyStartedAt: trace.BuyStartedAt, BuyFinishedAt: trace.BuyFinishedAt,
			ConversionStartedAt: trace.ConversionStartedAt, ConversionFinishedAt: trace.ConversionFinishedAt,
			SellStartedAt: trace.SellStartedAt, SellFinishedAt: trace.SellFinishedAt,
			PnLStartedAt: trace.PnLStartedAt, PnLFinishedAt: trace.PnLFinishedAt,
			EvaluationFinishedAt: trace.EvaluationFinishedAt,
		},
		Snapshots: trackingSnapshots(signal.snapshots), Input: state.window.Input,
		FixedThreshold: state.window.FixedThreshold, PercentageThreshold: state.window.PercentageThreshold,
		EffectiveThreshold: state.window.EffectiveThreshold, Classification: opportunity.Classification,
		Reason:               strings.Join(opportunity.Reasons, ","),
		IntervalFromPrevious: nonNegativeDuration(signal.triggerReceivedAt.Sub(state.previousTrigger)),
		SinceOpening:         nonNegativeDuration(signal.triggerReceivedAt.Sub(state.window.OpenedAt)),
	}
	zeroQuote, _ := market.NewAssetQuantity(state.window.Input.Asset(), new(big.Rat))
	var candidate arbitrage.Candidate
	hasCandidate := opportunity.SelectedIndex >= 0 && opportunity.SelectedIndex < len(opportunity.Candidates)
	if hasCandidate {
		candidate = opportunity.Candidates[opportunity.SelectedIndex]
		point.SellOutput, point.GrossPnL, point.NetPnL = candidate.Output, candidate.GrossPnL, candidate.NetPnL
		point.FixedThreshold, point.PercentageThreshold, point.EffectiveThreshold = candidate.FixedThreshold, candidate.PercentageThreshold, candidate.EffectiveThreshold
		point.BuyOutput, err = candidateBuyOutput(registry, state.window.Direction, candidate)
		if err != nil {
			return false, Report{}, err
		}
		point.DeltaFromOpening, _ = candidate.NetPnL.Sub(state.initialPnL)
		point.DeltaFromPrevious, _ = candidate.NetPnL.Sub(state.previous.NetPnL)
		point.EconomicChange = candidate.Output.String() != state.previous.Output.String() || candidate.NetPnL.String() != state.previous.NetPnL.String() || candidate.BuyQuote.AmountOut.Units().Cmp(state.previous.BuyQuote.AmountOut.Units()) != 0
		state.current = candidate
		state.currentBuyOutput = point.BuyOutput
		if point.EconomicChange {
			state.changes++
		}
		if quantityGreater(candidate.NetPnL, state.bestPnL) {
			state.bestPnL = candidate.NetPnL
		}
		if quantityGreater(state.worstPnL, candidate.NetPnL) {
			state.worstPnL = candidate.NetPnL
		}
	} else {
		point.SellOutput, point.GrossPnL, point.NetPnL = zeroQuote, zeroQuote, zeroQuote
		point.BuyOutput = state.currentBuyOutput
		point.DeltaFromOpening, _ = zeroQuote.Sub(state.initialPnL)
		point.DeltaFromPrevious, _ = zeroQuote.Sub(state.previous.NetPnL)
	}
	point.Timestamps.PersistenceStartedAt = r.clock().UTC()
	if err := store.RecordTrackingPoint(ctx, &point); err != nil {
		return false, Report{}, err
	}
	durations := point.Timestamps.Durations()
	state.cumulativeCalc += durations.LocalCalculation
	state.cumulativeQueue += durations.Queue
	if durations.Queue > state.maximumQueue {
		state.maximumQueue = durations.Queue
	}
	state.history = appendBoundedHistory(state.history, notificationport.TrackingHistoryPoint{
		SinceOpening: nonNegativeDuration(signal.triggerReceivedAt.Sub(state.window.OpenedAt)), SellOutput: formatQuantity(point.SellOutput),
		NetPnL: formatQuantity(point.NetPnL), Delta: formatQuantity(point.DeltaFromPrevious), Calculation: durations.LocalCalculation,
		Total: durations.EventToEvaluation,
	})
	state.latencies = appendBoundedLatency(state.latencies, durations.EventToEvaluation)
	closed := !hasCandidate || opportunity.Classification != arbitrage.ClassificationPolicyQualified
	status, reason := "open", ""
	if closed {
		status = "closed"
		reason = point.Reason
		if !hasCandidate || opportunity.Classification == arbitrage.ClassificationUnclassifiable {
			status = "uncertain"
			if reason == "" {
				reason = "unclassifiable"
			}
		}
		closing := r.fixedClosing(state, point.NetPnL, signal.triggerReceivedAt, r.clock().UTC(), reason, status == "uncertain")
		if err := store.CloseTrackingWindow(ctx, closing); err != nil {
			return false, Report{}, err
		}
	}
	finished := r.clock().UTC()
	displayCandidate := state.current
	if !hasCandidate {
		displayCandidate.Output = point.SellOutput
		displayCandidate.NetPnL = point.NetPnL
	}
	update := r.fixedWindowUpdate(state, point.BuyOutput, displayCandidate, signal, status, reason, trace, durations, finished)
	if !hasCandidate {
		update.BuyOutput, update.SellOutput, update.NetPnL = formatQuantity(point.BuyOutput), formatQuantity(point.SellOutput), formatQuantity(point.NetPnL)
	}
	if err := notifier.enqueue(ctx, trackingNotification{window: state.window.ID, update: update}); err != nil {
		return false, Report{}, err
	}
	if simulations != nil && hasCandidate {
		if request, ok := r.simulationRequestForCandidate(state.window.ID, point.Sequence, candidate, signal.snapshots, opportunity.Classification == arbitrage.ClassificationPolicyQualified); ok {
			simulations.submit(request, update)
		}
	}
	point.Timestamps.NotificationEnqueuedAt = r.clock().UTC()
	point.Timestamps.EvaluationFinishedAt = point.Timestamps.NotificationEnqueuedAt
	if err := store.MarkTrackingNotificationEnqueued(ctx, state.window.ID, point.Sequence, point.Timestamps.NotificationEnqueuedAt); err != nil {
		return false, Report{}, err
	}
	if hasCandidate {
		state.previous = candidate
	}
	state.previousTrigger = signal.triggerReceivedAt
	return closed, report, nil
}

func (r *Runner) closeFixedWindow(ctx context.Context, store persistenceport.TrackingStore, notifier *trackingNotifier, state *fixedWindowState, signal fixedCandidateSignal, reason string, uncertain bool) error {
	var gapPoint *arbitrage.TrackingPoint
	if len(signal.snapshots) == 2 {
		zero, _ := market.NewAssetQuantity(state.window.Input.Asset(), new(big.Rat))
		state.sequence++
		now := r.clock().UTC()
		point := &arbitrage.TrackingPoint{
			ID: fmt.Sprintf("%s/%d", state.window.ID, state.sequence), WindowID: state.window.ID,
			Sequence: state.sequence, Evaluation: arbitrage.EvaluationID(fmt.Sprintf("route-tracking/%s/gap/%d", state.window.ID, state.sequence)),
			Trigger: signal.trigger, Snapshots: trackingSnapshots(signal.snapshots), Input: state.window.Input,
			BuyOutput: state.currentBuyOutput, SellOutput: state.current.Output, GrossPnL: state.current.GrossPnL,
			NetPnL: state.current.NetPnL, FixedThreshold: state.window.FixedThreshold,
			PercentageThreshold: state.window.PercentageThreshold, EffectiveThreshold: state.window.EffectiveThreshold,
			DeltaFromOpening: zero, DeltaFromPrevious: zero, Classification: arbitrage.ClassificationUnclassifiable,
			Reason:               reason,
			IntervalFromPrevious: nonNegativeDuration(signal.triggerReceivedAt.Sub(state.previousTrigger)),
			SinceOpening:         nonNegativeDuration(signal.triggerReceivedAt.Sub(state.window.OpenedAt)),
			Timestamps: arbitrage.TrackingTimestamps{
				TriggerReceivedAt: signal.triggerReceivedAt, SnapshotCapturedAt: signal.snapshotCapturedAt,
				QueuedAt: signal.queuedAt, EvaluationStartedAt: now, PnLStartedAt: now, PnLFinishedAt: now,
				PersistenceStartedAt: now, EvaluationFinishedAt: now,
			},
		}
		if err := store.RecordTrackingPoint(ctx, point); err != nil {
			return err
		}
		durations := point.Timestamps.Durations()
		state.cumulativeQueue += durations.Queue
		if durations.Queue > state.maximumQueue {
			state.maximumQueue = durations.Queue
		}
		state.history = appendBoundedHistory(state.history, notificationport.TrackingHistoryPoint{
			SinceOpening: nonNegativeDuration(signal.triggerReceivedAt.Sub(state.window.OpenedAt)),
			SellOutput:   formatQuantity(state.current.Output), NetPnL: formatQuantity(state.current.NetPnL),
			Delta: formatSignedZero(state.current.NetPnL.Asset()), Total: durations.EventToEvaluation,
		})
		state.latencies = appendBoundedLatency(state.latencies, durations.EventToEvaluation)
		gapPoint = point
	}
	closing := r.fixedClosing(state, state.current.NetPnL, signal.triggerReceivedAt, r.clock().UTC(), reason, uncertain)
	if err := store.CloseTrackingWindow(ctx, closing); err != nil {
		return err
	}
	status := "closed"
	if uncertain {
		status = "uncertain"
	}
	update := r.fixedWindowUpdate(state, state.currentBuyOutput, state.current, signal, status, reason, strategy.PinnedEvaluationTiming{}, arbitrage.TrackingDurations{}, closing.ClosedAt)
	if err := notifier.enqueue(ctx, trackingNotification{window: state.window.ID, update: update}); err != nil {
		return err
	}
	if gapPoint != nil {
		enqueued := r.clock().UTC()
		return store.MarkTrackingNotificationEnqueued(ctx, state.window.ID, gapPoint.Sequence, enqueued)
	}
	return nil
}

func (r *Runner) fixedClosing(state *fixedWindowState, final market.AssetQuantity, closingTrigger, closedAt time.Time, reason string, uncertain bool) arbitrage.TrackingWindowClosing {
	status := arbitrage.WindowStatusClosed
	if uncertain {
		status = arbitrage.WindowStatusFailed
	}
	minimum, mean, maximum := trackingLatencyBounds(state.latencies)
	return arbitrage.TrackingWindowClosing{
		WindowID: state.window.ID, Status: status, Reason: reason, ClosedAt: closedAt,
		ClosingTriggerAt: closingTrigger, EconomicDuration: nonNegativeDuration(closingTrigger.Sub(state.window.OpenedAt)),
		ObservedDuration:      nonNegativeDuration(closedAt.Sub(state.window.OpenedAt)),
		CumulativeCalculation: state.cumulativeCalc, CumulativeQueue: state.cumulativeQueue,
		MaximumQueue: state.maximumQueue, Events: state.sequence, EconomicChanges: state.changes,
		InitialPnL: state.initialPnL, FinalPnL: final, BestPnL: state.bestPnL, WorstPnL: state.worstPnL,
		LatencyMinimum: minimum, LatencyMean: mean, LatencyP50: percentileDuration(state.latencies, 50),
		LatencyP95: percentileDuration(state.latencies, 95), LatencyP99: percentileDuration(state.latencies, 99), LatencyMaximum: maximum,
	}
}

func trackingLatencyBounds(values []time.Duration) (time.Duration, time.Duration, time.Duration) {
	if len(values) == 0 {
		return 0, 0, 0
	}
	minimum, maximum := values[0], values[0]
	var total time.Duration
	for _, value := range values {
		if value < minimum {
			minimum = value
		}
		if value > maximum {
			maximum = value
		}
		total += value
	}
	return minimum, total / time.Duration(len(values)), maximum
}

func percentileDuration(values []time.Duration, percentile int) time.Duration {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	index := (percentile*len(ordered) + 99) / 100
	if index < 1 {
		index = 1
	}
	return ordered[index-1]
}

func (r *Runner) fixedWindowUpdate(state *fixedWindowState, buyOutput market.AssetQuantity, candidate arbitrage.Candidate, signal fixedCandidateSignal, status, reason string, trace strategy.PinnedEvaluationTiming, durations arbitrage.TrackingDurations, finished time.Time) notificationport.TrackingWindowUpdate {
	triggerText, triggerURL := fixedTriggerDisplay(signal.trigger)
	deltaOpening, _ := candidate.NetPnL.Sub(state.initialPnL)
	deltaPrevious, _ := candidate.NetPnL.Sub(state.previous.NetPnL)
	return notificationport.TrackingWindowUpdate{
		WindowID: string(state.window.ID), State: status, Direction: fmt.Sprintf("%s -> %s", state.window.Direction.BuyMarket, state.window.Direction.SellMarket),
		Input: formatQuantity(state.window.Input), BuyOutput: formatQuantity(buyOutput), SellOutput: formatQuantity(candidate.Output),
		NetPnL: formatQuantity(candidate.NetPnL), DeltaOpening: formatQuantity(deltaOpening), DeltaPrevious: formatQuantity(deltaPrevious),
		Threshold: formatQuantity(state.window.EffectiveThreshold), BestPnL: formatQuantity(state.bestPnL), WorstPnL: formatQuantity(state.worstPnL),
		Reason: reason, Trigger: triggerText, TriggerURL: triggerURL, OpenedAt: state.window.OpenedAt,
		Points: state.sequence, Changes: state.changes, SinceOpening: nonNegativeDuration(signal.triggerReceivedAt.Sub(state.window.OpenedAt)),
		EconomicDuration: nonNegativeDuration(signal.triggerReceivedAt.Sub(state.window.OpenedAt)), ObservedDuration: nonNegativeDuration(finished.Sub(state.window.OpenedAt)),
		QueueDuration: durations.Queue, BuyDuration: durations.BuyQuote, ConversionDuration: durations.DecimalConversion,
		SellDuration: durations.SellQuote, PnLDuration: durations.PnLCalculation, PersistenceDuration: durations.Persistence,
		CalculationDuration: durations.LocalCalculation, TriggerToResult: durations.EventToEvaluation,
		CumulativeCalculation: state.cumulativeCalc, CumulativeQueue: state.cumulativeQueue,
		History: append([]notificationport.TrackingHistoryPoint(nil), state.history...),
	}
}

type fixedRouteSink struct {
	route   *crosschain.Route
	child   market.MarketID
	capture func(arbitrage.TriggerMetadata, bool)
}

func (s *fixedRouteSink) Publish(ctx context.Context, event market.MarketEvent) error {
	result, err := s.route.Apply(ctx, event)
	if err != nil || result.Disposition == feedport.ApplyDispositionIgnoredStale {
		return err
	}
	s.capture(triggerFromEvent(event), false)
	return nil
}

func (s *fixedRouteSink) Reset(ctx context.Context, event market.MarketEvent) error {
	result, err := s.route.Reset(ctx, event)
	if err != nil || result.Disposition == feedport.ApplyDispositionIgnoredStale {
		return err
	}
	s.capture(triggerFromEvent(event), false)
	return nil
}

func (s *fixedRouteSink) SetHealth(ctx context.Context, update feedport.HealthUpdate) error {
	if err := s.route.SetChildHealth(ctx, s.child, update); err != nil {
		return err
	}
	if update.Health == market.HealthDegraded {
		s.capture(syntheticTrackingTrigger(s.child, "market_degraded", update.ObservedAt), true)
	}
	return nil
}

func (s *fixedRouteSink) ApplyState(ctx context.Context, event market.MarketEvent) error {
	result, err := s.route.Apply(ctx, event)
	if err != nil || result.Disposition == feedport.ApplyDispositionIgnoredStale {
		return err
	}
	return nil
}

func (s *fixedRouteSink) PublishTrigger(_ context.Context, event market.MarketEvent) error {
	s.capture(triggerFromEvent(event), false)
	return nil
}

func triggerFromEvent(event market.MarketEvent) arbitrage.TriggerMetadata {
	return arbitrage.TriggerMetadata{Market: event.Market, Source: event.Source, Position: event.Position, Reference: event.Reference, At: event.ReceivedAt.UTC()}
}

func syntheticTrackingTrigger(candidate market.MarketID, reason string, at time.Time) arbitrage.TriggerMetadata {
	return arbitrage.TriggerMetadata{Market: candidate, Source: market.SourceID("research/tracking"), Position: market.SourcePosition{Kind: "sequence", Value: uint64(at.UnixNano())}, Reference: market.SourceReference{Kind: "synthetic", Value: reason}, At: at.UTC()}
}

func trackingWindowID(run string, direction arbitrage.Direction, trigger arbitrage.TriggerMetadata, at time.Time) arbitrage.WindowID {
	value := fmt.Sprintf("%s|%s|%s|%s|%d|%s|%s", run, direction.BuyMarket, direction.SellMarket, trigger.Market, trigger.Position.Value, trigger.Reference.Value, at.UTC().Format(time.RFC3339Nano))
	hash := sha256.Sum256([]byte(value))
	return arbitrage.WindowID("tracking-" + hex.EncodeToString(hash[:16]))
}

func trackingSnapshots(snapshots []market.MarketSnapshot) []arbitrage.TrackingSnapshot {
	result := make([]arbitrage.TrackingSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		metadata := snapshot.Metadata()
		result = append(result, arbitrage.TrackingSnapshot{Market: metadata.Market, Version: metadata.Version, StateHash: metadata.StateHash})
	}
	return result
}

func trackingDiscoveryTrace(timing strategy.EvaluationTiming) []arbitrage.TrackingDiscoveryDirection {
	result := make([]arbitrage.TrackingDiscoveryDirection, 0, len(timing.Directions))
	for _, directionTiming := range timing.Directions {
		direction := arbitrage.TrackingDiscoveryDirection{Direction: directionTiming.Direction, Duration: directionTiming.Duration}
		for _, quote := range directionTiming.Quotes {
			direction.Quotes = append(direction.Quotes, arbitrage.TrackingDiscoveryQuote{
				Leg: quote.Leg, Input: quote.Input, Output: quote.Output, Duration: quote.Duration,
				Cached: quote.Cached, Error: quote.Error,
			})
		}
		result = append(result, direction)
	}
	return result
}

func candidateBuyOutput(registry *market.Registry, direction arbitrage.Direction, candidate arbitrage.Candidate) (market.AssetQuantity, error) {
	if registry == nil {
		// Used only for an emergency close projection where the already persisted
		// opening amount remains available to the caller.
		return market.AssetQuantity{}, nil
	}
	buyMarket, ok := registry.Market(direction.BuyMarket)
	if !ok {
		return market.AssetQuantity{}, fmt.Errorf("unknown buy market %q", direction.BuyMarket)
	}
	base, ok := registry.Token(buyMarket.BaseToken)
	if !ok {
		return market.AssetQuantity{}, fmt.Errorf("unknown buy base token")
	}
	return candidate.BuyQuote.AmountOut.ToAssetQuantity(base)
}

func researchReportForPinned(r *Runner, opportunity arbitrage.Opportunity, trace strategy.PinnedEvaluationTiming, evaluationNumber int, snapshots []market.MarketSnapshot) runtimeresearch.Report {
	status := runtimeresearch.StatusHealthy
	for _, snapshot := range snapshots {
		if snapshot.Metadata().Health != market.HealthHealthy {
			status = runtimeresearch.StatusDegraded
			break
		}
	}
	duration := nonNegativeDuration(trace.EvaluationFinishedAt.Sub(trace.EvaluationStartedAt))
	return runtimeresearch.Report{
		RunID: arbitrage.ResearchRunID(r.config.RunID), ConfigHash: r.config.Hash,
		Status: status, Evaluations: evaluationNumber, Opportunities: []arbitrage.Opportunity{opportunity},
		LocalTiming: strategy.EvaluationTiming{Duration: duration, Directions: []strategy.DirectionTiming{trace.Direction}},
	}
}

func quantityGreater(left, right market.AssetQuantity) bool {
	comparison, err := left.Cmp(right)
	return err == nil && comparison > 0
}

func formatQuantity(quantity market.AssetQuantity) string {
	if quantity.Asset() == "" {
		return ""
	}
	return quantity.String() + " " + strings.ToUpper(string(quantity.Asset()))
}

func formatSignedZero(asset market.AssetID) string { return "0 " + strings.ToUpper(string(asset)) }

func fixedTriggerDisplay(trigger arbitrage.TriggerMetadata) (string, string) {
	if trigger.Reference.Value == "" || strings.Contains(trigger.Reference.Value, "bootstrap") || strings.Contains(trigger.Reference.Value, "account-batch") {
		return trigger.Reference.Value, ""
	}
	value := trigger.Reference.Value
	if strings.HasPrefix(value, "0x") && len(value) == 66 {
		return value, "https://bscscan.com/tx/" + value
	}
	if len(value) >= 64 {
		return value, "https://solscan.io/tx/" + value
	}
	return value, ""
}

var _ feedport.Sink = (*fixedRouteSink)(nil)
