package research

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/adapters/feed/synthetic"
	"github.com/VarozXYZ/vernier/adapters/market/constantproduct"
	"github.com/VarozXYZ/vernier/core/costing"
	coreresearch "github.com/VarozXYZ/vernier/core/research"
	"github.com/VarozXYZ/vernier/core/sizing"
	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type Status string

const (
	StatusHealthy  Status = "healthy"
	StatusDegraded Status = "degraded"
)

type Gap struct {
	Market     market.MarketID
	Expected   uint64
	Actual     uint64
	Kind       string
	ReceivedAt time.Time
}

type Report struct {
	RunID         arbitrage.ResearchRunID
	ConfigHash    string
	Status        Status
	Evaluations   int
	Opportunities []arbitrage.Opportunity
	Gaps          []Gap
}

type Runner struct {
	runID         arbitrage.ResearchRunID
	configHash    string
	setup         arbitrage.ArbitrageSetup
	registry      *market.Registry
	maxAge        time.Duration
	cost          costing.Fixed
	evaluator     *coreresearch.Evaluator
	strategyClock *manualClock
	mirrors       map[market.MarketID]feedport.Mirror
	feeds         []builtFeed
	used          bool
	report        Report
}

type builtFeed struct {
	feed    *synthetic.Feed
	mirror  feedport.Mirror
	clock   *manualClock
	timings []eventTiming
}

type eventTiming struct {
	appliedAt  time.Time
	startedAt  time.Time
	finishedAt time.Time
}

type manualClock struct {
	mu  sync.RWMutex
	now time.Time
}

func (c *manualClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

func (c *manualClock) Set(value time.Time) {
	c.mu.Lock()
	c.now = value.UTC()
	c.mu.Unlock()
}

func NewRunner(fixture Fixture, configHash string) (*Runner, error) {
	if fixture.RunID == "" || configHash == "" {
		return nil, fmt.Errorf("run ID and config hash are required")
	}
	maxAge, err := time.ParseDuration(fixture.MaxSnapshotAge)
	if err != nil || maxAge < 0 {
		return nil, fmt.Errorf("invalid max_snapshot_age %q", fixture.MaxSnapshotAge)
	}
	registry, err := buildRegistry(fixture.Catalog)
	if err != nil {
		return nil, err
	}
	setupMarkets := make([]market.MarketID, len(fixture.Setup.Markets))
	for index, id := range fixture.Setup.Markets {
		setupMarkets[index] = market.MarketID(id)
	}
	setup, err := arbitrage.NewArbitrageSetup(arbitrage.SetupID(fixture.Setup.ID), market.PairID(fixture.Setup.Pair), setupMarkets, registry)
	if err != nil {
		return nil, err
	}
	pair, _ := registry.Pair(setup.Pair())
	fixedCost, err := market.ParseAssetQuantity(pair.QuoteAsset, fixture.FixedCost)
	if err != nil {
		return nil, fmt.Errorf("fixed cost: %w", err)
	}
	costSource, err := costing.NewFixed("fixture/fixed", fixedCost)
	if err != nil {
		return nil, err
	}

	strategyClock := &manualClock{}
	mirrors := make(map[market.MarketID]feedport.Mirror, len(setupMarkets))
	quoteSources := make(map[market.MarketID]quoteport.Source, len(setupMarkets))
	feeds := make([]builtFeed, 0, len(fixture.Feeds))
	seenFeeds := make(map[market.MarketID]struct{}, len(fixture.Feeds))
	for _, configuredFeed := range fixture.Feeds {
		marketID := market.MarketID(configuredFeed.Market)
		if len(configuredFeed.Events) == 0 {
			return nil, fmt.Errorf("feed for market %q requires at least one event", marketID)
		}
		if !containsMarket(setupMarkets, marketID) {
			return nil, fmt.Errorf("feed references market %q outside setup", marketID)
		}
		if _, duplicate := seenFeeds[marketID]; duplicate {
			return nil, fmt.Errorf("duplicate feed for market %q", marketID)
		}
		seenFeeds[marketID] = struct{}{}
		candidate, _ := registry.Market(marketID)
		if err := validateConstantProductMarket(registry, candidate); err != nil {
			return nil, err
		}
		clock := &manualClock{}
		mirror, err := constantproduct.NewMirror(marketID, market.SourceID(configuredFeed.Source), clock.Now)
		if err != nil {
			return nil, err
		}
		quoter, err := constantproduct.NewQuoter(market.SourceID("local/"+configuredFeed.Source), candidate)
		if err != nil {
			return nil, err
		}
		events, timings, err := buildEvents(configuredFeed)
		if err != nil {
			return nil, err
		}
		feed, err := synthetic.New(marketID, events)
		if err != nil {
			return nil, err
		}
		mirrors[marketID] = mirror
		quoteSources[marketID] = quoter
		feeds = append(feeds, builtFeed{feed: feed, mirror: mirror, clock: clock, timings: timings})
	}
	for _, marketID := range setupMarkets {
		if _, exists := seenFeeds[marketID]; !exists {
			return nil, fmt.Errorf("missing feed for market %q", marketID)
		}
	}

	strategies := make([]strategy.Strategy, 0, len(fixture.Strategies))
	for _, configuredStrategy := range fixture.Strategies {
		values := make([]market.AssetQuantity, len(configuredStrategy.Sizes))
		for index, size := range configuredStrategy.Sizes {
			values[index], err = market.ParseAssetQuantity(pair.QuoteAsset, size)
			if err != nil {
				return nil, fmt.Errorf("strategy %q size: %w", configuredStrategy.ID, err)
			}
		}
		grid, err := sizing.NewGrid(values)
		if err != nil {
			return nil, fmt.Errorf("strategy %q: %w", configuredStrategy.ID, err)
		}
		threshold, err := market.ParseAssetQuantity(pair.QuoteAsset, configuredStrategy.Threshold)
		if err != nil {
			return nil, fmt.Errorf("strategy %q threshold: %w", configuredStrategy.ID, err)
		}
		candidate, err := strategy.NewTwoMarket(strategy.TwoMarketConfig{
			ID: arbitrage.StrategyID(configuredStrategy.ID), Setup: setup, Registry: registry,
			Sources: quoteSources, Grid: grid, Threshold: threshold, Clock: strategyClock.Now,
		})
		if err != nil {
			return nil, err
		}
		strategies = append(strategies, candidate)
	}
	evaluator, err := coreresearch.NewEvaluator(strategies)
	if err != nil {
		return nil, err
	}
	return &Runner{
		runID: arbitrage.ResearchRunID(fixture.RunID), configHash: configHash, setup: setup,
		registry: registry, maxAge: maxAge, cost: costSource, evaluator: evaluator,
		strategyClock: strategyClock, mirrors: mirrors, feeds: feeds,
	}, nil
}

func (r *Runner) Run(ctx context.Context) (Report, error) {
	if r.used {
		return Report{}, fmt.Errorf("runner is single-use")
	}
	r.used = true
	r.report = Report{RunID: r.runID, ConfigHash: r.configHash, Status: StatusHealthy}
	for index := range r.feeds {
		configured := &r.feeds[index]
		sink := &runtimeSink{runner: r, mirror: configured.mirror, clock: configured.clock, timings: configured.timings}
		if err := configured.feed.Run(ctx, sink); err != nil {
			return Report{}, err
		}
	}
	return r.report, nil
}

type runtimeSink struct {
	runner  *Runner
	mirror  feedport.Mirror
	clock   *manualClock
	timings []eventTiming
	index   int
}

func (s *runtimeSink) Publish(ctx context.Context, event market.MarketEvent) error {
	if s.index >= len(s.timings) {
		return fmt.Errorf("event timing missing for market %q", event.Market)
	}
	timing := s.timings[s.index]
	s.index++
	s.clock.Set(timing.appliedAt)
	_, err := s.mirror.Apply(ctx, event)
	if err != nil {
		var violation feedport.SequenceViolation
		if !errors.As(err, &violation) {
			return err
		}
		kind := "duplicate_or_out_of_order"
		if violation.IsGap() {
			kind = "gap"
		}
		s.runner.report.Status = StatusDegraded
		s.runner.report.Gaps = append(s.runner.report.Gaps, Gap{
			Market: violation.Market, Expected: violation.Expected, Actual: violation.Actual,
			Kind: kind, ReceivedAt: event.ReceivedAt,
		})
	}
	return s.runner.evaluate(ctx, event, timing)
}

func (r *Runner) evaluate(ctx context.Context, event market.MarketEvent, timing eventTiming) error {
	snapshots := make([]market.MarketSnapshot, 0, len(r.setup.Markets()))
	for _, marketID := range r.setup.Markets() {
		snapshot, exists := r.mirrors[marketID].Current()
		if !exists {
			return nil
		}
		snapshots = append(snapshots, snapshot)
	}
	r.strategyClock.Set(timing.finishedAt)
	cost, err := r.cost.Snapshot(timing.startedAt)
	if err != nil {
		return err
	}
	r.report.Evaluations++
	opportunities, err := r.evaluator.Evaluate(ctx, coreresearch.EvaluationRequest{
		IDPrefix: fmt.Sprintf("evaluation-%04d", r.report.Evaluations), Run: r.runID,
		ConfigHash: r.configHash, Snapshots: snapshots, Cost: cost,
		TriggeredAt: event.ReceivedAt, StartedAt: timing.startedAt, MaxSnapshotAge: r.maxAge,
	})
	if err != nil {
		return err
	}
	r.report.Opportunities = append(r.report.Opportunities, opportunities...)
	return nil
}

func buildRegistry(fixture CatalogFixture) (*market.Registry, error) {
	catalog := market.Catalog{}
	for _, value := range fixture.Chains {
		catalog.Chains = append(catalog.Chains, market.Chain{ID: market.ChainID(value.ID)})
	}
	for _, value := range fixture.Assets {
		catalog.Assets = append(catalog.Assets, market.Asset{ID: market.AssetID(value.ID), Symbol: value.Symbol})
	}
	for _, value := range fixture.Tokens {
		catalog.Tokens = append(catalog.Tokens, market.Token{
			ID: market.TokenID(value.ID), Asset: market.AssetID(value.Asset), Chain: market.ChainID(value.Chain),
			Decimals: value.Decimals, Symbol: value.Symbol,
		})
	}
	for _, value := range fixture.Venues {
		catalog.Venues = append(catalog.Venues, market.Venue{ID: market.VenueID(value.ID)})
	}
	for _, value := range fixture.Pairs {
		catalog.Pairs = append(catalog.Pairs, market.Pair{
			ID: market.PairID(value.ID), BaseAsset: market.AssetID(value.BaseAsset), QuoteAsset: market.AssetID(value.QuoteAsset),
		})
	}
	for _, value := range fixture.Pools {
		tokens := make([]market.TokenID, len(value.Tokens))
		for index, token := range value.Tokens {
			tokens[index] = market.TokenID(token)
		}
		catalog.Pools = append(catalog.Pools, market.Pool{
			ID: market.PoolID(value.ID), Venue: market.VenueID(value.Venue), Chain: market.ChainID(value.Chain),
			Tokens: tokens, Adapter: value.Adapter,
		})
	}
	for _, value := range fixture.Paths {
		hops := make([]market.Hop, len(value.Hops))
		for index, hop := range value.Hops {
			hops[index] = market.Hop{Pool: market.PoolID(hop.Pool), TokenIn: market.TokenID(hop.TokenIn), TokenOut: market.TokenID(hop.TokenOut)}
		}
		catalog.Paths = append(catalog.Paths, market.Path{ID: market.PathID(value.ID), Chain: market.ChainID(value.Chain), Hops: hops})
	}
	for _, value := range fixture.Markets {
		catalog.Markets = append(catalog.Markets, market.Market{
			ID: market.MarketID(value.ID), Pair: market.PairID(value.Pair), Chain: market.ChainID(value.Chain),
			Path: market.PathID(value.Path), BaseToken: market.TokenID(value.BaseToken), QuoteToken: market.TokenID(value.QuoteToken),
		})
	}
	return market.NewRegistry(catalog)
}

func buildEvents(fixture FeedFixture) ([]market.MarketEvent, []eventTiming, error) {
	events := make([]market.MarketEvent, 0, len(fixture.Events))
	timings := make([]eventTiming, 0, len(fixture.Events))
	for index, configured := range fixture.Events {
		baseReserve, ok := new(big.Int).SetString(configured.BaseReserve, 10)
		if !ok {
			return nil, nil, fmt.Errorf("feed %q event %d has invalid base reserve", fixture.Market, index)
		}
		quoteReserve, ok := new(big.Int).SetString(configured.QuoteReserve, 10)
		if !ok {
			return nil, nil, fmt.Errorf("feed %q event %d has invalid quote reserve", fixture.Market, index)
		}
		update, err := constantproduct.NewReserveUpdate(baseReserve, quoteReserve, configured.FeeBPS)
		if err != nil {
			return nil, nil, fmt.Errorf("feed %q event %d: %w", fixture.Market, index, err)
		}
		receivedAt, err := parseTimestamp("received_at", configured.ReceivedAt)
		if err != nil {
			return nil, nil, err
		}
		appliedAt, err := parseTimestamp("applied_at", configured.AppliedAt)
		if err != nil {
			return nil, nil, err
		}
		startedAt, err := parseTimestamp("evaluation_started_at", configured.EvaluationStartedAt)
		if err != nil {
			return nil, nil, err
		}
		finishedAt, err := parseTimestamp("evaluation_finished_at", configured.EvaluationFinishedAt)
		if err != nil {
			return nil, nil, err
		}
		if appliedAt.Before(receivedAt) || startedAt.Before(appliedAt) || finishedAt.Before(startedAt) {
			return nil, nil, fmt.Errorf("feed %q event %d timestamps are not causal", fixture.Market, index)
		}
		finality, err := parseFinality(configured.Finality)
		if err != nil {
			return nil, nil, err
		}
		event := market.MarketEvent{
			Market: market.MarketID(fixture.Market), Source: market.SourceID(fixture.Source),
			Sequence: configured.Sequence, Finality: finality, ReceivedAt: receivedAt, Data: update,
		}
		if configured.SourceTime != "" {
			event.SourceTime, err = parseTimestamp("source_time", configured.SourceTime)
			if err != nil {
				return nil, nil, err
			}
			if event.SourceTime.After(receivedAt) {
				return nil, nil, fmt.Errorf("feed %q event %d source time is after receipt", fixture.Market, index)
			}
			event.SourceTimeKnown = true
		}
		event, err = market.NewMarketEvent(event)
		if err != nil {
			return nil, nil, fmt.Errorf("feed %q event %d: %w", fixture.Market, index, err)
		}
		events = append(events, event)
		timings = append(timings, eventTiming{appliedAt: appliedAt, startedAt: startedAt, finishedAt: finishedAt})
	}
	return events, timings, nil
}

func parseTimestamp(field, value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid %s timestamp %q", field, value)
	}
	return parsed.UTC(), nil
}

func parseFinality(value string) (market.Finality, error) {
	finality := market.Finality(value)
	switch finality {
	case market.FinalityPreconfirmed, market.FinalityConfirmed, market.FinalityFinalized:
		return finality, nil
	default:
		return "", fmt.Errorf("invalid finality %q", value)
	}
}

func validateConstantProductMarket(registry *market.Registry, candidate market.Market) error {
	path, ok := registry.Path(candidate.Path)
	if !ok || len(path.Hops) != 1 {
		return fmt.Errorf("synthetic market %q requires a one-hop path", candidate.ID)
	}
	pool, ok := registry.Pool(path.Hops[0].Pool)
	if !ok || pool.Adapter != "constant_product" {
		return fmt.Errorf("synthetic market %q requires constant_product adapter", candidate.ID)
	}
	return nil
}

func containsMarket(markets []market.MarketID, candidate market.MarketID) bool {
	for _, marketID := range markets {
		if marketID == candidate {
			return true
		}
	}
	return false
}
