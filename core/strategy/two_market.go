package strategy

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/core/sizing"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type Clock func() time.Time

// QuoteTiming describes one deterministic local quote used by a direction.
// Duration includes the cache lookup and, on a miss, the complete local
// calculation. It never includes an external validation request.
type QuoteTiming struct {
	Market    market.MarketID
	Source    market.SourceID
	Leg       string
	Mode      market.QuoteMode
	Duration  time.Duration
	Cached    bool
	Error     string
	AmountIn  market.TokenAmount
	AmountOut market.TokenAmount
	Input     market.AssetQuantity
	Output    market.AssetQuantity
	Hops      []quoteport.HopTiming
}

// DirectionTiming keeps the dependent buy-then-sell timings together with
// the total time spent evaluating one direction. Independent directions may
// run concurrently.
type DirectionTiming struct {
	Direction arbitrage.Direction
	Duration  time.Duration
	Quotes    []QuoteTiming
}

// DirectionProbeTiming records one fixed quote-budget probe used to choose a
// purchase market before the complete sizing curve is evaluated.
type DirectionProbeTiming struct {
	Size     market.AssetQuantity
	Outputs  []DirectionProbeOutput
	Winner   market.MarketID
	Reason   string
	Duration time.Duration
}

// DirectionProbeOutput is the comparable base-asset output from one market.
type DirectionProbeOutput struct {
	Market   market.MarketID
	Output   market.AssetQuantity
	Duration time.Duration
	Cached   bool
	Error    string
}

// DirectionDiscoveryTiming records the early direction decision and its
// evidence. An empty Selected value means the strategy used the safe fallback.
type DirectionDiscoveryTiming struct {
	Samples  int
	Duration time.Duration
	Selected arbitrage.Direction
	Decision string
	Probes   []DirectionProbeTiming
}

// EvaluationTiming is the local Research hot-path trace. Directions retain
// setup order even though they run concurrently. Within each direction sell
// quotes follow their dependent buy quotes.
type EvaluationTiming struct {
	Duration   time.Duration
	Discovery  *DirectionDiscoveryTiming
	Directions []DirectionTiming
}

// PinnedEvaluationTiming records the dependent stages of one exact trade.
// It is intentionally separate from grid discovery so Research can persist
// every stage boundary without inferring timestamps from aggregate durations.
type PinnedEvaluationTiming struct {
	EvaluationStartedAt  time.Time
	BuyStartedAt         time.Time
	BuyFinishedAt        time.Time
	ConversionStartedAt  time.Time
	ConversionFinishedAt time.Time
	SellStartedAt        time.Time
	SellFinishedAt       time.Time
	PnLStartedAt         time.Time
	PnLFinishedAt        time.Time
	EvaluationFinishedAt time.Time
	Direction            DirectionTiming
}

type SizingAsset string

const (
	SizingAssetBase  SizingAsset = "base"
	SizingAssetQuote SizingAsset = "quote"
)

type TwoMarketConfig struct {
	ID             arbitrage.StrategyID
	Setup          arbitrage.ArbitrageSetup
	Registry       *market.Registry
	Sources        map[market.MarketID]quoteport.Source
	Grid           sizing.Grid
	Threshold      market.AssetQuantity
	ThresholdFixed market.AssetQuantity
	ThresholdBPS   uint16
	Clock          Clock
	SizingAsset    SizingAsset
	// DirectionDiscoverySamples enables a quick min/mid/max-style direction
	// probe before exhaustive sizing. Zero preserves the explicit two-direction
	// behavior for callers that need it.
	DirectionDiscoverySamples int
}

type TwoMarketCrossChainArbitrage struct {
	id               arbitrage.StrategyID
	setup            arbitrage.ArbitrageSetup
	registry         *market.Registry
	sources          map[market.MarketID]quoteport.Source
	grid             sizing.Grid
	threshold        market.AssetQuantity
	thresholdFixed   market.AssetQuantity
	thresholdBPS     uint16
	clock            Clock
	cache            quoteCache
	sizingAsset      market.AssetID
	discoverySamples int
}

func NewTwoMarket(config TwoMarketConfig) (*TwoMarketCrossChainArbitrage, error) {
	if config.ID == "" || config.Registry == nil || config.Clock == nil {
		return nil, fmt.Errorf("strategy ID, registry, and clock are required")
	}
	if len(config.Setup.Markets()) != 2 {
		return nil, fmt.Errorf("two-market strategy requires exactly two markets")
	}
	pair, ok := config.Registry.Pair(config.Setup.Pair())
	if !ok {
		return nil, fmt.Errorf("setup references unknown pair %q", config.Setup.Pair())
	}
	basis := config.SizingAsset
	if basis == "" {
		basis = SizingAssetQuote
	}
	var sizingAsset market.AssetID
	switch basis {
	case SizingAssetBase:
		sizingAsset = pair.BaseAsset
	case SizingAssetQuote:
		sizingAsset = pair.QuoteAsset
	default:
		return nil, fmt.Errorf("unsupported sizing asset %q", basis)
	}
	if config.Grid.Asset() != sizingAsset {
		return nil, fmt.Errorf("sizing grid must use %s asset %q", basis, sizingAsset)
	}
	if config.DirectionDiscoverySamples != 0 && config.DirectionDiscoverySamples < 3 {
		return nil, fmt.Errorf("direction discovery requires at least three samples")
	}
	if config.DirectionDiscoverySamples > len(config.Grid.Values()) {
		return nil, fmt.Errorf("direction discovery samples exceed sizing grid")
	}
	if config.Threshold.Asset() != pair.QuoteAsset || config.Threshold.Sign() < 0 {
		return nil, fmt.Errorf("non-negative threshold must use quote asset %q", pair.QuoteAsset)
	}
	if config.ThresholdBPS > 0 {
		if config.ThresholdBPS > 10_000 || config.ThresholdFixed.Asset() != pair.QuoteAsset || config.ThresholdFixed.Sign() <= 0 || config.Threshold.Sign() != 0 {
			return nil, fmt.Errorf("percentage threshold requires a positive quote-asset floor, 1..10000 BPS, and zero legacy threshold")
		}
	}
	sources := make(map[market.MarketID]quoteport.Source, len(config.Sources))
	for _, marketID := range config.Setup.Markets() {
		source, exists := config.Sources[marketID]
		if !exists || source == nil {
			return nil, fmt.Errorf("quote source is required for market %q", marketID)
		}
		sources[marketID] = source
	}
	return &TwoMarketCrossChainArbitrage{
		id: config.ID, setup: config.Setup, registry: config.Registry, sources: sources,
		grid: config.Grid, threshold: config.Threshold, thresholdFixed: config.ThresholdFixed,
		thresholdBPS: config.ThresholdBPS, clock: config.Clock, sizingAsset: sizingAsset,
		discoverySamples: config.DirectionDiscoverySamples,
		cache:            newQuoteCache(),
	}, nil
}

func (s *TwoMarketCrossChainArbitrage) ID() arbitrage.StrategyID { return s.id }

func (s *TwoMarketCrossChainArbitrage) Evaluate(ctx context.Context, evaluation arbitrage.Evaluation) ([]arbitrage.Opportunity, error) {
	opportunities, _, err := s.EvaluateWithTiming(ctx, evaluation)
	return opportunities, err
}

// EvaluateWithTiming evaluates fixed snapshots and returns the local timing
// trace alongside the economic results. When direction discovery is enabled,
// it selects one direction before exhaustive sizing; an uncertain decision
// keeps both configured directions. External reference providers are
// intentionally absent from this method.
func (s *TwoMarketCrossChainArbitrage) EvaluateWithTiming(ctx context.Context, evaluation arbitrage.Evaluation) ([]arbitrage.Opportunity, EvaluationTiming, error) {
	return s.evaluateWithTiming(ctx, evaluation, false)
}

// EvaluateFreshWithTiming bypasses caches only for sources that explicitly
// implement FreshSource. It is intended for mandatory confirmation of an
// apparently profitable remote estimate, not for normal event evaluation.
func (s *TwoMarketCrossChainArbitrage) EvaluateFreshWithTiming(ctx context.Context, evaluation arbitrage.Evaluation) ([]arbitrage.Opportunity, EvaluationTiming, error) {
	return s.evaluateWithTiming(ctx, evaluation, true)
}

// EvaluatePinnedWithTiming evaluates exactly one previously selected
// direction and quote-asset input. It never consults the sizing grid and
// therefore cannot silently resize or flip an active opportunity window.
func (s *TwoMarketCrossChainArbitrage) EvaluatePinnedWithTiming(
	ctx context.Context,
	evaluation arbitrage.Evaluation,
	direction arbitrage.Direction,
	size market.AssetQuantity,
) (arbitrage.Opportunity, PinnedEvaluationTiming, error) {
	trace := PinnedEvaluationTiming{EvaluationStartedAt: s.clock().UTC()}
	if evaluation.Strategy() != s.id {
		return arbitrage.Opportunity{}, trace, fmt.Errorf("evaluation targets strategy %q, expected %q", evaluation.Strategy(), s.id)
	}
	if size.Asset() != s.sizingAsset || s.sizingAsset == "" {
		return arbitrage.Opportunity{}, trace, fmt.Errorf("pinned size must use sizing asset %q", s.sizingAsset)
	}
	configured := false
	for _, candidate := range s.setup.Directions() {
		if candidate == direction {
			configured = true
			break
		}
	}
	if !configured {
		return arbitrage.Opportunity{}, trace, fmt.Errorf("pinned direction is not part of the setup")
	}
	opportunity := arbitrage.Opportunity{
		Evaluation: evaluation.ID(), Run: evaluation.Run(), ConfigHash: evaluation.ConfigHash(),
		Strategy: s.id, Direction: direction, Classification: arbitrage.ClassificationUnclassifiable,
		SelectedIndex: -1, Threshold: s.threshold, TriggeredAt: evaluation.TriggeredAt(), StartedAt: evaluation.StartedAt(),
	}
	opportunity.Trigger, opportunity.HasTrigger = evaluation.Trigger()
	buySnapshot, buyOK := evaluation.Snapshot(direction.BuyMarket)
	sellSnapshot, sellOK := evaluation.Snapshot(direction.SellMarket)
	if !buyOK || !sellOK {
		opportunity.Reasons = []string{"missing_market_snapshot"}
		return s.finishPinned(opportunity, &trace), trace, nil
	}
	opportunity.Snapshots = []market.SnapshotMetadata{buySnapshot.Metadata(), sellSnapshot.Metadata()}
	for _, snapshot := range []market.MarketSnapshot{buySnapshot, sellSnapshot} {
		if snapshot.Metadata().Health != market.HealthHealthy {
			opportunity.Reasons = []string{"degraded_market_snapshot"}
			return s.finishPinned(opportunity, &trace), trace, nil
		}
	}
	buyMarket, buyOK := s.registry.Market(direction.BuyMarket)
	sellMarket, sellOK := s.registry.Market(direction.SellMarket)
	if !buyOK || !sellOK {
		opportunity.Reasons = []string{"unknown_market"}
		return s.finishPinned(opportunity, &trace), trace, nil
	}
	buyBase, _ := s.registry.Token(buyMarket.BaseToken)
	buyQuote, _ := s.registry.Token(buyMarket.QuoteToken)
	sellBase, _ := s.registry.Token(sellMarket.BaseToken)
	sellQuote, _ := s.registry.Token(sellMarket.QuoteToken)
	if s.sizingAsset != buyQuote.Asset {
		return arbitrage.Opportunity{}, trace, fmt.Errorf("pinned tracking currently requires quote-asset sizing")
	}
	directionTiming := DirectionTiming{Direction: direction}
	directionStarted := s.clock()
	candidate, err := s.quoteSizedCandidate(ctx, evaluation, direction, buySnapshot, sellSnapshot, buyBase, buyQuote, sellBase, sellQuote, size, &directionTiming, false, &trace)
	directionTiming.Duration = nonNegative(s.clock().Sub(directionStarted))
	trace.Direction = directionTiming
	if err != nil {
		opportunity.Reasons = []string{err.Error()}
		return s.finishPinned(opportunity, &trace), trace, nil
	}
	fixed, percentage, effective, err := s.candidateThreshold(candidate.Input, buyQuote)
	if err != nil {
		return arbitrage.Opportunity{}, trace, err
	}
	candidate.FixedThreshold = fixed
	candidate.PercentageThreshold = percentage
	candidate.EffectiveThreshold = effective
	opportunity.Candidates = []arbitrage.Candidate{candidate}
	opportunity.SelectedIndex = 0
	opportunity.Threshold = effective
	switch {
	case candidate.GrossPnL.Sign() <= 0:
		opportunity.Classification, opportunity.Reasons = arbitrage.ClassificationNoSpread, []string{"no_positive_gross_profit"}
	case candidate.NetPnL.Sign() <= 0:
		opportunity.Classification, opportunity.Reasons = arbitrage.ClassificationObservedSpread, []string{"costs_exceed_gross_profit"}
	case !greaterOrEqual(candidate.NetPnL, effective):
		opportunity.Classification, opportunity.Reasons = arbitrage.ClassificationEconomic, []string{"below_profit_threshold"}
	default:
		opportunity.Classification, opportunity.Reasons = arbitrage.ClassificationPolicyQualified, []string{"profit_threshold_met"}
	}
	return s.finishPinned(opportunity, &trace), trace, nil
}

func (s *TwoMarketCrossChainArbitrage) finishPinned(opportunity arbitrage.Opportunity, trace *PinnedEvaluationTiming) arbitrage.Opportunity {
	finished := s.clock().UTC()
	opportunity.FinishedAt = finished
	trace.EvaluationFinishedAt = finished
	return opportunity
}

func (s *TwoMarketCrossChainArbitrage) evaluateWithTiming(ctx context.Context, evaluation arbitrage.Evaluation, refresh bool) ([]arbitrage.Opportunity, EvaluationTiming, error) {
	if evaluation.Strategy() != s.id {
		return nil, EvaluationTiming{}, fmt.Errorf("evaluation targets strategy %q, expected %q", evaluation.Strategy(), s.id)
	}
	if evaluation.Cost().Amount.Asset() != s.threshold.Asset() {
		return nil, EvaluationTiming{}, fmt.Errorf("cost asset does not match strategy quote asset")
	}
	started := s.clock()
	opportunities := make([]arbitrage.Opportunity, 0, len(s.setup.Directions()))
	timing := EvaluationTiming{Directions: make([]DirectionTiming, 0, len(s.setup.Directions()))}
	directions := s.setup.Directions()
	if s.discoverySamples > 0 {
		selected, discovery, discoveryErr := s.discoverDirection(ctx, evaluation, refresh)
		if discoveryErr != nil {
			return nil, EvaluationTiming{}, discoveryErr
		}
		timing.Discovery = &discovery
		if selected != nil {
			directions = []arbitrage.Direction{*selected}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, EvaluationTiming{}, err
	}
	opportunities = make([]arbitrage.Opportunity, len(directions))
	timing.Directions = make([]DirectionTiming, len(directions))
	var directionsWG sync.WaitGroup
	for index, direction := range directions {
		index, direction := index, direction
		directionsWG.Add(1)
		go func() {
			defer directionsWG.Done()
			directionStarted := s.clock()
			directionTiming := DirectionTiming{Direction: direction}
			opportunities[index] = s.evaluateDirection(ctx, evaluation, direction, &directionTiming, refresh)
			directionTiming.Duration = nonNegative(s.clock().Sub(directionStarted))
			timing.Directions[index] = directionTiming
		}()
	}
	directionsWG.Wait()
	if err := ctx.Err(); err != nil {
		return nil, EvaluationTiming{}, err
	}
	timing.Duration = nonNegative(s.clock().Sub(started))
	return opportunities, timing, nil
}

// discoverDirection uses a small, deterministic probe set before the full
// sizing curve. For quote-asset sizing, more base asset output means a lower
// purchase price. A strict majority wins; ties, failed probes, and equal
// outputs deliberately fall back to evaluating both directions.
func (s *TwoMarketCrossChainArbitrage) discoverDirection(ctx context.Context, evaluation arbitrage.Evaluation, refresh bool) (*arbitrage.Direction, DirectionDiscoveryTiming, error) {
	started := s.clock()
	timing := DirectionDiscoveryTiming{Samples: s.discoverySamples}
	directions := s.setup.Directions()
	if len(directions) != 2 {
		timing.Decision = "unsupported_setup"
		timing.Duration = nonNegative(s.clock().Sub(started))
		return nil, timing, nil
	}
	marketA, aOK := s.registry.Market(directions[0].BuyMarket)
	marketB, bOK := s.registry.Market(directions[0].SellMarket)
	if !aOK || !bOK {
		timing.Decision = "unknown_market"
		timing.Duration = nonNegative(s.clock().Sub(started))
		return nil, timing, nil
	}
	snapshotA, aSnapshotOK := evaluation.Snapshot(marketA.ID)
	snapshotB, bSnapshotOK := evaluation.Snapshot(marketB.ID)
	if !aSnapshotOK || !bSnapshotOK {
		timing.Decision = "missing_snapshot"
		timing.Duration = nonNegative(s.clock().Sub(started))
		return nil, timing, nil
	}
	tokenAQuote, aTokenOK := s.registry.Token(marketA.QuoteToken)
	tokenBQuote, bTokenOK := s.registry.Token(marketB.QuoteToken)
	tokenABase, aBaseOK := s.registry.Token(marketA.BaseToken)
	tokenBBase, bBaseOK := s.registry.Token(marketB.BaseToken)
	if !aTokenOK || !bTokenOK || !aBaseOK || !bBaseOK {
		timing.Decision = "unknown_token"
		timing.Duration = nonNegative(s.clock().Sub(started))
		return nil, timing, nil
	}
	if s.sizingAsset != tokenAQuote.Asset || s.sizingAsset != tokenBQuote.Asset {
		timing.Decision = "unsupported_sizing_asset"
		timing.Duration = nonNegative(s.clock().Sub(started))
		return nil, timing, nil
	}
	values := s.grid.Values()
	wins := map[market.MarketID]int{marketA.ID: 0, marketB.ID: 0}
	valid := 0
	for sample := 0; sample < s.discoverySamples; sample++ {
		if err := ctx.Err(); err != nil {
			return nil, timing, err
		}
		index := sample * (len(values) - 1) / (s.discoverySamples - 1)
		probe := DirectionProbeTiming{Size: values[index]}
		probeStarted := s.clock()
		probeQuotes := make([]struct {
			market   market.MarketID
			snapshot market.MarketSnapshot
			tokenIn  market.Token
			tokenOut market.Token
		}, 2)
		probeQuotes[0] = struct {
			market   market.MarketID
			snapshot market.MarketSnapshot
			tokenIn  market.Token
			tokenOut market.Token
		}{marketA.ID, snapshotA, tokenAQuote, tokenABase}
		probeQuotes[1] = struct {
			market   market.MarketID
			snapshot market.MarketSnapshot
			tokenIn  market.Token
			tokenOut market.Token
		}{marketB.ID, snapshotB, tokenBQuote, tokenBBase}
		for _, candidate := range probeQuotes {
			quoteTiming := DirectionTiming{}
			input, err := probe.Size.ToTokenAmount(candidate.tokenIn)
			if err != nil || input.IsZero() {
				probe.Reason = "probe_size_rounds_to_zero"
				continue
			}
			quote, err := s.input(ctx, s.sources[candidate.market], quoteport.Input{
				Snapshot: candidate.snapshot, TokenIn: candidate.tokenIn.ID, TokenOut: candidate.tokenOut.ID,
				AmountIn: input, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: evaluation.StartedAt(),
			}, &quoteTiming, "discovery", refresh)
			if err != nil && ctx.Err() != nil {
				return nil, timing, ctx.Err()
			}
			var output market.AssetQuantity
			if err == nil {
				output, err = quote.AmountOut.ToAssetQuantity(candidate.tokenOut)
			}
			probeOutput := DirectionProbeOutput{Market: candidate.market}
			for _, recorded := range quoteTiming.Quotes {
				if recorded.Market == candidate.market {
					probeOutput.Duration = recorded.Duration
					probeOutput.Cached = recorded.Cached
					break
				}
			}
			if err != nil {
				if probe.Reason == "" {
					probe.Reason = "probe_quote_failed"
				}
				probeOutput.Output, _ = market.NewAssetQuantity(candidate.tokenOut.Asset, new(big.Rat))
				probeOutput.Error = err.Error()
				probe.Outputs = append(probe.Outputs, probeOutput)
				continue
			}
			probeOutput.Output = output
			probe.Outputs = append(probe.Outputs, probeOutput)
		}
		if len(probe.Outputs) == 2 && probe.Outputs[0].Error == "" && probe.Outputs[1].Error == "" && probe.Outputs[0].Output.Asset() == probe.Outputs[1].Output.Asset() {
			comparison, err := probe.Outputs[0].Output.Cmp(probe.Outputs[1].Output)
			if err == nil && comparison != 0 {
				valid++
				if comparison > 0 {
					probe.Winner = marketA.ID
					wins[marketA.ID]++
				} else {
					probe.Winner = marketB.ID
					wins[marketB.ID]++
				}
			} else if probe.Reason == "" {
				probe.Reason = "equal_probe_output"
			}
		} else if probe.Reason == "" {
			probe.Reason = "incomplete_probe"
		}
		probe.Duration = nonNegative(s.clock().Sub(probeStarted))
		timing.Probes = append(timing.Probes, probe)
	}
	if valid == s.discoverySamples && wins[marketA.ID] > valid/2 {
		selected := arbitrage.Direction{BuyMarket: marketA.ID, SellMarket: marketB.ID}
		timing.Selected, timing.Decision = selected, "majority"
		timing.Duration = nonNegative(s.clock().Sub(started))
		return &selected, timing, nil
	}
	if valid == s.discoverySamples && wins[marketB.ID] > valid/2 {
		selected := arbitrage.Direction{BuyMarket: marketB.ID, SellMarket: marketA.ID}
		timing.Selected, timing.Decision = selected, "majority"
		timing.Duration = nonNegative(s.clock().Sub(started))
		return &selected, timing, nil
	}
	timing.Decision = "uncertain_fallback_both"
	timing.Duration = nonNegative(s.clock().Sub(started))
	return nil, timing, nil
}

func (s *TwoMarketCrossChainArbitrage) evaluateDirection(ctx context.Context, evaluation arbitrage.Evaluation, direction arbitrage.Direction, timing *DirectionTiming, refresh bool) arbitrage.Opportunity {
	opportunity := arbitrage.Opportunity{
		Evaluation: evaluation.ID(), Run: evaluation.Run(), ConfigHash: evaluation.ConfigHash(),
		Strategy: s.id, Direction: direction,
		Classification: arbitrage.ClassificationUnclassifiable, SelectedIndex: -1,
		Threshold: s.threshold, TriggeredAt: evaluation.TriggeredAt(), StartedAt: evaluation.StartedAt(),
	}
	opportunity.Trigger, opportunity.HasTrigger = evaluation.Trigger()
	buySnapshot, buyOK := evaluation.Snapshot(direction.BuyMarket)
	sellSnapshot, sellOK := evaluation.Snapshot(direction.SellMarket)
	if !buyOK || !sellOK {
		opportunity.Reasons = []string{"missing_market_snapshot"}
		return s.finish(opportunity)
	}
	opportunity.Snapshots = []market.SnapshotMetadata{buySnapshot.Metadata(), sellSnapshot.Metadata()}
	for _, snapshot := range []market.MarketSnapshot{buySnapshot, sellSnapshot} {
		metadata := snapshot.Metadata()
		if metadata.Health != market.HealthHealthy {
			opportunity.Reasons = []string{"degraded_market_snapshot"}
			return s.finish(opportunity)
		}
	}

	buyMarket, buyExists := s.registry.Market(direction.BuyMarket)
	sellMarket, sellExists := s.registry.Market(direction.SellMarket)
	if !buyExists || !sellExists {
		opportunity.Reasons = []string{"unknown_market"}
		return s.finish(opportunity)
	}
	buyBase, _ := s.registry.Token(buyMarket.BaseToken)
	buyQuote, _ := s.registry.Token(buyMarket.QuoteToken)
	sellBase, _ := s.registry.Token(sellMarket.BaseToken)
	sellQuote, _ := s.registry.Token(sellMarket.QuoteToken)

	bestOverall := -1
	bestQualified := -1
	for _, size := range s.grid.Values() {
		candidate, err := s.candidate(ctx, evaluation, direction, buySnapshot, sellSnapshot, buyBase, buyQuote, sellBase, sellQuote, size, timing, refresh)
		if err != nil {
			opportunity.Reasons = append(opportunity.Reasons, err.Error())
			continue
		}
		fixedThreshold, percentageThreshold, effectiveThreshold, thresholdErr := s.candidateThreshold(candidate.Input, buyQuote)
		if thresholdErr != nil {
			opportunity.Reasons = append(opportunity.Reasons, "threshold_failed: "+thresholdErr.Error())
			continue
		}
		candidate.FixedThreshold = fixedThreshold
		candidate.PercentageThreshold = percentageThreshold
		candidate.EffectiveThreshold = effectiveThreshold
		opportunity.Candidates = append(opportunity.Candidates, candidate)
		index := len(opportunity.Candidates) - 1
		if bestOverall < 0 || greater(candidate.NetPnL, opportunity.Candidates[bestOverall].NetPnL) {
			bestOverall = index
		}
		if greaterOrEqual(candidate.NetPnL, effectiveThreshold) &&
			(bestQualified < 0 || greater(candidate.NetPnL, opportunity.Candidates[bestQualified].NetPnL)) {
			bestQualified = index
		}
	}
	if bestQualified >= 0 {
		opportunity.SelectedIndex = bestQualified
	} else {
		opportunity.SelectedIndex = bestOverall
	}
	if opportunity.SelectedIndex < 0 {
		if len(opportunity.Reasons) == 0 {
			opportunity.Reasons = []string{"no_valid_size"}
		}
		return s.finish(opportunity)
	}

	selected := opportunity.Candidates[opportunity.SelectedIndex]
	opportunity.Threshold = selected.EffectiveThreshold
	switch {
	case selected.GrossPnL.Sign() <= 0:
		opportunity.Classification = arbitrage.ClassificationNoSpread
		opportunity.Reasons = []string{"no_positive_gross_profit"}
	case selected.NetPnL.Sign() <= 0:
		opportunity.Classification = arbitrage.ClassificationObservedSpread
		opportunity.Reasons = []string{"costs_exceed_gross_profit"}
	case !greaterOrEqual(selected.NetPnL, selected.EffectiveThreshold):
		opportunity.Classification = arbitrage.ClassificationEconomic
		opportunity.Reasons = []string{"below_profit_threshold"}
	default:
		opportunity.Classification = arbitrage.ClassificationPolicyQualified
		opportunity.Reasons = []string{"profit_threshold_met"}
	}
	return s.finish(opportunity)
}

func (s *TwoMarketCrossChainArbitrage) candidateThreshold(input market.AssetQuantity, quote market.Token) (market.AssetQuantity, market.AssetQuantity, market.AssetQuantity, error) {
	zero, err := market.NewAssetQuantity(input.Asset(), new(big.Rat))
	if err != nil {
		return market.AssetQuantity{}, market.AssetQuantity{}, market.AssetQuantity{}, err
	}
	if s.thresholdBPS == 0 {
		return s.threshold, zero, s.threshold, nil
	}
	inputUnits, err := input.ToTokenAmount(quote)
	if err != nil {
		return market.AssetQuantity{}, market.AssetQuantity{}, market.AssetQuantity{}, err
	}
	numerator := new(big.Int).Mul(inputUnits.Units(), new(big.Int).SetUint64(uint64(s.thresholdBPS)))
	denominator := big.NewInt(10_000)
	percentageUnits := new(big.Int).Quo(numerator, denominator)
	if new(big.Int).Mod(numerator, denominator).Sign() != 0 {
		percentageUnits.Add(percentageUnits, big.NewInt(1))
	}
	percentageToken, err := market.NewTokenAmount(quote.ID, percentageUnits)
	if err != nil {
		return market.AssetQuantity{}, market.AssetQuantity{}, market.AssetQuantity{}, err
	}
	percentage, err := percentageToken.ToAssetQuantity(quote)
	if err != nil {
		return market.AssetQuantity{}, market.AssetQuantity{}, market.AssetQuantity{}, err
	}
	effective := s.thresholdFixed
	if greater(percentage, effective) {
		effective = percentage
	}
	return s.thresholdFixed, percentage, effective, nil
}

func (s *TwoMarketCrossChainArbitrage) candidate(ctx context.Context, evaluation arbitrage.Evaluation, direction arbitrage.Direction, buySnapshot, sellSnapshot market.MarketSnapshot, buyBase, buyQuote, sellBase, sellQuote market.Token, size market.AssetQuantity, timing *DirectionTiming, refresh bool) (arbitrage.Candidate, error) {
	if s.sizingAsset == buyQuote.Asset {
		return s.quoteSizedCandidate(ctx, evaluation, direction, buySnapshot, sellSnapshot, buyBase, buyQuote, sellBase, sellQuote, size, timing, refresh, nil)
	}
	targetBase, err := size.ToTokenAmount(buyBase)
	if err != nil || targetBase.IsZero() {
		return arbitrage.Candidate{}, fmt.Errorf("size_rounds_to_zero")
	}
	actualSize, _ := targetBase.ToAssetQuantity(buyBase)
	initialHigh, _ := market.NewTokenAmount(buyQuote.ID, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(buyQuote.Decimals)), nil))
	buy, err := s.exactOutput(ctx, s.sources[direction.BuyMarket], sizing.ExactOutputRequest{
		Snapshot: buySnapshot, TokenIn: buyQuote.ID, TokenOut: buyBase.ID,
		TargetOut: targetBase, InitialHigh: initialHigh,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: evaluation.StartedAt(),
	}, timing, "buy")
	if err != nil {
		return arbitrage.Candidate{}, fmt.Errorf("buy_quote_failed: %w", err)
	}
	if hasUnmodeledFee(buy) {
		return arbitrage.Candidate{}, fmt.Errorf("buy_quote_has_unmodeled_fee")
	}
	actualInput, err := buy.AmountIn.ToAssetQuantity(buyQuote)
	if err != nil {
		return arbitrage.Candidate{}, fmt.Errorf("buy_input_invalid")
	}
	sellInput, err := actualSize.ToTokenAmount(sellBase)
	if err != nil || sellInput.IsZero() {
		return arbitrage.Candidate{}, fmt.Errorf("sell_input_rounds_to_zero")
	}
	sell, err := s.input(ctx, s.sources[direction.SellMarket], quoteport.Input{
		Snapshot: sellSnapshot, TokenIn: sellBase.ID, TokenOut: sellQuote.ID, AmountIn: sellInput,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: evaluation.StartedAt(),
	}, timing, "sell", refresh)
	if err != nil {
		return arbitrage.Candidate{}, fmt.Errorf("sell_quote_failed: %w", err)
	}
	if hasUnmodeledFee(sell) {
		return arbitrage.Candidate{}, fmt.Errorf("sell_quote_has_unmodeled_fee")
	}
	output, err := sell.AmountOut.ToAssetQuantity(sellQuote)
	if err != nil {
		return arbitrage.Candidate{}, fmt.Errorf("sell_output_invalid")
	}
	gross, _ := output.Sub(actualInput)
	net, _ := gross.Sub(evaluation.Cost().Amount)
	return arbitrage.Candidate{
		Size: actualSize, Input: actualInput, Output: output, GrossPnL: gross, Cost: evaluation.Cost(), NetPnL: net,
		BuyQuote: buy, SellQuote: sell,
	}, nil
}

func (s *TwoMarketCrossChainArbitrage) quoteSizedCandidate(ctx context.Context, evaluation arbitrage.Evaluation, direction arbitrage.Direction, buySnapshot, sellSnapshot market.MarketSnapshot, buyBase, buyQuote, sellBase, sellQuote market.Token, budget market.AssetQuantity, timing *DirectionTiming, refresh bool, trace *PinnedEvaluationTiming) (arbitrage.Candidate, error) {
	buyInput, err := budget.ToTokenAmount(buyQuote)
	if err != nil || buyInput.IsZero() {
		return arbitrage.Candidate{}, fmt.Errorf("size_rounds_to_zero")
	}
	if trace != nil {
		trace.BuyStartedAt = s.clock().UTC()
	}
	buy, err := s.input(ctx, s.sources[direction.BuyMarket], quoteport.Input{
		Snapshot: buySnapshot, TokenIn: buyQuote.ID, TokenOut: buyBase.ID, AmountIn: buyInput,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: evaluation.StartedAt(),
	}, timing, "buy", refresh)
	if trace != nil {
		trace.BuyFinishedAt = s.clock().UTC()
	}
	if err != nil {
		return arbitrage.Candidate{}, fmt.Errorf("buy_quote_failed: %w", err)
	}
	if hasUnmodeledFee(buy) {
		return arbitrage.Candidate{}, fmt.Errorf("buy_quote_has_unmodeled_fee")
	}
	baseReceived, err := buy.AmountOut.ToAssetQuantity(buyBase)
	if err != nil || baseReceived.Sign() <= 0 {
		return arbitrage.Candidate{}, fmt.Errorf("buy_output_invalid")
	}
	if trace != nil {
		trace.ConversionStartedAt = s.clock().UTC()
	}
	sellInput, err := baseReceived.ToTokenAmount(sellBase)
	if trace != nil {
		trace.ConversionFinishedAt = s.clock().UTC()
	}
	if err != nil || sellInput.IsZero() {
		return arbitrage.Candidate{}, fmt.Errorf("sell_input_rounds_to_zero")
	}
	if trace != nil {
		trace.SellStartedAt = s.clock().UTC()
	}
	sell, err := s.input(ctx, s.sources[direction.SellMarket], quoteport.Input{
		Snapshot: sellSnapshot, TokenIn: sellBase.ID, TokenOut: sellQuote.ID, AmountIn: sellInput,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: evaluation.StartedAt(),
	}, timing, "sell", refresh)
	if trace != nil {
		trace.SellFinishedAt = s.clock().UTC()
	}
	if err != nil {
		return arbitrage.Candidate{}, fmt.Errorf("sell_quote_failed: %w", err)
	}
	if hasUnmodeledFee(sell) {
		return arbitrage.Candidate{}, fmt.Errorf("sell_quote_has_unmodeled_fee")
	}
	output, err := sell.AmountOut.ToAssetQuantity(sellQuote)
	if err != nil {
		return arbitrage.Candidate{}, fmt.Errorf("sell_output_invalid")
	}
	if trace != nil {
		trace.PnLStartedAt = s.clock().UTC()
	}
	input, _ := buyInput.ToAssetQuantity(buyQuote)
	gross, _ := output.Sub(input)
	net, _ := gross.Sub(evaluation.Cost().Amount)
	if trace != nil {
		trace.PnLFinishedAt = s.clock().UTC()
	}
	return arbitrage.Candidate{
		Size: budget, Input: input, Output: output, GrossPnL: gross, Cost: evaluation.Cost(), NetPnL: net,
		BuyQuote: buy, SellQuote: sell,
	}, nil
}

func (s *TwoMarketCrossChainArbitrage) exactOutput(ctx context.Context, source quoteport.Source, request sizing.ExactOutputRequest, timing *DirectionTiming, leg string) (market.Quote, error) {
	started := s.clock()
	quote, cached, err := s.cache.getOrCompute(ctx, request.Snapshot, source, market.QuoteModeExactOutput, request.TokenIn, request.TokenOut, request.TargetOut, request.Purpose, request.QuotedAt, func() (market.Quote, error) {
		return sizing.MinimumInputForOutput(ctx, source, request)
	})
	trace := QuoteTiming{
		Market: request.Snapshot.Metadata().Market, Source: source.ID(), Leg: leg,
		Mode: market.QuoteModeExactOutput, Duration: nonNegative(s.clock().Sub(started)),
		Cached: cached, Error: quoteError(err), AmountOut: request.TargetOut,
	}
	s.addQuoteAmounts(&trace, quote, request.TokenIn, request.TokenOut)
	s.recordQuoteTiming(timing, source, trace)
	return quote, err
}

func (s *TwoMarketCrossChainArbitrage) input(ctx context.Context, source quoteport.Source, request quoteport.Input, timing *DirectionTiming, leg string, refresh bool) (market.Quote, error) {
	started := s.clock()
	var (
		quote  market.Quote
		cached bool
		err    error
	)
	if fresh, ok := source.(quoteport.FreshSource); refresh && ok {
		quote, err = fresh.QuoteFresh(ctx, request)
	} else {
		quote, cached, err = s.cache.getOrCompute(ctx, request.Snapshot, source, market.QuoteModeExactInput, request.TokenIn, request.TokenOut, request.AmountIn, request.Purpose, request.QuotedAt, func() (market.Quote, error) {
			return source.Quote(ctx, request)
		})
	}
	cached = cached || quote.Quality.RequiresRefresh()
	trace := QuoteTiming{
		Market: request.Snapshot.Metadata().Market, Source: source.ID(), Leg: leg,
		Mode: market.QuoteModeExactInput, Duration: nonNegative(s.clock().Sub(started)),
		Cached: cached, Error: quoteError(err), AmountIn: request.AmountIn,
	}
	s.addQuoteAmounts(&trace, quote, request.TokenIn, request.TokenOut)
	s.recordQuoteTiming(timing, source, trace)
	return quote, err
}

func (s *TwoMarketCrossChainArbitrage) addQuoteAmounts(trace *QuoteTiming, quoted market.Quote, inputToken, outputToken market.TokenID) {
	if quoted.AmountIn.Token() != "" {
		trace.AmountIn = quoted.AmountIn
	}
	if quoted.AmountOut.Token() != "" {
		trace.AmountOut = quoted.AmountOut
	}
	if token, ok := s.registry.Token(inputToken); ok && trace.AmountIn.Token() != "" {
		trace.Input, _ = trace.AmountIn.ToAssetQuantity(token)
	}
	if token, ok := s.registry.Token(outputToken); ok && trace.AmountOut.Token() != "" {
		trace.Output, _ = trace.AmountOut.ToAssetQuantity(token)
	}
}

func quoteError(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (s *TwoMarketCrossChainArbitrage) recordQuoteTiming(timing *DirectionTiming, source quoteport.Source, quote QuoteTiming) {
	if timing != nil {
		if traced, ok := source.(quoteport.TimingSource); ok {
			trace := traced.LastTiming()
			quote.Cached = quote.Cached || trace.Cached
			quote.Hops = append([]quoteport.HopTiming(nil), trace.Hops...)
		}
		timing.Quotes = append(timing.Quotes, quote)
	}
}

func hasUnmodeledFee(quote market.Quote) bool {
	for _, fee := range quote.Fees() {
		if !fee.IncludedInAmounts() {
			return true
		}
	}
	return false
}

func (s *TwoMarketCrossChainArbitrage) finish(opportunity arbitrage.Opportunity) arbitrage.Opportunity {
	opportunity.FinishedAt = s.clock().UTC()
	return opportunity
}

func greater(left, right market.AssetQuantity) bool {
	comparison, err := left.Cmp(right)
	return err == nil && comparison > 0
}

func greaterOrEqual(left, right market.AssetQuantity) bool {
	comparison, err := left.Cmp(right)
	return err == nil && comparison >= 0
}

func nonNegative(value time.Duration) time.Duration {
	if value < 0 {
		return 0
	}
	return value
}
