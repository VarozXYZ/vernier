package strategy

import (
	"context"
	"fmt"
	"math/big"
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
	Market   market.MarketID
	Leg      string
	Mode     market.QuoteMode
	Duration time.Duration
	Cached   bool
	Hops     []quoteport.HopTiming
}

// DirectionTiming keeps the sequential buy-then-sell timings together with
// the total time spent evaluating one direction.
type DirectionTiming struct {
	Direction arbitrage.Direction
	Duration  time.Duration
	Quotes    []QuoteTiming
}

// EvaluationTiming is the local Research hot-path trace. The quote order is
// the order in which the strategy evaluated it; sell quotes therefore follow
// their dependent buy quotes.
type EvaluationTiming struct {
	Duration   time.Duration
	Directions []DirectionTiming
}

type SizingAsset string

const (
	SizingAssetBase  SizingAsset = "base"
	SizingAssetQuote SizingAsset = "quote"
)

type TwoMarketConfig struct {
	ID          arbitrage.StrategyID
	Setup       arbitrage.ArbitrageSetup
	Registry    *market.Registry
	Sources     map[market.MarketID]quoteport.Source
	Grid        sizing.Grid
	Threshold   market.AssetQuantity
	Clock       Clock
	SizingAsset SizingAsset
}

type TwoMarketCrossChainArbitrage struct {
	id          arbitrage.StrategyID
	setup       arbitrage.ArbitrageSetup
	registry    *market.Registry
	sources     map[market.MarketID]quoteport.Source
	grid        sizing.Grid
	threshold   market.AssetQuantity
	clock       Clock
	cache       quoteCache
	sizingAsset market.AssetID
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
	if config.Threshold.Asset() != pair.QuoteAsset || config.Threshold.Sign() < 0 {
		return nil, fmt.Errorf("non-negative threshold must use quote asset %q", pair.QuoteAsset)
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
		grid: config.Grid, threshold: config.Threshold, clock: config.Clock, sizingAsset: sizingAsset,
		cache: newQuoteCache(),
	}, nil
}

func (s *TwoMarketCrossChainArbitrage) ID() arbitrage.StrategyID { return s.id }

func (s *TwoMarketCrossChainArbitrage) Evaluate(ctx context.Context, evaluation arbitrage.Evaluation) ([]arbitrage.Opportunity, error) {
	opportunities, _, err := s.EvaluateWithTiming(ctx, evaluation)
	return opportunities, err
}

// EvaluateWithTiming evaluates both directions using only the fixed
// snapshots in evaluation and returns the local timing trace alongside the
// economic results. External reference providers are intentionally absent
// from this method.
func (s *TwoMarketCrossChainArbitrage) EvaluateWithTiming(ctx context.Context, evaluation arbitrage.Evaluation) ([]arbitrage.Opportunity, EvaluationTiming, error) {
	if evaluation.Strategy() != s.id {
		return nil, EvaluationTiming{}, fmt.Errorf("evaluation targets strategy %q, expected %q", evaluation.Strategy(), s.id)
	}
	if evaluation.Cost().Amount.Asset() != s.threshold.Asset() {
		return nil, EvaluationTiming{}, fmt.Errorf("cost asset does not match strategy quote asset")
	}
	started := s.clock()
	opportunities := make([]arbitrage.Opportunity, 0, len(s.setup.Directions()))
	timing := EvaluationTiming{Directions: make([]DirectionTiming, 0, len(s.setup.Directions()))}
	for _, direction := range s.setup.Directions() {
		if err := ctx.Err(); err != nil {
			return nil, EvaluationTiming{}, err
		}
		directionStarted := s.clock()
		directionTiming := DirectionTiming{Direction: direction}
		opportunities = append(opportunities, s.evaluateDirection(ctx, evaluation, direction, &directionTiming))
		directionTiming.Duration = nonNegative(s.clock().Sub(directionStarted))
		timing.Directions = append(timing.Directions, directionTiming)
	}
	timing.Duration = nonNegative(s.clock().Sub(started))
	return opportunities, timing, nil
}

func (s *TwoMarketCrossChainArbitrage) evaluateDirection(ctx context.Context, evaluation arbitrage.Evaluation, direction arbitrage.Direction, timing *DirectionTiming) arbitrage.Opportunity {
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

	for _, size := range s.grid.Values() {
		candidate, err := s.candidate(ctx, evaluation, direction, buySnapshot, sellSnapshot, buyBase, buyQuote, sellBase, sellQuote, size, timing)
		if err != nil {
			opportunity.Reasons = append(opportunity.Reasons, err.Error())
			continue
		}
		opportunity.Candidates = append(opportunity.Candidates, candidate)
		if opportunity.SelectedIndex < 0 || greater(candidate.NetPnL, opportunity.Candidates[opportunity.SelectedIndex].NetPnL) {
			opportunity.SelectedIndex = len(opportunity.Candidates) - 1
		}
	}
	if opportunity.SelectedIndex < 0 {
		if len(opportunity.Reasons) == 0 {
			opportunity.Reasons = []string{"no_valid_size"}
		}
		return s.finish(opportunity)
	}

	selected := opportunity.Candidates[opportunity.SelectedIndex]
	switch {
	case selected.GrossPnL.Sign() <= 0:
		opportunity.Classification = arbitrage.ClassificationNoSpread
		opportunity.Reasons = []string{"no_positive_gross_profit"}
	case selected.NetPnL.Sign() <= 0:
		opportunity.Classification = arbitrage.ClassificationObservedSpread
		opportunity.Reasons = []string{"costs_exceed_gross_profit"}
	case !greaterOrEqual(selected.NetPnL, s.threshold):
		opportunity.Classification = arbitrage.ClassificationEconomic
		opportunity.Reasons = []string{"below_profit_threshold"}
	default:
		opportunity.Classification = arbitrage.ClassificationPolicyQualified
		opportunity.Reasons = []string{"profit_threshold_met"}
	}
	return s.finish(opportunity)
}

func (s *TwoMarketCrossChainArbitrage) candidate(ctx context.Context, evaluation arbitrage.Evaluation, direction arbitrage.Direction, buySnapshot, sellSnapshot market.MarketSnapshot, buyBase, buyQuote, sellBase, sellQuote market.Token, size market.AssetQuantity, timing *DirectionTiming) (arbitrage.Candidate, error) {
	if s.sizingAsset == buyQuote.Asset {
		return s.quoteSizedCandidate(ctx, evaluation, direction, buySnapshot, sellSnapshot, buyBase, buyQuote, sellBase, sellQuote, size, timing)
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
		return arbitrage.Candidate{}, fmt.Errorf("buy_quote_failed")
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
	}, timing, "sell")
	if err != nil {
		return arbitrage.Candidate{}, fmt.Errorf("sell_quote_failed")
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

func (s *TwoMarketCrossChainArbitrage) quoteSizedCandidate(ctx context.Context, evaluation arbitrage.Evaluation, direction arbitrage.Direction, buySnapshot, sellSnapshot market.MarketSnapshot, buyBase, buyQuote, sellBase, sellQuote market.Token, budget market.AssetQuantity, timing *DirectionTiming) (arbitrage.Candidate, error) {
	buyInput, err := budget.ToTokenAmount(buyQuote)
	if err != nil || buyInput.IsZero() {
		return arbitrage.Candidate{}, fmt.Errorf("size_rounds_to_zero")
	}
	buy, err := s.input(ctx, s.sources[direction.BuyMarket], quoteport.Input{
		Snapshot: buySnapshot, TokenIn: buyQuote.ID, TokenOut: buyBase.ID, AmountIn: buyInput,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: evaluation.StartedAt(),
	}, timing, "buy")
	if err != nil {
		return arbitrage.Candidate{}, fmt.Errorf("buy_quote_failed")
	}
	if hasUnmodeledFee(buy) {
		return arbitrage.Candidate{}, fmt.Errorf("buy_quote_has_unmodeled_fee")
	}
	baseReceived, err := buy.AmountOut.ToAssetQuantity(buyBase)
	if err != nil || baseReceived.Sign() <= 0 {
		return arbitrage.Candidate{}, fmt.Errorf("buy_output_invalid")
	}
	sellInput, err := baseReceived.ToTokenAmount(sellBase)
	if err != nil || sellInput.IsZero() {
		return arbitrage.Candidate{}, fmt.Errorf("sell_input_rounds_to_zero")
	}
	sell, err := s.input(ctx, s.sources[direction.SellMarket], quoteport.Input{
		Snapshot: sellSnapshot, TokenIn: sellBase.ID, TokenOut: sellQuote.ID, AmountIn: sellInput,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: evaluation.StartedAt(),
	}, timing, "sell")
	if err != nil {
		return arbitrage.Candidate{}, fmt.Errorf("sell_quote_failed")
	}
	if hasUnmodeledFee(sell) {
		return arbitrage.Candidate{}, fmt.Errorf("sell_quote_has_unmodeled_fee")
	}
	output, err := sell.AmountOut.ToAssetQuantity(sellQuote)
	if err != nil {
		return arbitrage.Candidate{}, fmt.Errorf("sell_output_invalid")
	}
	input, _ := buyInput.ToAssetQuantity(buyQuote)
	gross, _ := output.Sub(input)
	net, _ := gross.Sub(evaluation.Cost().Amount)
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
	s.recordQuoteTiming(timing, source, QuoteTiming{Market: request.Snapshot.Metadata().Market, Leg: leg, Mode: market.QuoteModeExactOutput, Duration: nonNegative(s.clock().Sub(started)), Cached: cached})
	return quote, err
}

func (s *TwoMarketCrossChainArbitrage) input(ctx context.Context, source quoteport.Source, request quoteport.Input, timing *DirectionTiming, leg string) (market.Quote, error) {
	started := s.clock()
	quote, cached, err := s.cache.getOrCompute(ctx, request.Snapshot, source, market.QuoteModeExactInput, request.TokenIn, request.TokenOut, request.AmountIn, request.Purpose, request.QuotedAt, func() (market.Quote, error) {
		return source.Quote(ctx, request)
	})
	s.recordQuoteTiming(timing, source, QuoteTiming{Market: request.Snapshot.Metadata().Market, Leg: leg, Mode: market.QuoteModeExactInput, Duration: nonNegative(s.clock().Sub(started)), Cached: cached})
	return quote, err
}

func (s *TwoMarketCrossChainArbitrage) recordQuoteTiming(timing *DirectionTiming, source quoteport.Source, quote QuoteTiming) {
	if timing != nil {
		if traced, ok := source.(quoteport.TimingSource); ok {
			trace := traced.LastTiming()
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
