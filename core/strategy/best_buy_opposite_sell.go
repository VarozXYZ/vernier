package strategy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

// BestBuyOppositeSell evaluates both complete cross-market directions in
// parallel. Within each direction, the sell starts as soon as its buy returns
// and uses the actual bought amount, converted to the opposite chain's token
// precision. The direction with the greatest final quote output is selected.
//
// The type name is retained for configuration compatibility with existing
// Research setups.
type BestBuyOppositeSell struct {
	id         arbitrage.StrategyID
	setup      arbitrage.ArbitrageSetup
	registry   *market.Registry
	sources    map[market.MarketID]quoteport.Source
	notional   market.AssetQuantity
	threshold  market.AssetQuantity
	clock      Clock
	retries    int
	retryDelay time.Duration
}

type BestBuyOppositeSellConfig struct {
	ID         arbitrage.StrategyID
	Setup      arbitrage.ArbitrageSetup
	Registry   *market.Registry
	Sources    map[market.MarketID]quoteport.Source
	Notional   market.AssetQuantity
	Threshold  market.AssetQuantity
	Clock      Clock
	Retries    int
	RetryDelay time.Duration
}

func NewBestBuyOppositeSell(config BestBuyOppositeSellConfig) (*BestBuyOppositeSell, error) {
	if config.ID == "" || config.Registry == nil || config.Clock == nil || len(config.Setup.Markets()) != 2 {
		return nil, fmt.Errorf("best-buy strategy requires id, two-market setup, registry, and clock")
	}
	pair, ok := config.Registry.Pair(config.Setup.Pair())
	if !ok || config.Notional.Asset() != pair.QuoteAsset || config.Notional.Sign() <= 0 ||
		config.Threshold.Asset() != pair.QuoteAsset || config.Threshold.Sign() < 0 {
		return nil, fmt.Errorf("best-buy notional and threshold must use the positive quote asset")
	}
	if config.Retries < 0 || config.Retries > 1 {
		return nil, fmt.Errorf("best-buy strategy supports at most one retry")
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = 100 * time.Millisecond
	}
	sources := make(map[market.MarketID]quoteport.Source, 2)
	for _, id := range config.Setup.Markets() {
		source := config.Sources[id]
		if source == nil {
			return nil, fmt.Errorf("quote source is required for market %q", id)
		}
		sources[id] = source
	}
	return &BestBuyOppositeSell{
		id: config.ID, setup: config.Setup, registry: config.Registry, sources: sources,
		notional: config.Notional, threshold: config.Threshold, clock: config.Clock,
		retries: config.Retries, retryDelay: config.RetryDelay,
	}, nil
}

func (s *BestBuyOppositeSell) ID() arbitrage.StrategyID { return s.id }

func (s *BestBuyOppositeSell) Evaluate(ctx context.Context, evaluation arbitrage.Evaluation) ([]arbitrage.Opportunity, error) {
	result, _, err := s.EvaluateWithTiming(ctx, evaluation)
	return result, err
}

type roundTripResult struct {
	opportunity arbitrage.Opportunity
	timing      DirectionTiming
}

func (s *BestBuyOppositeSell) EvaluateWithTiming(ctx context.Context, evaluation arbitrage.Evaluation) ([]arbitrage.Opportunity, EvaluationTiming, error) {
	if evaluation.Strategy() != s.id {
		return nil, EvaluationTiming{}, fmt.Errorf("evaluation targets strategy %q, expected %q", evaluation.Strategy(), s.id)
	}
	for _, direction := range s.setup.Directions() {
		if evaluation.CostFor(direction).Amount.Asset() != s.threshold.Asset() {
			return nil, EvaluationTiming{}, fmt.Errorf("cost asset does not match strategy quote asset")
		}
	}

	started := s.clock()
	directions := s.setup.Directions()
	results := make([]roundTripResult, len(directions))
	var wg sync.WaitGroup
	for index, direction := range directions {
		index, direction := index, direction
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[index] = s.evaluateDirection(ctx, evaluation, direction)
		}()
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, EvaluationTiming{}, err
	}

	opportunities := make([]arbitrage.Opportunity, len(results))
	timing := EvaluationTiming{Directions: make([]DirectionTiming, len(results))}
	for index := range results {
		opportunities[index] = results[index].opportunity
		timing.Directions[index] = results[index].timing
	}
	timing.Duration = nonNegative(s.clock().Sub(started))

	// Both complete paths are required to prove which direction has the best
	// output. A partial comparison is intentionally unclassifiable.
	for _, opportunity := range opportunities {
		if opportunity.SelectedIndex < 0 || opportunity.SelectedIndex >= len(opportunity.Candidates) {
			for index := range opportunities {
				if opportunities[index].SelectedIndex >= 0 {
					opportunities[index].SelectedIndex = -1
					opportunities[index].Classification = arbitrage.ClassificationUnclassifiable
					opportunities[index].Reasons = []string{"round_trip_comparison_incomplete"}
					opportunities[index] = s.finish(opportunities[index])
				}
			}
			return opportunities, timing, nil
		}
	}

	left := opportunities[0].Candidates[opportunities[0].SelectedIndex]
	right := opportunities[1].Candidates[opportunities[1].SelectedIndex]
	comparison, err := left.Output.Cmp(right.Output)
	if err != nil {
		for index := range opportunities {
			opportunities[index].SelectedIndex = -1
			opportunities[index].Classification = arbitrage.ClassificationUnclassifiable
			opportunities[index].Reasons = []string{"round_trip_outputs_incomparable"}
			opportunities[index] = s.finish(opportunities[index])
		}
		return opportunities, timing, nil
	}
	if comparison == 0 {
		for index := range opportunities {
			opportunities[index].SelectedIndex = -1
			opportunities[index].Classification = arbitrage.ClassificationNoSpread
			opportunities[index].Reasons = []string{"equal_round_trip_output"}
			opportunities[index] = s.finish(opportunities[index])
		}
		return opportunities, timing, nil
	}

	loser := 1
	if comparison < 0 {
		loser = 0
	}
	opportunities[loser].SelectedIndex = -1
	opportunities[loser].Classification = arbitrage.ClassificationNoSpread
	opportunities[loser].Reasons = []string{"lower_round_trip_output"}
	opportunities[loser] = s.finish(opportunities[loser])
	return opportunities, timing, nil
}

func (s *BestBuyOppositeSell) evaluateDirection(
	ctx context.Context,
	evaluation arbitrage.Evaluation,
	direction arbitrage.Direction,
) roundTripResult {
	started := s.clock()
	opportunity := s.emptyOpportunity(evaluation, direction)
	timing := DirectionTiming{Direction: direction}
	finish := func(reason string) roundTripResult {
		if reason != "" {
			opportunity.Reasons = []string{reason}
		}
		opportunity = s.finish(opportunity)
		timing.Duration = nonNegative(s.clock().Sub(started))
		return roundTripResult{opportunity: opportunity, timing: timing}
	}

	buyMarket, buyMarketOK := s.registry.Market(direction.BuyMarket)
	sellMarket, sellMarketOK := s.registry.Market(direction.SellMarket)
	if !buyMarketOK || !sellMarketOK {
		return finish("market_definition_missing")
	}
	buyBase, buyBaseOK := s.registry.Token(buyMarket.BaseToken)
	buyQuoteToken, buyQuoteOK := s.registry.Token(buyMarket.QuoteToken)
	sellBase, sellBaseOK := s.registry.Token(sellMarket.BaseToken)
	sellQuoteToken, sellQuoteOK := s.registry.Token(sellMarket.QuoteToken)
	if !buyBaseOK || !buyQuoteOK || !sellBaseOK || !sellQuoteOK {
		return finish("token_definition_missing")
	}
	buySnapshot, buySnapshotOK := evaluation.Snapshot(direction.BuyMarket)
	sellSnapshot, sellSnapshotOK := evaluation.Snapshot(direction.SellMarket)
	if !buySnapshotOK || !sellSnapshotOK ||
		buySnapshot.Metadata().Health != market.HealthHealthy ||
		sellSnapshot.Metadata().Health != market.HealthHealthy {
		return finish("degraded_or_missing_market_snapshot")
	}

	buyInput, err := s.notional.ToTokenAmount(buyQuoteToken)
	if err != nil || buyInput.IsZero() {
		return finish("buy_input_rounds_to_zero")
	}
	buyResult, buyTrace, buyErr := s.timedQuote(
		ctx, evaluation, direction.BuyMarket, "buy",
		buyQuoteToken, buyBase, buyInput, buySnapshot,
	)
	timing.Quotes = append(timing.Quotes, buyTrace)
	if buyErr != nil {
		return finish("buy_quote_unavailable")
	}
	bought, err := buyResult.AmountOut.ToAssetQuantity(buyBase)
	if err != nil {
		return finish("buy_output_invalid")
	}

	// The second chain receives the exact economic output of the buy, floored
	// only once when converting to that chain's base-token precision.
	sellInput, err := bought.ToTokenAmount(sellBase)
	if err != nil || sellInput.IsZero() {
		return finish("sell_input_rounds_to_zero")
	}
	sellResult, sellTrace, sellErr := s.timedQuote(
		ctx, evaluation, direction.SellMarket, "sell",
		sellBase, sellQuoteToken, sellInput, sellSnapshot,
	)
	timing.Quotes = append(timing.Quotes, sellTrace)
	if sellErr != nil {
		return finish("sell_quote_unavailable")
	}
	output, err := sellResult.AmountOut.ToAssetQuantity(sellQuoteToken)
	if err != nil {
		return finish("sell_output_invalid")
	}

	cost := evaluation.CostFor(direction)
	gross, _ := output.Sub(s.notional)
	net, _ := gross.Sub(cost.Amount)
	opportunity.Candidates = []arbitrage.Candidate{{
		Size: s.notional, Input: s.notional, Output: output, GrossPnL: gross,
		Cost: cost, NetPnL: net, BuyQuote: buyResult, SellQuote: sellResult,
	}}
	opportunity.SelectedIndex = 0
	switch {
	case gross.Sign() <= 0:
		opportunity.Classification = arbitrage.ClassificationNoSpread
		opportunity.Reasons = []string{"no_positive_gross_profit"}
	case net.Sign() <= 0:
		opportunity.Classification = arbitrage.ClassificationObservedSpread
		opportunity.Reasons = []string{"costs_exceed_gross_profit"}
	case !greaterOrEqual(net, s.threshold):
		opportunity.Classification = arbitrage.ClassificationEconomic
		opportunity.Reasons = []string{"below_profit_threshold"}
	default:
		opportunity.Classification = arbitrage.ClassificationPolicyQualified
		opportunity.Reasons = []string{"profit_threshold_met"}
	}
	return finish("")
}

func (s *BestBuyOppositeSell) timedQuote(
	ctx context.Context,
	evaluation arbitrage.Evaluation,
	marketID market.MarketID,
	leg string,
	tokenIn market.Token,
	tokenOut market.Token,
	amountIn market.TokenAmount,
	snapshot market.MarketSnapshot,
) (market.Quote, QuoteTiming, error) {
	source := s.sources[marketID]
	request := quoteport.Input{
		Snapshot: snapshot, TokenIn: tokenIn.ID, TokenOut: tokenOut.ID, AmountIn: amountIn,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: evaluation.StartedAt(),
	}
	started := s.clock()
	result, err := s.quote(ctx, source, request)
	input, _ := amountIn.ToAssetQuantity(tokenIn)
	trace := QuoteTiming{
		Market: marketID, Source: source.ID(), Leg: leg, Mode: market.QuoteModeExactInput,
		Duration: nonNegative(s.clock().Sub(started)), Error: quoteError(err),
		AmountIn: amountIn, Input: input,
	}
	if err == nil {
		trace.AmountIn = result.AmountIn
		trace.AmountOut = result.AmountOut
		trace.Input, _ = result.AmountIn.ToAssetQuantity(tokenIn)
		trace.Output, _ = result.AmountOut.ToAssetQuantity(tokenOut)
	}
	if traced, ok := source.(quoteport.TimingSource); ok {
		sourceTiming := traced.LastTiming()
		trace.Cached = sourceTiming.Cached
		trace.Hops = append([]quoteport.HopTiming(nil), sourceTiming.Hops...)
	}
	return result, trace, err
}

func (s *BestBuyOppositeSell) emptyOpportunity(evaluation arbitrage.Evaluation, direction arbitrage.Direction) arbitrage.Opportunity {
	opportunity := arbitrage.Opportunity{
		Evaluation: evaluation.ID(), Run: evaluation.Run(), ConfigHash: evaluation.ConfigHash(),
		Strategy: s.id, Direction: direction, Classification: arbitrage.ClassificationUnclassifiable,
		SelectedIndex: -1, Threshold: s.threshold, TriggeredAt: evaluation.TriggeredAt(),
		StartedAt: evaluation.StartedAt(),
	}
	opportunity.Trigger, opportunity.HasTrigger = evaluation.Trigger()
	for _, marketID := range []market.MarketID{direction.BuyMarket, direction.SellMarket} {
		if snapshot, ok := evaluation.Snapshot(marketID); ok {
			opportunity.Snapshots = append(opportunity.Snapshots, snapshot.Metadata())
		}
	}
	return opportunity
}

func (s *BestBuyOppositeSell) quote(ctx context.Context, source quoteport.Source, input quoteport.Input) (market.Quote, error) {
	var last error
	for attempt := 0; attempt <= s.retries; attempt++ {
		result, err := source.Quote(ctx, input)
		if err == nil {
			return result, nil
		}
		last = err
		if ctx.Err() != nil || attempt == s.retries || !transientQuoteError(err) {
			break
		}
		delay := s.retryDelay
		var hinted interface{ RetryAfter() time.Duration }
		if errors.As(err, &hinted) && hinted.RetryAfter() > 0 {
			delay = hinted.RetryAfter()
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return market.Quote{}, ctx.Err()
		case <-timer.C:
		}
	}
	return market.Quote{}, last
}

func transientQuoteError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var status interface{ HTTPStatusCode() int }
	if !errors.As(err, &status) {
		return true
	}
	code := status.HTTPStatusCode()
	return code == 408 || code == 425 || code == 429 || code >= 500
}

func (s *BestBuyOppositeSell) finish(opportunity arbitrage.Opportunity) arbitrage.Opportunity {
	opportunity.FinishedAt = s.clock().UTC()
	return opportunity
}

var _ Strategy = (*BestBuyOppositeSell)(nil)
