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

type TwoMarketConfig struct {
	ID        arbitrage.StrategyID
	Setup     arbitrage.ArbitrageSetup
	Registry  *market.Registry
	Sources   map[market.MarketID]quoteport.Source
	Grid      sizing.Grid
	Threshold market.AssetQuantity
	Clock     Clock
}

type TwoMarketCrossChainArbitrage struct {
	id        arbitrage.StrategyID
	setup     arbitrage.ArbitrageSetup
	registry  *market.Registry
	sources   map[market.MarketID]quoteport.Source
	grid      sizing.Grid
	threshold market.AssetQuantity
	clock     Clock
	cache     quoteCache
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
	if config.Grid.Asset() != pair.BaseAsset {
		return nil, fmt.Errorf("sizing grid must use base asset %q", pair.BaseAsset)
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
		grid: config.Grid, threshold: config.Threshold, clock: config.Clock,
		cache: newQuoteCache(),
	}, nil
}

func (s *TwoMarketCrossChainArbitrage) ID() arbitrage.StrategyID { return s.id }

func (s *TwoMarketCrossChainArbitrage) Evaluate(ctx context.Context, evaluation arbitrage.Evaluation) ([]arbitrage.Opportunity, error) {
	if evaluation.Strategy() != s.id {
		return nil, fmt.Errorf("evaluation targets strategy %q, expected %q", evaluation.Strategy(), s.id)
	}
	if evaluation.Cost().Amount.Asset() != s.threshold.Asset() {
		return nil, fmt.Errorf("cost asset does not match strategy quote asset")
	}
	opportunities := make([]arbitrage.Opportunity, 0, len(s.setup.Directions()))
	for _, direction := range s.setup.Directions() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		opportunities = append(opportunities, s.evaluateDirection(ctx, evaluation, direction))
	}
	return opportunities, nil
}

func (s *TwoMarketCrossChainArbitrage) evaluateDirection(ctx context.Context, evaluation arbitrage.Evaluation, direction arbitrage.Direction) arbitrage.Opportunity {
	opportunity := arbitrage.Opportunity{
		Evaluation: evaluation.ID(), Run: evaluation.Run(), ConfigHash: evaluation.ConfigHash(),
		Strategy: s.id, Direction: direction,
		Classification: arbitrage.ClassificationUnclassifiable, SelectedIndex: -1,
		Threshold: s.threshold, TriggeredAt: evaluation.TriggeredAt(), StartedAt: evaluation.StartedAt(),
	}
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
		candidate, err := s.candidate(ctx, evaluation, direction, buySnapshot, sellSnapshot, buyBase, buyQuote, sellBase, sellQuote, size)
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

func (s *TwoMarketCrossChainArbitrage) candidate(ctx context.Context, evaluation arbitrage.Evaluation, direction arbitrage.Direction, buySnapshot, sellSnapshot market.MarketSnapshot, buyBase, buyQuote, sellBase, sellQuote market.Token, size market.AssetQuantity) (arbitrage.Candidate, error) {
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
	})
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
	})
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

func (s *TwoMarketCrossChainArbitrage) exactOutput(ctx context.Context, source quoteport.Source, request sizing.ExactOutputRequest) (market.Quote, error) {
	return s.cache.getOrCompute(ctx, request.Snapshot, source, market.QuoteModeExactOutput, request.TokenIn, request.TokenOut, request.TargetOut, request.Purpose, request.QuotedAt, func() (market.Quote, error) {
		return sizing.MinimumInputForOutput(ctx, source, request)
	})
}

func (s *TwoMarketCrossChainArbitrage) input(ctx context.Context, source quoteport.Source, request quoteport.Input) (market.Quote, error) {
	return s.cache.getOrCompute(ctx, request.Snapshot, source, market.QuoteModeExactInput, request.TokenIn, request.TokenOut, request.AmountIn, request.Purpose, request.QuotedAt, func() (market.Quote, error) {
		return source.Quote(ctx, request)
	})
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
