package strategy

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type LiveConfig struct {
	Setup    arbitrage.ArbitrageSetup
	Registry *market.Registry
	Sources  map[market.MarketID]quoteport.Source
	Clock    Clock
}

type LiveEvaluator struct {
	setup    arbitrage.ArbitrageSetup
	registry *market.Registry
	sources  map[market.MarketID]quoteport.Source
	clock    Clock
}

type LiveEvaluationRequest struct {
	ID                  string
	Direction           arbitrage.Direction
	Snapshots           map[market.MarketID]market.MarketSnapshot
	Notional            market.AssetQuantity
	Valuation           arbitrage.ValuationSnapshot
	Cost                market.AssetQuantity
	Threshold           market.AssetQuantity
	MaximumCost         market.AssetQuantity
	MaximumBaseExposure market.AssetQuantity
	TriggeredAt         time.Time
	QuoteConversion     *market.QuoteConversionSnapshot
}

func NewLiveEvaluator(config LiveConfig) (*LiveEvaluator, error) {
	if config.Setup.ID() == "" || config.Registry == nil || config.Clock == nil {
		return nil, fmt.Errorf("live evaluator requires setup, registry, and clock")
	}
	sources := make(map[market.MarketID]quoteport.Source, len(config.Setup.Markets()))
	for _, marketID := range config.Setup.Markets() {
		source := config.Sources[marketID]
		if source == nil {
			return nil, fmt.Errorf("live evaluator requires source for market %q", marketID)
		}
		sources[marketID] = source
	}
	return &LiveEvaluator{setup: config.Setup, registry: config.Registry, sources: sources, clock: config.Clock}, nil
}

// Evaluate fixes both inputs before starting either quote and starts buy and
// sell concurrently.
func (e *LiveEvaluator) Evaluate(ctx context.Context, request LiveEvaluationRequest) (arbitrage.LiveOpportunity, error) {
	buyInput, sellInput, tokens, snapshots, err := e.inputs(request)
	if err != nil {
		return arbitrage.LiveOpportunity{}, err
	}
	quotedAt := e.clock().UTC()
	type result struct {
		quote market.Quote
		err   error
	}
	buyResult := make(chan result, 1)
	sellResult := make(chan result, 1)
	var group sync.WaitGroup
	group.Add(2)
	go func() {
		defer group.Done()
		quote, quoteErr := e.sources[request.Direction.BuyMarket].Quote(ctx, quoteport.Input{
			Snapshot: snapshots.buy, TokenIn: tokens.buyQuote.ID, TokenOut: tokens.buyBase.ID,
			AmountIn: buyInput, Purpose: market.QuotePurposeLiveDiscovery, QuotedAt: quotedAt,
		})
		buyResult <- result{quote: quote, err: quoteErr}
	}()
	go func() {
		defer group.Done()
		quote, quoteErr := e.sources[request.Direction.SellMarket].Quote(ctx, quoteport.Input{
			Snapshot: snapshots.sell, TokenIn: tokens.sellBase.ID, TokenOut: tokens.sellQuote.ID,
			AmountIn: sellInput, Purpose: market.QuotePurposeLiveDiscovery, QuotedAt: quotedAt,
		})
		sellResult <- result{quote: quote, err: quoteErr}
	}()
	group.Wait()
	close(buyResult)
	close(sellResult)
	buy, sell := <-buyResult, <-sellResult
	if buy.err != nil {
		return arbitrage.LiveOpportunity{}, fmt.Errorf("live buy discovery: %w", buy.err)
	}
	if sell.err != nil {
		return arbitrage.LiveOpportunity{}, fmt.Errorf("live sell discovery: %w", sell.err)
	}
	return e.Value(request, buy.quote, sell.quote, time.Time{})
}

// Value recomputes economic deltas from discovery or validation quotes
// without changing either fixed input.
func (e *LiveEvaluator) Value(request LiveEvaluationRequest, buy, sell market.Quote, validatedAt time.Time) (arbitrage.LiveOpportunity, error) {
	buyInput, sellInput, tokens, _, err := e.inputs(request)
	if err != nil {
		return arbitrage.LiveOpportunity{}, err
	}
	if buy.AmountIn.Token() != buyInput.Token() || buy.AmountIn.Units().Cmp(buyInput.Units()) != 0 ||
		sell.AmountIn.Token() != sellInput.Token() || sell.AmountIn.Units().Cmp(sellInput.Units()) != 0 {
		return arbitrage.LiveOpportunity{}, fmt.Errorf("validated quotes changed independently fixed inputs")
	}
	buyQuoteInput, err := buy.AmountIn.ToAssetQuantity(tokens.buyQuote)
	if err != nil {
		return arbitrage.LiveOpportunity{}, err
	}
	buyBaseOutput, err := buy.AmountOut.ToAssetQuantity(tokens.buyBase)
	if err != nil {
		return arbitrage.LiveOpportunity{}, err
	}
	sellBaseInput, err := sell.AmountIn.ToAssetQuantity(tokens.sellBase)
	if err != nil {
		return arbitrage.LiveOpportunity{}, err
	}
	sellQuoteOutput, err := sell.AmountOut.ToAssetQuantity(tokens.sellQuote)
	if err != nil {
		return arbitrage.LiveOpportunity{}, err
	}
	if request.QuoteConversion != nil {
		conversion := request.QuoteConversion
		if !conversion.ValidAt(e.clock().UTC()) ||
			conversion.Input.Token() != tokens.sellQuote.ID ||
			conversion.Output.Token() != tokens.buyQuote.ID {
			return arbitrage.LiveOpportunity{}, fmt.Errorf("live quote conversion is unavailable")
		}
		conversionInput, conversionErr := conversion.Input.ToAssetQuantity(tokens.sellQuote)
		if conversionErr != nil {
			return arbitrage.LiveOpportunity{}, conversionErr
		}
		conversionOutput, conversionErr := conversion.Output.ToAssetQuantity(tokens.buyQuote)
		if conversionErr != nil {
			return arbitrage.LiveOpportunity{}, conversionErr
		}
		rate := new(big.Rat).Quo(conversionOutput.Rat(), conversionInput.Rat())
		sellQuoteOutput, err = market.NewAssetQuantity(tokens.buyQuote.Asset,
			new(big.Rat).Mul(sellQuoteOutput.Rat(), rate))
		if err != nil {
			return arbitrage.LiveOpportunity{}, err
		}
	}
	quoteDelta, err := sellQuoteOutput.Sub(buyQuoteInput)
	if err != nil {
		return arbitrage.LiveOpportunity{}, err
	}
	baseDelta, err := buyBaseOutput.Sub(sellBaseInput)
	if err != nil {
		return arbitrage.LiveOpportunity{}, err
	}
	markedBase, err := market.NewAssetQuantity(request.Valuation.Quote(), new(big.Rat).Mul(baseDelta.Rat(), request.Valuation.Price()))
	if err != nil {
		return arbitrage.LiveOpportunity{}, err
	}
	gross, err := quoteDelta.Add(markedBase)
	if err != nil {
		return arbitrage.LiveOpportunity{}, err
	}
	net, err := gross.Sub(request.Cost)
	if err != nil {
		return arbitrage.LiveOpportunity{}, err
	}
	riskBlock := ""
	if comparison, compareErr := request.Cost.Cmp(request.MaximumCost); compareErr != nil {
		return arbitrage.LiveOpportunity{}, compareErr
	} else if comparison > 0 {
		riskBlock = "execution_cost_limit"
	}
	absoluteBaseDelta := new(big.Rat).Abs(baseDelta.Rat())
	if absoluteBaseDelta.Cmp(request.MaximumBaseExposure.Rat()) > 0 {
		riskBlock = "base_exposure_limit"
	}
	opportunity := arbitrage.LiveOpportunity{
		ID: request.ID, Setup: e.setup.ID(), Direction: request.Direction, Valuation: request.Valuation,
		BuyQuote: buy, SellQuote: sell, QuoteDelta: quoteDelta, BaseDelta: baseDelta,
		MarkedBase: markedBase, GrossPnL: gross, Cost: request.Cost, NetPnL: net,
		Threshold: request.Threshold, RiskBlock: riskBlock,
		DiscoveredAt: request.TriggeredAt.UTC(), ValidatedAt: validatedAt.UTC(),
	}
	if err := opportunity.Validate(); err != nil {
		return arbitrage.LiveOpportunity{}, err
	}
	return opportunity, nil
}

type liveTokens struct {
	buyBase, buyQuote, sellBase, sellQuote market.Token
}

type liveSnapshots struct {
	buy, sell market.MarketSnapshot
}

func (e *LiveEvaluator) inputs(request LiveEvaluationRequest) (market.TokenAmount, market.TokenAmount, liveTokens, liveSnapshots, error) {
	if request.ID == "" || request.Direction.BuyMarket == "" || request.Direction.SellMarket == "" ||
		request.Direction.BuyMarket == request.Direction.SellMarket || request.TriggeredAt.IsZero() {
		return market.TokenAmount{}, market.TokenAmount{}, liveTokens{}, liveSnapshots{}, fmt.Errorf("live evaluation identity, direction, and trigger are required")
	}
	buyMarket, buyOK := e.registry.Market(request.Direction.BuyMarket)
	sellMarket, sellOK := e.registry.Market(request.Direction.SellMarket)
	if !buyOK || !sellOK {
		return market.TokenAmount{}, market.TokenAmount{}, liveTokens{}, liveSnapshots{}, fmt.Errorf("live evaluation references unknown market")
	}
	tokens := liveTokens{}
	tokens.buyBase, buyOK = e.registry.Token(buyMarket.BaseToken)
	tokens.buyQuote, sellOK = e.registry.Token(buyMarket.QuoteToken)
	if !buyOK || !sellOK {
		return market.TokenAmount{}, market.TokenAmount{}, liveTokens{}, liveSnapshots{}, fmt.Errorf("buy market tokens are unavailable")
	}
	tokens.sellBase, buyOK = e.registry.Token(sellMarket.BaseToken)
	tokens.sellQuote, sellOK = e.registry.Token(sellMarket.QuoteToken)
	if !buyOK || !sellOK {
		return market.TokenAmount{}, market.TokenAmount{}, liveTokens{}, liveSnapshots{}, fmt.Errorf("sell market tokens are unavailable")
	}
	if request.Notional.Asset() != tokens.buyQuote.Asset || request.Cost.Asset() != tokens.buyQuote.Asset ||
		request.Threshold.Asset() != tokens.buyQuote.Asset || request.Valuation.Base() != tokens.buyBase.Asset ||
		request.Valuation.Quote() != tokens.buyQuote.Asset ||
		request.MaximumCost.Asset() != tokens.buyQuote.Asset ||
		request.MaximumBaseExposure.Asset() != tokens.buyBase.Asset ||
		request.MaximumCost.Sign() <= 0 || request.MaximumBaseExposure.Sign() <= 0 {
		return market.TokenAmount{}, market.TokenAmount{}, liveTokens{}, liveSnapshots{}, fmt.Errorf("live evaluation economic assets are inconsistent")
	}
	buyInput, err := request.Notional.ToTokenAmount(tokens.buyQuote)
	if err != nil || buyInput.IsZero() {
		return market.TokenAmount{}, market.TokenAmount{}, liveTokens{}, liveSnapshots{}, fmt.Errorf("live buy input is invalid")
	}
	sellQuantity, err := market.NewAssetQuantity(tokens.sellBase.Asset, new(big.Rat).Quo(request.Notional.Rat(), request.Valuation.Price()))
	if err != nil {
		return market.TokenAmount{}, market.TokenAmount{}, liveTokens{}, liveSnapshots{}, err
	}
	sellInput, err := sellQuantity.ToTokenAmount(tokens.sellBase)
	if err != nil || sellInput.IsZero() {
		return market.TokenAmount{}, market.TokenAmount{}, liveTokens{}, liveSnapshots{}, fmt.Errorf("live sell input is invalid")
	}
	snapshots := liveSnapshots{buy: request.Snapshots[request.Direction.BuyMarket], sell: request.Snapshots[request.Direction.SellMarket]}
	for _, snapshot := range []market.MarketSnapshot{snapshots.buy, snapshots.sell} {
		if snapshot.Metadata().Market == "" || snapshot.Metadata().Health != market.HealthHealthy {
			return market.TokenAmount{}, market.TokenAmount{}, liveTokens{}, liveSnapshots{}, fmt.Errorf("live evaluation requires healthy fixed snapshots")
		}
	}
	return buyInput, sellInput, tokens, snapshots, nil
}
