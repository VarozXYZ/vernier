package livecompare

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/adapters/feed/evmlogs"
	"github.com/VarozXYZ/vernier/adapters/feed/sourceorder"
	"github.com/VarozXYZ/vernier/adapters/market/aerodromeslipstream"
	"github.com/VarozXYZ/vernier/adapters/market/constantproduct"
	"github.com/VarozXYZ/vernier/adapters/market/uniswapv2"
	"github.com/VarozXYZ/vernier/adapters/market/uniswapv3"
	"github.com/VarozXYZ/vernier/adapters/price/chainlink"
	"github.com/VarozXYZ/vernier/adapters/price/coingecko"
	"github.com/VarozXYZ/vernier/core/costing"
	"github.com/VarozXYZ/vernier/core/marketstate"
	"github.com/VarozXYZ/vernier/core/sizing"
	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	priceport "github.com/VarozXYZ/vernier/ports/price"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
	runtimeresearch "github.com/VarozXYZ/vernier/runtime/research"
)

var ErrParityMismatch = errors.New("local quote differs from venue reference")

type Networks map[string]evm.Network

type Options struct {
	Clock       func() time.Time
	LookupEnv   LookupEnv
	PriceClient coingecko.Client
}

type Runner struct {
	config   ParsedConfig
	networks Networks
	clock    func() time.Time
	lookup   LookupEnv
	client   coingecko.Client
}

type CostEvidence struct {
	FixedAmount *big.Rat
	FixedAsset  market.AssetID
	Cost        market.AssetQuantity
	Price       market.PriceObservation
}

type ParityEvidence struct {
	Market       market.MarketID
	Leg          string
	AmountIn     market.TokenAmount
	LocalOut     market.TokenAmount
	ReferenceOut *big.Int
	Matches      bool
}

type Report struct {
	Research runtimeresearch.Report
	Cost     CostEvidence
	Parity   []ParityEvidence
}

type marketRuntime struct {
	config    ResolvedMarket
	snapshot  market.MarketSnapshot
	source    quoteport.Source
	reference func(context.Context, market.TokenID, market.TokenID, *big.Int) (*big.Int, error)
}

func New(config ParsedConfig, networks Networks, options Options) (*Runner, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.LookupEnv == nil {
		options.LookupEnv = func(string) (string, bool) { return "", false }
	}
	if options.PriceClient == nil {
		options.PriceClient = http.DefaultClient
	}
	limited := make(Networks, len(config.Chains))
	for id, profile := range config.Chains {
		network := networks[id]
		if network == nil || network.ID() != id {
			return nil, fmt.Errorf("configured EVM network %q is required", id)
		}
		wrapped, err := evm.NewRateLimitedNetwork(network, profile.RPCMinInterval)
		if err != nil {
			return nil, err
		}
		limited[id] = wrapped
	}
	return &Runner{config: config, networks: limited, clock: options.Clock, lookup: options.LookupEnv, client: options.PriceClient}, nil
}

func (r *Runner) Run(ctx context.Context) (Report, error) {
	startedAt := r.clock().UTC()
	blocks := make(map[string]evm.BlockReference, len(r.networks))
	for id, network := range r.networks {
		block, err := network.CurrentBlock(ctx)
		if err != nil {
			return Report{}, err
		}
		blocks[id] = block
	}
	registry, setup, err := r.registry()
	if err != nil {
		return Report{}, err
	}
	minimum, _ := market.NewAssetQuantity(r.config.Markets[0].Base.Token.Asset, r.config.MinimumSize)
	maximum, _ := market.NewAssetQuantity(r.config.Markets[0].Base.Token.Asset, r.config.MaximumSize)
	grid, err := sizing.NewLinearRange(minimum, maximum, r.config.SizeSamples)
	if err != nil {
		return Report{}, err
	}
	runtimes := make([]marketRuntime, 0, len(r.config.Markets))
	sources := make(map[market.MarketID]quoteport.Source, len(r.config.Markets))
	snapshots := make([]market.MarketSnapshot, 0, len(r.config.Markets))
	for _, configured := range r.config.Markets {
		candidate, err := r.bootstrapMarket(ctx, configured, registry, blocks[configured.Venue.Chain], maximum, startedAt)
		if err != nil {
			return Report{}, err
		}
		runtimes = append(runtimes, candidate)
		sources[configured.ID] = candidate.source
		snapshots = append(snapshots, candidate.snapshot)
	}
	costEvidence, cost, err := r.cost(ctx, blocks, startedAt)
	if err != nil {
		return Report{}, err
	}
	threshold, err := market.NewAssetQuantity(r.config.Markets[0].Quote.Token.Asset, r.config.MinimumNet)
	if err != nil {
		return Report{}, err
	}
	candidate, err := strategy.NewTwoMarket(strategy.TwoMarketConfig{
		ID: arbitrage.StrategyID(r.config.ResearchID), Setup: setup, Registry: registry,
		Sources: sources, Grid: grid, Threshold: threshold, Clock: r.clock,
	})
	if err != nil {
		return Report{}, err
	}
	evaluation, err := arbitrage.NewEvaluation(
		arbitrage.EvaluationID("live-evaluation/"+r.config.ResearchID), arbitrage.ResearchRunID(r.config.RunID), candidate.ID(),
		r.config.Hash, snapshots, cost, startedAt, startedAt,
	)
	if err != nil {
		return Report{}, err
	}
	opportunities, err := candidate.Evaluate(ctx, evaluation)
	if err != nil {
		return Report{}, err
	}
	report := Report{Research: runtimeresearch.Report{
		RunID: arbitrage.ResearchRunID(r.config.RunID), ConfigHash: r.config.Hash,
		Status: runtimeresearch.StatusHealthy, Evaluations: 1, Opportunities: opportunities,
	}, Cost: costEvidence}
	report.Parity, err = validateParity(ctx, opportunities, runtimes)
	if err != nil {
		return report, err
	}
	return report, nil
}

func (r *Runner) bootstrapMarket(ctx context.Context, configured ResolvedMarket, registry *market.Registry, block evm.BlockReference, maximum market.AssetQuantity, now time.Time) (marketRuntime, error) {
	network := r.networks[configured.Venue.Chain]
	domainMarket, ok := registry.Market(configured.ID)
	if !ok {
		return marketRuntime{}, fmt.Errorf("registry is missing market %q", configured.ID)
	}
	sourceID := market.SourceID(configured.Venue.Chain + "/pool-logs")
	addresses := map[market.TokenID]common.Address{
		configured.Base.Token.ID:  configured.Base.Address,
		configured.Quote.Token.ID: configured.Quote.Address,
	}
	switch configured.Venue.Kind {
	case "uniswap_v2":
		adapter, err := uniswapv2.NewAdapter(uniswapv2.Config{
			Pool: configured.Venue.Pool, Factory: configured.Venue.Factory,
			BaseToken: configured.Base.Address, QuoteToken: configured.Quote.Address, FeeBPS: configured.Venue.FeeBPS,
		})
		if err != nil {
			return marketRuntime{}, err
		}
		data, err := adapter.Bootstrap(ctx, network, block)
		if err != nil {
			return marketRuntime{}, err
		}
		snapshot, err := snapshotAt(ctx, configured.ID, sourceID, block, constantproduct.Reducer{}, data, now)
		if err != nil {
			return marketRuntime{}, err
		}
		local, err := constantproduct.NewQuoter(market.SourceID(configured.Venue.ID+"/local"), domainMarket)
		if err != nil {
			return marketRuntime{}, err
		}
		reference, err := uniswapv2.NewReferenceQuoter(configured.Venue.Reference)
		if err != nil {
			return marketRuntime{}, err
		}
		return marketRuntime{config: configured, snapshot: snapshot, source: local, reference: func(ctx context.Context, tokenIn, tokenOut market.TokenID, amount *big.Int) (*big.Int, error) {
			return reference.QuoteExactInput(ctx, network, block, addresses[tokenIn], addresses[tokenOut], amount)
		}}, nil
	case "uniswap_v3":
		maxBase, initialQuote, baseToQuoteZero, err := v3Inputs(configured, maximum)
		if err != nil {
			return marketRuntime{}, err
		}
		adapter, err := uniswapv3.NewAdapter(uniswapv3.OnChainConfig{
			Pool: configured.Venue.Pool, MaxTickWords: configured.Venue.MaxTickWords,
			Probes: []uniswapv3.CoverageProbe{{ZeroForOne: baseToQuoteZero, AmountIn: maxBase}, {ZeroForOne: !baseToQuoteZero, AmountIn: initialQuote}},
		})
		if err != nil {
			return marketRuntime{}, err
		}
		data, err := adapter.Bootstrap(ctx, network, block)
		if err != nil {
			return marketRuntime{}, err
		}
		snapshot, err := snapshotAt(ctx, configured.ID, sourceID, block, uniswapv3.Reducer{}, data, now)
		if err != nil {
			return marketRuntime{}, err
		}
		info, ok := adapter.PoolInfo()
		if !ok {
			return marketRuntime{}, fmt.Errorf("Uniswap V3 pool metadata is unavailable")
		}
		token0, token1 := configured.Base.Token.ID, configured.Quote.Token.ID
		if info.Token0 == configured.Quote.Address {
			token0, token1 = token1, token0
		}
		local, err := uniswapv3.NewQuoter(market.SourceID(configured.Venue.ID+"/local"), domainMarket, token0, token1)
		if err != nil {
			return marketRuntime{}, err
		}
		reference, err := uniswapv3.NewReferenceQuoter(configured.Venue.Reference)
		if err != nil {
			return marketRuntime{}, err
		}
		return marketRuntime{config: configured, snapshot: snapshot, source: local, reference: func(ctx context.Context, tokenIn, tokenOut market.TokenID, amount *big.Int) (*big.Int, error) {
			return reference.QuoteExactInputSingle(ctx, network, block, addresses[tokenIn], addresses[tokenOut], amount, info.Fee)
		}}, nil
	case "aerodrome_slipstream":
		maxBase, initialQuote, baseToQuoteZero, err := v3Inputs(configured, maximum)
		if err != nil {
			return marketRuntime{}, err
		}
		adapter, err := aerodromeslipstream.NewAdapter(aerodromeslipstream.Config{
			Pool: configured.Venue.Pool, Factory: configured.Venue.Factory,
			BaseToken: configured.Base.Address, QuoteToken: configured.Quote.Address, MaxTickWords: configured.Venue.MaxTickWords,
			Probes: []uniswapv3.CoverageProbe{{ZeroForOne: baseToQuoteZero, AmountIn: maxBase}, {ZeroForOne: !baseToQuoteZero, AmountIn: initialQuote}},
		})
		if err != nil {
			return marketRuntime{}, err
		}
		data, err := adapter.Bootstrap(ctx, network, block)
		if err != nil {
			return marketRuntime{}, err
		}
		snapshot, err := snapshotAt(ctx, configured.ID, sourceID, block, uniswapv3.Reducer{}, data, now)
		if err != nil {
			return marketRuntime{}, err
		}
		info, ok := adapter.PoolInfo()
		if !ok {
			return marketRuntime{}, fmt.Errorf("Slipstream pool metadata is unavailable")
		}
		token0, token1 := configured.Base.Token.ID, configured.Quote.Token.ID
		if info.Token0 == configured.Quote.Address {
			token0, token1 = token1, token0
		}
		local, err := uniswapv3.NewQuoter(market.SourceID(configured.Venue.ID+"/local"), domainMarket, token0, token1)
		if err != nil {
			return marketRuntime{}, err
		}
		reference, err := aerodromeslipstream.NewReferenceQuoter(configured.Venue.Reference)
		if err != nil {
			return marketRuntime{}, err
		}
		tickSpacing := snapshot.Data().(uniswapv3.Snapshot).TickSpacing()
		return marketRuntime{config: configured, snapshot: snapshot, source: local, reference: func(ctx context.Context, tokenIn, tokenOut market.TokenID, amount *big.Int) (*big.Int, error) {
			return reference.QuoteExactInputSingle(ctx, network, block, addresses[tokenIn], addresses[tokenOut], amount, tickSpacing)
		}}, nil
	default:
		return marketRuntime{}, fmt.Errorf("unsupported venue kind %q", configured.Venue.Kind)
	}
}

func v3Inputs(configured ResolvedMarket, maximum market.AssetQuantity) (*big.Int, *big.Int, bool, error) {
	maxBase, err := maximum.ToTokenAmount(configured.Base.Token)
	if err != nil {
		return nil, nil, false, err
	}
	oneQuote, _ := market.NewAssetQuantity(configured.Quote.Token.Asset, big.NewRat(1, 1))
	initialQuote, err := oneQuote.ToTokenAmount(configured.Quote.Token)
	if err != nil {
		return nil, nil, false, err
	}
	return maxBase.Units(), initialQuote.Units(), bytes.Compare(configured.Base.Address.Bytes(), configured.Quote.Address.Bytes()) < 0, nil
}

func (r *Runner) registry() (*market.Registry, arbitrage.ArbitrageSetup, error) {
	chains := make([]market.Chain, 0, len(r.config.Markets))
	assets := make([]market.Asset, 0, 2)
	tokens := make([]market.Token, 0, 4)
	venues := make([]market.Venue, 0, 2)
	pools := make([]market.Pool, 0, 2)
	paths := make([]market.Path, 0, 2)
	markets := make([]market.Market, 0, 2)
	seenChains := map[market.ChainID]bool{}
	seenAssets := map[market.AssetID]bool{}
	pairID := market.PairID(r.config.SetupID + "/pair")
	for _, configured := range r.config.Markets {
		chainID := market.ChainID(configured.Venue.Chain)
		if !seenChains[chainID] {
			chains = append(chains, market.Chain{ID: chainID})
			seenChains[chainID] = true
		}
		for _, token := range []market.Token{configured.Base.Token, configured.Quote.Token} {
			tokens = append(tokens, token)
			if !seenAssets[token.Asset] {
				assets = append(assets, r.config.Assets[token.Asset])
				seenAssets[token.Asset] = true
			}
		}
		venueID := market.VenueID(configured.Venue.ID)
		poolID := market.PoolID(configured.Venue.ID + "/pool")
		pathID := market.PathID(string(configured.ID) + "/path")
		adapterID := uniswapv2.ID
		if configured.Venue.Kind == "uniswap_v3" {
			adapterID = uniswapv3.ID
		} else if configured.Venue.Kind == "aerodrome_slipstream" {
			adapterID = aerodromeslipstream.ID
		}
		venues = append(venues, market.Venue{ID: venueID})
		pools = append(pools, market.Pool{ID: poolID, Venue: venueID, Chain: chainID, Tokens: []market.TokenID{configured.Base.Token.ID, configured.Quote.Token.ID}, Adapter: adapterID})
		paths = append(paths, market.Path{ID: pathID, Chain: chainID, Hops: []market.Hop{{Pool: poolID, TokenIn: configured.Base.Token.ID, TokenOut: configured.Quote.Token.ID}}})
		markets = append(markets, market.Market{ID: configured.ID, Pair: pairID, Chain: chainID, Path: pathID, BaseToken: configured.Base.Token.ID, QuoteToken: configured.Quote.Token.ID})
	}
	registry, err := market.NewRegistry(market.Catalog{
		Chains: chains, Assets: assets, Tokens: tokens, Venues: venues,
		Pairs: []market.Pair{{ID: pairID, BaseAsset: r.config.Markets[0].Base.Token.Asset, QuoteAsset: r.config.Markets[0].Quote.Token.Asset}},
		Pools: pools, Paths: paths, Markets: markets,
	})
	if err != nil {
		return nil, arbitrage.ArbitrageSetup{}, err
	}
	setup, err := arbitrage.NewArbitrageSetup(arbitrage.SetupID(r.config.SetupID), pairID, []market.MarketID{r.config.Markets[0].ID, r.config.Markets[1].ID}, registry)
	return registry, setup, err
}

func (r *Runner) cost(ctx context.Context, blocks map[string]evm.BlockReference, at time.Time) (CostEvidence, arbitrage.CostSnapshot, error) {
	primaryConfig := r.config.PriceSource.Primary
	apiKey := ""
	apiHeader := ""
	baseURL := primaryConfig.BaseURL
	if primaryConfig.APIKeyEnv != "" {
		var ok bool
		apiKey, ok = r.lookup(primaryConfig.APIKeyEnv)
		if !ok || strings.TrimSpace(apiKey) == "" {
			return CostEvidence{}, arbitrage.CostSnapshot{}, fmt.Errorf("CoinGecko API key is unset")
		}
		if primaryConfig.APIKeyKind == "pro" {
			apiHeader, baseURL = "x-cg-pro-api-key", coingecko.ProBaseURL
		} else {
			apiHeader = "x-cg-demo-api-key"
		}
	}
	primary, err := coingecko.New(coingecko.Config{
		ID: market.SourceID(string(r.config.PriceSource.ID) + "/coingecko"), Base: r.config.PriceSource.Base, Quote: r.config.PriceSource.Quote,
		CoinID: primaryConfig.CoinID, Currency: primaryConfig.Currency, BaseURL: baseURL,
		APIKey: apiKey, APIKeyHeader: apiHeader, Client: r.client, Clock: r.clock,
	})
	if err != nil {
		return CostEvidence{}, arbitrage.CostSnapshot{}, err
	}
	fallbackConfig := r.config.PriceSource.Fallback
	fallback, err := chainlink.NewSource(
		market.SourceID(string(r.config.PriceSource.ID)+"/chainlink"), r.config.PriceSource.Base, r.config.PriceSource.Quote,
		r.networks[fallbackConfig.Chain], blocks[fallbackConfig.Chain], common.HexToAddress(fallbackConfig.FeedAddress), r.clock,
	)
	if err != nil {
		return CostEvidence{}, arbitrage.CostSnapshot{}, err
	}
	source, err := costing.NewFallbackPriceSource(r.config.PriceSource.ID, primary, fallback)
	if err != nil {
		return CostEvidence{}, arbitrage.CostSnapshot{}, err
	}
	price, err := source.Observe(ctx, priceport.Request{Base: r.config.PriceSource.Base, Quote: r.config.PriceSource.Quote})
	if err != nil {
		return CostEvidence{}, arbitrage.CostSnapshot{}, err
	}
	costAmount, err := market.NewAssetQuantity(r.config.PriceSource.Base, new(big.Rat).Quo(new(big.Rat).Set(r.config.FixedCost), price.Value()))
	if err != nil {
		return CostEvidence{}, arbitrage.CostSnapshot{}, err
	}
	cost := arbitrage.CostSnapshot{ID: "fixed/" + string(price.Source()), Amount: costAmount, CapturedAt: at}
	return CostEvidence{FixedAmount: new(big.Rat).Set(r.config.FixedCost), FixedAsset: r.config.PriceSource.Quote, Cost: costAmount, Price: price}, cost, nil
}

func validateParity(ctx context.Context, opportunities []arbitrage.Opportunity, runtimes []marketRuntime) ([]ParityEvidence, error) {
	byMarket := make(map[market.MarketID]marketRuntime, len(runtimes))
	for _, runtime := range runtimes {
		byMarket[runtime.config.ID] = runtime
	}
	var evidence []ParityEvidence
	for _, opportunity := range opportunities {
		for _, candidate := range opportunity.Candidates {
			for _, leg := range []struct {
				name  string
				quote market.Quote
			}{{"buy", candidate.BuyQuote}, {"sell", candidate.SellQuote}} {
				runtime, ok := byMarket[leg.quote.Market]
				if !ok {
					return evidence, fmt.Errorf("unknown parity market %q", leg.quote.Market)
				}
				reference, err := runtime.reference(ctx, leg.quote.AmountIn.Token(), leg.quote.AmountOut.Token(), leg.quote.AmountIn.Units())
				if err != nil {
					return evidence, err
				}
				matches := leg.quote.AmountOut.Units().Cmp(reference) == 0
				evidence = append(evidence, ParityEvidence{Market: leg.quote.Market, Leg: leg.name, AmountIn: leg.quote.AmountIn, LocalOut: leg.quote.AmountOut, ReferenceOut: new(big.Int).Set(reference), Matches: matches})
				if !matches {
					return evidence, ErrParityMismatch
				}
			}
		}
	}
	return evidence, nil
}

func snapshotAt(ctx context.Context, marketID market.MarketID, source market.SourceID, block evm.BlockReference, reducer marketstate.Reducer, data market.EventData, now time.Time) (market.MarketSnapshot, error) {
	mirror, err := marketstate.NewMirror(marketID, source, reducer, sourceorder.NewMonotonic(evmlogs.BlockPositionKind, false), func() time.Time { return now })
	if err != nil {
		return market.MarketSnapshot{}, err
	}
	event, err := market.NewMarketEvent(market.MarketEvent{
		Market: marketID, Source: source,
		Position: market.SourcePosition{Kind: evmlogs.BlockPositionKind, Value: block.Number}, Reference: market.SourceReference{Kind: evmlogs.BlockHashReferenceKind, Value: block.Hash.Hex()},
		Finality: market.FinalityPreconfirmed, ReceivedAt: now, Data: data,
	})
	if err != nil {
		return market.MarketSnapshot{}, err
	}
	result, err := mirror.Apply(ctx, event)
	if err != nil {
		return market.MarketSnapshot{}, err
	}
	return result.Snapshot, nil
}
