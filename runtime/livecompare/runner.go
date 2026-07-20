package livecompare

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/adapters/feed/evmlogs"
	"github.com/VarozXYZ/vernier/adapters/feed/sourceorder"
	"github.com/VarozXYZ/vernier/adapters/market/aerodromeslipstream"
	"github.com/VarozXYZ/vernier/adapters/market/constantproduct"
	"github.com/VarozXYZ/vernier/adapters/market/uniswapv2"
	"github.com/VarozXYZ/vernier/adapters/market/uniswapv3"
	"github.com/VarozXYZ/vernier/adapters/price/chainlink"
	"github.com/VarozXYZ/vernier/core/marketstate"
	"github.com/VarozXYZ/vernier/core/sizing"
	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
	runtimeresearch "github.com/VarozXYZ/vernier/runtime/research"
)

const (
	robinhoodMarketID market.MarketID = "virtual-weth-robinhood"
	baseMarketID      market.MarketID = "virtual-weth-base"
	baseAssetID       market.AssetID  = "virtual"
	quoteAssetID      market.AssetID  = "weth"
)

var ErrParityMismatch = errors.New("local quote differs from venue reference")

const erc20ABIJSON = `[
  {"type":"function","name":"decimals","stateMutability":"view","inputs":[],"outputs":[{"type":"uint8"}]}
]`

var erc20ABI = mustRuntimeABI(erc20ABIJSON)

type Networks struct {
	Robinhood evm.Network
	Base      evm.Network
}

type Options struct {
	Clock func() time.Time
}

type Runner struct {
	config   ParsedConfig
	networks Networks
	clock    func() time.Time
}

type CostEvidence struct {
	FixedUSD       *big.Rat
	WETHCost       market.AssetQuantity
	WETHUSD        *big.Rat
	PriceBlock     evm.BlockReference
	PriceRound     *big.Int
	PriceUpdatedAt time.Time
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

func New(config ParsedConfig, networks Networks, options Options) (*Runner, error) {
	if networks.Robinhood == nil || networks.Base == nil ||
		networks.Robinhood.ID() != RobinhoodNetworkAdapter ||
		networks.Base.ID() != BaseNetworkAdapter {
		return nil, fmt.Errorf("live comparison requires explicit Robinhood and Base adapters")
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	robinhoodNetwork, err := evm.NewRateLimitedNetwork(
		networks.Robinhood, time.Duration(config.Robinhood.RPCMinIntervalMS)*time.Millisecond,
	)
	if err != nil {
		return nil, err
	}
	baseNetwork, err := evm.NewRateLimitedNetwork(
		networks.Base, time.Duration(config.Base.RPCMinIntervalMS)*time.Millisecond,
	)
	if err != nil {
		return nil, err
	}
	networks.Robinhood = robinhoodNetwork
	networks.Base = baseNetwork
	return &Runner{config: config, networks: networks, clock: options.Clock}, nil
}

func (r *Runner) Run(ctx context.Context) (Report, error) {
	startedAt := r.clock().UTC()
	robinhoodBlock, err := r.networks.Robinhood.CurrentBlock(ctx)
	if err != nil {
		return Report{}, err
	}
	baseBlock, err := r.networks.Base.CurrentBlock(ctx)
	if err != nil {
		return Report{}, err
	}
	tokens, err := r.loadTokens(ctx, robinhoodBlock, baseBlock)
	if err != nil {
		return Report{}, err
	}
	registry, setup, err := buildRegistry(tokens)
	if err != nil {
		return Report{}, err
	}
	maxSize, err := market.NewAssetQuantity(baseAssetID, r.config.SizeValues[len(r.config.SizeValues)-1])
	if err != nil {
		return Report{}, err
	}
	maxBaseInput, err := maxSize.ToTokenAmount(tokens.baseBase)
	if err != nil {
		return Report{}, err
	}
	oneQuote, err := market.NewAssetQuantity(quoteAssetID, big.NewRat(1, 1))
	if err != nil {
		return Report{}, err
	}
	initialQuoteInput, err := oneQuote.ToTokenAmount(tokens.baseQuote)
	if err != nil {
		return Report{}, err
	}
	baseToQuoteZero := addressLess(r.config.BaseBaseToken, r.config.BaseQuoteToken)
	slipstream, err := aerodromeslipstream.NewAdapter(aerodromeslipstream.Config{
		Pool: r.config.BasePool, Factory: r.config.BaseFactory,
		BaseToken: r.config.BaseBaseToken, QuoteToken: r.config.BaseQuoteToken,
		MaxTickWords: r.config.Base.MaxTickWords,
		Probes: []uniswapv3.CoverageProbe{
			{ZeroForOne: baseToQuoteZero, AmountIn: maxBaseInput.Units()},
			{ZeroForOne: !baseToQuoteZero, AmountIn: initialQuoteInput.Units()},
		},
	})
	if err != nil {
		return Report{}, err
	}
	v2, err := uniswapv2.NewAdapter(uniswapv2.Config{
		Pool: r.config.RobinhoodPool, Factory: r.config.RobinhoodFactory,
		BaseToken: r.config.RobinhoodBaseToken, QuoteToken: r.config.RobinhoodQuoteToken,
		FeeBPS: r.config.Robinhood.FeeBPS,
	})
	if err != nil {
		return Report{}, err
	}
	v2Data, err := v2.Bootstrap(ctx, r.networks.Robinhood, robinhoodBlock)
	if err != nil {
		return Report{}, err
	}
	baseData, err := slipstream.Bootstrap(ctx, r.networks.Base, baseBlock)
	if err != nil {
		return Report{}, err
	}
	robinhoodSnapshot, err := snapshotAt(
		ctx, robinhoodMarketID, "robinhood/pool-logs", robinhoodBlock,
		constantproduct.Reducer{}, v2Data, startedAt,
	)
	if err != nil {
		return Report{}, err
	}
	baseSnapshot, err := snapshotAt(
		ctx, baseMarketID, "base/pool-logs", baseBlock,
		uniswapv3.Reducer{}, baseData, startedAt,
	)
	if err != nil {
		return Report{}, err
	}
	costEvidence, cost, err := r.cost(ctx, baseBlock, startedAt)
	if err != nil {
		return Report{}, err
	}
	gridValues := make([]market.AssetQuantity, len(r.config.SizeValues))
	for index, value := range r.config.SizeValues {
		gridValues[index], err = market.NewAssetQuantity(baseAssetID, value)
		if err != nil {
			return Report{}, err
		}
	}
	grid, err := sizing.NewGrid(gridValues)
	if err != nil {
		return Report{}, err
	}
	threshold, _ := market.NewAssetQuantity(quoteAssetID, new(big.Rat))
	robinhoodMarket, _ := registry.Market(robinhoodMarketID)
	baseMarket, _ := registry.Market(baseMarketID)
	v2Local, err := constantproduct.NewQuoter("uniswap-v2/local", robinhoodMarket)
	if err != nil {
		return Report{}, err
	}
	baseInfo, ok := slipstream.PoolInfo()
	if !ok {
		return Report{}, fmt.Errorf("Slipstream pool metadata is unavailable")
	}
	token0ID, token1ID := tokens.baseBase.ID, tokens.baseQuote.ID
	if baseInfo.Token0 == r.config.BaseQuoteToken {
		token0ID, token1ID = token1ID, token0ID
	}
	slipstreamLocal, err := uniswapv3.NewQuoter("aerodrome-slipstream/local", baseMarket, token0ID, token1ID)
	if err != nil {
		return Report{}, err
	}
	candidate, err := strategy.NewTwoMarket(strategy.TwoMarketConfig{
		ID: "virtual-weth-live", Setup: setup, Registry: registry,
		Sources: map[market.MarketID]quoteport.Source{
			robinhoodMarketID: v2Local,
			baseMarketID:      slipstreamLocal,
		},
		Grid: grid, Threshold: threshold, Clock: r.clock,
	})
	if err != nil {
		return Report{}, err
	}
	evaluation, err := arbitrage.NewEvaluation(
		"live-evaluation/virtual-weth-live", arbitrage.ResearchRunID(r.config.RunID), candidate.ID(),
		r.config.Hash, []market.MarketSnapshot{robinhoodSnapshot, baseSnapshot}, cost,
		startedAt, startedAt,
	)
	if err != nil {
		return Report{}, err
	}
	opportunities, err := candidate.Evaluate(ctx, evaluation)
	if err != nil {
		return Report{}, err
	}
	report := Report{
		Research: runtimeresearch.Report{
			RunID: arbitrage.ResearchRunID(r.config.RunID), ConfigHash: r.config.Hash,
			Status: runtimeresearch.StatusHealthy, Evaluations: 1, Opportunities: opportunities,
		},
		Cost: costEvidence,
	}
	report.Parity, err = r.validateParity(
		ctx, report.Research.Opportunities, robinhoodBlock, baseBlock,
		v2, slipstream, tokens, baseSnapshot.Data().(uniswapv3.Snapshot).TickSpacing(),
	)
	if err != nil {
		return report, err
	}
	return report, nil
}

type liveTokens struct {
	robinhoodBase  market.Token
	robinhoodQuote market.Token
	baseBase       market.Token
	baseQuote      market.Token
}

func (r *Runner) loadTokens(
	ctx context.Context,
	robinhoodBlock evm.BlockReference,
	baseBlock evm.BlockReference,
) (liveTokens, error) {
	robinhoodBaseDecimals, err := readDecimals(ctx, r.networks.Robinhood, robinhoodBlock, r.config.RobinhoodBaseToken)
	if err != nil {
		return liveTokens{}, err
	}
	robinhoodQuoteDecimals, err := readDecimals(ctx, r.networks.Robinhood, robinhoodBlock, r.config.RobinhoodQuoteToken)
	if err != nil {
		return liveTokens{}, err
	}
	baseBaseDecimals, err := readDecimals(ctx, r.networks.Base, baseBlock, r.config.BaseBaseToken)
	if err != nil {
		return liveTokens{}, err
	}
	baseQuoteDecimals, err := readDecimals(ctx, r.networks.Base, baseBlock, r.config.BaseQuoteToken)
	if err != nil {
		return liveTokens{}, err
	}
	return liveTokens{
		robinhoodBase:  market.Token{ID: "virtual-robinhood", Asset: baseAssetID, Chain: "robinhood", Decimals: robinhoodBaseDecimals, Symbol: "VIRTUAL"},
		robinhoodQuote: market.Token{ID: "weth-robinhood", Asset: quoteAssetID, Chain: "robinhood", Decimals: robinhoodQuoteDecimals, Symbol: "WETH"},
		baseBase:       market.Token{ID: "virtual-base", Asset: baseAssetID, Chain: "base", Decimals: baseBaseDecimals, Symbol: "VIRTUAL"},
		baseQuote:      market.Token{ID: "weth-base", Asset: quoteAssetID, Chain: "base", Decimals: baseQuoteDecimals, Symbol: "WETH"},
	}, nil
}

func buildRegistry(tokens liveTokens) (*market.Registry, arbitrage.ArbitrageSetup, error) {
	basePoolTokens := []market.TokenID{tokens.baseBase.ID, tokens.baseQuote.ID}
	registry, err := market.NewRegistry(market.Catalog{
		Chains: []market.Chain{{ID: "robinhood"}, {ID: "base"}},
		Assets: []market.Asset{{ID: baseAssetID, Symbol: "VIRTUAL"}, {ID: quoteAssetID, Symbol: "WETH"}},
		Tokens: []market.Token{tokens.robinhoodBase, tokens.robinhoodQuote, tokens.baseBase, tokens.baseQuote},
		Venues: []market.Venue{{ID: "uniswap-v2"}, {ID: "aerodrome-slipstream"}},
		Pairs:  []market.Pair{{ID: "virtual-weth", BaseAsset: baseAssetID, QuoteAsset: quoteAssetID}},
		Pools: []market.Pool{
			{ID: "robinhood-pool", Venue: "uniswap-v2", Chain: "robinhood", Tokens: []market.TokenID{tokens.robinhoodBase.ID, tokens.robinhoodQuote.ID}, Adapter: uniswapv2.ID},
			{ID: "base-pool", Venue: "aerodrome-slipstream", Chain: "base", Tokens: basePoolTokens, Adapter: aerodromeslipstream.ID},
		},
		Paths: []market.Path{
			{ID: "robinhood-path", Chain: "robinhood", Hops: []market.Hop{{Pool: "robinhood-pool", TokenIn: tokens.robinhoodBase.ID, TokenOut: tokens.robinhoodQuote.ID}}},
			{ID: "base-path", Chain: "base", Hops: []market.Hop{{Pool: "base-pool", TokenIn: tokens.baseBase.ID, TokenOut: tokens.baseQuote.ID}}},
		},
		Markets: []market.Market{
			{ID: robinhoodMarketID, Pair: "virtual-weth", Chain: "robinhood", Path: "robinhood-path", BaseToken: tokens.robinhoodBase.ID, QuoteToken: tokens.robinhoodQuote.ID},
			{ID: baseMarketID, Pair: "virtual-weth", Chain: "base", Path: "base-path", BaseToken: tokens.baseBase.ID, QuoteToken: tokens.baseQuote.ID},
		},
	})
	if err != nil {
		return nil, arbitrage.ArbitrageSetup{}, err
	}
	setup, err := arbitrage.NewArbitrageSetup(
		"virtual-weth-cross-chain", "virtual-weth",
		[]market.MarketID{robinhoodMarketID, baseMarketID}, registry,
	)
	return registry, setup, err
}

func snapshotAt(
	ctx context.Context,
	marketID market.MarketID,
	source market.SourceID,
	block evm.BlockReference,
	reducer marketstate.Reducer,
	data market.EventData,
	now time.Time,
) (market.MarketSnapshot, error) {
	mirror, err := marketstate.NewMirror(
		marketID, source, reducer,
		sourceorder.NewMonotonic(evmlogs.BlockPositionKind, false), func() time.Time { return now },
	)
	if err != nil {
		return market.MarketSnapshot{}, err
	}
	event, err := market.NewMarketEvent(market.MarketEvent{
		Market: marketID, Source: source,
		Position:  market.SourcePosition{Kind: evmlogs.BlockPositionKind, Value: block.Number},
		Reference: market.SourceReference{Kind: evmlogs.BlockHashReferenceKind, Value: block.Hash.Hex()},
		Finality:  market.FinalityPreconfirmed, ReceivedAt: now, Data: data,
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

func (r *Runner) cost(
	ctx context.Context,
	baseBlock evm.BlockReference,
	at time.Time,
) (CostEvidence, arbitrage.CostSnapshot, error) {
	price, err := chainlink.Read(ctx, r.networks.Base, baseBlock, r.config.PriceFeed)
	if err != nil {
		return CostEvidence{}, arbitrage.CostSnapshot{}, err
	}
	wethCost, err := market.NewAssetQuantity(
		quoteAssetID, new(big.Rat).Quo(new(big.Rat).Set(r.config.FixedCost), price.Value()),
	)
	if err != nil {
		return CostEvidence{}, arbitrage.CostSnapshot{}, err
	}
	cost := arbitrage.CostSnapshot{ID: "fixed-usd/chainlink-weth-usd", Amount: wethCost, CapturedAt: at}
	return CostEvidence{
		FixedUSD: new(big.Rat).Set(r.config.FixedCost), WETHCost: wethCost,
		WETHUSD: price.Value(), PriceBlock: price.Block, PriceRound: new(big.Int).Set(price.RoundID),
		PriceUpdatedAt: price.UpdatedAt,
	}, cost, nil
}

func (r *Runner) validateParity(
	ctx context.Context,
	opportunities []arbitrage.Opportunity,
	robinhoodBlock evm.BlockReference,
	baseBlock evm.BlockReference,
	v2 *uniswapv2.Adapter,
	slipstream *aerodromeslipstream.Adapter,
	tokens liveTokens,
	tickSpacing int32,
) ([]ParityEvidence, error) {
	v2Reference, err := uniswapv2.NewReferenceQuoter(r.config.RobinhoodRouter)
	if err != nil {
		return nil, err
	}
	slipstreamReference, err := aerodromeslipstream.NewReferenceQuoter(r.config.BaseQuoter)
	if err != nil {
		return nil, err
	}
	_, ok := v2.PoolInfo()
	if !ok {
		return nil, fmt.Errorf("Uniswap V2 pool metadata is unavailable")
	}
	_, ok = slipstream.PoolInfo()
	if !ok {
		return nil, fmt.Errorf("Slipstream pool metadata is unavailable")
	}
	tokenAddress := map[market.TokenID]common.Address{
		tokens.robinhoodBase.ID: r.config.RobinhoodBaseToken, tokens.robinhoodQuote.ID: r.config.RobinhoodQuoteToken,
		tokens.baseBase.ID: r.config.BaseBaseToken, tokens.baseQuote.ID: r.config.BaseQuoteToken,
	}
	var evidence []ParityEvidence
	for _, opportunity := range opportunities {
		for _, candidate := range opportunity.Candidates {
			for _, leg := range []struct {
				name  string
				quote market.Quote
			}{{name: "buy", quote: candidate.BuyQuote}, {name: "sell", quote: candidate.SellQuote}} {
				inputAddress := tokenAddress[leg.quote.AmountIn.Token()]
				outputAddress := tokenAddress[leg.quote.AmountOut.Token()]
				var reference *big.Int
				switch leg.quote.Market {
				case robinhoodMarketID:
					reference, err = v2Reference.QuoteExactInput(
						ctx, r.networks.Robinhood, robinhoodBlock,
						inputAddress, outputAddress, leg.quote.AmountIn.Units(),
					)
				case baseMarketID:
					reference, err = slipstreamReference.QuoteExactInputSingle(
						ctx, r.networks.Base, baseBlock,
						inputAddress, outputAddress, leg.quote.AmountIn.Units(),
						tickSpacing,
					)
				default:
					err = fmt.Errorf("unknown parity market %q", leg.quote.Market)
				}
				if err != nil {
					return evidence, err
				}
				matches := leg.quote.AmountOut.Units().Cmp(reference) == 0
				evidence = append(evidence, ParityEvidence{
					Market: leg.quote.Market, Leg: leg.name, AmountIn: leg.quote.AmountIn,
					LocalOut: leg.quote.AmountOut, ReferenceOut: new(big.Int).Set(reference), Matches: matches,
				})
				if !matches {
					return evidence, ErrParityMismatch
				}
			}
		}
	}
	return evidence, nil
}

func readDecimals(
	ctx context.Context,
	network evm.Network,
	block evm.BlockReference,
	token common.Address,
) (uint8, error) {
	input, _ := erc20ABI.Pack("decimals")
	output, err := network.CallContract(ctx, block, geth.CallMsg{To: &token, Data: input})
	if err != nil {
		return 0, err
	}
	values, err := erc20ABI.Unpack("decimals", output)
	if err != nil {
		return 0, fmt.Errorf("decode ERC-20 decimals: %w", err)
	}
	decimals, ok := values[0].(uint8)
	if !ok || decimals > 36 {
		return 0, fmt.Errorf("ERC-20 returned invalid decimals")
	}
	return decimals, nil
}

func addressLess(left, right common.Address) bool {
	return bytes.Compare(left.Bytes(), right.Bytes()) < 0
}

func mustRuntimeABI(definition string) abi.ABI {
	parsed, err := abi.JSON(strings.NewReader(definition))
	if err != nil {
		panic(err)
	}
	return parsed
}
