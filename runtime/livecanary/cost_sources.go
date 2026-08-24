package livecanary

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	jupiterquote "github.com/VarozXYZ/vernier/adapters/quote/jupiter"
	kyberquote "github.com/VarozXYZ/vernier/adapters/quote/kyberswap"
	"github.com/VarozXYZ/vernier/domain/market"
	"github.com/VarozXYZ/vernier/runtime/configuration"
)

const (
	wrappedSOLMint      = "So11111111111111111111111111111111111111112"
	wrappedPOLAddress   = "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270"
	nativePriceInputSOL = "1000000000"
	nativePriceInputPOL = "1000000000000000000"
)

func composeCostValuator(
	ctx context.Context,
	config ComposeConfig,
	solanaMarket configuration.ResolvedMarket,
	evmMarket configuration.ResolvedMarket,
) (*CostValuator, error) {
	solanaSource, ok := config.Research.QuoteSources[solanaMarket.QuoteSource]
	if !ok || solanaSource.Kind != "jupiter" {
		return nil, fmt.Errorf("native SOL cost pricing requires Jupiter")
	}
	evmSource, ok := config.Research.QuoteSources[evmMarket.QuoteSource]
	if !ok || evmSource.Kind != "kyberswap" {
		return nil, fmt.Errorf("native POL cost pricing requires KyberSwap")
	}
	keysText, err := requiredEnv(config.LookupEnv, solanaSource.APIKeyEnv)
	if err != nil {
		return nil, err
	}
	jupiterClient, err := jupiterquote.NewQuoteSource(jupiterquote.QuoteConfig{
		ID: "live-cost/sol-usdc", BaseURL: solanaSource.BaseURL,
		QuotePath: solanaSource.QuotePath, APIKeys: splitValues(keysText),
		SlippageBPS: solanaSource.SlippageBPS, ExpectedMode: solanaSource.ExpectedMode,
		SwapMode: "ExactIn", Limiter: jupiterquote.ImmediateLimiter{},
	})
	if err != nil {
		return nil, err
	}
	clientID, err := requiredEnv(config.LookupEnv, evmSource.ClientIDEnv)
	if err != nil {
		return nil, err
	}
	kyberClient, err := kyberquote.New(kyberquote.Config{
		BaseURL: evmSource.BaseURL, ClientID: clientID,
	})
	if err != nil {
		return nil, err
	}
	quoteDecimals := solanaMarket.Quote.Token.Decimals
	if quoteDecimals != evmMarket.Quote.Token.Decimals {
		return nil, fmt.Errorf("native cost pricing requires equivalent quote decimals")
	}
	scale := new(big.Int).Exp(
		big.NewInt(10), big.NewInt(int64(quoteDecimals)), nil,
	)
	refresh := func(refreshCtx context.Context) (map[market.AssetID]CostAssetPrice, error) {
		type observed struct {
			asset  market.AssetID
			price  *big.Rat
			source string
			err    error
		}
		results := make(chan observed, 2)
		var group sync.WaitGroup
		group.Add(2)
		go func() {
			defer group.Done()
			quote, quoteErr := jupiterClient.Quote(
				refreshCtx,
				jupiterquote.QuoteRequest{
					InputMint:  wrappedSOLMint,
					OutputMint: solanaMarket.Quote.AddressText,
					Amount:     nativePriceInputSOL,
				},
			)
			if quoteErr != nil {
				results <- observed{asset: "sol", err: quoteErr}
				return
			}
			units, valid := new(big.Int).SetString(quote.ToTokenAmount, 10)
			if !valid || units.Sign() <= 0 {
				results <- observed{
					asset: "sol",
					err:   fmt.Errorf("jupiter returned invalid SOL/USDC output"),
				}
				return
			}
			results <- observed{
				asset: "sol", price: new(big.Rat).SetFrac(units, scale),
				source: "jupiter_sol_usdc",
			}
		}()
		go func() {
			defer group.Done()
			quote, quoteErr := kyberClient.Route(
				refreshCtx,
				kyberquote.RouteRequest{
					Chain:    evmSource.ChainSlug,
					TokenIn:  wrappedPOLAddress,
					TokenOut: evmMarket.Quote.AddressText,
					AmountIn: nativePriceInputPOL,
				},
			)
			if quoteErr != nil {
				results <- observed{asset: "pol", err: quoteErr}
				return
			}
			units, valid := new(big.Int).SetString(quote.AmountOut, 10)
			if !valid || units.Sign() <= 0 {
				results <- observed{
					asset: "pol",
					err:   fmt.Errorf("KyberSwap returned invalid POL/USDC output"),
				}
				return
			}
			results <- observed{
				asset: "pol", price: new(big.Rat).SetFrac(units, scale),
				source: "kyberswap_pol_usdc",
			}
		}()
		go func() {
			group.Wait()
			close(results)
		}()
		prices := make(map[market.AssetID]CostAssetPrice, 2)
		now := time.Now().UTC()
		for result := range results {
			if result.err != nil {
				return nil, fmt.Errorf("refresh %s native price: %w", result.asset, result.err)
			}
			prices[result.asset] = CostAssetPrice{
				Value: result.price, CapturedAt: now, Source: result.source,
			}
		}
		return prices, nil
	}
	valuator, err := NewCostValuator(
		solanaMarket.Quote.Token.Asset, refresh, time.Now,
	)
	if err != nil {
		return nil, err
	}
	if err := valuator.Warm(ctx); err != nil {
		return nil, err
	}
	go valuator.Run(ctx, config.Live.FeeCacheMaxAge)
	return valuator, nil
}

func composeEVMCostValuator(ctx context.Context, config ComposeConfig,
	markets []configuration.ResolvedMarket) (*CostValuator, error) {
	if len(markets) != 2 || markets[0].Quote.Token.Asset != markets[1].Quote.Token.Asset {
		return nil, fmt.Errorf("EVM native cost pricing requires two markets with one quote asset")
	}
	quoteAsset := markets[0].Quote.Token.Asset
	var kyberProfile configuration.ResolvedQuoteSource
	for _, candidate := range config.Research.QuoteSources {
		if candidate.Kind == "kyberswap" {
			kyberProfile = candidate
			break
		}
	}
	if kyberProfile.Kind == "" {
		return nil, fmt.Errorf("EVM native cost pricing requires KyberSwap")
	}
	clientID, err := requiredEnv(config.LookupEnv, kyberProfile.ClientIDEnv)
	if err != nil {
		return nil, err
	}
	client, err := kyberquote.New(kyberquote.Config{BaseURL: kyberProfile.BaseURL, ClientID: clientID})
	if err != nil {
		return nil, err
	}
	type nativeSource struct {
		asset         market.AssetID
		chain         string
		outputAddress string
		outputScale   *big.Int
	}
	sources := make([]nativeSource, 0, 2)
	for _, configured := range markets {
		nativeConfig, ok := config.Live.NativeAssets[configured.Chain]
		if !ok {
			return nil, fmt.Errorf("native asset metadata for %s is unavailable", configured.Chain)
		}
		native, err := evmNativeToken(configured.Chain, config.Research.Chains[configured.Chain].ChainID, nativeConfig)
		if err != nil {
			return nil, err
		}
		sources = append(sources, nativeSource{asset: native.Asset, chain: configured.Chain,
			outputAddress: configured.Quote.AddressText,
			outputScale:   new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(configured.Quote.Token.Decimals)), nil)})
	}
	refresh := func(refreshCtx context.Context) (map[market.AssetID]CostAssetPrice, error) {
		result := make(map[market.AssetID]CostAssetPrice, len(sources))
		for _, item := range sources {
			quote, quoteErr := client.Route(refreshCtx, kyberquote.RouteRequest{Chain: item.chain,
				TokenIn: evmNativePseudoAddress, TokenOut: item.outputAddress, AmountIn: "1000000000000000000"})
			if quoteErr != nil {
				return nil, fmt.Errorf("refresh %s native price through KyberSwap: %w", item.asset, quoteErr)
			}
			units, ok := new(big.Int).SetString(quote.AmountOut, 10)
			if !ok || units.Sign() <= 0 {
				return nil, fmt.Errorf("KyberSwap returned invalid %s native price", item.asset)
			}
			result[item.asset] = CostAssetPrice{Value: new(big.Rat).SetFrac(units, item.outputScale),
				CapturedAt: time.Now().UTC(), Source: "kyberswap_native_quote/" + item.chain}
		}
		return result, nil
	}
	valuator, err := NewCostValuator(quoteAsset, refresh, time.Now)
	if err != nil {
		return nil, err
	}
	if err := valuator.Warm(ctx); err != nil {
		return nil, err
	}
	go valuator.Run(ctx, config.Live.FeeCacheMaxAge)
	return valuator, nil
}
