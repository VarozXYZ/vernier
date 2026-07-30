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
