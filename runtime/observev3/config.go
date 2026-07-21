// Package observev3 composes read-only canonical Uniswap V3 observation.
package observev3

import (
	"bytes"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"

	"github.com/VarozXYZ/vernier/adapters/market/uniswapv3"
	"github.com/VarozXYZ/vernier/domain/market"
	"github.com/VarozXYZ/vernier/runtime/configuration"
)

type QuoteInput struct {
	TokenIn string
	Amount  string
}

type Config struct {
	Hash            string
	Network         configuration.ResolvedChain
	NetworkAdapter  string
	VenueAdapter    string
	MarketID        string
	PoolAddress     string
	QuoterV2Address string
	Token0ID        string
	Token1ID        string
	QuoteInputs     []QuoteInput
	MaxTickWords    int
	Pool            common.Address
	QuoterV2        common.Address
	ProbeInput      []*big.Int
}

type ParsedConfig = Config

type Endpoints struct {
	HTTP string
	WS   string
}

func FromConfig(bundle configuration.ParsedConfig, selected string) (Config, error) {
	var configured *configuration.ResolvedMarket
	for index := range bundle.Markets {
		candidate := &bundle.Markets[index]
		if selected != "" && string(candidate.ID) != selected {
			continue
		}
		if candidate.Venue.Kind != "uniswap_v3" {
			if selected != "" {
				return Config{}, fmt.Errorf("market %q is not canonical Uniswap V3", selected)
			}
			continue
		}
		if configured != nil {
			return Config{}, fmt.Errorf("multiple Uniswap V3 markets require an explicit selection")
		}
		configured = candidate
	}
	if configured == nil {
		return Config{}, fmt.Errorf("configuration has no selected canonical Uniswap V3 market")
	}
	chain, ok := bundle.Chains[configured.Venue.Chain]
	if !ok {
		return Config{}, fmt.Errorf("market network profile is unavailable")
	}
	var baseAmount, quoteAmount market.TokenAmount
	if bundle.SizingAsset == "base" {
		maximum, err := market.NewAssetQuantity(configured.Base.Token.Asset, bundle.MaximumSize)
		if err != nil {
			return Config{}, fmt.Errorf("maximum sizing probe is invalid")
		}
		baseAmount, err = maximum.ToTokenAmount(configured.Base.Token)
		if err != nil || baseAmount.IsZero() {
			return Config{}, fmt.Errorf("maximum sizing probe is invalid")
		}
		oneQuote, err := market.NewAssetQuantity(configured.Quote.Token.Asset, big.NewRat(1, 1))
		if err != nil {
			return Config{}, fmt.Errorf("quote-asset probe is invalid")
		}
		quoteAmount, err = oneQuote.ToTokenAmount(configured.Quote.Token)
		if err != nil || quoteAmount.IsZero() {
			return Config{}, fmt.Errorf("quote-asset probe is invalid")
		}
	} else {
		maximum, err := market.NewAssetQuantity(configured.Quote.Token.Asset, bundle.MaximumSize)
		if err != nil {
			return Config{}, fmt.Errorf("maximum sizing probe is invalid")
		}
		quoteAmount, err = maximum.ToTokenAmount(configured.Quote.Token)
		if err != nil || quoteAmount.IsZero() {
			return Config{}, fmt.Errorf("maximum sizing probe is invalid")
		}
		// The opposite-token probe is a deterministic coverage hint. Exact
		// quote sizing remains the responsibility of the local quoter.
		baseAmount, err = market.NewTokenAmount(configured.Base.Token.ID, quoteAmount.Units())
		if err != nil || baseAmount.IsZero() {
			return Config{}, fmt.Errorf("base-asset probe is invalid")
		}
	}
	token0, token1 := configured.Base, configured.Quote
	if bytes.Compare(token0.Address.Bytes(), token1.Address.Bytes()) > 0 {
		token0, token1 = token1, token0
	}
	amountByToken := map[market.TokenID]*big.Int{
		configured.Base.Token.ID: baseAmount.Units(), configured.Quote.Token.ID: quoteAmount.Units(),
	}
	return Config{
		Hash: bundle.Hash, Network: chain, NetworkAdapter: chain.ID, VenueAdapter: uniswapv3.ID,
		MarketID: string(configured.ID), PoolAddress: configured.Venue.Pool.Hex(), QuoterV2Address: configured.Venue.Reference.Hex(),
		Token0ID: string(token0.Token.ID), Token1ID: string(token1.Token.ID), MaxTickWords: configured.Venue.MaxTickWords,
		Pool: configured.Venue.Pool, QuoterV2: configured.Venue.Reference,
		QuoteInputs: []QuoteInput{{TokenIn: string(token0.Token.ID), Amount: amountByToken[token0.Token.ID].String()}, {TokenIn: string(token1.Token.ID), Amount: amountByToken[token1.Token.ID].String()}},
		ProbeInput:  []*big.Int{new(big.Int).Set(amountByToken[token0.Token.ID]), new(big.Int).Set(amountByToken[token1.Token.ID])},
	}, nil
}

func (c Config) ResolveEndpoints(lookup configuration.LookupEnv) (Endpoints, error) {
	if lookup == nil {
		return Endpoints{}, fmt.Errorf("environment lookup is required")
	}
	value, ok := lookup(c.Network.RPCURLEnv)
	if !ok || strings.TrimSpace(value) == "" {
		return Endpoints{}, fmt.Errorf("required RPC endpoint for market network is unset")
	}
	return Endpoints{HTTP: value, WS: value}, nil
}
