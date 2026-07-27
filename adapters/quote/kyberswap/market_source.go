package kyberswap

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/big"
	"strings"
	"sync"

	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

// MarketSource adapts KyberSwap's exact-input route endpoint to Research's
// provider-neutral quote contract. It deliberately performs a real request
// for every Quote call.
type MarketSource struct {
	id        market.SourceID
	market    market.Market
	addresses map[market.TokenID]string
	chain     string
	origin    string
	client    *Source

	mu     sync.RWMutex
	timing quoteport.Timing
}

type MarketSourceConfig struct {
	ID             market.SourceID
	Market         market.Market
	TokenAddresses map[market.TokenID]string
	Chain          string
	Origin         string
	Client         *Source
}

func NewMarketSource(config MarketSourceConfig) (*MarketSource, error) {
	if config.ID == "" || config.Market.ID == "" || config.Client == nil || strings.TrimSpace(config.Chain) == "" {
		return nil, fmt.Errorf("KyberSwap market source requires id, market, chain, and client")
	}
	addresses := make(map[market.TokenID]string, len(config.TokenAddresses))
	for token, address := range config.TokenAddresses {
		if token == "" || !commonAddress(address) {
			return nil, fmt.Errorf("KyberSwap market source token mapping is invalid")
		}
		addresses[token] = address
	}
	for _, token := range []market.TokenID{config.Market.BaseToken, config.Market.QuoteToken} {
		if _, ok := addresses[token]; !ok {
			return nil, fmt.Errorf("KyberSwap market source is missing token %q", token)
		}
	}
	return &MarketSource{
		id: config.ID, market: config.Market, addresses: addresses,
		chain: strings.TrimSpace(config.Chain), origin: strings.TrimSpace(config.Origin), client: config.Client,
	}, nil
}

func (s *MarketSource) ID() market.SourceID { return s.id }
func (*MarketSource) CacheQuotes() bool     { return false }

func (s *MarketSource) Quote(ctx context.Context, input quoteport.Input) (market.Quote, error) {
	s.mu.Lock()
	s.timing = quoteport.Timing{}
	s.mu.Unlock()
	metadata := input.Snapshot.Metadata()
	if metadata.Market != s.market.ID || input.AmountIn.Token() != input.TokenIn || input.AmountIn.IsZero() {
		return market.Quote{}, fmt.Errorf("KyberSwap quote input does not match market snapshot")
	}
	tokenIn, inputOK := s.addresses[input.TokenIn]
	tokenOut, outputOK := s.addresses[input.TokenOut]
	if !inputOK || !outputOK || input.TokenIn == input.TokenOut {
		return market.Quote{}, fmt.Errorf("KyberSwap quote requires configured distinct tokens")
	}
	result, err := s.client.Route(ctx, RouteRequest{
		Chain: s.chain, TokenIn: tokenIn, TokenOut: tokenOut,
		AmountIn: input.AmountIn.Units().String(), Origin: s.origin,
	})
	s.mu.Lock()
	s.timing = quoteport.Timing{Duration: result.TotalDuration}
	s.mu.Unlock()
	if err != nil {
		return market.Quote{}, err
	}
	outputUnits, ok := new(big.Int).SetString(result.AmountOut, 10)
	if !ok || outputUnits.Sign() <= 0 {
		return market.Quote{}, fmt.Errorf("KyberSwap quote output is not a positive integer")
	}
	output, err := market.NewTokenAmount(input.TokenOut, outputUnits)
	if err != nil {
		return market.Quote{}, err
	}
	return market.NewQuote(market.Quote{
		Source: s.id, Market: s.market.ID, SnapshotVersion: metadata.Version,
		SnapshotHash: metadata.StateHash, SourcePosition: metadata.EventPosition,
		ResponseHash: sha256.Sum256(result.RawResponse), Purpose: input.Purpose,
		Mode: market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
		AmountIn: input.AmountIn, AmountOut: output, QuotedAt: input.QuotedAt,
	})
}

func (s *MarketSource) LastTiming() quoteport.Timing {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.timing
}

func commonAddress(value string) bool {
	return validAddress(strings.TrimSpace(value), false)
}

var _ quoteport.Source = (*MarketSource)(nil)
var _ quoteport.CachePolicy = (*MarketSource)(nil)
var _ quoteport.TimingSource = (*MarketSource)(nil)
