package jupiter

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

const ContextSlotPositionKind market.SourcePositionKind = "jupiter_context_slot"

const RateLimitFallbackMaxAge = 250 * time.Millisecond

// MarketSource adapts the direct Jupiter quote endpoint to the protocol-
// neutral Research quote contract. Its snapshot is causal evidence for one
// event-triggered quote generation; it is not local Solana pool state.
type MarketSource struct {
	id        market.SourceID
	market    market.Market
	mints     map[market.TokenID]string
	client    *QuoteSource
	clock     Clock
	freshOnly bool

	mu             sync.RWMutex
	timing         quoteport.Timing
	cache          map[marketQuoteCacheKey]marketQuoteCacheEntry
	rateLimitRetry bool
	warnings       []quoteport.OperationalWarning
}

type MarketSourceConfig struct {
	ID         market.SourceID
	Market     market.Market
	TokenMints map[market.TokenID]string
	Client     *QuoteSource
	Clock      Clock
	// FreshOnly disables generation and rate-limit fallback caches. It is used
	// by Research setups whose every evaluation must hit the provider.
	FreshOnly bool
}

type marketQuoteCacheKey struct {
	tokenIn  market.TokenID
	tokenOut market.TokenID
	purpose  market.QuotePurpose
}

type marketQuoteCacheEntry struct {
	quote     market.Quote
	fetchedAt time.Time
}

func NewMarketSource(config MarketSourceConfig) (*MarketSource, error) {
	if config.ID == "" || config.Market.ID == "" || config.Client == nil {
		return nil, fmt.Errorf("jupiter market source requires id, market, and direct quote client")
	}
	mints := make(map[market.TokenID]string, len(config.TokenMints))
	for token, mint := range config.TokenMints {
		mint = strings.TrimSpace(mint)
		if token == "" || mint == "" {
			return nil, fmt.Errorf("jupiter market source token mapping is incomplete")
		}
		mints[token] = mint
	}
	for _, token := range []market.TokenID{config.Market.BaseToken, config.Market.QuoteToken} {
		if _, ok := mints[token]; !ok {
			return nil, fmt.Errorf("jupiter market source is missing token %q", token)
		}
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	return &MarketSource{
		id: config.ID, market: config.Market, mints: mints, client: config.Client, clock: config.Clock, freshOnly: config.FreshOnly,
		cache: make(map[marketQuoteCacheKey]marketQuoteCacheEntry),
	}, nil
}

func (s *MarketSource) ID() market.SourceID { return s.id }

// CacheQuotes is false because MarketSource owns generation-aware caching and
// proportional estimation itself. The generic Strategy cache requires an
// exact amount match and cannot distinguish a Solana generation from an event
// on another chain.
func (*MarketSource) CacheQuotes() bool { return false }

func (s *MarketSource) Quote(ctx context.Context, input quoteport.Input) (market.Quote, error) {
	return s.quote(ctx, input, false)
}

// QuoteFresh bypasses generation caching. It is used to confirm an apparently
// profitable cached result and never accepts a rate-limit fallback as proof.
func (s *MarketSource) QuoteFresh(ctx context.Context, input quoteport.Input) (market.Quote, error) {
	return s.quote(ctx, input, true)
}

func (s *MarketSource) quote(ctx context.Context, input quoteport.Input, forceFresh bool) (market.Quote, error) {
	s.mu.Lock()
	s.timing = quoteport.Timing{}
	s.mu.Unlock()
	metadata := input.Snapshot.Metadata()
	if metadata.Market != s.market.ID {
		return market.Quote{}, fmt.Errorf("snapshot belongs to market %q, expected %q", metadata.Market, s.market.ID)
	}
	if input.AmountIn.Token() != input.TokenIn || input.AmountIn.IsZero() {
		return market.Quote{}, fmt.Errorf("jupiter quote input amount does not match the requested token")
	}
	inputMint, inputOK := s.mints[input.TokenIn]
	outputMint, outputOK := s.mints[input.TokenOut]
	if !inputOK || !outputOK || input.TokenIn == input.TokenOut {
		return market.Quote{}, fmt.Errorf("jupiter quote requires configured distinct input/output tokens")
	}
	if !forceFresh && !s.freshOnly {
		if cached, ok := s.generationCache(input); ok {
			return cached, nil
		}
	}
	result, err := s.client.Quote(ctx, QuoteRequest{
		InputMint: inputMint, OutputMint: outputMint, Amount: input.AmountIn.Units().String(),
	})
	s.mu.Lock()
	s.timing = quoteport.Timing{Duration: result.TotalDuration}
	s.mu.Unlock()
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.RateLimited() && !s.freshOnly {
			if !forceFresh {
				if cached, ok := s.rateLimitFallback(input); ok {
					return cached, nil
				}
			}
			s.mu.Lock()
			s.rateLimitRetry = true
			s.mu.Unlock()
		}
		return market.Quote{}, err
	}
	if result.ModeMismatch {
		details := make(map[string]string, 1)
		if router := strings.TrimSpace(result.Router); router != "" {
			details["router"] = router
		}
		s.mu.Lock()
		s.warnings = append(s.warnings, quoteport.OperationalWarning{
			Code:       "jupiter_order_mode_mismatch",
			Provider:   s.id,
			Market:     s.market.ID,
			Expected:   result.ExpectedMode,
			Observed:   strings.ToLower(strings.TrimSpace(result.Mode)),
			Details:    details,
			ObservedAt: s.clock().UTC(),
		})
		s.mu.Unlock()
	}
	if result.InputMint != inputMint || result.OutputMint != outputMint ||
		result.InTokenAmount != input.AmountIn.Units().String() {
		return market.Quote{}, fmt.Errorf("jupiter quote response does not match the requested input")
	}
	outputUnits, ok := new(big.Int).SetString(result.ToTokenAmount, 10)
	if !ok || outputUnits.Sign() <= 0 {
		return market.Quote{}, fmt.Errorf("jupiter quote output is not a positive integer")
	}
	output, err := market.NewTokenAmount(input.TokenOut, outputUnits)
	if err != nil {
		return market.Quote{}, err
	}
	position := metadata.EventPosition
	if result.ContextSlot > 0 {
		position = market.SourcePosition{Kind: ContextSlotPositionKind, Value: result.ContextSlot}
	}
	quote, err := market.NewQuote(market.Quote{
		Source: s.id, Market: s.market.ID, SnapshotVersion: metadata.Version,
		SnapshotHash:   metadata.StateHash,
		SourcePosition: position,
		ResponseHash:   sha256.Sum256(result.RawResponse),
		Purpose:        input.Purpose, Mode: market.QuoteModeExactInput,
		Quality:  market.QuoteQualityExact,
		AmountIn: input.AmountIn, AmountOut: output, QuotedAt: input.QuotedAt,
	})
	if err != nil {
		return market.Quote{}, err
	}
	if !s.freshOnly {
		key := marketQuoteCacheKey{tokenIn: input.TokenIn, tokenOut: input.TokenOut, purpose: input.Purpose}
		s.mu.Lock()
		s.cache[key] = marketQuoteCacheEntry{quote: quote, fetchedAt: s.clock().UTC()}
		s.mu.Unlock()
	}
	return quote, nil
}

func (s *MarketSource) generationCache(input quoteport.Input) (market.Quote, bool) {
	key := marketQuoteCacheKey{tokenIn: input.TokenIn, tokenOut: input.TokenOut, purpose: input.Purpose}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.cache[key]
	if !ok || entry.quote.SnapshotHash != input.Snapshot.Metadata().StateHash {
		return market.Quote{}, false
	}
	rebound, ok := cachedQuote(input, entry.quote, true)
	if !ok {
		return market.Quote{}, false
	}
	s.timing = quoteport.Timing{Cached: true}
	return rebound, true
}

func (s *MarketSource) LastTiming() quoteport.Timing {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.timing
}

func (s *MarketSource) TakeOperationalWarnings() []quoteport.OperationalWarning {
	s.mu.Lock()
	defer s.mu.Unlock()
	warnings := make([]quoteport.OperationalWarning, len(s.warnings))
	for index, warning := range s.warnings {
		warnings[index] = warning
		if warning.Details != nil {
			warnings[index].Details = make(map[string]string, len(warning.Details))
			for key, value := range warning.Details {
				warnings[index].Details[key] = value
			}
		}
	}
	s.warnings = nil
	return warnings
}

// TakeRateLimitRetry reports and clears whether a 429 could not use the
// bounded fallback cache. The event stream uses this signal to perform one
// coalesced retry against its newest snapshots.
func (s *MarketSource) TakeRateLimitRetry() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	required := s.rateLimitRetry
	s.rateLimitRetry = false
	return required
}

func (s *MarketSource) rateLimitFallback(input quoteport.Input) (market.Quote, bool) {
	key := marketQuoteCacheKey{tokenIn: input.TokenIn, tokenOut: input.TokenOut, purpose: input.Purpose}
	now := s.clock().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.cache[key]
	if !ok || entry.quote.AmountIn.Token() != input.AmountIn.Token() ||
		entry.quote.AmountIn.Units().Cmp(input.AmountIn.Units()) != 0 {
		return market.Quote{}, false
	}
	age := now.Sub(entry.fetchedAt)
	if age < 0 || age > RateLimitFallbackMaxAge {
		return market.Quote{}, false
	}
	rebound, ok := cachedQuote(input, entry.quote, false)
	if !ok {
		return market.Quote{}, false
	}
	s.timing.Cached = true
	return rebound, true
}

func cachedQuote(input quoteport.Input, cached market.Quote, allowProportional bool) (market.Quote, bool) {
	if cached.AmountIn.Token() != input.AmountIn.Token() || cached.AmountOut.IsZero() {
		return market.Quote{}, false
	}
	output := cached.AmountOut
	quality := market.QuoteQualityCachedExact
	if cached.AmountIn.Units().Cmp(input.AmountIn.Units()) != 0 {
		if !allowProportional || cached.AmountIn.IsZero() {
			return market.Quote{}, false
		}
		scaled := new(big.Int).Mul(cached.AmountOut.Units(), input.AmountIn.Units())
		scaled.Quo(scaled, cached.AmountIn.Units())
		if scaled.Sign() <= 0 {
			return market.Quote{}, false
		}
		var err error
		output, err = market.NewTokenAmount(input.TokenOut, scaled)
		if err != nil {
			return market.Quote{}, false
		}
		quality = market.QuoteQualityProportionalEstimate
	}
	metadata := input.Snapshot.Metadata()
	rebound, err := market.NewQuote(market.Quote{
		Source: cached.Source, Market: cached.Market,
		SnapshotVersion: metadata.Version, SnapshotHash: metadata.StateHash,
		SourcePosition: cached.SourcePosition, ResponseHash: cached.ResponseHash,
		Purpose: input.Purpose, Mode: cached.Mode, Quality: quality,
		AmountIn: input.AmountIn, AmountOut: output, QuotedAt: input.QuotedAt,
	}, cached.Fees()...)
	return rebound, err == nil
}

var _ quoteport.TimingSource = (*MarketSource)(nil)
var _ quoteport.CachePolicy = (*MarketSource)(nil)
var _ quoteport.FreshSource = (*MarketSource)(nil)
var _ quoteport.WarningSource = (*MarketSource)(nil)
