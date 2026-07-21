package strategy

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

// quoteCache retains only the latest state for each market. A live stream can
// produce unbounded snapshot versions, so retaining historical curves would
// turn a performance optimization into a memory leak.
type quoteCache struct {
	mu     sync.Mutex
	states map[market.MarketID]cachedMarketQuotes
}

type cachedMarketQuotes struct {
	stateHash [32]byte
	quotes    map[quoteCacheKey]market.Quote
}

type quoteCacheKey struct {
	source   market.SourceID
	market   market.MarketID
	mode     market.QuoteMode
	tokenIn  market.TokenID
	tokenOut market.TokenID
	amount   string
	purpose  market.QuotePurpose
}

func newQuoteCache() quoteCache {
	return quoteCache{states: make(map[market.MarketID]cachedMarketQuotes)}
}

func (c *quoteCache) getOrCompute(
	ctx context.Context,
	snapshot market.MarketSnapshot,
	source quoteport.Source,
	mode market.QuoteMode,
	tokenIn, tokenOut market.TokenID,
	amount market.TokenAmount,
	purpose market.QuotePurpose,
	quotedAt time.Time,
	compute func() (market.Quote, error),
) (market.Quote, error) {
	if source == nil || compute == nil {
		return market.Quote{}, fmt.Errorf("quote source and computation are required")
	}
	if err := ctx.Err(); err != nil {
		return market.Quote{}, err
	}
	metadata := snapshot.Metadata()
	key := quoteCacheKey{
		source: source.ID(), market: metadata.Market, mode: mode,
		tokenIn: tokenIn, tokenOut: tokenOut, amount: amount.String(), purpose: purpose,
	}

	// The strategy evaluates a stream serially. Holding this small lock during
	// a miss also prevents concurrent evaluations from duplicating expensive
	// V3 tick traversals for the same immutable snapshot and amount.
	c.mu.Lock()
	defer c.mu.Unlock()
	state, ok := c.states[metadata.Market]
	if !ok || state.stateHash != metadata.StateHash {
		state = cachedMarketQuotes{stateHash: metadata.StateHash, quotes: make(map[quoteCacheKey]market.Quote)}
		c.states[metadata.Market] = state
	}
	if cached, ok := state.quotes[key]; ok {
		return rebind(cached, snapshot, quotedAt)
	}
	quote, err := compute()
	if err != nil {
		return market.Quote{}, err
	}
	state.quotes[key] = quote
	return quote, nil
}

func rebind(cached market.Quote, snapshot market.MarketSnapshot, quotedAt time.Time) (market.Quote, error) {
	metadata := snapshot.Metadata()
	return market.NewQuote(market.Quote{
		Source: cached.Source, Market: cached.Market, SnapshotVersion: metadata.Version,
		SnapshotHash: metadata.StateHash, Purpose: cached.Purpose, Mode: cached.Mode,
		AmountIn: cached.AmountIn, AmountOut: cached.AmountOut, QuotedAt: quotedAt,
	}, cached.Fees()...)
}
