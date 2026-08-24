package kyberswap

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

// MarketSource adapts KyberSwap's exact-input route endpoint to Research's
// provider-neutral quote contract. Live event-refreshed profiles can opt into
// the bounded route cache; ordinary Research callers retain request-per-call
// behavior.
type MarketSource struct {
	id           market.SourceID
	market       market.Market
	addresses    map[market.TokenID]string
	chain        string
	origin       string
	client       *Source
	cacheEnabled bool

	mu            sync.RWMutex
	timing        quoteport.Timing
	routes        map[[32]byte]RouteResult
	order         [][32]byte
	cache         map[marketQuoteCacheKey]marketQuoteCacheEntry
	latest        map[marketQuoteCacheSlot]marketQuoteCacheKey
	refreshCancel context.CancelFunc
}

type marketQuoteCacheSlot struct {
	tokenIn  market.TokenID
	tokenOut market.TokenID
	purpose  market.QuotePurpose
}

type marketQuoteCacheKey struct {
	tokenIn  market.TokenID
	tokenOut market.TokenID
	purpose  market.QuotePurpose
	amount   string
}

type marketQuoteCacheEntry struct {
	quote      market.Quote
	input      quoteport.Input
	receivedAt time.Time
	valid      bool
}

type MarketSourceConfig struct {
	ID             market.SourceID
	Market         market.Market
	TokenAddresses map[market.TokenID]string
	Chain          string
	Origin         string
	Client         *Source
	CacheEnabled   bool
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
		cacheEnabled: config.CacheEnabled,
		routes:       make(map[[32]byte]RouteResult), order: make([][32]byte, 0, 32),
		cache: make(map[marketQuoteCacheKey]marketQuoteCacheEntry), latest: make(map[marketQuoteCacheSlot]marketQuoteCacheKey),
	}, nil
}

func (s *MarketSource) ID() market.SourceID { return s.id }
func (*MarketSource) CacheQuotes() bool     { return false }

func (s *MarketSource) Quote(ctx context.Context, input quoteport.Input) (market.Quote, error) {
	key := quoteCacheKey(input)
	if !s.cacheEnabled {
		return s.quoteRemote(ctx, input)
	}
	s.mu.Lock()
	cached, cachedKey, ok := s.cachedEntryLocked(input)
	if ok && cached.valid {
		if cachedKey != key {
			delete(s.cache, cachedKey)
			s.latest[quoteCacheSlot(input)] = key
		}
		cached.input = input
		s.cache[key] = cached
		s.timing = quoteport.Timing{Cached: true}
		s.mu.Unlock()
		return rebindCachedQuote(input, cached.quote)
	}
	// A local evaluation is never allowed to perform provider I/O. On the
	// first cache miss it only registers the fixed input; the independent
	// refresh worker observes the zero receipt timestamp and warms it.
	s.registerInputLocked(input)
	s.mu.Unlock()
	return market.Quote{}, fmt.Errorf("KyberSwap quote cache unavailable")
}

func (s *MarketSource) cachedEntryLocked(input quoteport.Input) (marketQuoteCacheEntry, marketQuoteCacheKey, bool) {
	key, ok := s.latest[quoteCacheSlot(input)]
	if !ok {
		return marketQuoteCacheEntry{}, marketQuoteCacheKey{}, false
	}
	entry, ok := s.cache[key]
	return entry, key, ok
}

func rebindCachedQuote(input quoteport.Input, cached market.Quote) (market.Quote, error) {
	metadata := input.Snapshot.Metadata()
	output := cached.AmountOut
	quality := market.QuoteQualityExact
	if cached.AmountIn.Units().Cmp(input.AmountIn.Units()) != 0 {
		scaled := new(big.Int).Mul(cached.AmountOut.Units(), input.AmountIn.Units())
		scaled.Quo(scaled, cached.AmountIn.Units())
		var err error
		output, err = market.NewTokenAmount(input.TokenOut, scaled)
		if err != nil {
			return market.Quote{}, err
		}
		quality = market.QuoteQualityProportionalEstimate
	}
	return market.NewQuote(market.Quote{
		Source: cached.Source, Market: cached.Market,
		SnapshotVersion: metadata.Version, SnapshotHash: metadata.StateHash,
		SourcePosition: cached.SourcePosition, ResponseHash: cached.ResponseHash,
		Purpose: input.Purpose, Mode: cached.Mode, Quality: quality,
		AmountIn: input.AmountIn, AmountOut: output, QuotedAt: cached.QuotedAt,
	})
}

// QuoteFresh bypasses the Live cache. It is used only for remote-triggered
// evaluations and for the background refresh loop.
func (s *MarketSource) QuoteFresh(ctx context.Context, input quoteport.Input) (market.Quote, error) {
	if s.cacheEnabled {
		s.mu.Lock()
		s.registerInputLocked(input)
		s.mu.Unlock()
	}
	return s.quoteRemote(ctx, input)
}

func (s *MarketSource) quoteRemote(ctx context.Context, input quoteport.Input) (market.Quote, error) {
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
	responseHash := sha256.Sum256(result.RawResponse)
	quoted, err := market.NewQuote(market.Quote{
		Source: s.id, Market: s.market.ID, SnapshotVersion: metadata.Version,
		SnapshotHash: metadata.StateHash, SourcePosition: metadata.EventPosition,
		ResponseHash: responseHash, Purpose: input.Purpose,
		Mode: market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
		AmountIn: input.AmountIn, AmountOut: output, QuotedAt: input.QuotedAt,
	})
	if err != nil {
		return market.Quote{}, err
	}
	s.rememberRoute(responseHash, result)
	s.mu.Lock()
	key := quoteCacheKey(input)
	if latest, ok := s.latest[quoteCacheSlot(input)]; ok && latest == key {
		s.cache[key] = marketQuoteCacheEntry{quote: quoted, input: input, receivedAt: time.Now().UTC(), valid: true}
	}
	s.mu.Unlock()
	return quoted, nil
}

func quoteCacheKey(input quoteport.Input) marketQuoteCacheKey {
	return marketQuoteCacheKey{tokenIn: input.TokenIn, tokenOut: input.TokenOut, purpose: input.Purpose, amount: input.AmountIn.String()}
}

func quoteCacheSlot(input quoteport.Input) marketQuoteCacheSlot {
	return marketQuoteCacheSlot{tokenIn: input.TokenIn, tokenOut: input.TokenOut, purpose: input.Purpose}
}

func (s *MarketSource) registerInputLocked(input quoteport.Input) {
	key := quoteCacheKey(input)
	slot := quoteCacheSlot(input)
	entry := s.cache[key]
	if previous, ok := s.latest[slot]; ok && previous != key {
		entry = s.cache[previous]
		delete(s.cache, previous)
	}
	s.latest[slot] = key
	entry.input = input
	s.cache[key] = entry
}

// Invalidate marks all cached Kyber quotes unusable after a remote market event.
// The next remote-triggered evaluation must obtain a fresh provider quote.
func (s *MarketSource) Invalidate() {
	if !s.cacheEnabled {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	for key, value := range s.cache {
		value.valid = false
		value.receivedAt = now
		s.cache[key] = value
	}
}

// StartRefresh keeps the most recently requested routes warm. The interval is
// measured from quote receipt; a failed refresh invalidates that route rather
// than serving an erroneous value.
func (s *MarketSource) StartRefresh(ctx context.Context, interval time.Duration) {
	if !s.cacheEnabled {
		return
	}
	if interval <= 0 {
		return
	}
	s.mu.Lock()
	if s.refreshCancel != nil {
		s.refreshCancel()
	}
	refreshCtx, cancel := context.WithCancel(ctx)
	s.refreshCancel = cancel
	s.mu.Unlock()
	go func() {
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-refreshCtx.Done():
				return
			case now := <-ticker.C:
				s.refreshDue(refreshCtx, now.UTC(), interval)
			}
		}
	}()
}

// Refresh requests every known fixed input immediately. It is used by a
// remote-triggered fixed-candidate evaluation after invalidation.
func (s *MarketSource) Refresh(ctx context.Context) error {
	if !s.cacheEnabled {
		return nil
	}
	s.mu.RLock()
	inputs := make([]quoteport.Input, 0, len(s.cache))
	for _, entry := range s.cache {
		inputs = append(inputs, entry.input)
	}
	s.mu.RUnlock()
	if len(inputs) == 0 {
		return fmt.Errorf("KyberSwap quote cache has no inputs to refresh")
	}
	errs := make(chan error, len(inputs))
	for _, input := range inputs {
		input := input
		go func() {
			_, err := s.quoteRemote(ctx, input)
			if err != nil {
				s.markRefreshFailure(input, time.Now().UTC())
			}
			errs <- err
		}()
	}
	var failures []error
	for range inputs {
		if err := <-errs; err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (s *MarketSource) refreshDue(ctx context.Context, now time.Time, interval time.Duration) {
	s.mu.RLock()
	entries := make([]marketQuoteCacheEntry, 0, len(s.cache))
	for _, entry := range s.cache {
		if entry.receivedAt.IsZero() || now.Sub(entry.receivedAt) >= interval {
			entries = append(entries, entry)
		}
	}
	s.mu.RUnlock()
	for _, entry := range entries {
		if _, err := s.quoteRemote(ctx, entry.input); err != nil {
			s.markRefreshFailure(entry.input, now)
		}
	}
}

func (s *MarketSource) markRefreshFailure(input quoteport.Input, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key, ok := s.latest[quoteCacheSlot(input)]
	if !ok {
		return
	}
	cached := s.cache[key]
	cached.valid = false
	cached.receivedAt = at.UTC()
	s.cache[key] = cached
}

// DiscoveryRoute returns the exact provider route that produced quote. The
// bounded in-memory journal lets validation proceed directly to /route/build
// without making a second discovery request.
func (s *MarketSource) DiscoveryRoute(quote market.Quote) (RouteResult, bool) {
	if quote.Source != s.id || quote.Market != s.market.ID || quote.ResponseHash == ([32]byte{}) {
		return RouteResult{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result, ok := s.routes[quote.ResponseHash]
	if !ok || result.AmountIn != quote.AmountIn.String() || result.AmountOut != quote.AmountOut.String() {
		return RouteResult{}, false
	}
	return result, true
}

func (s *MarketSource) rememberRoute(hash [32]byte, result RouteResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.routes[hash]; !exists {
		s.order = append(s.order, hash)
	}
	s.routes[hash] = result
	for len(s.order) > 32 {
		delete(s.routes, s.order[0])
		s.order = s.order[1:]
	}
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
var _ quoteport.FreshSource = (*MarketSource)(nil)
