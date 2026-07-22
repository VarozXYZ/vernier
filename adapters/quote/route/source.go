// Package route composes protocol-neutral hop quoters into one market quote
// source. The caller provides the configured hop order; the source can quote
// that path in either direction without leaking DEX-specific types.
package route

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type Hop struct {
	Market market.MarketID
	In     market.TokenID
	Out    market.TokenID
	Source quoteport.Source
}

type Source struct {
	id         market.SourceID
	market     market.Market
	hops       []Hop
	mu         sync.RWMutex
	hopCache   map[hopKey]market.Quote
	routeCache map[routeKey]routeCacheEntry
	timingMu   sync.RWMutex
	lastTiming quoteport.Timing
}

type hopKey struct {
	market  market.MarketID
	version uint64
	hash    [32]byte
	in, out market.TokenID
	amount  string
}
type routeKey struct {
	version uint64
	hash    [32]byte
	in, out market.TokenID
	amount  string
	mode    market.QuoteMode
}

type routeCacheEntry struct {
	quote market.Quote
	hops  []quoteport.HopTiming
}

func New(id market.SourceID, candidate market.Market, hops []Hop) (*Source, error) {
	if id == "" || candidate.ID == "" || len(hops) == 0 {
		return nil, fmt.Errorf("route source, market, and hops are required")
	}
	previous := candidate.BaseToken
	for index, hop := range hops {
		if hop.Market == "" || hop.In == "" || hop.Out == "" || hop.In == hop.Out || hop.Source == nil {
			return nil, fmt.Errorf("route hop %d is incomplete", index)
		}
		if hop.In != previous && index == 0 {
			return nil, fmt.Errorf("route first hop input does not match market base token")
		}
		if index > 0 && hop.In != hops[index-1].Out {
			return nil, fmt.Errorf("route hops are discontinuous at %d", index)
		}
		previous = hop.Out
	}
	if previous != candidate.QuoteToken {
		return nil, fmt.Errorf("route final hop output does not match market quote token")
	}
	return &Source{id: id, market: candidate, hops: append([]Hop(nil), hops...), hopCache: make(map[hopKey]market.Quote), routeCache: make(map[routeKey]routeCacheEntry)}, nil
}

func (s *Source) ID() market.SourceID { return s.id }

func (s *Source) LastTiming() quoteport.Timing {
	s.timingMu.RLock()
	defer s.timingMu.RUnlock()
	result := s.lastTiming
	result.Hops = append([]quoteport.HopTiming(nil), result.Hops...)
	return result
}

func (s *Source) setTiming(value quoteport.Timing) {
	s.timingMu.Lock()
	s.lastTiming = value
	s.timingMu.Unlock()
}

func (s *Source) Quote(ctx context.Context, input quoteport.Input) (market.Quote, error) {
	started := time.Now()
	timing := quoteport.Timing{}
	defer func() {
		timing.Duration = time.Since(started)
		s.setTiming(timing)
	}()
	if err := ctx.Err(); err != nil {
		return market.Quote{}, err
	}
	if input.Snapshot.Metadata().Market != s.market.ID {
		return market.Quote{}, fmt.Errorf("snapshot belongs to market %q, expected %q", input.Snapshot.Metadata().Market, s.market.ID)
	}
	reverse, err := s.reverseFor(input.TokenIn, input.TokenOut)
	if err != nil {
		return market.Quote{}, err
	}
	bundle, ok := input.Snapshot.Data().(market.SnapshotBundle)
	if !ok {
		return market.Quote{}, fmt.Errorf("route quote requires a snapshot bundle")
	}
	routeKey := routeKey{version: input.Snapshot.Metadata().Version, hash: input.Snapshot.Metadata().StateHash, in: input.TokenIn, out: input.TokenOut, amount: input.AmountIn.String(), mode: market.QuoteModeExactInput}
	s.mu.RLock()
	cached, found := s.routeCache[routeKey]
	s.mu.RUnlock()
	if found {
		timing.Cached = true
		timing.Hops = append([]quoteport.HopTiming(nil), cached.hops...)
		for index := range timing.Hops {
			timing.Hops[index].Cached = true
			timing.Hops[index].Duration = 0
		}
		return cached.quote, nil
	}
	current := input.AmountIn
	var first market.Quote
	for index := range s.hops {
		hop := s.hopAt(index, reverse)
		snapshot, ok := child(bundle, hop.Market)
		if !ok {
			return market.Quote{}, fmt.Errorf("route snapshot is missing hop %q", hop.Market)
		}
		if current.Token() != hop.In {
			return market.Quote{}, fmt.Errorf("route amount token mismatch at hop %d", index)
		}
		hopKey := hopKey{market: hop.Market, version: snapshot.Metadata().Version, hash: snapshot.Metadata().StateHash, in: hop.In, out: hop.Out, amount: current.String()}
		s.mu.RLock()
		result, cachedHop := s.hopCache[hopKey]
		s.mu.RUnlock()
		var err error
		hopStarted := time.Now()
		if !cachedHop {
			result, err = hop.Source.Quote(ctx, quoteport.Input{Snapshot: snapshot, TokenIn: hop.In, TokenOut: hop.Out, AmountIn: current, Purpose: input.Purpose, QuotedAt: input.QuotedAt})
			if err == nil {
				s.mu.Lock()
				s.hopCache[hopKey] = result
				s.mu.Unlock()
			}
		}
		if err != nil {
			return market.Quote{}, fmt.Errorf("quote route hop %d: %w", index, err)
		}
		timing.Hops = append(timing.Hops, quoteport.HopTiming{Market: hop.Market, Duration: time.Since(hopStarted), Cached: cachedHop, AmountIn: result.AmountIn.Units().String(), AmountOut: result.AmountOut.Units().String()})
		if index == 0 {
			first = result
		}
		current = result.AmountOut
	}
	result, err := market.NewQuote(market.Quote{Source: s.id, Market: s.market.ID, SnapshotVersion: input.Snapshot.Metadata().Version, SnapshotHash: input.Snapshot.Metadata().StateHash, Purpose: input.Purpose, Mode: market.QuoteModeExactInput, AmountIn: input.AmountIn, AmountOut: current, QuotedAt: input.QuotedAt}, first.Fees()...)
	if err == nil {
		s.mu.Lock()
		s.routeCache[routeKey] = routeCacheEntry{quote: result, hops: append([]quoteport.HopTiming(nil), timing.Hops...)}
		s.mu.Unlock()
	}
	return result, err
}

func (s *Source) QuoteExactOutput(ctx context.Context, input quoteport.ExactOutputInput) (market.Quote, error) {
	started := time.Now()
	timing := quoteport.Timing{}
	defer func() {
		timing.Duration = time.Since(started)
		s.setTiming(timing)
	}()
	if err := ctx.Err(); err != nil {
		return market.Quote{}, err
	}
	reverse, err := s.reverseFor(input.TokenIn, input.TokenOut)
	if err != nil {
		return market.Quote{}, err
	}
	bundle, ok := input.Snapshot.Data().(market.SnapshotBundle)
	if !ok {
		return market.Quote{}, fmt.Errorf("route quote requires a snapshot bundle")
	}
	routeKey := routeKey{version: input.Snapshot.Metadata().Version, hash: input.Snapshot.Metadata().StateHash, in: input.TokenIn, out: input.TokenOut, amount: input.AmountOut.String(), mode: market.QuoteModeExactOutput}
	s.mu.RLock()
	cached, found := s.routeCache[routeKey]
	s.mu.RUnlock()
	if found {
		timing.Cached = true
		timing.Hops = append([]quoteport.HopTiming(nil), cached.hops...)
		for index := range timing.Hops {
			timing.Hops[index].Cached = true
			timing.Hops[index].Duration = 0
		}
		return cached.quote, nil
	}
	current := input.AmountOut
	var final market.Quote
	for index := len(s.hops) - 1; index >= 0; index-- {
		hop := s.hopAt(index, reverse)
		source, ok := hop.Source.(quoteport.ExactOutputSource)
		if !ok {
			return market.Quote{}, fmt.Errorf("route hop %d does not support exact output", index)
		}
		snapshot, ok := child(bundle, hop.Market)
		if !ok {
			return market.Quote{}, fmt.Errorf("route snapshot is missing hop %q", hop.Market)
		}
		hopKey := hopKey{market: hop.Market, version: snapshot.Metadata().Version, hash: snapshot.Metadata().StateHash, in: hop.In, out: hop.Out, amount: current.String()}
		s.mu.RLock()
		result, cachedHop := s.hopCache[hopKey]
		s.mu.RUnlock()
		var err error
		hopStarted := time.Now()
		if !cachedHop {
			result, err = source.QuoteExactOutput(ctx, quoteport.ExactOutputInput{Snapshot: snapshot, TokenIn: hop.In, TokenOut: hop.Out, AmountOut: current, Purpose: input.Purpose, QuotedAt: input.QuotedAt})
			if err == nil {
				s.mu.Lock()
				s.hopCache[hopKey] = result
				s.mu.Unlock()
			}
		}
		if err != nil {
			return market.Quote{}, fmt.Errorf("quote route hop %d: %w", index, err)
		}
		timing.Hops = append(timing.Hops, quoteport.HopTiming{Market: hop.Market, Duration: time.Since(hopStarted), Cached: cachedHop, AmountIn: result.AmountIn.Units().String(), AmountOut: result.AmountOut.Units().String()})
		if index == len(s.hops)-1 {
			final = result
		}
		current = result.AmountIn
	}
	result, err := market.NewQuote(market.Quote{Source: s.id, Market: s.market.ID, SnapshotVersion: input.Snapshot.Metadata().Version, SnapshotHash: input.Snapshot.Metadata().StateHash, Purpose: input.Purpose, Mode: market.QuoteModeExactOutput, AmountIn: current, AmountOut: input.AmountOut, QuotedAt: input.QuotedAt}, final.Fees()...)
	if err == nil {
		s.mu.Lock()
		s.routeCache[routeKey] = routeCacheEntry{quote: result, hops: append([]quoteport.HopTiming(nil), timing.Hops...)}
		s.mu.Unlock()
	}
	return result, err
}

func (s *Source) reverseFor(tokenIn, tokenOut market.TokenID) (bool, error) {
	switch {
	case tokenIn == s.market.BaseToken && tokenOut == s.market.QuoteToken:
		return false, nil
	case tokenIn == s.market.QuoteToken && tokenOut == s.market.BaseToken:
		return true, nil
	default:
		return false, fmt.Errorf("route does not support token direction %q -> %q", tokenIn, tokenOut)
	}
}

func (s *Source) hopAt(index int, reverse bool) Hop {
	if !reverse {
		return s.hops[index]
	}
	hop := s.hops[len(s.hops)-1-index]
	return Hop{Market: hop.Market, In: hop.Out, Out: hop.In, Source: hop.Source}
}

func child(bundle market.SnapshotBundle, id market.MarketID) (market.MarketSnapshot, bool) {
	for _, snapshot := range bundle.Snapshots() {
		if snapshot.Metadata().Market == id {
			return snapshot, true
		}
	}
	return market.MarketSnapshot{}, false
}

var _ quoteport.Source = (*Source)(nil)
var _ quoteport.ExactOutputSource = (*Source)(nil)
var _ quoteport.TimingSource = (*Source)(nil)
