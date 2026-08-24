package crosschain

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	localexecution "github.com/VarozXYZ/vernier/adapters/execution/local"
	"github.com/VarozXYZ/vernier/core/marketstate"
	"github.com/VarozXYZ/vernier/core/sizing"
	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

// SplitLeg is one fixed pool curve in the canonical base->quote direction.
// First is base->intermediate and Second is intermediate->quote; the source
// reverses and swaps those stages automatically for quote->base requests.
type SplitLeg struct {
	Market market.Market
	Source quoteport.Source
}

type SplitRouteConfig struct {
	Candidate    market.Market
	Source       market.SourceID
	Intermediate market.TokenID
	Direct       []SplitLeg
	First        []SplitLeg
	Second       []SplitLeg
	Mirrors      []feedport.Mirror
	Clock        marketstate.Clock
	MaxSweeps    int
	MaxStarts    int
	Neighborhood int
}

type SplitRoute struct {
	Market market.Market
	Mirror *marketstate.RouteMirror
	Source *SplitSource
}

type SplitSource struct {
	id           market.SourceID
	market       market.Market
	intermediate market.TokenID
	direct       []SplitLeg
	first        []SplitLeg
	second       []SplitLeg
	maxSweeps    int
	maxStarts    int
	neighborhood int
	cacheMu      sync.Mutex
	cacheVersion uint64
	cacheHash    [32]byte
	cache        map[string]localexecution.ExecutableQuote
	curveCaches  map[market.MarketID]*splitCurveCache
}

type splitCurveCache struct {
	mu      sync.Mutex
	version uint64
	hash    [32]byte
	values  map[string]*big.Int
}

func NewSplitRoute(config SplitRouteConfig) (*SplitRoute, error) {
	if config.Candidate.ID == "" || config.Source == "" || config.Intermediate == "" ||
		config.Intermediate == config.Candidate.BaseToken ||
		config.Intermediate == config.Candidate.QuoteToken || config.Clock == nil ||
		len(config.Mirrors) == 0 || len(config.Direct) == 0 ||
		len(config.First) == 0 || len(config.Second) == 0 {
		return nil, fmt.Errorf("split route configuration is incomplete")
	}
	if err := validateSplitLegs(config); err != nil {
		return nil, err
	}
	mirror, err := marketstate.NewRouteMirror(config.Candidate.ID, config.Source, config.Mirrors, config.Clock)
	if err != nil {
		return nil, err
	}
	source := &SplitSource{
		id: config.Source + "/local", market: config.Candidate, intermediate: config.Intermediate,
		direct: append([]SplitLeg(nil), config.Direct...), first: append([]SplitLeg(nil), config.First...),
		second: append([]SplitLeg(nil), config.Second...), maxSweeps: config.MaxSweeps,
		maxStarts: config.MaxStarts, neighborhood: config.Neighborhood,
		cache: make(map[string]localexecution.ExecutableQuote), curveCaches: make(map[market.MarketID]*splitCurveCache),
	}
	for _, leg := range append(append(append([]SplitLeg(nil), config.Direct...), config.First...), config.Second...) {
		source.curveCaches[leg.Market.ID] = &splitCurveCache{values: make(map[string]*big.Int)}
	}
	return &SplitRoute{Market: config.Candidate, Mirror: mirror, Source: source}, nil
}

func validateSplitLegs(config SplitRouteConfig) error {
	check := func(label string, legs []SplitLeg, in, out market.TokenID) error {
		for index, leg := range legs {
			if leg.Market.ID == "" || leg.Source == nil ||
				leg.Market.BaseToken != in || leg.Market.QuoteToken != out {
				return fmt.Errorf("split route %s leg %d has incompatible endpoints", label, index)
			}
		}
		return nil
	}
	if err := check("direct", config.Direct, config.Candidate.BaseToken, config.Candidate.QuoteToken); err != nil {
		return err
	}
	if err := check("first", config.First, config.Candidate.BaseToken, config.Intermediate); err != nil {
		return err
	}
	return check("second", config.Second, config.Intermediate, config.Candidate.QuoteToken)
}

func (s *SplitSource) ID() market.SourceID { return s.id }

func (s *SplitSource) Quote(ctx context.Context, input quoteport.Input) (market.Quote, error) {
	executable, err := s.QuoteExecutable(ctx, input)
	return executable.Quote, err
}

func (s *SplitSource) QuoteExecutable(ctx context.Context,
	input quoteport.Input) (localexecution.ExecutableQuote, error) {
	if err := ctx.Err(); err != nil {
		return localexecution.ExecutableQuote{}, err
	}
	if input.Snapshot.Metadata().Market != s.market.ID ||
		input.Snapshot.Metadata().Health != market.HealthHealthy || input.AmountIn.IsZero() {
		return localexecution.ExecutableQuote{}, fmt.Errorf("split quote requires a healthy matching snapshot")
	}
	bundle, ok := input.Snapshot.Data().(market.SnapshotBundle)
	if !ok {
		return localexecution.ExecutableQuote{}, fmt.Errorf("split quote requires a snapshot bundle")
	}
	reverse := false
	switch {
	case input.TokenIn == s.market.BaseToken && input.TokenOut == s.market.QuoteToken:
	case input.TokenIn == s.market.QuoteToken && input.TokenOut == s.market.BaseToken:
		reverse = true
	default:
		return localexecution.ExecutableQuote{}, fmt.Errorf("split quote does not support token direction")
	}
	if cached, ok := s.cached(input); ok {
		return cached, nil
	}

	direct := s.splitCurves(bundle, s.direct, reverse)
	firstLegs, secondLegs := s.first, s.second
	if reverse {
		firstLegs, secondLegs = s.second, s.first
	}
	first := s.splitCurves(bundle, firstLegs, reverse)
	second := s.splitCurves(bundle, secondLegs, reverse)
	result, err := sizing.OptimizeTwoStageSplit(ctx, sizing.TwoStageSplitRequest{
		TotalInput: input.AmountIn.Units(), Direct: direct, FirstStage: first, SecondStage: second,
		MaxSweeps: s.maxSweeps, MaxStarts: s.maxStarts, Neighborhood: s.neighborhood,
	})
	if err != nil {
		return localexecution.ExecutableQuote{}, err
	}
	intermediate := s.intermediate
	allocation, err := sizing.BuildRouteAllocation(
		result, input.TokenIn, intermediate, input.TokenOut,
	)
	if err != nil {
		return localexecution.ExecutableQuote{}, err
	}
	output, err := market.NewTokenAmount(input.TokenOut, result.TotalOutput)
	if err != nil {
		return localexecution.ExecutableQuote{}, err
	}
	quote, err := market.NewQuote(market.Quote{
		Source: s.id, Market: s.market.ID, SnapshotVersion: input.Snapshot.Metadata().Version,
		SnapshotHash: input.Snapshot.Metadata().StateHash, Purpose: input.Purpose,
		Mode: market.QuoteModeExactInput, AmountIn: input.AmountIn, AmountOut: output,
		QuotedAt: normalizedQuoteTime(input.QuotedAt),
	})
	if err != nil {
		return localexecution.ExecutableQuote{}, err
	}
	executable := localexecution.ExecutableQuote{Quote: quote, Allocation: allocation}
	s.remember(input, executable)
	return executable, nil
}

func (s *SplitSource) cached(input quoteport.Input) (localexecution.ExecutableQuote, bool) {
	metadata := input.Snapshot.Metadata()
	key := splitCacheKey(input)
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	if metadata.Version != s.cacheVersion || metadata.StateHash != s.cacheHash {
		return localexecution.ExecutableQuote{}, false
	}
	cached, ok := s.cache[key]
	if !ok {
		return localexecution.ExecutableQuote{}, false
	}
	quote := cached.Quote
	quote.QuotedAt = normalizedQuoteTime(input.QuotedAt)
	quote.Purpose = input.Purpose
	quote.Quality = market.QuoteQualityCachedExact
	return localexecution.ExecutableQuote{Quote: quote, Allocation: cached.Allocation.Clone()}, true
}

func (s *SplitSource) remember(input quoteport.Input, result localexecution.ExecutableQuote) {
	metadata := input.Snapshot.Metadata()
	key := splitCacheKey(input)
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	if metadata.Version < s.cacheVersion {
		return
	}
	if metadata.Version != s.cacheVersion || metadata.StateHash != s.cacheHash {
		s.cacheVersion = metadata.Version
		s.cacheHash = metadata.StateHash
		clear(s.cache)
	}
	s.cache[key] = localexecution.ExecutableQuote{Quote: result.Quote, Allocation: result.Allocation.Clone()}
}

func splitCacheKey(input quoteport.Input) string {
	return string(input.TokenIn) + "\x00" + string(input.TokenOut) + "\x00" + input.AmountIn.Units().String()
}

func (s *SplitSource) splitCurves(bundle market.SnapshotBundle, legs []SplitLeg, reverse bool) []sizing.SplitCurve {
	curves := make([]sizing.SplitCurve, 0, len(legs))
	for _, configured := range legs {
		leg := configured
		snapshot, found := splitChild(bundle, leg.Market.ID)
		curves = append(curves, sizing.SplitCurve{ID: string(leg.Market.ID), Quote: func(ctx context.Context, units *big.Int) (*big.Int, error) {
			if !found {
				return nil, fmt.Errorf("split snapshot is missing pool %q", leg.Market.ID)
			}
			in, out := leg.Market.BaseToken, leg.Market.QuoteToken
			if reverse {
				in, out = out, in
			}
			return s.quoteCurve(ctx, leg, snapshot, in, out, units)
		}})
	}
	return curves
}

func (s *SplitSource) quoteCurve(ctx context.Context, leg SplitLeg, snapshot market.MarketSnapshot,
	in, out market.TokenID, units *big.Int) (*big.Int, error) {
	cache := s.curveCaches[leg.Market.ID]
	metadata := snapshot.Metadata()
	key := string(in) + "\x00" + string(out) + "\x00" + units.String()
	if cache != nil {
		cache.mu.Lock()
		if cache.version == metadata.Version && cache.hash == metadata.StateHash {
			if value, ok := cache.values[key]; ok {
				result := new(big.Int).Set(value)
				cache.mu.Unlock()
				return result, nil
			}
		}
		cache.mu.Unlock()
	}
	amount, err := market.NewTokenAmount(in, units)
	if err != nil {
		return nil, err
	}
	quote, err := leg.Source.Quote(ctx, quoteport.Input{Snapshot: snapshot, TokenIn: in, TokenOut: out,
		AmountIn: amount, Purpose: market.QuotePurposeLiveValidation, QuotedAt: time.Now().UTC()})
	if err != nil {
		return nil, err
	}
	output := quote.AmountOut.Units()
	if cache != nil {
		cache.mu.Lock()
		if metadata.Version >= cache.version {
			if metadata.Version != cache.version || metadata.StateHash != cache.hash {
				cache.version, cache.hash = metadata.Version, metadata.StateHash
				clear(cache.values)
			}
			cache.values[key] = new(big.Int).Set(output)
		}
		cache.mu.Unlock()
	}
	return output, nil
}

func splitChild(bundle market.SnapshotBundle, id market.MarketID) (market.MarketSnapshot, bool) {
	for _, snapshot := range bundle.Snapshots() {
		if snapshot.Metadata().Market == id {
			return snapshot, true
		}
	}
	return market.MarketSnapshot{}, false
}

func normalizedQuoteTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Now().UTC()
	}
	return value.UTC()
}

func (r *SplitRoute) Apply(ctx context.Context, event market.MarketEvent) (feedport.ApplyResult, error) {
	return r.Mirror.Apply(ctx, event)
}

func (r *SplitRoute) Reset(ctx context.Context, event market.MarketEvent) (feedport.ApplyResult, error) {
	return r.Mirror.Reset(ctx, event)
}

func (r *SplitRoute) ApplyBatch(ctx context.Context, events []market.MarketEvent) (feedport.ApplyResult, error) {
	return r.Mirror.ApplyBatch(ctx, events)
}

func (r *SplitRoute) SetChildHealth(ctx context.Context, child market.MarketID, update feedport.HealthUpdate) error {
	return r.Mirror.SetChildHealth(ctx, child, update)
}

func (r *SplitRoute) Snapshot() (market.MarketSnapshot, bool) { return r.Mirror.Current() }

var _ quoteport.Source = (*SplitSource)(nil)
var _ localexecution.ExecutableSource = (*SplitSource)(nil)
