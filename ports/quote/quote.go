// Package quote defines quote-source contracts consumed by the core.
package quote

import (
	"context"
	"crypto/sha256"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
)

type Input struct {
	Snapshot market.MarketSnapshot
	TokenIn  market.TokenID
	TokenOut market.TokenID
	AmountIn market.TokenAmount
	Purpose  market.QuotePurpose
	QuotedAt time.Time
}

type ExactOutputInput struct {
	Snapshot  market.MarketSnapshot
	TokenIn   market.TokenID
	TokenOut  market.TokenID
	AmountOut market.TokenAmount
	Purpose   market.QuotePurpose
	QuotedAt  time.Time
}

type Source interface {
	ID() market.SourceID
	Quote(context.Context, Input) (market.Quote, error)
}

// OperationalWarning reports a non-fatal provider anomaly observed while
// producing an otherwise usable quote. It never changes quote economics.
type OperationalWarning struct {
	Code       string
	Provider   market.SourceID
	Market     market.MarketID
	Expected   string
	Observed   string
	Details    map[string]string
	ObservedAt time.Time
}

// WarningSource exposes and clears provider anomalies accumulated by a quote
// source. Implementations must be safe when Quote is called concurrently.
type WarningSource interface {
	Source
	TakeOperationalWarnings() []OperationalWarning
}

// FreshSource is implemented by remote sources that can bypass their normal
// event-generation cache. Research uses it only to confirm an apparently
// profitable result before accepting it.
type FreshSource interface {
	Source
	QuoteFresh(context.Context, Input) (market.Quote, error)
}

// CachePolicy lets a source opt out of the strategy quote cache. Remote
// providers whose contract requires a real request for every invocation use
// this capability; local deterministic sources remain cacheable by default.
type CachePolicy interface {
	Source
	CacheQuotes() bool
}

// HopTiming is optional observability for composed sources. It is not part of
// the economic quote and is therefore safe to omit for single-pool sources.
type HopTiming struct {
	Market   market.MarketID
	Duration time.Duration
	Cached   bool
	// Amounts are raw token units. They are observational data only and do
	// not participate in quote economics.
	AmountIn  string
	AmountOut string
}

type Timing struct {
	Duration time.Duration
	Cached   bool
	Hops     []HopTiming
}

// TimingSource exposes the most recent local quote trace. Consumers must read
// it immediately after Quote returns; implementations return a defensive
// copy and remain protocol-neutral.
type TimingSource interface {
	Source
	LastTiming() Timing
}

// ExactOutputSource is an optional local capability. Strategies can fall back
// to deterministic exact-input search when a source does not implement it.
type ExactOutputSource interface {
	Source
	QuoteExactOutput(context.Context, ExactOutputInput) (market.Quote, error)
}

type ReferenceStatus string

const (
	ReferenceAvailable   ReferenceStatus = "available"
	ReferenceUnavailable ReferenceStatus = "unavailable"
)

type ReferenceHop struct {
	AMM        string
	Label      string
	InputMint  string
	OutputMint string
	InAmount   string
	OutAmount  string
}

// ReferenceEvidence is intentionally provider-neutral. Raw response bodies,
// API keys, and transaction instructions are never retained.
type ReferenceEvidence struct {
	Provider     market.SourceID
	Status       ReferenceStatus
	AmountOut    market.TokenAmount
	ContextSlot  uint64
	Latency      time.Duration
	ResponseHash [sha256.Size]byte
	Route        []ReferenceHop
	Error        string
}

type ReferenceResult struct {
	Local    market.Quote
	Evidence ReferenceEvidence
}

// ReferenceSource adds asynchronous external validation to a local quote.
// Implementations must return the local result even when the reference is
// unavailable; external providers never participate in the hot path.
type ReferenceSource interface {
	Source
	QuoteWithReference(context.Context, Input) (ReferenceResult, error)
}

// ExternalReferenceSource validates an already-computed local quote. This is
// the non-blocking boundary used by Research: callers pass the selected local
// quote so an external provider cannot recalculate the sizing curve or become
// part of the local decision hot path.
type ExternalReferenceSource interface {
	Source
	Reference(context.Context, Input, market.Quote) (ReferenceEvidence, error)
}
