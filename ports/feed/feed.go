// Package feed defines market-data ingestion and mirror contracts.
package feed

import (
	"context"
	"fmt"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
)

type EventSink interface {
	Publish(context.Context, market.MarketEvent) error
}

// TransactionBatchSink atomically publishes all normalized pool events from
// one chain transaction. Mutable child mirrors may advance individually, but
// a composed market must expose only the final post-transaction snapshot.
type TransactionBatchSink interface {
	PublishBatch(context.Context, []market.MarketEvent) error
}

// CausalTransactionBatchSink preserves the ordered transaction batch while
// identifying the pool event that made the batch economically relevant.
// Feeds fall back to TransactionBatchSink for consumers that do not need the
// causal event separately.
type CausalTransactionBatchSink interface {
	PublishBatchTriggered(context.Context, []market.MarketEvent, market.MarketEvent) error
}

// StateOnlyBatchSink atomically applies a live transaction batch without
// converting it into an economic trigger. Composite markets use it for
// auxiliary pools whose state affects later quotes but whose isolated changes
// cannot create the strategy's arbitrage signal.
type StateOnlyBatchSink interface {
	PublishBatchStateOnly(context.Context, []market.MarketEvent) error
}

// SynchronizationSink applies bootstrap/catch-up state without converting
// historical synchronization into an economic trigger. Feeds use it while a
// market is degraded and only restore health after reaching their watermark.
type SynchronizationSink interface {
	ResetSynchronized(context.Context, market.MarketEvent) error
	PublishBatchSynchronized(context.Context, []market.MarketEvent) error
}

// ResetSink is an optional extension used by reconnecting feeds. A reset is
// a complete current-state bootstrap and must replace the mirror state; it is
// deliberately separate from normal event publication.
type ResetSink interface {
	Reset(context.Context, market.MarketEvent) error
}

// Sink receives market events and explicit feed-liveness changes. Ordering and
// health remain independent signals.
type Sink interface {
	EventSink
	SetHealth(context.Context, HealthUpdate) error
}

type Feed interface {
	MarketID() market.MarketID
	Run(context.Context, Sink) error
}

type Mirror interface {
	MarketID() market.MarketID
	Apply(context.Context, market.MarketEvent) (ApplyResult, error)
	Reset(context.Context, market.MarketEvent) (ApplyResult, error)
	SetHealth(context.Context, HealthUpdate) error
	Current() (market.MarketSnapshot, bool)
	Health() market.Health
}

type ApplyDisposition string

const (
	ApplyDispositionApplied      ApplyDisposition = "applied"
	ApplyDispositionIgnoredStale ApplyDisposition = "ignored_stale"
)

type ApplyResult struct {
	Disposition ApplyDisposition
	Reason      string
	Snapshot    market.MarketSnapshot
}

func (r ApplyResult) Validate() error {
	if r.Disposition != ApplyDispositionApplied && r.Disposition != ApplyDispositionIgnoredStale {
		return fmt.Errorf("invalid apply disposition %q", r.Disposition)
	}
	if r.Snapshot.Metadata().Version == 0 {
		return fmt.Errorf("apply result requires a snapshot")
	}
	if r.Disposition == ApplyDispositionIgnoredStale && r.Reason == "" {
		return fmt.Errorf("ignored event requires a reason")
	}
	if r.Disposition == ApplyDispositionApplied && r.Reason != "" {
		return fmt.Errorf("applied event cannot have an ignore reason")
	}
	return nil
}

type HealthUpdate struct {
	Health     market.Health
	Reason     string
	ObservedAt time.Time
}

func (u HealthUpdate) Validate() error {
	if u.Health != market.HealthHealthy && u.Health != market.HealthDegraded {
		return fmt.Errorf("invalid feed health %q", u.Health)
	}
	if u.ObservedAt.IsZero() {
		return fmt.Errorf("feed health timestamp is required")
	}
	if u.Health == market.HealthDegraded && u.Reason == "" {
		return fmt.Errorf("degraded feed health requires a reason")
	}
	if u.Health == market.HealthHealthy && u.Reason != "" {
		return fmt.Errorf("healthy feed health cannot have a reason")
	}
	return nil
}
