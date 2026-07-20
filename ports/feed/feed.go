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

type Feed interface {
	MarketID() market.MarketID
	Run(context.Context, EventSink) error
}

type Mirror interface {
	MarketID() market.MarketID
	Apply(context.Context, market.MarketEvent) (ApplyResult, error)
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
	Snapshot    market.MarketSnapshot
}

func (r ApplyResult) Validate() error {
	if r.Disposition != ApplyDispositionApplied && r.Disposition != ApplyDispositionIgnoredStale {
		return fmt.Errorf("invalid apply disposition %q", r.Disposition)
	}
	if r.Snapshot.Metadata().Version == 0 {
		return fmt.Errorf("apply result requires a snapshot")
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
