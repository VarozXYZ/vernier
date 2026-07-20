// Package marketstate owns generic market-mirror lifecycle and snapshot publication.
package marketstate

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
)

type Clock func() time.Time

// Reducer is the protocol-specific state transition used by a generic Mirror.
// Implementations must return immutable snapshot data and its deterministic hash.
type Reducer interface {
	Reduce(context.Context, market.SnapshotData, market.EventData) (market.SnapshotData, [sha256.Size]byte, error)
}

// Orderer encapsulates source-specific stale-event detection. An event is
// accepted unless the policy has positive evidence that it is older.
type Orderer interface {
	Stale(market.SnapshotMetadata, market.MarketEvent) (bool, string)
}

type Mirror struct {
	mu              sync.RWMutex
	market          market.MarketID
	source          market.SourceID
	clock           Clock
	reducer         Reducer
	orderer         Orderer
	version         uint64
	current         market.MarketSnapshot
	hasState        bool
	health          market.Health
	healthReason    string
	healthChangedAt time.Time
}

func NewMirror(marketID market.MarketID, source market.SourceID, reducer Reducer, orderer Orderer, clock Clock) (*Mirror, error) {
	if marketID == "" || source == "" || reducer == nil || orderer == nil || clock == nil {
		return nil, fmt.Errorf("market, source, reducer, orderer, and clock are required")
	}
	return &Mirror{
		market: marketID, source: source, reducer: reducer, orderer: orderer, clock: clock,
		health: market.HealthHealthy, healthChangedAt: clock().UTC(),
	}, nil
}

func (m *Mirror) MarketID() market.MarketID { return m.market }

func (m *Mirror) Apply(ctx context.Context, event market.MarketEvent) (feedport.ApplyResult, error) {
	if err := ctx.Err(); err != nil {
		return feedport.ApplyResult{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if event.Market != m.market || event.Source != m.source {
		return feedport.ApplyResult{}, fmt.Errorf("event market/source does not match mirror")
	}
	if m.hasState {
		if stale, reason := m.orderer.Stale(m.current.Metadata(), event); stale {
			return feedport.ApplyResult{
				Disposition: feedport.ApplyDispositionIgnoredStale, Reason: reason, Snapshot: m.current,
			}, nil
		}
	}
	var previous market.SnapshotData
	if m.hasState {
		previous = m.current.Data()
	}
	data, stateHash, err := m.reducer.Reduce(ctx, previous, event.Data)
	if err != nil {
		return feedport.ApplyResult{}, err
	}
	appliedAt := m.clock().UTC()
	m.version++
	if m.health != market.HealthHealthy || m.healthChangedAt.IsZero() {
		m.healthChangedAt = appliedAt
	}
	m.health = market.HealthHealthy
	m.healthReason = ""
	metadata := market.SnapshotMetadata{
		Market: m.market, Source: m.source, Version: m.version, EventPosition: event.Position,
		EventReference: event.Reference,
		Finality:       event.Finality, SourceTime: event.SourceTime, SourceTimeKnown: event.SourceTimeKnown,
		ReceivedAt: event.ReceivedAt, AppliedAt: appliedAt, Health: m.health,
		HealthReason: m.healthReason, HealthChangedAt: m.healthChangedAt, StateHash: stateHash,
	}
	snapshot, err := market.NewMarketSnapshot(metadata, data)
	if err != nil {
		return feedport.ApplyResult{}, err
	}
	m.current = snapshot
	m.hasState = true
	return feedport.ApplyResult{Disposition: feedport.ApplyDispositionApplied, Snapshot: snapshot}, nil
}

func (m *Mirror) Current() (market.MarketSnapshot, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current, m.hasState
}

func (m *Mirror) Health() market.Health {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.health
}

func (m *Mirror) SetHealth(ctx context.Context, update feedport.HealthUpdate) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := update.Validate(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	observedAt := update.ObservedAt.UTC()
	if m.health == update.Health && m.healthReason == update.Reason {
		return nil
	}
	m.health = update.Health
	m.healthReason = update.Reason
	m.healthChangedAt = observedAt
	if !m.hasState {
		return nil
	}
	metadata := m.current.Metadata()
	metadata.Health = update.Health
	metadata.HealthReason = update.Reason
	metadata.HealthChangedAt = observedAt
	snapshot, err := market.NewMarketSnapshot(metadata, m.current.Data())
	if err != nil {
		return err
	}
	m.current = snapshot
	return nil
}
