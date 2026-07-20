package constantproduct

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

type Mirror struct {
	mu              sync.RWMutex
	market          market.MarketID
	source          market.SourceID
	clock           Clock
	version         uint64
	current         market.MarketSnapshot
	hasState        bool
	health          market.Health
	healthReason    string
	healthChangedAt time.Time
}

func NewMirror(marketID market.MarketID, source market.SourceID, clock Clock) (*Mirror, error) {
	if marketID == "" || source == "" || clock == nil {
		return nil, fmt.Errorf("market, source, and clock are required")
	}
	return &Mirror{
		market: marketID, source: source, clock: clock, health: market.HealthHealthy,
		healthChangedAt: clock().UTC(),
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
	update, ok := event.Data.(ReserveUpdate)
	if !ok {
		return feedport.ApplyResult{}, fmt.Errorf("unsupported event payload %T", event.Data)
	}
	if err := update.validate(); err != nil {
		return feedport.ApplyResult{}, err
	}
	if m.hasState && isProvablyOlder(event, m.current.Metadata()) {
		return feedport.ApplyResult{
			Disposition: feedport.ApplyDispositionIgnoredStale,
			Snapshot:    m.current,
		}, nil
	}

	state := Snapshot{
		schemaVersion: snapshotSchemaVersion,
		baseReserve:   update.BaseReserve(), quoteReserve: update.QuoteReserve(), feeBPS: update.FeeBPS(),
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
		Finality: event.Finality, SourceTime: event.SourceTime, SourceTimeKnown: event.SourceTimeKnown,
		ReceivedAt: event.ReceivedAt, AppliedAt: appliedAt, Health: m.health,
		HealthReason: m.healthReason, HealthChangedAt: m.healthChangedAt, StateHash: stateHash(state),
	}
	snapshot, err := market.NewMarketSnapshot(metadata, state)
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

func isProvablyOlder(event market.MarketEvent, current market.SnapshotMetadata) bool {
	if comparison, comparable := event.Position.Compare(current.EventPosition); comparable {
		return comparison < 0
	}
	return event.SourceTimeKnown && current.SourceTimeKnown && event.SourceTime.Before(current.SourceTime)
}

func stateHash(snapshot Snapshot) [sha256.Size]byte {
	payload := fmt.Sprintf("%d|%s|%s|%d", snapshot.schemaVersion, snapshot.baseReserve, snapshot.quoteReserve, snapshot.feeBPS)
	return sha256.Sum256([]byte(payload))
}
