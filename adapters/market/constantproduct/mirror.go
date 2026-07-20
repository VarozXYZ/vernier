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
	mu       sync.RWMutex
	market   market.MarketID
	source   market.SourceID
	clock    Clock
	version  uint64
	expected uint64
	current  market.MarketSnapshot
	hasState bool
	health   market.Health
}

func NewMirror(marketID market.MarketID, source market.SourceID, clock Clock) (*Mirror, error) {
	if marketID == "" || source == "" || clock == nil {
		return nil, fmt.Errorf("market, source, and clock are required")
	}
	return &Mirror{market: marketID, source: source, clock: clock, expected: 1, health: market.HealthHealthy}, nil
}

func (m *Mirror) MarketID() market.MarketID { return m.market }

func (m *Mirror) Apply(ctx context.Context, event market.MarketEvent) (market.MarketSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return market.MarketSnapshot{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if event.Market != m.market || event.Source != m.source {
		return market.MarketSnapshot{}, fmt.Errorf("event market/source does not match mirror")
	}
	if event.Sequence != m.expected {
		m.degradeLocked()
		return market.MarketSnapshot{}, feedport.SequenceViolation{Market: m.market, Expected: m.expected, Actual: event.Sequence}
	}
	update, ok := event.Data.(ReserveUpdate)
	if !ok {
		return market.MarketSnapshot{}, fmt.Errorf("unsupported event payload %T", event.Data)
	}
	if err := update.validate(); err != nil {
		return market.MarketSnapshot{}, err
	}

	state := Snapshot{
		schemaVersion: snapshotSchemaVersion,
		baseReserve:   update.BaseReserve(), quoteReserve: update.QuoteReserve(), feeBPS: update.FeeBPS(),
	}
	m.version++
	metadata := market.SnapshotMetadata{
		Market: m.market, Source: m.source, Version: m.version, EventSequence: event.Sequence,
		Finality: event.Finality, SourceTime: event.SourceTime, SourceTimeKnown: event.SourceTimeKnown,
		ReceivedAt: event.ReceivedAt, AppliedAt: m.clock().UTC(), Health: market.HealthHealthy,
		StateHash: stateHash(state),
	}
	snapshot, err := market.NewMarketSnapshot(metadata, state)
	if err != nil {
		return market.MarketSnapshot{}, err
	}
	m.current = snapshot
	m.hasState = true
	m.health = market.HealthHealthy
	m.expected++
	return snapshot, nil
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

func (m *Mirror) degradeLocked() {
	m.health = market.HealthDegraded
	if !m.hasState {
		return
	}
	metadata := m.current.Metadata()
	metadata.Health = market.HealthDegraded
	metadata.AppliedAt = m.clock().UTC()
	snapshot, err := market.NewMarketSnapshot(metadata, m.current.Data())
	if err == nil {
		m.current = snapshot
	}
}

func stateHash(snapshot Snapshot) [sha256.Size]byte {
	payload := fmt.Sprintf("%d|%s|%s|%d", snapshot.schemaVersion, snapshot.baseReserve, snapshot.quoteReserve, snapshot.feeBPS)
	return sha256.Sum256([]byte(payload))
}
