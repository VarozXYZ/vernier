// Package marketstream owns reusable runtime state for event-driven markets.
package marketstream

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
)

// EventSnapshotStore owns the causal snapshot of an event-refreshed market.
// Feed events advance quote generations but never carry or mutate venue state.
// A snapshot becomes available only after every configured trigger feed has
// completed a bootstrap and reports healthy.
type EventSnapshotStore struct {
	mu              sync.RWMutex
	market          market.MarketID
	source          market.SourceID
	clock           func() time.Time
	order           []string
	feeds           map[string]eventFeedState
	version         uint64
	current         market.MarketSnapshot
	has             bool
	last            market.MarketEvent
	healthChangedAt time.Time
}

type eventFeedState struct {
	ready  bool
	health market.Health
	reason string
	reset  bool
}

// EventSnapshotData is opaque causal evidence interpreted by remote quote
// sources. Generation changes for every accepted trigger.
type EventSnapshotData struct {
	generation uint64
}

func (EventSnapshotData) SnapshotKind() string { return "event_refreshed_market/v1" }

func NewEventSnapshotStore(marketID market.MarketID, source market.SourceID, feeds []string, clock func() time.Time) (*EventSnapshotStore, error) {
	if marketID == "" || source == "" || len(feeds) == 0 || clock == nil {
		return nil, fmt.Errorf("event snapshot store requires market, source, feeds, and clock")
	}
	states := make(map[string]eventFeedState, len(feeds))
	order := make([]string, 0, len(feeds))
	for _, id := range feeds {
		id = strings.TrimSpace(id)
		if id == "" {
			return nil, fmt.Errorf("event snapshot feed ID cannot be empty")
		}
		if _, duplicate := states[id]; duplicate {
			return nil, fmt.Errorf("duplicate event snapshot feed %q", id)
		}
		states[id] = eventFeedState{health: market.HealthDegraded, reason: "feed_initializing"}
		order = append(order, id)
	}
	return &EventSnapshotStore{market: marketID, source: source, clock: clock, order: order, feeds: states}, nil
}

// Reset records one feed's completed bootstrap. The following healthy update
// publishes the refreshed generation once all configured feeds are ready.
func (s *EventSnapshotStore) Reset(_ context.Context, feed string, event market.MarketEvent) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.feeds[feed]
	if !ok {
		return false, fmt.Errorf("unknown event snapshot feed %q", feed)
	}
	state.ready = true
	state.reset = true
	s.feeds[feed] = state
	s.last = event
	return false, nil
}

// Publish advances one generation for every accepted relevant pool event.
func (s *EventSnapshotStore) Publish(_ context.Context, feed string, event market.MarketEvent) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.feeds[feed]
	if !ok {
		return false, fmt.Errorf("unknown event snapshot feed %q", feed)
	}
	if !state.ready {
		return false, fmt.Errorf("event snapshot feed %q published before bootstrap", feed)
	}
	s.last = event
	if !s.allReady() {
		return false, nil
	}
	return true, s.advance(event)
}

// Refresh advances the remote quote generation because another market event
// triggered a cross-market Evaluation. Unlike Publish, this does not claim
// that a configured Solana trigger pool changed; it records the external
// event's own causal metadata and guarantees that provider quotes cannot be
// reused across Evaluations.
func (s *EventSnapshotStore) Refresh(_ context.Context, event market.MarketEvent) (bool, error) {
	if event.Source == "" || event.ReceivedAt.IsZero() {
		return false, fmt.Errorf("event snapshot refresh requires source and received timestamp")
	}
	if err := event.Position.Validate(); err != nil {
		return false, err
	}
	if err := event.Reference.Validate(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.has || !s.allReady() {
		return false, nil
	}
	s.last = event
	return true, s.advance(event)
}

// SetHealth aggregates trigger-feed health. A degraded feed degrades the
// market snapshot; recovery publishes a fresh generation so no pre-failure
// remote quote can be reused.
func (s *EventSnapshotStore) SetHealth(_ context.Context, feed string, update feedport.HealthUpdate) (bool, error) {
	if err := update.Validate(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.feeds[feed]
	if !ok {
		return false, fmt.Errorf("unknown event snapshot feed %q", feed)
	}
	beforeHealth, beforeReason := s.aggregateHealth()
	state.health, state.reason = update.Health, update.Reason
	s.feeds[feed] = state
	afterHealth, afterReason := s.aggregateHealth()
	if beforeHealth != afterHealth || beforeReason != afterReason {
		s.healthChangedAt = update.ObservedAt.UTC()
	}
	if !s.allReady() {
		return false, nil
	}
	pendingReset := false
	for id, candidate := range s.feeds {
		if candidate.reset && candidate.health == market.HealthHealthy {
			candidate.reset = false
			s.feeds[id] = candidate
			pendingReset = true
		}
	}
	if !s.has || pendingReset || beforeHealth != afterHealth || beforeReason != afterReason {
		return true, s.advance(s.last)
	}
	return false, nil
}

func (s *EventSnapshotStore) Current() (market.MarketSnapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current, s.has
}

func (s *EventSnapshotStore) advance(event market.MarketEvent) error {
	if event.ReceivedAt.IsZero() {
		return fmt.Errorf("event snapshot requires a received timestamp")
	}
	s.version++
	health, reason := s.aggregateHealth()
	var version [8]byte
	binary.BigEndian.PutUint64(version[:], s.version)
	hasher := sha256.New()
	hasher.Write([]byte(s.market))
	hasher.Write(version[:])
	hasher.Write([]byte(event.Source))
	hasher.Write([]byte(event.Position.Kind))
	binary.BigEndian.PutUint64(version[:], event.Position.Value)
	hasher.Write(version[:])
	hasher.Write([]byte(event.Reference.Kind))
	hasher.Write([]byte(event.Reference.Value))
	var hash [sha256.Size]byte
	copy(hash[:], hasher.Sum(nil))
	appliedAt := s.clock().UTC()
	if s.healthChangedAt.IsZero() {
		s.healthChangedAt = appliedAt
	}
	snapshot, err := market.NewMarketSnapshot(market.SnapshotMetadata{
		Market: s.market, Source: s.source, Version: s.version,
		EventPosition: event.Position, EventReference: event.Reference, Finality: event.Finality,
		SourceTime: event.SourceTime, SourceTimeKnown: event.SourceTimeKnown,
		ReceivedAt: event.ReceivedAt, AppliedAt: appliedAt,
		Health: health, HealthReason: reason, HealthChangedAt: s.healthChangedAt, StateHash: hash,
	}, EventSnapshotData{generation: s.version})
	if err != nil {
		return err
	}
	s.current, s.has = snapshot, true
	return nil
}

func (s *EventSnapshotStore) allReady() bool {
	for _, id := range s.order {
		if !s.feeds[id].ready {
			return false
		}
	}
	return true
}

func (s *EventSnapshotStore) aggregateHealth() (market.Health, string) {
	var reasons []string
	for _, id := range s.order {
		state := s.feeds[id]
		if state.health != market.HealthHealthy {
			reason := state.reason
			if reason == "" {
				reason = "feed_degraded"
			}
			reasons = append(reasons, id+":"+reason)
		}
	}
	if len(reasons) > 0 {
		return market.HealthDegraded, strings.Join(reasons, ",")
	}
	return market.HealthHealthy, ""
}
