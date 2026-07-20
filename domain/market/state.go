package market

import (
	"crypto/sha256"
	"fmt"
	"time"
)

type Finality string

const (
	FinalityPreconfirmed Finality = "preconfirmed"
	FinalityConfirmed    Finality = "confirmed"
	FinalityFinalized    Finality = "finalized"
)

type Health string

const (
	HealthHealthy  Health = "healthy"
	HealthDegraded Health = "degraded"
)

type EventData interface {
	EventKind() string
}

type SnapshotData interface {
	SnapshotKind() string
}

type SourcePositionKind string

// SourcePosition is optional, source-provided ordering evidence. Positions are
// comparable only when they use the same non-empty kind.
type SourcePosition struct {
	Kind  SourcePositionKind
	Value uint64
}

func (p SourcePosition) Known() bool { return p.Kind != "" }

func (p SourcePosition) Validate() error {
	if p.Kind == "" && p.Value != 0 {
		return fmt.Errorf("source position value requires a kind")
	}
	return nil
}

func (p SourcePosition) Compare(other SourcePosition) (int, bool) {
	if !p.Known() || p.Kind != other.Kind {
		return 0, false
	}
	switch {
	case p.Value < other.Value:
		return -1, true
	case p.Value > other.Value:
		return 1, true
	default:
		return 0, true
	}
}

type MarketEvent struct {
	Market          MarketID
	Source          SourceID
	Position        SourcePosition
	Finality        Finality
	SourceTime      time.Time
	SourceTimeKnown bool
	ReceivedAt      time.Time
	Reverts         bool
	Data            EventData
}

func NewMarketEvent(event MarketEvent) (MarketEvent, error) {
	if event.Market == "" || event.Source == "" {
		return MarketEvent{}, fmt.Errorf("market and source are required")
	}
	if err := event.Position.Validate(); err != nil {
		return MarketEvent{}, err
	}
	if event.ReceivedAt.IsZero() {
		return MarketEvent{}, fmt.Errorf("received timestamp is required")
	}
	if event.Data == nil || event.Data.EventKind() == "" {
		return MarketEvent{}, fmt.Errorf("event data and kind are required")
	}
	event.ReceivedAt = event.ReceivedAt.UTC()
	if event.SourceTimeKnown {
		if event.SourceTime.IsZero() {
			return MarketEvent{}, fmt.Errorf("known source timestamp cannot be zero")
		}
		event.SourceTime = event.SourceTime.UTC()
	}
	return event, nil
}

type SnapshotMetadata struct {
	Market          MarketID
	Source          SourceID
	Version         uint64
	EventPosition   SourcePosition
	Finality        Finality
	SourceTime      time.Time
	SourceTimeKnown bool
	ReceivedAt      time.Time
	AppliedAt       time.Time
	Health          Health
	HealthReason    string
	HealthChangedAt time.Time
	StateHash       [sha256.Size]byte
}

// MarketSnapshot is an immutable envelope. SnapshotData implementations must
// also be immutable and are interpreted only by their owning adapter.
type MarketSnapshot struct {
	metadata SnapshotMetadata
	data     SnapshotData
}

func NewMarketSnapshot(metadata SnapshotMetadata, data SnapshotData) (MarketSnapshot, error) {
	if metadata.Market == "" || metadata.Source == "" {
		return MarketSnapshot{}, fmt.Errorf("market and source are required")
	}
	if metadata.Version == 0 {
		return MarketSnapshot{}, fmt.Errorf("snapshot version must be positive")
	}
	if err := metadata.EventPosition.Validate(); err != nil {
		return MarketSnapshot{}, err
	}
	if metadata.ReceivedAt.IsZero() || metadata.AppliedAt.IsZero() {
		return MarketSnapshot{}, fmt.Errorf("received and applied timestamps are required")
	}
	if metadata.Health != HealthHealthy && metadata.Health != HealthDegraded {
		return MarketSnapshot{}, fmt.Errorf("invalid snapshot health %q", metadata.Health)
	}
	if metadata.HealthChangedAt.IsZero() {
		return MarketSnapshot{}, fmt.Errorf("health changed timestamp is required")
	}
	if metadata.Health == HealthDegraded && metadata.HealthReason == "" {
		return MarketSnapshot{}, fmt.Errorf("degraded snapshot requires a health reason")
	}
	if metadata.Health == HealthHealthy && metadata.HealthReason != "" {
		return MarketSnapshot{}, fmt.Errorf("healthy snapshot cannot have a health reason")
	}
	if data == nil || data.SnapshotKind() == "" {
		return MarketSnapshot{}, fmt.Errorf("snapshot data and kind are required")
	}
	metadata.ReceivedAt = metadata.ReceivedAt.UTC()
	metadata.AppliedAt = metadata.AppliedAt.UTC()
	metadata.HealthChangedAt = metadata.HealthChangedAt.UTC()
	if metadata.SourceTimeKnown {
		metadata.SourceTime = metadata.SourceTime.UTC()
	}
	return MarketSnapshot{metadata: metadata, data: data}, nil
}

func (s MarketSnapshot) Metadata() SnapshotMetadata { return s.metadata }
func (s MarketSnapshot) Data() SnapshotData         { return s.data }

func (s MarketSnapshot) Age(at time.Time) time.Duration {
	return at.UTC().Sub(s.metadata.ReceivedAt)
}
