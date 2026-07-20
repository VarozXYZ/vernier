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

type MarketEvent struct {
	Market          MarketID
	Source          SourceID
	Sequence        uint64
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
	if event.Sequence == 0 {
		return MarketEvent{}, fmt.Errorf("event sequence must be positive")
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
	EventSequence   uint64
	Finality        Finality
	SourceTime      time.Time
	SourceTimeKnown bool
	ReceivedAt      time.Time
	AppliedAt       time.Time
	Health          Health
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
	if metadata.Version == 0 || metadata.EventSequence == 0 {
		return MarketSnapshot{}, fmt.Errorf("snapshot version and event sequence must be positive")
	}
	if metadata.ReceivedAt.IsZero() || metadata.AppliedAt.IsZero() {
		return MarketSnapshot{}, fmt.Errorf("received and applied timestamps are required")
	}
	if metadata.Health != HealthHealthy && metadata.Health != HealthDegraded {
		return MarketSnapshot{}, fmt.Errorf("invalid snapshot health %q", metadata.Health)
	}
	if data == nil || data.SnapshotKind() == "" {
		return MarketSnapshot{}, fmt.Errorf("snapshot data and kind are required")
	}
	metadata.ReceivedAt = metadata.ReceivedAt.UTC()
	metadata.AppliedAt = metadata.AppliedAt.UTC()
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
