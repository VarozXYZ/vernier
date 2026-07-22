package market

import (
	"crypto/sha256"
	"encoding/binary"
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
type SourceReferenceKind string

// SourceReference identifies the source object from which an event or
// snapshot was derived. It is opaque to the economic domain and is never used
// for ordering.
type SourceReference struct {
	Kind  SourceReferenceKind
	Value string
}

func (r SourceReference) Known() bool { return r.Kind != "" }

func (r SourceReference) Validate() error {
	if (r.Kind == "") != (r.Value == "") {
		return fmt.Errorf("source reference kind and value must be provided together")
	}
	return nil
}

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
	Reference       SourceReference
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
	if err := event.Reference.Validate(); err != nil {
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
	EventReference  SourceReference
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

// SnapshotBundle is the immutable state of a complete multi-hop market. The
// child snapshots retain ownership of each pool's protocol-specific state;
// the bundle only composes their identities and hashes for a route-level
// evaluation.
type SnapshotBundle struct {
	route     MarketID
	snapshots []MarketSnapshot
	version   uint64
	hash      [sha256.Size]byte
}

func NewSnapshotBundle(route MarketID, snapshots []MarketSnapshot) (SnapshotBundle, error) {
	if route == "" {
		return SnapshotBundle{}, fmt.Errorf("route market is required")
	}
	if len(snapshots) == 0 {
		return SnapshotBundle{}, fmt.Errorf("at least one child snapshot is required")
	}
	seen := make(map[MarketID]struct{}, len(snapshots))
	var version uint64
	hasher := sha256.New()
	hasher.Write([]byte(route))
	for _, snapshot := range snapshots {
		metadata := snapshot.Metadata()
		if metadata.Market == "" || metadata.Version == 0 {
			return SnapshotBundle{}, fmt.Errorf("child snapshot has invalid identity")
		}
		if _, ok := seen[metadata.Market]; ok {
			return SnapshotBundle{}, fmt.Errorf("duplicate child snapshot market %q", metadata.Market)
		}
		seen[metadata.Market] = struct{}{}
		if metadata.Version > version {
			version = metadata.Version
		}
		hasher.Write([]byte(metadata.Market))
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], metadata.Version)
		hasher.Write(encoded[:])
		hasher.Write(metadata.StateHash[:])
		hasher.Write([]byte(metadata.Health))
	}
	if version == 0 {
		return SnapshotBundle{}, fmt.Errorf("child snapshot version must be positive")
	}
	var hash [sha256.Size]byte
	copy(hash[:], hasher.Sum(nil))
	return SnapshotBundle{route: route, snapshots: append([]MarketSnapshot(nil), snapshots...), version: version, hash: hash}, nil
}

func (b SnapshotBundle) Route() MarketID         { return b.route }
func (b SnapshotBundle) Version() uint64         { return b.version }
func (b SnapshotBundle) Hash() [sha256.Size]byte { return b.hash }
func (b SnapshotBundle) Snapshots() []MarketSnapshot {
	return append([]MarketSnapshot(nil), b.snapshots...)
}
func (b SnapshotBundle) SnapshotKind() string { return "market_snapshot_bundle/v1" }

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
	if err := metadata.EventReference.Validate(); err != nil {
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
