// Package sourceorder provides ordering policies selected with feed adapters.
package sourceorder

import "github.com/VarozXYZ/vernier/domain/market"

const BlockPositionKind market.SourcePositionKind = "block"

type Monotonic struct {
	kind              market.SourcePositionKind
	fallbackTimestamp bool
}

func NewMonotonic(kind market.SourcePositionKind, fallbackTimestamp bool) Monotonic {
	return Monotonic{kind: kind, fallbackTimestamp: fallbackTimestamp}
}

func (p Monotonic) Stale(current market.SnapshotMetadata, incoming market.MarketEvent) (bool, string) {
	if p.kind != "" && incoming.Position.Kind == p.kind && current.EventPosition.Kind == p.kind {
		if incoming.Position.Value < current.EventPosition.Value {
			return true, "older_" + string(p.kind)
		}
		return false, ""
	}
	if p.fallbackTimestamp && incoming.SourceTimeKnown && current.SourceTimeKnown {
		if incoming.SourceTime.Before(current.SourceTime) {
			return true, "older_source_timestamp"
		}
	}
	return false, ""
}

type Arrival struct{}

func (Arrival) Stale(market.SnapshotMetadata, market.MarketEvent) (bool, string) {
	return false, ""
}
