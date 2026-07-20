package sourceorder_test

import (
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/feed/sourceorder"
	"github.com/VarozXYZ/vernier/domain/market"
)

func TestMonotonicUsesOnlyComparableEvidence(t *testing.T) {
	policy := sourceorder.NewMonotonic(sourceorder.BlockPositionKind, true)
	current := market.SnapshotMetadata{
		EventPosition: market.SourcePosition{Kind: sourceorder.BlockPositionKind, Value: 20},
		SourceTime:    time.Date(2026, 1, 1, 0, 0, 20, 0, time.UTC), SourceTimeKnown: true,
	}
	for _, test := range []struct {
		name  string
		event market.MarketEvent
		stale bool
	}{
		{name: "older block", event: market.MarketEvent{Position: market.SourcePosition{Kind: sourceorder.BlockPositionKind, Value: 19}}, stale: true},
		{name: "same block", event: market.MarketEvent{Position: market.SourcePosition{Kind: sourceorder.BlockPositionKind, Value: 20}}},
		{name: "non-contiguous later block", event: market.MarketEvent{Position: market.SourcePosition{Kind: sourceorder.BlockPositionKind, Value: 900}}},
		{name: "older timestamp fallback", event: market.MarketEvent{SourceTime: current.SourceTime.Add(-time.Second), SourceTimeKnown: true}, stale: true},
		{name: "arrival fallback", event: market.MarketEvent{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			stale, _ := policy.Stale(current, test.event)
			if stale != test.stale {
				t.Fatalf("stale = %v, want %v", stale, test.stale)
			}
		})
	}
}
