package market_test

import (
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
)

type testEventData struct{ kind string }

func (d testEventData) EventKind() string { return d.kind }

type testSnapshotData struct{ kind string }

func (d testSnapshotData) SnapshotKind() string { return d.kind }

func TestMarketEventNormalizesTimestamps(t *testing.T) {
	zone := time.FixedZone("test", 3600)
	event, err := market.NewMarketEvent(market.MarketEvent{
		Market: "market", Source: "source", Sequence: 1,
		Finality:   market.FinalityConfirmed,
		SourceTime: time.Date(2026, 1, 1, 1, 0, 0, 0, zone), SourceTimeKnown: true,
		ReceivedAt: time.Date(2026, 1, 1, 1, 0, 1, 0, zone),
		Data:       testEventData{kind: "test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if event.SourceTime.Location() != time.UTC || event.ReceivedAt.Location() != time.UTC {
		t.Fatal("timestamps were not normalized to UTC")
	}
}

func TestSnapshotCopiesMetadataAndReportsAge(t *testing.T) {
	received := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	metadata := market.SnapshotMetadata{
		Market: "market", Source: "source", Version: 1, EventSequence: 1,
		Finality: market.FinalityConfirmed, ReceivedAt: received, AppliedAt: received,
		Health: market.HealthHealthy,
	}
	snapshot, err := market.NewMarketSnapshot(metadata, testSnapshotData{kind: "test"})
	if err != nil {
		t.Fatal(err)
	}
	metadata.Version = 99
	if snapshot.Metadata().Version != 1 {
		t.Fatal("snapshot metadata changed through caller mutation")
	}
	if got := snapshot.Age(received.Add(3 * time.Second)); got != 3*time.Second {
		t.Fatalf("unexpected snapshot age: %s", got)
	}
}

func TestStateConstructorsRejectMissingPayloads(t *testing.T) {
	if _, err := market.NewMarketEvent(market.MarketEvent{Market: "m", Source: "s", Sequence: 1, ReceivedAt: time.Now()}); err == nil {
		t.Fatal("expected event without payload to fail")
	}
	if _, err := market.NewMarketSnapshot(market.SnapshotMetadata{Market: "m", Source: "s", Version: 1, EventSequence: 1, ReceivedAt: time.Now(), AppliedAt: time.Now(), Health: market.HealthHealthy}, nil); err == nil {
		t.Fatal("expected snapshot without payload to fail")
	}
}
