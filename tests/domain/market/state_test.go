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
		Market: "market", Source: "source", Position: market.SourcePosition{Kind: "block", Value: 1},
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

func TestSnapshotCopiesMetadata(t *testing.T) {
	received := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	metadata := market.SnapshotMetadata{
		Market: "market", Source: "source", Version: 1,
		EventPosition: market.SourcePosition{Kind: "block", Value: 1},
		Finality:      market.FinalityConfirmed, ReceivedAt: received, AppliedAt: received,
		Health: market.HealthHealthy, HealthChangedAt: received,
	}
	snapshot, err := market.NewMarketSnapshot(metadata, testSnapshotData{kind: "test"})
	if err != nil {
		t.Fatal(err)
	}
	metadata.Version = 99
	if snapshot.Metadata().Version != 1 {
		t.Fatal("snapshot metadata changed through caller mutation")
	}
}

func TestStateConstructorsRejectMissingPayloads(t *testing.T) {
	if _, err := market.NewMarketEvent(market.MarketEvent{Market: "m", Source: "s", ReceivedAt: time.Now()}); err == nil {
		t.Fatal("expected event without payload to fail")
	}
	if _, err := market.NewMarketSnapshot(market.SnapshotMetadata{Market: "m", Source: "s", Version: 1, ReceivedAt: time.Now(), AppliedAt: time.Now(), Health: market.HealthHealthy, HealthChangedAt: time.Now()}, nil); err == nil {
		t.Fatal("expected snapshot without payload to fail")
	}
}

func TestSourcePositionsCompareOnlyWithinTheSameKnownKind(t *testing.T) {
	block10 := market.SourcePosition{Kind: "block", Value: 10}
	block11 := market.SourcePosition{Kind: "block", Value: 11}
	if comparison, ok := block10.Compare(block11); !ok || comparison != -1 {
		t.Fatalf("unexpected comparison: %d, %v", comparison, ok)
	}
	if _, ok := block10.Compare(market.SourcePosition{}); ok {
		t.Fatal("known and unknown positions must not be comparable")
	}
}

func TestSourceReferenceRequiresKindAndValueTogether(t *testing.T) {
	for _, reference := range []market.SourceReference{
		{Kind: "evm_block_hash"},
		{Value: "0xabc"},
	} {
		_, err := market.NewMarketEvent(market.MarketEvent{
			Market: "market", Source: "source", Reference: reference,
			ReceivedAt: time.Now(), Data: testEventData{kind: "test"},
		})
		if err == nil {
			t.Fatalf("expected invalid reference %+v to fail", reference)
		}
	}
}

func TestSnapshotBundleCopiesChildrenAndHashesVersions(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	makeSnapshot := func(id market.MarketID, version uint64, value string) market.MarketSnapshot {
		snapshot, err := market.NewMarketSnapshot(market.SnapshotMetadata{
			Market: id, Source: "source", Version: version,
			EventPosition:  market.SourcePosition{Kind: "slot", Value: version},
			EventReference: market.SourceReference{Kind: "signature", Value: value},
			ReceivedAt:     now, AppliedAt: now, Health: market.HealthHealthy, HealthChangedAt: now,
		}, testSnapshotData{kind: value})
		if err != nil {
			t.Fatal(err)
		}
		return snapshot
	}
	first := makeSnapshot("hop-a", 2, "a")
	second := makeSnapshot("hop-b", 7, "b")
	bundle, err := market.NewSnapshotBundle("route", []market.MarketSnapshot{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if bundle.Version() != 7 || bundle.Route() != "route" || bundle.SnapshotKind() == "" {
		t.Fatalf("unexpected bundle identity: route=%s version=%d kind=%s", bundle.Route(), bundle.Version(), bundle.SnapshotKind())
	}
	children := bundle.Snapshots()
	children[0] = second
	if bundle.Snapshots()[0].Metadata().Market != "hop-a" {
		t.Fatal("bundle children were aliased")
	}
	if _, err := market.NewSnapshotBundle("route", []market.MarketSnapshot{first, first}); err == nil {
		t.Fatal("duplicate child markets must be rejected")
	}
}
