package marketstate_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/core/marketstate"
	"github.com/VarozXYZ/vernier/domain/market"
)

type opaqueEvent struct{ value int }

func (opaqueEvent) EventKind() string { return "opaque/event" }

type opaqueState struct{ value int }

func (opaqueState) SnapshotKind() string { return "opaque/state" }

type opaqueReducer struct{}

func (opaqueReducer) Reduce(_ context.Context, previous market.SnapshotData, event market.EventData) (market.SnapshotData, [sha256.Size]byte, error) {
	update, ok := event.(opaqueEvent)
	if !ok {
		return nil, [sha256.Size]byte{}, fmt.Errorf("wrong event")
	}
	value := update.value
	if prior, ok := previous.(opaqueState); ok {
		value += prior.value
	}
	state := opaqueState{value: value}
	return state, sha256.Sum256([]byte(fmt.Sprint(value))), nil
}

type arrivalOrder struct{}

func (arrivalOrder) Stale(market.SnapshotMetadata, market.MarketEvent) (bool, string) {
	return false, ""
}

func TestMirrorOwnsLifecycleButNotProtocolState(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mirror, err := marketstate.NewMirror("market", "source", opaqueReducer{}, arrivalOrder{}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	for index, value := range []int{2, 3} {
		event, err := market.NewMarketEvent(market.MarketEvent{
			Market: "market", Source: "source", ReceivedAt: now.Add(time.Duration(index) * time.Second),
			Reference: market.SourceReference{Kind: "test", Value: fmt.Sprint(index)}, Data: opaqueEvent{value: value},
		})
		if err != nil {
			t.Fatal(err)
		}
		result, err := mirror.Apply(context.Background(), event)
		if err != nil {
			t.Fatal(err)
		}
		if result.Snapshot.Metadata().Version != uint64(index+1) {
			t.Fatalf("version = %d", result.Snapshot.Metadata().Version)
		}
		if result.Snapshot.Metadata().EventReference != event.Reference {
			t.Fatal("snapshot did not preserve source reference")
		}
	}
	current, _ := mirror.Current()
	if current.Data().(opaqueState).value != 5 {
		t.Fatalf("reducer state = %+v", current.Data())
	}
}

func TestMirrorResetReplacesStateAndKeepsMonotonicVersion(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mirror, err := marketstate.NewMirror("market", "source", opaqueReducer{}, arrivalOrder{}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	newEvent := func(value int, reference string) market.MarketEvent {
		event, err := market.NewMarketEvent(market.MarketEvent{
			Market: "market", Source: "source", ReceivedAt: now,
			Reference: market.SourceReference{Kind: "test", Value: reference}, Data: opaqueEvent{value: value},
		})
		if err != nil {
			t.Fatal(err)
		}
		return event
	}
	if _, err := mirror.Apply(context.Background(), newEvent(10, "event")); err != nil {
		t.Fatal(err)
	}
	result, err := mirror.Reset(context.Background(), newEvent(3, "bootstrap"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Snapshot.Metadata().Version != 2 {
		t.Fatalf("reset version = %d, want 2", result.Snapshot.Metadata().Version)
	}
	if got := result.Snapshot.Data().(opaqueState).value; got != 3 {
		t.Fatalf("reset retained old state: %d", got)
	}
}
