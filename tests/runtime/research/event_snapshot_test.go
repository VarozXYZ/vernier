package research_test

import (
	"context"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
	runtimeresearch "github.com/VarozXYZ/vernier/runtime/research"
)

type triggerData struct{}

func (triggerData) EventKind() string { return "trigger/test" }

func TestEventSnapshotWaitsForAllFeedsAndAdvancesEveryTrigger(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	store, err := runtimeresearch.NewEventSnapshotStore(
		"remote-market", "remote/events", []string{"pool-a", "pool-b"}, func() time.Time { return now },
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first := triggerEvent(t, "pool-a-event", 10, now)
	second := triggerEvent(t, "pool-b-event", 10, now)
	if changed, err := store.Reset(ctx, "pool-a", first); err != nil || changed {
		t.Fatalf("first reset changed snapshot: changed=%t err=%v", changed, err)
	}
	if changed, err := store.SetHealth(ctx, "pool-a", feedport.HealthUpdate{Health: market.HealthHealthy, ObservedAt: now}); err != nil || changed {
		t.Fatalf("partial initialization changed snapshot: changed=%t err=%v", changed, err)
	}
	if _, ok := store.Current(); ok {
		t.Fatal("snapshot became available before every trigger feed initialized")
	}
	if changed, err := store.Reset(ctx, "pool-b", second); err != nil || changed {
		t.Fatalf("second reset emitted before health: changed=%t err=%v", changed, err)
	}
	changed, err := store.SetHealth(ctx, "pool-b", feedport.HealthUpdate{Health: market.HealthHealthy, ObservedAt: now})
	if err != nil || !changed {
		t.Fatalf("completed initialization did not publish: changed=%t err=%v", changed, err)
	}
	initial, ok := store.Current()
	if !ok || initial.Metadata().Health != market.HealthHealthy || initial.Metadata().Version != 1 {
		t.Fatalf("unexpected initial snapshot: %+v exists=%t", initial.Metadata(), ok)
	}

	// A second event at the same slot is still a distinct causal generation.
	next := triggerEvent(t, "pool-a-event-2", 10, now.Add(time.Millisecond))
	changed, err = store.Publish(ctx, "pool-a", next)
	if err != nil || !changed {
		t.Fatalf("relevant event did not advance generation: changed=%t err=%v", changed, err)
	}
	updated, _ := store.Current()
	if updated.Metadata().Version != 2 || updated.Metadata().StateHash == initial.Metadata().StateHash {
		t.Fatalf("event generation did not invalidate quote identity: initial=%+v updated=%+v", initial.Metadata(), updated.Metadata())
	}

	// An event from the other market advances the remote causal generation as
	// well, even though no watched remote pool state is mutated.
	local := triggerEvent(t, "local-pool-event", 77, now.Add(2*time.Millisecond))
	changed, err = store.Refresh(ctx, local)
	if err != nil || !changed {
		t.Fatalf("local event did not refresh remote generation: changed=%t err=%v", changed, err)
	}
	refreshed, _ := store.Current()
	if refreshed.Metadata().Version != 3 || refreshed.Metadata().StateHash == updated.Metadata().StateHash ||
		refreshed.Metadata().EventPosition != local.Position {
		t.Fatalf("unexpected cross-market refresh: %+v", refreshed.Metadata())
	}
}

func TestEventSnapshotDegradesAndRefreshesAfterRecovery(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	store, err := runtimeresearch.NewEventSnapshotStore(
		"remote-market", "remote/events", []string{"pool"}, func() time.Time { return now },
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	event := triggerEvent(t, "bootstrap", 10, now)
	_, _ = store.Reset(ctx, "pool", event)
	_, _ = store.SetHealth(ctx, "pool", feedport.HealthUpdate{Health: market.HealthHealthy, ObservedAt: now})
	before, _ := store.Current()

	changed, err := store.SetHealth(ctx, "pool", feedport.HealthUpdate{
		Health: market.HealthDegraded, Reason: "websocket_disconnected", ObservedAt: now.Add(time.Second),
	})
	if err != nil || !changed {
		t.Fatalf("disconnect did not publish degradation: changed=%t err=%v", changed, err)
	}
	degraded, _ := store.Current()
	if degraded.Metadata().Health != market.HealthDegraded || degraded.Metadata().Version != before.Metadata().Version+1 {
		t.Fatalf("unexpected degraded snapshot: %+v", degraded.Metadata())
	}

	reconnected := triggerEvent(t, "reconnect", 20, now.Add(2*time.Second))
	_, _ = store.Reset(ctx, "pool", reconnected)
	changed, err = store.SetHealth(ctx, "pool", feedport.HealthUpdate{Health: market.HealthHealthy, ObservedAt: now.Add(2 * time.Second)})
	if err != nil || !changed {
		t.Fatalf("recovery did not publish fresh generation: changed=%t err=%v", changed, err)
	}
	recovered, _ := store.Current()
	if recovered.Metadata().Health != market.HealthHealthy ||
		recovered.Metadata().Version != degraded.Metadata().Version+1 ||
		recovered.Metadata().StateHash == before.Metadata().StateHash {
		t.Fatalf("unexpected recovered snapshot: %+v", recovered.Metadata())
	}
}

func triggerEvent(t *testing.T, source market.SourceID, slot uint64, at time.Time) market.MarketEvent {
	t.Helper()
	event, err := market.NewMarketEvent(market.MarketEvent{
		Market: "trigger-child", Source: source,
		Position:   market.SourcePosition{Kind: "slot", Value: slot},
		Reference:  market.SourceReference{Kind: "signature", Value: string(source)},
		Finality:   market.FinalityPreconfirmed,
		ReceivedAt: at, Data: triggerData{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return event
}
