package marketstate_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/core/marketstate"
	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
)

func TestRouteMirrorAppliesChildrenAndAggregatesHealth(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	makeChild := func(id market.MarketID) *marketstate.Mirror {
		child, err := marketstate.NewMirror(id, "feed", opaqueReducer{}, arrivalOrder{}, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		return child
	}
	first, second := makeChild("hop-a"), makeChild("hop-b")
	route, err := marketstate.NewRouteMirror("cashcat-usdg", "route", []feedport.Mirror{first, second}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	makeEvent := func(id market.MarketID, value int, position uint64) market.MarketEvent {
		event, err := market.NewMarketEvent(market.MarketEvent{
			Market: id, Source: "feed", Position: market.SourcePosition{Kind: "slot", Value: position},
			Reference: market.SourceReference{Kind: "signature", Value: fmt.Sprintf("%s-%d", id, position)}, ReceivedAt: now,
			Data: opaqueEvent{value: value},
		})
		if err != nil {
			t.Fatal(err)
		}
		return event
	}
	if _, err := route.Apply(context.Background(), makeEvent("hop-a", 2, 1)); err != nil {
		t.Fatal(err)
	}
	if _, ok := route.Current(); ok {
		t.Fatal("route published before all child snapshots existed")
	}
	if _, err := route.Apply(context.Background(), makeEvent("hop-b", 5, 1)); err != nil {
		t.Fatal(err)
	}
	current, ok := route.Current()
	if !ok {
		t.Fatal("route did not publish complete snapshot")
	}
	bundle, ok := current.Data().(market.SnapshotBundle)
	if !ok || len(bundle.Snapshots()) != 2 {
		t.Fatalf("unexpected route data %T", current.Data())
	}
	before := current.Metadata().StateHash
	if _, err := route.Apply(context.Background(), makeEvent("hop-b", 4, 2)); err != nil {
		t.Fatal(err)
	}
	after, _ := route.Current()
	if before == after.Metadata().StateHash {
		t.Fatal("route hash did not change after child update")
	}
	if err := route.SetChildHealth(context.Background(), "hop-a", feedport.HealthUpdate{Health: market.HealthDegraded, Reason: "websocket_disconnected", ObservedAt: now}); err != nil {
		t.Fatal(err)
	}
	if route.Health() != market.HealthDegraded {
		t.Fatal("route health did not aggregate degraded child")
	}
}
