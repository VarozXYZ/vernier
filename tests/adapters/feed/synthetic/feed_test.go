package synthetic_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/feed/synthetic"
	"github.com/VarozXYZ/vernier/domain/market"
)

type eventData struct{}

func (eventData) EventKind() string { return "test" }

type sink struct{ blocks []uint64 }

func (s *sink) Publish(_ context.Context, event market.MarketEvent) error {
	s.blocks = append(s.blocks, event.Position.Value)
	return nil
}

func TestFeedPublishesEventsInFixtureOrder(t *testing.T) {
	events := []market.MarketEvent{newEvent(t, 2), newEvent(t, 1)}
	feed, err := synthetic.New("market", events)
	if err != nil {
		t.Fatal(err)
	}
	collector := &sink{}
	if err := feed.Run(context.Background(), collector); err != nil {
		t.Fatal(err)
	}
	if len(collector.blocks) != 2 || collector.blocks[0] != 2 || collector.blocks[1] != 1 {
		t.Fatalf("events were reordered: %v", collector.blocks)
	}
}

func TestFeedHonorsCancellation(t *testing.T) {
	feed, err := synthetic.New("market", []market.MarketEvent{newEvent(t, 1)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := feed.Run(ctx, &sink{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

func newEvent(t *testing.T, block uint64) market.MarketEvent {
	t.Helper()
	event, err := market.NewMarketEvent(market.MarketEvent{
		Market: "market", Source: "source", Position: market.SourcePosition{Kind: "block", Value: block},
		Finality: market.FinalityConfirmed, ReceivedAt: time.Date(2026, 1, 1, 0, 0, int(block), 0, time.UTC),
		Data: eventData{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return event
}
