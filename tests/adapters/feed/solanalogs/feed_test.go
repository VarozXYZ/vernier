package solanalogs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/chain/solana"
	"github.com/VarozXYZ/vernier/adapters/feed/solanalogs"
	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
)

type eventData struct{ value int }

func (eventData) EventKind() string { return "test/event" }

type decoder struct{}

func (decoder) Bootstrap(context.Context, solanalogs.Network, uint64) (market.EventData, error) {
	return eventData{value: 0}, nil
}

func (decoder) Decode(_ context.Context, _ solanalogs.Network, notification solana.LogNotification) ([]market.EventData, error) {
	if notification.Signature == "bad" {
		return nil, errors.New("bad log")
	}
	return []market.EventData{eventData{value: 1}, eventData{value: 2}}, nil
}

type fakeNetwork struct {
	subscriptions int
}

type accountDecoder struct{ decoded int }

func (d *accountDecoder) Bootstrap(context.Context, solanalogs.Network, uint64) (market.EventData, error) {
	return eventData{value: 0}, nil
}
func (*accountDecoder) Decode(context.Context, solanalogs.Network, solana.LogNotification) ([]market.EventData, error) {
	return nil, errors.New("log decoder must not be called")
}
func (*accountDecoder) AccountSubscriptions() []string { return []string{"pool-account"} }
func (d *accountDecoder) DecodeAccount(context.Context, solana.AccountNotification) ([]market.EventData, error) {
	d.decoded++
	return []market.EventData{eventData{value: 3}}, nil
}

type accountNetwork struct{}

func (accountNetwork) CurrentSlot(context.Context) (uint64, error) { return 10, nil }
func (accountNetwork) SubscribeLogs(context.Context, string) (solana.LogsSubscription, error) {
	return nil, errors.New("logs must not be subscribed")
}
func (accountNetwork) SubscribeAccount(context.Context, string) (solana.AccountSubscription, error) {
	subscription := &accountSubscription{notifications: make(chan solana.AccountNotification, 1), errors: make(chan error, 1)}
	subscription.notifications <- solana.AccountNotification{Slot: 10, Account: "pool-account", Value: solana.Account{Data: []byte{1}}}
	close(subscription.notifications)
	return subscription, nil
}

type accountSubscription struct {
	notifications chan solana.AccountNotification
	errors        chan error
}

func (s *accountSubscription) Err() <-chan error { return s.errors }
func (s *accountSubscription) Notifications() <-chan solana.AccountNotification {
	return s.notifications
}
func (*accountSubscription) Unsubscribe() {}

type programDecoder struct{ decoded int }

func (d *programDecoder) Bootstrap(context.Context, solanalogs.Network, uint64) (market.EventData, error) {
	return eventData{value: 0}, nil
}
func (*programDecoder) Decode(context.Context, solanalogs.Network, solana.LogNotification) ([]market.EventData, error) {
	return nil, errors.New("log decoder must not be called")
}
func (*programDecoder) AccountSubscriptions() []string { return nil }
func (*programDecoder) ProgramSubscriptions() []solana.ProgramSubscriptionRequest {
	return []solana.ProgramSubscriptionRequest{{Program: "program"}}
}
func (d *programDecoder) DecodeAccount(context.Context, solana.AccountNotification) ([]market.EventData, error) {
	return nil, errors.New("account decoder must not be called")
}
func (d *programDecoder) DecodeProgram(context.Context, solana.ProgramNotification) ([]market.EventData, error) {
	d.decoded++
	return []market.EventData{eventData{value: 4}}, nil
}

type programNetwork struct{}

func (programNetwork) CurrentSlot(context.Context) (uint64, error) { return 10, nil }
func (programNetwork) SubscribeLogs(context.Context, string) (solana.LogsSubscription, error) {
	return nil, errors.New("logs must not be subscribed")
}
func (programNetwork) SubscribeAccount(context.Context, string) (solana.AccountSubscription, error) {
	return nil, errors.New("accounts must not be subscribed")
}
func (programNetwork) SubscribeProgram(context.Context, solana.ProgramSubscriptionRequest) (solana.ProgramSubscription, error) {
	subscription := &programSubscription{notifications: make(chan solana.ProgramNotification, 1), errors: make(chan error, 1)}
	subscription.notifications <- solana.ProgramNotification{Slot: 10, Account: "tick-account", Value: solana.Account{Data: []byte{1}}}
	close(subscription.notifications)
	return subscription, nil
}

type programSubscription struct {
	notifications chan solana.ProgramNotification
	errors        chan error
}

func (s *programSubscription) Err() <-chan error { return s.errors }
func (s *programSubscription) Notifications() <-chan solana.ProgramNotification {
	return s.notifications
}
func (*programSubscription) Unsubscribe() {}

func (*fakeNetwork) CurrentSlot(context.Context) (uint64, error) { return 10, nil }

func (n *fakeNetwork) SubscribeLogs(context.Context, string) (solana.LogsSubscription, error) {
	n.subscriptions++
	subscription := newFakeSubscription()
	if n.subscriptions == 1 {
		go func() {
			subscription.notifications <- solana.LogNotification{Slot: 10, Signature: "same"}
			subscription.notifications <- solana.LogNotification{Slot: 9, Signature: "old"}
			subscription.notifications <- solana.LogNotification{Slot: 10, Signature: "same-2"}
			close(subscription.notifications)
		}()
	}
	return subscription, nil
}

type fakeSubscription struct {
	notifications chan solana.LogNotification
	errors        chan error
}

func newFakeSubscription() *fakeSubscription {
	return &fakeSubscription{notifications: make(chan solana.LogNotification, 8), errors: make(chan error, 1)}
}
func (s *fakeSubscription) Err() <-chan error                            { return s.errors }
func (s *fakeSubscription) Notifications() <-chan solana.LogNotification { return s.notifications }
func (*fakeSubscription) Unsubscribe()                                   {}

type sink struct {
	cancel         context.CancelFunc
	resets         []market.MarketEvent
	events         []market.MarketEvent
	health         []feedport.HealthUpdate
	cancelHealthy  int
	cancelDegraded bool
}

func (s *sink) Publish(_ context.Context, event market.MarketEvent) error {
	s.events = append(s.events, event)
	return nil
}
func (s *sink) Reset(_ context.Context, event market.MarketEvent) error {
	s.resets = append(s.resets, event)
	return nil
}
func (s *sink) SetHealth(_ context.Context, update feedport.HealthUpdate) error {
	s.health = append(s.health, update)
	if update.Health == market.HealthDegraded && s.cancelDegraded {
		s.cancel()
		return nil
	}
	if update.Health == market.HealthHealthy && s.cancelHealthy > 0 {
		s.cancelHealthy--
		if s.cancelHealthy == 0 {
			s.cancel()
		}
	}
	return nil
}

func TestFeedAppliesSameSlotInArrivalOrderAndIgnoresOlderEvidence(t *testing.T) {
	network := &fakeNetwork{}
	ctx, cancel := context.WithCancel(context.Background())
	s := &sink{cancel: cancel, cancelDegraded: true}
	feed, err := solanalogs.New(solanalogs.Config{Market: "pool", Source: "solana", Pool: "pool-account", Network: network, Decoder: decoder{}, Retry: solanalogs.RetryPolicy{Initial: time.Millisecond, Maximum: time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	if err := feed.Run(ctx, s); !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v", err)
	}
	if len(s.resets) != 1 || len(s.events) != 4 {
		t.Fatalf("resets=%d events=%d", len(s.resets), len(s.events))
	}
	if s.events[0].Reference.Value != "same" || s.events[1].Reference.Value != "same" || s.events[2].Reference.Value != "same-2" {
		t.Fatalf("unexpected event references: %+v", s.events)
	}
	if len(s.health) < 2 || s.health[len(s.health)-1].Health != market.HealthDegraded {
		t.Fatalf("missing disconnect health update: %+v", s.health)
	}
}

func TestFeedReconnectBootstrapsWithReset(t *testing.T) {
	network := &fakeNetwork{}
	ctx, cancel := context.WithCancel(context.Background())
	s := &sink{cancel: cancel, cancelHealthy: 2}
	feed, err := solanalogs.New(solanalogs.Config{Market: "pool", Source: "solana", Pool: "pool-account", Network: network, Decoder: decoder{}, Retry: solanalogs.RetryPolicy{Initial: time.Millisecond, Maximum: time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	if err := feed.Run(ctx, s); !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v", err)
	}
	if len(s.resets) != 2 {
		t.Fatalf("reset count = %d, want 2", len(s.resets))
	}
}

func TestFeedUsesAccountWebSocketWithoutLogOrRPCDecode(t *testing.T) {
	decoder := &accountDecoder{}
	ctx, cancel := context.WithCancel(context.Background())
	s := &sink{cancel: cancel, cancelDegraded: true}
	feed, err := solanalogs.New(solanalogs.Config{Market: "pool", Source: "solana", Pool: "pool-account", Network: accountNetwork{}, Decoder: decoder, Retry: solanalogs.RetryPolicy{Initial: time.Millisecond, Maximum: time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	if err := feed.Run(ctx, s); !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v", err)
	}
	if decoder.decoded != 1 || len(s.resets) != 1 || len(s.events) != 1 {
		t.Fatalf("decoded=%d resets=%d events=%d", decoder.decoded, len(s.resets), len(s.events))
	}
}

func TestFeedUsesProgramWebSocketForDiscoveredAccounts(t *testing.T) {
	decoder := &programDecoder{}
	ctx, cancel := context.WithCancel(context.Background())
	s := &sink{cancel: cancel, cancelDegraded: true}
	feed, err := solanalogs.New(solanalogs.Config{Market: "pool", Source: "solana", Pool: "pool-account", Network: programNetwork{}, Decoder: decoder, Retry: solanalogs.RetryPolicy{Initial: time.Millisecond, Maximum: time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	if err := feed.Run(ctx, s); !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v", err)
	}
	if decoder.decoded != 1 || len(s.resets) != 1 || len(s.events) != 1 {
		t.Fatalf("decoded=%d resets=%d events=%d", decoder.decoded, len(s.resets), len(s.events))
	}
}
