// Package solanalogs adapts Solana logsSubscribe notifications to normalized
// market events. It never subscribes to new heads, infers gaps, or applies a
// TTL to a healthy market mirror.
package solanalogs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/VarozXYZ/vernier/adapters/chain/solana"
	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
)

const SlotPositionKind market.SourcePositionKind = "slot"
const SignatureReferenceKind market.SourceReferenceKind = "solana_signature"

type Network interface {
	CurrentSlot(context.Context) (uint64, error)
	SubscribeLogs(context.Context, string) (solana.LogsSubscription, error)
}

// Decoder owns protocol-specific account bootstrap and log interpretation. A
// single notification may contain multiple instructions; returned data must
// retain that instruction order.
type Decoder interface {
	Bootstrap(context.Context, Network, uint64) (market.EventData, error)
	Decode(context.Context, Network, solana.LogNotification) ([]market.EventData, error)
}

type Clock func() time.Time

type RetryPolicy struct {
	Initial time.Duration
	Maximum time.Duration
}

type Config struct {
	Market  market.MarketID
	Source  market.SourceID
	Pool    string
	Network Network
	Decoder Decoder
	Clock   Clock
	Retry   RetryPolicy
	Logger  *slog.Logger
}

type Feed struct {
	market  market.MarketID
	source  market.SourceID
	pool    string
	network Network
	decoder Decoder
	clock   Clock
	retry   RetryPolicy
	logger  *slog.Logger
}

func New(config Config) (*Feed, error) {
	if config.Market == "" || config.Source == "" || config.Pool == "" || config.Network == nil || config.Decoder == nil {
		return nil, fmt.Errorf("market, source, pool, network, and decoder are required")
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.Logger == nil {
		config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if config.Retry.Initial == 0 {
		config.Retry.Initial = 250 * time.Millisecond
	}
	if config.Retry.Maximum == 0 {
		config.Retry.Maximum = 10 * time.Second
	}
	if config.Retry.Initial < 0 || config.Retry.Maximum < config.Retry.Initial {
		return nil, fmt.Errorf("invalid reconnect retry policy")
	}
	return &Feed{market: config.Market, source: config.Source, pool: config.Pool, network: config.Network, decoder: config.Decoder, clock: config.Clock, retry: config.Retry, logger: config.Logger}, nil
}

func (f *Feed) MarketID() market.MarketID { return f.market }

func (f *Feed) Run(ctx context.Context, sink feedport.Sink) error {
	if sink == nil {
		return fmt.Errorf("feed sink is required")
	}
	established := false
	delay := f.retry.Initial
	f.logger.Info("feed run started", "market", f.market, "source", f.source)
	for {
		f.logger.Debug("feed session starting", "market", f.market)
		bootstrapped, disconnected, err := f.runSession(ctx, sink)
		if err == nil || ctx.Err() != nil {
			f.logger.Debug("feed run stopped", "market", f.market, "reason", ctx.Err())
			return ctx.Err()
		}
		if !established && !bootstrapped {
			return err
		}
		if bootstrapped {
			established = true
			delay = f.retry.Initial
		}
		if !disconnected {
			return err
		}
		f.logger.Warn("feed WebSocket disconnected", "market", f.market, "error", err)
		if healthErr := sink.SetHealth(ctx, feedport.HealthUpdate{Health: market.HealthDegraded, Reason: "websocket_disconnected", ObservedAt: f.clock().UTC()}); healthErr != nil {
			return healthErr
		}
		if err := wait(ctx, delay); err != nil {
			return err
		}
		if delay < f.retry.Maximum {
			delay *= 2
			if delay > f.retry.Maximum {
				delay = f.retry.Maximum
			}
		}
		f.logger.Info("feed reconnecting", "market", f.market, "retry_delay", delay)
	}
}

func (f *Feed) runSession(ctx context.Context, sink feedport.Sink) (established, disconnected bool, result error) {
	subscription, err := f.network.SubscribeLogs(ctx, f.pool)
	if err != nil {
		return false, true, err
	}
	defer subscription.Unsubscribe()
	f.logger.Debug("feed subscribing to filtered logs", "market", f.market)
	slot, err := f.network.CurrentSlot(ctx)
	if err != nil {
		return false, false, err
	}
	f.logger.Info("feed bootstrap started", "market", f.market, "slot", slot)
	bootstrapStarted := time.Now()
	data, err := f.decoder.Bootstrap(ctx, f.network, slot)
	if err != nil {
		return false, false, fmt.Errorf("bootstrap %s at slot %d: %w", f.market, slot, err)
	}
	f.logger.Info("feed bootstrap completed", "market", f.market, "slot", slot, "duration", time.Since(bootstrapStarted))
	bootstrap := f.event(slot, "bootstrap", data)
	if resetSink, ok := sink.(feedport.ResetSink); ok {
		if err := resetSink.Reset(ctx, bootstrap); err != nil {
			return false, false, err
		}
	} else if err := sink.Publish(ctx, bootstrap); err != nil {
		return false, false, err
	}
	if err := sink.SetHealth(ctx, feedport.HealthUpdate{Health: market.HealthHealthy, ObservedAt: f.clock().UTC()}); err != nil {
		return false, false, err
	}
	f.logger.Info("feed bootstrap applied", "market", f.market, "slot", slot)
	established = true
	highest := slot
	for {
		select {
		case <-ctx.Done():
			return true, false, ctx.Err()
		case err, open := <-subscription.Err():
			if !open || err == nil {
				err = fmt.Errorf("solana log subscription closed")
			}
			return true, true, err
		case notification, open := <-subscription.Notifications():
			if !open {
				return true, true, fmt.Errorf("solana log stream closed")
			}
			if notification.Err != nil && string(notification.Err) != "null" {
				return true, false, fmt.Errorf("solana transaction %s failed: %s", notification.Signature, notification.Err)
			}
			if notification.Slot < highest {
				// Slot is explicit evidence that this notification predates the
				// current state. Silence and slot gaps are otherwise irrelevant.
				continue
			}
			f.logger.Debug("feed event received", "market", f.market, "slot", notification.Slot, "signature", notification.Signature)
			eventStarted := time.Now()
			data, err := f.decoder.Decode(ctx, f.network, notification)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return true, false, err
				}
				return true, false, fmt.Errorf("decode %s log %s: %w", f.market, notification.Signature, err)
			}
			for index, item := range data {
				if item == nil {
					return true, false, fmt.Errorf("decode %s log %s returned nil event at index %d", f.market, notification.Signature, index)
				}
				if err := sink.Publish(ctx, f.event(notification.Slot, notification.Signature, item)); err != nil {
					return true, false, err
				}
				f.logger.Debug("feed event applied", "market", f.market, "slot", notification.Slot, "signature", notification.Signature, "instruction", index, "duration", time.Since(eventStarted))
			}
			if notification.Slot > highest {
				highest = notification.Slot
			}
		}
	}
}

func (f *Feed) event(slot uint64, signature string, data market.EventData) market.MarketEvent {
	reference := signature
	if reference == "" {
		reference = "bootstrap"
	}
	event, err := market.NewMarketEvent(market.MarketEvent{
		Market: f.market, Source: f.source,
		Position:  market.SourcePosition{Kind: SlotPositionKind, Value: slot},
		Reference: market.SourceReference{Kind: SignatureReferenceKind, Value: reference},
		Finality:  market.FinalityPreconfirmed, ReceivedAt: f.clock().UTC(), Data: data,
	})
	if err != nil {
		panic(fmt.Sprintf("solanalogs constructed invalid event: %v", err))
	}
	return event
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

var _ feedport.Feed = (*Feed)(nil)
