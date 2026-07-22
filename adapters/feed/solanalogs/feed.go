// Package solanalogs adapts Solana logsSubscribe and accountSubscribe
// notifications to normalized market events. It never subscribes to new
// heads, infers gaps, or applies a TTL to a healthy market mirror.
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

type AccountNetwork interface {
	SubscribeAccount(context.Context, string) (solana.AccountSubscription, error)
}

type ProgramNetwork interface {
	SubscribeProgram(context.Context, solana.ProgramSubscriptionRequest) (solana.ProgramSubscription, error)
}

// Decoder owns protocol-specific account bootstrap and log interpretation. A
// single notification may contain multiple instructions; returned data must
// retain that instruction order.
type Decoder interface {
	Bootstrap(context.Context, Network, uint64) (market.EventData, error)
	Decode(context.Context, Network, solana.LogNotification) ([]market.EventData, error)
}

// AccountDecoder is an optional protocol contract for mirrors whose state is
// carried by account data rather than logs. Implementations must not perform
// network reads from DecodeAccount.
type AccountDecoder interface {
	AccountSubscriptions() []string
	DecodeAccount(context.Context, solana.AccountNotification) ([]market.EventData, error)
}

type ProgramDecoder interface {
	ProgramSubscriptions() []solana.ProgramSubscriptionRequest
	DecodeProgram(context.Context, solana.ProgramNotification) ([]market.EventData, error)
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
	accountDecoder, accountNetwork := f.accountMode()
	if accountDecoder != nil && accountNetwork != nil {
		return f.runAccountSession(ctx, sink, accountDecoder, accountNetwork)
	}
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

func (f *Feed) accountMode() (AccountDecoder, AccountNetwork) {
	decoder, decoderOK := f.decoder.(AccountDecoder)
	network, networkOK := f.network.(AccountNetwork)
	if !decoderOK || !networkOK {
		return nil, nil
	}
	return decoder, network
}

type accountUpdate struct {
	slot           uint64
	account        string
	value          solana.Account
	accountDecoder AccountDecoder
	programDecoder ProgramDecoder
}

func (f *Feed) runAccountSession(ctx context.Context, sink feedport.Sink, decoder AccountDecoder, network AccountNetwork) (bool, bool, error) {
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
	addresses := decoder.AccountSubscriptions()
	programDecoder, programNetwork := f.programMode()
	programRequests := []solana.ProgramSubscriptionRequest(nil)
	if programDecoder != nil && programNetwork != nil {
		programRequests = programDecoder.ProgramSubscriptions()
	}
	if len(addresses) == 0 && len(programRequests) == 0 {
		return false, false, fmt.Errorf("account feed %s has no subscriptions", f.market)
	}
	updates := make(chan accountUpdate, 128)
	errorsCh := make(chan error, len(addresses)+len(programRequests))
	accountSubscriptions := make([]solana.AccountSubscription, 0, len(addresses))
	programSubscriptions := make([]solana.ProgramSubscription, 0, len(programRequests))
	for _, address := range addresses {
		subscription, subscribeErr := network.SubscribeAccount(ctx, address)
		if subscribeErr != nil {
			for _, active := range accountSubscriptions {
				active.Unsubscribe()
			}
			return true, true, subscribeErr
		}
		accountSubscriptions = append(accountSubscriptions, subscription)
		go forwardAccounts(ctx, subscription, updates, errorsCh, decoder)
	}
	for _, request := range programRequests {
		subscription, subscribeErr := programNetwork.SubscribeProgram(ctx, request)
		if subscribeErr != nil {
			for _, active := range accountSubscriptions {
				active.Unsubscribe()
			}
			for _, active := range programSubscriptions {
				active.Unsubscribe()
			}
			return true, true, subscribeErr
		}
		programSubscriptions = append(programSubscriptions, subscription)
		go forwardPrograms(ctx, subscription, updates, errorsCh, programDecoder)
	}
	defer func() {
		for _, subscription := range accountSubscriptions {
			subscription.Unsubscribe()
		}
		for _, subscription := range programSubscriptions {
			subscription.Unsubscribe()
		}
	}()
	f.logger.Info("feed account subscriptions active", "market", f.market, "accounts", len(addresses), "programs", len(programRequests))
	highest := slot
	for {
		select {
		case <-ctx.Done():
			return true, false, ctx.Err()
		case err := <-errorsCh:
			if err == nil {
				err = fmt.Errorf("solana account stream closed")
			}
			return true, true, err
		case update := <-updates:
			if update.slot < highest {
				continue
			}
			started := time.Now()
			var data []market.EventData
			var decodeErr error
			if update.accountDecoder != nil {
				data, decodeErr = update.accountDecoder.DecodeAccount(ctx, solana.AccountNotification{Slot: update.slot, Account: update.account, Value: update.value})
			} else {
				data, decodeErr = update.programDecoder.DecodeProgram(ctx, solana.ProgramNotification{Slot: update.slot, Account: update.account, Value: update.value})
			}
			if decodeErr != nil {
				return true, false, fmt.Errorf("decode %s account %s: %w", f.market, update.account, decodeErr)
			}
			for index, item := range data {
				if item == nil {
					return true, false, fmt.Errorf("decode %s account %s returned nil event at index %d", f.market, update.account, index)
				}
				if err := sink.Publish(ctx, f.event(update.slot, "account:"+update.account, item)); err != nil {
					return true, false, err
				}
				f.logger.Debug("feed account event applied", "market", f.market, "account", update.account, "slot", update.slot, "duration", time.Since(started))
			}
			if update.slot > highest {
				highest = update.slot
			}
		}
	}
}

func (f *Feed) programMode() (ProgramDecoder, ProgramNetwork) {
	decoder, decoderOK := f.decoder.(ProgramDecoder)
	network, networkOK := f.network.(ProgramNetwork)
	if !decoderOK || !networkOK {
		return nil, nil
	}
	return decoder, network
}

func forwardAccounts(ctx context.Context, subscription solana.AccountSubscription, updates chan<- accountUpdate, errorsCh chan<- error, decoder AccountDecoder) {
	for {
		select {
		case <-ctx.Done():
			return
		case err, open := <-subscription.Err():
			if !open || err == nil {
				err = fmt.Errorf("solana account subscription closed")
			}
			select {
			case errorsCh <- err:
			case <-ctx.Done():
			}
			return
		case notification, open := <-subscription.Notifications():
			if !open {
				select {
				case errorsCh <- fmt.Errorf("solana account notification stream closed"):
				case <-ctx.Done():
				}
				return
			}
			select {
			case updates <- accountUpdate{slot: notification.Slot, account: notification.Account, value: notification.Value, accountDecoder: decoder}:
			case <-ctx.Done():
				return
			}
		}
	}
}

func forwardPrograms(ctx context.Context, subscription solana.ProgramSubscription, updates chan<- accountUpdate, errorsCh chan<- error, decoder ProgramDecoder) {
	for {
		select {
		case <-ctx.Done():
			return
		case err, open := <-subscription.Err():
			if !open || err == nil {
				err = fmt.Errorf("solana program subscription closed")
			}
			select {
			case errorsCh <- err:
			case <-ctx.Done():
			}
			return
		case notification, open := <-subscription.Notifications():
			if !open {
				select {
				case errorsCh <- fmt.Errorf("solana program notification stream closed"):
				case <-ctx.Done():
				}
				return
			}
			select {
			case updates <- accountUpdate{slot: notification.Slot, account: notification.Account, value: notification.Value, programDecoder: decoder}:
			case <-ctx.Done():
				return
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
