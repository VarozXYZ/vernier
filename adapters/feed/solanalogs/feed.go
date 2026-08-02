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
	"sort"
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

// AccountChange is one immutable WebSocket account observation delivered to
// a protocol batch decoder. Program is true for programSubscribe updates.
type AccountChange struct {
	Slot    uint64
	Account string
	Value   solana.Account
	Program bool
}

// BatchAccountDecoder composes account and program notifications that belong
// to the same slot. Implementations must not perform network reads here.
type BatchAccountDecoder interface {
	AccountSubscriptions() []string
	ProgramSubscriptions() []solana.ProgramSubscriptionRequest
	DecodeAccountBatch(context.Context, []AccountChange) ([]market.EventData, error)
}

// EconomicLogMatcher marks the successful pool transactions that should
// trigger an evaluation. Account notifications still carry the authoritative
// state, but are no longer interpreted as distinct economic actions.
type EconomicLogMatcher interface {
	IsEconomicLog(solana.LogNotification) bool
}

// SeparatedStateSink lets account-driven feeds update a mirror without
// emitting a Research trigger. PublishTrigger is called only after the state
// for the transaction slot has been applied.
type SeparatedStateSink interface {
	ApplyState(context.Context, market.MarketEvent) error
	PublishTrigger(context.Context, market.MarketEvent) error
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
	delay := f.retry.Initial
	f.logger.Info("feed run started", "market", f.market, "source", f.source)
	for {
		f.logger.Debug("feed session starting", "market", f.market)
		bootstrapped, disconnected, err := f.runSession(ctx, sink)
		if err == nil || ctx.Err() != nil {
			f.logger.Debug("feed run stopped", "market", f.market, "reason", ctx.Err())
			return ctx.Err()
		}
		if bootstrapped {
			delay = f.retry.Initial
		}
		if !disconnected {
			return err
		}
		f.logger.Warn(
			"feed WebSocket unavailable",
			"market", f.market,
			"bootstrapped", bootstrapped,
			"error", err,
			"retry_delay", delay,
		)
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
	batchDecoder, accountNetwork, programNetwork := f.batchMode()
	if batchDecoder != nil && accountNetwork != nil && programNetwork != nil {
		return f.runBatchAccountSession(ctx, sink, batchDecoder, accountNetwork, programNetwork)
	}
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
				// Failed transactions cannot mutate account state. They are
				// neither market events nor feed-health failures.
				f.logger.Debug("failed Solana transaction ignored", "market", f.market, "slot", notification.Slot, "signature", notification.Signature)
				continue
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

func (f *Feed) batchMode() (BatchAccountDecoder, AccountNetwork, ProgramNetwork) {
	decoder, decoderOK := f.decoder.(BatchAccountDecoder)
	accountNetwork, accountOK := f.network.(AccountNetwork)
	programNetwork, programOK := f.network.(ProgramNetwork)
	if !decoderOK || !accountOK || !programOK {
		return nil, nil, nil
	}
	return decoder, accountNetwork, programNetwork
}

func (f *Feed) runBatchAccountSession(ctx context.Context, sink feedport.Sink, decoder BatchAccountDecoder, accountNetwork AccountNetwork, programNetwork ProgramNetwork) (bool, bool, error) {
	addresses := decoder.AccountSubscriptions()
	requests := decoder.ProgramSubscriptions()
	if len(addresses) == 0 && len(requests) == 0 {
		return false, false, fmt.Errorf("batch account feed %s has no subscriptions", f.market)
	}
	matcher, separatesTriggers := f.decoder.(EconomicLogMatcher)
	separatedSink, separatesState := sink.(SeparatedStateSink)
	separated := separatesTriggers && separatesState
	updates := make(chan accountUpdate, 256)
	errorsCh := make(chan error, len(addresses)+len(requests))
	accountSubscriptions := make([]solana.AccountSubscription, 0, len(addresses))
	programSubscriptions := make([]solana.ProgramSubscription, 0, len(requests))
	for _, address := range addresses {
		subscription, err := accountNetwork.SubscribeAccount(ctx, address)
		if err != nil {
			return false, true, err
		}
		accountSubscriptions = append(accountSubscriptions, subscription)
		go forwardBatchAccounts(ctx, subscription, updates, errorsCh)
	}
	for _, request := range requests {
		subscription, err := programNetwork.SubscribeProgram(ctx, request)
		if err != nil {
			for _, active := range accountSubscriptions {
				active.Unsubscribe()
			}
			return false, true, err
		}
		programSubscriptions = append(programSubscriptions, subscription)
		go forwardBatchPrograms(ctx, subscription, updates, errorsCh)
	}
	var logSubscription solana.LogsSubscription
	if separated {
		var subscribeErr error
		logSubscription, subscribeErr = f.network.SubscribeLogs(ctx, f.pool)
		if subscribeErr != nil {
			for _, active := range accountSubscriptions {
				active.Unsubscribe()
			}
			for _, active := range programSubscriptions {
				active.Unsubscribe()
			}
			return false, true, subscribeErr
		}
	}
	defer func() {
		for _, subscription := range accountSubscriptions {
			subscription.Unsubscribe()
		}
		for _, subscription := range programSubscriptions {
			subscription.Unsubscribe()
		}
		if logSubscription != nil {
			logSubscription.Unsubscribe()
		}
	}()
	f.logger.Info("feed batch subscriptions active", "market", f.market, "accounts", len(addresses), "programs", len(requests), "economic_logs", separated)
	slot, err := f.network.CurrentSlot(ctx)
	if err != nil {
		return false, false, err
	}
	f.logger.Info("feed bootstrap started", "market", f.market, "slot", slot)
	started := time.Now()
	data, err := f.decoder.Bootstrap(ctx, f.network, slot)
	if err != nil {
		return false, false, fmt.Errorf("bootstrap %s at slot %d: %w", f.market, slot, err)
	}
	bootstrap := f.event(slot, "bootstrap", data)
	if reset, ok := sink.(feedport.ResetSink); ok {
		err = reset.Reset(ctx, bootstrap)
	} else {
		err = sink.Publish(ctx, bootstrap)
	}
	if err != nil {
		return false, false, err
	}
	if err := sink.SetHealth(ctx, feedport.HealthUpdate{Health: market.HealthHealthy, ObservedAt: f.clock().UTC()}); err != nil {
		return false, false, err
	}
	f.logger.Info("feed bootstrap applied", "market", f.market, "slot", slot, "duration", time.Since(started))

	pending := make(map[uint64][]AccountChange)
	lastUpdate := make(map[uint64]time.Time)
	ready := make(map[uint64]market.MarketEvent)
	pendingTriggers := make(map[uint64][]solana.LogNotification)
	processedSignatures := make(map[string]uint64)
	const retainedBatchSlots uint64 = 512
	prune := func(observed uint64) {
		if observed <= retainedBatchSlots {
			return
		}
		cutoff := observed - retainedBatchSlots
		for candidate := range ready {
			if candidate < cutoff {
				delete(ready, candidate)
			}
		}
		for candidate := range pendingTriggers {
			if candidate < cutoff {
				delete(pendingTriggers, candidate)
			}
		}
		for signature, candidate := range processedSignatures {
			if candidate < cutoff {
				delete(processedSignatures, signature)
			}
		}
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		slots := make([]uint64, 0, len(pending))
		for candidate := range pending {
			slots = append(slots, candidate)
		}
		sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
		for _, candidate := range slots {
			if separated && time.Since(lastUpdate[candidate]) < 25*time.Millisecond {
				continue
			}
			changes := pending[candidate]
			delete(pending, candidate)
			delete(lastUpdate, candidate)
			if candidate < slot {
				continue
			}
			items, decodeErr := decoder.DecodeAccountBatch(ctx, changes)
			if decodeErr != nil {
				return decodeErr
			}
			for _, item := range items {
				if item == nil {
					return fmt.Errorf("decode %s batch at slot %d returned nil event", f.market, candidate)
				}
				stateEvent := f.event(candidate, fmt.Sprintf("account-batch:%d", candidate), item)
				if separated {
					if err := separatedSink.ApplyState(ctx, stateEvent); err != nil {
						return err
					}
					ready[candidate] = stateEvent
				} else if err := sink.Publish(ctx, stateEvent); err != nil {
					return err
				}
			}
			if separated {
				for _, trigger := range pendingTriggers[candidate] {
					stateEvent, ok := ready[candidate]
					if !ok {
						break
					}
					triggerEvent := f.event(candidate, trigger.Signature, stateEvent.Data)
					if err := separatedSink.PublishTrigger(ctx, triggerEvent); err != nil {
						return err
					}
					processedSignatures[trigger.Signature] = candidate
				}
				delete(pendingTriggers, candidate)
			}
		}
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return true, false, ctx.Err()
		case err := <-errorsCh:
			if err == nil {
				err = fmt.Errorf("solana batch account stream closed")
			}
			return true, true, err
		case update := <-updates:
			prune(update.slot)
			pending[update.slot] = append(pending[update.slot], AccountChange{Slot: update.slot, Account: update.account, Value: update.value, Program: update.programDecoder != nil})
			lastUpdate[update.slot] = time.Now()
		case err, open := <-logErrors(logSubscription):
			if separated {
				if !open || err == nil {
					err = fmt.Errorf("solana economic log subscription closed")
				}
				return true, true, err
			}
		case notification, open := <-logNotifications(logSubscription):
			if !separated {
				continue
			}
			if !open {
				return true, true, fmt.Errorf("solana economic log stream closed")
			}
			if notification.Err != nil && string(notification.Err) != "null" || notification.Signature == "" || !matcher.IsEconomicLog(notification) {
				continue
			}
			prune(notification.Slot)
			if _, duplicate := processedSignatures[notification.Signature]; duplicate {
				continue
			}
			duplicatePending := false
			for _, current := range pendingTriggers[notification.Slot] {
				if current.Signature == notification.Signature {
					duplicatePending = true
					break
				}
			}
			if duplicatePending {
				continue
			}
			pendingTriggers[notification.Slot] = append(pendingTriggers[notification.Slot], notification)
			if stateEvent, ok := ready[notification.Slot]; ok {
				triggerEvent := f.event(notification.Slot, notification.Signature, stateEvent.Data)
				if err := separatedSink.PublishTrigger(ctx, triggerEvent); err != nil {
					return true, false, err
				}
				processedSignatures[notification.Signature] = notification.Slot
				delete(pendingTriggers, notification.Slot)
			}
		case <-ticker.C:
			if err := flush(); err != nil {
				return true, false, fmt.Errorf("decode %s account batch: %w", f.market, err)
			}
		}
	}
}

func logErrors(subscription solana.LogsSubscription) <-chan error {
	if subscription == nil {
		return nil
	}
	return subscription.Err()
}

func logNotifications(subscription solana.LogsSubscription) <-chan solana.LogNotification {
	if subscription == nil {
		return nil
	}
	return subscription.Notifications()
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
	f.logger.Info("feed bootstrap applied", "market", f.market, "slot", slot)
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
			f.logger.Debug("feed account event received", "market", f.market, "account", update.account, "slot", update.slot)
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

func forwardBatchAccounts(ctx context.Context, subscription solana.AccountSubscription, updates chan<- accountUpdate, errorsCh chan<- error) {
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
			case updates <- accountUpdate{slot: notification.Slot, account: notification.Account, value: notification.Value, accountDecoder: batchAccountMarker{}}:
			case <-ctx.Done():
				return
			}
		}
	}
}

func forwardBatchPrograms(ctx context.Context, subscription solana.ProgramSubscription, updates chan<- accountUpdate, errorsCh chan<- error) {
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
			case updates <- accountUpdate{slot: notification.Slot, account: notification.Account, value: notification.Value, programDecoder: batchProgramMarker{}}:
			case <-ctx.Done():
				return
			}
		}
	}
}

// Marker implementations identify the source subscription in accountUpdate;
// they are never called by the batch path.
type batchAccountMarker struct{}

func (batchAccountMarker) AccountSubscriptions() []string { return nil }
func (batchAccountMarker) DecodeAccount(context.Context, solana.AccountNotification) ([]market.EventData, error) {
	return nil, nil
}

type batchProgramMarker struct{}

func (batchProgramMarker) ProgramSubscriptions() []solana.ProgramSubscriptionRequest { return nil }
func (batchProgramMarker) DecodeProgram(context.Context, solana.ProgramNotification) ([]market.EventData, error) {
	return nil, nil
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
