package evmlogs

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
)

const multiBootstrapLogBuffer = 8192

// MultiTarget binds one fixed pool address to its normalized market decoder.
type MultiTarget struct {
	Market                    market.MarketID
	Source                    market.SourceID
	Venue                     Venue
	SuppressEvaluationTrigger bool
}

type MultiConfig struct {
	Market     market.MarketID
	Network    evm.Network
	Targets    []MultiTarget
	Clock      Clock
	Retry      RetryPolicy
	FlushDelay time.Duration
	// TransactionBoundaryOnly publishes a transaction only after the ordered
	// stream exposes a different transaction hash. It avoids heuristic idle
	// timers for execution-sensitive composite markets.
	TransactionBoundaryOnly bool
	Logger                  *slog.Logger
}

// MultiFeed subscribes once to a fixed address allowlist and emits one event
// batch per EVM transaction. It is intended for composed local routes where a
// multihop swap can mutate more than one watched pool.
type MultiFeed struct {
	market       market.MarketID
	network      evm.Network
	targets      map[common.Address]MultiTarget
	ordered      []MultiTarget
	filter       evm.LogFilter
	clock        Clock
	retry        RetryPolicy
	flushDelay   time.Duration
	boundaryOnly bool
	logger       *slog.Logger
}

func NewMulti(config MultiConfig) (*MultiFeed, error) {
	if config.Market == "" || config.Network == nil || len(config.Targets) < 2 {
		return nil, fmt.Errorf("multi EVM feed requires market, network, and at least two targets")
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
	if config.FlushDelay == 0 {
		config.FlushDelay = 10 * time.Millisecond
	}
	if config.FlushDelay < 0 || config.FlushDelay > 10*time.Millisecond ||
		config.Retry.Initial < 0 || config.Retry.Maximum < config.Retry.Initial {
		return nil, fmt.Errorf("multi EVM feed timing policy is invalid")
	}
	targets := make(map[common.Address]MultiTarget, len(config.Targets))
	addresses := make([]common.Address, 0, len(config.Targets))
	topicSet := make(map[common.Hash]struct{})
	ordered := append([]MultiTarget(nil), config.Targets...)
	for _, target := range ordered {
		if target.Market == "" || target.Source == "" || target.Venue == nil {
			return nil, fmt.Errorf("multi EVM feed target is incomplete")
		}
		filter := target.Venue.Filter()
		if filter.Address == (common.Address{}) || len(filter.Addresses) != 0 || len(filter.Topics) == 0 {
			return nil, fmt.Errorf("multi EVM feed target requires one fixed address and topics")
		}
		if _, duplicate := targets[filter.Address]; duplicate {
			return nil, fmt.Errorf("multi EVM feed repeats pool %s", filter.Address.Hex())
		}
		if _, ok := target.Venue.(LogDecoder); !ok {
			return nil, fmt.Errorf("multi EVM feed target %q has no log decoder", target.Market)
		}
		targets[filter.Address] = target
		addresses = append(addresses, filter.Address)
		for _, topic := range filter.Topics {
			topicSet[topic] = struct{}{}
		}
	}
	topics := make([]common.Hash, 0, len(topicSet))
	for topic := range topicSet {
		topics = append(topics, topic)
	}
	sort.Slice(topics, func(i, j int) bool { return topics[i].Hex() < topics[j].Hex() })
	return &MultiFeed{market: config.Market, network: config.Network, targets: targets,
		ordered: ordered, filter: evm.LogFilter{Addresses: addresses, Topics: topics},
		clock: config.Clock, retry: config.Retry, flushDelay: config.FlushDelay,
		boundaryOnly: config.TransactionBoundaryOnly, logger: config.Logger}, nil
}

func (f *MultiFeed) MarketID() market.MarketID { return f.market }

func (f *MultiFeed) Run(ctx context.Context, sink feedport.Sink) error {
	batchSink, ok := sink.(feedport.TransactionBatchSink)
	if !ok {
		return fmt.Errorf("multi EVM feed requires a transaction batch sink")
	}
	for _, target := range f.ordered {
		if target.SuppressEvaluationTrigger {
			if _, ok := sink.(feedport.StateOnlyBatchSink); !ok {
				return fmt.Errorf("multi EVM feed with state-only targets requires a state-only batch sink")
			}
			break
		}
	}
	everBootstrapped := false
	delay := f.retry.Initial
	for {
		established, disconnected, err := f.runSession(ctx, sink, batchSink)
		if err == nil || ctx.Err() != nil {
			return ctx.Err()
		}
		if !everBootstrapped && !established {
			return err
		}
		if established {
			everBootstrapped, delay = true, f.retry.Initial
		}
		if !disconnected {
			return err
		}
		if everBootstrapped {
			f.logger.Warn("multi feed session interrupted", "market", f.market, "error", err)
			if healthErr := sink.SetHealth(ctx, feedport.HealthUpdate{Health: market.HealthDegraded,
				Reason: "websocket_disconnected", ObservedAt: f.clock().UTC()}); healthErr != nil {
				return healthErr
			}
		}
		if err := wait(ctx, delay); err != nil {
			return err
		}
		delay *= 2
		if delay > f.retry.Maximum {
			delay = f.retry.Maximum
		}
	}
}

func (f *MultiFeed) runSession(ctx context.Context, sink feedport.Sink,
	batchSink feedport.TransactionBatchSink) (established, disconnected bool, result error) {
	// Subscribe before the immutable bootstrap so no intervening state is
	// missed. Composite V3 bootstraps may be deliberately rate-limited, hence
	// the bounded buffer must absorb a busy pool until catch-up begins.
	logs := make(chan types.Log, multiBootstrapLogBuffer)
	subscription, err := f.network.SubscribeLogs(ctx, f.filter, logs)
	if err != nil {
		return false, true, err
	}
	defer subscription.Unsubscribe()
	block, err := f.network.CurrentBlock(ctx)
	if err != nil {
		return false, false, err
	}
	f.logger.Info("multi feed bootstrap started", "market", f.market, "targets", len(f.ordered), "block", block.Number)
	started := time.Now()
	bootstrapData := make([]market.EventData, len(f.ordered))
	bootstrapErrors := make([]error, len(f.ordered))
	var bootstrapGroup sync.WaitGroup
	for index, target := range f.ordered {
		index, target := index, target
		bootstrapGroup.Add(1)
		go func() {
			defer bootstrapGroup.Done()
			bootstrapData[index], bootstrapErrors[index] = target.Venue.Bootstrap(ctx, f.network, block)
		}()
	}
	bootstrapGroup.Wait()
	synchronizationSink, synchronizes := sink.(feedport.SynchronizationSink)
	for index, target := range f.ordered {
		if bootstrapErrors[index] != nil {
			return false, false, fmt.Errorf("bootstrap %s: %w", target.Market, bootstrapErrors[index])
		}
		data := bootstrapData[index]
		event := normalizedMultiEvent(target, block, common.Hash{}, f.clock().UTC(), data)
		if synchronizes {
			if err := synchronizationSink.ResetSynchronized(ctx, event); err != nil {
				return false, false, err
			}
		} else if reset, ok := sink.(feedport.ResetSink); ok {
			if err := reset.Reset(ctx, event); err != nil {
				return false, false, err
			}
		} else if err := sink.Publish(ctx, event); err != nil {
			return false, false, err
		}
	}
	f.logger.Info("multi feed bootstrap completed", "market", f.market, "targets", len(f.ordered),
		"block", block.Number, "duration", time.Since(started))
	catchupThrough, err := f.network.CurrentBlock(ctx)
	if err != nil {
		return false, false, err
	}
	caughtUp := catchupThrough.Number <= block.Number
	if !caughtUp {
		if err := sink.SetHealth(ctx, feedport.HealthUpdate{Health: market.HealthDegraded,
			Reason: "bootstrap_catchup", ObservedAt: f.clock().UTC()}); err != nil {
			return false, false, err
		}
	} else if err := sink.SetHealth(ctx, feedport.HealthUpdate{Health: market.HealthHealthy,
		ObservedAt: f.clock().UTC()}); err != nil {
		return false, false, err
	}
	established = true
	highest := block.Number
	processed := make(map[logIdentity]struct{})
	publishedTransactions := make(map[common.Hash]struct{})
	pendingSeen := make(map[logIdentity]struct{})
	var pending []types.Log
	var pendingTx common.Hash
	var timer *time.Timer
	var timerC <-chan time.Time
	stopTimer := func() {
		if timer != nil && !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerC = nil
	}
	flush := func() error {
		stopTimer()
		if len(pending) == 0 {
			return nil
		}
		sort.Slice(pending, func(i, j int) bool { return pending[i].Index < pending[j].Index })
		events := make([]market.MarketEvent, 0, len(pending))
		triggersEvaluation := false
		var triggerEvent market.MarketEvent
		for _, observed := range pending {
			target, ok := f.targets[observed.Address]
			if !ok {
				return fmt.Errorf("received log for unconfigured pool")
			}
			decoder := target.Venue.(LogDecoder)
			active := evm.BlockReference{Number: observed.BlockNumber, Hash: observed.BlockHash}
			data, err := decoder.DecodeLog(ctx, f.network, active, observed)
			if err != nil {
				return err
			}
			event := normalizedMultiEvent(target, active, observed.TxHash, f.clock().UTC(), data)
			events = append(events, event)
			if !target.SuppressEvaluationTrigger {
				triggersEvaluation = true
				triggerEvent = event
			}
			processed[identity(observed)] = struct{}{}
		}
		pendingBlock, completedTx := pending[len(pending)-1].BlockNumber, pendingTx
		pending, pendingTx = nil, common.Hash{}
		clear(pendingSeen)
		if !caughtUp && pendingBlock <= catchupThrough.Number && synchronizes {
			if err := synchronizationSink.PublishBatchSynchronized(ctx, events); err != nil {
				return err
			}
			publishedTransactions[completedTx] = struct{}{}
			return nil
		}
		if !caughtUp {
			if err := sink.SetHealth(ctx, feedport.HealthUpdate{Health: market.HealthHealthy,
				ObservedAt: f.clock().UTC()}); err != nil {
				return err
			}
			caughtUp = true
			f.logger.Info("multi feed catch-up completed", "market", f.market,
				"bootstrap_block", block.Number, "watermark_block", catchupThrough.Number,
				"live_block", pendingBlock)
		}
		if triggersEvaluation {
			var err error
			if causal, ok := sink.(feedport.CausalTransactionBatchSink); ok {
				err = causal.PublishBatchTriggered(ctx, events, triggerEvent)
			} else {
				err = batchSink.PublishBatch(ctx, events)
			}
			if err != nil {
				return err
			}
		} else {
			stateOnly := sink.(feedport.StateOnlyBatchSink)
			if err := stateOnly.PublishBatchStateOnly(ctx, events); err != nil {
				return err
			}
		}
		publishedTransactions[completedTx] = struct{}{}
		return nil
	}
	defer stopTimer()
	for {
		select {
		case <-ctx.Done():
			return true, false, ctx.Err()
		case <-timerC:
			if err := flush(); err != nil {
				return true, false, err
			}
		case err, open := <-subscription.Err():
			if !open || err == nil {
				err = fmt.Errorf("ethereum log subscription closed")
			}
			return true, true, err
		case observed, open := <-logs:
			if !open {
				return true, true, fmt.Errorf("ethereum log stream closed")
			}
			if observed.Removed || observed.BlockNumber < highest || observed.BlockNumber == block.Number {
				continue
			}
			if _, duplicate := processed[identity(observed)]; duplicate {
				continue
			}
			if _, late := publishedTransactions[observed.TxHash]; late {
				return true, true, fmt.Errorf("received a late log for already published transaction %s", observed.TxHash.Hex())
			}
			if _, duplicate := pendingSeen[identity(observed)]; duplicate {
				continue
			}
			if len(pending) > 0 && observed.TxHash != pendingTx {
				if err := flush(); err != nil {
					return true, false, err
				}
			}
			pendingTx = observed.TxHash
			pending = append(pending, observed)
			pendingSeen[identity(observed)] = struct{}{}
			if observed.BlockNumber > highest {
				highest = observed.BlockNumber
			}
			if !f.boundaryOnly {
				stopTimer()
				timer = time.NewTimer(f.flushDelay)
				timerC = timer.C
			}
		}
	}
}

func normalizedMultiEvent(target MultiTarget, block evm.BlockReference, tx common.Hash,
	received time.Time, data market.EventData) market.MarketEvent {
	reference := market.SourceReference{Kind: BlockHashReferenceKind, Value: block.Hash.Hex()}
	if tx != (common.Hash{}) {
		reference = market.SourceReference{Kind: TransactionReferenceKind, Value: tx.Hex()}
	}
	event, err := market.NewMarketEvent(market.MarketEvent{Market: target.Market, Source: target.Source,
		Position: market.SourcePosition{Kind: BlockPositionKind, Value: block.Number}, Reference: reference,
		Finality: market.FinalityPreconfirmed, ReceivedAt: received, Data: data})
	if err != nil {
		panic(fmt.Sprintf("multi EVM feed constructed invalid event: %v", err))
	}
	return event
}

var _ feedport.Feed = (*MultiFeed)(nil)
