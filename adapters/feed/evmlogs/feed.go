// Package evmlogs provides a filtered EVM log feed. It subscribes only to one
// market address and its venue-selected topics; it never subscribes to heads
// or infers gaps from block-number jumps.
package evmlogs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
)

const (
	BlockPositionKind      market.SourcePositionKind  = "block"
	BlockHashReferenceKind market.SourceReferenceKind = "evm_block_hash"
)

type Venue interface {
	ID() string
	Filter() evm.LogFilter
	Bootstrap(context.Context, evm.Network, evm.BlockReference) (market.EventData, error)
	DecodeBlock(context.Context, evm.Network, evm.BlockReference, []types.Log) (market.EventData, error)
}

// LogDecoder is an optional venue capability for filtered subscriptions. It
// lets a venue turn each WebSocket log into one normalized event without
// querying the whole block again. The feed keeps DecodeBlock as a compatibility
// fallback for venues that need block-level correlation.
type LogDecoder interface {
	DecodeLog(context.Context, evm.Network, evm.BlockReference, types.Log) (market.EventData, error)
}

type Clock func() time.Time

type RetryPolicy struct {
	Initial time.Duration
	Maximum time.Duration
}

type Config struct {
	Market  market.MarketID
	Source  market.SourceID
	Network evm.Network
	Venue   Venue
	Clock   Clock
	Retry   RetryPolicy
	Logger  *slog.Logger
}

type Feed struct {
	market  market.MarketID
	source  market.SourceID
	network evm.Network
	venue   Venue
	clock   Clock
	retry   RetryPolicy
	logger  *slog.Logger
}

func New(config Config) (*Feed, error) {
	if config.Market == "" || config.Source == "" || config.Network == nil || config.Venue == nil {
		return nil, fmt.Errorf("market, source, network, and venue are required")
	}
	filter := config.Venue.Filter()
	if filter.Address == (common.Address{}) || len(filter.Topics) == 0 {
		return nil, fmt.Errorf("venue requires a filtered address and topics")
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
	return &Feed{
		market: config.Market, source: config.Source, network: config.Network,
		venue: config.Venue, clock: config.Clock, retry: config.Retry, logger: config.Logger,
	}, nil
}

func (f *Feed) MarketID() market.MarketID { return f.market }

func (f *Feed) Run(ctx context.Context, sink feedport.Sink) error {
	if sink == nil {
		return fmt.Errorf("feed sink is required")
	}
	everBootstrapped := false
	delay := f.retry.Initial
	f.logger.Info("feed run started", "market", f.market, "source", f.source)
	for {
		f.logger.Debug("feed session starting", "market", f.market)
		established, disconnected, err := f.runSession(ctx, sink)
		if err == nil || ctx.Err() != nil {
			f.logger.Debug("feed run stopped", "market", f.market, "reason", ctx.Err())
			return ctx.Err()
		}
		if !everBootstrapped && !established {
			return err
		}
		if established {
			everBootstrapped = true
			delay = f.retry.Initial
		}
		if !disconnected && established {
			f.logger.Error("feed session stopped without a disconnect", "market", f.market, "error", err)
			return err
		}
		if everBootstrapped {
			f.logger.Warn("feed WebSocket disconnected", "market", f.market, "error", err)
			if healthErr := sink.SetHealth(ctx, feedport.HealthUpdate{
				Health: market.HealthDegraded, Reason: "websocket_disconnected", ObservedAt: f.clock().UTC(),
			}); healthErr != nil {
				return healthErr
			}
		}
		if err := wait(ctx, delay); err != nil {
			return err
		}
		f.logger.Info("feed reconnecting", "market", f.market, "retry_delay", delay)
		delay *= 2
		if delay > f.retry.Maximum {
			delay = f.retry.Maximum
		}
	}
}

func (f *Feed) runSession(ctx context.Context, sink feedport.Sink) (established, disconnected bool, result error) {
	logs := make(chan types.Log, 256)
	f.logger.Debug("feed subscribing to filtered logs", "market", f.market)
	subscription, err := f.network.SubscribeLogs(ctx, f.venue.Filter(), logs)
	if err != nil {
		return false, true, err
	}
	defer subscription.Unsubscribe()

	block, err := f.network.CurrentBlock(ctx)
	if err != nil {
		return false, false, err
	}
	f.logger.Info("feed bootstrap started", "market", f.market, "block", block.Number)
	bootstrapStarted := time.Now()
	data, err := f.venue.Bootstrap(ctx, f.network, block)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return false, false, err
		}
		f.logger.Error("feed bootstrap failed", "market", f.market, "block", block.Number, "duration", time.Since(bootstrapStarted), "error", err)
		return false, false, err
	}
	f.logger.Info("feed bootstrap completed", "market", f.market, "block", block.Number, "duration", time.Since(bootstrapStarted))
	bootstrapEvent := f.event(block, data)
	if resetSink, ok := sink.(feedport.ResetSink); ok {
		if err := resetSink.Reset(ctx, bootstrapEvent); err != nil {
			return false, false, err
		}
	} else if err := sink.Publish(ctx, bootstrapEvent); err != nil {
		return false, false, err
	}
	if err := sink.SetHealth(ctx, feedport.HealthUpdate{Health: market.HealthHealthy, ObservedAt: f.clock().UTC()}); err != nil {
		return false, false, err
	}
	established = true
	f.logger.Info("feed bootstrap applied", "market", f.market, "block", block.Number)
	highest := block.Number
	var logDecoder LogDecoder
	if decoder, ok := f.venue.(LogDecoder); ok {
		logDecoder = decoder
	}
	processedLogs := make(map[logIdentity]struct{})
	processedBlocks := map[common.Hash]struct{}{block.Hash: {}}

	for {
		select {
		case <-ctx.Done():
			return true, false, ctx.Err()
		case err, open := <-subscription.Err():
			if !open || err == nil {
				err = fmt.Errorf("ethereum log subscription closed")
			}
			return true, true, err
		case observed, open := <-logs:
			if !open {
				return true, true, fmt.Errorf("ethereum log stream closed")
			}
			if observed.Removed || observed.BlockNumber < highest {
				f.logger.Debug("feed log ignored", "market", f.market, "block", observed.BlockNumber, "reason", ignoredLogReason(observed, highest))
				continue
			}
			active := evm.BlockReference{Number: observed.BlockNumber, Hash: observed.BlockHash}
			if logDecoder != nil {
				key := identity(observed)
				if _, duplicate := processedLogs[key]; duplicate {
					f.logger.Debug("feed log ignored", "market", f.market, "block", active.Number, "tx_hash", observed.TxHash.Hex(), "log_index", observed.Index, "reason", "duplicate_log")
					continue
				}
				f.logger.Debug("feed event received", "market", f.market, "block", active.Number, "tx_hash", observed.TxHash.Hex(), "log_index", observed.Index)
				eventStarted := time.Now()
				data, decodeErr := logDecoder.DecodeLog(ctx, f.network, active, observed)
				if decodeErr != nil {
					if errors.Is(decodeErr, context.Canceled) {
						return true, false, decodeErr
					}
					f.logger.Error("feed event decode failed", "market", f.market, "block", active.Number, "tx_hash", observed.TxHash.Hex(), "log_index", observed.Index, "duration", time.Since(eventStarted), "error", decodeErr)
					return true, false, fmt.Errorf("decode %s event at block %d index %d: %w", f.market, active.Number, observed.Index, decodeErr)
				}
				if err := sink.Publish(ctx, f.event(active, data)); err != nil {
					return true, false, err
				}
				processedLogs[key] = struct{}{}
				f.logger.Info("feed event applied", "market", f.market, "block", active.Number, "tx_hash", observed.TxHash.Hex(), "log_index", observed.Index, "duration", time.Since(eventStarted))
				if active.Number > highest {
					highest = active.Number
				}
				continue
			}
			if _, duplicate := processedBlocks[observed.BlockHash]; duplicate {
				f.logger.Debug("feed block ignored", "market", f.market, "block", observed.BlockNumber, "reason", "duplicate_block")
				continue
			}
			f.logger.Debug("feed block received", "market", f.market, "block", active.Number)
			blockStarted := time.Now()
			blockLogs, err := f.network.LogsAt(ctx, active, f.venue.Filter())
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return true, false, err
				}
				f.logger.Error("feed block log query failed", "market", f.market, "block", active.Number, "duration", time.Since(blockStarted), "error", err)
				return true, false, fmt.Errorf("read %s logs at block %d: %w", f.market, active.Number, err)
			}
			data, err := f.venue.DecodeBlock(ctx, f.network, active, blockLogs)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return true, false, err
				}
				f.logger.Error("feed block decode failed", "market", f.market, "block", active.Number, "logs", len(blockLogs), "duration", time.Since(blockStarted), "error", err)
				return true, false, fmt.Errorf("decode %s block %d: %w", f.market, active.Number, err)
			}
			if err := sink.Publish(ctx, f.event(active, data)); err != nil {
				return true, false, err
			}
			processedBlocks[active.Hash] = struct{}{}
			f.logger.Info("feed block applied", "market", f.market, "block", active.Number, "logs", len(blockLogs), "duration", time.Since(blockStarted))
			if active.Number > highest {
				highest = active.Number
			}
		}
	}
}

type logIdentity struct {
	blockHash common.Hash
	txHash    common.Hash
	index     uint
}

func identity(log types.Log) logIdentity {
	return logIdentity{blockHash: log.BlockHash, txHash: log.TxHash, index: log.Index}
}

func ignoredLogReason(observed types.Log, highest uint64) string {
	if observed.Removed {
		return "removed_log"
	}
	if observed.BlockNumber < highest {
		return "older_block"
	}
	return "ignored"
}

func (f *Feed) event(block evm.BlockReference, data market.EventData) market.MarketEvent {
	event, err := market.NewMarketEvent(market.MarketEvent{
		Market: f.market, Source: f.source,
		Position:  market.SourcePosition{Kind: BlockPositionKind, Value: block.Number},
		Reference: market.SourceReference{Kind: BlockHashReferenceKind, Value: block.Hash.Hex()},
		Finality:  market.FinalityPreconfirmed, ReceivedAt: f.clock().UTC(), Data: data,
	})
	if err != nil {
		panic(fmt.Sprintf("evmlogs constructed invalid event: %v", err))
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
