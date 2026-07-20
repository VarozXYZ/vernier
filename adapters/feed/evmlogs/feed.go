// Package evmlogs provides a filtered EVM log feed. It subscribes only to one
// market address and its venue-selected topics; it never subscribes to heads
// or infers gaps from block-number jumps.
package evmlogs

import (
	"context"
	"fmt"
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
}

type Feed struct {
	market  market.MarketID
	source  market.SourceID
	network evm.Network
	venue   Venue
	clock   Clock
	retry   RetryPolicy
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
		venue: config.Venue, clock: config.Clock, retry: config.Retry,
	}, nil
}

func (f *Feed) MarketID() market.MarketID { return f.market }

func (f *Feed) Run(ctx context.Context, sink feedport.Sink) error {
	if sink == nil {
		return fmt.Errorf("feed sink is required")
	}
	everBootstrapped := false
	delay := f.retry.Initial
	for {
		established, disconnected, err := f.runSession(ctx, sink)
		if err == nil || ctx.Err() != nil {
			return ctx.Err()
		}
		if !everBootstrapped && !established {
			return err
		}
		if established {
			everBootstrapped = true
		}
		if !disconnected && established {
			return err
		}
		if everBootstrapped {
			if healthErr := sink.SetHealth(ctx, feedport.HealthUpdate{
				Health: market.HealthDegraded, Reason: "websocket_disconnected", ObservedAt: f.clock().UTC(),
			}); healthErr != nil {
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

func (f *Feed) runSession(ctx context.Context, sink feedport.Sink) (established, disconnected bool, result error) {
	logs := make(chan types.Log, 256)
	subscription, err := f.network.SubscribeLogs(ctx, f.venue.Filter(), logs)
	if err != nil {
		return false, true, err
	}
	defer subscription.Unsubscribe()

	block, err := f.network.CurrentBlock(ctx)
	if err != nil {
		return false, false, err
	}
	data, err := f.venue.Bootstrap(ctx, f.network, block)
	if err != nil {
		return false, false, err
	}
	if err := sink.Publish(ctx, f.event(block, data)); err != nil {
		return false, false, err
	}
	if err := sink.SetHealth(ctx, feedport.HealthUpdate{Health: market.HealthHealthy, ObservedAt: f.clock().UTC()}); err != nil {
		return false, false, err
	}
	established = true
	highest := block.Number
	processed := map[common.Hash]struct{}{block.Hash: {}}

	for {
		select {
		case <-ctx.Done():
			return true, false, ctx.Err()
		case err, open := <-subscription.Err():
			if !open || err == nil {
				err = fmt.Errorf("Ethereum log subscription closed")
			}
			return true, true, err
		case observed, open := <-logs:
			if !open {
				return true, true, fmt.Errorf("Ethereum log stream closed")
			}
			if observed.Removed || observed.BlockNumber < highest {
				continue
			}
			if _, duplicate := processed[observed.BlockHash]; duplicate {
				continue
			}
			active := evm.BlockReference{Number: observed.BlockNumber, Hash: observed.BlockHash}
			blockLogs, err := f.network.LogsAt(ctx, active, f.venue.Filter())
			if err != nil {
				return true, false, err
			}
			data, err := f.venue.DecodeBlock(ctx, f.network, active, blockLogs)
			if err != nil {
				return true, false, err
			}
			if err := sink.Publish(ctx, f.event(active, data)); err != nil {
				return true, false, err
			}
			processed[active.Hash] = struct{}{}
			if active.Number > highest {
				highest = active.Number
			}
		}
	}
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
