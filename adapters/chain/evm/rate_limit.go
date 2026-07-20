package evm

import (
	"context"
	"fmt"
	"sync"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// RateLimitedNetwork spaces RPC operations without coupling a venue adapter to
// provider-specific request limits. Log delivery itself is never delayed.
type RateLimitedNetwork struct {
	delegate Network
	interval time.Duration
	mu       sync.Mutex
	next     time.Time
}

func NewRateLimitedNetwork(delegate Network, minimumInterval time.Duration) (*RateLimitedNetwork, error) {
	if delegate == nil || minimumInterval < 0 {
		return nil, fmt.Errorf("network and non-negative request interval are required")
	}
	return &RateLimitedNetwork{delegate: delegate, interval: minimumInterval}, nil
}

func (n *RateLimitedNetwork) ID() string { return n.delegate.ID() }

func (n *RateLimitedNetwork) CurrentBlock(ctx context.Context) (BlockReference, error) {
	if err := n.wait(ctx); err != nil {
		return BlockReference{}, err
	}
	return n.delegate.CurrentBlock(ctx)
}

func (n *RateLimitedNetwork) SubscribeLogs(
	ctx context.Context,
	filter LogFilter,
	output chan<- types.Log,
) (Subscription, error) {
	if err := n.wait(ctx); err != nil {
		return nil, err
	}
	return n.delegate.SubscribeLogs(ctx, filter, output)
}

func (n *RateLimitedNetwork) LogsAt(
	ctx context.Context,
	block BlockReference,
	filter LogFilter,
) ([]types.Log, error) {
	if err := n.wait(ctx); err != nil {
		return nil, err
	}
	return n.delegate.LogsAt(ctx, block, filter)
}

func (n *RateLimitedNetwork) CallContract(
	ctx context.Context,
	block BlockReference,
	call geth.CallMsg,
) ([]byte, error) {
	if err := n.wait(ctx); err != nil {
		return nil, err
	}
	return n.delegate.CallContract(ctx, block, call)
}

func (n *RateLimitedNetwork) CodeAt(
	ctx context.Context,
	block BlockReference,
	address common.Address,
) ([]byte, error) {
	if err := n.wait(ctx); err != nil {
		return nil, err
	}
	return n.delegate.CodeAt(ctx, block, address)
}

// Close is deliberately delegated so the wrapper remains a complete Network.
func (n *RateLimitedNetwork) Close() { n.delegate.Close() }

func (n *RateLimitedNetwork) wait(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	now := time.Now()
	if delay := time.Until(n.next); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		now = time.Now()
	}
	n.next = now.Add(n.interval)
	return nil
}

var _ Network = (*RateLimitedNetwork)(nil)
