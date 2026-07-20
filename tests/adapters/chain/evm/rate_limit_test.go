package evm_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
)

type countingNetwork struct {
	mu    sync.Mutex
	calls int
}

func (*countingNetwork) ID() string { return "test" }
func (n *countingNetwork) CurrentBlock(context.Context) (evm.BlockReference, error) {
	n.mu.Lock()
	n.calls++
	n.mu.Unlock()
	return evm.BlockReference{Number: 1, Hash: common.HexToHash("0x1")}, nil
}
func (*countingNetwork) SubscribeLogs(context.Context, evm.LogFilter, chan<- types.Log) (evm.Subscription, error) {
	return nil, errors.New("not used")
}
func (*countingNetwork) LogsAt(context.Context, evm.BlockReference, evm.LogFilter) ([]types.Log, error) {
	return nil, errors.New("not used")
}
func (*countingNetwork) CallContract(context.Context, evm.BlockReference, geth.CallMsg) ([]byte, error) {
	return nil, errors.New("not used")
}
func (*countingNetwork) CodeAt(context.Context, evm.BlockReference, common.Address) ([]byte, error) {
	return nil, errors.New("not used")
}
func (*countingNetwork) Close() {}

func TestRateLimitedNetworkHonorsCancellationBeforeDelegating(t *testing.T) {
	delegate := &countingNetwork{}
	network, err := evm.NewRateLimitedNetwork(delegate, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := network.CurrentBlock(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := network.CurrentBlock(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled wait, got %v", err)
	}
	delegate.mu.Lock()
	defer delegate.mu.Unlock()
	if delegate.calls != 1 {
		t.Fatalf("delegate calls = %d, want 1", delegate.calls)
	}
}
