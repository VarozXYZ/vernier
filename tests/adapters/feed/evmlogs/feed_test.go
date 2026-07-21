package evmlogs_test

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/adapters/feed/evmlogs"
	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
)

type blockData uint64

func (blockData) EventKind() string { return "test/block" }

type subscription struct{ errors chan error }

func (s *subscription) Err() <-chan error { return s.errors }
func (s *subscription) Unsubscribe()      {}

type network struct {
	mu              sync.Mutex
	current         []evm.BlockReference
	sessionLogs     [][]types.Log
	sessionErrors   []error
	subscriptions   int
	currentCalls    int
	logBlocks       []uint64
	observedFilters []evm.LogFilter
}

func (n *network) ID() string { return "fake" }

func (n *network) CurrentBlock(context.Context) (evm.BlockReference, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	index := n.currentCalls
	n.currentCalls++
	if index >= len(n.current) {
		index = len(n.current) - 1
	}
	return n.current[index], nil
}

func (n *network) SubscribeLogs(_ context.Context, filter evm.LogFilter, output chan<- types.Log) (evm.Subscription, error) {
	n.mu.Lock()
	index := n.subscriptions
	n.subscriptions++
	n.observedFilters = append(n.observedFilters, filter)
	var logs []types.Log
	if index < len(n.sessionLogs) {
		logs = n.sessionLogs[index]
	}
	var sessionErr error
	if index < len(n.sessionErrors) {
		sessionErr = n.sessionErrors[index]
	}
	n.mu.Unlock()

	result := &subscription{errors: make(chan error, 1)}
	for _, event := range logs {
		output <- event
	}
	if sessionErr != nil {
		result.errors <- sessionErr
	}
	return result, nil
}

func (n *network) LogsAt(_ context.Context, block evm.BlockReference, _ evm.LogFilter) ([]types.Log, error) {
	n.mu.Lock()
	n.logBlocks = append(n.logBlocks, block.Number)
	n.mu.Unlock()
	return []types.Log{{BlockNumber: block.Number, BlockHash: block.Hash}}, nil
}

func (*network) CallContract(context.Context, evm.BlockReference, geth.CallMsg) ([]byte, error) {
	return nil, nil
}

func (*network) CodeAt(context.Context, evm.BlockReference, common.Address) ([]byte, error) {
	return []byte{1}, nil
}

func (*network) Close() {}

type venue struct {
	filter     evm.LogFilter
	bootstraps []uint64
	decoded    []uint64
}

type singleLogVenue struct {
	venue
	decodedLogs []types.Log
}

func (*venue) ID() string              { return "test" }
func (v *venue) Filter() evm.LogFilter { return v.filter }

func (v *venue) Bootstrap(_ context.Context, _ evm.Network, block evm.BlockReference) (market.EventData, error) {
	v.bootstraps = append(v.bootstraps, block.Number)
	return blockData(block.Number), nil
}

func (v *venue) DecodeBlock(_ context.Context, _ evm.Network, block evm.BlockReference, _ []types.Log) (market.EventData, error) {
	v.decoded = append(v.decoded, block.Number)
	return blockData(block.Number), nil
}

func (v *singleLogVenue) DecodeLog(_ context.Context, _ evm.Network, _ evm.BlockReference, log types.Log) (market.EventData, error) {
	v.decodedLogs = append(v.decodedLogs, log)
	return blockData(log.BlockNumber*100 + uint64(log.Index)), nil
}

type sink struct {
	cancel   context.CancelFunc
	cancelAt int
	events   []market.MarketEvent
	health   []feedport.HealthUpdate
}

func (s *sink) Publish(_ context.Context, event market.MarketEvent) error {
	s.events = append(s.events, event)
	if s.cancel != nil && len(s.events) == s.cancelAt {
		s.cancel()
	}
	return nil
}

func (s *sink) SetHealth(_ context.Context, update feedport.HealthUpdate) error {
	s.health = append(s.health, update)
	return nil
}

func TestFilteredFeedProcessesOnlyActiveBlocksWithoutGapInference(t *testing.T) {
	address := common.HexToAddress("0x1000000000000000000000000000000000000001")
	topic := common.HexToHash("0x01")
	hash10, hash15, hash20 := common.BigToHash(big.NewInt(10)), common.BigToHash(big.NewInt(15)), common.BigToHash(big.NewInt(20))
	chain := &network{
		current: []evm.BlockReference{{Number: 10, Hash: hash10}},
		sessionLogs: [][]types.Log{{
			{BlockNumber: 15, BlockHash: hash15},
			{BlockNumber: 15, BlockHash: hash15},
			{BlockNumber: 14, BlockHash: common.BigToHash(big.NewInt(14))},
			{BlockNumber: 20, BlockHash: hash20},
		}},
	}
	protocol := &venue{filter: evm.LogFilter{Address: address, Topics: []common.Hash{topic}}}
	ctx, cancel := context.WithCancel(context.Background())
	collector := &sink{cancel: cancel, cancelAt: 3}
	feed, err := evmlogs.New(evmlogs.Config{
		Market: "market", Source: "source", Network: chain, Venue: protocol,
		Clock: func() time.Time { return time.Unix(1, 0) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := feed.Run(ctx, collector); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if got := positions(collector.events); !equalUint64(got, []uint64{10, 15, 20}) {
		t.Fatalf("published blocks %v", got)
	}
	if !equalUint64(chain.logBlocks, []uint64{15, 20}) {
		t.Fatalf("exact-block queries %v", chain.logBlocks)
	}
	if !equalUint64(protocol.bootstraps, []uint64{10}) {
		t.Fatalf("unexpected full loads %v", protocol.bootstraps)
	}
	if len(chain.observedFilters) != 1 || chain.observedFilters[0].Address != address ||
		len(chain.observedFilters[0].Topics) != 1 || chain.observedFilters[0].Topics[0] != topic {
		t.Fatalf("subscription was not narrowly filtered: %+v", chain.observedFilters)
	}
}

func TestDisconnectIsTheOnlyFeedDegradationAndReloadsOnReconnect(t *testing.T) {
	chain := &network{
		current: []evm.BlockReference{
			{Number: 10, Hash: common.BigToHash(big.NewInt(10))},
			{Number: 20, Hash: common.BigToHash(big.NewInt(20))},
		},
		sessionLogs:   [][]types.Log{{}, {}},
		sessionErrors: []error{errors.New("socket lost"), nil},
	}
	protocol := &venue{filter: evm.LogFilter{
		Address: common.HexToAddress("0x1000000000000000000000000000000000000001"),
		Topics:  []common.Hash{common.HexToHash("0x01")},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	collector := &sink{cancel: cancel, cancelAt: 2}
	feed, err := evmlogs.New(evmlogs.Config{
		Market: "market", Source: "source", Network: chain, Venue: protocol,
		Retry: evmlogs.RetryPolicy{Initial: time.Millisecond, Maximum: time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := feed.Run(ctx, collector); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if !equalUint64(protocol.bootstraps, []uint64{10, 20}) {
		t.Fatalf("bootstrap blocks %v", protocol.bootstraps)
	}
	var states []market.Health
	for _, update := range collector.health {
		states = append(states, update.Health)
	}
	want := []market.Health{market.HealthHealthy, market.HealthDegraded, market.HealthHealthy}
	if len(states) != len(want) {
		t.Fatalf("health changes %v", states)
	}
	for index := range want {
		if states[index] != want[index] {
			t.Fatalf("health changes %v", states)
		}
	}
}

func TestFilteredFeedAppliesSameBlockLogsInArrivalOrder(t *testing.T) {
	address := common.HexToAddress("0x1000000000000000000000000000000000000001")
	topic := common.HexToHash("0x01")
	hash10 := common.BigToHash(big.NewInt(10))
	hash15 := common.BigToHash(big.NewInt(15))
	chain := &network{
		current: []evm.BlockReference{{Number: 10, Hash: hash10}},
		sessionLogs: [][]types.Log{{
			{BlockNumber: 15, BlockHash: hash15, TxHash: common.BigToHash(big.NewInt(2)), Index: 2},
			{BlockNumber: 15, BlockHash: hash15, TxHash: common.BigToHash(big.NewInt(1)), Index: 1},
			{BlockNumber: 15, BlockHash: hash15, TxHash: common.BigToHash(big.NewInt(2)), Index: 2},
			{BlockNumber: 14, BlockHash: common.BigToHash(big.NewInt(14)), TxHash: common.BigToHash(big.NewInt(3)), Index: 0},
		}},
	}
	protocol := &singleLogVenue{venue: venue{filter: evm.LogFilter{Address: address, Topics: []common.Hash{topic}}}}
	ctx, cancel := context.WithCancel(context.Background())
	collector := &sink{cancel: cancel, cancelAt: 3}
	feed, err := evmlogs.New(evmlogs.Config{Market: "market", Source: "source", Network: chain, Venue: protocol})
	if err != nil {
		t.Fatal(err)
	}
	if err := feed.Run(ctx, collector); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if got := positions(collector.events); !equalUint64(got, []uint64{10, 15, 15}) {
		t.Fatalf("published blocks %v", got)
	}
	if len(protocol.decodedLogs) != 2 || protocol.decodedLogs[0].Index != 2 || protocol.decodedLogs[1].Index != 1 {
		t.Fatalf("logs were not decoded in arrival order: %+v", protocol.decodedLogs)
	}
	if len(chain.logBlocks) != 0 {
		t.Fatalf("single-log decoder unexpectedly queried exact blocks: %v", chain.logBlocks)
	}
}

func positions(events []market.MarketEvent) []uint64 {
	result := make([]uint64, len(events))
	for index, event := range events {
		result[index] = event.Position.Value
		if event.Reference.Kind != evmlogs.BlockHashReferenceKind {
			panic("missing block hash reference")
		}
	}
	return result
}

func equalUint64(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
