package evm

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/types"

	"github.com/VarozXYZ/vernier/domain/execution"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
)

// SettlementLogDecoder is implemented by the private, contract-specific
// adapter. The shared EVM transport owns subscription lifecycle and matching
// by transaction hash; the decoder owns ABI semantics and economic amounts.
type SettlementLogDecoder interface {
	Filter() LogFilter
	DecodeLog(execution.OperationStep, types.Log) (execution.Settlement, bool, error)
}

// ReceiptSettlementDecoder extracts the same economic evidence from an RPC
// receipt during post-broadcast reconciliation.
type ReceiptSettlementDecoder interface {
	DecodeReceipt(execution.OperationStep, *types.Receipt) (execution.Settlement, error)
}

type ConfirmationSourceConfig struct {
	Network Network
	Decoder SettlementLogDecoder
	Clock   func() time.Time
}

// ConfirmationSource keeps the contract-event subscription open before any
// broadcast. Logs arriving before Await are buffered by transaction hash.
type ConfirmationSource struct {
	config ConfirmationSourceConfig

	mu      sync.Mutex
	started bool
	closed  bool
	err     error
	logs    map[string][]types.Log
	waiters map[string][]chan struct{}
}

func NewConfirmationSource(config ConfirmationSourceConfig) (*ConfirmationSource, error) {
	if config.Network == nil || config.Decoder == nil || config.Clock == nil {
		return nil, fmt.Errorf("EVM confirmation source requires network, decoder, and clock")
	}
	filter := config.Decoder.Filter()
	if len(filter.Query(nil).Addresses) == 0 || len(filter.Topics) == 0 {
		return nil, fmt.Errorf("EVM settlement event filter requires addresses and topic")
	}
	return &ConfirmationSource{
		config:  config,
		logs:    make(map[string][]types.Log),
		waiters: make(map[string][]chan struct{}),
	}, nil
}

func (s *ConfirmationSource) Warm(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		err := s.err
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()

	output := make(chan types.Log, 32)
	subscription, err := s.config.Network.SubscribeLogs(ctx, s.config.Decoder.Filter(), output)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		subscription.Unsubscribe()
		return nil
	}
	s.started = true
	s.mu.Unlock()
	go s.readLoop(ctx, subscription, output)
	return nil
}

func (s *ConfirmationSource) Await(ctx context.Context, step execution.OperationStep) (execution.Settlement, error) {
	if err := step.Identity.Validate(); err != nil {
		return execution.Settlement{}, err
	}
	key := strings.ToLower(step.Identity.Hash)
	wake := make(chan struct{}, 1)
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return execution.Settlement{}, fmt.Errorf("EVM confirmation source is not warmed")
	}
	s.waiters[key] = append(s.waiters[key], wake)
	s.mu.Unlock()
	defer s.removeWaiter(key, wake)

	for {
		logs, terminalErr := s.takeLogs(key)
		for _, observed := range logs {
			settlement, matched, err := s.config.Decoder.DecodeLog(step, observed)
			if err != nil {
				return execution.Settlement{}, err
			}
			if !matched {
				continue
			}
			if settlement.ObservedAt.IsZero() {
				settlement.ObservedAt = s.config.Clock().UTC()
			}
			if settlement.Evidence == "" {
				settlement.Evidence = "evm_contract_event"
			}
			settlement.Identity = step.Identity
			return settlement, nil
		}
		if terminalErr != nil {
			return execution.Settlement{}, terminalErr
		}
		select {
		case <-ctx.Done():
			return execution.Settlement{}, ctx.Err()
		case <-wake:
		}
	}
}

func (s *ConfirmationSource) readLoop(ctx context.Context, subscription Subscription, output <-chan types.Log) {
	defer subscription.Unsubscribe()
	for {
		select {
		case <-ctx.Done():
			s.finish(ctx.Err())
			return
		case err, ok := <-subscription.Err():
			if !ok || err == nil {
				err = fmt.Errorf("EVM settlement subscription closed")
			}
			s.finish(err)
			return
		case observed, ok := <-output:
			if !ok {
				s.finish(fmt.Errorf("EVM settlement log stream closed"))
				return
			}
			if observed.Removed {
				continue
			}
			key := strings.ToLower(observed.TxHash.Hex())
			s.mu.Lock()
			s.logs[key] = append(s.logs[key], observed)
			waiters := append([]chan struct{}(nil), s.waiters[key]...)
			s.mu.Unlock()
			for _, waiter := range waiters {
				select {
				case waiter <- struct{}{}:
				default:
				}
			}
		}
	}
}

func (s *ConfirmationSource) takeLogs(key string) ([]types.Log, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	logs := s.logs[key]
	delete(s.logs, key)
	return logs, s.err
}

func (s *ConfirmationSource) removeWaiter(key string, target chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.waiters[key]
	for index, waiter := range current {
		if waiter != target {
			continue
		}
		current = append(current[:index], current[index+1:]...)
		break
	}
	if len(current) == 0 {
		delete(s.waiters, key)
	} else {
		s.waiters[key] = current
	}
}

func (s *ConfirmationSource) finish(err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.err = err
	var waiters []chan struct{}
	for _, candidates := range s.waiters {
		waiters = append(waiters, candidates...)
	}
	s.mu.Unlock()
	for _, waiter := range waiters {
		select {
		case waiter <- struct{}{}:
		default:
		}
	}
}

var _ chainport.ConfirmationSource = (*ConfirmationSource)(nil)
