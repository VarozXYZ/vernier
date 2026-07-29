package solana

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
)

type TransactionSubscriber interface {
	SubscribeTransactions(context.Context, string) (TransactionSubscription, error)
}

type TransactionSettlementDecoder interface {
	DecodeTransaction(execution.OperationStep, Transaction) (execution.Settlement, error)
}

type ConfirmationSourceConfig struct {
	AccountAddress string
	Subscriber     TransactionSubscriber
	Decoder        TransactionSettlementDecoder
	Clock          func() time.Time
}

// ConfirmationSource keeps a Helius transactionSubscribe stream open before
// broadcast and buffers confirmed transactions by signature.
type ConfirmationSource struct {
	config ConfirmationSourceConfig

	mu           sync.Mutex
	started      bool
	terminalErr  error
	transactions map[string]Transaction
	waiters      map[string][]chan struct{}
}

func NewConfirmationSource(config ConfirmationSourceConfig) (*ConfirmationSource, error) {
	if strings.TrimSpace(config.AccountAddress) == "" || config.Subscriber == nil ||
		config.Decoder == nil || config.Clock == nil {
		return nil, fmt.Errorf("solana confirmation source requires account, subscriber, decoder, and clock")
	}
	return &ConfirmationSource{
		config:       config,
		transactions: make(map[string]Transaction),
		waiters:      make(map[string][]chan struct{}),
	}, nil
}

func (s *ConfirmationSource) Warm(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		err := s.terminalErr
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	subscription, err := s.config.Subscriber.SubscribeTransactions(ctx, s.config.AccountAddress)
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
	go s.readLoop(ctx, subscription)
	return nil
}

func (s *ConfirmationSource) Await(ctx context.Context, step execution.OperationStep) (execution.Settlement, error) {
	if err := step.Identity.Validate(); err != nil {
		return execution.Settlement{}, err
	}
	key := step.Identity.Hash
	wake := make(chan struct{}, 1)
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return execution.Settlement{}, fmt.Errorf("solana confirmation source is not warmed")
	}
	s.waiters[key] = append(s.waiters[key], wake)
	s.mu.Unlock()
	defer s.removeWaiter(key, wake)
	for {
		transaction, found, terminalErr := s.take(key)
		if found {
			settlement, err := s.config.Decoder.DecodeTransaction(step, transaction)
			if err != nil {
				return execution.Settlement{}, err
			}
			settlement.Identity = step.Identity
			if settlement.ObservedAt.IsZero() {
				settlement.ObservedAt = s.config.Clock().UTC()
			}
			if settlement.Evidence == "" {
				settlement.Evidence = "helius_transaction_subscribe"
			}
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

func (s *ConfirmationSource) readLoop(ctx context.Context, subscription TransactionSubscription) {
	defer subscription.Unsubscribe()
	for {
		select {
		case <-ctx.Done():
			s.finish(ctx.Err())
			return
		case err, ok := <-subscription.Err():
			if !ok || err == nil {
				err = fmt.Errorf("solana transaction subscription closed")
			}
			s.finish(err)
			return
		case observed, ok := <-subscription.Notifications():
			if !ok {
				s.finish(fmt.Errorf("solana transaction notification stream closed"))
				return
			}
			transaction := Transaction{
				Signature: observed.Signature, Slot: observed.Slot,
				Transaction: append(json.RawMessage(nil), observed.Transaction...),
				Meta:        append(json.RawMessage(nil), observed.Meta...),
			}
			s.mu.Lock()
			s.transactions[observed.Signature] = transaction
			waiters := append([]chan struct{}(nil), s.waiters[observed.Signature]...)
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

func (s *ConfirmationSource) take(signature string) (Transaction, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	transaction, found := s.transactions[signature]
	if found {
		delete(s.transactions, signature)
	}
	return transaction, found, s.terminalErr
}

func (s *ConfirmationSource) removeWaiter(signature string, target chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.waiters[signature]
	for index, waiter := range current {
		if waiter == target {
			current = append(current[:index], current[index+1:]...)
			break
		}
	}
	if len(current) == 0 {
		delete(s.waiters, signature)
	} else {
		s.waiters[signature] = current
	}
}

func (s *ConfirmationSource) finish(err error) {
	s.mu.Lock()
	if s.terminalErr != nil {
		s.mu.Unlock()
		return
	}
	s.terminalErr = err
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

type SPLBalanceDecoderConfig struct {
	Owner       string
	TokenMints  map[market.TokenID]string
	NativeAsset market.AssetID
	Clock       func() time.Time
}

// SPLBalanceDecoder proves the wallet's actual input and output from summed
// pre/post token balances. It supports multiple token accounts for one mint.
type SPLBalanceDecoder struct {
	owner       string
	tokenMints  map[market.TokenID]string
	nativeAsset market.AssetID
	clock       func() time.Time
}

func NewSPLBalanceDecoder(config SPLBalanceDecoderConfig) (*SPLBalanceDecoder, error) {
	if strings.TrimSpace(config.Owner) == "" || len(config.TokenMints) < 2 || config.Clock == nil {
		return nil, fmt.Errorf("SPL balance decoder requires owner, token mints, and clock")
	}
	mints := make(map[market.TokenID]string, len(config.TokenMints))
	for token, mint := range config.TokenMints {
		if token == "" || strings.TrimSpace(mint) == "" {
			return nil, fmt.Errorf("SPL balance decoder token mapping is invalid")
		}
		mints[token] = mint
	}
	if config.NativeAsset == "" {
		config.NativeAsset = "sol"
	}
	return &SPLBalanceDecoder{
		owner: config.Owner, tokenMints: mints,
		nativeAsset: config.NativeAsset, clock: config.Clock,
	}, nil
}

func (d *SPLBalanceDecoder) DecodeTransaction(step execution.OperationStep, transaction Transaction) (execution.Settlement, error) {
	var meta transactionMeta
	if err := json.Unmarshal(transaction.Meta, &meta); err != nil {
		return execution.Settlement{}, fmt.Errorf("decode Solana transaction meta: %w", err)
	}
	if hasSolanaError(meta.Err) {
		return execution.Settlement{
			Identity: step.Identity, Technical: execution.StateConfirmedRevert,
			Economic: execution.EconomicReserved, ObservedAt: d.clock().UTC(),
			Evidence: "solana_transaction_meta",
		}, nil
	}
	inputMint := d.tokenMints[step.Leg.Input.Token()]
	outputMint := d.tokenMints[step.Leg.ExpectedOutput.Token()]
	if inputMint == "" || outputMint == "" {
		return execution.Settlement{}, fmt.Errorf("solana settlement token mapping is missing")
	}
	pre, err := sumOwnedBalances(meta.PreTokenBalances, d.owner)
	if err != nil {
		return execution.Settlement{}, err
	}
	post, err := sumOwnedBalances(meta.PostTokenBalances, d.owner)
	if err != nil {
		return execution.Settlement{}, err
	}
	actualInUnits := new(big.Int).Sub(valueOrZero(pre[inputMint]), valueOrZero(post[inputMint]))
	actualOutUnits := new(big.Int).Sub(valueOrZero(post[outputMint]), valueOrZero(pre[outputMint]))
	if actualInUnits.Sign() <= 0 || actualOutUnits.Sign() <= 0 {
		return execution.Settlement{}, fmt.Errorf(
			"solana transaction does not prove positive input and output deltas",
		)
	}
	actualIn, err := market.NewTokenAmount(step.Leg.Input.Token(), actualInUnits)
	if err != nil {
		return execution.Settlement{}, err
	}
	actualOut, err := market.NewTokenAmount(step.Leg.ExpectedOutput.Token(), actualOutUnits)
	if err != nil {
		return execution.Settlement{}, err
	}
	costs, err := d.costs(step.Leg.Chain, meta)
	if err != nil {
		return execution.Settlement{}, err
	}
	return execution.Settlement{
		Identity: step.Identity, Technical: execution.StateConfirmedSuccess,
		Economic: execution.EconomicEffectVerified,
		ActualIn: actualIn, ActualOut: actualOut, ObservedAt: d.clock().UTC(),
		Costs: costs, Evidence: "solana_token_balance_delta",
	}, nil
}

type transactionMeta struct {
	Err               json.RawMessage `json:"err"`
	Fee               uint64          `json:"fee"`
	PreBalances       []uint64        `json:"preBalances"`
	PostBalances      []uint64        `json:"postBalances"`
	PreTokenBalances  []tokenBalance  `json:"preTokenBalances"`
	PostTokenBalances []tokenBalance  `json:"postTokenBalances"`
}

func (d *SPLBalanceDecoder) costs(
	chain market.ChainID,
	meta transactionMeta,
) ([]execution.CostComponent, error) {
	result := make([]execution.CostComponent, 0, 2)
	appendLamports := func(kind string, lamports uint64, evidence string) error {
		if lamports == 0 {
			return nil
		}
		amount, err := market.NewAssetQuantity(
			d.nativeAsset,
			new(big.Rat).SetFrac(
				new(big.Int).SetUint64(lamports),
				new(big.Int).Exp(big.NewInt(10), big.NewInt(9), nil),
			),
		)
		if err != nil {
			return err
		}
		result = append(result, execution.CostComponent{
			Kind: kind, Chain: chain, Amount: amount, Evidence: evidence,
		})
		return nil
	}
	if err := appendLamports("network_fee", meta.Fee, "solana_transaction_meta_fee"); err != nil {
		return nil, err
	}
	if len(meta.PreBalances) > 0 && len(meta.PostBalances) > 0 &&
		meta.PreBalances[0] >= meta.PostBalances[0] {
		total := meta.PreBalances[0] - meta.PostBalances[0]
		if total > meta.Fee {
			if err := appendLamports(
				"additional_payer_debit", total-meta.Fee,
				"solana_payer_balance_delta",
			); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

type tokenBalance struct {
	Mint          string `json:"mint"`
	Owner         string `json:"owner"`
	UITokenAmount struct {
		Amount string `json:"amount"`
	} `json:"uiTokenAmount"`
}

func sumOwnedBalances(balances []tokenBalance, owner string) (map[string]*big.Int, error) {
	result := make(map[string]*big.Int)
	for _, balance := range balances {
		if balance.Owner != owner {
			continue
		}
		amount, ok := new(big.Int).SetString(balance.UITokenAmount.Amount, 10)
		if !ok || amount.Sign() < 0 {
			return nil, fmt.Errorf("solana token balance amount is invalid")
		}
		if result[balance.Mint] == nil {
			result[balance.Mint] = new(big.Int)
		}
		result[balance.Mint].Add(result[balance.Mint], amount)
	}
	return result, nil
}

func valueOrZero(value *big.Int) *big.Int {
	if value == nil {
		return new(big.Int)
	}
	return new(big.Int).Set(value)
}

var (
	_ chainport.ConfirmationSource = (*ConfirmationSource)(nil)
	_ TransactionSettlementDecoder = (*SPLBalanceDecoder)(nil)
)
