package evm

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

type TxClient interface {
	ChainID(context.Context) (*big.Int, error)
	PendingNonceAt(context.Context, common.Address) (uint64, error)
	SuggestGasTipCap(context.Context) (*big.Int, error)
	HeaderByNumber(context.Context, *big.Int) (*types.Header, error)
	SendTransaction(context.Context, *types.Transaction) error
	TransactionReceipt(context.Context, common.Hash) (*types.Receipt, error)
}

type TransactionSimulator interface {
	CallContract(context.Context, geth.CallMsg, *big.Int) ([]byte, error)
}

type TxManagerConfig struct {
	Chain              market.ChainID
	Account            execution.AccountID
	ChainID            *big.Int
	PrivateKey         *ecdsa.PrivateKey
	Primary            TxClient
	Simulator          TransactionSimulator
	Fanout             map[string]TxClient
	DefaultGasLimit    uint64
	Clock              func() time.Time
	OnFanoutResult     func(FanoutAttempt)
	ReceiptDecoder     ReceiptSettlementDecoder
	FeeRefreshInterval time.Duration
}

type FanoutAttempt struct {
	Endpoint     string
	Accepted     bool
	AlreadyKnown bool
	Err          error
	CompletedAt  time.Time
}

type TxManager struct {
	chain              market.ChainID
	account            execution.AccountID
	chainID            *big.Int
	privateKey         *ecdsa.PrivateKey
	address            common.Address
	primary            TxClient
	simulator          TransactionSimulator
	fanout             map[string]TxClient
	gasLimit           uint64
	clock              func() time.Time
	onFanout           func(FanoutAttempt)
	receiptDecoder     ReceiptSettlementDecoder
	feeRefreshInterval time.Duration
	warmOnce           sync.Once

	mu      sync.Mutex
	warmed  bool
	nonce   uint64
	baseFee *big.Int
	tipCap  *big.Int
	feeCap  *big.Int
	feesAt  time.Time
}

type FeeSnapshot struct {
	BaseFee    *big.Int
	TipCap     *big.Int
	FeeCap     *big.Int
	CapturedAt time.Time
}

func NewTxManager(config TxManagerConfig) (*TxManager, error) {
	if config.Chain == "" || config.Account == "" || config.ChainID == nil || config.ChainID.Sign() <= 0 ||
		config.PrivateKey == nil || config.Primary == nil || len(config.Fanout) == 0 || config.Clock == nil {
		return nil, fmt.Errorf("EVM TxManager requires chain, account, signer, primary client, fanout, and clock")
	}
	fanout := make(map[string]TxClient, len(config.Fanout))
	for name, client := range config.Fanout {
		if strings.TrimSpace(name) == "" || client == nil {
			return nil, fmt.Errorf("EVM TxManager fanout contains an invalid endpoint")
		}
		fanout[name] = client
	}
	if config.DefaultGasLimit == 0 {
		config.DefaultGasLimit = 1_500_000
	}
	if config.FeeRefreshInterval == 0 {
		config.FeeRefreshInterval = 5 * time.Second
	}
	if config.FeeRefreshInterval < time.Second {
		return nil, fmt.Errorf("EVM fee refresh interval must be at least one second")
	}
	return &TxManager{
		chain: config.Chain, account: config.Account, chainID: new(big.Int).Set(config.ChainID),
		privateKey: config.PrivateKey, address: crypto.PubkeyToAddress(config.PrivateKey.PublicKey),
		primary: config.Primary, simulator: config.Simulator,
		fanout: fanout, gasLimit: config.DefaultGasLimit, clock: config.Clock,
		onFanout: config.OnFanoutResult, receiptDecoder: config.ReceiptDecoder,
		feeRefreshInterval: config.FeeRefreshInterval,
	}, nil
}

func (m *TxManager) Account() execution.AccountID { return m.account }

// NextNonce returns the next nonce preloaded for this account without making a
// network call. Callers must invoke MarkNonceUsed after a broadcast is accepted
// or has an uncertain outcome.
func (m *TxManager) NextNonce() (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.warmed {
		return 0, fmt.Errorf("EVM nonce coordinator is not warmed")
	}
	return m.nonce, nil
}

// MarkNonceUsed advances monotonically. It deliberately treats an uncertain
// broadcast as consumed so the process never reuses a nonce that may be in the
// public or private mempool.
func (m *TxManager) MarkNonceUsed(nonce uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.warmed || nonce < m.nonce {
		return
	}
	m.nonce = nonce + 1
}

// Warm preloads nonce and fee inputs so Prepare performs no network calls.
func (m *TxManager) Warm(ctx context.Context) error {
	chainID, err := m.primary.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("validate EVM TxManager chain ID: %w", err)
	}
	if chainID == nil || chainID.Cmp(m.chainID) != 0 {
		return fmt.Errorf("validate EVM TxManager chain ID: configured chain does not match endpoint")
	}
	for name, client := range m.fanout {
		candidate, candidateErr := client.ChainID(ctx)
		if candidateErr != nil || candidate == nil || candidate.Cmp(m.chainID) != 0 {
			return fmt.Errorf("validate EVM fanout endpoint %q chain ID", name)
		}
	}
	nonce, err := m.primary.PendingNonceAt(ctx, m.address)
	if err != nil {
		return fmt.Errorf("preload EVM nonce: %w", err)
	}
	baseFee, tip, feeCap, err := m.readFees(ctx)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.nonce, m.baseFee, m.tipCap, m.feeCap, m.feesAt, m.warmed =
		nonce, baseFee, new(big.Int).Set(tip), feeCap, m.clock().UTC(), true
	m.mu.Unlock()
	m.warmOnce.Do(func() {
		go m.refreshFees(ctx)
	})
	return nil
}

func (m *TxManager) readFees(ctx context.Context) (*big.Int, *big.Int, *big.Int, error) {
	tip, err := m.primary.SuggestGasTipCap(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("preload EVM priority fee: %w", err)
	}
	if tip == nil || tip.Sign() <= 0 {
		return nil, nil, nil, fmt.Errorf("preload EVM priority fee: endpoint returned a non-positive fee")
	}
	header, err := m.primary.HeaderByNumber(ctx, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("preload EVM base fee: %w", err)
	}
	if header == nil || header.BaseFee == nil || header.BaseFee.Sign() < 0 {
		return nil, nil, nil, fmt.Errorf("preload EVM base fee: endpoint returned no EIP-1559 base fee")
	}
	feeCap := new(big.Int).Add(new(big.Int).Mul(header.BaseFee, big.NewInt(2)), tip)
	return new(big.Int).Set(header.BaseFee), tip, feeCap, nil
}

func (m *TxManager) refreshFees(ctx context.Context) {
	ticker := time.NewTicker(m.feeRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshCtx, cancel := context.WithTimeout(ctx, m.feeRefreshInterval)
			baseFee, tip, feeCap, err := m.readFees(refreshCtx)
			cancel()
			if err != nil {
				continue
			}
			m.mu.Lock()
			m.baseFee, m.tipCap, m.feeCap, m.feesAt =
				new(big.Int).Set(baseFee), new(big.Int).Set(tip),
				new(big.Int).Set(feeCap), m.clock().UTC()
			m.mu.Unlock()
		}
	}
}

// FeeSnapshot returns the fee inputs maintained by the background refresher.
// It never performs RPC I/O and is safe for complete-flow cost estimation.
func (m *TxManager) FeeSnapshot() (FeeSnapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.warmed || m.baseFee == nil || m.tipCap == nil ||
		m.feeCap == nil || m.feesAt.IsZero() {
		return FeeSnapshot{}, false
	}
	return FeeSnapshot{
		BaseFee:    new(big.Int).Set(m.baseFee),
		TipCap:     new(big.Int).Set(m.tipCap),
		FeeCap:     new(big.Int).Set(m.feeCap),
		CapturedAt: m.feesAt,
	}, true
}

// EstimateArtifactNetworkCost values the artifact's expected gas usage with the
// fee inputs maintained by the background cache. The transaction gas limit is
// deliberately independent and is not used as the expected economic cost.
func (m *TxManager) EstimateArtifactNetworkCost(
	_ context.Context,
	artifact executionport.Artifact,
) (*big.Int, time.Time, error) {
	text := strings.TrimSpace(artifact.Metadata["expected_gas_used"])
	gas, err := strconv.ParseUint(text, 10, 64)
	if err != nil || gas == 0 {
		return nil, time.Time{}, fmt.Errorf("EVM artifact has no expected gas usage evidence")
	}
	fees, ok := m.FeeSnapshot()
	if !ok {
		return nil, time.Time{}, fmt.Errorf("EVM fee cache is unavailable")
	}
	expectedGasPrice := new(big.Int).Add(fees.BaseFee, fees.TipCap)
	return new(big.Int).Mul(
		new(big.Int).SetUint64(gas), expectedGasPrice,
	), fees.CapturedAt, nil
}

func (m *TxManager) ConfirmedGasUsed(
	ctx context.Context,
	identity string,
) (uint64, error) {
	if len(identity) != 66 || !strings.HasPrefix(identity, "0x") {
		return 0, fmt.Errorf("EVM transaction identity is invalid")
	}
	receipt, err := m.primary.TransactionReceipt(ctx, common.HexToHash(identity))
	if err != nil {
		return 0, err
	}
	if receipt == nil || receipt.Status != types.ReceiptStatusSuccessful ||
		receipt.GasUsed == 0 {
		return 0, fmt.Errorf("EVM calibration receipt is not a successful transaction")
	}
	return receipt.GasUsed, nil
}

func (m *TxManager) Prepare(_ context.Context, artifact executionport.Artifact) (chainport.PreparedTransaction, error) {
	if artifact.Leg.Account != m.account || artifact.Leg.Chain != m.chain {
		return chainport.PreparedTransaction{}, fmt.Errorf("EVM artifact targets another TxManager")
	}
	toText := strings.TrimSpace(artifact.Metadata["to"])
	if !common.IsHexAddress(toText) || common.HexToAddress(toText) == (common.Address{}) {
		return chainport.PreparedTransaction{}, fmt.Errorf("EVM artifact requires non-zero destination metadata")
	}
	value := new(big.Int)
	if text := strings.TrimSpace(artifact.Metadata["value"]); text != "" {
		var ok bool
		value, ok = new(big.Int).SetString(text, 10)
		if !ok || value.Sign() < 0 {
			return chainport.PreparedTransaction{}, fmt.Errorf("EVM artifact value is invalid")
		}
	}
	gasLimit := m.gasLimit
	if text := strings.TrimSpace(artifact.Metadata["gas_limit"]); text != "" {
		parsed, err := strconv.ParseUint(text, 10, 64)
		if err != nil || parsed == 0 {
			return chainport.PreparedTransaction{}, fmt.Errorf("EVM artifact gas limit is invalid")
		}
		gasLimit = parsed
	}
	nonce, err := m.NextNonce()
	if err != nil {
		return chainport.PreparedTransaction{}, err
	}
	m.mu.Lock()
	tipCap := new(big.Int).Set(m.tipCap)
	feeCap := new(big.Int).Set(m.feeCap)
	m.mu.Unlock()
	to := common.HexToAddress(toText)
	transaction := types.NewTx(&types.DynamicFeeTx{
		ChainID: m.chainID, Nonce: nonce, GasTipCap: tipCap, GasFeeCap: feeCap,
		Gas: gasLimit, To: &to, Value: value, Data: append([]byte(nil), artifact.Payload...),
	})
	signed, err := types.SignTx(transaction, types.LatestSignerForChainID(m.chainID), m.privateKey)
	if err != nil {
		return chainport.PreparedTransaction{}, err
	}
	raw, err := signed.MarshalBinary()
	if err != nil {
		return chainport.PreparedTransaction{}, err
	}
	identityNonce := nonce
	return chainport.PreparedTransaction{
		Leg: artifact.Leg,
		Identity: execution.TransactionIdentity{
			Chain: m.chain, Account: m.account, Hash: signed.Hash().Hex(), Nonce: &identityNonce,
		},
		SignedPayload: raw, PreparedAt: m.clock().UTC(),
	}, nil
}

func (m *TxManager) SimulatePrepared(
	ctx context.Context,
	prepared chainport.PreparedTransaction,
) error {
	if m.simulator == nil {
		return fmt.Errorf("EVM transaction simulator is unavailable")
	}
	if prepared.Leg.Account != m.account || len(prepared.SignedPayload) == 0 {
		return fmt.Errorf("EVM prepared transaction is invalid")
	}
	var transaction types.Transaction
	if err := transaction.UnmarshalBinary(prepared.SignedPayload); err != nil {
		return fmt.Errorf("decode EVM transaction for simulation: %w", err)
	}
	if transaction.To() == nil {
		return fmt.Errorf("simulate EVM contract creation is unsupported")
	}
	_, err := m.simulator.CallContract(ctx, geth.CallMsg{
		From:       m.address,
		To:         transaction.To(),
		Gas:        transaction.Gas(),
		GasFeeCap:  transaction.GasFeeCap(),
		GasTipCap:  transaction.GasTipCap(),
		Value:      transaction.Value(),
		Data:       transaction.Data(),
		AccessList: transaction.AccessList(),
	}, nil)
	if err != nil {
		return fmt.Errorf("simulate EVM prepared transaction: %w", err)
	}
	return nil
}

func (m *TxManager) Broadcast(ctx context.Context, prepared chainport.PreparedTransaction) (chainport.BroadcastResult, error) {
	if prepared.Leg.Account != m.account || len(prepared.SignedPayload) == 0 {
		return chainport.BroadcastResult{}, fmt.Errorf("EVM prepared transaction is invalid")
	}
	var transaction types.Transaction
	if err := transaction.UnmarshalBinary(prepared.SignedPayload); err != nil {
		return chainport.BroadcastResult{}, err
	}
	type response struct {
		name string
		err  error
	}
	results := make(chan response, len(m.fanout))
	for name, client := range m.fanout {
		name, client := name, client
		go func() {
			sendErr := client.SendTransaction(ctx, &transaction)
			known := isKnownTransaction(sendErr)
			results <- response{name: name, err: sendErr}
			if m.onFanout != nil {
				m.onFanout(FanoutAttempt{
					Endpoint: name, Accepted: sendErr == nil || known,
					AlreadyKnown: known, Err: sendErr, CompletedAt: m.clock().UTC(),
				})
			}
		}()
	}
	var failures []error
	for attempt := 1; attempt <= len(m.fanout); attempt++ {
		result := <-results
		if result.err == nil || isKnownTransaction(result.err) {
			m.MarkNonceUsed(transaction.Nonce())
			return chainport.BroadcastResult{
				Identity: prepared.Identity, Disposition: chainport.BroadcastAccepted,
				Accepted: true, Endpoint: result.name,
				Attempts: attempt, AcceptedAt: m.clock().UTC(),
			}, nil
		}
		failures = append(failures, fmt.Errorf("%s: %w", result.name, result.err))
	}
	disposition := chainport.BroadcastRejected
	for _, failure := range failures {
		if broadcastMayHaveSucceeded(failure) {
			disposition = chainport.BroadcastPossible
			break
		}
	}
	if disposition == chainport.BroadcastPossible {
		m.MarkNonceUsed(transaction.Nonce())
	}
	return chainport.BroadcastResult{
		Identity: prepared.Identity, Disposition: disposition, Attempts: len(m.fanout),
	}, errors.Join(failures...)
}

func (m *TxManager) Reconcile(ctx context.Context, step execution.OperationStep) (execution.Settlement, error) {
	identity := step.Identity
	if identity.Chain != m.chain || identity.Account != m.account || !common.IsHexHash(identity.Hash) {
		return execution.Settlement{}, fmt.Errorf("EVM reconciliation identity is invalid")
	}
	hash := common.HexToHash(identity.Hash)
	var unavailable []error
	for name, client := range m.fanout {
		receipt, err := client.TransactionReceipt(ctx, hash)
		if errors.Is(err, geth.NotFound) {
			continue
		}
		if err != nil {
			unavailable = append(unavailable, fmt.Errorf("%s: %w", name, err))
			continue
		}
		state := execution.StateConfirmedSuccess
		if receipt.Status != types.ReceiptStatusSuccessful {
			state = execution.StateConfirmedRevert
		}
		if state == execution.StateConfirmedSuccess && m.receiptDecoder != nil {
			settlement, decodeErr := m.receiptDecoder.DecodeReceipt(step, receipt)
			if decodeErr != nil {
				return execution.Settlement{}, fmt.Errorf("decode EVM settlement receipt: %w", decodeErr)
			}
			if settlement.ObservedAt.IsZero() {
				settlement.ObservedAt = m.clock().UTC()
			}
			if settlement.Evidence == "" {
				settlement.Evidence = "evm_receipt_event"
			}
			settlement.Identity = identity
			return settlement, nil
		}
		return execution.Settlement{
			Identity: identity, Technical: state, Economic: execution.EconomicReserved,
			ObservedAt: m.clock().UTC(), Evidence: "evm_receipt",
		}, nil
	}
	if len(unavailable) == len(m.fanout) {
		return execution.Settlement{}, errors.Join(unavailable...)
	}
	return execution.Settlement{
		Identity: identity, Technical: execution.StateOutcomeUnknown,
		Economic: execution.EconomicReserved, ObservedAt: m.clock().UTC(), Evidence: "receipt_not_found",
	}, nil
}

func isKnownTransaction(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "already known") || strings.Contains(text, "known transaction")
}

func broadcastMayHaveSucceeded(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	var timeout interface{ Timeout() bool }
	return errors.As(err, &timeout) && timeout.Timeout()
}

var _ chainport.TxManager = (*TxManager)(nil)
var _ chainport.EVMNonceCoordinator = (*TxManager)(nil)
var _ chainport.PreparedTransactionSimulator = (*TxManager)(nil)
