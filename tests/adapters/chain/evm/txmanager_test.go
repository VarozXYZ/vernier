package evm_test

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	evmadapter "github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

type txClientStub struct {
	chainID      *big.Int
	sendErr      error
	delay        time.Duration
	receipt      *types.Receipt
	receiptErr   error
	receiptDelay time.Duration
	receiptCalls atomic.Int32
	calls        atomic.Int32
	sentMu       sync.Mutex
	sent         []*types.Transaction
	simulated    atomic.Int32
}

func (c *txClientStub) ChainID(context.Context) (*big.Int, error) {
	c.calls.Add(1)
	return new(big.Int).Set(c.chainID), nil
}
func (c *txClientStub) PendingNonceAt(context.Context, common.Address) (uint64, error) {
	c.calls.Add(1)
	return 9, nil
}
func (c *txClientStub) SuggestGasTipCap(context.Context) (*big.Int, error) {
	c.calls.Add(1)
	return big.NewInt(2), nil
}
func (c *txClientStub) HeaderByNumber(context.Context, *big.Int) (*types.Header, error) {
	c.calls.Add(1)
	return &types.Header{BaseFee: big.NewInt(10)}, nil
}
func (c *txClientStub) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	c.calls.Add(1)
	if c.delay > 0 {
		timer := time.NewTimer(c.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	c.sentMu.Lock()
	c.sent = append(c.sent, tx)
	c.sentMu.Unlock()
	return c.sendErr
}
func (c *txClientStub) TransactionReceipt(ctx context.Context, _ common.Hash) (*types.Receipt, error) {
	c.receiptCalls.Add(1)
	if c.receiptDelay > 0 {
		timer := time.NewTimer(c.receiptDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return c.receipt, c.receiptErr
}
func (c *txClientStub) CallContract(
	context.Context,
	geth.CallMsg,
	*big.Int,
) ([]byte, error) {
	c.simulated.Add(1)
	return nil, nil
}

func TestEVMTxManagerPreloadsAndFansOutSameSignedTransaction(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	chainID := big.NewInt(12345)
	primary := &txClientStub{chainID: chainID}
	failed := &txClientStub{chainID: chainID, sendErr: errors.New("unavailable")}
	accepted := &txClientStub{chainID: chainID, delay: time.Millisecond}
	slow := &txClientStub{chainID: chainID, delay: time.Second}
	telemetry := make(chan evmadapter.FanoutAttempt, 3)
	manager, err := evmadapter.NewTxManager(evmadapter.TxManagerConfig{
		Chain: "evm", Account: "executor", ChainID: chainID, PrivateKey: key, Primary: primary,
		Simulator: primary,
		Fanout: map[string]evmadapter.TxClient{
			"failed": failed, "accepted": accepted, "slow": slow,
		},
		DefaultGasLimit: 100_000, Clock: time.Now,
		FanoutRequestTimeout: 100 * time.Millisecond,
		OnFanoutResult:       func(attempt evmadapter.FanoutAttempt) { telemetry <- attempt },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	if failed.calls.Load() != 1 ||
		accepted.calls.Load() != 1 ||
		slow.calls.Load() != 1 {
		t.Fatal("Warm did not validate every fanout endpoint")
	}
	warmCalls := primary.calls.Load()
	input, _ := market.NewTokenAmount("base", big.NewInt(100))
	output, _ := market.NewTokenAmount("quote", big.NewInt(101))
	quote, _ := market.NewQuote(market.Quote{
		Source: "local", Market: "local-market", SnapshotVersion: 1,
		Purpose: market.QuotePurposeLiveValidation, Mode: market.QuoteModeExactInput,
		Quality: market.QuoteQualityExact, AmountIn: input, AmountOut: output, QuotedAt: time.Now(),
	})
	leg := execution.Leg{
		ID: "sell", Side: execution.LegSell, Chain: "evm", Account: "executor",
		Market: "local-market", Input: input, ExpectedOutput: output,
	}
	prepared, err := manager.Prepare(context.Background(), executionport.Artifact{
		Leg: leg, ValidatedQuote: quote, Payload: []byte{1, 2, 3},
		Metadata: map[string]string{
			"to": "0x1111111111111111111111111111111111111111", "gas_limit": "90000",
		},
		BuiltAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if primary.calls.Load() != warmCalls {
		t.Fatal("Prepare performed a network call instead of using preloaded nonce and fees")
	}
	var signed types.Transaction
	if err := signed.UnmarshalBinary(prepared.SignedPayload); err != nil {
		t.Fatal(err)
	}
	if signed.Gas() != 90_000 {
		t.Fatalf("signed transaction gas limit = %d; want 90000", signed.Gas())
	}
	if prepared.Identity.Nonce == nil || *prepared.Identity.Nonce != 9 || prepared.Identity.Hash == "" {
		t.Fatalf("prepared identity = %+v", prepared.Identity)
	}
	if err := manager.SimulatePrepared(
		context.Background(),
		prepared,
	); err != nil {
		t.Fatal(err)
	}
	if primary.simulated.Load() != 1 {
		t.Fatalf("prepared simulations = %d", primary.simulated.Load())
	}
	result, err := manager.Broadcast(context.Background(), prepared)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Accepted || result.Endpoint != "accepted" {
		t.Fatalf("broadcast result = %+v", result)
	}
	observed := map[string]evmadapter.FanoutAttempt{}
	for len(observed) < 3 {
		select {
		case attempt := <-telemetry:
			observed[attempt.Endpoint] = attempt
		case <-time.After(time.Second):
			t.Fatal("fanout telemetry did not collect all endpoint results asynchronously")
		}
	}
	if observed["accepted"].Accepted != true || observed["failed"].Err == nil {
		t.Fatalf("fanout telemetry = %+v", observed)
	}
	if !errors.Is(observed["slow"].Err, context.DeadlineExceeded) {
		t.Fatalf("slow endpoint was not bounded: %+v", observed["slow"])
	}
	if observed["accepted"].ErrorClass != "accepted" ||
		observed["failed"].ErrorClass != "rejected_or_transport_error" ||
		observed["slow"].ErrorClass != "timeout" ||
		observed["accepted"].Latency <= 0 {
		t.Fatalf("fanout classifications = %+v", observed)
	}
	failed.sentMu.Lock()
	accepted.sentMu.Lock()
	fanoutInvalid := len(failed.sent) != 1 || len(accepted.sent) != 1
	if !fanoutInvalid {
		fanoutInvalid = failed.sent[0].Hash() != accepted.sent[0].Hash() ||
			failed.sent[0].Hash().Hex() != prepared.Identity.Hash
	}
	failed.sentMu.Unlock()
	accepted.sentMu.Unlock()
	if fanoutInvalid {
		t.Fatal("fanout endpoints did not receive the same signed transaction")
	}
	if _, err := manager.BroadcastPrimary(context.Background(), prepared); err != nil {
		t.Fatal(err)
	}
	primary.sentMu.Lock()
	defer primary.sentMu.Unlock()
	if len(primary.sent) != 1 || primary.sent[0].Hash().Hex() != prepared.Identity.Hash {
		t.Fatal("primary-only broadcast did not use exactly the primary RPC")
	}
}

func TestEVMTxManagerValuesExpectedGasInsteadOfTransactionLimit(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	chainID := big.NewInt(12345)
	primary := &txClientStub{chainID: chainID}
	manager, err := evmadapter.NewTxManager(evmadapter.TxManagerConfig{
		Chain: "evm", Account: "executor", ChainID: chainID, PrivateKey: key,
		Primary: primary, Simulator: primary,
		Fanout: map[string]evmadapter.TxClient{"primary": primary},
		Clock:  time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}

	cost, _, err := manager.EstimateArtifactNetworkCost(
		context.Background(),
		executionport.Artifact{Metadata: map[string]string{
			"gas_limit":         "1500000",
			"expected_gas_used": "1000000",
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	// The stub warms a base fee of 10 and a tip of 2.
	if cost.Cmp(big.NewInt(12_000_000)) != 0 {
		t.Fatalf("estimated network cost = %s; want 12000000", cost)
	}
}

func TestEVMTxManagerReconcilesFromFirstFanoutReceipt(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	chainID := big.NewInt(12345)
	slow := &txClientStub{
		chainID: chainID, receiptDelay: time.Second,
		receiptErr: geth.NotFound,
	}
	fast := &txClientStub{
		chainID: chainID,
		receipt: &types.Receipt{Status: types.ReceiptStatusSuccessful},
	}
	manager, err := evmadapter.NewTxManager(evmadapter.TxManagerConfig{
		Chain: "evm", Account: "executor", ChainID: chainID,
		PrivateKey: key, Primary: slow, Simulator: slow,
		Fanout: map[string]evmadapter.TxClient{
			"slow": slow,
			"fast": fast,
		},
		Clock: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	settlement, err := manager.Reconcile(
		context.Background(),
		execution.OperationStep{
			Identity: execution.TransactionIdentity{
				Chain: "evm", Account: "executor",
				Hash: common.HexToHash("0x01").Hex(),
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if settlement.Technical != execution.StateConfirmedSuccess {
		t.Fatalf("settlement = %+v", settlement)
	}
	if fast.receiptCalls.Load() != 1 {
		t.Fatalf("fast receipt calls=%d", fast.receiptCalls.Load())
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("parallel receipt reconciliation took %s", elapsed)
	}
}

func TestEVMTxManagerAcceptsOnlySameTransactionEvidence(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	chainID := big.NewInt(12345)
	primary := &txClientStub{chainID: chainID}
	imported := &txClientStub{
		chainID: chainID, sendErr: errors.New("already imported"),
	}
	manager, err := evmadapter.NewTxManager(evmadapter.TxManagerConfig{
		Chain: "evm", Account: "executor", ChainID: chainID,
		PrivateKey: key, Primary: primary, Simulator: primary,
		Fanout: map[string]evmadapter.TxClient{"imported": imported},
		Clock:  time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	input, _ := market.NewTokenAmount("base", big.NewInt(100))
	output, _ := market.NewTokenAmount("quote", big.NewInt(101))
	quote, _ := market.NewQuote(market.Quote{
		Source: "local", Market: "local-market", SnapshotVersion: 1,
		Purpose: market.QuotePurposeLiveValidation,
		Mode:    market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
		AmountIn: input, AmountOut: output, QuotedAt: time.Now(),
	})
	prepared, err := manager.Prepare(context.Background(), executionport.Artifact{
		Leg: execution.Leg{
			ID: "sell", Side: execution.LegSell, Chain: "evm",
			Account: "executor", Market: "local-market",
			Input: input, ExpectedOutput: output,
		},
		ValidatedQuote: quote,
		Metadata: map[string]string{
			"to": "0x1111111111111111111111111111111111111111",
		},
		BuiltAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := manager.Broadcast(context.Background(), prepared)
	if err != nil || !result.Accepted {
		t.Fatalf("already-imported broadcast result=%+v err=%v", result, err)
	}

	nonceTooLow := &txClientStub{
		chainID: chainID, sendErr: errors.New("nonce too low"),
	}
	second, err := evmadapter.NewTxManager(evmadapter.TxManagerConfig{
		Chain: "evm", Account: "executor", ChainID: chainID,
		PrivateKey: key, Primary: primary, Simulator: primary,
		Fanout: map[string]evmadapter.TxClient{"nonce": nonceTooLow},
		Clock:  time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Broadcast(context.Background(), prepared); err == nil {
		t.Fatal("nonce too low was incorrectly accepted as same-transaction evidence")
	}

	unknown := &txClientStub{
		chainID: chainID, sendErr: errors.New("unknown transaction"),
	}
	third, err := evmadapter.NewTxManager(evmadapter.TxManagerConfig{
		Chain: "evm", Account: "executor", ChainID: chainID,
		PrivateKey: key, Primary: primary, Simulator: primary,
		Fanout: map[string]evmadapter.TxClient{"unknown": unknown},
		Clock:  time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := third.Broadcast(context.Background(), prepared); err == nil {
		t.Fatal("unknown transaction was incorrectly accepted as known")
	}
}
