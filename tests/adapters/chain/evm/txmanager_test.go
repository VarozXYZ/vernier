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
	chainID   *big.Int
	sendErr   error
	delay     time.Duration
	calls     atomic.Int32
	sentMu    sync.Mutex
	sent      []*types.Transaction
	simulated atomic.Int32
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
func (*txClientStub) TransactionReceipt(context.Context, common.Hash) (*types.Receipt, error) {
	return nil, errors.New("not used")
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
	telemetry := make(chan evmadapter.FanoutAttempt, 2)
	manager, err := evmadapter.NewTxManager(evmadapter.TxManagerConfig{
		Chain: "evm", Account: "executor", ChainID: chainID, PrivateKey: key, Primary: primary,
		Simulator:       primary,
		Fanout:          map[string]evmadapter.TxClient{"failed": failed, "accepted": accepted},
		DefaultGasLimit: 100_000, Clock: time.Now,
		OnFanoutResult: func(attempt evmadapter.FanoutAttempt) { telemetry <- attempt },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Warm(context.Background()); err != nil {
		t.Fatal(err)
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
	for len(observed) < 2 {
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
	failed.sentMu.Lock()
	accepted.sentMu.Lock()
	defer failed.sentMu.Unlock()
	defer accepted.sentMu.Unlock()
	if len(failed.sent) != 1 || len(accepted.sent) != 1 ||
		failed.sent[0].Hash() != accepted.sent[0].Hash() ||
		failed.sent[0].Hash().Hex() != prepared.Identity.Hash {
		t.Fatal("fanout endpoints did not receive the same signed transaction")
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
