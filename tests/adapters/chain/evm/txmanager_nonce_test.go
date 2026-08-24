package evm_test

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	evmadapter "github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
)

type nonceTestClient struct {
	nonce   uint64
	sendErr error
}

func (c nonceTestClient) ChainID(context.Context) (*big.Int, error) {
	return big.NewInt(137), nil
}
func (c nonceTestClient) PendingNonceAt(context.Context, common.Address) (uint64, error) {
	return c.nonce, nil
}
func (c nonceTestClient) SuggestGasTipCap(context.Context) (*big.Int, error) {
	return big.NewInt(1), nil
}
func (c nonceTestClient) HeaderByNumber(context.Context, *big.Int) (*types.Header, error) {
	return &types.Header{BaseFee: big.NewInt(1)}, nil
}
func (c nonceTestClient) SendTransaction(context.Context, *types.Transaction) error {
	return c.sendErr
}
func (c nonceTestClient) TransactionReceipt(context.Context, common.Hash) (*types.Receipt, error) {
	return nil, errors.New("unused")
}

func newNonceTestManager(
	t *testing.T,
	nonce uint64,
	sendErr error,
) (*evmadapter.TxManager, context.CancelFunc) {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	client := nonceTestClient{nonce: nonce, sendErr: sendErr}
	manager, err := evmadapter.NewTxManager(evmadapter.TxManagerConfig{
		Chain: "polygon", Account: "polygon-executor",
		ChainID: big.NewInt(137), PrivateKey: key,
		Primary: client,
		Fanout:  map[string]evmadapter.TxClient{"test": client},
		Clock:   time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := manager.Warm(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	return manager, cancel
}

func TestNonceCoordinatorAdvancesMonotonicallyAcrossComponents(t *testing.T) {
	manager, cancel := newNonceTestManager(t, 12, nil)
	defer cancel()

	nonce, err := manager.NextNonce()
	if err != nil || nonce != 12 {
		t.Fatalf("NextNonce() = %d, %v; want 12, nil", nonce, err)
	}
	manager.MarkNonceUsed(12)
	nonce, err = manager.NextNonce()
	if err != nil || nonce != 13 {
		t.Fatalf("NextNonce() after bridge = %d, %v; want 13, nil", nonce, err)
	}
	manager.MarkNonceUsed(11)
	nonce, err = manager.NextNonce()
	if err != nil || nonce != 13 {
		t.Fatalf("NextNonce() after stale report = %d, %v; want 13, nil", nonce, err)
	}
}

func TestBroadcastPossibleConsumesNonce(t *testing.T) {
	manager, cancel := newNonceTestManager(t, 12, context.DeadlineExceeded)
	defer cancel()
	result, err := manager.Broadcast(
		context.Background(),
		nonceTestPreparedTransaction(t, 12),
	)
	if err == nil || result.Disposition != chainport.BroadcastPossible {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	nonce, nonceErr := manager.NextNonce()
	if nonceErr != nil || nonce != 13 {
		t.Fatalf("NextNonce() = %d, %v; want 13, nil", nonce, nonceErr)
	}
}

func TestBroadcastRejectedDoesNotConsumeNonce(t *testing.T) {
	manager, cancel := newNonceTestManager(t, 12, errors.New("insufficient funds"))
	defer cancel()
	result, err := manager.Broadcast(
		context.Background(),
		nonceTestPreparedTransaction(t, 12),
	)
	if err == nil || result.Disposition != chainport.BroadcastRejected {
		t.Fatalf("result=%+v error=%v", result, err)
	}
	nonce, nonceErr := manager.NextNonce()
	if nonceErr != nil || nonce != 12 {
		t.Fatalf("NextNonce() = %d, %v; want 12, nil", nonce, nonceErr)
	}
}

func TestAllFanoutNonceTooLowIsTypedAndCanResynchronize(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	primary := &nonceTestClient{nonce: 12}
	lowA := &nonceTestClient{nonce: 12, sendErr: errors.New("nonce too low")}
	lowB := &nonceTestClient{nonce: 12, sendErr: errors.New("nonce too low: next nonce 13")}
	manager, err := evmadapter.NewTxManager(evmadapter.TxManagerConfig{
		Chain: "polygon", Account: "polygon-executor", ChainID: big.NewInt(137),
		PrivateKey: key, Primary: primary,
		Fanout: map[string]evmadapter.TxClient{"a": lowA, "b": lowB}, Clock: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Warm(context.Background()); err != nil {
		t.Fatal(err)
	}
	result, err := manager.Broadcast(context.Background(), nonceTestPreparedTransaction(t, 12))
	var nonceLow *chainport.AllFanoutNonceTooLowError
	if !errors.As(err, &nonceLow) {
		t.Fatalf("error=%v; want AllFanoutNonceTooLowError", err)
	}
	if result.Disposition != chainport.BroadcastRejected || nonceLow.Nonce != 12 || nonceLow.Attempts != 2 {
		t.Fatalf("result=%+v nonceLow=%+v", result, nonceLow)
	}
	primary.nonce = 15
	refreshed, err := manager.ResyncNonce(context.Background(), 12)
	if err != nil || refreshed != 15 {
		t.Fatalf("ResyncNonce() = %d, %v; want 15, nil", refreshed, err)
	}
	manager.MarkNonceUsed(16)
	primary.nonce = 14
	if _, err := manager.ResyncNonce(context.Background(), 12); err != nil {
		t.Fatal(err)
	}
	next, _ := manager.NextNonce()
	if next != 17 {
		t.Fatalf("lagging resync regressed nonce to %d; want 17", next)
	}
}

func TestMixedFanoutNonceTooLowDoesNotClaimDeterministicNonceFailure(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	primary := &nonceTestClient{nonce: 12}
	manager, err := evmadapter.NewTxManager(evmadapter.TxManagerConfig{
		Chain: "polygon", Account: "polygon-executor", ChainID: big.NewInt(137),
		PrivateKey: key, Primary: primary,
		Fanout: map[string]evmadapter.TxClient{
			"low":      &nonceTestClient{sendErr: errors.New("nonce too low")},
			"accepted": &nonceTestClient{},
		}, Clock: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := manager.Broadcast(context.Background(), nonceTestPreparedTransaction(t, 12))
	if err != nil || !result.Accepted {
		t.Fatalf("mixed fanout result=%+v err=%v; want accepted", result, err)
	}
}

func nonceTestPreparedTransaction(
	t *testing.T,
	nonce uint64,
) chainport.PreparedTransaction {
	t.Helper()
	to := common.HexToAddress("0x0000000000000000000000000000000000000001")
	transaction := types.NewTx(&types.DynamicFeeTx{
		ChainID: big.NewInt(137), Nonce: nonce, Gas: 21_000,
		To: &to, Value: big.NewInt(0),
	})
	raw, err := transaction.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return chainport.PreparedTransaction{
		Leg: execution.Leg{
			Chain:   market.ChainID("polygon"),
			Account: execution.AccountID("polygon-executor"),
		},
		SignedPayload: raw,
	}
}
