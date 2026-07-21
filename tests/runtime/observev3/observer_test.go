package observev3_test

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/runtime/observev3"
)

type liveSubscription struct{ err chan error }

func (s *liveSubscription) Err() <-chan error { return s.err }
func (*liveSubscription) Unsubscribe()        {}

type liveNetwork struct {
	pool       common.Address
	quoter     common.Address
	block      evm.BlockReference
	activeLog  types.Log
	poolResult map[string][]byte
	filter     evm.LogFilter
	reference  *big.Int
}

func (*liveNetwork) ID() string { return "ethereum" }
func (n *liveNetwork) CurrentBlock(context.Context) (evm.BlockReference, error) {
	return n.block, nil
}
func (n *liveNetwork) SubscribeLogs(_ context.Context, filter evm.LogFilter, output chan<- types.Log) (evm.Subscription, error) {
	n.filter = filter
	output <- n.activeLog
	return &liveSubscription{err: make(chan error)}, nil
}
func (n *liveNetwork) LogsAt(_ context.Context, block evm.BlockReference, _ evm.LogFilter) ([]types.Log, error) {
	if block.Number != n.activeLog.BlockNumber || block.Hash != n.activeLog.BlockHash {
		return nil, errors.New("unexpected active block")
	}
	return []types.Log{n.activeLog}, nil
}
func (n *liveNetwork) CallContract(_ context.Context, _ evm.BlockReference, call geth.CallMsg) ([]byte, error) {
	if call.To == nil || len(call.Data) < 4 {
		return nil, errors.New("invalid call")
	}
	if *call.To == n.quoter {
		amount := n.reference
		if amount == nil {
			amount = big.NewInt(996999)
		}
		return packRuntimeValues([]string{"uint256", "uint160", "uint32", "uint256"},
			amount, runtimeQ96(), uint32(0), big.NewInt(100000))
	}
	if *call.To != n.pool {
		return nil, errors.New("unexpected contract")
	}
	result, exists := n.poolResult[common.Bytes2Hex(call.Data[:4])]
	if !exists {
		return nil, errors.New("unexpected pool method")
	}
	return result, nil
}
func (*liveNetwork) CodeAt(context.Context, evm.BlockReference, common.Address) ([]byte, error) {
	return []byte{1}, nil
}
func (*liveNetwork) Close() {}

func TestObserverEmitsRedactedParityRecordsAndStopsAfterUpdate(t *testing.T) {
	config := observerConfig(t)
	token0 := common.HexToAddress("0x3000000000000000000000000000000000000003")
	token1 := common.HexToAddress("0x4000000000000000000000000000000000000004")
	activeHash := common.HexToHash("0x11")
	network := &liveNetwork{
		pool: config.Pool, quoter: config.QuoterV2,
		block: evm.BlockReference{Number: 10, Hash: common.HexToHash("0x10")},
		poolResult: map[string][]byte{
			runtimeSelector("token0()"):          mustPackRuntime(t, []string{"address"}, token0),
			runtimeSelector("token1()"):          mustPackRuntime(t, []string{"address"}, token1),
			runtimeSelector("fee()"):             mustPackRuntime(t, []string{"uint24"}, big.NewInt(3000)),
			runtimeSelector("tickSpacing()"):     mustPackRuntime(t, []string{"int24"}, big.NewInt(60)),
			runtimeSelector("liquidity()"):       mustPackRuntime(t, []string{"uint128"}, big.NewInt(1_000_000_000_000)),
			runtimeSelector("slot0()"):           mustPackRuntime(t, []string{"uint160", "int24", "uint16", "uint16", "uint16", "uint8", "bool"}, runtimeQ96(), big.NewInt(0), uint16(0), uint16(0), uint16(0), uint8(0), true),
			runtimeSelector("tickBitmap(int16)"): mustPackRuntime(t, []string{"uint256"}, big.NewInt(0)),
		},
	}
	network.activeLog = types.Log{
		Address: config.Pool, BlockNumber: 11, BlockHash: activeHash,
		Topics: []common.Hash{
			crypto.Keccak256Hash([]byte("Swap(address,address,int256,int256,uint160,uint128,int24)")),
			common.Hash{}, common.Hash{},
		},
		Data: mustPackRuntime(t, []string{"int256", "int256", "uint160", "uint128", "int24"},
			big.NewInt(1), big.NewInt(-1), runtimeQ96(), big.NewInt(1_000_000_000_000), big.NewInt(0)),
	}
	var output bytes.Buffer
	observer, err := observev3.New(config, network, observev3.Options{
		Format: "jsonl", Updates: 1, Output: &output,
		Clock: func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := observer.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	if strings.Count(text, `"type":"snapshot"`) != 2 || !strings.Contains(text, `"block":11`) ||
		strings.Count(text, `"parity":true`) != 4 {
		t.Fatalf("unexpected observer output:\n%s", text)
	}
	if strings.Contains(text, config.PoolAddress) || strings.Contains(text, config.QuoterV2Address) ||
		strings.Contains(text, "HTTP_URL") || strings.Contains(text, "WS_URL") {
		t.Fatalf("observer output leaked operational configuration:\n%s", text)
	}
	if len(network.filter.Topics) != 4 || network.filter.Address != config.Pool {
		t.Fatalf("unexpected subscription filter: %+v", network.filter)
	}

	network.reference = big.NewInt(996998)
	output.Reset()
	mismatch, err := observev3.New(config, network, observev3.Options{
		Format: "jsonl", Updates: 1, Output: &output,
		Clock: func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := mismatch.Run(context.Background()); !errors.Is(err, observev3.ErrParityMismatch) {
		t.Fatalf("expected parity mismatch, got %v", err)
	}
	if !strings.Contains(output.String(), `"parity":false`) {
		t.Fatalf("mismatch evidence was not emitted: %s", output.String())
	}
}

func runtimeSelector(signature string) string {
	return common.Bytes2Hex(crypto.Keccak256([]byte(signature))[:4])
}

func mustPackRuntime(t *testing.T, names []string, values ...any) []byte {
	t.Helper()
	result, err := packRuntimeValues(names, values...)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func packRuntimeValues(names []string, values ...any) ([]byte, error) {
	arguments := make(abi.Arguments, len(names))
	for index, name := range names {
		kind, err := abi.NewType(name, "", nil)
		if err != nil {
			return nil, err
		}
		arguments[index] = abi.Argument{Type: kind}
	}
	return arguments.Pack(values...)
}

func runtimeQ96() *big.Int {
	value, _ := new(big.Int).SetString("79228162514264337593543950336", 10)
	return value
}
