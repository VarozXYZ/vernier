package uniswapv3_test

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/adapters/feed/sourceorder"
	"github.com/VarozXYZ/vernier/adapters/market/uniswapv3"
	"github.com/VarozXYZ/vernier/core/marketstate"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type rpcNetwork struct {
	pool      common.Address
	responses map[string][]byte
	calls     []geth.CallMsg
	blocks    []evm.BlockReference
}

func (*rpcNetwork) ID() string { return "test" }
func (*rpcNetwork) CurrentBlock(context.Context) (evm.BlockReference, error) {
	return evm.BlockReference{}, nil
}
func (*rpcNetwork) SubscribeLogs(context.Context, evm.LogFilter, chan<- types.Log) (evm.Subscription, error) {
	return nil, errors.New("not used")
}
func (*rpcNetwork) LogsAt(context.Context, evm.BlockReference, evm.LogFilter) ([]types.Log, error) {
	return nil, errors.New("not used")
}
func (n *rpcNetwork) CallContract(_ context.Context, block evm.BlockReference, call geth.CallMsg) ([]byte, error) {
	n.calls = append(n.calls, call)
	n.blocks = append(n.blocks, block)
	if len(call.Data) < 4 {
		return nil, errors.New("missing selector")
	}
	result, exists := n.responses[common.Bytes2Hex(call.Data[:4])]
	if !exists {
		return nil, errors.New("unexpected contract call")
	}
	return append([]byte(nil), result...), nil
}
func (n *rpcNetwork) CodeAt(context.Context, evm.BlockReference, common.Address) ([]byte, error) {
	return []byte{1}, nil
}
func (*rpcNetwork) Close() {}

func TestCoveredQuoteFailsClosedOutsideLoadedWords(t *testing.T) {
	coverage, err := uniswapv3.NewTickCoverage(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	update, err := uniswapv3.NewCoveredStateUpdate(q96(), 0, big.NewInt(1_000_000_000_000), 3000, 60, nil, coverage)
	if err != nil {
		t.Fatal(err)
	}
	mirror := newV3Mirror(t)
	snapshot := applyV3(t, mirror, v3Event(t, 1, update))
	quoter, _ := uniswapv3.NewQuoter("local-v3", testMarket(), "token0", "token1")
	amount, _ := market.ParseTokenAmount("token0", "1")
	_, err = quoter.Quote(context.Background(), quoteport.Input{
		Snapshot: snapshot, TokenIn: "token0", TokenOut: "token1", AmountIn: amount,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: testTime(),
	})
	if !errors.Is(err, uniswapv3.ErrInsufficientTickCoverage) {
		t.Fatalf("expected insufficient coverage, got %v", err)
	}
}

func TestBootstrapLoadsCanonicalPoolStateAtOneBlockHash(t *testing.T) {
	pool := common.HexToAddress("0x1000000000000000000000000000000000000001")
	token0 := common.HexToAddress("0x2000000000000000000000000000000000000002")
	token1 := common.HexToAddress("0x3000000000000000000000000000000000000003")
	network := &rpcNetwork{pool: pool, responses: map[string][]byte{
		selector("token0()"):          packValues(t, []string{"address"}, token0),
		selector("token1()"):          packValues(t, []string{"address"}, token1),
		selector("fee()"):             packValues(t, []string{"uint24"}, big.NewInt(3000)),
		selector("tickSpacing()"):     packValues(t, []string{"int24"}, big.NewInt(60)),
		selector("liquidity()"):       packValues(t, []string{"uint128"}, big.NewInt(1_000_000_000_000)),
		selector("slot0()"):           packValues(t, []string{"uint160", "int24", "uint16", "uint16", "uint16", "uint8", "bool"}, q96(), big.NewInt(0), uint16(0), uint16(0), uint16(0), uint8(0), true),
		selector("tickBitmap(int16)"): packValues(t, []string{"uint256"}, big.NewInt(0)),
	}}
	adapter, err := uniswapv3.NewAdapter(uniswapv3.OnChainConfig{Pool: pool, MaxTickWords: 1})
	if err != nil {
		t.Fatal(err)
	}
	block := evm.BlockReference{Number: 123, Hash: common.HexToHash("0x123")}
	data, err := adapter.Bootstrap(context.Background(), network, block)
	if err != nil {
		t.Fatal(err)
	}
	update, ok := data.(uniswapv3.StateUpdate)
	if !ok {
		t.Fatalf("bootstrap payload %T", data)
	}
	snapshot := applyV3(t, newV3Mirror(t), v3Event(t, block.Number, update)).Data().(uniswapv3.Snapshot)
	if snapshot.Coverage().Full() || snapshot.Coverage().MinWord() != 0 || snapshot.Coverage().MaxWord() != 0 {
		t.Fatalf("unexpected coverage %+v", snapshot.Coverage())
	}
	info, ok := adapter.PoolInfo()
	if !ok || info.Token0 != token0 || info.Token1 != token1 || info.Fee != 3000 {
		t.Fatalf("unexpected pool info %+v, %v", info, ok)
	}
	for index, call := range network.calls {
		if call.To == nil || *call.To != pool || network.blocks[index] != block {
			t.Fatal("pool load escaped configured pool or block hash")
		}
	}
}

func TestDecoderOrdersMintBeforeSwapAndAppliesIncrementally(t *testing.T) {
	pool := common.HexToAddress("0x1000000000000000000000000000000000000001")
	adapter, err := uniswapv3.NewAdapter(uniswapv3.OnChainConfig{Pool: pool, MaxTickWords: 1})
	if err != nil {
		t.Fatal(err)
	}
	block := evm.BlockReference{Number: 20, Hash: common.HexToHash("0x20")}
	mint := types.Log{
		Address: pool, BlockNumber: block.Number, BlockHash: block.Hash, TxIndex: 0, Index: 1,
		Topics: []common.Hash{
			crypto.Keccak256Hash([]byte("Mint(address,address,int24,int24,uint128,uint256,uint256)")),
			common.BytesToHash(common.HexToAddress("0x4000000000000000000000000000000000000004").Bytes()),
			signedTopic(-60), signedTopic(60),
		},
		Data: packValues(t, []string{"address", "uint128", "uint256", "uint256"},
			common.HexToAddress("0x5000000000000000000000000000000000000005"),
			big.NewInt(500), big.NewInt(1), big.NewInt(1)),
	}
	swap := types.Log{
		Address: pool, BlockNumber: block.Number, BlockHash: block.Hash, TxIndex: 0, Index: 2,
		Topics: []common.Hash{
			crypto.Keccak256Hash([]byte("Swap(address,address,int256,int256,uint160,uint128,int24)")),
			common.Hash{}, common.Hash{},
		},
		Data: packValues(t, []string{"int256", "int256", "uint160", "uint128", "int24"},
			big.NewInt(1), big.NewInt(-1), q96(), big.NewInt(1500), big.NewInt(0)),
	}
	data, err := adapter.DecodeBlock(context.Background(), nil, block, []types.Log{swap, mint})
	if err != nil {
		t.Fatal(err)
	}
	mirror, err := marketstate.NewMirror("market", "feed", uniswapv3.Reducer{}, sourceorder.NewMonotonic(sourceorder.BlockPositionKind, false), func() time.Time { return testTime() })
	if err != nil {
		t.Fatal(err)
	}
	initial := stateUpdateForTest(t, big.NewInt(1000), nil)
	applyV3(t, mirror, v3Event(t, 1, initial))
	state := applyV3(t, mirror, v3Event(t, 20, data)).Data().(uniswapv3.Snapshot)
	if state.Liquidity().Cmp(big.NewInt(1500)) != 0 || len(state.Ticks()) != 2 {
		t.Fatalf("unexpected reduced block: liquidity=%s ticks=%d", state.Liquidity(), len(state.Ticks()))
	}
}

func selector(signature string) string {
	return common.Bytes2Hex(crypto.Keccak256([]byte(signature))[:4])
}

func packValues(t *testing.T, typeNames []string, values ...any) []byte {
	t.Helper()
	arguments := make(abi.Arguments, len(typeNames))
	for index, name := range typeNames {
		kind, err := abi.NewType(name, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		arguments[index] = abi.Argument{Type: kind}
	}
	result, err := arguments.Pack(values...)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func signedTopic(value int64) common.Hash {
	integer := big.NewInt(value)
	if value < 0 {
		integer.Add(integer, new(big.Int).Lsh(big.NewInt(1), 256))
	}
	return common.BigToHash(integer)
}
