package uniswapv4_test

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
	"github.com/VarozXYZ/vernier/adapters/market/uniswapv4"
	"github.com/VarozXYZ/vernier/core/marketstate"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type rpcNetwork struct {
	stateView common.Address
	responses map[string][]byte
	calls     []geth.CallMsg
}

func (*rpcNetwork) ID() string { return "synthetic" }
func (*rpcNetwork) CurrentBlock(context.Context) (evm.BlockReference, error) {
	return evm.BlockReference{}, nil
}
func (*rpcNetwork) SubscribeLogs(context.Context, evm.LogFilter, chan<- types.Log) (evm.Subscription, error) {
	return nil, errors.New("not used")
}
func (*rpcNetwork) LogsAt(context.Context, evm.BlockReference, evm.LogFilter) ([]types.Log, error) {
	return nil, errors.New("not used")
}
func (n *rpcNetwork) CallContract(_ context.Context, _ evm.BlockReference, call geth.CallMsg) ([]byte, error) {
	if call.To == nil || *call.To != n.stateView || len(call.Data) < 4 {
		return nil, errors.New("unexpected contract call")
	}
	n.calls = append(n.calls, call)
	result, ok := n.responses[common.Bytes2Hex(call.Data[:4])]
	if !ok {
		return nil, errors.New("unexpected selector")
	}
	return append([]byte(nil), result...), nil
}
func (*rpcNetwork) CodeAt(context.Context, evm.BlockReference, common.Address) ([]byte, error) {
	return []byte{1}, nil
}
func (*rpcNetwork) Close() {}

func TestBootstrapPublishesCompatibleStateAndNarrowPoolFilter(t *testing.T) {
	manager := common.HexToAddress("0x1000000000000000000000000000000000000001")
	stateView := common.HexToAddress("0x2000000000000000000000000000000000000002")
	currency0 := common.HexToAddress("0x3000000000000000000000000000000000000003")
	currency1 := common.HexToAddress("0x4000000000000000000000000000000000000004")
	poolID, err := uniswapv4.PoolID(currency0, currency1, 3000, 60, common.Address{})
	if err != nil {
		t.Fatal(err)
	}
	network := &rpcNetwork{stateView: stateView, responses: map[string][]byte{
		selector("getSlot0(bytes32)"):            packValues(t, []string{"uint160", "int24", "uint24", "uint24"}, q96(), big.NewInt(0), big.NewInt(0), big.NewInt(3000)),
		selector("getLiquidity(bytes32)"):        packValues(t, []string{"uint128"}, big.NewInt(1_000_000_000_000)),
		selector("getTickBitmap(bytes32,int16)"): packValues(t, []string{"uint256"}, big.NewInt(0)),
	}}
	adapter, err := uniswapv4.NewAdapter(uniswapv4.Config{
		PoolManager: manager, StateView: stateView, PoolID: poolID,
		Currency0: currency0, Currency1: currency1, Fee: 3000, TickSpacing: 60, MaxTickWords: 2,
		Probes: []uniswapv4.CoverageProbe{{ZeroForOne: true, AmountIn: big.NewInt(1000)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	filter := adapter.Filter()
	if filter.Address != manager || len(filter.IndexedTopics) != 1 ||
		len(filter.IndexedTopics[0]) != 1 || filter.IndexedTopics[0][0] != poolID {
		t.Fatalf("filter is not narrowed to pool ID: %+v", filter)
	}
	block := evm.BlockReference{Number: 77, Hash: common.HexToHash("0x77")}
	data, err := adapter.Bootstrap(context.Background(), network, block)
	if err != nil {
		t.Fatal(err)
	}
	mirror, err := marketstate.NewMirror(
		"pool", "watcher", uniswapv4.Reducer{},
		sourceorder.NewMonotonic(sourceorder.BlockPositionKind, false),
		func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
	)
	if err != nil {
		t.Fatal(err)
	}
	event, err := market.NewMarketEvent(market.MarketEvent{
		Market: "pool", Source: "watcher",
		Position: market.SourcePosition{Kind: sourceorder.BlockPositionKind, Value: block.Number},
		Finality: market.FinalityPreconfirmed, ReceivedAt: time.Unix(1_700_000_000, 0).UTC(), Data: data,
	})
	if err != nil {
		t.Fatal(err)
	}
	applied, err := mirror.Reset(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	candidate := market.Market{ID: "pool", BaseToken: "token0", QuoteToken: "token1"}
	quoter, err := uniswapv4.NewQuoter("local", candidate, "token0", "token1")
	if err != nil {
		t.Fatal(err)
	}
	amountIn, _ := market.ParseTokenAmount("token0", "1000")
	quote, err := quoter.Quote(context.Background(), quoteport.Input{
		Snapshot: applied.Snapshot, TokenIn: "token0", TokenOut: "token1", AmountIn: amountIn,
		Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Unix(1_700_000_001, 0).UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if quote.AmountOut.IsZero() || len(network.calls) != 4 {
		t.Fatalf("unexpected local quote or bootstrap calls: output=%s calls=%d", quote.AmountOut, len(network.calls))
	}
}

func TestAdapterRejectsHooksAndPoolKeyMismatch(t *testing.T) {
	manager := common.HexToAddress("0x1000000000000000000000000000000000000001")
	stateView := common.HexToAddress("0x2000000000000000000000000000000000000002")
	currency0 := common.HexToAddress("0x3000000000000000000000000000000000000003")
	currency1 := common.HexToAddress("0x4000000000000000000000000000000000000004")
	poolID, _ := uniswapv4.PoolID(currency0, currency1, 3000, 60, common.Address{})
	base := uniswapv4.Config{
		PoolManager: manager, StateView: stateView, PoolID: poolID,
		Currency0: currency0, Currency1: currency1, Fee: 3000, TickSpacing: 60,
	}
	withHook := base
	withHook.Hooks = common.HexToAddress("0x5000000000000000000000000000000000000005")
	if _, err := uniswapv4.NewAdapter(withHook); err == nil {
		t.Fatal("hooked V4 pool was accepted")
	}
	wrongKey := base
	wrongKey.Fee = 500
	if _, err := uniswapv4.NewAdapter(wrongKey); err == nil {
		t.Fatal("mismatched V4 pool key was accepted")
	}
}

func TestBootstrapRejectsProtocolFee(t *testing.T) {
	manager := common.HexToAddress("0x1000000000000000000000000000000000000001")
	stateView := common.HexToAddress("0x2000000000000000000000000000000000000002")
	currency0 := common.HexToAddress("0x3000000000000000000000000000000000000003")
	currency1 := common.HexToAddress("0x4000000000000000000000000000000000000004")
	poolID, _ := uniswapv4.PoolID(currency0, currency1, 3000, 60, common.Address{})
	adapter, err := uniswapv4.NewAdapter(uniswapv4.Config{
		PoolManager: manager, StateView: stateView, PoolID: poolID,
		Currency0: currency0, Currency1: currency1, Fee: 3000, TickSpacing: 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	network := &rpcNetwork{stateView: stateView, responses: map[string][]byte{
		selector("getSlot0(bytes32)"): packValues(t, []string{"uint160", "int24", "uint24", "uint24"}, q96(), big.NewInt(0), big.NewInt(1), big.NewInt(3000)),
	}}
	_, err = adapter.Bootstrap(context.Background(), network, evm.BlockReference{Number: 1, Hash: common.HexToHash("0x01")})
	if err == nil {
		t.Fatal("V4 pool with protocol fee was accepted")
	}
}

func TestDecodeBlockOrdersLiquidityBeforeSwap(t *testing.T) {
	manager := common.HexToAddress("0x1000000000000000000000000000000000000001")
	stateView := common.HexToAddress("0x2000000000000000000000000000000000000002")
	currency0 := common.HexToAddress("0x3000000000000000000000000000000000000003")
	currency1 := common.HexToAddress("0x4000000000000000000000000000000000000004")
	poolID, _ := uniswapv4.PoolID(currency0, currency1, 3000, 60, common.Address{})
	adapter, err := uniswapv4.NewAdapter(uniswapv4.Config{
		PoolManager: manager, StateView: stateView, PoolID: poolID,
		Currency0: currency0, Currency1: currency1, Fee: 3000, TickSpacing: 60,
	})
	if err != nil {
		t.Fatal(err)
	}
	block := evm.BlockReference{Number: 88, Hash: common.HexToHash("0x88")}
	sender := common.HexToAddress("0x5000000000000000000000000000000000000005")
	modify := types.Log{
		Address: manager, BlockNumber: block.Number, BlockHash: block.Hash, TxIndex: 0, Index: 1,
		Topics: []common.Hash{
			crypto.Keccak256Hash([]byte("ModifyLiquidity(bytes32,address,int24,int24,int256,bytes32)")),
			poolID, common.BytesToHash(sender.Bytes()),
		},
		Data: packValues(t, []string{"int24", "int24", "int256", "bytes32"},
			big.NewInt(-60), big.NewInt(60), big.NewInt(500), [32]byte{}),
	}
	swap := types.Log{
		Address: manager, BlockNumber: block.Number, BlockHash: block.Hash, TxIndex: 0, Index: 2,
		Topics: []common.Hash{
			crypto.Keccak256Hash([]byte("Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)")),
			poolID, common.BytesToHash(sender.Bytes()),
		},
		Data: packValues(t, []string{"int128", "int128", "uint160", "uint128", "int24", "uint24"},
			big.NewInt(1), big.NewInt(-1), q96(), big.NewInt(1500), big.NewInt(0), big.NewInt(3000)),
	}
	update, err := adapter.DecodeBlock(context.Background(), nil, block, []types.Log{swap, modify})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := uniswapv3.NewStateUpdate(q96(), 0, big.NewInt(1000), 3000, 60, nil)
	if err != nil {
		t.Fatal(err)
	}
	reducer := uniswapv4.Reducer{}
	state, _, err := reducer.Reduce(context.Background(), nil, initial)
	if err != nil {
		t.Fatal(err)
	}
	state, _, err = reducer.Reduce(context.Background(), state, update)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := state.(uniswapv3.Snapshot)
	if snapshot.Liquidity().Cmp(big.NewInt(1500)) != 0 || len(snapshot.Ticks()) != 2 {
		t.Fatalf("unexpected reduced V4 state: liquidity=%s ticks=%d", snapshot.Liquidity(), len(snapshot.Ticks()))
	}
}

func selector(signature string) string {
	return common.Bytes2Hex(crypto.Keccak256([]byte(signature))[:4])
}

func packValues(t *testing.T, names []string, values ...any) []byte {
	t.Helper()
	arguments := make(abi.Arguments, len(names))
	for index, name := range names {
		kind, err := abi.NewType(name, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		arguments[index] = abi.Argument{Type: kind}
	}
	encoded, err := arguments.Pack(values...)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func q96() *big.Int {
	return new(big.Int).Lsh(big.NewInt(1), 96)
}

var _ evm.Network = (*rpcNetwork)(nil)
