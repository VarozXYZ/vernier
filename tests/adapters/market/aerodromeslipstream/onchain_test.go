package aerodromeslipstream_test

import (
	"context"
	"errors"
	"math/big"
	"testing"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/adapters/market/aerodromeslipstream"
	"github.com/VarozXYZ/vernier/adapters/market/uniswapv3"
)

type rpcNetwork struct {
	responses map[string][]byte
	calls     []geth.CallMsg
	blocks    []evm.BlockReference
}

func (*rpcNetwork) ID() string { return "base-test" }
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
func (*rpcNetwork) CodeAt(context.Context, evm.BlockReference, common.Address) ([]byte, error) {
	return []byte{1}, nil
}
func (*rpcNetwork) Close() {}

func TestBootstrapAcceptsSlipstreamSlot0AndValidatesIdentity(t *testing.T) {
	pool := address(1)
	factory := address(2)
	baseToken := address(3)
	quoteToken := address(4)
	network := bootstrapNetwork(t, factory, baseToken, quoteToken)
	adapter, err := aerodromeslipstream.NewAdapter(aerodromeslipstream.Config{
		Pool: pool, Factory: factory, BaseToken: baseToken, QuoteToken: quoteToken, MaxTickWords: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	block := evm.BlockReference{Number: 123, Hash: common.HexToHash("0x123")}
	data, err := adapter.Bootstrap(context.Background(), network, block)
	if err != nil {
		t.Fatal(err)
	}
	state, _, err := (uniswapv3.Reducer{}).Reduce(context.Background(), nil, data)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := state.(uniswapv3.Snapshot)
	if snapshot.FeePips() != 100 || snapshot.TickSpacing() != 100 || snapshot.Tick() != 0 {
		t.Fatalf("unexpected bootstrap state: fee=%d spacing=%d tick=%d", snapshot.FeePips(), snapshot.TickSpacing(), snapshot.Tick())
	}
	for index, call := range network.calls {
		if call.To == nil || *call.To != pool || network.blocks[index] != block {
			t.Fatal("Slipstream load escaped configured pool or block")
		}
	}
}

func TestBootstrapRejectsUnexpectedFactory(t *testing.T) {
	adapter, err := aerodromeslipstream.NewAdapter(aerodromeslipstream.Config{
		Pool: address(1), Factory: address(9), BaseToken: address(3), QuoteToken: address(4), MaxTickWords: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Bootstrap(
		context.Background(),
		bootstrapNetwork(t, address(2), address(3), address(4)),
		evm.BlockReference{Number: 1, Hash: common.HexToHash("0x1")},
	)
	if err == nil {
		t.Fatal("expected factory mismatch")
	}
}

func TestActiveBlockRefreshesDynamicFeeWithoutReloadingState(t *testing.T) {
	pool := address(1)
	factory := address(2)
	network := bootstrapNetwork(t, factory, address(3), address(4))
	adapter, err := aerodromeslipstream.NewAdapter(aerodromeslipstream.Config{
		Pool: pool, Factory: factory, BaseToken: address(3), QuoteToken: address(4), MaxTickWords: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	initialData, err := adapter.Bootstrap(
		context.Background(), network,
		evm.BlockReference{Number: 1, Hash: common.HexToHash("0x1")},
	)
	if err != nil {
		t.Fatal(err)
	}
	initial, _, err := (uniswapv3.Reducer{}).Reduce(context.Background(), nil, initialData)
	if err != nil {
		t.Fatal(err)
	}
	network.responses[selector("fee()")] = packValues(t, []string{"uint24"}, big.NewInt(500))
	block := evm.BlockReference{Number: 2, Hash: common.HexToHash("0x2")}
	swap := types.Log{
		Address: pool, BlockNumber: block.Number, BlockHash: block.Hash,
		Topics: []common.Hash{
			crypto.Keccak256Hash([]byte("Swap(address,address,int256,int256,uint160,uint128,int24)")),
			common.Hash{}, common.Hash{},
		},
		Data: packValues(t, []string{"int256", "int256", "uint160", "uint128", "int24"},
			big.NewInt(1), big.NewInt(-1), q96(), big.NewInt(1_000_000_000_000), big.NewInt(0)),
	}
	update, err := adapter.DecodeBlock(context.Background(), network, block, []types.Log{swap})
	if err != nil {
		t.Fatal(err)
	}
	reduced, _, err := (uniswapv3.Reducer{}).Reduce(context.Background(), initial, update)
	if err != nil {
		t.Fatal(err)
	}
	if fee := reduced.(uniswapv3.Snapshot).FeePips(); fee != 500 {
		t.Fatalf("dynamic fee not applied: %d", fee)
	}
}

func TestReferenceQuoterUsesTickSpacingTuple(t *testing.T) {
	quoterAddress := address(8)
	network := &rpcNetwork{responses: map[string][]byte{
		selector("quoteExactInputSingle((address,address,uint256,int24,uint160))"): packValues(
			t, []string{"uint256", "uint160", "uint32", "uint256"},
			big.NewInt(999), q96(), uint32(0), big.NewInt(100_000),
		),
	}}
	quoter, err := aerodromeslipstream.NewReferenceQuoter(quoterAddress)
	if err != nil {
		t.Fatal(err)
	}
	output, err := quoter.QuoteExactInputSingle(
		context.Background(), network,
		evm.BlockReference{Number: 4, Hash: common.HexToHash("0x4")},
		address(3), address(4), big.NewInt(1_000), 100,
	)
	if err != nil {
		t.Fatal(err)
	}
	if output.Cmp(big.NewInt(999)) != 0 || len(network.calls) != 1 ||
		network.calls[0].To == nil || *network.calls[0].To != quoterAddress {
		t.Fatalf("unexpected reference quote: output=%s calls=%d", output, len(network.calls))
	}
}

func bootstrapNetwork(
	t *testing.T,
	factory common.Address,
	token0 common.Address,
	token1 common.Address,
) *rpcNetwork {
	t.Helper()
	return &rpcNetwork{responses: map[string][]byte{
		selector("factory()"):         packValues(t, []string{"address"}, factory),
		selector("token0()"):          packValues(t, []string{"address"}, token0),
		selector("token1()"):          packValues(t, []string{"address"}, token1),
		selector("fee()"):             packValues(t, []string{"uint24"}, big.NewInt(100)),
		selector("tickSpacing()"):     packValues(t, []string{"int24"}, big.NewInt(100)),
		selector("liquidity()"):       packValues(t, []string{"uint128"}, big.NewInt(1_000_000_000_000)),
		selector("slot0()"):           packValues(t, []string{"uint160", "int24", "uint16", "uint16", "uint16", "bool"}, q96(), big.NewInt(0), uint16(0), uint16(0), uint16(0), true),
		selector("tickBitmap(int16)"): packValues(t, []string{"uint256"}, big.NewInt(0)),
	}}
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

func address(suffix byte) common.Address {
	var result common.Address
	result[len(result)-1] = suffix
	return result
}

func q96() *big.Int {
	return new(big.Int).Lsh(big.NewInt(1), 96)
}
