package uniswapv2_test

import (
	"context"
	"encoding/hex"
	"math/big"
	"strings"
	"testing"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/adapters/market/constantproduct"
	"github.com/VarozXYZ/vernier/adapters/market/uniswapv2"
)

type network struct {
	responses map[string][]byte
	calls     []common.Address
}

func (*network) ID() string { return "test" }
func (*network) CurrentBlock(context.Context) (evm.BlockReference, error) {
	return evm.BlockReference{}, nil
}
func (*network) SubscribeLogs(context.Context, evm.LogFilter, chan<- types.Log) (evm.Subscription, error) {
	return nil, nil
}
func (*network) LogsAt(context.Context, evm.BlockReference, evm.LogFilter) ([]types.Log, error) {
	return nil, nil
}
func (n *network) CallContract(_ context.Context, _ evm.BlockReference, call geth.CallMsg) ([]byte, error) {
	n.calls = append(n.calls, *call.To)
	return n.responses[hex.EncodeToString(call.Data[:4])], nil
}
func (*network) CodeAt(context.Context, evm.BlockReference, common.Address) ([]byte, error) {
	return []byte{1}, nil
}
func (*network) Close() {}

func TestAdapterBootstrapsAndMapsPoolOrderToMarketOrder(t *testing.T) {
	pool := address(1)
	factory := address(2)
	base := address(3)
	quote := address(4)
	chain := &network{responses: map[string][]byte{
		"0dfe1681": addressResult(quote),
		"d21220a7": addressResult(base),
		"c45a0155": addressResult(factory),
		"0902f1ac": words(big.NewInt(1_000), big.NewInt(2_000), big.NewInt(10)),
	}}
	adapter, err := uniswapv2.NewAdapter(uniswapv2.Config{
		Pool: pool, Factory: factory, BaseToken: base, QuoteToken: quote, FeeBPS: 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	block := evm.BlockReference{Number: 10, Hash: common.BigToHash(big.NewInt(10))}
	data, err := adapter.Bootstrap(context.Background(), chain, block)
	if err != nil {
		t.Fatal(err)
	}
	update := data.(constantproduct.ReserveUpdate)
	if update.BaseReserve().Cmp(big.NewInt(2_000)) != 0 || update.QuoteReserve().Cmp(big.NewInt(1_000)) != 0 {
		t.Fatalf("market reserves base=%s quote=%s", update.BaseReserve(), update.QuoteReserve())
	}
	filter := adapter.Filter()
	if filter.Address != pool || len(filter.Topics) != 1 || filter.Topics[0] != crypto.Keccak256Hash([]byte("Sync(uint112,uint112)")) {
		t.Fatalf("unexpected Sync filter: %+v", filter)
	}

	logs := []types.Log{
		syncLog(pool, block, 1, 1_100, 2_100),
		syncLog(pool, block, 2, 1_200, 2_200),
	}
	data, err = adapter.DecodeBlock(context.Background(), chain, block, logs)
	if err != nil {
		t.Fatal(err)
	}
	update = data.(constantproduct.ReserveUpdate)
	if update.BaseReserve().Cmp(big.NewInt(2_200)) != 0 || update.QuoteReserve().Cmp(big.NewInt(1_200)) != 0 {
		t.Fatalf("latest Sync was not selected: base=%s quote=%s", update.BaseReserve(), update.QuoteReserve())
	}
}

func TestReferenceQuoterDecodesRouterAmounts(t *testing.T) {
	router := address(10)
	chain := &network{responses: map[string][]byte{
		"d06ca61f": routerResult(t, "getAmountsOut", big.NewInt(100), big.NewInt(97)),
		"1f00ca74": routerResult(t, "getAmountsIn", big.NewInt(104), big.NewInt(100)),
	}}
	quoter, err := uniswapv2.NewReferenceQuoter(router)
	if err != nil {
		t.Fatal(err)
	}
	result, err := quoter.QuoteExactInput(
		context.Background(), chain, evm.BlockReference{Hash: common.BigToHash(big.NewInt(1))},
		address(1), address(2), big.NewInt(100),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Cmp(big.NewInt(97)) != 0 || len(chain.calls) != 1 || chain.calls[0] != router {
		t.Fatalf("unexpected router quote %s via %v", result, chain.calls)
	}
	exactOutput, err := quoter.QuoteExactOutput(
		context.Background(), chain, evm.BlockReference{Hash: common.BigToHash(big.NewInt(1))},
		address(1), address(2), big.NewInt(100),
	)
	if err != nil {
		t.Fatal(err)
	}
	if exactOutput.Cmp(big.NewInt(104)) != 0 || len(chain.calls) != 2 {
		t.Fatalf("unexpected exact-output router quote %s via %v", exactOutput, chain.calls)
	}
}

func syncLog(pool common.Address, block evm.BlockReference, index uint, reserve0, reserve1 int64) types.Log {
	return types.Log{
		Address: pool, Topics: []common.Hash{crypto.Keccak256Hash([]byte("Sync(uint112,uint112)"))},
		Data: words(big.NewInt(reserve0), big.NewInt(reserve1)), BlockNumber: block.Number, BlockHash: block.Hash, Index: index,
	}
}

func address(value int64) common.Address { return common.BigToAddress(big.NewInt(value)) }

func addressResult(value common.Address) []byte { return common.LeftPadBytes(value.Bytes(), 32) }

func words(values ...*big.Int) []byte {
	result := make([]byte, 32*len(values))
	for index, value := range values {
		value.FillBytes(result[index*32 : (index+1)*32])
	}
	return result
}

func routerResult(t *testing.T, method string, values ...*big.Int) []byte {
	t.Helper()
	definition := `[
		{"type":"function","name":"getAmountsOut","stateMutability":"view","inputs":[{"type":"uint256"},{"type":"address[]"}],"outputs":[{"type":"uint256[]"}]},
		{"type":"function","name":"getAmountsIn","stateMutability":"view","inputs":[{"type":"uint256"},{"type":"address[]"}],"outputs":[{"type":"uint256[]"}]}
	]`
	parsed, err := abi.JSON(strings.NewReader(definition))
	if err != nil {
		t.Fatal(err)
	}
	result, err := parsed.Methods[method].Outputs.Pack(values)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
