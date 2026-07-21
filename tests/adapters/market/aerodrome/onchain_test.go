package aerodrome_test

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
	"github.com/VarozXYZ/vernier/adapters/market/aerodrome"
	"github.com/VarozXYZ/vernier/adapters/market/constantproduct"
)

type network struct{ responses map[string][]byte }

func (*network) ID() string { return "base" }
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
	return n.responses[hex.EncodeToString(call.Data[:4])], nil
}
func (*network) CodeAt(context.Context, evm.BlockReference, common.Address) ([]byte, error) {
	return []byte{1}, nil
}
func (*network) Close() {}

func TestVolatileAdapterUsesAerodromeSyncTopicAndGenericState(t *testing.T) {
	pool, factory, base, quote := address(1), address(2), address(3), address(4)
	chain := &network{responses: map[string][]byte{
		"0dfe1681": addressResult(quote),
		"d21220a7": addressResult(base),
		"c45a0155": addressResult(factory),
		"0902f1ac": words(big.NewInt(1_000), big.NewInt(2_000), big.NewInt(10)),
	}}
	adapter, err := aerodrome.NewAdapter(aerodrome.Config{Pool: pool, Factory: factory, BaseToken: base, QuoteToken: quote, FeeBPS: 100})
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
	if filter.Address != pool || len(filter.Topics) != 1 || filter.Topics[0] != crypto.Keccak256Hash([]byte("Sync(uint256,uint256)")) {
		t.Fatalf("unexpected Aerodrome Sync filter: %+v", filter)
	}
	data, err = adapter.DecodeLog(context.Background(), chain, block, types.Log{
		Address: pool, Topics: []common.Hash{aerodrome.SyncTopic()}, Data: words(big.NewInt(1_100), big.NewInt(2_100)), BlockNumber: block.Number, BlockHash: block.Hash,
	})
	if err != nil {
		t.Fatal(err)
	}
	update = data.(constantproduct.ReserveUpdate)
	if update.BaseReserve().Cmp(big.NewInt(2_100)) != 0 || update.QuoteReserve().Cmp(big.NewInt(1_100)) != 0 {
		t.Fatalf("decoded reserves base=%s quote=%s", update.BaseReserve(), update.QuoteReserve())
	}
}

func TestReferenceQuoterUsesAerodromeRoute(t *testing.T) {
	router, factory, base, quote := address(10), address(11), address(12), address(13)
	routerABI, err := abi.JSON(strings.NewReader(`[{"type":"function","name":"getAmountsOut","inputs":[{"name":"amountIn","type":"uint256"},{"name":"routes","type":"tuple[]","components":[{"name":"from","type":"address"},{"name":"to","type":"address"},{"name":"stable","type":"bool"},{"name":"factory","type":"address"}]}],"outputs":[{"name":"amounts","type":"uint256[]"}]}]`))
	if err != nil {
		t.Fatal(err)
	}
	response, err := routerABI.Methods["getAmountsOut"].Outputs.Pack([]*big.Int{big.NewInt(100), big.NewInt(97)})
	if err != nil {
		t.Fatal(err)
	}
	chain := &network{responses: map[string][]byte{hex.EncodeToString(crypto.Keccak256([]byte("getAmountsOut(uint256,(address,address,bool,address)[])"))[:4]): response}}
	quoter, err := aerodrome.NewReferenceQuoter(router, factory, base, quote, false)
	if err != nil {
		t.Fatal(err)
	}
	result, err := quoter.QuoteExactInput(context.Background(), chain, evm.BlockReference{Hash: common.BigToHash(big.NewInt(1))}, base, quote, big.NewInt(100))
	if err != nil {
		t.Fatal(err)
	}
	if result.Cmp(big.NewInt(97)) != 0 {
		t.Fatalf("unexpected Aerodrome route quote %s", result)
	}
}

func address(value int64) common.Address        { return common.BigToAddress(big.NewInt(value)) }
func addressResult(value common.Address) []byte { return common.LeftPadBytes(value.Bytes(), 32) }
func words(values ...*big.Int) []byte {
	result := make([]byte, 32*len(values))
	for index, value := range values {
		value.FillBytes(result[index*32 : (index+1)*32])
	}
	return result
}
