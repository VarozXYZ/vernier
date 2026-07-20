package chainlink_test

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
	"github.com/VarozXYZ/vernier/adapters/price/chainlink"
)

type network struct {
	responses map[string][]byte
}

func (*network) ID() string { return "base" }
func (*network) CurrentBlock(context.Context) (evm.BlockReference, error) {
	return evm.BlockReference{}, nil
}
func (*network) SubscribeLogs(context.Context, evm.LogFilter, chan<- types.Log) (evm.Subscription, error) {
	return nil, errors.New("not used")
}
func (*network) LogsAt(context.Context, evm.BlockReference, evm.LogFilter) ([]types.Log, error) {
	return nil, errors.New("not used")
}
func (n *network) CallContract(_ context.Context, _ evm.BlockReference, call geth.CallMsg) ([]byte, error) {
	return n.responses[common.Bytes2Hex(call.Data[:4])], nil
}
func (*network) CodeAt(context.Context, evm.BlockReference, common.Address) ([]byte, error) {
	return nil, errors.New("not used")
}
func (*network) Close() {}

func TestReadReturnsExactRationalObservation(t *testing.T) {
	rpc := &network{responses: map[string][]byte{
		selector("decimals()"): pack(t, []string{"uint8"}, uint8(8)),
		selector("latestRoundData()"): pack(
			t, []string{"uint80", "int256", "uint256", "uint256", "uint80"},
			big.NewInt(9), big.NewInt(350_012_345_678), big.NewInt(0), big.NewInt(1_700_000_000), big.NewInt(9),
		),
	}}
	block := evm.BlockReference{Number: 10, Hash: common.HexToHash("0x10")}
	observation, err := chainlink.Read(context.Background(), rpc, block, common.HexToAddress("0x1"))
	if err != nil {
		t.Fatal(err)
	}
	if got := observation.Value().RatString(); got != "175006172839/50000000" {
		t.Fatalf("value = %s", got)
	}
	if !observation.UpdatedAt.Equal(time.Unix(1_700_000_000, 0).UTC()) || observation.Block != block {
		t.Fatalf("unexpected evidence: %+v", observation)
	}
}

func TestReadRejectsIncompleteRound(t *testing.T) {
	rpc := &network{responses: map[string][]byte{
		selector("decimals()"): pack(t, []string{"uint8"}, uint8(8)),
		selector("latestRoundData()"): pack(
			t, []string{"uint80", "int256", "uint256", "uint256", "uint80"},
			big.NewInt(10), big.NewInt(1), big.NewInt(0), big.NewInt(1_700_000_000), big.NewInt(9),
		),
	}}
	_, err := chainlink.Read(
		context.Background(), rpc,
		evm.BlockReference{Number: 10, Hash: common.HexToHash("0x10")},
		common.HexToAddress("0x1"),
	)
	if err == nil {
		t.Fatal("expected incomplete round rejection")
	}
}

func selector(signature string) string {
	return common.Bytes2Hex(crypto.Keccak256([]byte(signature))[:4])
}

func pack(t *testing.T, names []string, values ...any) []byte {
	t.Helper()
	arguments := make(abi.Arguments, len(names))
	for index, name := range names {
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
