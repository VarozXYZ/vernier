package evm_test

import (
	"context"
	"encoding/json"
	"math/big"
	"testing"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	gethcrypto "github.com/ethereum/go-ethereum/crypto"

	evmadapter "github.com/VarozXYZ/vernier/adapters/chain/evm"
)

type stateOverrideCall struct {
	method string
	args   []any
}

type stateOverrideRPC struct {
	calls []stateOverrideCall
}

func (r *stateOverrideRPC) CallContext(
	_ context.Context,
	result any,
	method string,
	args ...any,
) error {
	r.calls = append(r.calls, stateOverrideCall{method: method, args: args})
	switch target := result.(type) {
	case *hexutil.Bytes:
		*target = hexutil.Bytes{0x12, 0x34}
	case *hexutil.Uint64:
		*target = hexutil.Uint64(321_000)
	}
	return nil
}

func TestERC20StateOverrideSimulatorInjectsBalanceAndDynamicAllowance(t *testing.T) {
	rpc := &stateOverrideRPC{}
	token := common.HexToAddress("0x1111111111111111111111111111111111111111")
	owner := common.HexToAddress("0x2222222222222222222222222222222222222222")
	router := common.HexToAddress("0x3333333333333333333333333333333333333333")
	simulator, err := evmadapter.NewERC20StateOverrideSimulator(
		evmadapter.ERC20StateOverrideSimulatorConfig{
			Client: rpc, Token: token, Owner: owner,
			BalanceSlot: 3, AllowanceSlot: 4,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	call := geth.CallMsg{
		From: owner, To: &router, Value: big.NewInt(0),
		Data: []byte{0xaa, 0xbb},
	}
	result, err := simulator.CallContract(context.Background(), call, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != string([]byte{0x12, 0x34}) {
		t.Fatalf("result=%x", result)
	}
	gas, err := simulator.EstimateGas(context.Background(), call)
	if err != nil {
		t.Fatal(err)
	}
	if gas != 321_000 || len(rpc.calls) != 2 ||
		rpc.calls[0].method != "eth_call" ||
		rpc.calls[1].method != "eth_estimateGas" {
		t.Fatalf("gas=%d calls=%+v", gas, rpc.calls)
	}
	assertStateOverride(
		t,
		rpc.calls[0].args,
		token,
		mappingKey(owner, 3),
		nestedMappingKey(router, mappingKey(owner, 4)),
	)
	assertStateOverride(
		t,
		rpc.calls[1].args,
		token,
		mappingKey(owner, 3),
		nestedMappingKey(router, mappingKey(owner, 4)),
	)
}

func TestERC20StateOverrideSimulatorRejectsAnotherSender(t *testing.T) {
	owner := common.HexToAddress("0x2222222222222222222222222222222222222222")
	router := common.HexToAddress("0x3333333333333333333333333333333333333333")
	simulator, err := evmadapter.NewERC20StateOverrideSimulator(
		evmadapter.ERC20StateOverrideSimulatorConfig{
			Client:      &stateOverrideRPC{},
			Token:       common.HexToAddress("0x1111111111111111111111111111111111111111"),
			Owner:       owner,
			BalanceSlot: 3, AllowanceSlot: 4,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = simulator.CallContract(
		context.Background(),
		geth.CallMsg{
			From: common.HexToAddress("0x4444444444444444444444444444444444444444"),
			To:   &router,
		},
		nil,
	)
	if err == nil {
		t.Fatal("expected mismatched sender rejection")
	}
}

func assertStateOverride(
	t *testing.T,
	args []any,
	token common.Address,
	balanceKey common.Hash,
	allowanceKey common.Hash,
) {
	t.Helper()
	if len(args) != 3 || args[1] != "latest" {
		t.Fatalf("RPC arguments=%+v", args)
	}
	encoded, err := json.Marshal(args[2])
	if err != nil {
		t.Fatal(err)
	}
	var overrides map[string]struct {
		StateDiff map[string]string `json:"stateDiff"`
	}
	if err := json.Unmarshal(encoded, &overrides); err != nil {
		t.Fatal(err)
	}
	state, ok := overrides[token.Hex()]
	if !ok {
		state, ok = overrides[token.String()]
	}
	if !ok || len(state.StateDiff) != 2 {
		t.Fatalf("override=%s", encoded)
	}
	maximum := "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	if state.StateDiff[balanceKey.Hex()] != maximum ||
		state.StateDiff[allowanceKey.Hex()] != maximum {
		t.Fatalf("stateDiff=%+v", state.StateDiff)
	}
}

func mappingKey(address common.Address, slot uint64) common.Hash {
	return gethcrypto.Keccak256Hash(
		common.LeftPadBytes(address.Bytes(), 32),
		common.LeftPadBytes(new(big.Int).SetUint64(slot).Bytes(), 32),
	)
}

func nestedMappingKey(address common.Address, parent common.Hash) common.Hash {
	return gethcrypto.Keccak256Hash(
		common.LeftPadBytes(address.Bytes(), 32),
		parent.Bytes(),
	)
}
