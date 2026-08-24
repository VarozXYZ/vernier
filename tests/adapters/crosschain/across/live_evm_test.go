package across_test

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/VarozXYZ/vernier/adapters/crosschain/across"
)

func TestValidateDepositCalldataBindsEconomicFields(t *testing.T) {
	owner := common.HexToAddress("0x1000000000000000000000000000000000000001")
	recipient := common.HexToAddress("0x2000000000000000000000000000000000000002")
	input := common.HexToAddress("0x3000000000000000000000000000000000000003")
	output := common.HexToAddress("0x4000000000000000000000000000000000000004")
	payload := depositV3Payload(t, owner, recipient, input, output, big.NewInt(1_000_000), big.NewInt(995_000), 8453, nil)
	payload = append(payload, 0x1d, 0xc0, 0xde, 0x12, 0x34)
	expected := across.DepositExpectation{Depositor: owner, Recipient: recipient, InputToken: input,
		OutputToken: output, InputAmount: big.NewInt(1_000_000), MinimumOutput: big.NewInt(990_000), DestinationChainID: 8453}
	if err := across.ValidateDepositCalldata(payload, expected); err != nil {
		t.Fatal(err)
	}
	expected.Recipient = owner
	if err := across.ValidateDepositCalldata(payload, expected); err == nil {
		t.Fatal("recipient mismatch was accepted")
	}
}

func TestValidateDepositCalldataRejectsDestinationMessage(t *testing.T) {
	owner := common.HexToAddress("0x1000000000000000000000000000000000000001")
	input := common.HexToAddress("0x3000000000000000000000000000000000000003")
	output := common.HexToAddress("0x4000000000000000000000000000000000000004")
	payload := depositV3Payload(t, owner, owner, input, output, big.NewInt(1), big.NewInt(1), 56, []byte{1})
	err := across.ValidateDepositCalldata(payload, across.DepositExpectation{Depositor: owner, Recipient: owner,
		InputToken: input, OutputToken: output, InputAmount: big.NewInt(1), MinimumOutput: big.NewInt(1), DestinationChainID: 56})
	if err == nil {
		t.Fatal("non-empty destination message was accepted")
	}
}

func depositV3Payload(t *testing.T, depositor, recipient, input, output common.Address,
	inputAmount, outputAmount *big.Int, destination uint64, message []byte) []byte {
	t.Helper()
	typeOf := func(name string) abi.Type {
		value, err := abi.NewType(name, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	arguments := abi.Arguments{
		{Type: typeOf("address")}, {Type: typeOf("address")}, {Type: typeOf("address")}, {Type: typeOf("address")},
		{Type: typeOf("uint256")}, {Type: typeOf("uint256")}, {Type: typeOf("uint256")}, {Type: typeOf("address")},
		{Type: typeOf("uint32")}, {Type: typeOf("uint32")}, {Type: typeOf("uint32")}, {Type: typeOf("bytes")},
	}
	raw, err := arguments.Pack(depositor, recipient, input, output, inputAmount, outputAmount,
		new(big.Int).SetUint64(destination), common.Address{}, uint32(1), uint32(2), uint32(0), message)
	if err != nil {
		t.Fatal(err)
	}
	signature := "depositV3(address,address,address,address,uint256,uint256,uint256,address,uint32,uint32,uint32,bytes)"
	return append(crypto.Keccak256([]byte(signature))[:4], raw...)
}
