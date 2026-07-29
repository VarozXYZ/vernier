package across_test

import (
	"encoding/hex"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/VarozXYZ/vernier/adapters/crosschain/across"
)

func TestVerifyDirectCCTPBindsEveryEconomicField(t *testing.T) {
	messenger := common.HexToAddress("0x1111111111111111111111111111111111111111")
	token := common.HexToAddress("0x2222222222222222222222222222222222222222")
	var recipient [32]byte
	copy(recipient[:], common.LeftPadBytes([]byte("synthetic-recipient"), 32))
	var caller [32]byte
	copy(caller[:], common.LeftPadBytes([]byte("synthetic-caller"), 32))
	data := encodeDepositForBurn(t, big.NewInt(1_000_000), 5, recipient, token, caller, big.NewInt(0), 2000)

	deposit, err := across.VerifyDirectCCTP(across.Transaction{
		To: messenger.Hex(), Data: "0x" + hex.EncodeToString(data), ChainID: 137,
	}, across.DirectCCTPExpectation{
		TokenMessenger: messenger, BurnToken: token, Amount: big.NewInt(1_000_000),
		DestinationDomain: 5, MintRecipient: recipient, DestinationCaller: caller,
		MinimumFinality: 2000, MaximumFee: big.NewInt(0),
	})
	if err != nil {
		t.Fatal(err)
	}
	if deposit.Amount.Cmp(big.NewInt(1_000_000)) != 0 || deposit.MinimumFinality != 2000 {
		t.Fatalf("unexpected decoded deposit: %+v", deposit)
	}
}

func TestVerifyDirectCCTPRejectsRecipientOrAmountSubstitution(t *testing.T) {
	messenger := common.HexToAddress("0x1111111111111111111111111111111111111111")
	token := common.HexToAddress("0x2222222222222222222222222222222222222222")
	var recipient [32]byte
	recipient[31] = 1
	var attacker [32]byte
	attacker[31] = 2
	data := encodeDepositForBurn(t, big.NewInt(2_000_000), 5, attacker, token, [32]byte{}, big.NewInt(0), 2000)

	_, err := across.VerifyDirectCCTP(across.Transaction{
		To: messenger.Hex(), Data: "0x" + hex.EncodeToString(data), ChainID: 137,
	}, across.DirectCCTPExpectation{
		TokenMessenger: messenger, BurnToken: token, Amount: big.NewInt(1_000_000),
		DestinationDomain: 5, MintRecipient: recipient, MinimumFinality: 2000,
	})
	if err == nil {
		t.Fatal("expected substituted transfer intent to be rejected")
	}
}

func encodeDepositForBurn(
	t *testing.T,
	amount *big.Int,
	domain uint32,
	recipient [32]byte,
	token common.Address,
	caller [32]byte,
	maximumFee *big.Int,
	finality uint32,
) []byte {
	t.Helper()
	contractABI, err := abi.JSON(strings.NewReader(`[{
	  "type":"function","name":"depositForBurn","stateMutability":"nonpayable",
	  "inputs":[
	    {"name":"amount","type":"uint256"},{"name":"destinationDomain","type":"uint32"},
	    {"name":"mintRecipient","type":"bytes32"},{"name":"burnToken","type":"address"},
	    {"name":"destinationCaller","type":"bytes32"},{"name":"maxFee","type":"uint256"},
	    {"name":"minFinalityThreshold","type":"uint32"}
	  ],"outputs":[{"name":"nonce","type":"uint64"}]
	}]`))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := contractABI.Pack(
		"depositForBurn", amount, domain, recipient, token, caller, maximumFee, finality,
	)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
