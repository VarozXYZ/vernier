package across

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

const DepositForBurnSelector = "8e0250ee"

const tokenMessengerV2ABI = `[{
  "type":"function",
  "name":"depositForBurn",
  "stateMutability":"nonpayable",
  "inputs":[
    {"name":"amount","type":"uint256"},
    {"name":"destinationDomain","type":"uint32"},
    {"name":"mintRecipient","type":"bytes32"},
    {"name":"burnToken","type":"address"},
    {"name":"destinationCaller","type":"bytes32"},
    {"name":"maxFee","type":"uint256"},
    {"name":"minFinalityThreshold","type":"uint32"}
  ],
  "outputs":[{"name":"nonce","type":"uint64"}]
}]`

type DirectCCTPExpectation struct {
	TokenMessenger    common.Address
	BurnToken         common.Address
	Amount            *big.Int
	DestinationDomain uint32
	MintRecipient     [32]byte
	DestinationCaller [32]byte
	MinimumFinality   uint32
	MaximumFee        *big.Int
}

type DirectCCTPDeposit struct {
	TokenMessenger    common.Address
	BurnToken         common.Address
	Amount            *big.Int
	DestinationDomain uint32
	MintRecipient     [32]byte
	DestinationCaller [32]byte
	MaximumFee        *big.Int
	MinimumFinality   uint32
}

func VerifyDirectCCTP(transaction Transaction, expected DirectCCTPExpectation) (DirectCCTPDeposit, error) {
	if expected.TokenMessenger == (common.Address{}) || expected.BurnToken == (common.Address{}) ||
		expected.Amount == nil || expected.Amount.Sign() <= 0 {
		return DirectCCTPDeposit{}, fmt.Errorf("direct CCTP expectation is incomplete")
	}
	if !isHexAddress(transaction.To) || common.HexToAddress(transaction.To) != expected.TokenMessenger {
		return DirectCCTPDeposit{}, fmt.Errorf("across artifact targets an unexpected CCTP messenger")
	}
	payload, err := hex.DecodeString(strings.TrimPrefix(transaction.Data, "0x"))
	if err != nil || len(payload) < 4 || hex.EncodeToString(payload[:4]) != DepositForBurnSelector {
		return DirectCCTPDeposit{}, fmt.Errorf("across artifact is not TokenMessengerV2 depositForBurn")
	}
	contractABI, err := abi.JSON(strings.NewReader(tokenMessengerV2ABI))
	if err != nil {
		return DirectCCTPDeposit{}, err
	}
	method := contractABI.Methods["depositForBurn"]
	values, err := method.Inputs.Unpack(payload[4:])
	if err != nil || len(values) != 7 {
		return DirectCCTPDeposit{}, fmt.Errorf("decode Across CCTP depositForBurn: %w", err)
	}
	deposit, err := cctpDepositFromValues(common.HexToAddress(transaction.To), values)
	if err != nil {
		return DirectCCTPDeposit{}, err
	}
	if deposit.Amount.Cmp(expected.Amount) != 0 ||
		deposit.DestinationDomain != expected.DestinationDomain ||
		deposit.BurnToken != expected.BurnToken ||
		!bytes.Equal(deposit.MintRecipient[:], expected.MintRecipient[:]) ||
		!bytes.Equal(deposit.DestinationCaller[:], expected.DestinationCaller[:]) {
		return DirectCCTPDeposit{}, fmt.Errorf("across CCTP artifact does not match the fixed transfer intent")
	}
	if expected.MinimumFinality != 0 && deposit.MinimumFinality != expected.MinimumFinality {
		return DirectCCTPDeposit{}, fmt.Errorf("across CCTP artifact uses unexpected finality threshold")
	}
	if expected.MaximumFee != nil && deposit.MaximumFee.Cmp(expected.MaximumFee) > 0 {
		return DirectCCTPDeposit{}, fmt.Errorf("across CCTP artifact exceeds maximum fee")
	}
	return deposit, nil
}

func cctpDepositFromValues(messenger common.Address, values []any) (DirectCCTPDeposit, error) {
	amount, amountOK := values[0].(*big.Int)
	domain, domainOK := values[1].(uint32)
	recipient, recipientOK := values[2].([32]byte)
	token, tokenOK := values[3].(common.Address)
	caller, callerOK := values[4].([32]byte)
	maximumFee, feeOK := values[5].(*big.Int)
	finality, finalityOK := values[6].(uint32)
	if !amountOK || !domainOK || !recipientOK || !tokenOK || !callerOK ||
		!feeOK || !finalityOK {
		return DirectCCTPDeposit{}, fmt.Errorf("across CCTP depositForBurn has unexpected ABI values")
	}
	return DirectCCTPDeposit{
		TokenMessenger: messenger, BurnToken: token, Amount: new(big.Int).Set(amount),
		DestinationDomain: domain, MintRecipient: recipient, DestinationCaller: caller,
		MaximumFee: new(big.Int).Set(maximumFee), MinimumFinality: finality,
	}, nil
}
