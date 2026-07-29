package wormholentt

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	crosschainport "github.com/VarozXYZ/vernier/ports/crosschain"
)

const (
	manualTransceiverInstructionsHex = "01000101"
	nttManagerABIJSON                = `[
		{"type":"function","name":"quoteDeliveryPrice","stateMutability":"view","inputs":[
			{"name":"recipientChain","type":"uint16"},
			{"name":"transceiverInstructions","type":"bytes"}
		],"outputs":[{"name":"quotes","type":"uint256[]"},{"name":"totalPrice","type":"uint256"}]},
		{"type":"function","name":"transfer","stateMutability":"payable","inputs":[
			{"name":"amount","type":"uint256"},
			{"name":"recipientChain","type":"uint16"},
			{"name":"recipient","type":"bytes32"},
			{"name":"refundAddress","type":"bytes32"},
			{"name":"shouldQueue","type":"bool"},
			{"name":"encodedInstructions","type":"bytes"}
		],"outputs":[{"name":"msgId","type":"uint64"}]}
	]`
	wormholeTransceiverABIJSON = `[
		{"type":"function","name":"receiveMessage","stateMutability":"nonpayable",
		 "inputs":[{"name":"encodedMessage","type":"bytes"}],"outputs":[]}
	]`
	erc20ABIJSON = `[
		{"type":"function","name":"balanceOf","stateMutability":"view",
		 "inputs":[{"name":"account","type":"address"}],
		 "outputs":[{"name":"","type":"uint256"}]},
		{"type":"function","name":"allowance","stateMutability":"view",
		 "inputs":[{"name":"owner","type":"address"},{"name":"spender","type":"address"}],
		 "outputs":[{"name":"","type":"uint256"}]},
		{"type":"function","name":"approve","stateMutability":"nonpayable",
		 "inputs":[{"name":"spender","type":"address"},{"name":"amount","type":"uint256"}],
		 "outputs":[{"name":"","type":"bool"}]}
	]`
	wormholeCoreEventsABIJSON = `[
		{"anonymous":false,"type":"event","name":"LogMessagePublished","inputs":[
			{"indexed":true,"name":"sender","type":"address"},
			{"indexed":false,"name":"sequence","type":"uint64"},
			{"indexed":false,"name":"nonce","type":"uint32"},
			{"indexed":false,"name":"payload","type":"bytes"},
			{"indexed":false,"name":"consistencyLevel","type":"uint8"}
		]}
	]`
)

var (
	nttManagerABI          = mustABI(nttManagerABIJSON)
	wormholeTransceiverABI = mustABI(wormholeTransceiverABIJSON)
	erc20ABI               = mustABI(erc20ABIJSON)
	wormholeCoreEventsABI  = mustABI(wormholeCoreEventsABIJSON)
	manualInstructions, _  = hex.DecodeString(manualTransceiverInstructionsHex)
)

type EVMConfig struct {
	ChainID       *big.Int
	WormholeChain uint16
	Token         common.Address
	Manager       common.Address
	Transceiver   common.Address
	WormholeCore  common.Address
	TransferGas   uint64
	RedeemGas     uint64
	ApprovalGas   uint64
}

type EVMCall struct {
	To       common.Address
	Value    *big.Int
	Data     []byte
	GasLimit uint64
	Kind     string
}

type EVMAdapter struct {
	config EVMConfig
}

func NewEVMAdapter(config EVMConfig) (*EVMAdapter, error) {
	if config.ChainID == nil || config.ChainID.Sign() <= 0 || config.WormholeChain == 0 ||
		zeroAddress(config.Token) || zeroAddress(config.Manager) ||
		zeroAddress(config.Transceiver) || zeroAddress(config.WormholeCore) {
		return nil, fmt.Errorf("EVM NTT profile requires chain IDs and non-zero token, manager, transceiver, and core addresses")
	}
	if config.TransferGas == 0 {
		config.TransferGas = 1_000_000
	}
	if config.RedeemGas == 0 {
		config.RedeemGas = 1_000_000
	}
	if config.ApprovalGas == 0 {
		config.ApprovalGas = 100_000
	}
	config.ChainID = new(big.Int).Set(config.ChainID)
	return &EVMAdapter{config: config}, nil
}

func (a *EVMAdapter) ChainID() *big.Int {
	return new(big.Int).Set(a.config.ChainID)
}

func (a *EVMAdapter) WormholeChainID() uint16 { return a.config.WormholeChain }

func (a *EVMAdapter) Token() common.Address       { return a.config.Token }
func (a *EVMAdapter) Manager() common.Address     { return a.config.Manager }
func (a *EVMAdapter) Transceiver() common.Address { return a.config.Transceiver }

func (a *EVMAdapter) BuildApproval(amount *big.Int) (EVMCall, error) {
	if amount == nil || amount.Sign() <= 0 {
		return EVMCall{}, fmt.Errorf("approval amount must be positive")
	}
	data, err := erc20ABI.Pack("approve", a.config.Manager, amount)
	if err != nil {
		return EVMCall{}, err
	}
	return EVMCall{
		To: a.config.Token, Value: new(big.Int), Data: data,
		GasLimit: a.config.ApprovalGas, Kind: "ntt_approval",
	}, nil
}

func (a *EVMAdapter) BuildAllowanceCall(owner common.Address) (EVMCall, error) {
	if zeroAddress(owner) {
		return EVMCall{}, fmt.Errorf("allowance owner is required")
	}
	data, err := erc20ABI.Pack("allowance", owner, a.config.Manager)
	if err != nil {
		return EVMCall{}, fmt.Errorf("encode NTT allowance query: %w", err)
	}
	return EVMCall{
		To: a.config.Token, Value: new(big.Int), Data: data,
		Kind: "ntt_allowance",
	}, nil
}

func (a *EVMAdapter) BuildBalanceCall(owner common.Address) (EVMCall, error) {
	if zeroAddress(owner) {
		return EVMCall{}, fmt.Errorf("balance owner is required")
	}
	data, err := erc20ABI.Pack("balanceOf", owner)
	if err != nil {
		return EVMCall{}, fmt.Errorf("encode NTT token balance query: %w", err)
	}
	return EVMCall{
		To: a.config.Token, Value: new(big.Int), Data: data,
		Kind: "ntt_token_balance",
	}, nil
}

func DecodeTokenBalance(output []byte) (*big.Int, error) {
	values, err := erc20ABI.Unpack("balanceOf", output)
	if err != nil || len(values) != 1 {
		return nil, fmt.Errorf("decode NTT token balance result")
	}
	balance, ok := values[0].(*big.Int)
	if !ok || balance.Sign() < 0 {
		return nil, fmt.Errorf("NTT token balance result has an unexpected type")
	}
	return new(big.Int).Set(balance), nil
}

func DecodeAllowance(output []byte) (*big.Int, error) {
	values, err := erc20ABI.Unpack("allowance", output)
	if err != nil || len(values) != 1 {
		return nil, fmt.Errorf("decode NTT allowance result")
	}
	allowance, ok := values[0].(*big.Int)
	if !ok || allowance.Sign() < 0 {
		return nil, fmt.Errorf("NTT allowance result has an unexpected type")
	}
	return new(big.Int).Set(allowance), nil
}

func (a *EVMAdapter) BuildDeliveryPriceCall(destinationChain uint16) (EVMCall, error) {
	if destinationChain == 0 {
		return EVMCall{}, fmt.Errorf("destination Wormhole chain is required")
	}
	data, err := nttManagerABI.Pack(
		"quoteDeliveryPrice",
		destinationChain,
		append([]byte(nil), manualInstructions...),
	)
	if err != nil {
		return EVMCall{}, fmt.Errorf("encode NTT delivery-price query: %w", err)
	}
	return EVMCall{
		To: a.config.Manager, Value: new(big.Int), Data: data,
		Kind: "ntt_quote_delivery_price",
	}, nil
}

func DecodeDeliveryPrice(output []byte) (*big.Int, error) {
	values, err := nttManagerABI.Unpack("quoteDeliveryPrice", output)
	if err != nil || len(values) != 2 {
		return nil, fmt.Errorf("decode NTT delivery-price result")
	}
	total, ok := values[1].(*big.Int)
	if !ok || total.Sign() < 0 {
		return nil, fmt.Errorf("NTT delivery-price result has an unexpected type")
	}
	return new(big.Int).Set(total), nil
}

func (a *EVMAdapter) BuildTransfer(
	amount *big.Int,
	destinationChain uint16,
	recipient [32]byte,
	refundAddress [32]byte,
	messageValue *big.Int,
) (EVMCall, error) {
	if amount == nil || amount.Sign() <= 0 || destinationChain == 0 ||
		recipient == ([32]byte{}) || refundAddress == ([32]byte{}) {
		return EVMCall{}, fmt.Errorf("manual EVM NTT transfer requires amount, destination, recipient, and refund address")
	}
	if messageValue == nil {
		messageValue = new(big.Int)
	}
	if messageValue.Sign() < 0 {
		return EVMCall{}, fmt.Errorf("manual EVM NTT transfer message value cannot be negative")
	}
	data, err := nttManagerABI.Pack(
		"transfer",
		amount,
		destinationChain,
		recipient,
		refundAddress,
		false,
		append([]byte(nil), manualInstructions...),
	)
	if err != nil {
		return EVMCall{}, fmt.Errorf("encode manual EVM NTT transfer: %w", err)
	}
	return EVMCall{
		To: a.config.Manager, Value: new(big.Int).Set(messageValue), Data: data,
		GasLimit: a.config.TransferGas, Kind: "ntt_transfer_manual",
	}, nil
}

func (a *EVMAdapter) BuildRedeem(rawVAA []byte) (EVMCall, error) {
	parsed, err := ParseVAA(rawVAA)
	if err != nil {
		return EVMCall{}, err
	}
	message, err := parsed.NTTMessage()
	if err != nil {
		return EVMCall{}, err
	}
	if message.DestinationChain != a.config.WormholeChain {
		return EVMCall{}, fmt.Errorf(
			"VAA targets Wormhole chain %d, adapter is chain %d",
			message.DestinationChain, a.config.WormholeChain,
		)
	}
	if !bytes.Equal(message.DestinationManager[12:], a.config.Manager.Bytes()) ||
		!allZero(message.DestinationManager[:12]) {
		return EVMCall{}, fmt.Errorf("VAA targets another destination NTT manager")
	}
	data, err := wormholeTransceiverABI.Pack("receiveMessage", rawVAA)
	if err != nil {
		return EVMCall{}, fmt.Errorf("encode EVM NTT redemption: %w", err)
	}
	return EVMCall{
		To: a.config.Transceiver, Value: new(big.Int), Data: data,
		GasLimit: a.config.RedeemGas, Kind: "ntt_receive_message",
	}, nil
}

func (a *EVMAdapter) MessageFromReceipt(receipt *types.Receipt) (crosschainport.MessageID, error) {
	if receipt == nil || receipt.Status != types.ReceiptStatusSuccessful {
		return crosschainport.MessageID{}, fmt.Errorf("successful EVM source receipt is required")
	}
	event := wormholeCoreEventsABI.Events["LogMessagePublished"]
	for _, observed := range receipt.Logs {
		if observed == nil || observed.Address != a.config.WormholeCore ||
			len(observed.Topics) != 2 || observed.Topics[0] != event.ID {
			continue
		}
		sender := common.BytesToAddress(observed.Topics[1].Bytes()[12:])
		if sender != a.config.Transceiver {
			continue
		}
		values, err := event.Inputs.NonIndexed().Unpack(observed.Data)
		if err != nil || len(values) != 4 {
			return crosschainport.MessageID{}, fmt.Errorf("decode Wormhole LogMessagePublished event")
		}
		sequence, ok := values[0].(uint64)
		if !ok {
			return crosschainport.MessageID{}, fmt.Errorf("wormhole sequence has an unexpected type")
		}
		var emitter [32]byte
		copy(emitter[12:], a.config.Transceiver.Bytes())
		return crosschainport.MessageID{
			EmitterChain: a.config.WormholeChain, EmitterAddress: emitter, Sequence: sequence,
		}, nil
	}
	return crosschainport.MessageID{}, fmt.Errorf("receipt contains no matching Wormhole message")
}

func EVMUniversalAddress(address common.Address) [32]byte {
	var result [32]byte
	copy(result[12:], address.Bytes())
	return result
}

func DecodeEVMUniversalAddress(address [32]byte) (common.Address, error) {
	if !allZero(address[:12]) {
		return common.Address{}, fmt.Errorf("universal address is not an EVM address")
	}
	result := common.BytesToAddress(address[12:])
	if zeroAddress(result) {
		return common.Address{}, fmt.Errorf("universal EVM address cannot be zero")
	}
	return result, nil
}

func ManualTransceiverInstructions() []byte {
	return append([]byte(nil), manualInstructions...)
}

func WormholeMessageEventTopic() common.Hash {
	return crypto.Keccak256Hash([]byte("LogMessagePublished(address,uint64,uint32,bytes,uint8)"))
}

func mustABI(definition string) abi.ABI {
	parsed, err := abi.JSON(strings.NewReader(definition))
	if err != nil {
		panic(err)
	}
	return parsed
}

func zeroAddress(address common.Address) bool { return address == (common.Address{}) }

func allZero(value []byte) bool {
	for _, candidate := range value {
		if candidate != 0 {
			return false
		}
	}
	return true
}
