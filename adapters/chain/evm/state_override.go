package evm

import (
	"context"
	"fmt"
	"math/big"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	gethcrypto "github.com/ethereum/go-ethereum/crypto"
)

type StateOverrideRPC interface {
	CallContext(context.Context, any, string, ...any) error
}

type ERC20StateOverrideSimulatorConfig struct {
	Client        StateOverrideRPC
	Token         common.Address
	Owner         common.Address
	BalanceSlot   uint64
	AllowanceSlot uint64
}

// ERC20StateOverrideSimulator executes read-only EVM simulations after
// ephemerally granting Owner a maximum token balance and allowance to the
// transaction target. The overrides exist only for the RPC call and can never
// be broadcast or modify chain state.
type ERC20StateOverrideSimulator struct {
	client        StateOverrideRPC
	token         common.Address
	owner         common.Address
	balanceSlot   uint64
	allowanceSlot uint64
}

func NewERC20StateOverrideSimulator(
	config ERC20StateOverrideSimulatorConfig,
) (*ERC20StateOverrideSimulator, error) {
	if config.Client == nil || config.Token == (common.Address{}) ||
		config.Owner == (common.Address{}) {
		return nil, fmt.Errorf("ERC-20 state override simulator configuration is incomplete")
	}
	if config.BalanceSlot == config.AllowanceSlot {
		return nil, fmt.Errorf(
			"ERC-20 balance and allowance storage slots must be distinct",
		)
	}
	return &ERC20StateOverrideSimulator{
		client: config.Client, token: config.Token, owner: config.Owner,
		balanceSlot: config.BalanceSlot, allowanceSlot: config.AllowanceSlot,
	}, nil
}

func (s *ERC20StateOverrideSimulator) CallContract(
	ctx context.Context,
	call geth.CallMsg,
	blockNumber *big.Int,
) ([]byte, error) {
	args, overrides, err := s.arguments(call)
	if err != nil {
		return nil, err
	}
	var result hexutil.Bytes
	if err := s.client.CallContext(
		ctx,
		&result,
		"eth_call",
		args,
		blockArgument(blockNumber),
		overrides,
	); err != nil {
		return nil, err
	}
	return append([]byte(nil), result...), nil
}

func (s *ERC20StateOverrideSimulator) EstimateGas(
	ctx context.Context,
	call geth.CallMsg,
) (uint64, error) {
	args, overrides, err := s.arguments(call)
	if err != nil {
		return 0, err
	}
	var result hexutil.Uint64
	if err := s.client.CallContext(
		ctx,
		&result,
		"eth_estimateGas",
		args,
		"latest",
		overrides,
	); err != nil {
		return 0, err
	}
	if result == 0 {
		return 0, fmt.Errorf("state override gas estimate is zero")
	}
	return uint64(result), nil
}

func (s *ERC20StateOverrideSimulator) arguments(
	call geth.CallMsg,
) (rpcCallArguments, map[common.Address]stateOverrideAccount, error) {
	if call.To == nil || *call.To == (common.Address{}) {
		return rpcCallArguments{}, nil,
			fmt.Errorf("state override simulation requires a transaction target")
	}
	if call.From != s.owner {
		return rpcCallArguments{}, nil,
			fmt.Errorf("state override simulation sender does not match configured owner")
	}
	balanceKey := mappingStorageKey(s.owner, s.balanceSlot)
	allowanceOwnerKey := mappingStorageKey(s.owner, s.allowanceSlot)
	allowanceKey := nestedMappingStorageKey(*call.To, allowanceOwnerKey)
	maximum := common.HexToHash(
		"0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
	)
	return rpcArguments(call), map[common.Address]stateOverrideAccount{
		s.token: {
			StateDiff: map[common.Hash]common.Hash{
				balanceKey:   maximum,
				allowanceKey: maximum,
			},
		},
	}, nil
}

type rpcCallArguments struct {
	From                 common.Address   `json:"from"`
	To                   *common.Address  `json:"to,omitempty"`
	Gas                  hexutil.Uint64   `json:"gas,omitempty"`
	GasPrice             *hexutil.Big     `json:"gasPrice,omitempty"`
	MaxFeePerGas         *hexutil.Big     `json:"maxFeePerGas,omitempty"`
	MaxPriorityFeePerGas *hexutil.Big     `json:"maxPriorityFeePerGas,omitempty"`
	Value                *hexutil.Big     `json:"value,omitempty"`
	Data                 hexutil.Bytes    `json:"data,omitempty"`
	AccessList           types.AccessList `json:"accessList,omitempty"`
}

type stateOverrideAccount struct {
	StateDiff map[common.Hash]common.Hash `json:"stateDiff"`
}

func rpcArguments(call geth.CallMsg) rpcCallArguments {
	return rpcCallArguments{
		From: call.From, To: call.To, Gas: hexutil.Uint64(call.Gas),
		GasPrice:             bigToHex(call.GasPrice),
		MaxFeePerGas:         bigToHex(call.GasFeeCap),
		MaxPriorityFeePerGas: bigToHex(call.GasTipCap),
		Value:                bigToHex(call.Value), Data: append(hexutil.Bytes(nil), call.Data...),
		AccessList: append(types.AccessList(nil), call.AccessList...),
	}
}

func bigToHex(value *big.Int) *hexutil.Big {
	if value == nil {
		return nil
	}
	copyOf := new(big.Int).Set(value)
	return (*hexutil.Big)(copyOf)
}

func blockArgument(number *big.Int) string {
	if number == nil {
		return "latest"
	}
	return hexutil.EncodeBig(number)
}

func mappingStorageKey(key common.Address, slot uint64) common.Hash {
	slotWord := common.LeftPadBytes(new(big.Int).SetUint64(slot).Bytes(), 32)
	return gethcrypto.Keccak256Hash(
		common.LeftPadBytes(key.Bytes(), 32),
		slotWord,
	)
}

func nestedMappingStorageKey(
	key common.Address,
	parent common.Hash,
) common.Hash {
	return gethcrypto.Keccak256Hash(
		common.LeftPadBytes(key.Bytes(), 32),
		parent.Bytes(),
	)
}

var _ interface {
	CallContract(context.Context, geth.CallMsg, *big.Int) ([]byte, error)
	EstimateGas(context.Context, geth.CallMsg) (uint64, error)
} = (*ERC20StateOverrideSimulator)(nil)
