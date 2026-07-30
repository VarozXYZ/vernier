package evm

import (
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
)

var erc20TransferTopic = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))

// ERC20TransferReceiptDecoder derives wallet settlement from the receipt's
// token Transfer logs. This avoids post-confirmation balance RPC calls and
// captures fee-on-transfer behavior visible at the wallet boundary.
type ERC20TransferReceiptDecoder struct {
	owner       common.Address
	tokens      map[market.TokenID]common.Address
	nativeAsset market.AssetID
	clock       func() time.Time
}

func NewERC20TransferReceiptDecoder(
	owner common.Address,
	tokens map[market.TokenID]string,
	clock func() time.Time,
	nativeAsset ...market.AssetID,
) (*ERC20TransferReceiptDecoder, error) {
	if owner == (common.Address{}) || len(tokens) < 2 {
		return nil, fmt.Errorf("ERC-20 settlement decoder requires owner and token mappings")
	}
	if clock == nil {
		clock = time.Now
	}
	resolved := make(map[market.TokenID]common.Address, len(tokens))
	for token, raw := range tokens {
		if token == "" || !common.IsHexAddress(raw) {
			return nil, fmt.Errorf("ERC-20 settlement token mapping is invalid")
		}
		address := common.HexToAddress(raw)
		if address == (common.Address{}) {
			return nil, fmt.Errorf("ERC-20 settlement token mapping contains zero address")
		}
		resolved[token] = address
	}
	asset := market.AssetID("evm_native")
	if len(nativeAsset) > 0 && nativeAsset[0] != "" {
		asset = nativeAsset[0]
	}
	return &ERC20TransferReceiptDecoder{
		owner: owner, tokens: resolved, nativeAsset: asset, clock: clock,
	}, nil
}

func (d *ERC20TransferReceiptDecoder) DecodeReceipt(
	step execution.OperationStep,
	receipt *types.Receipt,
) (execution.Settlement, error) {
	if receipt == nil {
		return execution.Settlement{}, fmt.Errorf("EVM settlement receipt is required")
	}
	costs, err := d.receiptCosts(step.Leg.Chain, receipt)
	if err != nil {
		return execution.Settlement{}, err
	}
	if receipt.Status != types.ReceiptStatusSuccessful {
		return execution.Settlement{
			Identity: step.Identity, Technical: execution.StateConfirmedRevert,
			Economic: execution.EconomicReserved, Costs: costs,
			ObservedAt: d.clock().UTC(), Evidence: "evm_receipt_revert",
		}, nil
	}
	inputAddress, inputOK := d.tokens[step.Leg.Input.Token()]
	outputAddress, outputOK := d.tokens[step.Leg.ExpectedOutput.Token()]
	if !inputOK || !outputOK {
		return execution.Settlement{}, fmt.Errorf("EVM settlement token mapping is missing")
	}
	inputNet := new(big.Int)
	outputNet := new(big.Int)
	for _, observed := range receipt.Logs {
		if observed == nil || len(observed.Topics) != 3 ||
			observed.Topics[0] != erc20TransferTopic || len(observed.Data) != 32 {
			continue
		}
		token := observed.Address
		if token != inputAddress && token != outputAddress {
			continue
		}
		from := common.BytesToAddress(observed.Topics[1].Bytes()[12:])
		to := common.BytesToAddress(observed.Topics[2].Bytes()[12:])
		value := new(big.Int).SetBytes(observed.Data)
		if value.Sign() <= 0 || from == to {
			continue
		}
		target := inputNet
		if token == outputAddress {
			target = outputNet
		}
		if strings.EqualFold(from.Hex(), d.owner.Hex()) {
			target.Sub(target, value)
		}
		if strings.EqualFold(to.Hex(), d.owner.Hex()) {
			target.Add(target, value)
		}
	}
	actualInputUnits := new(big.Int).Neg(inputNet)
	actualOutputUnits := new(big.Int).Set(outputNet)
	if actualInputUnits.Sign() <= 0 || actualOutputUnits.Sign() <= 0 {
		return execution.Settlement{}, fmt.Errorf(
			"EVM receipt does not prove positive wallet input and output deltas",
		)
	}
	actualInput, err := market.NewTokenAmount(step.Leg.Input.Token(), actualInputUnits)
	if err != nil {
		return execution.Settlement{}, err
	}
	actualOutput, err := market.NewTokenAmount(step.Leg.ExpectedOutput.Token(), actualOutputUnits)
	if err != nil {
		return execution.Settlement{}, err
	}
	return execution.Settlement{
		Identity: step.Identity, Technical: execution.StateConfirmedSuccess,
		Economic: execution.EconomicEffectVerified,
		ActualIn: actualInput, ActualOut: actualOutput,
		Costs:      costs,
		ObservedAt: d.clock().UTC(), Evidence: "evm_erc20_transfer_receipt",
	}, nil
}

func (d *ERC20TransferReceiptDecoder) receiptCosts(
	chain market.ChainID,
	receipt *types.Receipt,
) ([]execution.CostComponent, error) {
	costs := make([]execution.CostComponent, 0, 1)
	if receipt.GasUsed > 0 && receipt.EffectiveGasPrice != nil &&
		receipt.EffectiveGasPrice.Sign() > 0 {
		wei := new(big.Int).Mul(
			new(big.Int).SetUint64(receipt.GasUsed),
			receipt.EffectiveGasPrice,
		)
		amount, amountErr := market.NewAssetQuantity(
			d.nativeAsset,
			new(big.Rat).SetFrac(
				wei,
				new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil),
			),
		)
		if amountErr != nil {
			return nil, amountErr
		}
		costs = append(costs, execution.CostComponent{
			Kind: "network_fee", Chain: chain, Amount: amount,
			Evidence: "evm_receipt_gas",
		})
	}
	return costs, nil
}

var _ ReceiptSettlementDecoder = (*ERC20TransferReceiptDecoder)(nil)
