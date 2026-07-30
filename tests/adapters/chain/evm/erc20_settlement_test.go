package evm_test

import (
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	evmadapter "github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
)

func transferLog(token, from, to common.Address, amount int64) *types.Log {
	return &types.Log{
		Address: token,
		Topics: []common.Hash{
			crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)")),
			common.BytesToHash(common.LeftPadBytes(from.Bytes(), 32)),
			common.BytesToHash(common.LeftPadBytes(to.Bytes(), 32)),
		},
		Data: common.LeftPadBytes(big.NewInt(amount).Bytes(), 32),
	}
}

func TestERC20TransferReceiptDecoderUsesNetWalletDeltas(t *testing.T) {
	owner := common.HexToAddress("0x0000000000000000000000000000000000000011")
	router := common.HexToAddress("0x0000000000000000000000000000000000000022")
	inputToken := common.HexToAddress("0x0000000000000000000000000000000000000033")
	outputToken := common.HexToAddress("0x0000000000000000000000000000000000000044")
	decoder, err := evmadapter.NewERC20TransferReceiptDecoder(
		owner,
		map[market.TokenID]string{
			"input": inputToken.Hex(), "output": outputToken.Hex(),
		},
		time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	input, _ := market.NewTokenAmount("input", big.NewInt(1_000_000))
	output, _ := market.NewTokenAmount("output", big.NewInt(4_000_000))
	step := execution.OperationStep{
		Leg: execution.Leg{
			ID: "swap", Side: execution.LegBuy, Chain: "chain-a",
			Account: "wallet", Market: "market", Input: input,
			ExpectedOutput: output,
		},
		Identity: execution.TransactionIdentity{
			Chain: "chain-a", Account: "wallet", Hash: "0x01",
		},
	}
	receipt := &types.Receipt{
		Status:  types.ReceiptStatusSuccessful,
		GasUsed: 250_000, EffectiveGasPrice: big.NewInt(40_000_000_000),
		Logs: []*types.Log{
			transferLog(inputToken, owner, router, 1_000_000),
			transferLog(inputToken, router, owner, 1_000),
			transferLog(outputToken, router, owner, 4_100_000),
			transferLog(outputToken, owner, router, 100_000),
		},
	}
	settlement, err := decoder.DecodeReceipt(step, receipt)
	if err != nil {
		t.Fatal(err)
	}
	if settlement.ActualIn.Units().Cmp(big.NewInt(999_000)) != 0 ||
		settlement.ActualOut.Units().Cmp(big.NewInt(4_000_000)) != 0 {
		t.Fatalf("unexpected settlement in=%s out=%s",
			settlement.ActualIn, settlement.ActualOut)
	}
	if len(settlement.Costs) != 1 ||
		settlement.Costs[0].Amount.Asset() != "evm_native" ||
		settlement.Costs[0].Amount.Rat().Cmp(big.NewRat(1, 100)) != 0 {
		t.Fatalf("unexpected receipt cost: %+v", settlement.Costs)
	}
}

func TestERC20TransferReceiptDecoderRetainsGasCostOnConfirmedRevert(t *testing.T) {
	owner := common.HexToAddress("0x0000000000000000000000000000000000000011")
	inputToken := common.HexToAddress("0x0000000000000000000000000000000000000033")
	outputToken := common.HexToAddress("0x0000000000000000000000000000000000000044")
	decoder, err := evmadapter.NewERC20TransferReceiptDecoder(
		owner,
		map[market.TokenID]string{
			"input": inputToken.Hex(), "output": outputToken.Hex(),
		},
		time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	input, _ := market.NewTokenAmount("input", big.NewInt(1_000_000))
	output, _ := market.NewTokenAmount("output", big.NewInt(4_000_000))
	settlement, err := decoder.DecodeReceipt(
		execution.OperationStep{
			Leg: execution.Leg{
				ID: "swap", Side: execution.LegSell, Chain: "chain-a",
				Account: "wallet", Market: "market", Input: input,
				ExpectedOutput: output,
			},
			Identity: execution.TransactionIdentity{
				Chain: "chain-a", Account: "wallet", Hash: "0x02",
			},
		},
		&types.Receipt{
			Status:  types.ReceiptStatusFailed,
			GasUsed: 250_000, EffectiveGasPrice: big.NewInt(40_000_000_000),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if settlement.Technical != execution.StateConfirmedRevert ||
		settlement.Economic != execution.EconomicReserved ||
		!settlement.ActualIn.IsZero() ||
		!settlement.ActualOut.IsZero() {
		t.Fatalf("unexpected revert settlement: %+v", settlement)
	}
	if len(settlement.Costs) != 1 ||
		settlement.Costs[0].Amount.Rat().Cmp(big.NewRat(1, 100)) != 0 {
		t.Fatalf("unexpected revert costs: %+v", settlement.Costs)
	}
}

func TestERC20TransferReceiptDecoderUsesInboundLogAsInclusionEvidence(t *testing.T) {
	owner := common.HexToAddress("0x0000000000000000000000000000000000000011")
	router := common.HexToAddress("0x0000000000000000000000000000000000000022")
	inputToken := common.HexToAddress("0x0000000000000000000000000000000000000033")
	outputToken := common.HexToAddress("0x0000000000000000000000000000000000000044")
	decoder, err := evmadapter.NewERC20TransferReceiptDecoder(
		owner,
		map[market.TokenID]string{
			"input": inputToken.Hex(), "output": outputToken.Hex(),
		},
		time.Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	filter := decoder.Filter().Query(nil)
	if len(filter.Addresses) != 2 ||
		len(filter.Topics) != 3 ||
		len(filter.Topics[2]) != 1 ||
		common.BytesToAddress(filter.Topics[2][0].Bytes()) != owner {
		t.Fatalf("unexpected inbound transfer filter: %+v", filter)
	}
	input, _ := market.NewTokenAmount("input", big.NewInt(1_000_000))
	output, _ := market.NewTokenAmount("output", big.NewInt(4_000_000))
	step := execution.OperationStep{
		Leg: execution.Leg{
			ID: "swap", Side: execution.LegSell, Chain: "chain-a",
			Account: "wallet", Market: "market",
			Input: input, ExpectedOutput: output,
		},
		Identity: execution.TransactionIdentity{
			Chain: "chain-a", Account: "wallet",
			Hash: common.HexToHash("0x01").Hex(),
		},
	}
	log := transferLog(outputToken, router, owner, 4_100_000)
	log.TxHash = common.HexToHash(step.Identity.Hash)
	settlement, matched, err := decoder.DecodeLog(step, *log)
	if err != nil {
		t.Fatal(err)
	}
	if !matched ||
		settlement.Technical != execution.StateConfirmedSuccess ||
		settlement.Economic != execution.EconomicReserved ||
		settlement.ActualOut.Units().Cmp(big.NewInt(4_100_000)) != 0 ||
		settlement.Evidence != "evm_erc20_transfer_log" {
		t.Fatalf("unexpected websocket settlement: %+v matched=%t", settlement, matched)
	}
}
