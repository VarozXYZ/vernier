package livecanary

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"

	solanaadapter "github.com/VarozXYZ/vernier/adapters/chain/solana"
	"github.com/VarozXYZ/vernier/domain/execution"
)

type EVMRefuelNetwork struct {
	Client  *ethclient.Client
	Address common.Address
}

func (n EVMRefuelNetwork) NativeBalance(
	ctx context.Context,
) (*big.Int, error) {
	if n.Client == nil || n.Address == (common.Address{}) {
		return nil, fmt.Errorf("EVM refuel network is incomplete")
	}
	return n.Client.BalanceAt(ctx, n.Address, nil)
}

func (n EVMRefuelNetwork) AwaitRefuel(
	ctx context.Context,
	identity execution.TransactionIdentity,
	before *big.Int,
) (*big.Int, *big.Int, *big.Int, error) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		settled, failed, after, received, fee, err :=
			n.ReconcileRefuel(ctx, identity, before)
		if err != nil {
			return nil, nil, nil, err
		}
		if failed {
			return nil, nil, fee, fmt.Errorf(
				"EVM refuel transaction reverted",
			)
		}
		if settled {
			return after, received, fee, nil
		}
		select {
		case <-ctx.Done():
			return nil, nil, nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (n EVMRefuelNetwork) ReconcileRefuel(
	ctx context.Context,
	identity execution.TransactionIdentity,
	before *big.Int,
) (bool, bool, *big.Int, *big.Int, *big.Int, error) {
	if n.Client == nil || before == nil || before.Sign() < 0 ||
		!common.IsHexHash(identity.Hash) {
		return false, false, nil, nil, nil,
			fmt.Errorf("EVM refuel reconciliation input is invalid")
	}
	receipt, err := n.Client.TransactionReceipt(
		ctx,
		common.HexToHash(identity.Hash),
	)
	if errors.Is(err, geth.NotFound) {
		return false, false, nil, nil, nil, nil
	}
	if err != nil {
		return false, false, nil, nil, nil, err
	}
	fee := new(big.Int)
	if receipt.EffectiveGasPrice != nil {
		fee.Mul(
			new(big.Int).SetUint64(receipt.GasUsed),
			receipt.EffectiveGasPrice,
		)
	}
	after, err := n.Client.BalanceAt(ctx, n.Address, receipt.BlockNumber)
	if err != nil {
		return false, false, nil, nil, nil, err
	}
	if receipt.Status != types.ReceiptStatusSuccessful {
		return true, true, after, new(big.Int), fee, nil
	}
	received := new(big.Int).Sub(after, before)
	received.Add(received, fee)
	if received.Sign() <= 0 {
		return false, false, nil, nil, nil,
			fmt.Errorf("EVM refuel produced no positive native balance delta")
	}
	return true, false, after, received, fee, nil
}

type SolanaRefuelNetwork struct {
	Network                 *solanaadapter.ReadOnlyNetwork
	Address                 string
	AdditionalDebitLamports uint64
}

func (n SolanaRefuelNetwork) NativeBalance(
	ctx context.Context,
) (*big.Int, error) {
	if n.Network == nil || strings.TrimSpace(n.Address) == "" {
		return nil, fmt.Errorf("solana refuel network is incomplete")
	}
	return n.Network.NativeBalance(ctx, n.Address)
}

func (n SolanaRefuelNetwork) AwaitRefuel(
	ctx context.Context,
	identity execution.TransactionIdentity,
	before *big.Int,
) (*big.Int, *big.Int, *big.Int, error) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		settled, failed, after, received, fee, err :=
			n.ReconcileRefuel(ctx, identity, before)
		if err != nil {
			return nil, nil, nil, err
		}
		if failed {
			return nil, nil, fee, fmt.Errorf(
				"solana refuel transaction reverted",
			)
		}
		if settled {
			return after, received, fee, nil
		}
		select {
		case <-ctx.Done():
			return nil, nil, nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (n SolanaRefuelNetwork) ReconcileRefuel(
	ctx context.Context,
	identity execution.TransactionIdentity,
	before *big.Int,
) (bool, bool, *big.Int, *big.Int, *big.Int, error) {
	if n.Network == nil || before == nil || before.Sign() < 0 ||
		strings.TrimSpace(identity.Hash) == "" {
		return false, false, nil, nil, nil,
			fmt.Errorf("solana refuel reconciliation input is invalid")
	}
	status, err := n.Network.ReadSignatureStatus(ctx, identity.Hash)
	if err != nil {
		return false, false, nil, nil, nil, err
	}
	if !status.Found ||
		status.ConfirmationStatus != "confirmed" &&
			status.ConfirmationStatus != "finalized" {
		return false, false, nil, nil, nil, nil
	}
	if len(status.Err) > 0 && string(status.Err) != "null" {
		return true, true, new(big.Int), new(big.Int), new(big.Int), nil
	}
	transaction, err := n.Network.ReadTransaction(ctx, identity.Hash)
	if err != nil {
		return false, false, nil, nil, nil, err
	}
	var metadata struct {
		Fee uint64          `json:"fee"`
		Err json.RawMessage `json:"err"`
	}
	if err := json.Unmarshal(transaction.Meta, &metadata); err != nil {
		return false, false, nil, nil, nil, err
	}
	if len(metadata.Err) > 0 && string(metadata.Err) != "null" {
		return true, true, new(big.Int), new(big.Int),
			new(big.Int).SetUint64(metadata.Fee), nil
	}
	after, err := n.Network.NativeBalance(ctx, n.Address)
	if err != nil {
		return false, false, nil, nil, nil, err
	}
	fee := new(big.Int).SetUint64(metadata.Fee)
	fee.Add(
		fee,
		new(big.Int).SetUint64(n.AdditionalDebitLamports),
	)
	received := new(big.Int).Sub(after, before)
	received.Add(received, fee)
	if received.Sign() <= 0 {
		return false, false, nil, nil, nil,
			fmt.Errorf("solana refuel produced no positive native balance delta")
	}
	return true, false, after, received, fee, nil
}
