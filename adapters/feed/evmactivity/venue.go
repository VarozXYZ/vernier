// Package evmactivity provides trigger-only EVM pool observation for remote
// aggregator markets. It recognizes economic Uniswap V3 state changes but
// carries no pool state into Research.
package evmactivity

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/adapters/feed/evmlogs"
	"github.com/VarozXYZ/vernier/domain/market"
)

var economicTopics = []common.Hash{
	crypto.Keccak256Hash([]byte("Swap(address,address,int256,int256,uint160,uint128,int24)")),
	crypto.Keccak256Hash([]byte("Mint(address,address,int24,int24,uint128,uint256,uint256)")),
	crypto.Keccak256Hash([]byte("Burn(address,int24,int24,uint128,uint256,uint256)")),
}

type Venue struct {
	id      string
	address common.Address
}

type TriggerEvent struct {
	Transaction common.Hash
}

func (TriggerEvent) EventKind() string { return "evm_pool_economic_activity/v1" }

func (e TriggerEvent) EventReference() market.SourceReference {
	if e.Transaction == (common.Hash{}) {
		return market.SourceReference{Kind: evmlogs.BlockHashReferenceKind, Value: "bootstrap"}
	}
	return market.SourceReference{Kind: evmlogs.TransactionReferenceKind, Value: e.Transaction.Hex()}
}

func NewVenue(id, address string) (*Venue, error) {
	if id == "" || !common.IsHexAddress(address) {
		return nil, fmt.Errorf("EVM activity venue requires id and pool address")
	}
	return &Venue{id: id, address: common.HexToAddress(address)}, nil
}

func (v *Venue) ID() string { return v.id }
func (v *Venue) Filter() evm.LogFilter {
	return evm.LogFilter{Address: v.address, Topics: append([]common.Hash(nil), economicTopics...)}
}
func (*Venue) Bootstrap(context.Context, evm.Network, evm.BlockReference) (market.EventData, error) {
	return TriggerEvent{}, nil
}
func (*Venue) DecodeBlock(_ context.Context, _ evm.Network, _ evm.BlockReference, logs []types.Log) (market.EventData, error) {
	if len(logs) == 0 {
		return nil, fmt.Errorf("economic activity block contains no matching logs")
	}
	return TriggerEvent{Transaction: logs[len(logs)-1].TxHash}, nil
}
func (*Venue) DecodeLog(_ context.Context, _ evm.Network, _ evm.BlockReference, log types.Log) (market.EventData, error) {
	if len(log.Topics) == 0 || !isEconomic(log.Topics[0]) {
		return nil, fmt.Errorf("unsupported EVM pool event")
	}
	return TriggerEvent{Transaction: log.TxHash}, nil
}

func isEconomic(topic common.Hash) bool {
	for _, candidate := range economicTopics {
		if topic == candidate {
			return true
		}
	}
	return false
}

var _ evmlogs.Venue = (*Venue)(nil)
var _ evmlogs.LogDecoder = (*Venue)(nil)
