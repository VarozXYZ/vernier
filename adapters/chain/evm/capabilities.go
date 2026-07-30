// Package evm defines the small EVM capabilities shared by compatible chain,
// feed, and market adapters. It is not a selectable chain implementation.
package evm

import (
	"context"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type BlockReference struct {
	Number uint64
	Hash   common.Hash
}

type LogFilter struct {
	Address       common.Address
	Addresses     []common.Address
	Topics        []common.Hash
	IndexedTopics [][]common.Hash
}

func (f LogFilter) Query(blockHash *common.Hash) geth.FilterQuery {
	addresses := append([]common.Address(nil), f.Addresses...)
	if len(addresses) == 0 && f.Address != (common.Address{}) {
		addresses = []common.Address{f.Address}
	}
	query := geth.FilterQuery{Addresses: addresses}
	if len(f.Topics) > 0 {
		query.Topics = [][]common.Hash{append([]common.Hash(nil), f.Topics...)}
	}
	for _, topics := range f.IndexedTopics {
		query.Topics = append(query.Topics, append([]common.Hash(nil), topics...))
	}
	query.BlockHash = blockHash
	return query
}

type Subscription interface {
	Err() <-chan error
	Unsubscribe()
}

// Network is the EVM capability set currently required by filtered log feeds
// and read-only market adapters. Domain and strategy packages never depend on
// this interface.
type Network interface {
	ID() string
	CurrentBlock(context.Context) (BlockReference, error)
	SubscribeLogs(context.Context, LogFilter, chan<- types.Log) (Subscription, error)
	LogsAt(context.Context, BlockReference, LogFilter) ([]types.Log, error)
	CallContract(context.Context, BlockReference, geth.CallMsg) ([]byte, error)
	CodeAt(context.Context, BlockReference, common.Address) ([]byte, error)
	Close()
}
