// Package aerodrome adapts Aerodrome volatile pools to the generic
// constant-product mirror and quote contracts.
package aerodrome

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/VarozXYZ/vernier/adapters/market/uniswapv2"
)

const ID = "aerodrome-volatile"

var syncTopic = crypto.Keccak256Hash([]byte("Sync(uint256,uint256)"))

type Config = uniswapv2.Config
type PoolInfo = uniswapv2.PoolInfo

// Adapter reuses the canonical reserve/event implementation. Aerodrome's
// volatile pool has the same state transitions as a V2 pair; only its Sync
// event uses uint256 arguments and therefore a different topic.
type Adapter struct{ *uniswapv2.Adapter }

func NewAdapter(config Config) (*Adapter, error) {
	config.SyncTopic = syncTopic
	adapter, err := uniswapv2.NewAdapter(config)
	if err != nil {
		return nil, err
	}
	return &Adapter{Adapter: adapter}, nil
}

func (*Adapter) ID() string { return ID }

func SyncTopic() common.Hash { return syncTopic }
