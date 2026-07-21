// Package aerodromeslipstream adapts Aerodrome Slipstream pools while reusing
// the canonical Uniswap V3 state, reducer, and local quote implementation.
package aerodromeslipstream

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/adapters/market/uniswapv3"
	"github.com/VarozXYZ/vernier/domain/market"
)

const ID = "aerodrome-slipstream"

const metadataABIJSON = `[
  {"type":"function","name":"factory","stateMutability":"view","inputs":[],"outputs":[{"type":"address"}]},
  {"type":"function","name":"fee","stateMutability":"view","inputs":[],"outputs":[{"type":"uint24"}]}
]`

var metadataABI = mustABI(metadataABIJSON)

type Config struct {
	Pool         common.Address
	Factory      common.Address
	BaseToken    common.Address
	QuoteToken   common.Address
	MaxTickWords int
	Probes       []uniswapv3.CoverageProbe
}

type Adapter struct {
	config Config
	inner  *uniswapv3.Adapter
}

func NewAdapter(config Config) (*Adapter, error) {
	if config.Pool == (common.Address{}) || config.Factory == (common.Address{}) ||
		config.BaseToken == (common.Address{}) || config.QuoteToken == (common.Address{}) ||
		config.BaseToken == config.QuoteToken {
		return nil, fmt.Errorf("slipstream pool, factory, and distinct market tokens are required")
	}
	inner, err := uniswapv3.NewAdapter(uniswapv3.OnChainConfig{
		Pool: config.Pool, MaxTickWords: config.MaxTickWords, Probes: config.Probes,
	})
	if err != nil {
		return nil, err
	}
	return &Adapter{config: config, inner: inner}, nil
}

func (*Adapter) ID() string { return ID }

func (a *Adapter) Filter() evm.LogFilter { return a.inner.Filter() }

func (a *Adapter) PoolInfo() (uniswapv3.PoolInfo, bool) { return a.inner.PoolInfo() }

func (a *Adapter) Bootstrap(
	ctx context.Context,
	network evm.Network,
	block evm.BlockReference,
) (market.EventData, error) {
	factory, err := a.addressCall(ctx, network, block, "factory")
	if err != nil {
		return nil, err
	}
	if factory != a.config.Factory {
		return nil, fmt.Errorf("slipstream pool factory does not match configuration")
	}
	update, err := a.inner.Bootstrap(ctx, network, block)
	if err != nil {
		return nil, err
	}
	info, ok := a.inner.PoolInfo()
	if !ok || !sameEndpoints(info.Token0, info.Token1, a.config.BaseToken, a.config.QuoteToken) {
		return nil, fmt.Errorf("slipstream pool tokens do not match market endpoints")
	}
	return update, nil
}

// DecodeBlock reuses canonical V3 log decoding and refreshes the dynamic pool
// fee at that exact active block. It does not poll or reload state between
// pool events.
func (a *Adapter) DecodeBlock(
	ctx context.Context,
	network evm.Network,
	block evm.BlockReference,
	logs []types.Log,
) (market.EventData, error) {
	update, err := a.inner.DecodeBlock(ctx, network, block, logs)
	if err != nil {
		return nil, err
	}
	fee, err := a.feeAt(ctx, network, block)
	if err != nil {
		return nil, err
	}
	feeUpdate, err := uniswapv3.NewFeeUpdate(fee)
	if err != nil {
		return nil, err
	}
	return uniswapv3.NewBlockUpdate(update, feeUpdate)
}

// DecodeLog preserves subscription arrival order while retaining the
// variant's dynamic fee update for the exact event block.
func (a *Adapter) DecodeLog(
	ctx context.Context,
	network evm.Network,
	block evm.BlockReference,
	event types.Log,
) (market.EventData, error) {
	update, err := a.inner.DecodeLog(ctx, network, block, event)
	if err != nil {
		return nil, err
	}
	fee, err := a.feeAt(ctx, network, block)
	if err != nil {
		return nil, err
	}
	feeUpdate, err := uniswapv3.NewFeeUpdate(fee)
	if err != nil {
		return nil, err
	}
	return uniswapv3.NewBlockUpdate(update, feeUpdate)
}

func (a *Adapter) ExpandCoverage(
	ctx context.Context,
	network evm.Network,
	block evm.BlockReference,
	snapshot uniswapv3.Snapshot,
) (uniswapv3.StateUpdate, error) {
	return a.inner.ExpandCoverage(ctx, network, block, snapshot)
}

func (a *Adapter) addressCall(
	ctx context.Context,
	network evm.Network,
	block evm.BlockReference,
	method string,
) (common.Address, error) {
	values, err := a.call(ctx, network, block, method)
	if err != nil {
		return common.Address{}, err
	}
	value, ok := values[0].(common.Address)
	if !ok || value == (common.Address{}) {
		return common.Address{}, fmt.Errorf("slipstream %s returned an invalid address", method)
	}
	return value, nil
}

func (a *Adapter) feeAt(
	ctx context.Context,
	network evm.Network,
	block evm.BlockReference,
) (uint32, error) {
	values, err := a.call(ctx, network, block, "fee")
	if err != nil {
		return 0, err
	}
	value, ok := values[0].(*big.Int)
	if !ok || value == nil || !value.IsUint64() || value.Uint64() >= 1_000_000 {
		return 0, fmt.Errorf("slipstream fee is invalid")
	}
	return uint32(value.Uint64()), nil
}

func (a *Adapter) call(
	ctx context.Context,
	network evm.Network,
	block evm.BlockReference,
	method string,
) ([]any, error) {
	if network == nil {
		return nil, fmt.Errorf("EVM network is required")
	}
	input, err := metadataABI.Pack(method)
	if err != nil {
		return nil, fmt.Errorf("encode Slipstream %s call: %w", method, err)
	}
	result, err := network.CallContract(ctx, block, geth.CallMsg{To: &a.config.Pool, Data: input})
	if err != nil {
		return nil, err
	}
	values, err := metadataABI.Unpack(method, result)
	if err != nil {
		return nil, fmt.Errorf("decode Slipstream %s response: %w", method, err)
	}
	return values, nil
}

func sameEndpoints(token0, token1, base, quote common.Address) bool {
	return token0 == base && token1 == quote || token0 == quote && token1 == base
}

func mustABI(definition string) abi.ABI {
	parsed, err := abi.JSON(strings.NewReader(definition))
	if err != nil {
		panic(err)
	}
	return parsed
}
