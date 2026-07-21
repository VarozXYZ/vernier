// Package uniswapv2 adapts canonical Uniswap V2 pools to Vernier's generic
// constant-product state and quote contracts.
package uniswapv2

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strings"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/adapters/market/constantproduct"
	"github.com/VarozXYZ/vernier/domain/market"
)

const ID = "uniswap-v2"

const pairABIJSON = `[
  {"type":"function","name":"token0","stateMutability":"view","inputs":[],"outputs":[{"type":"address"}]},
  {"type":"function","name":"token1","stateMutability":"view","inputs":[],"outputs":[{"type":"address"}]},
  {"type":"function","name":"factory","stateMutability":"view","inputs":[],"outputs":[{"type":"address"}]},
  {"type":"function","name":"getReserves","stateMutability":"view","inputs":[],"outputs":[{"type":"uint112"},{"type":"uint112"},{"type":"uint32"}]},
  {"type":"event","name":"Sync","anonymous":false,"inputs":[{"indexed":false,"name":"reserve0","type":"uint112"},{"indexed":false,"name":"reserve1","type":"uint112"}]}
]`

var pairABI = mustABI(pairABIJSON)

type Config struct {
	Pool       common.Address
	Factory    common.Address
	BaseToken  common.Address
	QuoteToken common.Address
	FeeBPS     uint16
}

type PoolInfo struct {
	Token0  common.Address
	Token1  common.Address
	Factory common.Address
}

type Adapter struct {
	config Config
	info   PoolInfo
	loaded bool
}

func NewAdapter(config Config) (*Adapter, error) {
	if config.Pool == (common.Address{}) || config.Factory == (common.Address{}) ||
		config.BaseToken == (common.Address{}) || config.QuoteToken == (common.Address{}) ||
		config.BaseToken == config.QuoteToken {
		return nil, fmt.Errorf("uniswap V2 pool, factory, and distinct market tokens are required")
	}
	if config.FeeBPS == 0 {
		config.FeeBPS = 30
	}
	if config.FeeBPS >= 10_000 {
		return nil, fmt.Errorf("uniswap V2 fee must be below 10000 basis points")
	}
	return &Adapter{config: config}, nil
}

func (*Adapter) ID() string { return ID }

func (a *Adapter) Filter() evm.LogFilter {
	return evm.LogFilter{Address: a.config.Pool, Topics: []common.Hash{pairABI.Events["Sync"].ID}}
}

func (a *Adapter) PoolInfo() (PoolInfo, bool) { return a.info, a.loaded }

func (a *Adapter) Bootstrap(ctx context.Context, network evm.Network, block evm.BlockReference) (market.EventData, error) {
	if network == nil {
		return nil, fmt.Errorf("EVM network is required")
	}
	code, err := network.CodeAt(ctx, block, a.config.Pool)
	if err != nil {
		return nil, err
	}
	if len(code) == 0 {
		return nil, fmt.Errorf("uniswap V2 pool has no code")
	}
	token0, err := a.addressCall(ctx, network, block, "token0")
	if err != nil {
		return nil, err
	}
	token1, err := a.addressCall(ctx, network, block, "token1")
	if err != nil {
		return nil, err
	}
	factory, err := a.addressCall(ctx, network, block, "factory")
	if err != nil {
		return nil, err
	}
	if token0 == token1 || factory != a.config.Factory {
		return nil, fmt.Errorf("uniswap V2 pool metadata does not match configuration")
	}
	if !sameEndpoints(token0, token1, a.config.BaseToken, a.config.QuoteToken) {
		return nil, fmt.Errorf("uniswap V2 pool tokens do not match market endpoints")
	}
	values, err := a.call(ctx, network, block, "getReserves")
	if err != nil {
		return nil, err
	}
	reserve0, err := integer(values[0], "reserve0")
	if err != nil {
		return nil, err
	}
	reserve1, err := integer(values[1], "reserve1")
	if err != nil {
		return nil, err
	}
	a.info = PoolInfo{Token0: token0, Token1: token1, Factory: factory}
	a.loaded = true
	return a.update(token0, reserve0, reserve1)
}

func (a *Adapter) DecodeBlock(_ context.Context, _ evm.Network, block evm.BlockReference, logs []types.Log) (market.EventData, error) {
	if !a.loaded {
		return nil, fmt.Errorf("uniswap V2 pool metadata is unavailable")
	}
	ordered := append([]types.Log(nil), logs...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Index < ordered[j].Index })
	var latest *types.Log
	for index := range ordered {
		candidate := &ordered[index]
		if candidate.Address != a.config.Pool || candidate.BlockHash != block.Hash ||
			candidate.BlockNumber != block.Number || candidate.Removed || len(candidate.Topics) != 1 ||
			candidate.Topics[0] != pairABI.Events["Sync"].ID {
			continue
		}
		latest = candidate
	}
	if latest == nil {
		return nil, fmt.Errorf("uniswap V2 active block contains no usable Sync event")
	}
	values, err := pairABI.Events["Sync"].Inputs.NonIndexed().Unpack(latest.Data)
	if err != nil {
		return nil, fmt.Errorf("decode Uniswap V2 Sync event: %w", err)
	}
	reserve0, err := integer(values[0], "reserve0")
	if err != nil {
		return nil, err
	}
	reserve1, err := integer(values[1], "reserve1")
	if err != nil {
		return nil, err
	}
	return a.update(a.info.Token0, reserve0, reserve1)
}

// DecodeLog decodes one filtered Sync notification. The feed invokes this
// capability in WebSocket arrival order, so multiple events in one block are
// applied one at a time rather than collapsed into a block-level read.
func (a *Adapter) DecodeLog(_ context.Context, _ evm.Network, block evm.BlockReference, event types.Log) (market.EventData, error) {
	if !a.loaded {
		return nil, fmt.Errorf("uniswap V2 pool metadata is unavailable")
	}
	if event.Address != a.config.Pool || event.BlockHash != block.Hash ||
		event.BlockNumber != block.Number || event.Removed || len(event.Topics) != 1 ||
		event.Topics[0] != pairABI.Events["Sync"].ID {
		return nil, fmt.Errorf("uniswap V2 log does not belong to pool and block")
	}
	values, err := pairABI.Events["Sync"].Inputs.NonIndexed().Unpack(event.Data)
	if err != nil {
		return nil, fmt.Errorf("decode Uniswap V2 Sync event: %w", err)
	}
	reserve0, err := integer(values[0], "reserve0")
	if err != nil {
		return nil, err
	}
	reserve1, err := integer(values[1], "reserve1")
	if err != nil {
		return nil, err
	}
	return a.update(a.info.Token0, reserve0, reserve1)
}

func (a *Adapter) update(token0 common.Address, reserve0, reserve1 *big.Int) (constantproduct.ReserveUpdate, error) {
	base, quote := reserve0, reserve1
	if token0 != a.config.BaseToken {
		base, quote = reserve1, reserve0
	}
	return constantproduct.NewReserveUpdate(base, quote, a.config.FeeBPS)
}

func (a *Adapter) addressCall(ctx context.Context, network evm.Network, block evm.BlockReference, method string) (common.Address, error) {
	values, err := a.call(ctx, network, block, method)
	if err != nil {
		return common.Address{}, err
	}
	value, ok := values[0].(common.Address)
	if !ok || value == (common.Address{}) {
		return common.Address{}, fmt.Errorf("uniswap V2 %s returned an invalid address", method)
	}
	return value, nil
}

func (a *Adapter) call(ctx context.Context, network evm.Network, block evm.BlockReference, method string) ([]any, error) {
	input, err := pairABI.Pack(method)
	if err != nil {
		return nil, fmt.Errorf("encode Uniswap V2 %s call: %w", method, err)
	}
	result, err := network.CallContract(ctx, block, geth.CallMsg{To: &a.config.Pool, Data: input})
	if err != nil {
		return nil, err
	}
	values, err := pairABI.Unpack(method, result)
	if err != nil {
		return nil, fmt.Errorf("decode Uniswap V2 %s response: %w", method, err)
	}
	return values, nil
}

func sameEndpoints(token0, token1, base, quote common.Address) bool {
	return token0 == base && token1 == quote || token0 == quote && token1 == base
}

func integer(value any, name string) (*big.Int, error) {
	result, ok := value.(*big.Int)
	if !ok || result == nil || result.Sign() <= 0 {
		return nil, fmt.Errorf("uniswap V2 %s is invalid", name)
	}
	return new(big.Int).Set(result), nil
}

func mustABI(definition string) abi.ABI {
	parsed, err := abi.JSON(strings.NewReader(definition))
	if err != nil {
		panic(err)
	}
	return parsed
}
