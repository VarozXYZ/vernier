// Package uniswapv4 adapts standard, static-fee Uniswap V4 pools to Vernier's
// deterministic concentrated-liquidity state and quote model.
//
// Pools with hooks or dynamic fees are rejected because their swap semantics
// cannot be reconstructed from PoolManager state alone.
package uniswapv4

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/adapters/market/uniswapv3"
	"github.com/VarozXYZ/vernier/domain/market"
)

const ID = "uniswap-v4"

const stateViewABIJSON = "[" +
	"{\"type\":\"function\",\"name\":\"getSlot0\",\"stateMutability\":\"view\",\"inputs\":[{\"name\":\"poolId\",\"type\":\"bytes32\"}],\"outputs\":[{\"type\":\"uint160\"},{\"type\":\"int24\"},{\"type\":\"uint24\"},{\"type\":\"uint24\"}]}," +
	"{\"type\":\"function\",\"name\":\"getLiquidity\",\"stateMutability\":\"view\",\"inputs\":[{\"name\":\"poolId\",\"type\":\"bytes32\"}],\"outputs\":[{\"type\":\"uint128\"}]}," +
	"{\"type\":\"function\",\"name\":\"getTickBitmap\",\"stateMutability\":\"view\",\"inputs\":[{\"name\":\"poolId\",\"type\":\"bytes32\"},{\"name\":\"tick\",\"type\":\"int16\"}],\"outputs\":[{\"type\":\"uint256\"}]}," +
	"{\"type\":\"function\",\"name\":\"getTickInfo\",\"stateMutability\":\"view\",\"inputs\":[{\"name\":\"poolId\",\"type\":\"bytes32\"},{\"name\":\"tick\",\"type\":\"int24\"}],\"outputs\":[{\"type\":\"uint128\"},{\"type\":\"int128\"},{\"type\":\"uint256\"},{\"type\":\"uint256\"}]}" +
	"]"

const poolManagerABIJSON = "[" +
	"{\"type\":\"event\",\"name\":\"Initialize\",\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"name\":\"id\",\"type\":\"bytes32\"},{\"indexed\":true,\"name\":\"currency0\",\"type\":\"address\"},{\"indexed\":true,\"name\":\"currency1\",\"type\":\"address\"},{\"indexed\":false,\"name\":\"fee\",\"type\":\"uint24\"},{\"indexed\":false,\"name\":\"tickSpacing\",\"type\":\"int24\"},{\"indexed\":false,\"name\":\"hooks\",\"type\":\"address\"},{\"indexed\":false,\"name\":\"sqrtPriceX96\",\"type\":\"uint160\"},{\"indexed\":false,\"name\":\"tick\",\"type\":\"int24\"}]}," +
	"{\"type\":\"event\",\"name\":\"ModifyLiquidity\",\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"name\":\"id\",\"type\":\"bytes32\"},{\"indexed\":true,\"name\":\"sender\",\"type\":\"address\"},{\"indexed\":false,\"name\":\"tickLower\",\"type\":\"int24\"},{\"indexed\":false,\"name\":\"tickUpper\",\"type\":\"int24\"},{\"indexed\":false,\"name\":\"liquidityDelta\",\"type\":\"int256\"},{\"indexed\":false,\"name\":\"salt\",\"type\":\"bytes32\"}]}," +
	"{\"type\":\"event\",\"name\":\"Swap\",\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"name\":\"id\",\"type\":\"bytes32\"},{\"indexed\":true,\"name\":\"sender\",\"type\":\"address\"},{\"indexed\":false,\"name\":\"amount0\",\"type\":\"int128\"},{\"indexed\":false,\"name\":\"amount1\",\"type\":\"int128\"},{\"indexed\":false,\"name\":\"sqrtPriceX96\",\"type\":\"uint160\"},{\"indexed\":false,\"name\":\"liquidity\",\"type\":\"uint128\"},{\"indexed\":false,\"name\":\"tick\",\"type\":\"int24\"},{\"indexed\":false,\"name\":\"fee\",\"type\":\"uint24\"}]}" +
	"]"

var (
	stateViewABI   = mustParseABI(stateViewABIJSON)
	poolManagerABI = mustParseABI(poolManagerABIJSON)
	poolKeyArgs    = mustPoolKeyArguments()
)

type CoverageProbe struct {
	ZeroForOne bool
	AmountIn   *big.Int
}

type Config struct {
	PoolManager  common.Address
	StateView    common.Address
	PoolID       common.Hash
	Currency0    common.Address
	Currency1    common.Address
	Fee          uint32
	TickSpacing  int32
	Hooks        common.Address
	MaxTickWords int
	Probes       []CoverageProbe
}

type Adapter struct {
	manager     common.Address
	stateView   common.Address
	poolID      common.Hash
	currency0   common.Address
	currency1   common.Address
	fee         uint32
	tickSpacing int32
	maxWords    int
	probes      []CoverageProbe
}

func NewAdapter(config Config) (*Adapter, error) {
	if config.PoolManager == (common.Address{}) || config.StateView == (common.Address{}) {
		return nil, fmt.Errorf("uniswap V4 PoolManager and StateView addresses are required")
	}
	if config.PoolID == (common.Hash{}) {
		return nil, fmt.Errorf("uniswap V4 pool ID is required")
	}
	if config.Currency0 == (common.Address{}) || config.Currency1 == (common.Address{}) || config.Currency0 == config.Currency1 {
		return nil, fmt.Errorf("uniswap V4 requires two distinct non-native currencies")
	}
	if bytesCompare(config.Currency0.Bytes(), config.Currency1.Bytes()) >= 0 {
		return nil, fmt.Errorf("uniswap V4 currencies must be canonically ordered")
	}
	if config.Fee == 0 || config.Fee >= 1_000_000 || config.Fee&0x800000 != 0 {
		return nil, fmt.Errorf("uniswap V4 adapter supports only non-zero static fees")
	}
	if config.TickSpacing <= 0 || config.TickSpacing > 32767 {
		return nil, fmt.Errorf("invalid Uniswap V4 tick spacing")
	}
	if config.Hooks != (common.Address{}) {
		return nil, fmt.Errorf("uniswap V4 hooks are not supported by the local quoter")
	}
	if config.MaxTickWords == 0 {
		config.MaxTickWords = 64
	}
	if config.MaxTickWords < 1 || config.MaxTickWords > 512 {
		return nil, fmt.Errorf("max tick words must be between 1 and 512")
	}
	computed, err := PoolID(config.Currency0, config.Currency1, config.Fee, config.TickSpacing, config.Hooks)
	if err != nil {
		return nil, err
	}
	if computed != config.PoolID {
		return nil, fmt.Errorf("uniswap V4 pool key does not match configured pool ID")
	}
	probes := make([]CoverageProbe, len(config.Probes))
	for index, probe := range config.Probes {
		if probe.AmountIn == nil || probe.AmountIn.Sign() <= 0 || probe.AmountIn.BitLen() > 256 {
			return nil, fmt.Errorf("coverage probe %d requires a positive uint256 amount", index)
		}
		probes[index] = CoverageProbe{ZeroForOne: probe.ZeroForOne, AmountIn: new(big.Int).Set(probe.AmountIn)}
	}
	return &Adapter{
		manager: config.PoolManager, stateView: config.StateView, poolID: config.PoolID,
		currency0: config.Currency0, currency1: config.Currency1, fee: config.Fee,
		tickSpacing: config.TickSpacing, maxWords: config.MaxTickWords, probes: probes,
	}, nil
}

func (*Adapter) ID() string { return ID }

func (a *Adapter) Filter() evm.LogFilter {
	return evm.LogFilter{
		Address: a.manager,
		Topics: []common.Hash{
			poolManagerABI.Events["Initialize"].ID,
			poolManagerABI.Events["ModifyLiquidity"].ID,
			poolManagerABI.Events["Swap"].ID,
		},
		IndexedTopics: [][]common.Hash{{a.poolID}},
	}
}

func (a *Adapter) Bootstrap(ctx context.Context, network evm.Network, block evm.BlockReference) (market.EventData, error) {
	if network == nil {
		return nil, fmt.Errorf("EVM network is required")
	}
	for label, address := range map[string]common.Address{"PoolManager": a.manager, "StateView": a.stateView} {
		code, err := network.CodeAt(ctx, block, address)
		if err != nil {
			return nil, err
		}
		if len(code) == 0 {
			return nil, fmt.Errorf("uniswap V4 %s has no bytecode at block %d", label, block.Number)
		}
	}
	slot, err := a.call(ctx, network, block, "getSlot0", a.poolID)
	if err != nil {
		return nil, err
	}
	sqrtPrice, err := bigValue(slot[0], "sqrt price")
	if err != nil {
		return nil, err
	}
	tick, err := int32Value(slot[1], "tick")
	if err != nil {
		return nil, err
	}
	protocolFee, err := uint32Value(slot[2], "protocol fee")
	if err != nil {
		return nil, err
	}
	if protocolFee != 0 {
		return nil, fmt.Errorf("uniswap V4 protocol fee %d is not supported by the local quoter", protocolFee)
	}
	lpFee, err := uint32Value(slot[3], "LP fee")
	if err != nil {
		return nil, err
	}
	if lpFee != a.fee {
		return nil, fmt.Errorf("uniswap V4 current LP fee %d differs from configured static fee %d", lpFee, a.fee)
	}
	liquidityValues, err := a.call(ctx, network, block, "getLiquidity", a.poolID)
	if err != nil {
		return nil, err
	}
	liquidity, err := bigValue(liquidityValues[0], "liquidity")
	if err != nil {
		return nil, err
	}
	return a.loadCoveredState(ctx, network, block, sqrtPrice, tick, liquidity)
}

func (a *Adapter) DecodeBlock(ctx context.Context, network evm.Network, block evm.BlockReference, logs []types.Log) (market.EventData, error) {
	if len(logs) == 0 {
		return nil, fmt.Errorf("uniswap V4 block %d contains no matching logs", block.Number)
	}
	ordered := append([]types.Log(nil), logs...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].TxIndex == ordered[j].TxIndex {
			return ordered[i].Index < ordered[j].Index
		}
		return ordered[i].TxIndex < ordered[j].TxIndex
	})
	updates := make([]market.EventData, 0, len(ordered))
	for _, event := range ordered {
		update, err := a.DecodeLog(ctx, network, block, event)
		if err != nil {
			return nil, err
		}
		updates = append(updates, update)
	}
	return uniswapv3.NewBlockUpdate(updates...)
}

func (a *Adapter) DecodeLog(_ context.Context, _ evm.Network, block evm.BlockReference, event types.Log) (market.EventData, error) {
	if event.Address != a.manager || event.BlockHash != block.Hash || event.BlockNumber != block.Number || event.Removed {
		return nil, fmt.Errorf("uniswap V4 log does not belong to PoolManager and block")
	}
	if len(event.Topics) < 2 || event.Topics[1] != a.poolID {
		return nil, fmt.Errorf("uniswap V4 log does not belong to configured pool")
	}
	switch event.Topics[0] {
	case poolManagerABI.Events["Initialize"].ID:
		if len(event.Topics) != 4 {
			return nil, fmt.Errorf("invalid Uniswap V4 Initialize topics")
		}
		if common.BytesToAddress(event.Topics[2][12:]) != a.currency0 || common.BytesToAddress(event.Topics[3][12:]) != a.currency1 {
			return nil, fmt.Errorf("uniswap V4 Initialize currencies do not match configured pool")
		}
		values, err := poolManagerABI.Events["Initialize"].Inputs.NonIndexed().Unpack(event.Data)
		if err != nil {
			return nil, fmt.Errorf("decode Uniswap V4 Initialize: %w", err)
		}
		fee, err := uint32Value(values[0], "initialize fee")
		if err != nil {
			return nil, err
		}
		spacing, err := int32Value(values[1], "initialize tick spacing")
		if err != nil {
			return nil, err
		}
		hooks, ok := values[2].(common.Address)
		if !ok {
			return nil, fmt.Errorf("invalid initialize hooks")
		}
		if fee != a.fee || spacing != a.tickSpacing || hooks != (common.Address{}) {
			return nil, fmt.Errorf("uniswap V4 Initialize pool key differs from configured static pool")
		}
		price, err := bigValue(values[3], "initialize sqrt price")
		if err != nil {
			return nil, err
		}
		tick, err := int32Value(values[4], "initialize tick")
		if err != nil {
			return nil, err
		}
		return uniswapv3.NewInitializeUpdate(price, tick)
	case poolManagerABI.Events["ModifyLiquidity"].ID:
		if len(event.Topics) != 3 {
			return nil, fmt.Errorf("invalid Uniswap V4 ModifyLiquidity topics")
		}
		values, err := poolManagerABI.Events["ModifyLiquidity"].Inputs.NonIndexed().Unpack(event.Data)
		if err != nil {
			return nil, fmt.Errorf("decode Uniswap V4 ModifyLiquidity: %w", err)
		}
		lower, err := int32Value(values[0], "lower tick")
		if err != nil {
			return nil, err
		}
		upper, err := int32Value(values[1], "upper tick")
		if err != nil {
			return nil, err
		}
		delta, err := bigValue(values[2], "liquidity delta")
		if err != nil {
			return nil, err
		}
		return uniswapv3.NewLiquidityUpdate(lower, upper, delta)
	case poolManagerABI.Events["Swap"].ID:
		if len(event.Topics) != 3 {
			return nil, fmt.Errorf("invalid Uniswap V4 Swap topics")
		}
		values, err := poolManagerABI.Events["Swap"].Inputs.NonIndexed().Unpack(event.Data)
		if err != nil {
			return nil, fmt.Errorf("decode Uniswap V4 Swap: %w", err)
		}
		price, err := bigValue(values[2], "swap sqrt price")
		if err != nil {
			return nil, err
		}
		liquidity, err := bigValue(values[3], "swap liquidity")
		if err != nil {
			return nil, err
		}
		tick, err := int32Value(values[4], "swap tick")
		if err != nil {
			return nil, err
		}
		fee, err := uint32Value(values[5], "swap fee")
		if err != nil {
			return nil, err
		}
		if fee != a.fee {
			return nil, fmt.Errorf("uniswap V4 swap fee %d differs from configured static fee %d", fee, a.fee)
		}
		return uniswapv3.NewSwapUpdate(price, tick, liquidity)
	default:
		return nil, fmt.Errorf("unsupported Uniswap V4 event topic %s", event.Topics[0])
	}
}

func (a *Adapter) loadCoveredState(ctx context.Context, network evm.Network, block evm.BlockReference, sqrtPrice *big.Int, tick int32, liquidity *big.Int) (uniswapv3.StateUpdate, error) {
	currentWord := floorDiv32(floorDiv32(tick, a.tickSpacing), 256)
	loaded := make(map[int32][]uniswapv3.Tick)
	if err := a.loadWord(ctx, network, block, currentWord, loaded); err != nil {
		return uniswapv3.StateUpdate{}, err
	}
	minWord, maxWord := currentWord, currentWord
	for _, probe := range a.probes {
		for {
			coverage, _ := uniswapv3.NewTickCoverage(minWord, maxWord)
			update, err := uniswapv3.NewCoveredStateUpdate(sqrtPrice, tick, liquidity, a.fee, a.tickSpacing, flattenTicks(loaded), coverage)
			if err != nil {
				return uniswapv3.StateUpdate{}, err
			}
			err = uniswapv3.ValidateExactInputCoverage(update, probe.ZeroForOne, probe.AmountIn)
			if err == nil {
				break
			}
			if !errors.Is(err, uniswapv3.ErrInsufficientTickCoverage) {
				return uniswapv3.StateUpdate{}, fmt.Errorf("validate Uniswap V4 coverage probe: %w", err)
			}
			if len(loaded) >= a.maxWords {
				return uniswapv3.StateUpdate{}, fmt.Errorf("%w: maximum of %d words reached", uniswapv3.ErrInsufficientTickCoverage, a.maxWords)
			}
			word := maxWord + 1
			if probe.ZeroForOne {
				word = minWord - 1
			}
			if err := a.loadWord(ctx, network, block, word, loaded); err != nil {
				return uniswapv3.StateUpdate{}, err
			}
			if probe.ZeroForOne {
				minWord = word
			} else {
				maxWord = word
			}
		}
		if len(loaded) < a.maxWords {
			guard := maxWord + 1
			if probe.ZeroForOne {
				guard = minWord - 1
			}
			if _, exists := loaded[guard]; !exists {
				if err := a.loadWord(ctx, network, block, guard, loaded); err != nil {
					return uniswapv3.StateUpdate{}, err
				}
				if probe.ZeroForOne {
					minWord = guard
				} else {
					maxWord = guard
				}
			}
		}
	}
	coverage, _ := uniswapv3.NewTickCoverage(minWord, maxWord)
	return uniswapv3.NewCoveredStateUpdate(sqrtPrice, tick, liquidity, a.fee, a.tickSpacing, flattenTicks(loaded), coverage)
}

func (a *Adapter) loadWord(ctx context.Context, network evm.Network, block evm.BlockReference, word int32, loaded map[int32][]uniswapv3.Tick) error {
	if _, exists := loaded[word]; exists {
		return nil
	}
	if word < -32768 || word > 32767 {
		return fmt.Errorf("uniswap V4 bitmap word %d exceeds int16", word)
	}
	values, err := a.call(ctx, network, block, "getTickBitmap", a.poolID, int16(word))
	if err != nil {
		return err
	}
	bitmap, err := bigValue(values[0], "tick bitmap")
	if err != nil {
		return err
	}
	var indices []int32
	for bit := 0; bit < 256; bit++ {
		if bitmap.Bit(bit) == 1 {
			index := (int64(word)*256 + int64(bit)) * int64(a.tickSpacing)
			if index < int64(uniswapv3.MinTick) || index > int64(uniswapv3.MaxTick) {
				return fmt.Errorf("initialized tick %d is outside Uniswap bounds", index)
			}
			indices = append(indices, int32(index))
		}
	}
	ticks := make([]uniswapv3.Tick, len(indices))
	sem := make(chan struct{}, 8)
	var wait sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	for position, index := range indices {
		wait.Add(1)
		go func(position int, index int32) {
			defer wait.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			values, callErr := a.call(ctx, network, block, "getTickInfo", a.poolID, big.NewInt(int64(index)))
			if callErr != nil {
				setFirstError(&errMu, &firstErr, callErr)
				return
			}
			gross, grossErr := bigValue(values[0], "tick liquidity gross")
			net, netErr := bigValue(values[1], "tick liquidity net")
			if grossErr != nil {
				setFirstError(&errMu, &firstErr, grossErr)
				return
			}
			if netErr != nil {
				setFirstError(&errMu, &firstErr, netErr)
				return
			}
			initialized, tickErr := uniswapv3.NewTick(index, gross, net)
			if tickErr != nil {
				setFirstError(&errMu, &firstErr, tickErr)
				return
			}
			ticks[position] = initialized
		}(position, index)
	}
	wait.Wait()
	if firstErr != nil {
		return firstErr
	}
	loaded[word] = ticks
	return nil
}

func (a *Adapter) call(ctx context.Context, network evm.Network, block evm.BlockReference, method string, arguments ...any) ([]any, error) {
	input, err := stateViewABI.Pack(method, arguments...)
	if err != nil {
		return nil, fmt.Errorf("encode Uniswap V4 StateView %s call: %w", method, err)
	}
	result, err := network.CallContract(ctx, block, geth.CallMsg{To: &a.stateView, Data: input})
	if err != nil {
		return nil, err
	}
	values, err := stateViewABI.Unpack(method, result)
	if err != nil {
		return nil, fmt.Errorf("decode Uniswap V4 StateView %s response: %w", method, err)
	}
	return values, nil
}

func PoolID(currency0, currency1 common.Address, fee uint32, tickSpacing int32, hooks common.Address) (common.Hash, error) {
	encoded, err := poolKeyArgs.Pack(currency0, currency1, new(big.Int).SetUint64(uint64(fee)), big.NewInt(int64(tickSpacing)), hooks)
	if err != nil {
		return common.Hash{}, fmt.Errorf("encode Uniswap V4 pool key: %w", err)
	}
	return crypto.Keccak256Hash(encoded), nil
}

func mustPoolKeyArguments() abi.Arguments {
	names := []string{"address", "address", "uint24", "int24", "address"}
	result := make(abi.Arguments, len(names))
	for index, name := range names {
		kind, err := abi.NewType(name, "", nil)
		if err != nil {
			panic(err)
		}
		result[index] = abi.Argument{Type: kind}
	}
	return result
}

func mustParseABI(source string) abi.ABI {
	parsed, err := abi.JSON(strings.NewReader(source))
	if err != nil {
		panic(err)
	}
	return parsed
}

func bigValue(value any, name string) (*big.Int, error) {
	integer, ok := value.(*big.Int)
	if !ok || integer == nil {
		return nil, fmt.Errorf("invalid %s", name)
	}
	return new(big.Int).Set(integer), nil
}

func uint32Value(value any, name string) (uint32, error) {
	integer, err := bigValue(value, name)
	if err != nil {
		return 0, err
	}
	if integer.Sign() < 0 || integer.BitLen() > 32 {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return uint32(integer.Uint64()), nil
}

func int32Value(value any, name string) (int32, error) {
	integer, err := bigValue(value, name)
	if err != nil {
		return 0, err
	}
	if !integer.IsInt64() || integer.Int64() < -2147483648 || integer.Int64() > 2147483647 {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return int32(integer.Int64()), nil
}

func flattenTicks(words map[int32][]uniswapv3.Tick) []uniswapv3.Tick {
	var ticks []uniswapv3.Tick
	for _, wordTicks := range words {
		ticks = append(ticks, wordTicks...)
	}
	sort.Slice(ticks, func(i, j int) bool { return ticks[i].Index() < ticks[j].Index() })
	return ticks
}

func floorDiv32(value, divisor int32) int32 {
	quotient := value / divisor
	if value%divisor != 0 && (value < 0) != (divisor < 0) {
		quotient--
	}
	return quotient
}

func setFirstError(mu *sync.Mutex, target *error, candidate error) {
	mu.Lock()
	defer mu.Unlock()
	if *target == nil {
		*target = candidate
	}
}

func bytesCompare(left, right []byte) int {
	for index := range left {
		if left[index] < right[index] {
			return -1
		}
		if left[index] > right[index] {
			return 1
		}
	}
	return 0
}

var _ interface {
	ID() string
	Filter() evm.LogFilter
	Bootstrap(context.Context, evm.Network, evm.BlockReference) (market.EventData, error)
	DecodeBlock(context.Context, evm.Network, evm.BlockReference, []types.Log) (market.EventData, error)
} = (*Adapter)(nil)
