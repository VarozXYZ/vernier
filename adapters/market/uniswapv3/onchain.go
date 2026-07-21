package uniswapv3

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

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/domain/market"
)

const ID = "uniswap-v3"

const poolABIJSON = "[" +
	"{\"type\":\"function\",\"name\":\"token0\",\"stateMutability\":\"view\",\"inputs\":[],\"outputs\":[{\"type\":\"address\"}]}," +
	"{\"type\":\"function\",\"name\":\"token1\",\"stateMutability\":\"view\",\"inputs\":[],\"outputs\":[{\"type\":\"address\"}]}," +
	"{\"type\":\"function\",\"name\":\"fee\",\"stateMutability\":\"view\",\"inputs\":[],\"outputs\":[{\"type\":\"uint24\"}]}," +
	"{\"type\":\"function\",\"name\":\"tickSpacing\",\"stateMutability\":\"view\",\"inputs\":[],\"outputs\":[{\"type\":\"int24\"}]}," +
	"{\"type\":\"function\",\"name\":\"liquidity\",\"stateMutability\":\"view\",\"inputs\":[],\"outputs\":[{\"type\":\"uint128\"}]}," +
	"{\"type\":\"function\",\"name\":\"slot0\",\"stateMutability\":\"view\",\"inputs\":[],\"outputs\":[{\"type\":\"uint160\"},{\"type\":\"int24\"}]}," +
	"{\"type\":\"function\",\"name\":\"tickBitmap\",\"stateMutability\":\"view\",\"inputs\":[{\"type\":\"int16\"}],\"outputs\":[{\"type\":\"uint256\"}]}," +
	"{\"type\":\"function\",\"name\":\"ticks\",\"stateMutability\":\"view\",\"inputs\":[{\"type\":\"int24\"}],\"outputs\":[{\"type\":\"uint128\"},{\"type\":\"int128\"}]}," +
	"{\"type\":\"event\",\"name\":\"Initialize\",\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"name\":\"sqrtPriceX96\",\"type\":\"uint160\"},{\"indexed\":false,\"name\":\"tick\",\"type\":\"int24\"}]}," +
	"{\"type\":\"event\",\"name\":\"Swap\",\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"name\":\"sender\",\"type\":\"address\"},{\"indexed\":true,\"name\":\"recipient\",\"type\":\"address\"},{\"indexed\":false,\"name\":\"amount0\",\"type\":\"int256\"},{\"indexed\":false,\"name\":\"amount1\",\"type\":\"int256\"},{\"indexed\":false,\"name\":\"sqrtPriceX96\",\"type\":\"uint160\"},{\"indexed\":false,\"name\":\"liquidity\",\"type\":\"uint128\"},{\"indexed\":false,\"name\":\"tick\",\"type\":\"int24\"}]}," +
	"{\"type\":\"event\",\"name\":\"Mint\",\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"name\":\"sender\",\"type\":\"address\"},{\"indexed\":true,\"name\":\"owner\",\"type\":\"address\"},{\"indexed\":true,\"name\":\"tickLower\",\"type\":\"int24\"},{\"indexed\":true,\"name\":\"tickUpper\",\"type\":\"int24\"},{\"indexed\":false,\"name\":\"amount\",\"type\":\"uint128\"},{\"indexed\":false,\"name\":\"amount0\",\"type\":\"uint256\"},{\"indexed\":false,\"name\":\"amount1\",\"type\":\"uint256\"}]}," +
	"{\"type\":\"event\",\"name\":\"Burn\",\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"name\":\"owner\",\"type\":\"address\"},{\"indexed\":true,\"name\":\"tickLower\",\"type\":\"int24\"},{\"indexed\":true,\"name\":\"tickUpper\",\"type\":\"int24\"},{\"indexed\":false,\"name\":\"amount\",\"type\":\"uint128\"},{\"indexed\":false,\"name\":\"amount0\",\"type\":\"uint256\"},{\"indexed\":false,\"name\":\"amount1\",\"type\":\"uint256\"}]}" +
	"]"

var poolABI = mustParseABI(poolABIJSON)

type CoverageProbe struct {
	ZeroForOne bool
	AmountIn   *big.Int
}

type OnChainConfig struct {
	Pool         common.Address
	MaxTickWords int
	Probes       []CoverageProbe
}

type PoolInfo struct {
	Token0 common.Address
	Token1 common.Address
	Fee    uint32
}

type Adapter struct {
	pool     common.Address
	maxWords int
	probes   []CoverageProbe
	mu       sync.RWMutex
	info     PoolInfo
	hasInfo  bool
}

func NewAdapter(config OnChainConfig) (*Adapter, error) {
	if config.Pool == (common.Address{}) {
		return nil, fmt.Errorf("uniswap V3 pool address is required")
	}
	if config.MaxTickWords == 0 {
		config.MaxTickWords = 64
	}
	if config.MaxTickWords < 1 || config.MaxTickWords > 512 {
		return nil, fmt.Errorf("max tick words must be between 1 and 512")
	}
	probes := make([]CoverageProbe, len(config.Probes))
	for index, probe := range config.Probes {
		if probe.AmountIn == nil || probe.AmountIn.Sign() <= 0 || probe.AmountIn.BitLen() > 256 {
			return nil, fmt.Errorf("coverage probe %d requires a positive uint256 amount", index)
		}
		probes[index] = CoverageProbe{ZeroForOne: probe.ZeroForOne, AmountIn: cloneInt(probe.AmountIn)}
	}
	return &Adapter{pool: config.Pool, maxWords: config.MaxTickWords, probes: probes}, nil
}

func (*Adapter) ID() string { return ID }

func (a *Adapter) Filter() evm.LogFilter {
	return evm.LogFilter{
		Address: a.pool,
		Topics: []common.Hash{
			poolABI.Events["Initialize"].ID,
			poolABI.Events["Swap"].ID,
			poolABI.Events["Mint"].ID,
			poolABI.Events["Burn"].ID,
		},
	}
}

func (a *Adapter) PoolInfo() (PoolInfo, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.info, a.hasInfo
}

// ExpandCoverage loads only additional bitmap words required by the configured
// probes. It preserves the snapshot's absolute pool state and does not perform
// a full pool reload.
func (a *Adapter) ExpandCoverage(
	ctx context.Context,
	network evm.Network,
	block evm.BlockReference,
	snapshot Snapshot,
) (StateUpdate, error) {
	if snapshot.schemaVersion != snapshotSchemaVersion || snapshot.coverage.Full() {
		return StateUpdate{}, fmt.Errorf("bounded compatible Uniswap V3 snapshot is required")
	}
	loaded := make(map[int32][]Tick)
	for word := snapshot.coverage.minWord; word <= snapshot.coverage.maxWord; word++ {
		loaded[word] = nil
	}
	for _, initialized := range snapshot.ticks {
		word := int32(floorDiv(floorDiv(int64(initialized.index), int64(snapshot.tickSpacing)), 256))
		loaded[word] = append(loaded[word], initialized)
	}
	minWord, maxWord := snapshot.coverage.minWord, snapshot.coverage.maxWord
	if err := a.expandForProbes(ctx, network, block, snapshot.sqrtPriceX96, snapshot.tick, snapshot.liquidity, snapshot.feePips, snapshot.tickSpacing, loaded, &minWord, &maxWord); err != nil {
		return StateUpdate{}, err
	}
	coverage, _ := NewTickCoverage(minWord, maxWord)
	return NewCoveredStateUpdate(
		snapshot.sqrtPriceX96, snapshot.tick, snapshot.liquidity, snapshot.feePips,
		snapshot.tickSpacing, flattenTicks(loaded), coverage,
	)
}

func (a *Adapter) Bootstrap(ctx context.Context, network evm.Network, block evm.BlockReference) (market.EventData, error) {
	if network == nil {
		return nil, fmt.Errorf("EVM network is required")
	}
	code, err := network.CodeAt(ctx, block, a.pool)
	if err != nil {
		return nil, err
	}
	if len(code) == 0 {
		return nil, fmt.Errorf("uniswap V3 pool has no bytecode at block %d", block.Number)
	}
	token0Values, err := a.call(ctx, network, block, "token0")
	if err != nil {
		return nil, err
	}
	token1Values, err := a.call(ctx, network, block, "token1")
	if err != nil {
		return nil, err
	}
	token0, ok0 := token0Values[0].(common.Address)
	token1, ok1 := token1Values[0].(common.Address)
	if !ok0 || !ok1 || token0 == (common.Address{}) || token1 == (common.Address{}) || token0 == token1 {
		return nil, fmt.Errorf("uniswap V3 pool returned invalid tokens")
	}
	feeValues, err := a.call(ctx, network, block, "fee")
	if err != nil {
		return nil, err
	}
	fee, err := uint32Value(feeValues[0], "fee")
	if err != nil {
		return nil, err
	}
	spacingValues, err := a.call(ctx, network, block, "tickSpacing")
	if err != nil {
		return nil, err
	}
	spacing, err := int32Value(spacingValues[0], "tick spacing")
	if err != nil {
		return nil, err
	}
	slotValues, err := a.call(ctx, network, block, "slot0")
	if err != nil {
		return nil, err
	}
	sqrtPrice, err := bigValue(slotValues[0], "sqrt price")
	if err != nil {
		return nil, err
	}
	tick, err := int32Value(slotValues[1], "tick")
	if err != nil {
		return nil, err
	}
	liquidityValues, err := a.call(ctx, network, block, "liquidity")
	if err != nil {
		return nil, err
	}
	liquidity, err := bigValue(liquidityValues[0], "liquidity")
	if err != nil {
		return nil, err
	}
	update, err := a.loadCoveredState(ctx, network, block, sqrtPrice, tick, liquidity, fee, spacing)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.info = PoolInfo{Token0: token0, Token1: token1, Fee: fee}
	a.hasInfo = true
	a.mu.Unlock()
	return update, nil
}

func (a *Adapter) DecodeBlock(_ context.Context, _ evm.Network, block evm.BlockReference, logs []types.Log) (market.EventData, error) {
	if len(logs) == 0 {
		return nil, fmt.Errorf("uniswap V3 block %d contains no matching logs", block.Number)
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
		if event.Address != a.pool || event.BlockHash != block.Hash || event.BlockNumber != block.Number || event.Removed {
			return nil, fmt.Errorf("uniswap V3 log does not belong to pool and block")
		}
		update, err := decodeLog(event)
		if err != nil {
			return nil, err
		}
		updates = append(updates, update)
	}
	return NewBlockUpdate(updates...)
}

func (a *Adapter) loadCoveredState(ctx context.Context, network evm.Network, block evm.BlockReference, sqrtPrice *big.Int, tick int32, liquidity *big.Int, fee uint32, spacing int32) (StateUpdate, error) {
	currentWord := int32(floorDiv(floorDiv(int64(tick), int64(spacing)), 256))
	loaded := make(map[int32][]Tick)
	if err := a.loadWord(ctx, network, block, currentWord, spacing, loaded); err != nil {
		return StateUpdate{}, err
	}
	minWord, maxWord := currentWord, currentWord
	if err := a.expandForProbes(ctx, network, block, sqrtPrice, tick, liquidity, fee, spacing, loaded, &minWord, &maxWord); err != nil {
		return StateUpdate{}, err
	}
	coverage, _ := NewTickCoverage(minWord, maxWord)
	return NewCoveredStateUpdate(sqrtPrice, tick, liquidity, fee, spacing, flattenTicks(loaded), coverage)
}

func (a *Adapter) expandForProbes(
	ctx context.Context,
	network evm.Network,
	block evm.BlockReference,
	sqrtPrice *big.Int,
	tick int32,
	liquidity *big.Int,
	fee uint32,
	spacing int32,
	loaded map[int32][]Tick,
	minWord *int32,
	maxWord *int32,
) error {
	for _, probe := range a.probes {
		for {
			coverage, _ := NewTickCoverage(*minWord, *maxWord)
			state := Snapshot{
				schemaVersion: snapshotSchemaVersion, sqrtPriceX96: cloneInt(sqrtPrice), tick: tick,
				liquidity: cloneInt(liquidity), feePips: fee, tickSpacing: spacing,
				ticks: flattenTicks(loaded), coverage: coverage,
			}
			_, quoteErr := quoteExactInput(state, probe.ZeroForOne, probe.AmountIn)
			if quoteErr == nil {
				break
			}
			if !errors.Is(quoteErr, ErrInsufficientTickCoverage) {
				return fmt.Errorf("validate Uniswap V3 coverage probe: %w", quoteErr)
			}
			if len(loaded) >= a.maxWords {
				return fmt.Errorf("%w: maximum of %d words reached", ErrInsufficientTickCoverage, a.maxWords)
			}
			word := *maxWord + 1
			if probe.ZeroForOne {
				word = *minWord - 1
			}
			if err := a.loadWord(ctx, network, block, word, spacing, loaded); err != nil {
				return err
			}
			if probe.ZeroForOne {
				*minWord = word
			} else {
				*maxWord = word
			}
		}
		if len(loaded) < a.maxWords {
			guard := *maxWord + 1
			if probe.ZeroForOne {
				guard = *minWord - 1
			}
			if _, exists := loaded[guard]; !exists {
				if err := a.loadWord(ctx, network, block, guard, spacing, loaded); err != nil {
					return err
				}
				if probe.ZeroForOne {
					*minWord = guard
				} else {
					*maxWord = guard
				}
			}
		}
	}
	return nil
}

func (a *Adapter) loadWord(ctx context.Context, network evm.Network, block evm.BlockReference, word int32, spacing int32, loaded map[int32][]Tick) error {
	if _, exists := loaded[word]; exists {
		return nil
	}
	if word < -32768 || word > 32767 {
		return fmt.Errorf("uniswap V3 bitmap word %d exceeds int16", word)
	}
	values, err := a.call(ctx, network, block, "tickBitmap", int16(word))
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
			index := (int64(word)*256 + int64(bit)) * int64(spacing)
			if index < int64(MinTick) || index > int64(MaxTick) {
				return fmt.Errorf("initialized tick %d is outside Uniswap V3 bounds", index)
			}
			indices = append(indices, int32(index))
		}
	}
	ticks := make([]Tick, len(indices))
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	for position, index := range indices {
		wg.Add(1)
		go func(position int, index int32) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			values, callErr := a.call(ctx, network, block, "ticks", big.NewInt(int64(index)))
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
			initialized, tickErr := NewTick(index, gross, net)
			if tickErr != nil {
				setFirstError(&errMu, &firstErr, tickErr)
				return
			}
			ticks[position] = initialized
		}(position, index)
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	loaded[word] = ticks
	return nil
}

func setFirstError(mu *sync.Mutex, target *error, candidate error) {
	mu.Lock()
	defer mu.Unlock()
	if *target == nil {
		*target = candidate
	}
}

func (a *Adapter) call(ctx context.Context, network evm.Network, block evm.BlockReference, method string, arguments ...any) ([]any, error) {
	input, err := poolABI.Pack(method, arguments...)
	if err != nil {
		return nil, fmt.Errorf("encode Uniswap V3 %s call: %w", method, err)
	}
	result, err := network.CallContract(ctx, block, geth.CallMsg{To: &a.pool, Data: input})
	if err != nil {
		return nil, err
	}
	values, err := poolABI.Unpack(method, result)
	if err != nil {
		return nil, fmt.Errorf("decode Uniswap V3 %s response: %w", method, err)
	}
	return values, nil
}

func decodeLog(event types.Log) (market.EventData, error) {
	if len(event.Topics) == 0 {
		return nil, fmt.Errorf("uniswap V3 log has no signature topic")
	}
	switch event.Topics[0] {
	case poolABI.Events["Initialize"].ID:
		if len(event.Topics) != 1 {
			return nil, fmt.Errorf("invalid Uniswap V3 Initialize topics")
		}
		values, err := poolABI.Events["Initialize"].Inputs.NonIndexed().Unpack(event.Data)
		if err != nil {
			return nil, fmt.Errorf("decode Uniswap V3 Initialize: %w", err)
		}
		price, err := bigValue(values[0], "initialize sqrt price")
		if err != nil {
			return nil, err
		}
		tick, err := int32Value(values[1], "initialize tick")
		if err != nil {
			return nil, err
		}
		return NewInitializeUpdate(price, tick)
	case poolABI.Events["Swap"].ID:
		if len(event.Topics) != 3 {
			return nil, fmt.Errorf("invalid Uniswap V3 Swap topics")
		}
		values, err := poolABI.Events["Swap"].Inputs.NonIndexed().Unpack(event.Data)
		if err != nil {
			return nil, fmt.Errorf("decode Uniswap V3 Swap: %w", err)
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
		return NewSwapUpdate(price, tick, liquidity)
	case poolABI.Events["Mint"].ID:
		if len(event.Topics) != 4 {
			return nil, fmt.Errorf("invalid Uniswap V3 Mint topics")
		}
		values, err := poolABI.Events["Mint"].Inputs.NonIndexed().Unpack(event.Data)
		if err != nil {
			return nil, fmt.Errorf("decode Uniswap V3 Mint: %w", err)
		}
		amount, err := bigValue(values[1], "mint liquidity")
		if err != nil {
			return nil, err
		}
		return liquidityLogUpdate(event, amount)
	case poolABI.Events["Burn"].ID:
		if len(event.Topics) != 4 {
			return nil, fmt.Errorf("invalid Uniswap V3 Burn topics")
		}
		values, err := poolABI.Events["Burn"].Inputs.NonIndexed().Unpack(event.Data)
		if err != nil {
			return nil, fmt.Errorf("decode Uniswap V3 Burn: %w", err)
		}
		amount, err := bigValue(values[0], "burn liquidity")
		if err != nil {
			return nil, err
		}
		return liquidityLogUpdate(event, new(big.Int).Neg(amount))
	default:
		return nil, fmt.Errorf("unsupported Uniswap V3 event topic %s", event.Topics[0])
	}
}

func liquidityLogUpdate(event types.Log, delta *big.Int) (LiquidityUpdate, error) {
	lower, err := signedTopicInt32(event.Topics[2])
	if err != nil {
		return LiquidityUpdate{}, fmt.Errorf("decode lower tick: %w", err)
	}
	upper, err := signedTopicInt32(event.Topics[3])
	if err != nil {
		return LiquidityUpdate{}, fmt.Errorf("decode upper tick: %w", err)
	}
	return NewLiquidityUpdate(lower, upper, delta)
}

func signedTopicInt32(topic common.Hash) (int32, error) {
	value := new(big.Int).SetBytes(topic[:])
	if value.Bit(255) == 1 {
		value.Sub(value, new(big.Int).Lsh(big.NewInt(1), 256))
	}
	if !value.IsInt64() || value.Int64() < int64(MinTick) || value.Int64() > int64(MaxTick) {
		return 0, fmt.Errorf("signed topic is outside int24 tick bounds")
	}
	return int32(value.Int64()), nil
}

func bigValue(value any, name string) (*big.Int, error) {
	switch typed := value.(type) {
	case *big.Int:
		return cloneInt(typed), nil
	case uint8:
		return big.NewInt(int64(typed)), nil
	case uint16:
		return big.NewInt(int64(typed)), nil
	case uint32:
		return big.NewInt(int64(typed)), nil
	case uint64:
		return new(big.Int).SetUint64(typed), nil
	default:
		return nil, fmt.Errorf("invalid Uniswap V3 %s type %T", name, value)
	}
}

func uint32Value(value any, name string) (uint32, error) {
	integer, err := bigValue(value, name)
	if err != nil {
		return 0, err
	}
	if !integer.IsUint64() || integer.Uint64() > uint64(^uint32(0)) {
		return 0, fmt.Errorf("uniswap V3 %s exceeds uint32", name)
	}
	return uint32(integer.Uint64()), nil
}

func int32Value(value any, name string) (int32, error) {
	integer, err := bigValue(value, name)
	if err != nil {
		return 0, err
	}
	if !integer.IsInt64() || integer.Int64() < -1<<31 || integer.Int64() > 1<<31-1 {
		return 0, fmt.Errorf("uniswap V3 %s exceeds int32", name)
	}
	return int32(integer.Int64()), nil
}

func flattenTicks(words map[int32][]Tick) []Tick {
	var result []Tick
	for _, initialized := range words {
		result = append(result, initialized...)
	}
	return normalizedTicks(result)
}

func mustParseABI(definition string) abi.ABI {
	parsed, err := abi.JSON(strings.NewReader(definition))
	if err != nil {
		panic(err)
	}
	return parsed
}
