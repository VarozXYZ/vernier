package uniswapv3

import (
	"fmt"
	"math/big"
	"sort"
)

const (
	MinTick        int32  = -887272
	MaxTick        int32  = 887272
	maxTickSpacing int32  = 16383
	feeDenominator uint32 = 1_000_000
)

var (
	one          = big.NewInt(1)
	q32          = new(big.Int).Lsh(big.NewInt(1), 32)
	q96          = new(big.Int).Lsh(big.NewInt(1), 96)
	q128         = new(big.Int).Lsh(big.NewInt(1), 128)
	maxUint256   = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), one)
	maxUint128   = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), one)
	maxInt128    = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 127), one)
	minInt128    = new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127))
	minSqrtRatio = big.NewInt(4295128739)
	maxSqrtRatio = mustDecimal("1461446703485210103287273052203988822378723970342")
	tickFactors  = mustHexFactors([]string{
		"fffcb933bd6fad37aa2d162d1a594001", "fff97272373d413259a46990580e213a",
		"fff2e50f5f656932ef12357cf3c7fdcc", "ffe5caca7e10e4e61c3624eaa0941cd0",
		"ffcb9843d60f6159c9db58835c926644", "ff973b41fa98c081472e6896dfb254c0",
		"ff2ea16466c96a3843ec78b326b52861", "fe5dee046a99a2a811c461f1969c3053",
		"fcbe86c7900a88aedcffc83b479aa3a4", "f987a7253ac413176f2b074cf7815e54",
		"f3392b0822b70005940c7a398e4b70f3", "e7159475a2c29b7443b29c7fa6e889d9",
		"d097f3bdfd2022b8845ad8f792aa5825", "a9f746462d870fdf8a65dc1f90e061e5",
		"70d869a156d2a1b890bb3df62baf32f7", "31be135f97d08fd981231505542fcfa6",
		"9aa508b5b7a84e1c677de54f3e99bc9", "5d6af8dedb81196699c329225ee604",
		"2216e584f5fa1ea926041bedfe98", "48a170391f7dc42444e8fa2",
	})
)

// SqrtRatioAtTick returns sqrt(1.0001^tick) encoded as Q64.96 with
// Uniswap-compatible rounding.
func SqrtRatioAtTick(tick int32) (*big.Int, error) {
	if tick < MinTick || tick > MaxTick {
		return nil, fmt.Errorf("tick %d outside Uniswap V3 bounds", tick)
	}
	absTick := int64(tick)
	if absTick < 0 {
		absTick = -absTick
	}
	ratio := cloneInt(q128)
	for bit, factor := range tickFactors {
		if uint64(absTick)&(uint64(1)<<bit) != 0 {
			ratio.Rsh(new(big.Int).Mul(ratio, factor), 128)
		}
	}
	if tick > 0 {
		ratio.Quo(maxUint256, ratio)
	}
	result, remainder := new(big.Int).QuoRem(ratio, q32, new(big.Int))
	if remainder.Sign() != 0 {
		result.Add(result, one)
	}
	return result, nil
}

type swapResult struct {
	amountOut    *big.Int
	fee          *big.Int
	ticksCrossed int
}

func quoteExactInput(state Snapshot, zeroForOne bool, amountIn *big.Int) (swapResult, error) {
	if amountIn == nil || amountIn.Sign() <= 0 || amountIn.BitLen() > 256 {
		return swapResult{}, fmt.Errorf("amount in must fit uint256 and be positive")
	}
	remaining := cloneInt(amountIn)
	output := new(big.Int)
	fees := new(big.Int)
	ticksCrossed := 0
	sqrtPrice := state.SqrtPriceX96()
	currentTick := state.tick
	liquidity := state.Liquidity()
	ticks := state.Ticks()
	limit := new(big.Int).Sub(maxSqrtRatio, one)
	if zeroForOne {
		limit = new(big.Int).Add(minSqrtRatio, one)
	}

	maxSteps := int((int64(MaxTick)-int64(MinTick))/(int64(state.tickSpacing)*256)) + len(ticks) + 4
	for steps := 0; remaining.Sign() > 0 && sqrtPrice.Cmp(limit) != 0; steps++ {
		if steps > maxSteps {
			return swapResult{}, fmt.Errorf("uniswap V3 quote exceeded tick traversal bound")
		}
		if liquidity.Sign() <= 0 {
			return swapResult{}, fmt.Errorf("no active Uniswap V3 liquidity")
		}
		word := traversalWord(currentTick, state.tickSpacing, zeroForOne)
		if !state.coverage.Contains(word) {
			return swapResult{}, fmt.Errorf("%w: word %d", ErrInsufficientTickCoverage, word)
		}
		nextTick, initialized := nextInitializedTickWithinOneWord(ticks, currentTick, state.tickSpacing, zeroForOne)
		if nextTick < MinTick {
			nextTick = MinTick
		}
		if nextTick > MaxTick {
			nextTick = MaxTick
		}
		nextPrice, err := SqrtRatioAtTick(nextTick)
		if err != nil {
			return swapResult{}, err
		}
		target := nextPrice
		if zeroForOne && target.Cmp(limit) < 0 || !zeroForOne && target.Cmp(limit) > 0 {
			target = limit
		}
		step, err := computeSwapStep(sqrtPrice, target, liquidity, remaining, state.feePips)
		if err != nil {
			return swapResult{}, err
		}
		sqrtPrice = step.sqrtPrice
		remaining.Sub(remaining, new(big.Int).Add(step.amountIn, step.fee))
		output.Add(output, step.amountOut)
		fees.Add(fees, step.fee)

		if sqrtPrice.Cmp(nextPrice) == 0 {
			if initialized {
				tick := findTick(ticks, nextTick)
				liquidityDelta := tick.LiquidityNet()
				if zeroForOne {
					liquidityDelta.Neg(liquidityDelta)
				}
				liquidity.Add(liquidity, liquidityDelta)
				if liquidity.Sign() < 0 || liquidity.BitLen() > 128 {
					return swapResult{}, fmt.Errorf("tick %d produces invalid active liquidity", nextTick)
				}
				ticksCrossed++
			}
			if zeroForOne {
				currentTick = nextTick - 1
			} else {
				currentTick = nextTick
			}
		}
	}
	if remaining.Sign() != 0 {
		return swapResult{}, fmt.Errorf("price limit reached before consuming exact input")
	}
	if output.Sign() <= 0 {
		return swapResult{}, fmt.Errorf("quote output rounds to zero")
	}
	return swapResult{amountOut: output, fee: fees, ticksCrossed: ticksCrossed}, nil
}

func traversalWord(current, spacing int32, zeroForOne bool) int32 {
	compressed := floorDiv(int64(current), int64(spacing))
	if !zeroForOne {
		compressed++
	}
	return int32(floorDiv(compressed, 256))
}

type swapStep struct {
	sqrtPrice *big.Int
	amountIn  *big.Int
	amountOut *big.Int
	fee       *big.Int
}

func computeSwapStep(current, target, liquidity, remaining *big.Int, feePips uint32) (swapStep, error) {
	if current.Sign() <= 0 || target.Sign() <= 0 || liquidity.Sign() <= 0 || remaining.Sign() <= 0 || feePips >= feeDenominator {
		return swapStep{}, fmt.Errorf("invalid Uniswap V3 swap step")
	}
	zeroForOne := current.Cmp(target) >= 0
	feeComplement := big.NewInt(int64(feeDenominator - feePips))
	lessFee := mulDiv(remaining, feeComplement, big.NewInt(int64(feeDenominator)))
	var required *big.Int
	if zeroForOne {
		required = amount0Delta(target, current, liquidity, true)
	} else {
		required = amount1Delta(current, target, liquidity, true)
	}
	reachesTarget := lessFee.Cmp(required) >= 0
	next := cloneInt(target)
	if !reachesTarget {
		next = nextSqrtPriceFromInput(current, liquidity, lessFee, zeroForOne)
	}
	var amountIn, amountOut *big.Int
	if zeroForOne {
		amountIn = amount0Delta(next, current, liquidity, true)
		amountOut = amount1Delta(next, current, liquidity, false)
	} else {
		amountIn = amount1Delta(current, next, liquidity, true)
		amountOut = amount0Delta(current, next, liquidity, false)
	}
	fee := new(big.Int).Sub(remaining, amountIn)
	if reachesTarget {
		fee = mulDivRoundingUp(amountIn, big.NewInt(int64(feePips)), feeComplement)
	}
	return swapStep{sqrtPrice: next, amountIn: amountIn, amountOut: amountOut, fee: fee}, nil
}

func nextSqrtPriceFromInput(current, liquidity, amount *big.Int, zeroForOne bool) *big.Int {
	if amount.Sign() == 0 {
		return cloneInt(current)
	}
	if zeroForOne {
		numerator := new(big.Int).Lsh(cloneInt(liquidity), 96)
		denominator := new(big.Int).Add(numerator, new(big.Int).Mul(amount, current))
		return mulDivRoundingUp(numerator, current, denominator)
	}
	return new(big.Int).Add(current, mulDiv(amount, q96, liquidity))
}

func amount0Delta(a, b, liquidity *big.Int, roundUp bool) *big.Int {
	if a.Cmp(b) > 0 {
		a, b = b, a
	}
	numerator := new(big.Int).Lsh(cloneInt(liquidity), 96)
	delta := new(big.Int).Sub(b, a)
	if roundUp {
		return divRoundingUp(mulDivRoundingUp(numerator, delta, b), a)
	}
	return new(big.Int).Quo(mulDiv(numerator, delta, b), a)
}

func amount1Delta(a, b, liquidity *big.Int, roundUp bool) *big.Int {
	if a.Cmp(b) > 0 {
		a, b = b, a
	}
	delta := new(big.Int).Sub(b, a)
	if roundUp {
		return mulDivRoundingUp(liquidity, delta, q96)
	}
	return mulDiv(liquidity, delta, q96)
}

func nextInitializedTickWithinOneWord(ticks []Tick, current, spacing int32, zeroForOne bool) (int32, bool) {
	compressed := floorDiv(int64(current), int64(spacing))
	if zeroForOne {
		wordStart := floorDiv(compressed, 256) * 256
		index := sort.Search(len(ticks), func(i int) bool { return int64(ticks[i].index) > compressed*int64(spacing) }) - 1
		if index >= 0 && int64(ticks[index].index) >= wordStart*int64(spacing) {
			return ticks[index].index, true
		}
		return int32(wordStart * int64(spacing)), false
	}
	start := compressed + 1
	wordStart := floorDiv(start, 256) * 256
	wordEnd := wordStart + 255
	index := sort.Search(len(ticks), func(i int) bool { return int64(ticks[i].index) >= start*int64(spacing) })
	if index < len(ticks) && int64(ticks[index].index) <= wordEnd*int64(spacing) {
		return ticks[index].index, true
	}
	return int32(wordEnd * int64(spacing)), false
}

func floorDiv(value, divisor int64) int64 {
	quotient := value / divisor
	if value < 0 && value%divisor != 0 {
		quotient--
	}
	return quotient
}

func findTick(ticks []Tick, index int32) Tick {
	position := sort.Search(len(ticks), func(i int) bool { return ticks[i].index >= index })
	return ticks[position]
}

func mulDiv(a, b, denominator *big.Int) *big.Int {
	return new(big.Int).Quo(new(big.Int).Mul(a, b), denominator)
}

func mulDivRoundingUp(a, b, denominator *big.Int) *big.Int {
	return divRoundingUp(new(big.Int).Mul(a, b), denominator)
}

func divRoundingUp(numerator, denominator *big.Int) *big.Int {
	quotient, remainder := new(big.Int).QuoRem(numerator, denominator, new(big.Int))
	if remainder.Sign() != 0 {
		quotient.Add(quotient, one)
	}
	return quotient
}

func mustDecimal(value string) *big.Int {
	parsed, ok := new(big.Int).SetString(value, 10)
	if !ok {
		panic("invalid decimal constant")
	}
	return parsed
}

func mustHexFactors(values []string) []*big.Int {
	result := make([]*big.Int, len(values))
	for index, value := range values {
		parsed, ok := new(big.Int).SetString(value, 16)
		if !ok {
			panic("invalid hexadecimal constant")
		}
		result[index] = parsed
	}
	return result
}
