package orcawhirlpool

import (
	"fmt"
	"math/big"
)

const (
	whirlpoolQ64Bits  = 64
	whirlpoolMinTick  = -443636
	whirlpoolMaxTick  = 443636
	whirlpoolMinPrice = "4295048016"
	whirlpoolMaxPrice = "79226673515401279992447579055"
)

type swapStep struct {
	in, out, fee, next *big.Int
}

func swap(state Snapshot, aToB bool, amount *big.Int, exactInput bool) (*big.Int, *big.Int, error) {
	if amount == nil || amount.Sign() <= 0 || state.liquidity.Sign() <= 0 || state.feeRate >= feeRatePrecision {
		return nil, nil, fmt.Errorf("invalid whirlpool swap request")
	}
	remaining := new(big.Int).Set(amount)
	calculated, fees := new(big.Int), new(big.Int)
	price := clone(state.sqrtPriceX64)
	liquidity := clone(state.liquidity)
	tick := state.tick
	for remaining.Sign() > 0 {
		nextTick, target, hasTick := nextInitializedTick(state.ticks, tick, aToB)
		if !hasTick {
			if state.fullCoverage {
				if aToB {
					target, _ = new(big.Int).SetString(whirlpoolMinPrice, 10)
				} else {
					target, _ = new(big.Int).SetString(whirlpoolMaxPrice, 10)
				}
			} else {
				if aToB {
					target, _ = sqrtPriceFromTick(state.minTick)
				} else {
					// Tick arrays cover the interval through the tick after their
					// last stored index. B->A can therefore move to that upper
					// boundary even when the last initialized tick is empty.
					target, _ = sqrtPriceFromTick(state.maxTick + state.tickSpacing)
				}
			}
		}
		if target == nil || target.Sign() <= 0 || target.Cmp(price) == 0 {
			if hasTick {
				nextLiquidity, crossErr := crossLiquidity(liquidity, state.ticks, nextTick, aToB)
				if crossErr != nil {
					return nil, nil, crossErr
				}
				liquidity = nextLiquidity
				if aToB {
					tick = nextTick - 1
				} else {
					tick = nextTick
				}
				continue
			}
			return nil, nil, fmt.Errorf("whirlpool has insufficient tick coverage")
		}
		step, err := computeStep(remaining, state.feeRate, liquidity, price, target, exactInput, aToB)
		if err != nil {
			return nil, nil, err
		}
		if exactInput {
			used := new(big.Int).Add(step.in, step.fee)
			remaining.Sub(remaining, used)
			calculated.Add(calculated, step.out)
		} else {
			remaining.Sub(remaining, step.out)
			calculated.Add(calculated, step.in)
			calculated.Add(calculated, step.fee)
		}
		fees.Add(fees, step.fee)
		price.Set(step.next)
		if step.next.Cmp(target) == 0 {
			if hasTick {
				nextLiquidity, crossErr := crossLiquidity(liquidity, state.ticks, nextTick, aToB)
				if crossErr != nil {
					return nil, nil, crossErr
				}
				liquidity = nextLiquidity
				if aToB {
					tick = nextTick - 1
				} else {
					tick = nextTick
				}
				continue
			}
		}
		if remaining.Sign() > 0 {
			return nil, nil, fmt.Errorf("whirlpool has insufficient tick coverage")
		}
	}
	if !exactInput && calculated.Sign() == 0 {
		return nil, nil, fmt.Errorf("whirlpool output rounds to zero")
	}
	return calculated, fees, nil
}

func crossLiquidity(liquidity *big.Int, ticks []Tick, index int32, aToB bool) (*big.Int, error) {
	for _, tick := range ticks {
		if tick.index != index {
			continue
		}
		next := new(big.Int).Set(liquidity)
		if aToB {
			next.Sub(next, tick.liquidityNet)
		} else {
			next.Add(next, tick.liquidityNet)
		}
		if next.Sign() < 0 {
			return nil, fmt.Errorf("whirlpool liquidity underflow at tick %d", index)
		}
		return next, nil
	}
	return liquidity, nil
}

func computeStep(remaining *big.Int, feeRate uint32, liquidity, current, target *big.Int, exactInput, aToB bool) (swapStep, error) {
	if remaining.Sign() <= 0 || liquidity.Sign() <= 0 || current.Sign() <= 0 || target.Sign() <= 0 || feeRate >= feeRatePrecision {
		return swapStep{}, fmt.Errorf("invalid Whirlpool swap step")
	}
	if aToB && target.Cmp(current) > 0 || !aToB && target.Cmp(current) < 0 {
		return swapStep{}, fmt.Errorf("whirlpool target moves in the wrong direction")
	}
	feeBase := new(big.Int).SetUint64(uint64(feeRatePrecision))
	netFactor := new(big.Int).SetUint64(uint64(feeRatePrecision - feeRate))
	amountCalc := new(big.Int).Set(remaining)
	if exactInput {
		amountCalc.Mul(amountCalc, netFactor).Quo(amountCalc, feeBase)
	}
	initialFixed := fixedDelta(current, target, liquidity, exactInput, aToB)
	next := clone(target)
	if initialFixed.Cmp(amountCalc) > 0 {
		var err error
		next, err = nextSqrtPrice(current, liquidity, amountCalc, exactInput, aToB)
		if err != nil {
			return swapStep{}, err
		}
	}
	isMax := next.Cmp(target) == 0
	unfixed := unfixedDelta(current, next, liquidity, exactInput, aToB)
	fixed := initialFixed
	if !isMax {
		fixed = fixedDelta(current, next, liquidity, exactInput, aToB)
	}
	in, out := fixed, unfixed
	if !exactInput {
		in, out = unfixed, fixed
		if out.Cmp(remaining) > 0 {
			out = new(big.Int).Set(remaining)
		}
	}
	fee := new(big.Int)
	if exactInput && !isMax {
		fee.Sub(remaining, in)
	} else {
		fee.Mul(in, new(big.Int).SetUint64(uint64(feeRate)))
		fee.Add(fee, new(big.Int).Sub(netFactor, big.NewInt(1)))
		fee.Quo(fee, netFactor)
	}
	return swapStep{in: in, out: out, fee: fee, next: next}, nil
}

func fixedDelta(current, target, liquidity *big.Int, exactInput, aToB bool) *big.Int {
	if aToB == exactInput {
		lower, upper := ordered(current, target)
		return amountDeltaA(lower, upper, liquidity, exactInput)
	}
	lower, upper := ordered(current, target)
	return amountDeltaB(lower, upper, liquidity, exactInput)
}

func unfixedDelta(current, target, liquidity *big.Int, exactInput, aToB bool) *big.Int {
	if aToB == exactInput {
		lower, upper := ordered(current, target)
		return amountDeltaB(lower, upper, liquidity, !exactInput)
	}
	lower, upper := ordered(current, target)
	return amountDeltaA(lower, upper, liquidity, !exactInput)
}

func ordered(left, right *big.Int) (*big.Int, *big.Int) {
	if left.Cmp(right) <= 0 {
		return left, right
	}
	return right, left
}

// nextInitializedTick is kept in this file with the swap arithmetic so the
// direction and inclusivity rules remain adjacent to the official algorithm.
func nextInitializedTick(ticks []Tick, current int32, aToB bool) (int32, *big.Int, bool) {
	var candidate *Tick
	for i := range ticks {
		tick := &ticks[i]
		if (aToB && tick.index <= current) || (!aToB && tick.index > current) {
			if candidate == nil || (aToB && tick.index > candidate.index) || (!aToB && tick.index < candidate.index) {
				candidate = tick
			}
		}
	}
	if candidate == nil {
		return 0, nil, false
	}
	price, err := sqrtPriceFromTick(candidate.index)
	if err != nil {
		return 0, nil, false
	}
	return candidate.index, price, true
}

func sqrtPriceFromTick(tick int32) (*big.Int, error) {
	if tick < whirlpoolMinTick || tick > whirlpoolMaxTick {
		return nil, fmt.Errorf("tick %d outside Whirlpool bounds", tick)
	}
	if tick == 0 {
		return q64Int(), nil
	}
	positive := tick > 0
	value := tick
	if value < 0 {
		value = -value
	}
	var ratio *big.Int
	if positive {
		positiveFactors := []string{
			"79232123823359799118286999567", "79236085330515764027303304731", "79244008939048815603706035061",
			"79259858533276714757314932305", "79291567232598584799939703904", "79355022692464371645785046466",
			"79482085999252804386437311141", "79736823300114093921829183326", "80248749790819932309965073892",
			"81282483887344747381513967011", "83390072131320151908154831281", "87770609709833776024991924138",
			"97234110755111693312479820773", "119332217159966728226237229890", "179736315981702064433883588727",
			"407748233172238350107850275304", "2098478828474011932436660412517", "55581415166113811149459800483533",
			"38992368544603139932233054999993551",
		}
		ratio, _ = new(big.Int).SetString("79228162514264337593543950336", 10)
		if value&1 != 0 {
			ratio.SetString(positiveFactors[0], 10)
		}
		for bit := 1; bit < len(positiveFactors); bit++ {
			if value&(1<<bit) != 0 {
				factor, _ := new(big.Int).SetString(positiveFactors[bit], 10)
				ratio.Mul(ratio, factor).Rsh(ratio, 96)
			}
		}
		return ratio.Rsh(ratio, 32), nil
	}

	negativeFactors := []string{
		"18445821805675392311", "18444899583751176498", "18443055278223354162", "18439367220385604838",
		"18431993317065449817", "18417254355718160513", "18387811781193591352", "18329067761203520168",
		"18212142134806087854", "17980523815641551639", "17526086738831147013", "16651378430235024244",
		"15030750278693429944", "12247334978882834399", "8131365268884726200", "3584323654723342297",
		"696457651847595233", "26294789957452057", "37481735321082",
	}
	ratio = q64Int()
	if value&1 != 0 {
		ratio.SetString(negativeFactors[0], 10)
	}
	for bit := 1; bit < len(negativeFactors); bit++ {
		if value&(1<<bit) != 0 {
			factor, _ := new(big.Int).SetString(negativeFactors[bit], 10)
			ratio.Mul(ratio, factor).Rsh(ratio, 64)
		}
	}
	return ratio, nil
}

func divRound(numerator, denominator *big.Int, roundUp bool) *big.Int {
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(numerator, denominator, remainder)
	if roundUp && remainder.Sign() != 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	return quotient
}

func amountDeltaA(lower, upper, liquidity *big.Int, roundUp bool) *big.Int {
	diff := new(big.Int).Sub(upper, lower)
	numerator := new(big.Int).Mul(liquidity, diff)
	numerator.Lsh(numerator, 64)
	denominator := new(big.Int).Mul(upper, lower)
	return divRound(numerator, denominator, roundUp)
}

func amountDeltaB(lower, upper, liquidity *big.Int, roundUp bool) *big.Int {
	diff := new(big.Int).Sub(upper, lower)
	numerator := new(big.Int).Mul(liquidity, diff)
	return divRound(numerator, q64Int(), roundUp)
}

func nextSqrtPrice(price, liquidity, amount *big.Int, exactInput, aToB bool) (*big.Int, error) {
	if amount.Sign() == 0 {
		return clone(price), nil
	}
	if exactInput == aToB {
		numerator := new(big.Int).Mul(liquidity, price)
		numerator.Lsh(numerator, 64)
		product := new(big.Int).Mul(price, amount)
		denominator := new(big.Int).Set(liquidity)
		denominator.Lsh(denominator, 64)
		if exactInput {
			denominator.Add(denominator, product)
		} else {
			denominator.Sub(denominator, product)
			if denominator.Sign() <= 0 {
				return nil, fmt.Errorf("whirlpool output exceeds liquidity")
			}
		}
		return divRound(numerator, denominator, true), nil
	}
	delta := new(big.Int).Mul(amount, q64Int())
	delta = divRound(delta, liquidity, !exactInput)
	if exactInput {
		return new(big.Int).Add(price, delta), nil
	}
	if price.Cmp(delta) < 0 {
		return nil, fmt.Errorf("whirlpool price underflow")
	}
	return new(big.Int).Sub(price, delta), nil
}

func q64Int() *big.Int { return new(big.Int).Lsh(big.NewInt(1), whirlpoolQ64Bits) }
