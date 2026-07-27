package saga

import (
	"fmt"
	"math/big"
)

// BalanceDelta is the amount a serial hop actually produced. Contracts and
// adapters use the delta rather than a total balance so pre-existing dust can
// never become input to the next hop.
func BalanceDelta(before, after *big.Int) (*big.Int, error) {
	if before == nil || after == nil || before.Sign() < 0 || after.Cmp(before) < 0 {
		return nil, fmt.Errorf("hop balances must be non-negative and monotonic")
	}
	return new(big.Int).Sub(after, before), nil
}

// SplitUnits applies basis-point weights with floor rounding. The final branch
// receives the exact remainder, so the complete actual input is consumed.
func SplitUnits(total *big.Int, weightsBPS []uint16) ([]*big.Int, error) {
	if total == nil || total.Sign() <= 0 || len(weightsBPS) == 0 {
		return nil, fmt.Errorf("split requires positive input and weights")
	}
	var sum uint64
	for _, weight := range weightsBPS {
		if weight == 0 {
			return nil, fmt.Errorf("split weights must be positive")
		}
		sum += uint64(weight)
	}
	if sum != 10_000 {
		return nil, fmt.Errorf("split weights must total 10000 basis points")
	}
	result := make([]*big.Int, len(weightsBPS))
	allocated := new(big.Int)
	for index := 0; index < len(weightsBPS)-1; index++ {
		result[index] = new(big.Int).Mul(total, new(big.Int).SetUint64(uint64(weightsBPS[index])))
		result[index].Quo(result[index], big.NewInt(10_000))
		allocated.Add(allocated, result[index])
	}
	result[len(result)-1] = new(big.Int).Sub(total, allocated)
	return result, nil
}

// SplitByWeights applies arbitrary positive integer optimizer weights. It
// preserves more precision than basis points and gives the final branch the
// exact remainder.
func SplitByWeights(total *big.Int, weights []*big.Int) ([]*big.Int, error) {
	if total == nil || total.Sign() <= 0 || len(weights) == 0 {
		return nil, fmt.Errorf("split requires positive input and weights")
	}
	weightTotal := new(big.Int)
	for _, weight := range weights {
		if weight == nil || weight.Sign() <= 0 {
			return nil, fmt.Errorf("split weights must be positive")
		}
		weightTotal.Add(weightTotal, weight)
	}
	result := make([]*big.Int, len(weights))
	allocated := new(big.Int)
	for index := 0; index < len(weights)-1; index++ {
		result[index] = new(big.Int).Mul(total, weights[index])
		result[index].Quo(result[index], weightTotal)
		allocated.Add(allocated, result[index])
	}
	result[len(result)-1] = new(big.Int).Sub(total, allocated)
	return result, nil
}
