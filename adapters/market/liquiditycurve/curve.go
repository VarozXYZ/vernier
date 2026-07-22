// Package liquiditycurve contains the small integer engine shared by
// concentrated-liquidity adapters. Protocol packages provide ordered active
// segments; this package does not know how those segments are discovered.
package liquiditycurve

import (
	"fmt"
	"math/big"
)

const BasisPoints uint64 = 10_000

// FeeRatePrecision is the denominator used by protocols that represent a fee
// as a fraction of the gross input amount. Keeping the native rate avoids
// rounding a protocol fee down/up to basis points before quoting.
const FeeRatePrecision uint64 = 1_000_000_000

type Segment struct {
	In  *big.Int
	Out *big.Int
}

func ExactInput(segments []Segment, amount *big.Int, feeBPS uint16) (output, fee *big.Int, err error) {
	return ExactInputRate(segments, amount, uint64(feeBPS)*FeeRatePrecision/BasisPoints)
}

// ExactInputRate quotes a piecewise-linear curve with a fee charged on the
// gross input. Rounding follows the common on-chain rule: fee is ceil(gross *
// rate / precision), hence net input is gross-fee.
func ExactInputRate(segments []Segment, amount *big.Int, feeRate uint64) (output, fee *big.Int, err error) {
	if amount == nil || amount.Sign() <= 0 || feeRate >= FeeRatePrecision {
		return nil, nil, fmt.Errorf("amount must be positive and fee valid")
	}
	denominator := new(big.Int).SetUint64(FeeRatePrecision)
	netFactor := new(big.Int).SetUint64(FeeRatePrecision - feeRate)
	remaining := new(big.Int).Set(amount)
	output, fee = new(big.Int), new(big.Int)
	for _, segment := range segments {
		if remaining.Sign() == 0 {
			break
		}
		if segment.In == nil || segment.Out == nil || segment.In.Sign() <= 0 || segment.Out.Sign() <= 0 {
			return nil, nil, fmt.Errorf("segment reserves must be positive")
		}
		gross := new(big.Int).Set(remaining)
		afterFee := new(big.Int).Mul(gross, netFactor)
		afterFee.Quo(afterFee, denominator)
		if afterFee.Sign() == 0 {
			break
		}
		available := new(big.Int).Set(afterFee)
		if available.Cmp(segment.In) > 0 {
			available.Set(segment.In)
			// Gross input needed to consume this segment, rounded up.
			gross.Mul(available, denominator)
			gross.Add(gross, new(big.Int).Sub(new(big.Int).Set(netFactor), big.NewInt(1)))
			gross.Quo(gross, netFactor)
			afterFee.Set(available)
		}
		segmentOutput := new(big.Int).Mul(available, segment.Out)
		segmentOutput.Quo(segmentOutput, segment.In)
		if segmentOutput.Sign() == 0 {
			return nil, nil, fmt.Errorf("quote output rounds to zero")
		}
		output.Add(output, segmentOutput)
		segmentFee := new(big.Int).Sub(new(big.Int).Set(gross), afterFee)
		fee.Add(fee, segmentFee)
		remaining.Sub(remaining, gross)
	}
	if remaining.Sign() > 0 {
		return nil, nil, fmt.Errorf("insufficient curve liquidity")
	}
	return output, fee, nil
}

func ExactOutput(segments []Segment, amountOut *big.Int, feeBPS uint16) (input, fee *big.Int, err error) {
	return ExactOutputRate(segments, amountOut, uint64(feeBPS)*FeeRatePrecision/BasisPoints)
}

// ExactOutputRate is the inverse of ExactInputRate with explicit ceilings for
// both the linear segment and the gross amount including fees.
func ExactOutputRate(segments []Segment, amountOut *big.Int, feeRate uint64) (input, fee *big.Int, err error) {
	if amountOut == nil || amountOut.Sign() <= 0 || feeRate >= FeeRatePrecision {
		return nil, nil, fmt.Errorf("amount must be positive and fee valid")
	}
	denominator := new(big.Int).SetUint64(FeeRatePrecision)
	netFactor := new(big.Int).SetUint64(FeeRatePrecision - feeRate)
	remaining := new(big.Int).Set(amountOut)
	input, fee = new(big.Int), new(big.Int)
	for _, segment := range segments {
		if remaining.Sign() == 0 {
			break
		}
		if segment.In == nil || segment.Out == nil || segment.In.Sign() <= 0 || segment.Out.Sign() <= 0 {
			return nil, nil, fmt.Errorf("segment reserves must be positive")
		}
		available := new(big.Int).Set(remaining)
		if available.Cmp(segment.Out) > 0 {
			available.Set(segment.Out)
		}
		// For the segment model output is linear, making exact-output
		// arithmetic a deterministic integer ceiling operation.
		afterFee := new(big.Int).Mul(available, segment.In)
		afterFee.Add(afterFee, new(big.Int).Sub(segment.Out, big.NewInt(1)))
		afterFee.Quo(afterFee, segment.Out)
		gross := new(big.Int).Mul(afterFee, denominator)
		gross.Add(gross, new(big.Int).Sub(new(big.Int).Set(netFactor), big.NewInt(1)))
		gross.Quo(gross, netFactor)
		segmentFee := new(big.Int).Sub(new(big.Int).Set(gross), afterFee)
		input.Add(input, gross)
		fee.Add(fee, segmentFee)
		remaining.Sub(remaining, available)
	}
	if remaining.Sign() > 0 {
		return nil, nil, fmt.Errorf("insufficient curve liquidity")
	}
	return input, fee, nil
}
