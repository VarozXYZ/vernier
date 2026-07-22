// Package liquiditycurve contains the small integer engine shared by
// concentrated-liquidity adapters. Protocol packages provide ordered active
// segments; this package does not know how those segments are discovered.
package liquiditycurve

import (
	"fmt"
	"math/big"
)

const BasisPoints uint64 = 10_000

type Segment struct {
	In  *big.Int
	Out *big.Int
}

func ExactInput(segments []Segment, amount *big.Int, feeBPS uint16) (output, fee *big.Int, err error) {
	if amount == nil || amount.Sign() <= 0 || feeBPS >= uint16(BasisPoints) {
		return nil, nil, fmt.Errorf("amount must be positive and fee valid")
	}
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
		afterFee := new(big.Int).Mul(gross, new(big.Int).SetUint64(BasisPoints-uint64(feeBPS)))
		afterFee.Quo(afterFee, new(big.Int).SetUint64(BasisPoints))
		if afterFee.Sign() == 0 {
			break
		}
		available := new(big.Int).Set(afterFee)
		if available.Cmp(segment.In) > 0 {
			available.Set(segment.In)
			// Gross input needed to consume this segment, rounded up.
			gross.Mul(available, new(big.Int).SetUint64(BasisPoints))
			gross.Add(gross, new(big.Int).SetUint64(BasisPoints-uint64(feeBPS)-1))
			gross.Quo(gross, new(big.Int).SetUint64(BasisPoints-uint64(feeBPS)))
		}
		segmentOutput := new(big.Int).Mul(available, segment.Out)
		segmentOutput.Quo(segmentOutput, segment.In)
		if segmentOutput.Sign() == 0 {
			return nil, nil, fmt.Errorf("quote output rounds to zero")
		}
		output.Add(output, segmentOutput)
		segmentFee := new(big.Int).Mul(gross, new(big.Int).SetUint64(uint64(feeBPS)))
		segmentFee.Quo(segmentFee, new(big.Int).SetUint64(BasisPoints))
		fee.Add(fee, segmentFee)
		remaining.Sub(remaining, gross)
	}
	if remaining.Sign() > 0 {
		return nil, nil, fmt.Errorf("insufficient curve liquidity")
	}
	return output, fee, nil
}

func ExactOutput(segments []Segment, amountOut *big.Int, feeBPS uint16) (input, fee *big.Int, err error) {
	if amountOut == nil || amountOut.Sign() <= 0 || feeBPS >= uint16(BasisPoints) {
		return nil, nil, fmt.Errorf("amount must be positive and fee valid")
	}
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
		gross := new(big.Int).Mul(afterFee, new(big.Int).SetUint64(BasisPoints))
		gross.Add(gross, new(big.Int).SetUint64(BasisPoints-uint64(feeBPS)-1))
		gross.Quo(gross, new(big.Int).SetUint64(BasisPoints-uint64(feeBPS)))
		segmentFee := new(big.Int).Mul(gross, new(big.Int).SetUint64(uint64(feeBPS)))
		segmentFee.Quo(segmentFee, new(big.Int).SetUint64(BasisPoints))
		input.Add(input, gross)
		fee.Add(fee, segmentFee)
		remaining.Sub(remaining, available)
	}
	if remaining.Sign() > 0 {
		return nil, nil, fmt.Errorf("insufficient curve liquidity")
	}
	return input, fee, nil
}
