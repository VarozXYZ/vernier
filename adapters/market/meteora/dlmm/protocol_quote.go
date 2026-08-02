package dlmm

import (
	"fmt"
	"math/big"

	"github.com/VarozXYZ/vernier/domain/market"
)

var (
	meteoraPriceScale   = new(big.Int).Lsh(big.NewInt(1), priceScaleBits)
	meteoraFeePrecision = big.NewInt(1_000_000_000)
	meteoraMaxUint64    = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 64), big.NewInt(1))
)

type protocolQuoteResult struct {
	amountIn  *big.Int
	amountOut *big.Int
	fee       *big.Int
}

type binQuoteResult struct {
	amountIn  *big.Int
	amountOut *big.Int
	fee       *big.Int
}

type fillResult struct {
	amountIn   *big.Int
	amountOut  *big.Int
	amountLeft *big.Int
}

func quoteExactIn(state Snapshot, amountIn *big.Int, swapForY bool, timestamp int64) (protocolQuoteResult, error) {
	if !validProtocolAmount(amountIn) {
		return protocolQuoteResult{}, fmt.Errorf("meteora exact-input amount must fit u64")
	}
	variables, err := state.variablesAt(timestamp)
	if err != nil {
		return protocolQuoteResult{}, err
	}
	remaining := clone(amountIn)
	totalOut := new(big.Int)
	totalFee := new(big.Int)
	for _, bin := range routeBins(state, swapForY) {
		if remaining.Sign() == 0 {
			break
		}
		if maxAmountOut(bin, swapForY, state.supportsLimitOrders()).Sign() == 0 {
			continue
		}
		rate := state.feeRateAtBin(bin.id, variables)
		result, err := quoteExactInAtBin(bin, remaining, swapForY, state.supportsLimitOrders(), state.feeOnInput(swapForY), rate)
		if err != nil {
			return protocolQuoteResult{}, fmt.Errorf("quote Meteora bin %d: %w", bin.id, err)
		}
		if result.amountIn.Sign() == 0 {
			continue
		}
		remaining.Sub(remaining, result.amountIn)
		totalOut.Add(totalOut, result.amountOut)
		totalFee.Add(totalFee, result.fee)
		if totalOut.Cmp(meteoraMaxUint64) > 0 || totalFee.Cmp(meteoraMaxUint64) > 0 {
			return protocolQuoteResult{}, fmt.Errorf("meteora exact-input quote overflows u64")
		}
	}
	if remaining.Sign() > 0 {
		return protocolQuoteResult{}, fmt.Errorf("insufficient Meteora DLMM liquidity")
	}
	if totalOut.Sign() == 0 {
		return protocolQuoteResult{}, market.ErrQuoteOutputRoundsToZero
	}
	return protocolQuoteResult{amountIn: clone(amountIn), amountOut: totalOut, fee: totalFee}, nil
}

// RequiredBinArrayIndexes returns the exact bin-array accounts consumed by an
// exact-input quote, in the order expected by Meteora's swap2 instruction.
// Keeping this derived from the same integer quote loop prevents the simulator
// from silently using a different liquidity path than Research.
func RequiredBinArrayIndexes(state Snapshot, amountIn *big.Int, swapForY bool, timestamp int64) ([]int64, error) {
	if !validProtocolAmount(amountIn) {
		return nil, fmt.Errorf("meteora exact-input amount must fit u64")
	}
	variables, err := state.variablesAt(timestamp)
	if err != nil {
		return nil, err
	}
	remaining := clone(amountIn)
	seen := make(map[int64]struct{})
	result := make([]int64, 0)
	for _, bin := range routeBins(state, swapForY) {
		if remaining.Sign() == 0 {
			break
		}
		if maxAmountOut(bin, swapForY, state.supportsLimitOrders()).Sign() == 0 {
			continue
		}
		quoted, err := quoteExactInAtBin(bin, remaining, swapForY, state.supportsLimitOrders(), state.feeOnInput(swapForY), state.feeRateAtBin(bin.id, variables))
		if err != nil {
			return nil, err
		}
		if quoted.amountIn.Sign() == 0 {
			continue
		}
		remaining.Sub(remaining, quoted.amountIn)
		index := floorDiv(int64(bin.id), binArrayCount)
		if _, ok := seen[index]; !ok {
			seen[index] = struct{}{}
			result = append(result, index)
		}
	}
	if remaining.Sign() > 0 {
		return nil, fmt.Errorf("insufficient Meteora DLMM liquidity")
	}
	return result, nil
}

func quoteExactOut(state Snapshot, amountOut *big.Int, swapForY bool, timestamp int64) (protocolQuoteResult, error) {
	if !validProtocolAmount(amountOut) {
		return protocolQuoteResult{}, fmt.Errorf("meteora exact-output amount must fit u64")
	}
	variables, err := state.variablesAt(timestamp)
	if err != nil {
		return protocolQuoteResult{}, err
	}
	remaining := clone(amountOut)
	totalIn := new(big.Int)
	totalFee := new(big.Int)
	for _, bin := range routeBins(state, swapForY) {
		if remaining.Sign() == 0 {
			break
		}
		if maxAmountOut(bin, swapForY, state.supportsLimitOrders()).Sign() == 0 {
			continue
		}
		rate := state.feeRateAtBin(bin.id, variables)
		result, err := quoteExactOutAtBin(bin, remaining, swapForY, state.supportsLimitOrders(), state.feeOnInput(swapForY), rate)
		if err != nil {
			return protocolQuoteResult{}, fmt.Errorf("quote Meteora bin %d: %w", bin.id, err)
		}
		if result.amountOut.Sign() == 0 {
			continue
		}
		remaining.Sub(remaining, result.amountOut)
		totalIn.Add(totalIn, result.amountIn)
		totalFee.Add(totalFee, result.fee)
		if totalIn.Cmp(meteoraMaxUint64) > 0 || totalFee.Cmp(meteoraMaxUint64) > 0 {
			return protocolQuoteResult{}, fmt.Errorf("meteora exact-output quote overflows u64")
		}
	}
	if remaining.Sign() > 0 {
		return protocolQuoteResult{}, fmt.Errorf("insufficient Meteora DLMM liquidity")
	}
	return protocolQuoteResult{amountIn: totalIn, amountOut: clone(amountOut), fee: totalFee}, nil
}

func quoteExactInAtBin(bin Bin, amountIn *big.Int, swapForY, supportLimitOrders, feeOnInput bool, feeRate uint64) (binQuoteResult, error) {
	tradingFee := new(big.Int)
	excludedFeeInput := clone(amountIn)
	if feeOnInput {
		tradingFee = feeFromIncludedAmount(amountIn, feeRate)
		excludedFeeInput.Sub(excludedFeeInput, tradingFee)
	}
	if excludedFeeInput.Sign() <= 0 {
		return binQuoteResult{amountIn: new(big.Int), amountOut: new(big.Int), fee: new(big.Int)}, nil
	}
	fill, err := exactInFill(bin, excludedFeeInput, swapForY, supportLimitOrders)
	if err != nil {
		return binQuoteResult{}, err
	}
	includedFeeInput := clone(amountIn)
	if fill.amountLeft.Sign() > 0 {
		excludedFeeInput = clone(fill.amountIn)
		if feeOnInput {
			tradingFee = feeFromExcludedAmount(excludedFeeInput, feeRate)
			includedFeeInput.Add(excludedFeeInput, tradingFee)
		} else {
			includedFeeInput = excludedFeeInput
		}
	}
	excludedFeeOutput := clone(fill.amountOut)
	if !feeOnInput {
		tradingFee = feeFromIncludedAmount(fill.amountOut, feeRate)
		excludedFeeOutput.Sub(excludedFeeOutput, tradingFee)
	}
	return binQuoteResult{amountIn: includedFeeInput, amountOut: excludedFeeOutput, fee: tradingFee}, nil
}

func quoteExactOutAtBin(bin Bin, requestedOut *big.Int, swapForY, supportLimitOrders, feeOnInput bool, feeRate uint64) (binQuoteResult, error) {
	includedFeeOutput := clone(requestedOut)
	if !feeOnInput {
		includedFeeOutput.Add(includedFeeOutput, feeFromExcludedAmount(requestedOut, feeRate))
	}
	if includedFeeOutput.Cmp(maxAmountOut(bin, swapForY, supportLimitOrders)) >= 0 {
		return quoteExactInAtBin(bin, meteoraMaxUint64, swapForY, supportLimitOrders, feeOnInput, feeRate)
	}
	excludedFeeInput, err := inputForLayeredOutput(bin, includedFeeOutput, swapForY, supportLimitOrders)
	if err != nil {
		return binQuoteResult{}, err
	}
	includedFeeInput := clone(excludedFeeInput)
	if feeOnInput {
		includedFeeInput.Add(includedFeeInput, feeFromExcludedAmount(excludedFeeInput, feeRate))
	}
	result, err := quoteExactInAtBin(bin, includedFeeInput, swapForY, supportLimitOrders, feeOnInput, feeRate)
	if err != nil {
		return binQuoteResult{}, err
	}
	if result.amountOut.Cmp(requestedOut) < 0 {
		return binQuoteResult{}, fmt.Errorf("exact-output rounding produced insufficient output")
	}
	result.amountOut = clone(requestedOut)
	return result, nil
}

func exactInFill(bin Bin, amountIn *big.Int, swapForY, supportLimitOrders bool) (fillResult, error) {
	remaining := clone(amountIn)
	totalIn := new(big.Int)
	totalOut := new(big.Int)
	for _, layer := range liquidityLayers(bin, swapForY, supportLimitOrders) {
		if remaining.Sign() == 0 {
			break
		}
		fill, err := fillLayer(remaining, layer, bin.priceX64, swapForY)
		if err != nil {
			return fillResult{}, err
		}
		totalIn.Add(totalIn, fill.amountIn)
		totalOut.Add(totalOut, fill.amountOut)
		remaining = fill.amountLeft
	}
	return fillResult{amountIn: totalIn, amountOut: totalOut, amountLeft: remaining}, nil
}

func fillLayer(amountIn, maxAmountOut, price *big.Int, swapForY bool) (fillResult, error) {
	if maxAmountOut.Sign() == 0 {
		return fillResult{amountIn: new(big.Int), amountOut: new(big.Int), amountLeft: clone(amountIn)}, nil
	}
	maxAmountIn := amountInForOutput(maxAmountOut, price, swapForY)
	if amountIn.Cmp(maxAmountIn) >= 0 {
		return fillResult{amountIn: maxAmountIn, amountOut: clone(maxAmountOut), amountLeft: new(big.Int).Sub(clone(amountIn), maxAmountIn)}, nil
	}
	amountOut := amountOutForInput(amountIn, price, swapForY)
	if amountOut.Sign() == 0 {
		return fillResult{}, market.ErrQuoteOutputRoundsToZero
	}
	return fillResult{amountIn: clone(amountIn), amountOut: amountOut, amountLeft: new(big.Int)}, nil
}

func inputForLayeredOutput(bin Bin, amountOut *big.Int, swapForY, supportLimitOrders bool) (*big.Int, error) {
	remaining := clone(amountOut)
	totalIn := new(big.Int)
	for _, layer := range liquidityLayers(bin, swapForY, supportLimitOrders) {
		if remaining.Sign() == 0 {
			break
		}
		consumedOut := minBig(remaining, layer)
		totalIn.Add(totalIn, amountInForOutput(consumedOut, bin.priceX64, swapForY))
		remaining.Sub(remaining, consumedOut)
	}
	if remaining.Sign() > 0 {
		return nil, fmt.Errorf("insufficient bin liquidity")
	}
	return totalIn, nil
}

func liquidityLayers(bin Bin, swapForY, supportLimitOrders bool) []*big.Int {
	marketMaking := bin.reserveX
	if swapForY {
		marketMaking = bin.reserveY
	}
	layers := []*big.Int{clone(marketMaking)}
	if supportLimitOrders && ((swapForY && !bin.limitOrderAskSide) || (!swapForY && bin.limitOrderAskSide)) {
		layers = append(layers, clone(bin.processedOrderRemainingAmount), clone(bin.openOrderAmount))
	}
	return layers
}

func maxAmountOut(bin Bin, swapForY, supportLimitOrders bool) *big.Int {
	total := new(big.Int)
	for _, layer := range liquidityLayers(bin, swapForY, supportLimitOrders) {
		total.Add(total, layer)
		if total.Cmp(meteoraMaxUint64) > 0 {
			return clone(meteoraMaxUint64)
		}
	}
	return total
}

func amountOutForInput(amountIn, price *big.Int, swapForY bool) *big.Int {
	if swapForY {
		return new(big.Int).Quo(new(big.Int).Mul(clone(price), amountIn), meteoraPriceScale)
	}
	return new(big.Int).Quo(new(big.Int).Mul(clone(amountIn), meteoraPriceScale), price)
}

func amountInForOutput(amountOut, price *big.Int, swapForY bool) *big.Int {
	if swapForY {
		return ceilDiv(new(big.Int).Mul(clone(amountOut), meteoraPriceScale), price)
	}
	return ceilDiv(new(big.Int).Mul(clone(amountOut), price), meteoraPriceScale)
}

func feeFromIncludedAmount(amount *big.Int, rate uint64) *big.Int {
	if rate == 0 || amount.Sign() == 0 {
		return new(big.Int)
	}
	numerator := new(big.Int).Mul(clone(amount), new(big.Int).SetUint64(rate))
	return ceilDiv(numerator, meteoraFeePrecision)
}

func feeFromExcludedAmount(amount *big.Int, rate uint64) *big.Int {
	if rate == 0 || amount.Sign() == 0 {
		return new(big.Int)
	}
	denominator := new(big.Int).Sub(clone(meteoraFeePrecision), new(big.Int).SetUint64(rate))
	numerator := new(big.Int).Mul(clone(amount), new(big.Int).SetUint64(rate))
	return ceilDiv(numerator, denominator)
}

func routeBins(state Snapshot, swapForY bool) []Bin {
	result := make([]Bin, 0, len(state.bins))
	if swapForY {
		for index := len(state.bins) - 1; index >= 0; index-- {
			if state.bins[index].id <= state.activeID {
				result = append(result, state.bins[index])
			}
		}
		return result
	}
	for _, bin := range state.bins {
		if bin.id >= state.activeID {
			result = append(result, bin)
		}
	}
	return result
}

func validProtocolAmount(amount *big.Int) bool {
	return amount != nil && amount.Sign() > 0 && amount.Cmp(meteoraMaxUint64) <= 0
}

func minBig(left, right *big.Int) *big.Int {
	if left.Cmp(right) <= 0 {
		return clone(left)
	}
	return clone(right)
}
