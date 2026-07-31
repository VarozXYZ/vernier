package livecanary

import (
	"fmt"
	"math/big"
	"strconv"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

type DynamicSlippagePolicy struct {
	Enabled bool
	MaxBPS  uint16
}

func (d *SwapDriver) dynamicBuySlippage(
	plan execution.SequentialPlan,
) (*executionport.SlippageConstraint, error) {
	if !d.DynamicSlippage.Enabled ||
		isForcedCanaryOpportunity(plan.Opportunity) {
		return nil, nil
	}
	candidate, err := selectedCandidate(plan.Opportunity)
	if err != nil {
		return nil, err
	}
	if candidate.BuyQuote.AmountIn.Token() != plan.InitialInput.Token() ||
		candidate.BuyQuote.AmountIn.Units().Cmp(
			plan.InitialInput.Units(),
		) != 0 {
		return nil, fmt.Errorf(
			"dynamic buy slippage requires discovery and execution inputs to match",
		)
	}
	sellOutput, err := d.convertAmount(
		candidate.SellQuote.AmountOut,
		plan.InitialInput.Token(),
	)
	if err != nil {
		return nil, err
	}
	required, err := d.requiredFinalUnits(
		plan,
		candidate.Cost.Amount,
		nil,
	)
	if err != nil {
		return nil, err
	}
	if sellOutput.Units().Cmp(required) <= 0 {
		return nil, fmt.Errorf(
			"dynamic buy slippage has no economic budget: expected=%s required=%s",
			sellOutput,
			required,
		)
	}
	buyOutput := candidate.BuyQuote.AmountOut
	minimumUnits := ceilMulDiv(
		buyOutput.Units(),
		required,
		sellOutput.Units(),
	)
	minimum, err := market.NewTokenAmount(
		buyOutput.Token(),
		minimumUnits,
	)
	if err != nil {
		return nil, err
	}
	bps := maximumSlippageBPS(
		buyOutput.Units(),
		minimumUnits,
		d.DynamicSlippage.MaxBPS,
	)
	return &executionport.SlippageConstraint{
		BPS:           bps,
		MinimumOutput: minimum,
		Reason:        "dynamic_buy_budget",
		Evidence: map[string]string{
			"expected_buy_output_units":  buyOutput.String(),
			"expected_sell_output_units": sellOutput.String(),
			"required_final_units":       required.String(),
			"dynamic_budget_units": new(big.Int).Sub(
				sellOutput.Units(),
				required,
			).String(),
			"max_bps": strconv.FormatUint(
				uint64(d.DynamicSlippage.MaxBPS),
				10,
			),
		},
	}, nil
}

func (d *SwapDriver) requiredFinalUnits(
	plan execution.SequentialPlan,
	pending market.AssetQuantity,
	incurred []execution.CostComponent,
) (*big.Int, error) {
	decimals, ok := d.TokenDecimals[plan.InitialInput.Token()]
	if !ok {
		return nil, fmt.Errorf(
			"dynamic slippage quote-token decimals are unavailable",
		)
	}
	required := plan.InitialInput.Units()
	if pending.Asset() != "" {
		if pending.Asset() != d.QuoteAsset {
			return nil, fmt.Errorf(
				"dynamic slippage pending costs are not valued in %s",
				d.QuoteAsset,
			)
		}
		required.Add(required, ratUnitsCeil(pending.Rat(), decimals))
	}
	for _, component := range incurred {
		if component.IncludedInOutput {
			continue
		}
		if component.QuoteValue.Asset() != d.QuoteAsset {
			return nil, fmt.Errorf(
				"dynamic slippage incurred cost is not valued in %s",
				d.QuoteAsset,
			)
		}
		required.Add(
			required,
			ratUnitsCeil(component.QuoteValue.Rat(), decimals),
		)
	}
	required.Add(
		required,
		ratUnitsCeil(cloneRatOrZero(d.MinimumNet), decimals),
	)
	return required, nil
}

func selectedCandidate(
	opportunity arbitrage.Opportunity,
) (arbitrage.Candidate, error) {
	if opportunity.SelectedIndex < 0 ||
		opportunity.SelectedIndex >= len(opportunity.Candidates) {
		return arbitrage.Candidate{},
			fmt.Errorf("dynamic slippage has no selected candidate")
	}
	return opportunity.Candidates[opportunity.SelectedIndex], nil
}

func maximumSlippageBPS(
	expected *big.Int,
	minimum *big.Int,
	capBPS uint16,
) uint16 {
	if expected == nil || expected.Sign() <= 0 ||
		minimum == nil || minimum.Cmp(expected) >= 0 {
		return 0
	}
	ratio := ceilMulDiv(minimum, big.NewInt(10_000), expected)
	if ratio.Cmp(big.NewInt(10_000)) >= 0 {
		return 0
	}
	allowed := new(big.Int).Sub(big.NewInt(10_000), ratio).Uint64()
	if allowed > uint64(capBPS) {
		allowed = uint64(capBPS)
	}
	return uint16(allowed)
}

func ceilMulDiv(left, right, divisor *big.Int) *big.Int {
	numerator := new(big.Int).Mul(
		new(big.Int).Set(left),
		new(big.Int).Set(right),
	)
	quotient, remainder := new(big.Int).QuoRem(
		numerator,
		divisor,
		new(big.Int),
	)
	if remainder.Sign() != 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	return quotient
}

func ratUnitsCeil(value *big.Rat, decimals uint8) *big.Int {
	if value == nil || value.Sign() <= 0 {
		return new(big.Int)
	}
	scaled := new(big.Rat).Mul(
		new(big.Rat).Set(value),
		new(big.Rat).SetInt(decimalScale(decimals)),
	)
	units, remainder := new(big.Int).QuoRem(
		scaled.Num(),
		scaled.Denom(),
		new(big.Int),
	)
	if remainder.Sign() != 0 {
		units.Add(units, big.NewInt(1))
	}
	return units
}
