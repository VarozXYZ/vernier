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
	Enabled     bool
	MaxBPS      uint16
	HeadroomBPS uint16
}

func (d *SwapDriver) dynamicBuySlippage(
	plan execution.SequentialPlan,
) (*executionport.SlippageConstraint, error) {
	if !d.DynamicSlippage.Enabled || isForcedCanaryOpportunity(plan.Opportunity) {
		return nil, nil
	}
	candidate, err := selectedCandidate(plan.Opportunity)
	if err != nil {
		return nil, err
	}
	if plan.IsPrefundedDualInventory() && candidate.Valuation != nil {
		return d.dynamicPrefundedBuySlippage(plan, candidate)
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

// dynamicTriggerFirstSlippage reserves a fixed share of the complete expected
// net PnL for the not-yet-sent second leg and applies the configured output
// percentage cap. The stricter of the economic and percentage floors wins.
func (d *SwapDriver) dynamicTriggerFirstSlippage(plan execution.SequentialPlan,
	expected market.Quote) (*executionport.SlippageConstraint, error) {
	// Forced canaries deliberately bypass the profit threshold and may have no
	// positive economic budget from which to derive a 75/25 floor. They retain
	// the configured fixed on-chain slippage instead.
	if isForcedCanaryOpportunity(plan.Opportunity) {
		return nil, nil
	}
	if plan.EffectivePolicy() != execution.PolicyPrefundedTriggerFirst {
		return d.dynamicBuySlippage(plan)
	}
	candidate, err := selectedCandidate(plan.Opportunity)
	if err != nil {
		return nil, err
	}
	headroomBPS := d.DynamicSlippage.HeadroomBPS
	if headroomBPS == 0 {
		headroomBPS = 2_500
	}
	if headroomBPS >= 10_000 || candidate.NetPnL.Sign() <= 0 ||
		candidate.NetPnL.Asset() != d.QuoteAsset || candidate.Valuation == nil {
		return nil, fmt.Errorf("trigger-first dynamic slippage economics are incomplete")
	}
	consumableBPS := int64(10_000 - headroomBPS)
	budget := new(big.Rat).Mul(candidate.NetPnL.Rat(), big.NewRat(consumableBPS, 10_000))
	reserved := new(big.Rat).Sub(candidate.NetPnL.Rat(), budget)
	decimals, ok := d.TokenDecimals[expected.AmountOut.Token()]
	if !ok {
		return nil, fmt.Errorf("trigger-first output decimals are unavailable")
	}
	unitLoss := new(big.Rat).Set(budget)
	outputAsset := d.QuoteAsset
	if expected.AmountOut.Token() == candidate.BuyQuote.AmountOut.Token() {
		price := candidate.Valuation.Price()
		if price.Sign() <= 0 {
			return nil, fmt.Errorf("trigger-first mark price is invalid")
		}
		unitLoss.Quo(unitLoss, price)
		outputAsset = d.BaseAsset
	} else if conversion := candidate.QuoteConversion; conversion != nil &&
		expected.AmountOut.Token() == conversion.Input.Token() {
		inputDecimals, inputOK := d.TokenDecimals[conversion.Input.Token()]
		outputDecimals, outputOK := d.TokenDecimals[conversion.Output.Token()]
		if !inputOK || !outputOK || !conversion.ValidAt(d.now()) {
			return nil, fmt.Errorf("trigger-first quote conversion is stale or incomplete")
		}
		inputHuman := new(big.Rat).SetFrac(conversion.Input.Units(), decimalScale(inputDecimals))
		outputHuman := new(big.Rat).SetFrac(conversion.Output.Units(), decimalScale(outputDecimals))
		if inputHuman.Sign() <= 0 || outputHuman.Sign() <= 0 {
			return nil, fmt.Errorf("trigger-first quote conversion rate is invalid")
		}
		rate := new(big.Rat).Quo(outputHuman, inputHuman)
		unitLoss.Quo(unitLoss, rate)
	}
	expectedHuman := new(big.Rat).SetFrac(expected.AmountOut.Units(), decimalScale(decimals))
	minimumHuman := new(big.Rat).Sub(expectedHuman, unitLoss)
	if minimumHuman.Sign() <= 0 {
		return nil, fmt.Errorf("trigger-first minimum output is not positive")
	}
	minimumUnits := ratUnitsCeil(minimumHuman, decimals)
	if minimumUnits.Cmp(expected.AmountOut.Units()) > 0 {
		return nil, fmt.Errorf("trigger-first minimum output exceeds expected output")
	}
	economicMinimumUnits := new(big.Int).Set(minimumUnits)
	capBPS := d.DynamicSlippage.MaxBPS
	if capBPS == 0 {
		capBPS = 500
	}
	if capBPS > 500 {
		return nil, fmt.Errorf("trigger-first percentage cap exceeds 500 basis points")
	}
	capMinimumUnits := ceilMulDiv(expected.AmountOut.Units(), big.NewInt(int64(10_000-capBPS)), big.NewInt(10_000))
	limitingBound := "economic_budget"
	if capMinimumUnits.Cmp(minimumUnits) > 0 {
		minimumUnits = capMinimumUnits
		limitingBound = "percentage_cap"
	}
	minimum, err := market.NewTokenAmount(expected.AmountOut.Token(), minimumUnits)
	if err != nil {
		return nil, err
	}
	bps := maximumSlippageBPS(expected.AmountOut.Units(), minimumUnits, capBPS)
	return &executionport.SlippageConstraint{BPS: bps, MinimumOutput: minimum,
		Reason: "dynamic_trigger_first_75_25_capped", Evidence: map[string]string{
			"expected_net": candidate.NetPnL.String(), "reserved_headroom": reserved.RatString(),
			"consumable_budget": budget.RatString(), "headroom_bps": strconv.FormatUint(uint64(headroomBPS), 10),
			"equivalent_slippage_bps": strconv.FormatUint(uint64(bps), 10),
			"max_bps":                 strconv.FormatUint(uint64(capBPS), 10), "limiting_bound": limitingBound,
			"economic_minimum_output_units": economicMinimumUnits.String(),
			"cap_minimum_output_units":      capMinimumUnits.String(),
			"output_asset":                  string(outputAsset), "minimum_output_units": minimum.String(),
		}}, nil
}

// dynamicPrefundedBuySlippage derives the remote BUY floor from the complete
// two-inventory economics. Unlike a transported route, the SELL input is
// independent, so quote-token return alone cannot represent the available
// budget: residual base inventory must be valued as part of net PnL.
func (d *SwapDriver) dynamicPrefundedBuySlippage(
	plan execution.SequentialPlan,
	candidate arbitrage.Candidate,
) (*executionport.SlippageConstraint, error) {
	if candidate.NetPnL.Asset() != d.QuoteAsset ||
		candidate.Valuation.Base() != d.BaseAsset ||
		candidate.Valuation.Quote() != d.QuoteAsset {
		return nil, fmt.Errorf("prefunded dynamic buy slippage valuation is incompatible")
	}
	minimumNet, err := market.NewAssetQuantity(d.QuoteAsset, d.minimumNetFor(plan.Opportunity.Direction))
	if err != nil {
		return nil, fmt.Errorf("prefunded dynamic buy slippage threshold is incompatible: %w", err)
	}
	headroom, err := candidate.NetPnL.Sub(minimumNet)
	if err != nil {
		return nil, err
	}
	if headroom.Sign() < 0 {
		return nil, fmt.Errorf(
			"prefunded dynamic buy slippage has no economic budget: net=%s minimum=%s",
			candidate.NetPnL, minimumNet,
		)
	}
	expected := candidate.BuyQuote.AmountOut
	decimals, ok := d.TokenDecimals[expected.Token()]
	if !ok {
		return nil, fmt.Errorf("prefunded dynamic buy slippage base-token decimals are unavailable")
	}
	price := candidate.Valuation.Price()
	if price.Sign() <= 0 {
		return nil, fmt.Errorf("prefunded dynamic buy slippage mark price is invalid")
	}
	expectedHuman := new(big.Rat).SetFrac(expected.Units(), decimalScale(decimals))
	baseHeadroom := new(big.Rat).Quo(headroom.Rat(), price)
	minimumHuman := new(big.Rat).Sub(expectedHuman, baseHeadroom)
	minimumUnits := big.NewInt(1)
	if minimumHuman.Sign() > 0 {
		minimumUnits = ratUnitsCeil(minimumHuman, decimals)
	}
	if minimumUnits.Cmp(expected.Units()) > 0 {
		return nil, fmt.Errorf(
			"prefunded dynamic buy slippage floor exceeds expected output: expected=%s minimum_units=%s",
			expected, minimumUnits,
		)
	}
	minimum, err := market.NewTokenAmount(expected.Token(), minimumUnits)
	if err != nil {
		return nil, err
	}
	bps := maximumSlippageBPS(expected.Units(), minimumUnits, d.DynamicSlippage.MaxBPS)
	return &executionport.SlippageConstraint{
		BPS: bps, MinimumOutput: minimum,
		Reason: "dynamic_prefunded_buy_budget",
		Evidence: map[string]string{
			"expected_buy_output_units": expected.String(),
			"minimum_buy_output_units":  minimum.String(),
			"net_headroom":              headroom.String(),
			"mark_price":                price.RatString(),
			"max_bps":                   strconv.FormatUint(uint64(d.DynamicSlippage.MaxBPS), 10),
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
		ratUnitsCeil(d.minimumNetFor(plan.Opportunity.Direction), decimals),
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
