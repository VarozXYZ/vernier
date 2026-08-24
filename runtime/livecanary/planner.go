// Package livecanary composes a sequential Live execution from a
// policy-qualified Research opening. It remains setup-neutral: private token
// addresses and bridge profiles belong to ignored configuration.
package livecanary

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math/big"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
)

type Planner struct {
	MarketChains            map[market.MarketID]market.ChainID
	ExecutionUnits          *big.Int
	ExecutionAmount         *big.Rat
	AllowedExecutionAmounts []*big.Rat
	ExecutionPolicy         execution.ExecutionPolicyKind
	BaseAsset               market.AssetID
	QuoteAsset              market.AssetID
	TokenDecimals           map[market.TokenID]uint8
	Clock                   func() time.Time
}

func (p Planner) Plan(
	opportunity arbitrage.Opportunity,
) (execution.SequentialPlan, error) {
	if (p.ExecutionUnits == nil || p.ExecutionUnits.Sign() <= 0) &&
		(p.ExecutionAmount == nil || p.ExecutionAmount.Sign() <= 0) && len(p.AllowedExecutionAmounts) == 0 {
		return execution.SequentialPlan{}, fmt.Errorf("positive Live execution units are required")
	}
	if opportunity.SelectedIndex < 0 ||
		opportunity.SelectedIndex >= len(opportunity.Candidates) {
		return execution.SequentialPlan{}, fmt.Errorf("opening has no selected candidate")
	}
	buyChain := p.MarketChains[opportunity.Direction.BuyMarket]
	sellChain := p.MarketChains[opportunity.Direction.SellMarket]
	candidate := opportunity.Candidates[opportunity.SelectedIndex]
	units := p.ExecutionUnits
	if len(p.AllowedExecutionAmounts) > 0 {
		allowed := false
		for _, amount := range p.AllowedExecutionAmounts {
			if amount != nil && candidate.Input.Rat().Cmp(amount) == 0 {
				allowed = true
				break
			}
		}
		if !allowed {
			return execution.SequentialPlan{}, fmt.Errorf("selected Live input is not in the configured execution grid")
		}
		units = candidate.BuyQuote.AmountIn.Units()
	} else if p.ExecutionAmount != nil {
		decimals, ok := p.TokenDecimals[candidate.BuyQuote.AmountIn.Token()]
		if !ok {
			return execution.SequentialPlan{}, fmt.Errorf("live execution token decimals are unavailable")
		}
		scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
		scaled := new(big.Rat).Mul(p.ExecutionAmount, new(big.Rat).SetInt(scale))
		if !scaled.IsInt() {
			return execution.SequentialPlan{}, fmt.Errorf("live execution amount exceeds token precision")
		}
		units = new(big.Int).Set(scaled.Num())
	}
	initial, err := market.NewTokenAmount(
		candidate.BuyQuote.AmountIn.Token(),
		units,
	)
	if err != nil {
		return execution.SequentialPlan{}, err
	}
	id, err := randomIdentity("sequential-plan-")
	if err != nil {
		return execution.SequentialPlan{}, err
	}
	clock := p.Clock
	if clock == nil {
		clock = time.Now
	}
	switch p.ExecutionPolicy {
	case "", execution.PolicyTransportedSequential:
		return execution.NewSequentialPlan(
			execution.PlanID(id), opportunity, initial,
			buyChain, sellChain, clock().UTC(),
		)
	case execution.PolicyPrefundedSequential:
		return execution.NewPrefundedSequentialPlan(
			execution.PlanID(id), opportunity, initial,
			buyChain, sellChain, clock().UTC(),
		)
	case execution.PolicyPrefundedParallel, execution.PolicyPrefundedTriggerFirst:
		plan, planErr := execution.NewPrefundedParallelPlan(
			execution.PlanID(id), opportunity, initial,
			buyChain, sellChain, clock().UTC(),
		)
		if planErr != nil {
			return execution.SequentialPlan{}, planErr
		}
		plan.Policy = p.ExecutionPolicy
		plan.BaseAsset, plan.QuoteAsset = p.BaseAsset, p.QuoteAsset
		plan.TokenDecimals = make(map[market.TokenID]uint8, len(p.TokenDecimals))
		for token, decimals := range p.TokenDecimals {
			plan.TokenDecimals[token] = decimals
		}
		if plan.BaseAsset == "" || plan.QuoteAsset == "" {
			return execution.SequentialPlan{}, fmt.Errorf("parallel plan valuation assets are unavailable")
		}
		return plan, plan.Validate()
	default:
		return execution.SequentialPlan{}, fmt.Errorf(
			"unsupported dependent execution policy %q", p.ExecutionPolicy,
		)
	}
}

func newOperationID() (execution.OperationID, error) {
	value, err := randomIdentity("sequential-operation-")
	return execution.OperationID(value), err
}

func randomIdentity(prefix string) (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(entropy[:]), nil
}
