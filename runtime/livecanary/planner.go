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
	MarketChains    map[market.MarketID]market.ChainID
	ExecutionUnits  *big.Int
	ExecutionPolicy execution.ExecutionPolicyKind
	Clock           func() time.Time
}

func (p Planner) Plan(
	opportunity arbitrage.Opportunity,
) (execution.SequentialPlan, error) {
	if p.ExecutionUnits == nil || p.ExecutionUnits.Sign() <= 0 {
		return execution.SequentialPlan{}, fmt.Errorf("positive Live execution units are required")
	}
	if opportunity.SelectedIndex < 0 ||
		opportunity.SelectedIndex >= len(opportunity.Candidates) {
		return execution.SequentialPlan{}, fmt.Errorf("opening has no selected candidate")
	}
	buyChain := p.MarketChains[opportunity.Direction.BuyMarket]
	sellChain := p.MarketChains[opportunity.Direction.SellMarket]
	candidate := opportunity.Candidates[opportunity.SelectedIndex]
	initial, err := market.NewTokenAmount(
		candidate.BuyQuote.AmountIn.Token(),
		p.ExecutionUnits,
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
