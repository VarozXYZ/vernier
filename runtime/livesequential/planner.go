// Package livesequential orchestrates inventory-carrying execution without
// knowing concrete chains, protocols, transports, or private market topology.
package livesequential

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
	MarketChains   map[market.MarketID]market.ChainID
	ExecutionUnits *big.Int
	Clock          func() time.Time
}

func (p Planner) Plan(
	opportunity arbitrage.Opportunity,
) (execution.SequentialPlan, error) {
	if p.ExecutionUnits == nil || p.ExecutionUnits.Sign() <= 0 {
		return execution.SequentialPlan{}, fmt.Errorf(
			"positive sequential execution units are required",
		)
	}
	if opportunity.SelectedIndex < 0 ||
		opportunity.SelectedIndex >= len(opportunity.Candidates) {
		return execution.SequentialPlan{}, fmt.Errorf(
			"opening has no selected candidate",
		)
	}
	buyChain := p.MarketChains[opportunity.Direction.BuyMarket]
	sellChain := p.MarketChains[opportunity.Direction.SellMarket]
	if buyChain == "" || sellChain == "" {
		return execution.SequentialPlan{}, fmt.Errorf(
			"opportunity markets are not bound to execution chains",
		)
	}
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
	return execution.NewSequentialPlan(
		execution.PlanID(id), opportunity, initial,
		buyChain, sellChain, clock().UTC(),
	)
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
