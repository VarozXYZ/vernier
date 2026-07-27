// Package saga plans and executes durable Live operations.
package saga

import (
	"fmt"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
)

type LegBinding struct {
	Chain   market.ChainID
	Account execution.AccountID
}

type Planner struct {
	bindings map[market.MarketID]LegBinding
	clock    func() time.Time
}

func NewPlanner(bindings map[market.MarketID]LegBinding, clock func() time.Time) (*Planner, error) {
	if len(bindings) < 2 || clock == nil {
		return nil, fmt.Errorf("saga planner requires market bindings and clock")
	}
	copied := make(map[market.MarketID]LegBinding, len(bindings))
	for marketID, binding := range bindings {
		if marketID == "" || binding.Chain == "" || binding.Account == "" {
			return nil, fmt.Errorf("saga planner contains an incomplete market binding")
		}
		copied[marketID] = binding
	}
	return &Planner{bindings: copied, clock: clock}, nil
}

func (p *Planner) Plan(id execution.PlanID, opportunity arbitrage.LiveOpportunity) (execution.SagaPlan, error) {
	if err := opportunity.Validate(); err != nil {
		return execution.SagaPlan{}, err
	}
	buyBinding, buyOK := p.bindings[opportunity.Direction.BuyMarket]
	sellBinding, sellOK := p.bindings[opportunity.Direction.SellMarket]
	if !buyOK || !sellOK {
		return execution.SagaPlan{}, fmt.Errorf("opportunity markets do not have execution bindings")
	}
	legs := []execution.Leg{
		{
			ID: "buy", Side: execution.LegBuy, Chain: buyBinding.Chain, Account: buyBinding.Account,
			Market: opportunity.Direction.BuyMarket, Input: opportunity.BuyQuote.AmountIn,
			ExpectedOutput: opportunity.BuyQuote.AmountOut,
		},
		{
			ID: "sell", Side: execution.LegSell, Chain: sellBinding.Chain, Account: sellBinding.Account,
			Market: opportunity.Direction.SellMarket, Input: opportunity.SellQuote.AmountIn,
			ExpectedOutput: opportunity.SellQuote.AmountOut,
		},
	}
	return execution.NewSagaPlan(id, opportunity, legs, p.clock().UTC())
}
