// Package costing defines local, immutable cost sources.
package costing

import (
	"fmt"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
)

type Fixed struct {
	id     string
	amount market.AssetQuantity
}

func NewFixed(id string, amount market.AssetQuantity) (Fixed, error) {
	if id == "" || amount.Asset() == "" || amount.Sign() < 0 {
		return Fixed{}, fmt.Errorf("fixed cost requires an ID and non-negative amount")
	}
	return Fixed{id: id, amount: amount}, nil
}

func (f Fixed) Snapshot(at time.Time) (arbitrage.CostSnapshot, error) {
	if at.IsZero() {
		return arbitrage.CostSnapshot{}, fmt.Errorf("cost timestamp is required")
	}
	return arbitrage.CostSnapshot{ID: f.id, Amount: f.amount, CapturedAt: at.UTC()}, nil
}
