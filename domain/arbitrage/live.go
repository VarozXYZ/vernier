package arbitrage

import (
	"fmt"
	"math/big"
	"time"

	"github.com/VarozXYZ/vernier/domain/market"
)

// ValuationSnapshot is the immutable base-asset price captured before a Live
// evaluation fixes its independent buy and sell inputs. Price is expressed as
// quote asset per one base asset.
type ValuationSnapshot struct {
	version      uint64
	base         market.AssetID
	quote        market.AssetID
	price        *big.Rat
	observations int
	capturedAt   time.Time
}

func NewValuationSnapshot(version uint64, base, quote market.AssetID, price *big.Rat, observations int, capturedAt time.Time) (ValuationSnapshot, error) {
	if version == 0 || base == "" || quote == "" || base == quote || price == nil || price.Sign() <= 0 {
		return ValuationSnapshot{}, fmt.Errorf("valuation requires version, distinct assets, and a positive price")
	}
	if observations < 1 || capturedAt.IsZero() {
		return ValuationSnapshot{}, fmt.Errorf("valuation requires observations and capture time")
	}
	return ValuationSnapshot{
		version: version, base: base, quote: quote, price: new(big.Rat).Set(price),
		observations: observations, capturedAt: capturedAt.UTC(),
	}, nil
}

func (s ValuationSnapshot) Version() uint64       { return s.version }
func (s ValuationSnapshot) Base() market.AssetID  { return s.base }
func (s ValuationSnapshot) Quote() market.AssetID { return s.quote }
func (s ValuationSnapshot) Price() *big.Rat       { return new(big.Rat).Set(s.price) }
func (s ValuationSnapshot) Observations() int     { return s.observations }
func (s ValuationSnapshot) CapturedAt() time.Time { return s.capturedAt }

// LiveOpportunity is deliberately separate from the Research Opportunity.
// Both legs use prefunded inventory and therefore retain their independently
// fixed inputs.
type LiveOpportunity struct {
	ID           string
	Setup        SetupID
	Direction    Direction
	Valuation    ValuationSnapshot
	BuyQuote     market.Quote
	SellQuote    market.Quote
	QuoteDelta   market.AssetQuantity
	BaseDelta    market.AssetQuantity
	MarkedBase   market.AssetQuantity
	GrossPnL     market.AssetQuantity
	Cost         market.AssetQuantity
	NetPnL       market.AssetQuantity
	Threshold    market.AssetQuantity
	RiskBlock    string
	DiscoveredAt time.Time
	ValidatedAt  time.Time
}

func (o LiveOpportunity) Profitable() bool {
	comparison, err := o.NetPnL.Cmp(o.Threshold)
	return o.RiskBlock == "" && err == nil && comparison >= 0 && o.NetPnL.Sign() > 0
}

func (o LiveOpportunity) Validate() error {
	if o.ID == "" || o.Setup == "" || o.Direction.BuyMarket == "" || o.Direction.SellMarket == "" ||
		o.Direction.BuyMarket == o.Direction.SellMarket {
		return fmt.Errorf("live opportunity identity and direction are required")
	}
	if o.BuyQuote.AmountIn.IsZero() || o.BuyQuote.AmountOut.IsZero() ||
		o.SellQuote.AmountIn.IsZero() || o.SellQuote.AmountOut.IsZero() {
		return fmt.Errorf("live opportunity requires two non-zero quotes")
	}
	for name, quantity := range map[string]market.AssetQuantity{
		"quote delta": o.QuoteDelta, "base delta": o.BaseDelta, "marked base": o.MarkedBase,
		"gross PnL": o.GrossPnL, "cost": o.Cost, "net PnL": o.NetPnL, "threshold": o.Threshold,
	} {
		if quantity.Asset() == "" {
			return fmt.Errorf("live opportunity %s is required", name)
		}
	}
	if o.Cost.Sign() < 0 || o.Threshold.Sign() < 0 || o.DiscoveredAt.IsZero() {
		return fmt.Errorf("live opportunity costs, threshold, and discovery time are invalid")
	}
	return nil
}
