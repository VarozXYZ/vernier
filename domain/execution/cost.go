package execution

import (
	"fmt"
	"strings"

	"github.com/VarozXYZ/vernier/domain/market"
)

// CostComponent is measured execution evidence. Amount is denominated in the
// asset actually debited on-chain. QuoteValue is its value in the setup quote
// asset at observation time. Costs already reflected by ActualOutput remain
// visible in the breakdown but must not be subtracted from PnL a second time.
type CostComponent struct {
	Kind             string
	Chain            market.ChainID
	Amount           market.AssetQuantity
	QuoteValue       market.AssetQuantity
	IncludedInOutput bool
	Evidence         string
}

func (c CostComponent) Validate() error {
	if strings.TrimSpace(c.Kind) == "" || c.Chain == "" ||
		c.Amount.Asset() == "" || c.Amount.Sign() < 0 ||
		strings.TrimSpace(c.Evidence) == "" {
		return fmt.Errorf("execution cost component is incomplete")
	}
	if c.QuoteValue.Asset() != "" && c.QuoteValue.Sign() < 0 {
		return fmt.Errorf("execution cost quote value cannot be negative")
	}
	return nil
}

func (c CostComponent) WithQuoteValue(value market.AssetQuantity) (CostComponent, error) {
	if value.Asset() == "" || value.Sign() < 0 {
		return CostComponent{}, fmt.Errorf("execution cost quote value is invalid")
	}
	c.QuoteValue = value
	return c, c.Validate()
}
