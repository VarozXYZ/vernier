package localquoteexperiment

import (
	"fmt"
	"math/big"
	"strings"
)

// WholeTokenAmount is a positive whole-token amount converted to base units.
// Its accessors return immutable text or a defensive copy.
type WholeTokenAmount struct {
	label     string
	baseUnits *big.Int
}

func (a WholeTokenAmount) Label() string {
	return a.label
}

func (a WholeTokenAmount) BaseUnits() *big.Int {
	return new(big.Int).Set(a.baseUnits)
}

// ParseWholeTokenAmounts parses a strictly increasing comma-separated list of
// positive whole-token amounts and converts each value to base units.
func ParseWholeTokenAmounts(raw string, decimals uint8) ([]WholeTokenAmount, error) {
	if decimals > 36 {
		return nil, fmt.Errorf("token decimals exceed supported precision")
	}
	parts := strings.Split(raw, ",")
	if len(parts) == 0 {
		return nil, fmt.Errorf("at least one whole-token amount is required")
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	result := make([]WholeTokenAmount, 0, len(parts))
	var previous *big.Int
	for index, part := range parts {
		text := strings.TrimSpace(part)
		if text == "" || !decimalDigits(text) {
			return nil, fmt.Errorf("amount %d must be a positive whole-token integer", index+1)
		}
		whole, ok := new(big.Int).SetString(text, 10)
		if !ok || whole.Sign() <= 0 {
			return nil, fmt.Errorf("amount %d must be a positive whole-token integer", index+1)
		}
		if previous != nil && whole.Cmp(previous) <= 0 {
			return nil, fmt.Errorf("whole-token amounts must be unique and strictly increasing")
		}
		result = append(result, WholeTokenAmount{
			label:     whole.String(),
			baseUnits: new(big.Int).Mul(new(big.Int).Set(whole), scale),
		})
		previous = whole
	}
	return result, nil
}

func decimalDigits(value string) bool {
	for _, current := range value {
		if current < '0' || current > '9' {
			return false
		}
	}
	return value != ""
}
