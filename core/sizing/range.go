package sizing

import (
	"fmt"
	"math/big"

	"github.com/VarozXYZ/vernier/domain/market"
)

// NewLinearRange returns samples evenly spaced values including both bounds.
func NewLinearRange(minimum, maximum market.AssetQuantity, samples int) (Grid, error) {
	if minimum.Asset() == "" || minimum.Asset() != maximum.Asset() || minimum.Sign() <= 0 || samples < 2 {
		return Grid{}, fmt.Errorf("linear range requires one asset, positive bounds, and at least two samples")
	}
	comparison, err := minimum.Cmp(maximum)
	if err != nil || comparison >= 0 {
		return Grid{}, fmt.Errorf("linear range maximum must exceed minimum")
	}
	step := new(big.Rat).Quo(
		new(big.Rat).Sub(maximum.Rat(), minimum.Rat()),
		new(big.Rat).SetInt64(int64(samples-1)),
	)
	values := make([]market.AssetQuantity, samples)
	for index := range values {
		value := new(big.Rat).Add(minimum.Rat(), new(big.Rat).Mul(step, new(big.Rat).SetInt64(int64(index))))
		values[index], err = market.NewAssetQuantity(minimum.Asset(), value)
		if err != nil {
			return Grid{}, err
		}
	}
	return NewGrid(values)
}
