// Package sizing defines deterministic opportunity-sizing policies.
package sizing

import (
	"fmt"
	"sort"

	"github.com/VarozXYZ/vernier/domain/market"
)

type Grid struct {
	values []market.AssetQuantity
}

func NewGrid(values []market.AssetQuantity) (Grid, error) {
	if len(values) == 0 {
		return Grid{}, fmt.Errorf("sizing grid cannot be empty")
	}
	asset := values[0].Asset()
	if asset == "" {
		return Grid{}, fmt.Errorf("sizing values require an asset")
	}
	copyValues := append([]market.AssetQuantity(nil), values...)
	for _, value := range copyValues {
		if value.Asset() != asset || value.Sign() <= 0 {
			return Grid{}, fmt.Errorf("sizing values must be positive quantities of asset %q", asset)
		}
	}
	sort.Slice(copyValues, func(i, j int) bool {
		comparison, _ := copyValues[i].Cmp(copyValues[j])
		return comparison < 0
	})
	for index := 1; index < len(copyValues); index++ {
		comparison, _ := copyValues[index-1].Cmp(copyValues[index])
		if comparison == 0 {
			return Grid{}, fmt.Errorf("sizing grid contains duplicate value %s", copyValues[index].String())
		}
	}
	return Grid{values: copyValues}, nil
}

func (g Grid) Asset() market.AssetID {
	if len(g.values) == 0 {
		return ""
	}
	return g.values[0].Asset()
}

func (g Grid) Values() []market.AssetQuantity {
	return append([]market.AssetQuantity(nil), g.values...)
}
