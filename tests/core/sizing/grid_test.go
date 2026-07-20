package sizing_test

import (
	"testing"

	"github.com/VarozXYZ/vernier/core/sizing"
	"github.com/VarozXYZ/vernier/domain/market"
)

func TestGridSortsAndCopiesExactValues(t *testing.T) {
	two, _ := market.ParseAssetQuantity("quote", "2")
	one, _ := market.ParseAssetQuantity("quote", "1")
	input := []market.AssetQuantity{two, one}
	grid, err := sizing.NewGrid(input)
	if err != nil {
		t.Fatal(err)
	}
	input[0], _ = market.ParseAssetQuantity("quote", "99")
	values := grid.Values()
	if values[0].String() != "1" || values[1].String() != "2" {
		t.Fatalf("unexpected grid order: %s, %s", values[0], values[1])
	}
}

func TestGridRejectsInvalidValues(t *testing.T) {
	one, _ := market.ParseAssetQuantity("quote", "1")
	other, _ := market.ParseAssetQuantity("other", "2")
	if _, err := sizing.NewGrid([]market.AssetQuantity{one, other}); err == nil {
		t.Fatal("expected mixed assets to fail")
	}
	if _, err := sizing.NewGrid([]market.AssetQuantity{one, one}); err == nil {
		t.Fatal("expected duplicate values to fail")
	}
}
