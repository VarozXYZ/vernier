package sizing_test

import (
	"testing"

	"github.com/VarozXYZ/vernier/core/sizing"
	"github.com/VarozXYZ/vernier/domain/market"
)

func TestLinearRangeIncludesBoundsAndRequestedSamples(t *testing.T) {
	minimum, _ := market.ParseAssetQuantity("asset", "100")
	maximum, _ := market.ParseAssetQuantity("asset", "5000")
	grid, err := sizing.NewLinearRange(minimum, maximum, 10)
	if err != nil {
		t.Fatal(err)
	}
	values := grid.Values()
	if len(values) != 10 || values[0].Rat().Cmp(minimum.Rat()) != 0 || values[9].Rat().Cmp(maximum.Rat()) != 0 {
		t.Fatalf("unexpected range: %v", values)
	}
	if values[1].Rat().RatString() != "5800/9" {
		t.Fatalf("unexpected exact step: %s", values[1].Rat().RatString())
	}
}

func TestLinearRangeRejectsAmbiguousBounds(t *testing.T) {
	one, _ := market.ParseAssetQuantity("asset", "1")
	two, _ := market.ParseAssetQuantity("asset", "2")
	other, _ := market.ParseAssetQuantity("other", "2")
	for _, test := range []struct {
		maximum market.AssetQuantity
		samples int
	}{{one, 2}, {other, 2}, {two, 1}} {
		if _, err := sizing.NewLinearRange(one, test.maximum, test.samples); err == nil {
			t.Fatal("invalid range was accepted")
		}
	}
}
