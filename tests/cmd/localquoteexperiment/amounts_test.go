package localquoteexperiment_test

import (
	"testing"

	"github.com/VarozXYZ/vernier/cmd/localquoteexperiment"
)

func TestParseWholeTokenAmountsConvertsIncreasingValuesToBaseUnits(t *testing.T) {
	amounts, err := localquoteexperiment.ParseWholeTokenAmounts(
		"100, 200,500,1000,2500,5000", 6,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantLabels := []string{"100", "200", "500", "1000", "2500", "5000"}
	wantUnits := []string{
		"100000000", "200000000", "500000000",
		"1000000000", "2500000000", "5000000000",
	}
	if len(amounts) != len(wantLabels) {
		t.Fatalf("got %d amounts, want %d", len(amounts), len(wantLabels))
	}
	for index, amount := range amounts {
		if amount.Label() != wantLabels[index] || amount.BaseUnits().String() != wantUnits[index] {
			t.Fatalf(
				"amount %d = %s/%s, want %s/%s",
				index, amount.Label(), amount.BaseUnits(), wantLabels[index], wantUnits[index],
			)
		}
	}
}

func TestParseWholeTokenAmountsRejectsInvalidLists(t *testing.T) {
	tests := []string{
		"",
		"0",
		"-1",
		"1.5",
		"100,100",
		"200,100",
		"100,",
	}
	for _, input := range tests {
		if _, err := localquoteexperiment.ParseWholeTokenAmounts(input, 18); err == nil {
			t.Errorf("ParseWholeTokenAmounts(%q) accepted invalid input", input)
		}
	}
}

func TestWholeTokenAmountBaseUnitsReturnsDefensiveCopy(t *testing.T) {
	amounts, err := localquoteexperiment.ParseWholeTokenAmounts("100", 6)
	if err != nil {
		t.Fatal(err)
	}
	units := amounts[0].BaseUnits()
	units.SetInt64(1)
	if got := amounts[0].BaseUnits().String(); got != "100000000" {
		t.Fatalf("mutating returned units changed stored amount to %s", got)
	}
}
