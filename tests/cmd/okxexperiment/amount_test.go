package okxexperiment_test

import (
	"testing"

	"github.com/VarozXYZ/vernier/cmd/okxexperiment"
)

func TestFormatBaseUnits(t *testing.T) {
	got, err := okxexperiment.FormatBaseUnits("25500000", "6")
	if err != nil {
		t.Fatal(err)
	}
	if got != "25.500000" {
		t.Fatalf("formatted amount = %q, want %q", got, "25.500000")
	}
}

func TestDifferenceBaseUnits(t *testing.T) {
	got, err := okxexperiment.DifferenceBaseUnits("100000000", "99999999", "8")
	if err != nil {
		t.Fatal(err)
	}
	if got != "0.00000001" {
		t.Fatalf("amount difference = %q, want %q", got, "0.00000001")
	}
}

func TestSummarizeBaseUnits(t *testing.T) {
	got, err := okxexperiment.SummarizeBaseUnits([]string{"100000000", "200000000", "300000000"}, "8")
	if err != nil {
		t.Fatal(err)
	}
	if got.Samples != 3 || got.Min != "1.00000000" || got.Mean != "2.00000000" || got.P50 != "2.00000000" || got.Max != "3.00000000" {
		t.Fatalf("unexpected amount statistics: %+v", got)
	}
}

func TestRestrictedRequestUsesConfiguredDEXIDs(t *testing.T) {
	settings := okxexperiment.Settings{
		FromToken:        "from",
		ToToken:          "to",
		Amount:           "1000000",
		RestrictedDexIDs: "103,284",
	}
	request, err := settings.RestrictedRequest()
	if err != nil {
		t.Fatal(err)
	}
	if request.DexIDs != "103,284" {
		t.Fatalf("restricted dex IDs = %q, want %q", request.DexIDs, "103,284")
	}
}

func TestRestrictedRequestRequiresConfiguredDEXIDs(t *testing.T) {
	if _, err := (okxexperiment.Settings{}).RestrictedRequest(); err == nil {
		t.Fatal("expected missing restricted DEX IDs error")
	}
}
