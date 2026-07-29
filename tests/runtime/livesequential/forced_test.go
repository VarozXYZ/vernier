package livesequential_test

import (
	"testing"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/runtime/livesequential"
)

func TestForcedDirectionSelectsEitherCompleteRoute(t *testing.T) {
	first := syntheticOpportunity(t)
	second := syntheticOpportunity(t)
	second.Direction = arbitrage.Direction{
		BuyMarket:  "market-b",
		SellMarket: "market-a",
	}
	second.SelectedIndex = -1
	second.Classification = arbitrage.ClassificationNoSpread

	for _, test := range []struct {
		name      string
		requested livesequential.ForcedDirection
		want      arbitrage.Direction
	}{
		{
			name:      "first to second",
			requested: livesequential.ForceFirstToSecond,
			want: arbitrage.Direction{
				BuyMarket: "market-a", SellMarket: "market-b",
			},
		},
		{
			name:      "second to first",
			requested: livesequential.ForceSecondToFirst,
			want: arbitrage.Direction{
				BuyMarket: "market-b", SellMarket: "market-a",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := test.requested.Resolve(
				"market-a",
				"market-b",
			)
			if err != nil {
				t.Fatal(err)
			}
			if resolved != test.want {
				t.Fatalf("direction=%+v, want %+v", resolved, test.want)
			}
			forced, err := livesequential.ForceOpportunity(
				[]arbitrage.Opportunity{first, second},
				resolved,
			)
			if err != nil {
				t.Fatal(err)
			}
			if forced.Direction != test.want ||
				forced.SelectedIndex != 0 ||
				forced.Classification !=
					arbitrage.ClassificationPolicyQualified ||
				!livesequential.IsForcedOpportunity(forced) {
				t.Fatalf("forced opportunity=%+v", forced)
			}
		})
	}
}

func TestForcedDirectionRejectsUnknownAndIncompleteRoute(
	t *testing.T,
) {
	if _, err := livesequential.ParseForcedDirection(
		"sideways",
	); err == nil {
		t.Fatal("unknown forced direction was accepted")
	}
	incomplete := syntheticOpportunity(t)
	incomplete.Candidates = nil
	incomplete.SelectedIndex = -1
	_, err := livesequential.ForceOpportunity(
		[]arbitrage.Opportunity{incomplete},
		incomplete.Direction,
	)
	if err == nil {
		t.Fatal("incomplete forced route was accepted")
	}
}
