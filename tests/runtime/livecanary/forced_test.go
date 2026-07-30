package livecanary_test

import (
	"testing"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/runtime/livecanary"
)

func TestForcedCanaryDirectionSelectsEitherCompleteRoute(t *testing.T) {
	first := liveCanaryOpportunity(t)
	second := liveCanaryOpportunity(t)
	second.Direction = arbitrage.Direction{
		BuyMarket: "market-b", SellMarket: "market-a",
	}
	second.SelectedIndex = -1
	second.Classification = arbitrage.ClassificationNoSpread

	for _, test := range []struct {
		name      string
		requested livecanary.ForcedCanaryDirection
		want      arbitrage.Direction
	}{
		{
			name:      "solana to evm",
			requested: livecanary.ForceCanarySolanaToEVM,
			want: arbitrage.Direction{
				BuyMarket: "market-a", SellMarket: "market-b",
			},
		},
		{
			name:      "evm to solana",
			requested: livecanary.ForceCanaryEVMToSolana,
			want: arbitrage.Direction{
				BuyMarket: "market-b", SellMarket: "market-a",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := test.requested.Resolve("market-a", "market-b")
			if err != nil {
				t.Fatal(err)
			}
			if resolved != test.want {
				t.Fatalf("direction=%+v, want %+v", resolved, test.want)
			}
			forced, err := livecanary.ForceOpportunity(
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
				len(forced.Reasons) != 1 ||
				forced.Reasons[0] != "forced_canary_direction" {
				t.Fatalf("forced opportunity=%+v", forced)
			}
		})
	}
}

func TestForcedCanaryRejectsUnknownDirectionAndIncompleteRoute(t *testing.T) {
	if _, err := livecanary.ParseForcedCanaryDirection("sideways"); err == nil {
		t.Fatal("unknown forced direction was accepted")
	}
	incomplete := liveCanaryOpportunity(t)
	incomplete.Candidates = nil
	incomplete.SelectedIndex = -1
	_, err := livecanary.ForceOpportunity(
		[]arbitrage.Opportunity{incomplete},
		incomplete.Direction,
	)
	if err == nil {
		t.Fatal("incomplete forced route was accepted")
	}
}
