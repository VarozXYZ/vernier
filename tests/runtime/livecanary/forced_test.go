package livecanary_test

import (
	"math/big"
	"testing"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
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

func TestForcedCanaryDirectionAcceptsConfiguredMarketIDs(t *testing.T) {
	requested, err := livecanary.ParseForcedCanaryDirection("market-b:market-a")
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := requested.Resolve("market-a", "market-b")
	if err != nil {
		t.Fatal(err)
	}
	want := arbitrage.Direction{BuyMarket: "market-b", SellMarket: "market-a"}
	if resolved != want {
		t.Fatalf("direction=%+v, want %+v", resolved, want)
	}
	foreign, err := livecanary.ParseForcedCanaryDirection("market-c:market-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := foreign.Resolve("market-a", "market-b"); err == nil {
		t.Fatal("foreign market direction was accepted")
	}
}

func TestForcedCanaryUsesExactExecutableCandidate(t *testing.T) {
	opportunity := liveCanaryOpportunity(t)
	direction := opportunity.Direction
	discovery := opportunity.Candidates[0]
	discovery.BuyQuote.Market = direction.BuyMarket
	discovery.SellQuote.Market = direction.SellMarket
	executable := discovery
	buyOutput, _ := market.NewTokenAmount(discovery.BuyQuote.AmountOut.Token(), big.NewInt(7_777_777))
	sellOutput, _ := market.NewTokenAmount(discovery.SellQuote.AmountOut.Token(), big.NewInt(8_888_888))
	executable.BuyQuote.AmountOut = buyOutput
	executable.SellQuote.AmountOut = sellOutput

	forced, err := livecanary.ForceExecutableOpportunity(
		[]arbitrage.Opportunity{opportunity}, direction, &executable,
	)
	if err != nil {
		t.Fatal(err)
	}
	selected := forced.Candidates[forced.SelectedIndex]
	if selected.BuyQuote.AmountOut.Units().Cmp(executable.BuyQuote.AmountOut.Units()) != 0 ||
		selected.SellQuote.AmountOut.Units().Cmp(executable.SellQuote.AmountOut.Units()) != 0 {
		t.Fatal("forced opportunity did not carry the executable validation quotes")
	}
	if opportunity.Candidates[0].BuyQuote.AmountOut.Units().Cmp(discovery.BuyQuote.AmountOut.Units()) != 0 {
		t.Fatal("forced executable handoff mutated the Research opportunity")
	}
}
