package livecanary

import (
	"fmt"
	"strings"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
)

type ForcedCanaryDirection string

const (
	ForceCanarySolanaToEVM ForcedCanaryDirection = "solana-to-evm"
	ForceCanaryEVMToSolana ForcedCanaryDirection = "evm-to-solana"

	forcedCanaryReason = "forced_canary_direction"
)

func ParseForcedCanaryDirection(value string) (ForcedCanaryDirection, error) {
	normalized := ForcedCanaryDirection(strings.ToLower(strings.TrimSpace(value)))
	switch normalized {
	case "":
		return "", nil
	case ForceCanarySolanaToEVM, ForceCanaryEVMToSolana:
		return normalized, nil
	default:
		parts := strings.Split(string(normalized), ":")
		if len(parts) == 2 && strings.TrimSpace(parts[0]) != "" &&
			strings.TrimSpace(parts[1]) != "" && parts[0] != parts[1] {
			return normalized, nil
		}
		return "", fmt.Errorf(
			"forced canary direction must be <buy-market>:<sell-market>, %q, or %q",
			ForceCanarySolanaToEVM,
			ForceCanaryEVMToSolana,
		)
	}
}

func (d ForcedCanaryDirection) Resolve(
	firstMarket market.MarketID,
	secondMarket market.MarketID,
) (arbitrage.Direction, error) {
	if firstMarket == "" || secondMarket == "" || firstMarket == secondMarket {
		return arbitrage.Direction{}, fmt.Errorf(
			"forced canary requires two distinct markets",
		)
	}
	switch d {
	case ForceCanarySolanaToEVM:
		return arbitrage.Direction{
			BuyMarket: firstMarket, SellMarket: secondMarket,
		}, nil
	case ForceCanaryEVMToSolana:
		return arbitrage.Direction{
			BuyMarket: secondMarket, SellMarket: firstMarket,
		}, nil
	default:
		parts := strings.Split(string(d), ":")
		if len(parts) == 2 {
			buy, sell := market.MarketID(parts[0]), market.MarketID(parts[1])
			if (buy == firstMarket && sell == secondMarket) ||
				(buy == secondMarket && sell == firstMarket) {
				return arbitrage.Direction{BuyMarket: buy, SellMarket: sell}, nil
			}
		}
		return arbitrage.Direction{}, fmt.Errorf(
			"forced canary direction %q does not match the active setup",
			d,
		)
	}
}

func ForceOpportunity(
	opportunities []arbitrage.Opportunity,
	direction arbitrage.Direction,
) (arbitrage.Opportunity, error) {
	for _, opportunity := range opportunities {
		if opportunity.Direction != direction {
			continue
		}
		if len(opportunity.Candidates) == 0 {
			return arbitrage.Opportunity{}, fmt.Errorf(
				"forced canary direction %s -> %s has no complete fresh round trip",
				direction.BuyMarket,
				direction.SellMarket,
			)
		}
		opportunity.SelectedIndex = 0
		opportunity.Classification = arbitrage.ClassificationPolicyQualified
		opportunity.Reasons = []string{forcedCanaryReason}
		return opportunity, nil
	}
	return arbitrage.Opportunity{}, fmt.Errorf(
		"forced canary direction %s -> %s was not evaluated",
		direction.BuyMarket,
		direction.SellMarket,
	)
}

// ForceExecutableOpportunity preserves forced-canary threshold bypass while
// replacing discovery economics with the exact post-build, post-local-requote
// candidate retained by the executable validation round.
func ForceExecutableOpportunity(
	opportunities []arbitrage.Opportunity,
	direction arbitrage.Direction,
	executable *arbitrage.Candidate,
) (arbitrage.Opportunity, error) {
	if executable == nil || executable.BuyQuote.Market != direction.BuyMarket ||
		executable.SellQuote.Market != direction.SellMarket ||
		executable.BuyQuote.AmountIn.IsZero() || executable.SellQuote.AmountIn.IsZero() {
		return arbitrage.Opportunity{}, fmt.Errorf("forced canary has no exact executable candidate")
	}
	opportunity, err := ForceOpportunity(opportunities, direction)
	if err != nil {
		return arbitrage.Opportunity{}, err
	}
	opportunity.Candidates = append([]arbitrage.Candidate(nil), opportunity.Candidates...)
	opportunity.Candidates[opportunity.SelectedIndex] = *executable
	return opportunity, nil
}

func isForcedCanaryOpportunity(opportunity arbitrage.Opportunity) bool {
	for _, reason := range opportunity.Reasons {
		if reason == forcedCanaryReason {
			return true
		}
	}
	return false
}
