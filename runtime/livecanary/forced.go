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
	switch normalized := ForcedCanaryDirection(strings.ToLower(strings.TrimSpace(value))); normalized {
	case "":
		return "", nil
	case ForceCanarySolanaToEVM, ForceCanaryEVMToSolana:
		return normalized, nil
	default:
		return "", fmt.Errorf(
			"forced canary direction must be %q or %q",
			ForceCanarySolanaToEVM,
			ForceCanaryEVMToSolana,
		)
	}
}

func (d ForcedCanaryDirection) Resolve(
	solanaMarket market.MarketID,
	evmMarket market.MarketID,
) (arbitrage.Direction, error) {
	if solanaMarket == "" || evmMarket == "" || solanaMarket == evmMarket {
		return arbitrage.Direction{}, fmt.Errorf(
			"forced canary requires distinct Solana and EVM markets",
		)
	}
	switch d {
	case ForceCanarySolanaToEVM:
		return arbitrage.Direction{
			BuyMarket: solanaMarket, SellMarket: evmMarket,
		}, nil
	case ForceCanaryEVMToSolana:
		return arbitrage.Direction{
			BuyMarket: evmMarket, SellMarket: solanaMarket,
		}, nil
	default:
		return arbitrage.Direction{}, fmt.Errorf(
			"forced canary direction %q is invalid",
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

func isForcedCanaryOpportunity(opportunity arbitrage.Opportunity) bool {
	for _, reason := range opportunity.Reasons {
		if reason == forcedCanaryReason {
			return true
		}
	}
	return false
}
