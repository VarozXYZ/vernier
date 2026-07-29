package livesequential

import (
	"fmt"
	"strings"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
)

type ForcedDirection string

const (
	ForceFirstToSecond ForcedDirection = "first-to-second"
	ForceSecondToFirst ForcedDirection = "second-to-first"

	forcedReason = "forced_sequential_direction"
)

func ParseForcedDirection(value string) (ForcedDirection, error) {
	normalized := ForcedDirection(strings.ToLower(strings.TrimSpace(value)))
	switch normalized {
	case "":
		return "", nil
	case ForceFirstToSecond, ForceSecondToFirst:
		return normalized, nil
	default:
		return "", fmt.Errorf(
			"forced direction must be %q or %q",
			ForceFirstToSecond,
			ForceSecondToFirst,
		)
	}
}

func (d ForcedDirection) Resolve(
	firstMarket market.MarketID,
	secondMarket market.MarketID,
) (arbitrage.Direction, error) {
	if firstMarket == "" || secondMarket == "" ||
		firstMarket == secondMarket {
		return arbitrage.Direction{}, fmt.Errorf(
			"forced execution requires two distinct markets",
		)
	}
	switch d {
	case ForceFirstToSecond:
		return arbitrage.Direction{
			BuyMarket: firstMarket, SellMarket: secondMarket,
		}, nil
	case ForceSecondToFirst:
		return arbitrage.Direction{
			BuyMarket: secondMarket, SellMarket: firstMarket,
		}, nil
	default:
		return arbitrage.Direction{}, fmt.Errorf(
			"forced direction %q is invalid",
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
				"forced direction %s -> %s has no complete fresh round trip",
				direction.BuyMarket,
				direction.SellMarket,
			)
		}
		opportunity.SelectedIndex = 0
		opportunity.Classification =
			arbitrage.ClassificationPolicyQualified
		opportunity.Reasons = []string{forcedReason}
		return opportunity, nil
	}
	return arbitrage.Opportunity{}, fmt.Errorf(
		"forced direction %s -> %s was not evaluated",
		direction.BuyMarket,
		direction.SellMarket,
	)
}

func IsForcedOpportunity(opportunity arbitrage.Opportunity) bool {
	for _, reason := range opportunity.Reasons {
		if reason == forcedReason {
			return true
		}
	}
	return false
}
