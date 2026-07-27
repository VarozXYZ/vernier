package sizing

import (
	"fmt"
	"math/big"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
)

const (
	directGroupID execution.AllocationGroupID = "direct"
	firstGroupID  execution.AllocationGroupID = "intermediate_input"
	secondGroupID execution.AllocationGroupID = "intermediate_output"
)

// BuildRouteAllocation converts the optimizer's exact integer result into the
// protocol-neutral graph consumed by Live execution.
func BuildRouteAllocation(
	result TwoStageSplitResult,
	inputToken, intermediateToken, outputToken market.TokenID,
) (execution.RouteAllocation, error) {
	if inputToken == "" || outputToken == "" || inputToken == outputToken ||
		result.TotalInput == nil || result.TotalOutput == nil {
		return execution.RouteAllocation{}, fmt.Errorf("execution allocation endpoints and result are required")
	}
	input, err := market.NewTokenAmount(inputToken, result.TotalInput)
	if err != nil {
		return execution.RouteAllocation{}, err
	}
	output, err := market.NewTokenAmount(outputToken, result.TotalOutput)
	if err != nil {
		return execution.RouteAllocation{}, err
	}
	allocation := execution.RouteAllocation{Input: input, ExpectedOutput: output}
	if branches := executionBranches(result.Direct); len(branches) > 0 {
		allocation.Groups = append(allocation.Groups, execution.RouteGroup{
			ID: directGroupID, InputToken: inputToken, OutputToken: outputToken, Branches: branches,
		})
	}
	first := executionBranches(result.FirstStage)
	second := executionBranches(result.SecondStage)
	if len(first) > 0 || len(second) > 0 {
		if intermediateToken == "" || intermediateToken == inputToken || intermediateToken == outputToken ||
			len(first) == 0 || len(second) == 0 {
			return execution.RouteAllocation{}, fmt.Errorf("execution allocation has an incomplete intermediate branch")
		}
		allocation.Groups = append(allocation.Groups,
			execution.RouteGroup{
				ID: firstGroupID, InputToken: inputToken, OutputToken: intermediateToken, Branches: first,
			},
			execution.RouteGroup{
				ID: secondGroupID, Parent: firstGroupID,
				InputToken: intermediateToken, OutputToken: outputToken, Branches: second,
			},
		)
	}
	if err := allocation.Validate(); err != nil {
		return execution.RouteAllocation{}, err
	}
	return allocation, nil
}

func executionBranches(allocations []SplitAllocation) []execution.RouteBranch {
	result := make([]execution.RouteBranch, 0, len(allocations))
	for _, allocation := range allocations {
		if allocation.AmountIn == nil || allocation.AmountIn.Sign() == 0 {
			continue
		}
		result = append(result, execution.RouteBranch{
			Market:         market.MarketID(allocation.CurveID),
			PlannedInput:   new(big.Int).Set(allocation.AmountIn),
			ExpectedOutput: new(big.Int).Set(allocation.AmountOut),
		})
	}
	return result
}
