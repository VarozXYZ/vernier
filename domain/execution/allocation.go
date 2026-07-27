package execution

import (
	"fmt"
	"math/big"

	"github.com/VarozXYZ/vernier/domain/market"
)

type AllocationGroupID string

// RouteBranch is one optimizer-selected market inside a parallel allocation
// group. PlannedInput is also the exact integer weight used when the group
// consumes an actual parent output that differs from the quoted amount.
type RouteBranch struct {
	Market         market.MarketID
	PlannedInput   *big.Int
	ExpectedOutput *big.Int
}

// RouteGroup consumes either part of the leg input (Parent is empty) or the
// complete actual output of Parent. Every branch in a group has the same token
// direction and runs in parallel.
type RouteGroup struct {
	ID          AllocationGroupID
	Parent      AllocationGroupID
	InputToken  market.TokenID
	OutputToken market.TokenID
	Branches    []RouteBranch
}

// RouteAllocation is the protocol-neutral executable graph produced by local
// sizing. It contains stable domain IDs and integer amounts, never addresses,
// ABI values, or provider payloads.
type RouteAllocation struct {
	Input          market.TokenAmount
	ExpectedOutput market.TokenAmount
	Groups         []RouteGroup
}

func (a RouteAllocation) Validate() error {
	if a.Input.Token() == "" || a.Input.IsZero() ||
		a.ExpectedOutput.Token() == "" || a.ExpectedOutput.IsZero() ||
		len(a.Groups) == 0 {
		return fmt.Errorf("route allocation requires positive endpoints and groups")
	}
	groups := make(map[AllocationGroupID]RouteGroup, len(a.Groups))
	children := make(map[AllocationGroupID]int, len(a.Groups))
	rootInput := new(big.Int)
	terminalOutput := new(big.Int)
	for index, group := range a.Groups {
		if group.ID == "" || group.InputToken == "" || group.OutputToken == "" ||
			group.InputToken == group.OutputToken || len(group.Branches) == 0 {
			return fmt.Errorf("route allocation group %d is incomplete", index)
		}
		if _, exists := groups[group.ID]; exists {
			return fmt.Errorf("route allocation repeats group %q", group.ID)
		}
		if group.Parent == group.ID {
			return fmt.Errorf("route allocation group %q is its own parent", group.ID)
		}
		plannedInput := new(big.Int)
		expectedOutput := new(big.Int)
		seenMarkets := make(map[market.MarketID]struct{}, len(group.Branches))
		for branchIndex, branch := range group.Branches {
			if branch.Market == "" || branch.PlannedInput == nil || branch.PlannedInput.Sign() <= 0 ||
				branch.ExpectedOutput == nil || branch.ExpectedOutput.Sign() <= 0 {
				return fmt.Errorf("route allocation group %q branch %d is invalid", group.ID, branchIndex)
			}
			if _, exists := seenMarkets[branch.Market]; exists {
				return fmt.Errorf("route allocation group %q repeats market %q", group.ID, branch.Market)
			}
			seenMarkets[branch.Market] = struct{}{}
			plannedInput.Add(plannedInput, branch.PlannedInput)
			expectedOutput.Add(expectedOutput, branch.ExpectedOutput)
		}
		if group.Parent == "" {
			if group.InputToken != a.Input.Token() {
				return fmt.Errorf("route allocation root %q input token does not match leg input", group.ID)
			}
			rootInput.Add(rootInput, plannedInput)
		} else {
			parent, exists := groups[group.Parent]
			if !exists {
				return fmt.Errorf("route allocation group %q parent must precede it", group.ID)
			}
			if parent.OutputToken != group.InputToken {
				return fmt.Errorf("route allocation group %q is discontinuous from parent", group.ID)
			}
			parentOutput := sumExpectedOutput(parent.Branches)
			if plannedInput.Cmp(parentOutput) != 0 {
				return fmt.Errorf("route allocation group %q does not consume its quoted parent output", group.ID)
			}
			children[group.Parent]++
			if children[group.Parent] > 1 {
				return fmt.Errorf("route allocation group %q output has multiple consumers", group.Parent)
			}
		}
		groups[group.ID] = group
	}
	if rootInput.Cmp(a.Input.Units()) != 0 {
		return fmt.Errorf("route allocation root groups do not conserve leg input")
	}
	for _, group := range a.Groups {
		if children[group.ID] != 0 {
			continue
		}
		if group.OutputToken != a.ExpectedOutput.Token() {
			return fmt.Errorf("route allocation terminal group %q has unexpected output token", group.ID)
		}
		terminalOutput.Add(terminalOutput, sumExpectedOutput(group.Branches))
	}
	if terminalOutput.Cmp(a.ExpectedOutput.Units()) != 0 {
		return fmt.Errorf("route allocation terminal groups do not match expected output")
	}
	return nil
}

func (a RouteAllocation) Clone() RouteAllocation {
	result := RouteAllocation{Input: a.Input, ExpectedOutput: a.ExpectedOutput, Groups: make([]RouteGroup, len(a.Groups))}
	for index, group := range a.Groups {
		result.Groups[index] = RouteGroup{
			ID: group.ID, Parent: group.Parent, InputToken: group.InputToken,
			OutputToken: group.OutputToken, Branches: make([]RouteBranch, len(group.Branches)),
		}
		for branchIndex, branch := range group.Branches {
			result.Groups[index].Branches[branchIndex] = RouteBranch{
				Market: branch.Market, PlannedInput: cloneInt(branch.PlannedInput),
				ExpectedOutput: cloneInt(branch.ExpectedOutput),
			}
		}
	}
	return result
}

func sumExpectedOutput(branches []RouteBranch) *big.Int {
	total := new(big.Int)
	for _, branch := range branches {
		if branch.ExpectedOutput != nil {
			total.Add(total, branch.ExpectedOutput)
		}
	}
	return total
}

func cloneInt(value *big.Int) *big.Int {
	if value == nil {
		return nil
	}
	return new(big.Int).Set(value)
}
