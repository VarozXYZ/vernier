package execution_test

import (
	"math/big"
	"testing"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
)

func TestRouteAllocationModelsDirectAndActualIntermediateFlow(t *testing.T) {
	allocation := execution.RouteAllocation{
		Input:          amount(t, "quote", 100),
		ExpectedOutput: amount(t, "base", 152),
		Groups: []execution.RouteGroup{
			{
				ID: "direct", InputToken: "quote", OutputToken: "base",
				Branches: []execution.RouteBranch{
					{Market: "direct-a", PlannedInput: big.NewInt(20), ExpectedOutput: big.NewInt(30)},
				},
			},
			{
				ID: "first", InputToken: "quote", OutputToken: "middle",
				Branches: []execution.RouteBranch{
					{Market: "first-a", PlannedInput: big.NewInt(50), ExpectedOutput: big.NewInt(70)},
					{Market: "first-b", PlannedInput: big.NewInt(30), ExpectedOutput: big.NewInt(40)},
				},
			},
			{
				ID: "second", Parent: "first", InputToken: "middle", OutputToken: "base",
				Branches: []execution.RouteBranch{
					{Market: "second-a", PlannedInput: big.NewInt(60), ExpectedOutput: big.NewInt(70)},
					{Market: "second-b", PlannedInput: big.NewInt(50), ExpectedOutput: big.NewInt(52)},
				},
			},
		},
	}
	if err := allocation.Validate(); err != nil {
		t.Fatal(err)
	}
	cloned := allocation.Clone()
	cloned.Groups[0].Branches[0].PlannedInput.SetInt64(1)
	if allocation.Groups[0].Branches[0].PlannedInput.Int64() != 20 {
		t.Fatal("route allocation clone aliases branch integers")
	}
}

func TestRouteAllocationRejectsUnconsumedOrDuplicatedFlow(t *testing.T) {
	base := execution.RouteAllocation{
		Input:          amount(t, "quote", 100),
		ExpectedOutput: amount(t, "base", 90),
		Groups: []execution.RouteGroup{
			{
				ID: "first", InputToken: "quote", OutputToken: "middle",
				Branches: []execution.RouteBranch{
					{Market: "first", PlannedInput: big.NewInt(100), ExpectedOutput: big.NewInt(95)},
				},
			},
			{
				ID: "second", Parent: "first", InputToken: "middle", OutputToken: "base",
				Branches: []execution.RouteBranch{
					{Market: "second", PlannedInput: big.NewInt(94), ExpectedOutput: big.NewInt(90)},
				},
			},
		},
	}
	if err := base.Validate(); err == nil {
		t.Fatal("allocation that loses parent flow was accepted")
	}
	base.Groups[1].Branches[0].PlannedInput.SetInt64(95)
	base.Groups = append(base.Groups, execution.RouteGroup{
		ID: "duplicate-child", Parent: "first", InputToken: "middle", OutputToken: "base",
		Branches: []execution.RouteBranch{
			{Market: "third", PlannedInput: big.NewInt(95), ExpectedOutput: big.NewInt(1)},
		},
	})
	if err := base.Validate(); err == nil {
		t.Fatal("allocation with two consumers for one parent was accepted")
	}
}

func amount(t *testing.T, token market.TokenID, units int64) market.TokenAmount {
	t.Helper()
	value, err := market.NewTokenAmount(token, big.NewInt(units))
	if err != nil {
		t.Fatal(err)
	}
	return value
}
