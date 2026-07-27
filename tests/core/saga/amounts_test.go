package saga_test

import (
	"math/big"
	"testing"

	"github.com/VarozXYZ/vernier/core/saga"
)

func TestActualBalanceDeltaFeedsNextSerialHop(t *testing.T) {
	output, err := saga.BalanceDelta(big.NewInt(7), big.NewInt(107))
	if err != nil {
		t.Fatal(err)
	}
	if output.String() != "100" {
		t.Fatalf("actual output = %s", output)
	}
}

func TestLastSplitBranchReceivesRemainder(t *testing.T) {
	branches, err := saga.SplitUnits(big.NewInt(101), []uint16{3_333, 3_333, 3_334})
	if err != nil {
		t.Fatal(err)
	}
	if branches[0].String() != "33" || branches[1].String() != "33" || branches[2].String() != "35" {
		t.Fatalf("branches = %v", branches)
	}
	sum := new(big.Int)
	for _, branch := range branches {
		sum.Add(sum, branch)
	}
	if sum.String() != "101" {
		t.Fatalf("split lost units: %s", sum)
	}
}

func TestArbitraryOptimizerWeightsPreserveRemainder(t *testing.T) {
	branches, err := saga.SplitByWeights(
		big.NewInt(101),
		[]*big.Int{big.NewInt(1), big.NewInt(1), big.NewInt(1)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if branches[0].String() != "33" || branches[1].String() != "33" || branches[2].String() != "35" {
		t.Fatalf("branches = %v", branches)
	}
}
