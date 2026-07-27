package localquoteexperiment_test

import (
	"testing"

	"github.com/VarozXYZ/vernier/cmd/localquoteexperiment"
)

func TestBuildRoutesIncludesDirectAndEveryTwoHopCombination(t *testing.T) {
	pools := []localquoteexperiment.Pool{
		{Token0: "output", Token1: "input"},
		{Token0: "input", Token1: "output"},
		{Token0: "input", Token1: "middle"},
		{Token0: "middle", Token1: "input"},
		{Token0: "middle", Token1: "output"},
		{Token0: "output", Token1: "middle"},
		{Token0: "middle", Token1: "output"},
		{Token0: "output", Token1: "middle"},
	}
	routes, err := localquoteexperiment.BuildRoutes(pools, "input", "middle", "output")
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 10 {
		t.Fatalf("got %d routes, want 2 direct + 2*4 two-hop = 10", len(routes))
	}
	if len(routes[0].PoolIndexes) != 1 || len(routes[1].PoolIndexes) != 1 {
		t.Fatal("direct routes were not emitted first")
	}
	for _, candidate := range routes[2:] {
		if len(candidate.PoolIndexes) != 2 {
			t.Fatalf("unexpected route length: %+v", candidate)
		}
	}
}

func TestBuildRoutesRejectsDisconnectedPool(t *testing.T) {
	_, err := localquoteexperiment.BuildRoutes(
		[]localquoteexperiment.Pool{{Token0: "input", Token1: "unknown"}},
		"input", "middle", "output",
	)
	if err == nil {
		t.Fatal("disconnected pool was accepted")
	}
}
