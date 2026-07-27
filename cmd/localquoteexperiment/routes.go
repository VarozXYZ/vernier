// Package localquoteexperiment contains deterministic, provider-neutral
// helpers for standalone local pool comparison commands.
package localquoteexperiment

import "fmt"

type Pool struct {
	Token0 string
	Token1 string
}

type Route struct {
	PoolIndexes []int
}

// BuildRoutes returns every direct route plus the Cartesian product of the
// two legs through one configured intermediate token. It never splits an
// amount across routes.
func BuildRoutes(pools []Pool, input, intermediate, output string) ([]Route, error) {
	if input == "" || intermediate == "" || output == "" ||
		input == intermediate || input == output || intermediate == output {
		return nil, fmt.Errorf("three distinct route tokens are required")
	}
	var direct, first, second []int
	for index, pool := range pools {
		switch {
		case hasEndpoints(pool, input, output):
			direct = append(direct, index)
		case hasEndpoints(pool, input, intermediate):
			first = append(first, index)
		case hasEndpoints(pool, intermediate, output):
			second = append(second, index)
		default:
			return nil, fmt.Errorf("pool %d is disconnected from route graph", index)
		}
	}
	result := make([]Route, 0, len(direct)+len(first)*len(second))
	for _, index := range direct {
		result = append(result, Route{PoolIndexes: []int{index}})
	}
	for _, left := range first {
		for _, right := range second {
			result = append(result, Route{PoolIndexes: []int{left, right}})
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("pool graph contains no complete route")
	}
	return result, nil
}

func hasEndpoints(pool Pool, first, second string) bool {
	return pool.Token0 == first && pool.Token1 == second ||
		pool.Token0 == second && pool.Token1 == first
}
