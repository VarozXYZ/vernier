// Command aggregator-latency-compare measures unsigned route/build latency
// and exact-input output differences between two EVM aggregators.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/VarozXYZ/vernier/internal/aggregatorcompare"
)

func main() {
	if err := aggregatorcompare.Run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
