// Command local-aggregators-quote-compare compares a local split exact-input
// quote with a best-single-route baseline and concurrent read-only providers.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/VarozXYZ/vernier/internal/localaggregatorsquotecompare"
)

func main() {
	if err := localaggregatorsquotecompare.Run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
