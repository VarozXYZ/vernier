// Command local-grid-hour-compare keeps local pool watchers open for one hour
// and compares verified and latency-targeted split profiles every five minutes.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/VarozXYZ/vernier/internal/localaggregatorsquotecompare"
)

func main() {
	if err := localaggregatorsquotecompare.RunHourLocalProfile(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
