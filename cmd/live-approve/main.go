// Command live-approve audits and, behind an explicit arm barrier, grants the
// Polygon ERC-20 allowances required by the configured Live setup.
package main

import (
	"context"
	"os"

	"github.com/VarozXYZ/vernier/internal/liveapproval"
)

func main() {
	os.Exit(liveapproval.Run(
		context.Background(), os.Args[1:], os.Stdout, os.Stderr,
	))
}
