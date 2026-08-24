// Command wtt-bridge-canary executes or resumes one explicitly armed
// Wormhole Wrapped Token Transfer between two configured EVM chains.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/VarozXYZ/vernier/internal/wttbridgecanary"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	os.Exit(wttbridgecanary.Run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}
