package main

import (
	"context"
	"os"

	"github.com/VarozXYZ/vernier/internal/acrossbridgecanary"
)

func main() {
	os.Exit(acrossbridgecanary.Run(
		context.Background(), os.Args[1:], os.Stdout, os.Stderr,
	))
}
