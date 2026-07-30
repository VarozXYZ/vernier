package main

import (
	"context"
	"os"

	"github.com/VarozXYZ/vernier/internal/nttmanualcanary"
)

func main() {
	os.Exit(nttmanualcanary.Run(
		context.Background(), os.Args[1:], os.Stdout, os.Stderr,
	))
}
