// Command research runs the deterministic synthetic Research demonstration.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	runtimeresearch "github.com/VarozXYZ/vernier/runtime/research"
)

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("research", flag.ContinueOnError)
	flags.SetOutput(stderr)
	fixturePath := flags.String("fixture", "examples/synthetic/two-market.json", "path to the experimental fixture")
	format := flags.String("format", "text", "report format: text or json")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "research: positional arguments are not supported")
		return 2
	}
	if *format != "text" && *format != "json" {
		fmt.Fprintf(stderr, "research: unsupported format %q\n", *format)
		return 2
	}

	data, err := os.ReadFile(*fixturePath)
	if err != nil {
		fmt.Fprintf(stderr, "research: read fixture: %v\n", err)
		return 2
	}
	fixture, configHash, err := runtimeresearch.ParseFixture(data)
	if err != nil {
		fmt.Fprintf(stderr, "research: %v\n", err)
		return 2
	}
	runner, err := runtimeresearch.NewRunner(fixture, configHash)
	if err != nil {
		fmt.Fprintf(stderr, "research: invalid fixture: %v\n", err)
		return 2
	}
	report, err := runner.Run(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "research: run failed: %v\n", err)
		return 1
	}
	if *format == "json" {
		err = runtimeresearch.WriteJSON(stdout, report)
	} else {
		err = runtimeresearch.WriteText(stdout, report)
	}
	if err != nil {
		fmt.Fprintf(stderr, "research: write report: %v\n", err)
		return 1
	}
	return 0
}
