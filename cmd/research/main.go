// Command research runs the deterministic synthetic Research demonstration.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/VarozXYZ/vernier/adapters/chain/ethereum"
	"github.com/VarozXYZ/vernier/runtime/observev3"
	runtimeresearch "github.com/VarozXYZ/vernier/runtime/research"
)

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && args[0] == "observe-v3" {
		return runObserveV3(ctx, args[1:], stdout, stderr)
	}
	return runSynthetic(ctx, args, stdout, stderr)
}

func runSynthetic(ctx context.Context, args []string, stdout, stderr io.Writer) int {
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

func runObserveV3(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("research observe-v3", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "config/local/pool.local.json", "path to private pool configuration")
	format := flags.String("format", "text", "output format: text or jsonl")
	updates := flags.Int("updates", 0, "active pool blocks to observe; zero runs until canceled")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 || (*format != "text" && *format != "jsonl") || *updates < 0 {
		fmt.Fprintln(stderr, "research observe-v3: invalid arguments")
		return 2
	}
	data, err := os.ReadFile(*configPath)
	if err != nil {
		fmt.Fprintln(stderr, "research observe-v3: cannot read private configuration")
		return 2
	}
	config, err := observev3.ParseConfig(data)
	if err != nil {
		fmt.Fprintf(stderr, "research observe-v3: %v\n", err)
		return 2
	}
	endpoints, err := config.ResolveEndpoints(os.LookupEnv)
	if err != nil {
		fmt.Fprintf(stderr, "research observe-v3: %v\n", err)
		return 2
	}
	network, err := ethereum.Dial(ctx, endpoints.HTTP, endpoints.WS)
	if err != nil {
		fmt.Fprintf(stderr, "research observe-v3: %v\n", err)
		return 1
	}
	defer network.Close()
	observer, err := observev3.New(config, network, observev3.Options{
		Format: *format, Updates: *updates, Output: stdout,
	})
	if err != nil {
		fmt.Fprintf(stderr, "research observe-v3: invalid composition: %v\n", err)
		return 2
	}
	if err := observer.Run(ctx); err != nil {
		fmt.Fprintf(stderr, "research observe-v3: observation failed: %v\n", err)
		return 1
	}
	return 0
}
