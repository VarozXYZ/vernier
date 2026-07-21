// Command research runs the deterministic synthetic Research demonstration.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/runtime/livecompare"
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
	if len(args) > 0 && args[0] == "compare-live" {
		return runCompareLive(ctx, args[1:], stdout, stderr)
	}
	return runSynthetic(ctx, args, stdout, stderr)
}

func runCompareLive(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("research compare-live", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "config/local/vernier.yaml", "path to private YAML configuration manifest")
	envPath := flags.String("env-file", ".env", "path to private environment file")
	format := flags.String("format", "text", "output format: text or json")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 || (*format != "text" && *format != "json") {
		fmt.Fprintln(stderr, "research compare-live: invalid arguments")
		return 2
	}
	if err := loadEnvFile(*envPath, os.LookupEnv, os.Setenv); err != nil {
		fmt.Fprintln(stderr, "research compare-live: cannot load private environment")
		return 2
	}
	config, err := livecompare.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "research compare-live: %v\n", err)
		return 2
	}
	endpoints, err := config.ResolveEndpoints(os.LookupEnv)
	if err != nil {
		fmt.Fprintf(stderr, "research compare-live: %v\n", err)
		return 2
	}
	networks := make(livecompare.Networks, len(config.Chains))
	for id, profile := range config.Chains {
		network, dialErr := evm.DialReadOnlyNetwork(ctx, profile.ID, profile.Label, profile.ChainID, endpoints[id], endpoints[id])
		if dialErr != nil {
			for _, opened := range networks {
				if closer, ok := opened.(interface{ Close() }); ok {
					closer.Close()
				}
			}
			fmt.Fprintf(stderr, "research compare-live: %v\n", dialErr)
			return 1
		}
		networks[id] = network
	}
	defer func() {
		for _, network := range networks {
			if closer, ok := network.(interface{ Close() }); ok {
				closer.Close()
			}
		}
	}()
	runner, err := livecompare.New(config, networks, livecompare.Options{LookupEnv: os.LookupEnv})
	if err != nil {
		fmt.Fprintf(stderr, "research compare-live: invalid composition: %v\n", err)
		return 2
	}
	report, err := runner.Run(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "research compare-live: run failed: %v\n", err)
		return 1
	}
	if *format == "json" {
		err = livecompare.WriteJSON(stdout, report)
	} else {
		err = livecompare.WriteText(stdout, report)
	}
	if err != nil {
		fmt.Fprintf(stderr, "research compare-live: write report: %v\n", err)
		return 1
	}
	return 0
}

var envKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func loadEnvFile(path string, lookup func(string) (string, bool), set func(string, string) error) error {
	if strings.TrimSpace(path) == "" || lookup == nil || set == nil {
		return fmt.Errorf("environment file path, lookup, and setter are required")
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		key, value, found := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if !found || !envKey.MatchString(key) {
			return fmt.Errorf("invalid environment entry")
		}
		if _, exists := lookup(key); exists {
			continue
		}
		value = strings.TrimSpace(value)
		if len(value) >= 2 && (value[0] == '\'' && value[len(value)-1] == '\'' ||
			value[0] == '"' && value[len(value)-1] == '"') {
			if value[0] == '\'' {
				value = value[1 : len(value)-1]
			} else {
				value, err = strconv.Unquote(value)
				if err != nil {
					return fmt.Errorf("invalid quoted environment value")
				}
			}
		}
		if err := set(key, value); err != nil {
			return err
		}
	}
	return scanner.Err()
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
	configPath := flags.String("config", "config/local/vernier.yaml", "path to private YAML configuration manifest")
	marketID := flags.String("market", "", "configured canonical Uniswap V3 market ID")
	format := flags.String("format", "text", "output format: text or jsonl")
	updates := flags.Int("updates", 0, "active pool blocks to observe; zero runs until canceled")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 || (*format != "text" && *format != "jsonl") || *updates < 0 {
		fmt.Fprintln(stderr, "research observe-v3: invalid arguments")
		return 2
	}
	bundle, err := livecompare.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "research observe-v3: %v\n", err)
		return 2
	}
	config, err := observev3.FromConfig(bundle, *marketID)
	if err != nil {
		fmt.Fprintf(stderr, "research observe-v3: %v\n", err)
		return 2
	}
	endpoints, err := config.ResolveEndpoints(os.LookupEnv)
	if err != nil {
		fmt.Fprintf(stderr, "research observe-v3: %v\n", err)
		return 2
	}
	network, err := evm.DialReadOnlyNetwork(ctx, config.Network.ID, config.Network.Label, config.Network.ChainID, endpoints.HTTP, endpoints.WS)
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
