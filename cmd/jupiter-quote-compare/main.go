// Command jupiter-quote-compare compares Jupiter's default route with a
// Jupiter route restricted to the configured Meteora and Orca DEX labels.
// It is intentionally separate from tests and Research runtime.
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/adapters/quote/jupiter"
	"github.com/VarozXYZ/vernier/cmd/okxexperiment"
)

const pairInterval = time.Second

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(parent context.Context, args []string) error {
	flags := flag.NewFlagSet("jupiter-quote-compare", flag.ContinueOnError)
	envPath := flags.String("env-file", okxexperiment.DefaultEnvFile, "environment file; default .env.test")
	samples := flags.Int("samples", 0, "number of pairs; zero uses OKX_LATENCY_SAMPLES or 20")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || *samples < 0 {
		return fmt.Errorf("usage: go run ./cmd/jupiter-quote-compare [-env-file .env.test] [-samples N]")
	}
	settings, err := okxexperiment.LoadSettings(*envPath)
	if err != nil {
		return err
	}
	if *samples > 0 {
		settings.Samples = *samples
	}
	if strings.TrimSpace(settings.JupiterOutputDecimals) == "" {
		return fmt.Errorf("missing JUPITER_OUTPUT_DECIMALS in the environment")
	}
	ctx, stop := signal.NotifyContext(parent, os.Interrupt)
	defer stop()

	defaultSource, err := settings.JupiterSource(pairInterval)
	if err != nil {
		return err
	}
	restrictedSource, err := settings.JupiterSource(pairInterval)
	if err != nil {
		return err
	}
	defaultRequest := settings.JupiterRequest()
	restrictedRequest, err := settings.JupiterRestrictedRequest()
	if err != nil {
		return err
	}

	fmt.Printf("restricted_dexes=%s\n", restrictedRequest.Dexes)
	fmt.Println("route_definition=default_jupiter_vs_restricted_jupiter")
	fmt.Println("delta_definition=default_output-restricted_output positive_means_default_more_output")
	fmt.Println("provider_requests_per_second=2 (one default and one restricted request per second)")

	defaultLatencies := make([]time.Duration, 0, settings.Samples)
	restrictedLatencies := make([]time.Duration, 0, settings.Samples)
	defaultAmounts := make([]string, 0, settings.Samples)
	restrictedAmounts := make([]string, 0, settings.Samples)
	amountDeltas := make([]string, 0, settings.Samples)
	defaultSuccesses, defaultErrors := 0, 0
	restrictedSuccesses, restrictedErrors := 0, 0
	nextStart := time.Now()

	for index := 0; index < settings.Samples; index++ {
		if err := waitUntil(ctx, nextStart); err != nil {
			return err
		}
		started := time.Now()
		defaultMeasurement, restrictedMeasurement := quotePair(ctx, defaultSource, restrictedSource, defaultRequest, restrictedRequest)
		nextStart = started.Add(pairInterval)

		if defaultMeasurement.result.HTTPDuration > 0 {
			defaultLatencies = append(defaultLatencies, defaultMeasurement.result.HTTPDuration)
		}
		if restrictedMeasurement.result.HTTPDuration > 0 {
			restrictedLatencies = append(restrictedLatencies, restrictedMeasurement.result.HTTPDuration)
		}
		if defaultMeasurement.err != nil {
			defaultErrors++
		} else {
			defaultSuccesses++
			defaultAmounts = append(defaultAmounts, defaultMeasurement.result.ToTokenAmount)
		}
		if restrictedMeasurement.err != nil {
			restrictedErrors++
		} else {
			restrictedSuccesses++
			restrictedAmounts = append(restrictedAmounts, restrictedMeasurement.result.ToTokenAmount)
		}
		if defaultMeasurement.err == nil && restrictedMeasurement.err == nil {
			if delta, deltaErr := okxexperiment.SubtractBaseUnits(defaultMeasurement.result.ToTokenAmount, restrictedMeasurement.result.ToTokenAmount); deltaErr == nil {
				amountDeltas = append(amountDeltas, delta)
			}
		}

		printSample(index+1, settings.JupiterOutputDecimals, settings.JupiterOutputSymbol, defaultMeasurement, restrictedMeasurement)
	}

	if len(defaultLatencies) == 0 || len(restrictedLatencies) == 0 {
		return fmt.Errorf("no complete latency samples collected: default=%d restricted=%d", len(defaultLatencies), len(restrictedLatencies))
	}
	printLatencySummary("default", defaultLatencies, defaultSuccesses, defaultErrors)
	printLatencySummary("meteora_orca", restrictedLatencies, restrictedSuccesses, restrictedErrors)
	if err := printAmountSummary("default", defaultAmounts, settings.JupiterOutputDecimals, settings.JupiterOutputSymbol); err != nil {
		return err
	}
	if err := printAmountSummary("meteora_orca", restrictedAmounts, settings.JupiterOutputDecimals, settings.JupiterOutputSymbol); err != nil {
		return err
	}
	if err := printAmountSummary("delta(default-restricted)", amountDeltas, settings.JupiterOutputDecimals, settings.JupiterOutputSymbol); err != nil {
		return err
	}
	return nil
}

type measurement struct {
	result    jupiter.QuoteResult
	err       error
	startedAt time.Time
}

func quotePair(ctx context.Context, defaultSource, restrictedSource *jupiter.QuoteSource, defaultRequest, restrictedRequest jupiter.QuoteRequest) (measurement, measurement) {
	var defaultMeasurement measurement
	var restrictedMeasurement measurement
	var wait sync.WaitGroup
	start := make(chan struct{})
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		defaultMeasurement.startedAt = time.Now()
		defaultMeasurement.result, defaultMeasurement.err = defaultSource.Quote(ctx, defaultRequest)
	}()
	go func() {
		defer wait.Done()
		<-start
		restrictedMeasurement.startedAt = time.Now()
		restrictedMeasurement.result, restrictedMeasurement.err = restrictedSource.Quote(ctx, restrictedRequest)
	}()
	close(start)
	wait.Wait()
	return defaultMeasurement, restrictedMeasurement
}

func printSample(index int, decimals, unit string, defaultMeasurement, restrictedMeasurement measurement) {
	startSkew := defaultMeasurement.startedAt.Sub(restrictedMeasurement.startedAt)
	if startSkew < 0 {
		startSkew = -startSkew
	}
	defaultText := quoteOutput(defaultMeasurement.err, defaultMeasurement.result.ToTokenAmount, decimals, unit)
	restrictedText := quoteOutput(restrictedMeasurement.err, restrictedMeasurement.result.ToTokenAmount, decimals, unit)
	deltaText := "n/a"
	if defaultMeasurement.err == nil && restrictedMeasurement.err == nil {
		if delta, err := okxexperiment.DifferenceBaseUnits(defaultMeasurement.result.ToTokenAmount, restrictedMeasurement.result.ToTokenAmount, decimals); err == nil {
			deltaText = signed(delta) + unitSuffix(unit)
		}
	}
	fmt.Printf("sample=%d start_skew=%s default_output=%s default_http=%s default_total=%s restricted_output=%s restricted_http=%s restricted_total=%s output_delta(default-restricted)=%s\n", index, startSkew, defaultText, defaultMeasurement.result.HTTPDuration, defaultMeasurement.result.TotalDuration, restrictedText, restrictedMeasurement.result.HTTPDuration, restrictedMeasurement.result.TotalDuration, deltaText)
}

func quoteOutput(err error, rawAmount, decimals, unit string) string {
	if err != nil {
		return "error=" + err.Error()
	}
	formatted, formatErr := okxexperiment.FormatBaseUnits(rawAmount, decimals)
	if formatErr != nil {
		return "output_error=" + formatErr.Error()
	}
	return formatted + unitSuffix(unit)
}

func printLatencySummary(variant string, values []time.Duration, successes, errors int) {
	stats := summarize(values)
	fmt.Printf("summary variant=%s samples=%d successes=%d errors=%d min=%s mean=%s p50=%s p95=%s p99=%s max=%s\n", variant, len(values), successes, errors, stats.Min, stats.Mean, stats.P50, stats.P95, stats.P99, stats.Max)
}

func printAmountSummary(variant string, values []string, decimals, unit string) error {
	if len(values) == 0 {
		return fmt.Errorf("no amount samples collected for %s", variant)
	}
	statistics, err := okxexperiment.SummarizeBaseUnits(values, decimals)
	if err != nil {
		return fmt.Errorf("summarize %s amounts: %w", variant, err)
	}
	fmt.Printf("summary amounts variant=%s samples=%d unit=%s min=%s mean=%s p50=%s p95=%s p99=%s max=%s\n", variant, statistics.Samples, unit, statistics.Min, statistics.Mean, statistics.P50, statistics.P95, statistics.P99, statistics.Max)
	return nil
}

func unitSuffix(unit string) string {
	if strings.TrimSpace(unit) == "" {
		return ""
	}
	return " " + unit
}

func signed(value string) string {
	if strings.Trim(value, "0.") == "" || strings.HasPrefix(value, "-") {
		return value
	}
	return "+" + value
}

func waitUntil(ctx context.Context, target time.Time) error {
	delay := time.Until(target)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type statistics struct {
	Min, Mean, P50, P95, P99, Max time.Duration
}

func summarize(values []time.Duration) statistics {
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var total time.Duration
	for _, value := range sorted {
		total += value
	}
	return statistics{Min: sorted[0], Mean: total / time.Duration(len(sorted)), P50: percentile(sorted, 0.50), P95: percentile(sorted, 0.95), P99: percentile(sorted, 0.99), Max: sorted[len(sorted)-1]}
}

func percentile(sorted []time.Duration, fraction float64) time.Duration {
	index := int(math.Ceil(float64(len(sorted))*fraction)) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}
