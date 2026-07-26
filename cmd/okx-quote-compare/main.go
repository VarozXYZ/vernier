// Command okx-quote-compare compares OKX's default route with a route limited
// to the configured Meteora and Orca liquidity-source IDs on Solana.
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

	"github.com/VarozXYZ/vernier/adapters/quote/okx"
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
	flags := flag.NewFlagSet("okx-quote-compare", flag.ContinueOnError)
	envPath := flags.String("env-file", okxexperiment.DefaultEnvFile, "environment file; default .env.test")
	samples := flags.Int("samples", 0, "number of pairs; zero uses OKX_LATENCY_SAMPLES or 20")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || *samples < 0 {
		return fmt.Errorf("usage: go run ./cmd/okx-quote-compare [-env-file .env.test] [-samples N]")
	}
	settings, err := okxexperiment.LoadSettings(*envPath)
	if err != nil {
		return err
	}
	if *samples > 0 {
		settings.Samples = *samples
	}
	ctx, stop := signal.NotifyContext(parent, os.Interrupt)
	defer stop()

	// Each variant owns one limiter. This permits the two calls in a sample to
	// start together while keeping each variant at no more than one request per
	// second; together, the quote phase uses two requests per second.
	defaultSource, err := settings.Source(pairInterval)
	if err != nil {
		return err
	}
	restrictedSource, err := settings.Source(pairInterval)
	if err != nil {
		return err
	}
	limitedRequest, err := settings.RestrictedRequest()
	if err != nil {
		return err
	}
	fmt.Printf("restricted_dex_ids=%s\n", limitedRequest.DexIDs)
	fmt.Println("delta_definition=default_output-restricted_output positive_means_default_more_output")

	defaultLatencies := make([]time.Duration, 0, settings.Samples)
	restrictedLatencies := make([]time.Duration, 0, settings.Samples)
	defaultAmounts := make([]string, 0, settings.Samples)
	restrictedAmounts := make([]string, 0, settings.Samples)
	amountDeltas := make([]string, 0, settings.Samples)
	defaultAmountDecimals, restrictedAmountDecimals, deltaDecimals := "", "", ""
	defaultAmountUnit, restrictedAmountUnit := "", ""
	defaultSuccesses, defaultErrors := 0, 0
	restrictedSuccesses, restrictedErrors := 0, 0
	nextStart := time.Now()
	for index := 0; index < settings.Samples; index++ {
		if err := waitUntil(ctx, nextStart); err != nil {
			return err
		}
		started := time.Now()
		defaultMeasurement, restrictedMeasurement := quotePair(ctx, defaultSource, restrictedSource, settings.Request(), limitedRequest)
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
		}
		if restrictedMeasurement.err != nil {
			restrictedErrors++
		} else {
			restrictedSuccesses++
		}
		defaultRawAmount, defaultDecimals, defaultUnit, defaultAmountErr := outputAmount(defaultMeasurement.result, settings.ToToken)
		if defaultMeasurement.err == nil && defaultAmountErr == nil {
			if defaultAmountDecimals == "" {
				defaultAmountDecimals = defaultDecimals
				defaultAmountUnit = defaultUnit
			}
			if defaultDecimals == defaultAmountDecimals {
				defaultAmounts = append(defaultAmounts, defaultRawAmount)
			}
		}
		restrictedRawAmount, restrictedDecimals, restrictedUnit, restrictedAmountErr := outputAmount(restrictedMeasurement.result, settings.ToToken)
		if restrictedMeasurement.err == nil && restrictedAmountErr == nil {
			if restrictedAmountDecimals == "" {
				restrictedAmountDecimals = restrictedDecimals
				restrictedAmountUnit = restrictedUnit
			}
			if restrictedDecimals == restrictedAmountDecimals {
				restrictedAmounts = append(restrictedAmounts, restrictedRawAmount)
			}
		}
		if defaultMeasurement.err == nil && restrictedMeasurement.err == nil && defaultAmountErr == nil && restrictedAmountErr == nil && defaultDecimals == restrictedDecimals {
			delta, deltaErr := okxexperiment.SubtractBaseUnits(defaultRawAmount, restrictedRawAmount)
			if deltaErr == nil {
				if deltaDecimals == "" {
					deltaDecimals = defaultDecimals
				}
				if defaultDecimals == deltaDecimals {
					amountDeltas = append(amountDeltas, delta)
				}
			}
		}

		printSample(index+1, settings.ToToken, defaultMeasurement, restrictedMeasurement)
	}

	if len(defaultLatencies) == 0 || len(restrictedLatencies) == 0 {
		return fmt.Errorf("no complete latency samples collected: default=%d restricted=%d", len(defaultLatencies), len(restrictedLatencies))
	}
	defaultStats := summarize(defaultLatencies)
	restrictedStats := summarize(restrictedLatencies)
	fmt.Printf("summary variant=default samples=%d successes=%d errors=%d min=%s mean=%s p50=%s p95=%s p99=%s max=%s\n", len(defaultLatencies), defaultSuccesses, defaultErrors, defaultStats.Min, defaultStats.Mean, defaultStats.P50, defaultStats.P95, defaultStats.P99, defaultStats.Max)
	fmt.Printf("summary variant=meteora_orca samples=%d successes=%d errors=%d min=%s mean=%s p50=%s p95=%s p99=%s max=%s\n", len(restrictedLatencies), restrictedSuccesses, restrictedErrors, restrictedStats.Min, restrictedStats.Mean, restrictedStats.P50, restrictedStats.P95, restrictedStats.P99, restrictedStats.Max)
	if err := printAmountSummary("default", defaultAmounts, defaultAmountDecimals, defaultAmountUnit); err != nil {
		return err
	}
	if err := printAmountSummary("meteora_orca", restrictedAmounts, restrictedAmountDecimals, restrictedAmountUnit); err != nil {
		return err
	}
	if err := printAmountSummary("delta(default-restricted)", amountDeltas, deltaDecimals, defaultAmountUnit); err != nil {
		return err
	}
	return nil
}

type measurement struct {
	result    okx.Result
	err       error
	startedAt time.Time
}

func quotePair(ctx context.Context, defaultSource, restrictedSource *okx.Source, defaultRequest, restrictedRequest okx.QuoteRequest) (measurement, measurement) {
	var results [2]measurement
	var wait sync.WaitGroup
	start := make(chan struct{})
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		results[0].startedAt = time.Now()
		results[0].result, results[0].err = defaultSource.Quote(ctx, defaultRequest)
	}()
	go func() {
		defer wait.Done()
		<-start
		results[1].startedAt = time.Now()
		results[1].result, results[1].err = restrictedSource.Quote(ctx, restrictedRequest)
	}()
	close(start)
	wait.Wait()
	return results[0], results[1]
}

func printSample(index int, toTokenAddress string, defaultMeasurement, restrictedMeasurement measurement) {
	startSkew := defaultMeasurement.startedAt.Sub(restrictedMeasurement.startedAt)
	if startSkew < 0 {
		startSkew = -startSkew
	}
	defaultOutput, defaultUnit, defaultDecimals, defaultOutputErr := humanOutput(defaultMeasurement.result, toTokenAddress)
	restrictedOutput, restrictedUnit, restrictedDecimals, restrictedOutputErr := humanOutput(restrictedMeasurement.result, toTokenAddress)

	defaultText := "n/a"
	if defaultMeasurement.err != nil {
		defaultText = "error=" + defaultMeasurement.err.Error()
	} else if defaultOutputErr != nil {
		defaultText = "output_error=" + defaultOutputErr.Error()
	} else {
		defaultText = defaultOutput + unitSuffix(defaultUnit)
	}
	restrictedText := "n/a"
	if restrictedMeasurement.err != nil {
		restrictedText = "error=" + restrictedMeasurement.err.Error()
	} else if restrictedOutputErr != nil {
		restrictedText = "output_error=" + restrictedOutputErr.Error()
	} else {
		restrictedText = restrictedOutput + unitSuffix(restrictedUnit)
	}

	deltaText := "n/a"
	if defaultMeasurement.err == nil && restrictedMeasurement.err == nil && defaultOutputErr == nil && restrictedOutputErr == nil && defaultDecimals == restrictedDecimals {
		delta, err := okxexperiment.DifferenceBaseUnits(defaultMeasurement.result.ToTokenAmount, restrictedMeasurement.result.ToTokenAmount, defaultDecimals)
		if err == nil {
			deltaText = signed(delta) + unitSuffix(defaultUnit)
		}
	}
	fmt.Printf("sample=%d start_skew=%s default_output=%s default_http=%s default_total=%s restricted_output=%s restricted_http=%s restricted_total=%s output_delta(default-restricted)=%s\n", index, startSkew, defaultText, defaultMeasurement.result.HTTPDuration, defaultMeasurement.result.TotalDuration, restrictedText, restrictedMeasurement.result.HTTPDuration, restrictedMeasurement.result.TotalDuration, deltaText)
}

func humanOutput(result okx.Result, address string) (string, string, string, error) {
	rawAmount, decimals, symbol, err := outputAmount(result, address)
	if err != nil {
		return "", "", "", err
	}
	formatted, err := okxexperiment.FormatBaseUnits(rawAmount, decimals)
	return formatted, symbol, decimals, err
}

func outputAmount(result okx.Result, address string) (string, string, string, error) {
	decimals, symbol, err := outputTokenMetadata(result, address)
	if err != nil {
		return "", "", "", err
	}
	return result.ToTokenAmount, decimals, symbol, nil
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

func outputTokenMetadata(result okx.Result, address string) (string, string, error) {
	address = strings.TrimSpace(address)
	if result.ToToken.Decimal != "" && strings.EqualFold(strings.TrimSpace(result.ToToken.Address), address) {
		return result.ToToken.Decimal, result.ToToken.Symbol, nil
	}
	var fallback okx.TokenInfo
	for _, route := range result.Routes {
		for _, subRoute := range route.SubRoutes {
			token := subRoute.ToToken
			if token.Decimal == "" {
				continue
			}
			if fallback.Decimal == "" {
				fallback = token
			}
			if strings.EqualFold(strings.TrimSpace(token.Address), address) {
				return token.Decimal, token.Symbol, nil
			}
		}
	}
	if fallback.Decimal != "" {
		return fallback.Decimal, fallback.Symbol, nil
	}
	return "", "", fmt.Errorf("quote did not include output-token decimals")
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
