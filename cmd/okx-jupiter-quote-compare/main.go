// Command okx-jupiter-quote-compare compares the default read-only quote from
// OKX with the default read-only quote from Jupiter on Solana.
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
	"github.com/VarozXYZ/vernier/adapters/quote/okx"
	"github.com/VarozXYZ/vernier/cmd/okxexperiment"
)

const quoteInterval = time.Second

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(parent context.Context, args []string) error {
	flags := flag.NewFlagSet("okx-jupiter-quote-compare", flag.ContinueOnError)
	envPath := flags.String("env-file", okxexperiment.DefaultEnvFile, "environment file; default .env.test")
	samples := flags.Int("samples", 0, "number of pairs; zero uses OKX_LATENCY_SAMPLES or 20")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || *samples < 0 {
		return fmt.Errorf("usage: go run ./cmd/okx-jupiter-quote-compare [-env-file .env.test] [-samples N]")
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

	// Each provider has its own limiter. The two first calls can therefore be
	// released together, while neither provider exceeds one request per second.
	okxSource, err := settings.Source(quoteInterval)
	if err != nil {
		return err
	}
	jupiterSource, err := settings.JupiterSource(quoteInterval)
	if err != nil {
		return err
	}
	okxRequest := settings.Request()
	jupiterRequest := settings.JupiterRequest()

	fmt.Println("route_definition=default_okx_vs_default_jupiter no_dex_restrictions")
	fmt.Println("delta_definition=okx_output-jupiter_output positive_means_okx_more_output")

	okxLatencies := make([]time.Duration, 0, settings.Samples)
	jupiterLatencies := make([]time.Duration, 0, settings.Samples)
	okxAmounts := make([]string, 0, settings.Samples)
	jupiterAmounts := make([]string, 0, settings.Samples)
	amountDeltas := make([]string, 0, settings.Samples)
	var outputDecimals, outputUnit string
	okxSuccesses, okxErrors := 0, 0
	jupiterSuccesses, jupiterErrors := 0, 0
	nextStart := time.Now()

	for index := 0; index < settings.Samples; index++ {
		if err := waitUntil(ctx, nextStart); err != nil {
			return err
		}
		started := time.Now()
		okxMeasurement, jupiterMeasurement := quotePair(ctx, okxSource, jupiterSource, okxRequest, jupiterRequest)
		nextStart = started.Add(quoteInterval)

		if okxMeasurement.result.HTTPDuration > 0 {
			okxLatencies = append(okxLatencies, okxMeasurement.result.HTTPDuration)
		}
		if jupiterMeasurement.result.HTTPDuration > 0 {
			jupiterLatencies = append(jupiterLatencies, jupiterMeasurement.result.HTTPDuration)
		}
		if okxMeasurement.err != nil {
			okxErrors++
		} else {
			okxSuccesses++
		}
		if jupiterMeasurement.err != nil {
			jupiterErrors++
		} else {
			jupiterSuccesses++
		}

		okxRaw, decimals, unit, outputErr := outputAmount(okxMeasurement.result, settings.ToToken)
		if okxMeasurement.err == nil && outputErr == nil {
			if outputDecimals == "" {
				outputDecimals, outputUnit = decimals, unit
			}
			if decimals == outputDecimals {
				okxAmounts = append(okxAmounts, okxRaw)
			}
		}
		if jupiterMeasurement.err == nil && strings.TrimSpace(jupiterMeasurement.result.ToTokenAmount) != "" {
			jupiterAmounts = append(jupiterAmounts, jupiterMeasurement.result.ToTokenAmount)
		}
		if okxMeasurement.err == nil && jupiterMeasurement.err == nil && outputErr == nil && decimals == outputDecimals {
			if delta, deltaErr := okxexperiment.SubtractBaseUnits(okxRaw, jupiterMeasurement.result.ToTokenAmount); deltaErr == nil {
				amountDeltas = append(amountDeltas, delta)
			}
		}

		printSample(index+1, outputDecimals, outputUnit, okxMeasurement, jupiterMeasurement)
	}

	if len(okxLatencies) == 0 || len(jupiterLatencies) == 0 {
		return fmt.Errorf("no complete latency samples collected: okx=%d jupiter=%d", len(okxLatencies), len(jupiterLatencies))
	}
	printLatencySummary("okx", okxLatencies, okxSuccesses, okxErrors)
	printLatencySummary("jupiter", jupiterLatencies, jupiterSuccesses, jupiterErrors)
	if outputDecimals == "" {
		return fmt.Errorf("quotes succeeded but OKX did not return output-token decimals; cannot render amount statistics")
	}
	if err := printAmountSummary("okx", okxAmounts, outputDecimals, outputUnit); err != nil {
		return err
	}
	if err := printAmountSummary("jupiter", jupiterAmounts, outputDecimals, outputUnit); err != nil {
		return err
	}
	if err := printAmountSummary("delta(okx-jupiter)", amountDeltas, outputDecimals, outputUnit); err != nil {
		return err
	}
	return nil
}

type okxMeasurement struct {
	result    okx.Result
	err       error
	startedAt time.Time
}

type jupiterMeasurement struct {
	result    jupiter.QuoteResult
	err       error
	startedAt time.Time
}

func quotePair(ctx context.Context, okxSource *okx.Source, jupiterSource *jupiter.QuoteSource, okxRequest okx.QuoteRequest, jupiterRequest jupiter.QuoteRequest) (okxMeasurement, jupiterMeasurement) {
	var okxResult okxMeasurement
	var jupiterResult jupiterMeasurement
	var wait sync.WaitGroup
	start := make(chan struct{})
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		okxResult.startedAt = time.Now()
		okxResult.result, okxResult.err = okxSource.Quote(ctx, okxRequest)
	}()
	go func() {
		defer wait.Done()
		<-start
		jupiterResult.startedAt = time.Now()
		jupiterResult.result, jupiterResult.err = jupiterSource.Quote(ctx, jupiterRequest)
	}()
	close(start)
	wait.Wait()
	return okxResult, jupiterResult
}

func printSample(index int, decimals, unit string, okxMeasurement okxMeasurement, jupiterMeasurement jupiterMeasurement) {
	startSkew := okxMeasurement.startedAt.Sub(jupiterMeasurement.startedAt)
	if startSkew < 0 {
		startSkew = -startSkew
	}
	okxText := quoteOutput(okxMeasurement.err, okxMeasurement.result.ToTokenAmount, decimals, unit)
	jupiterText := quoteOutput(jupiterMeasurement.err, jupiterMeasurement.result.ToTokenAmount, decimals, unit)
	deltaText := "n/a"
	if okxMeasurement.err == nil && jupiterMeasurement.err == nil && decimals != "" {
		if delta, err := okxexperiment.DifferenceBaseUnits(okxMeasurement.result.ToTokenAmount, jupiterMeasurement.result.ToTokenAmount, decimals); err == nil {
			deltaText = signed(delta) + unitSuffix(unit)
		}
	}
	fmt.Printf("sample=%d start_skew=%s okx_output=%s okx_http=%s okx_total=%s jupiter_output=%s jupiter_http=%s jupiter_total=%s output_delta(okx-jupiter)=%s\n", index, startSkew, okxText, okxMeasurement.result.HTTPDuration, okxMeasurement.result.TotalDuration, jupiterText, jupiterMeasurement.result.HTTPDuration, jupiterMeasurement.result.TotalDuration, deltaText)
}

func quoteOutput(err error, rawAmount, decimals, unit string) string {
	if err != nil {
		return "error=" + err.Error()
	}
	if decimals == "" {
		return "n/a(output-token-decimals-unavailable)"
	}
	formatted, formatErr := okxexperiment.FormatBaseUnits(rawAmount, decimals)
	if formatErr != nil {
		return "output_error=" + formatErr.Error()
	}
	return formatted + unitSuffix(unit)
}

func outputAmount(result okx.Result, address string) (string, string, string, error) {
	address = strings.TrimSpace(address)
	if result.ToToken.Decimal != "" && strings.EqualFold(strings.TrimSpace(result.ToToken.Address), address) {
		return result.ToTokenAmount, result.ToToken.Decimal, result.ToToken.Symbol, nil
	}
	for _, route := range result.Routes {
		for _, subRoute := range route.SubRoutes {
			token := subRoute.ToToken
			if token.Decimal != "" && strings.EqualFold(strings.TrimSpace(token.Address), address) {
				return result.ToTokenAmount, token.Decimal, token.Symbol, nil
			}
		}
	}
	return "", "", "", fmt.Errorf("quote did not include output-token decimals")
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
