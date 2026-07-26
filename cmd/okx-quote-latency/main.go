// Command okx-quote-latency measures real OKX quote latency at one request per
// second. It is intentionally separate from tests and Research runtime.
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sort"
	"time"

	"github.com/VarozXYZ/vernier/cmd/okxexperiment"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(parent context.Context, args []string) error {
	flags := flag.NewFlagSet("okx-quote-latency", flag.ContinueOnError)
	envPath := flags.String("env-file", okxexperiment.DefaultEnvFile, "environment file; default .env.test")
	samples := flags.Int("samples", 0, "number of samples; zero uses OKX_LATENCY_SAMPLES or 20")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || *samples < 0 {
		return fmt.Errorf("usage: go run ./cmd/okx-quote-latency [-env-file .env.test] [-samples N]")
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
	source, err := settings.Source(time.Second)
	if err != nil {
		return err
	}

	latencies := make([]time.Duration, 0, settings.Samples)
	successes := 0
	errors := 0
	nextStart := time.Now()
	for index := 0; index < settings.Samples; index++ {
		if err := waitUntil(ctx, nextStart); err != nil {
			return err
		}
		started := time.Now()
		result, quoteErr := source.Quote(ctx, settings.Request())
		nextStart = started.Add(time.Second)
		if result.HTTPDuration > 0 {
			latencies = append(latencies, result.HTTPDuration)
		}
		if quoteErr != nil {
			errors++
			fmt.Printf("sample=%d error=%v http=%s total=%s\n", index+1, quoteErr, result.HTTPDuration, time.Since(started))
			continue
		}
		successes++
		fmt.Printf("sample=%d output=%s http=%s total=%s\n", index+1, result.ToTokenAmount, result.HTTPDuration, time.Since(started))
	}
	if len(latencies) == 0 {
		return fmt.Errorf("no HTTP latency samples collected: successes=%d errors=%d", successes, errors)
	}
	statistics := summarize(latencies)
	fmt.Printf("summary samples=%d successes=%d errors=%d min=%s mean=%s p50=%s p95=%s p99=%s max=%s\n", len(latencies), successes, errors, statistics.Min, statistics.Mean, statistics.P50, statistics.P95, statistics.P99, statistics.Max)
	return nil
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
