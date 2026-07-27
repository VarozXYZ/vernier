// Command kyberswap-zerox-quote-compare compares default exact-input routes
// from KyberSwap and 0x on Robinhood Chain. It never signs, approves, submits,
// or broadcasts a transaction.
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

	"github.com/VarozXYZ/vernier/adapters/quote/kyberswap"
	"github.com/VarozXYZ/vernier/adapters/quote/zerox"
	"github.com/VarozXYZ/vernier/cmd/okxexperiment"
	"github.com/VarozXYZ/vernier/cmd/zeroxexperiment"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(parent context.Context, args []string) error {
	flags := flag.NewFlagSet("kyberswap-zerox-quote-compare", flag.ContinueOnError)
	envPath := flags.String("env-file", zeroxexperiment.DefaultEnvFile, "environment file; default .env.test")
	samples := flags.Int("samples", 0, "number of quote pairs; zero uses ZEROX_LATENCY_SAMPLES or 20")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || *samples < 0 {
		return fmt.Errorf("usage: go run ./cmd/kyberswap-zerox-quote-compare [-env-file .env.test] [-samples N]")
	}
	settings, err := zeroxexperiment.LoadSettings(*envPath)
	if err != nil {
		return err
	}
	if *samples > 0 {
		settings.Samples = *samples
	}
	clientID := strings.TrimSpace(os.Getenv("KYBERSWAP_CLIENT_ID"))
	if clientID == "" {
		return fmt.Errorf("missing KYBERSWAP_CLIENT_ID in %s or process environment", *envPath)
	}
	chain := strings.TrimSpace(os.Getenv("KYBERSWAP_CHAIN"))
	if chain == "" {
		chain = kyberswap.DefaultChain
	}
	kyberSource, err := kyberswap.New(kyberswap.Config{ClientID: clientID})
	if err != nil {
		return err
	}
	zeroxSource, err := settings.Source()
	if err != nil {
		return err
	}
	kyberRequest := kyberswap.RouteRequest{
		Chain:    chain,
		TokenIn:  settings.SellToken,
		TokenOut: settings.BuyToken,
		AmountIn: settings.SellAmount,
		Origin:   settings.Taker,
	}
	zeroxRequest := settings.Request()

	ctx, stop := signal.NotifyContext(parent, os.Interrupt)
	defer stop()

	fmt.Printf("network=robinhood kyberswap_chain=%s zerox_chain_id=%s\n", chain, zeroxRequest.ChainID)
	fmt.Println("route_definition=default_kyberswap_vs_default_0x no_liquidity_source_restrictions")
	fmt.Println("delta_definition=kyberswap_output-0x_output positive_means_kyberswap_more_output")
	fmt.Printf("pair_interval=%s providers_called_in_parallel=true client_side_rate_limiter=false\n", settings.Interval)
	fmt.Println("broadcast=false signing=false approvals=false")

	kyberTimings := newTimingGroup()
	zeroxTimings := newTimingGroup()
	kyberAmounts := make([]string, 0, settings.Samples)
	zeroxAmounts := make([]string, 0, settings.Samples)
	deltas := make([]string, 0, settings.Samples)
	nextStart := time.Now()

	for index := 0; index < settings.Samples; index++ {
		if err := waitUntil(ctx, nextStart); err != nil {
			return err
		}
		started := time.Now()
		kyberMeasurement, zeroxMeasurement := quotePair(ctx, kyberSource, zeroxSource, kyberRequest, zeroxRequest)
		nextStart = started.Add(settings.Interval)

		kyberTimings.add(newCallTiming(kyberMeasurement.callDuration, kyberMeasurement.result.HTTPDuration), kyberMeasurement.err)
		zeroxTimings.add(newCallTiming(zeroxMeasurement.callDuration, zeroxMeasurement.result.HTTPDuration), zeroxMeasurement.err)
		if kyberMeasurement.err == nil {
			kyberAmounts = append(kyberAmounts, kyberMeasurement.result.AmountOut)
		}
		if zeroxMeasurement.err == nil {
			zeroxAmounts = append(zeroxAmounts, zeroxMeasurement.result.BuyAmount)
		}
		if kyberMeasurement.err == nil && zeroxMeasurement.err == nil {
			delta, deltaErr := okxexperiment.SubtractBaseUnits(kyberMeasurement.result.AmountOut, zeroxMeasurement.result.BuyAmount)
			if deltaErr == nil {
				deltas = append(deltas, delta)
			}
		}
		printSample(index+1, settings.BuyDecimals, settings.BuySymbol, kyberMeasurement, zeroxMeasurement)
	}

	if kyberTimings.attempts == 0 || zeroxTimings.attempts == 0 {
		return fmt.Errorf("no latency samples collected")
	}
	kyberTimings.print("kyberswap", "route")
	zeroxTimings.print("0x", "price")
	if err := printAmountSummary("kyberswap", kyberAmounts, settings.BuyDecimals, settings.BuySymbol); err != nil {
		return err
	}
	if err := printAmountSummary("0x", zeroxAmounts, settings.BuyDecimals, settings.BuySymbol); err != nil {
		return err
	}
	if err := printAmountSummary("delta(kyberswap-0x)", deltas, settings.BuyDecimals, settings.BuySymbol); err != nil {
		return err
	}
	return nil
}

type kyberMeasurement struct {
	result       kyberswap.RouteResult
	err          error
	startedAt    time.Time
	callDuration time.Duration
}

type zeroxMeasurement struct {
	result       zerox.Result
	err          error
	startedAt    time.Time
	callDuration time.Duration
}

func quotePair(
	ctx context.Context,
	kyberSource *kyberswap.Source,
	zeroxSource *zerox.Source,
	kyberRequest kyberswap.RouteRequest,
	zeroxRequest zerox.Request,
) (kyberMeasurement, zeroxMeasurement) {
	var kyberResult kyberMeasurement
	var zeroxResult zeroxMeasurement
	var wait sync.WaitGroup
	start := make(chan struct{})
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		kyberResult.startedAt = time.Now()
		kyberResult.result, kyberResult.err = kyberSource.Route(ctx, kyberRequest)
		kyberResult.callDuration = time.Since(kyberResult.startedAt)
	}()
	go func() {
		defer wait.Done()
		<-start
		zeroxResult.startedAt = time.Now()
		zeroxResult.result, zeroxResult.err = zeroxSource.Price(ctx, zeroxRequest)
		zeroxResult.callDuration = time.Since(zeroxResult.startedAt)
	}()
	close(start)
	wait.Wait()
	return kyberResult, zeroxResult
}

func printSample(index int, decimals, unit string, kyber kyberMeasurement, zero zeroxMeasurement) {
	startSkew := kyber.startedAt.Sub(zero.startedAt)
	if startSkew < 0 {
		startSkew = -startSkew
	}
	kyberTiming := newCallTiming(kyber.callDuration, kyber.result.HTTPDuration)
	zeroxTiming := newCallTiming(zero.callDuration, zero.result.HTTPDuration)
	deltaText := "n/a"
	if kyber.err == nil && zero.err == nil {
		if delta, err := okxexperiment.DifferenceBaseUnits(kyber.result.AmountOut, zero.result.BuyAmount, decimals); err == nil {
			deltaText = signed(delta) + unitSuffix(unit)
		}
	}
	fmt.Printf(
		"sample=%d start_skew=%s kyberswap_output=%s kyberswap_call=%s kyberswap_http=%s kyberswap_local=%s kyberswap_paths=%d kyberswap_steps=%d zerox_output=%s zerox_call=%s zerox_http=%s zerox_local=%s zerox_fills=%d output_delta(kyberswap-0x)=%s\n",
		index,
		startSkew,
		quoteOutput(kyber.err, kyber.result.AmountOut, decimals, unit),
		kyberTiming.callDuration,
		kyberTiming.httpDuration,
		kyberTiming.localDuration,
		len(kyber.result.Paths),
		routeSteps(kyber.result.Paths),
		quoteOutput(zero.err, zero.result.BuyAmount, decimals, unit),
		zeroxTiming.callDuration,
		zeroxTiming.httpDuration,
		zeroxTiming.localDuration,
		len(zero.result.Route),
		deltaText,
	)
}

func routeSteps(paths [][]kyberswap.RouteStep) int {
	total := 0
	for _, path := range paths {
		total += len(path)
	}
	return total
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

type callTiming struct {
	callDuration  time.Duration
	httpDuration  time.Duration
	localDuration time.Duration
}

func newCallTiming(callDuration, httpDuration time.Duration) callTiming {
	local := callDuration - httpDuration
	if local < 0 {
		local = 0
	}
	return callTiming{callDuration: callDuration, httpDuration: httpDuration, localDuration: local}
}

type timingGroup struct {
	attempts     int
	successes    int
	errors       int
	allCall      []time.Duration
	allHTTP      []time.Duration
	allLocal     []time.Duration
	successCall  []time.Duration
	successHTTP  []time.Duration
	successLocal []time.Duration
}

func newTimingGroup() *timingGroup {
	return &timingGroup{}
}

func (g *timingGroup) add(timing callTiming, err error) {
	g.attempts++
	g.allCall = append(g.allCall, timing.callDuration)
	g.allHTTP = append(g.allHTTP, timing.httpDuration)
	g.allLocal = append(g.allLocal, timing.localDuration)
	if err != nil {
		g.errors++
		return
	}
	g.successes++
	g.successCall = append(g.successCall, timing.callDuration)
	g.successHTTP = append(g.successHTTP, timing.httpDuration)
	g.successLocal = append(g.successLocal, timing.localDuration)
}

func (g *timingGroup) print(provider, stage string) {
	fmt.Printf(
		"summary provider=%s stage=%s attempts=%d successes=%d errors=%d %s %s %s",
		provider,
		stage,
		g.attempts,
		g.successes,
		g.errors,
		timingFields("all_call", g.allCall),
		timingFields("all_http", g.allHTTP),
		timingFields("all_local", g.allLocal),
	)
	if len(g.successCall) > 0 {
		fmt.Printf(
			" %s %s %s",
			timingFields("success_call", g.successCall),
			timingFields("success_http", g.successHTTP),
			timingFields("success_local", g.successLocal),
		)
	}
	fmt.Println()
}

func timingFields(prefix string, values []time.Duration) string {
	stats := summarize(values)
	return fmt.Sprintf("%s_min=%s %s_mean=%s %s_p50=%s %s_p95=%s %s_p99=%s %s_max=%s", prefix, stats.Min, prefix, stats.Mean, prefix, stats.P50, prefix, stats.P95, prefix, stats.P99, prefix, stats.Max)
}

func printAmountSummary(variant string, values []string, decimals, unit string) error {
	if len(values) == 0 {
		return fmt.Errorf("no successful amount samples collected for %s", variant)
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
	if len(values) == 0 {
		return statistics{}
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var total time.Duration
	for _, value := range sorted {
		total += value
	}
	return statistics{
		Min:  sorted[0],
		Mean: total / time.Duration(len(sorted)),
		P50:  percentile(sorted, 0.50),
		P95:  percentile(sorted, 0.95),
		P99:  percentile(sorted, 0.99),
		Max:  sorted[len(sorted)-1],
	}
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
