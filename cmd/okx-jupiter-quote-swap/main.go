// Command okx-jupiter-quote-swap measures quote followed by unsigned swap
// construction for Jupiter and OKX on Solana. It never signs or broadcasts.
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

const pairInterval = time.Second

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(parent context.Context, args []string) error {
	flags := flag.NewFlagSet("okx-jupiter-quote-swap", flag.ContinueOnError)
	envPath := flags.String("env-file", okxexperiment.DefaultEnvFile, "environment file; default .env.test")
	samples := flags.Int("samples", 0, "number of pairs; zero uses OKX_LATENCY_SAMPLES or 20")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || *samples < 0 {
		return fmt.Errorf("usage: go run ./cmd/okx-jupiter-quote-swap [-env-file .env.test] [-samples N]")
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
	if strings.TrimSpace(settings.JupiterUserPublicKey) == "" {
		return fmt.Errorf("missing JUPITER_USER_PUBLIC_KEY in the environment")
	}
	if strings.TrimSpace(settings.OKXUserWalletAddress) == "" {
		return fmt.Errorf("missing OKX_USER_WALLET_ADDRESS in the environment")
	}

	ctx, stop := signal.NotifyContext(parent, os.Interrupt)
	defer stop()

	okxQuoteSource, err := settings.SourceWithoutLimiter()
	if err != nil {
		return err
	}
	okxSwapSource, err := settings.SourceWithoutLimiter()
	if err != nil {
		return err
	}
	jupiterQuoteSource, err := settings.JupiterSourceWithoutLimiter()
	if err != nil {
		return err
	}
	jupiterSwapSource, err := settings.JupiterSwapSourceWithoutLimiter()
	if err != nil {
		return err
	}

	okxRequest := settings.Request()
	okxSwapRequest := settings.SwapInstructionRequest()
	jupiterRequest := settings.JupiterRequest()

	fmt.Println("flow=quote_then_unsigned_swap_construction")
	fmt.Println("broadcast=false signing=false")
	fmt.Println("pair_schedule=one_jupiter_pipeline_and_one_okx_pipeline_per_second")
	fmt.Println("client_side_rate_limiters=false")
	fmt.Printf("jupiter_api_key_pool_size=%d\n", settings.JupiterKeyPoolSize())

	latencies := newLatencyCollections()
	jupiterAmounts := make([]string, 0, settings.Samples)
	okxAmounts := make([]string, 0, settings.Samples)
	amountDeltas := make([]string, 0, settings.Samples)
	nextStart := time.Now()

	for index := 0; index < settings.Samples; index++ {
		if err := waitUntil(ctx, nextStart); err != nil {
			return err
		}
		started := time.Now()
		jupiterMeasurement, okxMeasurement := runPair(ctx, jupiterQuoteSource, jupiterSwapSource, jupiterRequest, settings.JupiterUserPublicKey, okxQuoteSource, okxSwapSource, okxRequest, okxSwapRequest)
		nextStart = started.Add(pairInterval)

		latencies.addJupiter(jupiterMeasurement)
		latencies.addOKX(okxMeasurement)
		if jupiterMeasurement.quoteErr == nil {
			jupiterAmounts = append(jupiterAmounts, jupiterMeasurement.quote.ToTokenAmount)
		}
		if okxMeasurement.quoteErr == nil {
			okxAmounts = append(okxAmounts, okxMeasurement.quote.ToTokenAmount)
		}
		if jupiterMeasurement.quoteErr == nil && okxMeasurement.quoteErr == nil {
			if delta, deltaErr := okxexperiment.SubtractBaseUnits(okxMeasurement.quote.ToTokenAmount, jupiterMeasurement.quote.ToTokenAmount); deltaErr == nil {
				amountDeltas = append(amountDeltas, delta)
			}
		}

		printSample(index+1, settings.JupiterOutputDecimals, settings.JupiterOutputSymbol, jupiterMeasurement, okxMeasurement)
	}

	if err := latencies.printSummaries(); err != nil {
		return err
	}
	if err := printAmountSummary("jupiter", jupiterAmounts, settings.JupiterOutputDecimals, settings.JupiterOutputSymbol); err != nil {
		return err
	}
	if err := printAmountSummary("okx", okxAmounts, settings.JupiterOutputDecimals, settings.JupiterOutputSymbol); err != nil {
		return err
	}
	if err := printAmountSummary("delta(okx-jupiter)", amountDeltas, settings.JupiterOutputDecimals, settings.JupiterOutputSymbol); err != nil {
		return err
	}
	return nil
}

type jupiterMeasurement struct {
	startedAt     time.Time
	quoteStarted  time.Time
	swapStarted   time.Time
	quote         jupiter.QuoteResult
	swap          jupiter.SwapResult
	quoteTiming   callTiming
	swapTiming    callTiming
	handoff       time.Duration
	quoteErr      error
	swapErr       error
	totalDuration time.Duration
}

type okxMeasurement struct {
	startedAt          time.Time
	quoteStarted       time.Time
	instructionStarted time.Time
	quote              okx.Result
	instructions       okx.SwapInstructionResult
	quoteTiming        callTiming
	instructionTiming  callTiming
	handoff            time.Duration
	quoteErr           error
	instructionErr     error
	totalDuration      time.Duration
}

// callTiming decomposes an adapter call into the time spent waiting for a
// client-side limiter, inside the HTTP client, and everywhere else in the
// adapter call. The latter includes request preparation and response parsing.
type callTiming struct {
	callDuration  time.Duration
	queueDuration time.Duration
	httpDuration  time.Duration
	localDuration time.Duration
}

func newCallTiming(callDuration, queueDuration, httpDuration time.Duration) callTiming {
	localDuration := callDuration - queueDuration - httpDuration
	if localDuration < 0 {
		localDuration = 0
	}
	return callTiming{
		callDuration:  callDuration,
		queueDuration: queueDuration,
		httpDuration:  httpDuration,
		localDuration: localDuration,
	}
}

func runPair(ctx context.Context, jupiterQuoteSource *jupiter.QuoteSource, jupiterSwapSource *jupiter.SwapSource, jupiterRequest jupiter.QuoteRequest, jupiterUserPublicKey string, okxQuoteSource *okx.Source, okxSwapSource *okx.Source, okxRequest okx.QuoteRequest, okxSwapRequest okx.SwapInstructionRequest) (jupiterMeasurement, okxMeasurement) {
	var jupiterResult jupiterMeasurement
	var okxResult okxMeasurement
	var wait sync.WaitGroup
	wait.Add(2)
	start := make(chan struct{})
	go func() {
		defer wait.Done()
		<-start
		jupiterResult.startedAt = time.Now()
		jupiterResult.quoteStarted = time.Now()
		jupiterResult.quote, jupiterResult.quoteErr = jupiterQuoteSource.Quote(ctx, jupiterRequest)
		quoteFinished := time.Now()
		jupiterResult.quoteTiming = newCallTiming(quoteFinished.Sub(jupiterResult.quoteStarted), jupiterResult.quote.QueueDuration, jupiterResult.quote.HTTPDuration)
		if jupiterResult.quoteErr != nil {
			jupiterResult.totalDuration = time.Since(jupiterResult.startedAt)
			return
		}
		jupiterResult.swapStarted = time.Now()
		jupiterResult.handoff = jupiterResult.swapStarted.Sub(quoteFinished)
		jupiterResult.swap, jupiterResult.swapErr = jupiterSwapSource.Swap(ctx, jupiter.SwapRequest{QuoteResponse: jupiterResult.quote.RawResponse, UserPublicKey: jupiterUserPublicKey})
		jupiterResult.swapTiming = newCallTiming(time.Since(jupiterResult.swapStarted), jupiterResult.swap.QueueDuration, jupiterResult.swap.HTTPDuration)
		jupiterResult.totalDuration = time.Since(jupiterResult.startedAt)
	}()
	go func() {
		defer wait.Done()
		<-start
		okxResult.startedAt = time.Now()
		okxResult.quoteStarted = time.Now()
		okxResult.quote, okxResult.quoteErr = okxQuoteSource.Quote(ctx, okxRequest)
		quoteFinished := time.Now()
		okxResult.quoteTiming = newCallTiming(quoteFinished.Sub(okxResult.quoteStarted), okxResult.quote.QueueDuration, okxResult.quote.HTTPDuration)
		if okxResult.quoteErr != nil {
			okxResult.totalDuration = time.Since(okxResult.startedAt)
			return
		}
		okxResult.instructionStarted = time.Now()
		okxResult.handoff = okxResult.instructionStarted.Sub(quoteFinished)
		okxResult.instructions, okxResult.instructionErr = okxSwapSource.SwapInstruction(ctx, okxSwapRequest)
		okxResult.instructionTiming = newCallTiming(time.Since(okxResult.instructionStarted), okxResult.instructions.QueueDuration, okxResult.instructions.HTTPDuration)
		okxResult.totalDuration = time.Since(okxResult.startedAt)
	}()
	close(start)
	wait.Wait()
	return jupiterResult, okxResult
}

func printSample(index int, decimals, unit string, jupiterResult jupiterMeasurement, okxResult okxMeasurement) {
	startSkew := jupiterResult.startedAt.Sub(okxResult.startedAt)
	if startSkew < 0 {
		startSkew = -startSkew
	}
	jupiterOutput := quoteOutput(jupiterResult.quoteErr, jupiterResult.quote.ToTokenAmount, decimals, unit)
	okxOutput := quoteOutput(okxResult.quoteErr, okxResult.quote.ToTokenAmount, decimals, unit)
	delta := "n/a"
	if jupiterResult.quoteErr == nil && okxResult.quoteErr == nil {
		if formatted, err := okxexperiment.DifferenceBaseUnits(okxResult.quote.ToTokenAmount, jupiterResult.quote.ToTokenAmount, decimals); err == nil {
			delta = signed(formatted) + unitSuffix(unit)
		}
	}
	fmt.Printf("sample=%d start_skew=%s jupiter_quote_output=%s jupiter_quote_call=%s jupiter_quote_queue=%s jupiter_quote_http=%s jupiter_quote_local=%s jupiter_handoff=%s jupiter_swap_call=%s jupiter_swap_queue=%s jupiter_swap_http=%s jupiter_swap_local=%s jupiter_total=%s jupiter_quote_status=%s jupiter_swap_status=%s okx_quote_output=%s okx_quote_call=%s okx_quote_queue=%s okx_quote_http=%s okx_quote_local=%s okx_handoff=%s okx_instruction_call=%s okx_instruction_queue=%s okx_instruction_http=%s okx_instruction_local=%s okx_total=%s okx_quote_status=%s okx_instruction_status=%s quote_output_delta(okx-jupiter)=%s\n", index, startSkew, jupiterOutput, jupiterResult.quoteTiming.callDuration, jupiterResult.quoteTiming.queueDuration, jupiterResult.quoteTiming.httpDuration, jupiterResult.quoteTiming.localDuration, jupiterResult.handoff, jupiterResult.swapTiming.callDuration, jupiterResult.swapTiming.queueDuration, jupiterResult.swapTiming.httpDuration, jupiterResult.swapTiming.localDuration, jupiterResult.totalDuration, errorStatus(jupiterResult.quoteErr), errorStatus(jupiterResult.swapErr), okxOutput, okxResult.quoteTiming.callDuration, okxResult.quoteTiming.queueDuration, okxResult.quoteTiming.httpDuration, okxResult.quoteTiming.localDuration, okxResult.handoff, okxResult.instructionTiming.callDuration, okxResult.instructionTiming.queueDuration, okxResult.instructionTiming.httpDuration, okxResult.instructionTiming.localDuration, okxResult.totalDuration, errorStatus(okxResult.quoteErr), errorStatus(okxResult.instructionErr), delta)
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

func errorStatus(err error) string {
	if err == nil {
		return "ok"
	}
	return "error=" + err.Error()
}

type latencyCollections struct {
	jupiterQuote          latencyGroup
	jupiterSwap           latencyGroup
	jupiterPipeline       latencyGroup
	okxQuote              latencyGroup
	okxInstructions       latencyGroup
	okxPipeline           latencyGroup
	jupiterQuoteTiming    timingGroup
	jupiterSwapTiming     timingGroup
	jupiterPipelineTiming timingGroup
	okxQuoteTiming        timingGroup
	okxInstructionsTiming timingGroup
	okxPipelineTiming     timingGroup
}

type latencyGroup struct {
	attempts []time.Duration
	success  []time.Duration
	errors   int
}

func newLatencyCollections() *latencyCollections { return &latencyCollections{} }

func (c *latencyCollections) addJupiter(measurement jupiterMeasurement) {
	addLatency(&c.jupiterQuote, measurement.quote.HTTPDuration, measurement.quoteErr)
	addLatency(&c.jupiterSwap, measurement.swap.HTTPDuration, measurement.swapErr)
	addLatency(&c.jupiterPipeline, measurement.totalDuration, combinedError(measurement.quoteErr, measurement.swapErr))
	addTiming(&c.jupiterQuoteTiming, measurement.quoteTiming, measurement.quoteErr)
	addTiming(&c.jupiterSwapTiming, measurement.swapTiming, measurement.swapErr)
	addTiming(&c.jupiterPipelineTiming, newCallTiming(measurement.totalDuration, measurement.quoteTiming.queueDuration+measurement.swapTiming.queueDuration, measurement.quoteTiming.httpDuration+measurement.swapTiming.httpDuration), combinedError(measurement.quoteErr, measurement.swapErr))
}

func (c *latencyCollections) addOKX(measurement okxMeasurement) {
	addLatency(&c.okxQuote, measurement.quote.HTTPDuration, measurement.quoteErr)
	addLatency(&c.okxInstructions, measurement.instructions.HTTPDuration, measurement.instructionErr)
	addLatency(&c.okxPipeline, measurement.totalDuration, combinedError(measurement.quoteErr, measurement.instructionErr))
	addTiming(&c.okxQuoteTiming, measurement.quoteTiming, measurement.quoteErr)
	addTiming(&c.okxInstructionsTiming, measurement.instructionTiming, measurement.instructionErr)
	addTiming(&c.okxPipelineTiming, newCallTiming(measurement.totalDuration, measurement.quoteTiming.queueDuration+measurement.instructionTiming.queueDuration, measurement.quoteTiming.httpDuration+measurement.instructionTiming.httpDuration), combinedError(measurement.quoteErr, measurement.instructionErr))
}

func addLatency(group *latencyGroup, duration time.Duration, err error) {
	if duration <= 0 {
		if err != nil {
			group.errors++
		}
		return
	}
	group.attempts = append(group.attempts, duration)
	if err != nil {
		group.errors++
	} else {
		group.success = append(group.success, duration)
	}
}

func combinedError(first, second error) error {
	if first != nil {
		return first
	}
	return second
}

func (c *latencyCollections) printSummaries() error {
	groups := []struct {
		provider string
		stage    string
		group    latencyGroup
	}{
		{"jupiter", "quote", c.jupiterQuote},
		{"jupiter", "swap", c.jupiterSwap},
		{"jupiter", "pipeline", c.jupiterPipeline},
		{"okx", "quote", c.okxQuote},
		{"okx", "instructions", c.okxInstructions},
		{"okx", "pipeline", c.okxPipeline},
	}
	for _, item := range groups {
		if len(item.group.attempts) == 0 {
			return fmt.Errorf("no latency samples collected for provider=%s stage=%s", item.provider, item.stage)
		}
		all := summarize(item.group.attempts)
		line := fmt.Sprintf("summary provider=%s stage=%s attempts=%d successes=%d errors=%d all_min=%s all_mean=%s all_p50=%s all_p95=%s all_p99=%s all_max=%s", item.provider, item.stage, len(item.group.attempts), len(item.group.success), item.group.errors, all.Min, all.Mean, all.P50, all.P95, all.P99, all.Max)
		if len(item.group.success) > 0 {
			success := summarize(item.group.success)
			line += fmt.Sprintf(" success_min=%s success_mean=%s success_p50=%s success_p95=%s success_p99=%s success_max=%s", success.Min, success.Mean, success.P50, success.P95, success.P99, success.Max)
		}
		fmt.Println(line)
	}
	if err := c.printTimingSummaries(); err != nil {
		return err
	}
	return nil
}

type timingGroup struct {
	calls        []time.Duration
	queues       []time.Duration
	https        []time.Duration
	locals       []time.Duration
	successCall  []time.Duration
	successQueue []time.Duration
	successHTTP  []time.Duration
	successLocal []time.Duration
	errors       int
}

func addTiming(group *timingGroup, timing callTiming, err error) {
	if timing.callDuration <= 0 {
		if err != nil {
			group.errors++
		}
		return
	}
	group.calls = append(group.calls, timing.callDuration)
	group.queues = append(group.queues, timing.queueDuration)
	group.https = append(group.https, timing.httpDuration)
	group.locals = append(group.locals, timing.localDuration)
	if err != nil {
		group.errors++
		return
	}
	group.successCall = append(group.successCall, timing.callDuration)
	group.successQueue = append(group.successQueue, timing.queueDuration)
	group.successHTTP = append(group.successHTTP, timing.httpDuration)
	group.successLocal = append(group.successLocal, timing.localDuration)
}

func (c *latencyCollections) printTimingSummaries() error {
	groups := []struct {
		provider string
		stage    string
		group    timingGroup
	}{
		{"jupiter", "quote", c.jupiterQuoteTiming},
		{"jupiter", "swap", c.jupiterSwapTiming},
		{"jupiter", "pipeline", c.jupiterPipelineTiming},
		{"okx", "quote", c.okxQuoteTiming},
		{"okx", "instructions", c.okxInstructionsTiming},
		{"okx", "pipeline", c.okxPipelineTiming},
	}
	for _, item := range groups {
		if len(item.group.calls) == 0 {
			return fmt.Errorf("no timing samples collected for provider=%s stage=%s", item.provider, item.stage)
		}
		line := fmt.Sprintf("timings provider=%s stage=%s attempts=%d successes=%d errors=%d", item.provider, item.stage, len(item.group.calls), len(item.group.successCall), item.group.errors)
		line += timingFields("call", item.group.calls)
		line += timingFields("queue", item.group.queues)
		line += timingFields("http", item.group.https)
		line += timingFields("local", item.group.locals)
		if len(item.group.successCall) > 0 {
			line += timingFields("success_call", item.group.successCall)
			line += timingFields("success_queue", item.group.successQueue)
			line += timingFields("success_http", item.group.successHTTP)
			line += timingFields("success_local", item.group.successLocal)
		}
		fmt.Println(line)
	}
	return nil
}

func timingFields(prefix string, values []time.Duration) string {
	statistics := summarize(values)
	return fmt.Sprintf(" %s_min=%s %s_mean=%s %s_p50=%s %s_p95=%s %s_p99=%s %s_max=%s", prefix, statistics.Min, prefix, statistics.Mean, prefix, statistics.P50, prefix, statistics.P95, prefix, statistics.P99, prefix, statistics.Max)
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
