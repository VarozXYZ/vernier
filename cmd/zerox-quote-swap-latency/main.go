// Command zerox-quote-swap-latency measures 0x indicative-price and firm-quote
// latency on Robinhood Chain. It never signs or broadcasts a transaction.
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
	"time"

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
	flags := flag.NewFlagSet("zerox-quote-swap-latency", flag.ContinueOnError)
	envPath := flags.String("env-file", zeroxexperiment.DefaultEnvFile, "environment file; default .env.test")
	samples := flags.Int("samples", 0, "number of pipelines; zero uses ZEROX_LATENCY_SAMPLES or 20")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || *samples < 0 {
		return fmt.Errorf("usage: go run ./cmd/zerox-quote-swap-latency [-env-file .env.test] [-samples N]")
	}
	settings, err := zeroxexperiment.LoadSettings(*envPath)
	if err != nil {
		return err
	}
	if *samples > 0 {
		settings.Samples = *samples
	}
	source, err := settings.Source()
	if err != nil {
		return err
	}
	request := settings.Request()

	ctx, stop := signal.NotifyContext(parent, os.Interrupt)
	defer stop()

	fmt.Println("provider=0x network=robinhood chain_id=" + request.ChainID)
	fmt.Println("flow=indicative_price_then_firm_quote")
	fmt.Println("broadcast=false signing=false approvals=false")
	fmt.Printf("pipeline_interval=%s client_side_rate_limiter=false\n", settings.Interval)

	priceTimings := newTimingGroup()
	quoteTimings := newTimingGroup()
	pipelineTimings := newTimingGroup()
	priceAmounts := make([]string, 0, settings.Samples)
	quoteAmounts := make([]string, 0, settings.Samples)
	deltas := make([]string, 0, settings.Samples)
	nextStart := time.Now()

	for index := 0; index < settings.Samples; index++ {
		if err := waitUntil(ctx, nextStart); err != nil {
			return err
		}
		pipelineStarted := time.Now()
		priceStarted := time.Now()
		price, priceErr := source.Price(ctx, request)
		priceCall := time.Since(priceStarted)
		priceTiming := newCallTiming(priceCall, price.HTTPDuration)
		priceTimings.add(priceTiming, priceErr)

		var quote zerox.Result
		var quoteErr error
		var quoteTiming callTiming
		handoff := time.Duration(0)
		if priceErr == nil {
			priceFinished := time.Now()
			quoteStarted := time.Now()
			handoff = quoteStarted.Sub(priceFinished)
			quote, quoteErr = source.Quote(ctx, request)
			quoteTiming = newCallTiming(time.Since(quoteStarted), quote.HTTPDuration)
			quoteTimings.add(quoteTiming, quoteErr)
		} else {
			quoteErr = fmt.Errorf("not attempted because indicative price failed")
		}
		pipelineCall := time.Since(pipelineStarted)
		pipelineHTTP := priceTiming.httpDuration + quoteTiming.httpDuration
		pipelineErr := firstError(priceErr, quoteErr)
		pipelineTimings.add(newCallTiming(pipelineCall, pipelineHTTP), pipelineErr)
		nextStart = pipelineStarted.Add(settings.Interval)

		priceOutput := output(price.BuyAmount, priceErr, settings.BuyDecimals, settings.BuySymbol)
		quoteOutput := output(quote.BuyAmount, quoteErr, settings.BuyDecimals, settings.BuySymbol)
		delta := "n/a"
		if priceErr == nil {
			priceAmounts = append(priceAmounts, price.BuyAmount)
		}
		if quoteErr == nil {
			quoteAmounts = append(quoteAmounts, quote.BuyAmount)
		}
		if priceErr == nil && quoteErr == nil {
			rawDelta, deltaErr := okxexperiment.SubtractBaseUnits(quote.BuyAmount, price.BuyAmount)
			if deltaErr == nil {
				deltas = append(deltas, rawDelta)
				if formatted, formatErr := okxexperiment.FormatBaseUnits(rawDelta, settings.BuyDecimals); formatErr == nil {
					delta = signed(formatted) + unitSuffix(settings.BuySymbol)
				}
			}
		}
		fmt.Printf("sample=%d price_output=%s price_call=%s price_http=%s price_local=%s price_status=%s price_routes=%d quote_output=%s quote_call=%s quote_http=%s quote_local=%s quote_status=%s quote_routes=%d quote_allowance_issue=%t quote_balance_issue=%t quote_simulation_incomplete=%t handoff=%s pipeline=%s output_delta(firm-indicative)=%s\n",
			index+1,
			priceOutput,
			priceTiming.callDuration,
			priceTiming.httpDuration,
			priceTiming.localDuration,
			errorStatus(priceErr),
			len(price.Route),
			quoteOutput,
			quoteTiming.callDuration,
			quoteTiming.httpDuration,
			quoteTiming.localDuration,
			errorStatus(quoteErr),
			len(quote.Route),
			quote.Issues.AllowanceSpender != "",
			quote.Issues.BalanceExpected != "",
			quote.Issues.SimulationIncomplete,
			handoff,
			pipelineCall,
			delta,
		)
	}

	priceTimings.print("price")
	quoteTimings.print("firm_quote")
	pipelineTimings.print("pipeline")
	if err := printAmountSummary("indicative", priceAmounts, settings.BuyDecimals, settings.BuySymbol); err != nil {
		return err
	}
	if err := printAmountSummary("firm", quoteAmounts, settings.BuyDecimals, settings.BuySymbol); err != nil {
		return err
	}
	if err := printAmountSummary("delta(firm-indicative)", deltas, settings.BuyDecimals, settings.BuySymbol); err != nil {
		return err
	}
	return nil
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
	calls        []time.Duration
	https        []time.Duration
	locals       []time.Duration
	successCall  []time.Duration
	successHTTP  []time.Duration
	successLocal []time.Duration
	errors       int
}

func newTimingGroup() *timingGroup { return &timingGroup{} }

func (g *timingGroup) add(timing callTiming, err error) {
	if timing.callDuration <= 0 {
		if err != nil {
			g.errors++
		}
		return
	}
	g.calls = append(g.calls, timing.callDuration)
	g.https = append(g.https, timing.httpDuration)
	g.locals = append(g.locals, timing.localDuration)
	if err != nil {
		g.errors++
		return
	}
	g.successCall = append(g.successCall, timing.callDuration)
	g.successHTTP = append(g.successHTTP, timing.httpDuration)
	g.successLocal = append(g.successLocal, timing.localDuration)
}

func (g *timingGroup) print(stage string) {
	if len(g.calls) == 0 {
		fmt.Printf("timings provider=0x stage=%s attempts=0 successes=0 errors=%d\n", stage, g.errors)
		return
	}
	line := fmt.Sprintf("timings provider=0x stage=%s attempts=%d successes=%d errors=%d", stage, len(g.calls), len(g.successCall), g.errors)
	line += timingFields("call", g.calls)
	line += timingFields("http", g.https)
	line += timingFields("local", g.locals)
	if len(g.successCall) > 0 {
		line += timingFields("success_call", g.successCall)
		line += timingFields("success_http", g.successHTTP)
		line += timingFields("success_local", g.successLocal)
	}
	fmt.Println(line)
}

func timingFields(prefix string, values []time.Duration) string {
	stats := summarize(values)
	return fmt.Sprintf(" %s_min=%s %s_mean=%s %s_p50=%s %s_p95=%s %s_p99=%s %s_max=%s", prefix, stats.Min, prefix, stats.Mean, prefix, stats.P50, prefix, stats.P95, prefix, stats.P99, prefix, stats.Max)
}

func output(raw string, err error, decimals, symbol string) string {
	if err != nil {
		return "n/a"
	}
	formatted, formatErr := okxexperiment.FormatBaseUnits(raw, decimals)
	if formatErr != nil {
		return "format_error=" + formatErr.Error()
	}
	return formatted + unitSuffix(symbol)
}

func printAmountSummary(variant string, values []string, decimals, symbol string) error {
	if len(values) == 0 {
		return fmt.Errorf("no successful amount samples collected for %s", variant)
	}
	stats, err := okxexperiment.SummarizeBaseUnits(values, decimals)
	if err != nil {
		return err
	}
	fmt.Printf("summary amounts provider=0x variant=%s samples=%d unit=%s min=%s mean=%s p50=%s p95=%s p99=%s max=%s\n", variant, stats.Samples, symbol, stats.Min, stats.Mean, stats.P50, stats.P95, stats.P99, stats.Max)
	return nil
}

func errorStatus(err error) string {
	if err == nil {
		return "ok"
	}
	return "error=" + strings.ReplaceAll(err.Error(), " ", "_")
}

func firstError(first, second error) error {
	if first != nil {
		return first
	}
	return second
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
	return statistics{
		Min: sorted[0], Mean: total / time.Duration(len(sorted)),
		P50: percentile(sorted, 0.50), P95: percentile(sorted, 0.95),
		P99: percentile(sorted, 0.99), Max: sorted[len(sorted)-1],
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
