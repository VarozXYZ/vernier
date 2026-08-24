// Package aggregatorcompare implements a standalone, read-only latency and
// quote-quality experiment for two EVM aggregators.
package aggregatorcompare

import (
	"bufio"
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/VarozXYZ/vernier/adapters/quote/kyberswap"
	"github.com/VarozXYZ/vernier/adapters/quote/velora"
)

const defaultEnvFile = ".env.test"

var envKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type settings struct {
	chainID                             uint64
	kyberChain                          string
	tokenA, tokenB                      string
	decimalsA, decimalsB                uint8
	userAddress, partner, kyberClientID string
	slippageBPS                         uint16
	minimum, maximum                    int64
	gasEstimation                       bool
}

type token struct {
	address  string
	decimals uint8
}
type trade struct {
	direction     string
	input, output token
	whole         int64
	amount        string
}

type kyberAPI interface {
	Route(context.Context, kyberswap.RouteRequest) (kyberswap.RouteResult, error)
	Build(context.Context, kyberswap.BuildRequest) (kyberswap.BuildResult, error)
}

type veloraAPI interface {
	Price(context.Context, velora.PriceRequest) (velora.PriceResult, error)
	Transaction(context.Context, velora.TransactionRequest) (velora.TransactionResult, error)
	Swap(context.Context, velora.SwapRequest) (velora.SwapResult, error)
}

type measurement struct {
	quote, build, pipeline      time.Duration
	quoteOutput, preparedOutput string
	quoteErr, buildErr          error
	err                         error
}

type sample struct {
	kyber, velora measurement
	startSkew     time.Duration
	direction     string
}

func Run(parent context.Context, args []string) error {
	flags := flag.NewFlagSet("aggregator-latency-compare", flag.ContinueOnError)
	envPath := flags.String("env-file", defaultEnvFile, "private environment file")
	mode := flags.String("mode", "both", "split, swap, or both (both runs two isolated experiments)")
	duration := flags.Duration("duration", 90*time.Second, "duration of each experiment")
	interval := flags.Duration("interval", 5*time.Second, "start-to-start sample interval")
	seed := flags.Int64("seed", 0, "random seed; zero generates and prints one")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || (*mode != "split" && *mode != "swap" && *mode != "both") || *duration <= 0 || *interval <= 0 {
		return fmt.Errorf("usage: go run ./cmd/aggregator-latency-compare [-mode split|swap|both] [-duration 90s] [-interval 5s] [-seed N]")
	}
	if err := loadEnvFile(*envPath); err != nil {
		return fmt.Errorf("load %s: %w", *envPath, err)
	}
	config, err := settingsFromEnvironment()
	if err != nil {
		return err
	}
	if *seed == 0 {
		var raw [8]byte
		if _, err := cryptorand.Read(raw[:]); err != nil {
			return fmt.Errorf("generate random seed: %w", err)
		}
		*seed = int64(binary.LittleEndian.Uint64(raw[:]))
	}
	kyberSource, err := kyberswap.New(kyberswap.Config{ClientID: config.kyberClientID})
	if err != nil {
		return err
	}
	veloraSource, err := velora.New(velora.Config{})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(parent, os.Interrupt)
	defer stop()

	modes := []string{*mode}
	if *mode == "both" {
		modes = []string{"split", "swap"}
	}
	fmt.Printf("experiments=%d duration_each=%s interval=%s seed=%d amount_range_whole=%d..%d direction=alternating_exact_input\n", len(modes), *duration, *interval, *seed, config.minimum, config.maximum)
	fmt.Printf("network_chain_id=%d kyberswap_chain=%s velora_version=%s gas_estimation=%t transaction_checks=%t\n", config.chainID, config.kyberChain, velora.Version, config.gasEstimation, config.gasEstimation)
	fmt.Println("broadcast=false signing=false approvals=false identifiers_logged=false")
	for _, selectedMode := range modes {
		if err := runMode(ctx, selectedMode, *duration, *interval, *seed, config, kyberSource, veloraSource); err != nil {
			return err
		}
	}
	return nil
}

func runMode(ctx context.Context, mode string, duration, interval time.Duration, seed int64, config settings, kyberSource kyberAPI, veloraSource veloraAPI) error {
	sampleCount := int((duration + interval - 1) / interval)
	random := rand.New(rand.NewSource(seed))
	results := make([]sample, 0, sampleCount)
	nextStart := time.Now()
	fmt.Printf("experiment_started mode=%s samples=%d flow=%s\n", mode, sampleCount, flowName(mode))
	for index := 0; index < sampleCount; index++ {
		if err := waitUntil(ctx, nextStart); err != nil {
			return err
		}
		started := time.Now()
		whole := config.minimum + random.Int63n(config.maximum-config.minimum+1)
		current := makeTrade(index, whole, config)
		var result sample
		if mode == "split" {
			result = splitSample(ctx, current, config, kyberSource, veloraSource)
		} else {
			result = swapSample(ctx, current, config, kyberSource, veloraSource)
		}
		result.direction = current.direction
		nextStart = started.Add(interval)
		results = append(results, result)
		printSample(index+1, mode, current, result)
	}
	return printSummary(mode, results)
}

func splitSample(ctx context.Context, current trade, config settings, kyberSource kyberAPI, veloraSource veloraAPI) sample {
	var result sample
	var route kyberswap.RouteResult
	var price velora.PriceResult
	var kyberStarted, veloraStarted time.Time
	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		kyberStarted = time.Now()
		stage := kyberStarted
		route, result.kyber.quoteErr = kyberSource.Route(ctx, kyberRequest(current, config))
		result.kyber.err = result.kyber.quoteErr
		result.kyber.quote = time.Since(stage)
		if result.kyber.quoteErr == nil {
			result.kyber.quoteOutput = route.AmountOut
		}
	}()
	go func() {
		defer wait.Done()
		<-start
		veloraStarted = time.Now()
		stage := veloraStarted
		price, result.velora.quoteErr = veloraSource.Price(ctx, veloraRequest(current, config))
		result.velora.err = result.velora.quoteErr
		result.velora.quote = time.Since(stage)
		if result.velora.quoteErr == nil {
			result.velora.quoteOutput = price.DestAmount
		}
	}()
	close(start)
	wait.Wait()
	result.startSkew = absoluteDuration(kyberStarted.Sub(veloraStarted))

	buildStart := make(chan struct{})
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-buildStart
		if result.kyber.quoteErr != nil {
			return
		}
		stage := time.Now()
		built, err := kyberSource.Build(ctx, kyberswap.BuildRequest{Route: route, Sender: config.userAddress, Recipient: config.userAddress, Origin: config.userAddress, SlippageBPS: config.slippageBPS, EnableGasEstimation: config.gasEstimation})
		result.kyber.build = time.Since(stage)
		result.kyber.buildErr, result.kyber.err = err, err
		if err == nil {
			result.kyber.preparedOutput = built.AmountOut
		}
	}()
	go func() {
		defer wait.Done()
		<-buildStart
		if result.velora.quoteErr != nil {
			return
		}
		stage := time.Now()
		_, err := veloraSource.Transaction(ctx, velora.TransactionRequest{Price: price, UserAddress: config.userAddress, SlippageBPS: config.slippageBPS, Partner: config.partner, IgnoreChecks: !config.gasEstimation})
		result.velora.build = time.Since(stage)
		result.velora.buildErr, result.velora.err = err, err
		if err == nil {
			result.velora.preparedOutput = price.DestAmount
		}
	}()
	close(buildStart)
	wait.Wait()
	result.kyber.pipeline = result.kyber.quote + result.kyber.build
	result.velora.pipeline = result.velora.quote + result.velora.build
	return result
}

func swapSample(ctx context.Context, current trade, config settings, kyberSource kyberAPI, veloraSource veloraAPI) sample {
	var result sample
	var kyberStarted, veloraStarted time.Time
	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		kyberStarted = time.Now()
		pipelineStarted := kyberStarted
		stage := time.Now()
		route, err := kyberSource.Route(ctx, kyberRequest(current, config))
		result.kyber.quote = time.Since(stage)
		result.kyber.quoteErr, result.kyber.err = err, err
		if err != nil {
			result.kyber.pipeline = time.Since(pipelineStarted)
			return
		}
		result.kyber.quoteOutput = route.AmountOut
		stage = time.Now()
		built, err := kyberSource.Build(ctx, kyberswap.BuildRequest{Route: route, Sender: config.userAddress, Recipient: config.userAddress, Origin: config.userAddress, SlippageBPS: config.slippageBPS, EnableGasEstimation: config.gasEstimation})
		result.kyber.build = time.Since(stage)
		result.kyber.pipeline = time.Since(pipelineStarted)
		result.kyber.buildErr, result.kyber.err = err, err
		if err == nil {
			result.kyber.preparedOutput = built.AmountOut
		}
	}()
	go func() {
		defer wait.Done()
		<-start
		veloraStarted = time.Now()
		stage := veloraStarted
		swapped, err := veloraSource.Swap(ctx, velora.SwapRequest{PriceRequest: veloraRequest(current, config), SlippageBPS: config.slippageBPS})
		result.velora.pipeline = time.Since(stage)
		result.velora.quote = result.velora.pipeline
		result.velora.quoteErr, result.velora.err = err, err
		if err == nil {
			result.velora.quoteOutput, result.velora.preparedOutput = swapped.Price.DestAmount, swapped.Price.DestAmount
		}
	}()
	close(start)
	wait.Wait()
	result.startSkew = absoluteDuration(kyberStarted.Sub(veloraStarted))
	return result
}

func makeTrade(index int, whole int64, config settings) trade {
	a := token{address: config.tokenA, decimals: config.decimalsA}
	b := token{address: config.tokenB, decimals: config.decimalsB}
	current := trade{direction: "a_to_b", input: a, output: b, whole: whole}
	if index%2 == 1 {
		current.direction, current.input, current.output = "b_to_a", b, a
	}
	current.amount = new(big.Int).Mul(big.NewInt(whole), new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(current.input.decimals)), nil)).String()
	return current
}

func kyberRequest(current trade, config settings) kyberswap.RouteRequest {
	return kyberswap.RouteRequest{Chain: config.kyberChain, TokenIn: current.input.address, TokenOut: current.output.address, AmountIn: current.amount, Origin: config.userAddress}
}

func veloraRequest(current trade, config settings) velora.PriceRequest {
	return velora.PriceRequest{Network: config.chainID, SourceToken: current.input.address, SourceUnits: current.input.decimals, DestToken: current.output.address, DestUnits: current.output.decimals, Amount: current.amount, UserAddress: config.userAddress, Partner: config.partner}
}

func printSample(index int, mode string, current trade, result sample) {
	quoteDelta, quoteOK := deltaBPS(result.kyber.quoteOutput, result.velora.quoteOutput)
	preparedDelta, preparedOK := deltaBPS(result.kyber.preparedOutput, result.velora.preparedOutput)
	fmt.Printf("sample=%d mode=%s direction=%s input_whole=%d start_skew=%s kyber_quote=%s kyber_build=%s kyber_pipeline=%s velora_quote=%s velora_build=%s velora_pipeline=%s quote_delta_bps=%s prepared_delta_bps=%s kyber_status=%s velora_status=%s\n",
		index, mode, current.direction, current.whole, result.startSkew,
		result.kyber.quote, result.kyber.build, result.kyber.pipeline,
		result.velora.quote, result.velora.build, result.velora.pipeline,
		formatDelta(quoteDelta, quoteOK), formatDelta(preparedDelta, preparedOK), errorStatus(result.kyber.err), errorStatus(result.velora.err))
}

func printSummary(mode string, results []sample) error {
	var kyberQuote, kyberBuild, kyberPipeline, veloraQuote, veloraBuild, veloraPipeline []time.Duration
	var quoteDeltas, preparedDeltas []float64
	kyberQuoteErrors, kyberBuildErrors, kyberErrors := 0, 0, 0
	veloraQuoteErrors, veloraBuildErrors, veloraErrors := 0, 0, 0
	for _, result := range results {
		if result.kyber.quote > 0 {
			kyberQuote = append(kyberQuote, result.kyber.quote)
		}
		if result.kyber.build > 0 {
			kyberBuild = append(kyberBuild, result.kyber.build)
		}
		if result.kyber.pipeline > 0 {
			kyberPipeline = append(kyberPipeline, result.kyber.pipeline)
		}
		if result.velora.quote > 0 {
			veloraQuote = append(veloraQuote, result.velora.quote)
		}
		if result.velora.build > 0 {
			veloraBuild = append(veloraBuild, result.velora.build)
		}
		if result.velora.pipeline > 0 {
			veloraPipeline = append(veloraPipeline, result.velora.pipeline)
		}
		if result.kyber.err != nil {
			kyberErrors++
		}
		if result.kyber.quoteErr != nil {
			kyberQuoteErrors++
		}
		if result.kyber.buildErr != nil {
			kyberBuildErrors++
		}
		if result.velora.err != nil {
			veloraErrors++
		}
		if result.velora.quoteErr != nil {
			veloraQuoteErrors++
		}
		if result.velora.buildErr != nil {
			veloraBuildErrors++
		}
		if value, ok := deltaBPS(result.kyber.quoteOutput, result.velora.quoteOutput); ok {
			quoteDeltas = append(quoteDeltas, value)
		}
		if value, ok := deltaBPS(result.kyber.preparedOutput, result.velora.preparedOutput); ok {
			preparedDeltas = append(preparedDeltas, value)
		}
	}
	printTiming(mode, "kyberswap", "quote", kyberQuote, kyberQuoteErrors)
	printTiming(mode, "kyberswap", "build", kyberBuild, kyberBuildErrors)
	printTiming(mode, "kyberswap", "pipeline", kyberPipeline, kyberErrors)
	veloraQuoteStage := "prices"
	if mode == "swap" {
		veloraQuoteStage = "swap"
	}
	printTiming(mode, "velora", veloraQuoteStage, veloraQuote, veloraQuoteErrors)
	if mode == "split" {
		printTiming(mode, "velora", "transactions", veloraBuild, veloraBuildErrors)
	}
	printTiming(mode, "velora", "pipeline", veloraPipeline, veloraErrors)
	printFloatSummary(mode, "quote_delta_bps(kyberswap-velora)", quoteDeltas)
	printFloatSummary(mode, "prepared_delta_bps(kyberswap-velora)", preparedDeltas)
	for _, direction := range []string{"a_to_b", "b_to_a"} {
		var directionalQuote, directionalPrepared []float64
		for _, result := range results {
			if result.direction != direction {
				continue
			}
			if value, ok := deltaBPS(result.kyber.quoteOutput, result.velora.quoteOutput); ok {
				directionalQuote = append(directionalQuote, value)
			}
			if value, ok := deltaBPS(result.kyber.preparedOutput, result.velora.preparedOutput); ok {
				directionalPrepared = append(directionalPrepared, value)
			}
		}
		printFloatSummary(mode, "quote_delta_bps(kyberswap-velora)_"+direction, directionalQuote)
		printFloatSummary(mode, "prepared_delta_bps(kyberswap-velora)_"+direction, directionalPrepared)
	}
	if len(quoteDeltas) == 0 {
		return fmt.Errorf("mode %s produced no paired successful quotes", mode)
	}
	return nil
}

func printTiming(mode, provider, stage string, values []time.Duration, errors int) {
	stats := summarizeDurations(values)
	fmt.Printf("summary mode=%s provider=%s stage=%s attempts=%d errors=%d min=%s mean=%s p50=%s p95=%s max=%s\n", mode, provider, stage, len(values), errors, stats.min, stats.mean, stats.p50, stats.p95, stats.max)
}

func printFloatSummary(mode, metric string, values []float64) {
	stats := summarizeFloats(values)
	fmt.Printf("summary mode=%s metric=%s samples=%d min=%.4f mean=%.4f p50=%.4f p95=%.4f max=%.4f\n", mode, metric, len(values), stats.min, stats.mean, stats.p50, stats.p95, stats.max)
}

type durationStats struct{ min, mean, p50, p95, max time.Duration }

func summarizeDurations(values []time.Duration) durationStats {
	if len(values) == 0 {
		return durationStats{}
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var total time.Duration
	for _, value := range sorted {
		total += value
	}
	return durationStats{min: sorted[0], mean: total / time.Duration(len(sorted)), p50: sorted[percentileIndex(len(sorted), .5)], p95: sorted[percentileIndex(len(sorted), .95)], max: sorted[len(sorted)-1]}
}

type floatStats struct{ min, mean, p50, p95, max float64 }

func summarizeFloats(values []float64) floatStats {
	if len(values) == 0 {
		return floatStats{}
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	var total float64
	for _, value := range sorted {
		total += value
	}
	return floatStats{min: sorted[0], mean: total / float64(len(sorted)), p50: sorted[percentileIndex(len(sorted), .5)], p95: sorted[percentileIndex(len(sorted), .95)], max: sorted[len(sorted)-1]}
}

func deltaBPS(first, second string) (float64, bool) {
	a, okA := new(big.Int).SetString(first, 10)
	b, okB := new(big.Int).SetString(second, 10)
	if !okA || !okB || b.Sign() <= 0 {
		return 0, false
	}
	delta := new(big.Int).Sub(a, b)
	ratio := new(big.Rat).SetFrac(new(big.Int).Mul(delta, big.NewInt(10_000)), b)
	value, _ := ratio.Float64()
	return value, true
}

func formatDelta(value float64, ok bool) string {
	if !ok {
		return "n/a"
	}
	return fmt.Sprintf("%+.4f", value)
}
func errorStatus(err error) string {
	if err == nil {
		return "ok"
	}
	var veloraError *velora.APIError
	if errors.As(err, &veloraError) {
		return fmt.Sprintf("error_http_%d", veloraError.HTTPStatus)
	}
	var kyberError *kyberswap.APIError
	if errors.As(err, &kyberError) {
		return fmt.Sprintf("error_http_%d", kyberError.HTTPStatusCode())
	}
	return "error"
}
func absoluteDuration(value time.Duration) time.Duration {
	if value < 0 {
		return -value
	}
	return value
}
func percentileIndex(length int, fraction float64) int {
	index := int(math.Ceil(float64(length)*fraction)) - 1
	if index < 0 {
		return 0
	}
	if index >= length {
		return length - 1
	}
	return index
}
func flowName(mode string) string {
	if mode == "split" {
		return "concurrent_quotes_barrier_then_concurrent_builds"
	}
	return "kyber_quote_then_build_vs_velora_swap"
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

func settingsFromEnvironment() (settings, error) {
	config := settings{kyberChain: strings.TrimSpace(os.Getenv("AGGREGATOR_COMPARE_KYBER_CHAIN")), tokenA: strings.TrimSpace(os.Getenv("AGGREGATOR_COMPARE_TOKEN_A")), tokenB: strings.TrimSpace(os.Getenv("AGGREGATOR_COMPARE_TOKEN_B")), userAddress: strings.TrimSpace(os.Getenv("AGGREGATOR_COMPARE_USER_ADDRESS")), partner: strings.TrimSpace(os.Getenv("AGGREGATOR_COMPARE_VELORA_PARTNER")), kyberClientID: strings.TrimSpace(os.Getenv("KYBERSWAP_CLIENT_ID")), minimum: 100, maximum: 500, slippageBPS: 50}
	if config.kyberChain == "" {
		config.kyberChain = "bsc"
	}
	if config.partner == "" {
		config.partner = "vernier"
	}
	if strings.TrimSpace(os.Getenv("AGGREGATOR_COMPARE_TOKEN_A_DECIMALS")) == "" || strings.TrimSpace(os.Getenv("AGGREGATOR_COMPARE_TOKEN_B_DECIMALS")) == "" {
		return settings{}, fmt.Errorf("AGGREGATOR_COMPARE_TOKEN_A_DECIMALS and AGGREGATOR_COMPARE_TOKEN_B_DECIMALS are required")
	}
	var err error
	if config.chainID, err = parseUint("AGGREGATOR_COMPARE_CHAIN_ID", os.Getenv("AGGREGATOR_COMPARE_CHAIN_ID"), 56, 64); err != nil {
		return settings{}, err
	}
	decimalsA, err := parseUint("AGGREGATOR_COMPARE_TOKEN_A_DECIMALS", os.Getenv("AGGREGATOR_COMPARE_TOKEN_A_DECIMALS"), 0, 8)
	if err != nil {
		return settings{}, err
	}
	config.decimalsA = uint8(decimalsA)
	decimalsB, err := parseUint("AGGREGATOR_COMPARE_TOKEN_B_DECIMALS", os.Getenv("AGGREGATOR_COMPARE_TOKEN_B_DECIMALS"), 0, 8)
	if err != nil {
		return settings{}, err
	}
	config.decimalsB = uint8(decimalsB)
	minimum, err := parseUint("AGGREGATOR_COMPARE_MIN_WHOLE", os.Getenv("AGGREGATOR_COMPARE_MIN_WHOLE"), 100, 63)
	if err != nil {
		return settings{}, err
	}
	config.minimum = int64(minimum)
	maximum, err := parseUint("AGGREGATOR_COMPARE_MAX_WHOLE", os.Getenv("AGGREGATOR_COMPARE_MAX_WHOLE"), 500, 63)
	if err != nil {
		return settings{}, err
	}
	config.maximum = int64(maximum)
	slippage, err := parseUint("AGGREGATOR_COMPARE_SLIPPAGE_BPS", os.Getenv("AGGREGATOR_COMPARE_SLIPPAGE_BPS"), 50, 16)
	if err != nil || slippage > 2_000 {
		return settings{}, fmt.Errorf("AGGREGATOR_COMPARE_SLIPPAGE_BPS must be between 0 and 2000")
	}
	config.slippageBPS = uint16(slippage)
	config.gasEstimation, err = strconv.ParseBool(defaultString(os.Getenv("AGGREGATOR_COMPARE_GAS_ESTIMATION"), "false"))
	if err != nil {
		return settings{}, fmt.Errorf("AGGREGATOR_COMPARE_GAS_ESTIMATION must be true or false")
	}
	if !common.IsHexAddress(config.tokenA) || !common.IsHexAddress(config.tokenB) || strings.EqualFold(config.tokenA, config.tokenB) {
		return settings{}, fmt.Errorf("AGGREGATOR_COMPARE_TOKEN_A and AGGREGATOR_COMPARE_TOKEN_B must be distinct EVM addresses")
	}
	if !common.IsHexAddress(config.userAddress) || common.HexToAddress(config.userAddress) == (common.Address{}) {
		return settings{}, fmt.Errorf("AGGREGATOR_COMPARE_USER_ADDRESS must be a non-zero EVM address")
	}
	if config.kyberClientID == "" {
		return settings{}, fmt.Errorf("KYBERSWAP_CLIENT_ID is required")
	}
	if config.decimalsA > 36 || config.decimalsB > 36 {
		return settings{}, fmt.Errorf("token decimals must be <= 36")
	}
	if config.minimum < 1 || config.maximum < config.minimum {
		return settings{}, fmt.Errorf("amount range must be positive and ordered")
	}
	return config, nil
}

func parseUint(name, raw string, fallback uint64, bits int) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseUint(raw, 10, bits)
	if err != nil {
		return 0, fmt.Errorf("%s must be an unsigned integer", name)
	}
	return value, nil
}
func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func loadEnvFile(path string) error {
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
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		value = strings.TrimSpace(value)
		if len(value) >= 2 && ((value[0] == '\'' && value[len(value)-1] == '\'') || (value[0] == '"' && value[len(value)-1] == '"')) {
			if value[0] == '\'' {
				value = value[1 : len(value)-1]
			} else {
				value, err = strconv.Unquote(value)
				if err != nil {
					return fmt.Errorf("invalid quoted environment value")
				}
			}
		}
		if err := os.Setenv(key, value); err != nil {
			return err
		}
	}
	return scanner.Err()
}
