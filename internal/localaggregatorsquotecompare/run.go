// Package localaggregatorsquotecompare implements standalone local split
// comparison experiments without exposing them to Research composition.
package localaggregatorsquotecompare

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/big"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/adapters/feed/evmlogs"
	"github.com/VarozXYZ/vernier/adapters/feed/sourceorder"
	"github.com/VarozXYZ/vernier/adapters/market/uniswapv3"
	"github.com/VarozXYZ/vernier/adapters/market/uniswapv4"
	"github.com/VarozXYZ/vernier/adapters/quote/kyberswap"
	"github.com/VarozXYZ/vernier/adapters/quote/route"
	"github.com/VarozXYZ/vernier/adapters/quote/zerox"
	"github.com/VarozXYZ/vernier/cmd/localquoteexperiment"
	"github.com/VarozXYZ/vernier/cmd/okxexperiment"
	"github.com/VarozXYZ/vernier/cmd/zeroxexperiment"
	"github.com/VarozXYZ/vernier/core/marketstate"
	"github.com/VarozXYZ/vernier/core/sizing"
	"github.com/VarozXYZ/vernier/domain/market"
	feedport "github.com/VarozXYZ/vernier/ports/feed"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

const (
	defaultEnvFile   = ".env.test"
	defaultPoolsFile = ".vernier/local-pools.json"
)

func Run(parent context.Context, args []string) error {
	flags := flag.NewFlagSet("local-aggregators-quote-compare", flag.ContinueOnError)
	envPath := flags.String("env-file", defaultEnvFile, "provider and EVM endpoint environment file; default .env.test")
	poolsFile := flags.String("pools-file", defaultPoolsFile, "private local pool topology JSON")
	samples := flags.Int("samples", 0, "number of quote triplets; zero uses ZEROX_LATENCY_SAMPLES")
	bootstrapTimeout := flags.Duration("bootstrap-timeout", 2*time.Minute, "maximum wait for every watcher bootstrap")
	rpcMinInterval := flags.Duration("rpc-min-interval", 100*time.Millisecond, "minimum spacing for chain RPC bootstrap/reconnect calls")
	localIterations := flags.Int("local-iterations", 1, "independent local calculations per sample for optional amortized timing")
	fastGridDivisions := flags.Int("fast-grid-divisions", 32, "bounded grid divisions for the target-50ms solver")
	hourLocalProfile := flags.Bool("hour-local-profile", false, "run only the two local profiles for 12 measurements, five minutes apart")
	localProfileInterval := flags.Duration("local-profile-interval", 5*time.Minute, "interval between scheduled long local-profile measurements")
	localProfileAmounts := flags.String("local-profile-amounts", "100,200,500,1000,2500,5000", "comma-separated whole-token amounts for the long local profile")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || *samples < 0 || *bootstrapTimeout <= 0 || *rpcMinInterval < 0 ||
		*localIterations < 1 || *localProfileInterval <= 0 ||
		*fastGridDivisions < 2 || *fastGridDivisions > 1024 {
		return fmt.Errorf("usage: go run ./cmd/local-aggregators-quote-compare [-samples N] [-bootstrap-timeout 2m]")
	}
	var settings zeroxexperiment.Settings
	var err error
	if *hourLocalProfile {
		settings, err = zeroxexperiment.LoadLocalSettings(*envPath)
	} else {
		settings, err = zeroxexperiment.LoadSettings(*envPath)
	}
	if err != nil {
		return err
	}
	if *samples > 0 {
		settings.Samples = *samples
	}
	topology, err := loadTopology(*poolsFile)
	if err != nil {
		return err
	}
	if err := topology.validateAgainst(settings); err != nil {
		return err
	}
	httpURL := strings.TrimSpace(os.Getenv("ROBINHOOD_HTTP_URL"))
	wsURL := strings.TrimSpace(os.Getenv("ROBINHOOD_WS_URL"))
	if httpURL == "" || wsURL == "" {
		return fmt.Errorf("ROBINHOOD_HTTP_URL and ROBINHOOD_WS_URL are required in %s or process environment", *envPath)
	}
	var zeroSource *zerox.Source
	var kyberSource *kyberswap.Source
	var kyberChain string
	if !*hourLocalProfile {
		clientID := strings.TrimSpace(os.Getenv("KYBERSWAP_CLIENT_ID"))
		if clientID == "" {
			return fmt.Errorf("KYBERSWAP_CLIENT_ID is required in %s or process environment", *envPath)
		}
		kyberChain = strings.TrimSpace(os.Getenv("KYBERSWAP_CHAIN"))
		if kyberChain == "" {
			kyberChain = kyberswap.DefaultChain
		}
		zeroSource, err = settings.Source()
		if err != nil {
			return err
		}
		kyberSource, err = kyberswap.New(kyberswap.Config{ClientID: clientID})
		if err != nil {
			return err
		}
	}

	ctx, stop := signal.NotifyContext(parent, os.Interrupt)
	defer stop()
	baseNetwork, err := dialNetwork(ctx, topology.ChainID, httpURL, wsURL)
	if err != nil {
		return err
	}
	defer baseNetwork.Close()
	network, err := evm.NewRateLimitedNetwork(baseNetwork, *rpcMinInterval)
	if err != nil {
		return err
	}

	amountIn, ok := new(big.Int).SetString(settings.SellAmount, 10)
	if !ok || amountIn.Sign() <= 0 {
		return fmt.Errorf("ZEROX_SELL_AMOUNT must be a positive base-unit integer")
	}
	referenceGridDivisions := 1000
	var referenceStep *big.Int
	var profileAmounts []profileAmount
	probeAmount := new(big.Int).Set(amountIn)
	if *hourLocalProfile {
		profileAmounts, err = parseProfileAmounts(
			*localProfileAmounts, topology.Input.Decimals, referenceGridDivisions,
		)
		if err != nil {
			return err
		}
		probeAmount = new(big.Int).Set(profileAmounts[len(profileAmounts)-1].amount.Units())
	} else {
		referenceGridDivisions, referenceStep, err = tenthTokenReferenceGrid(amountIn, topology.Input.Decimals)
		if err != nil {
			return err
		}
	}
	watchers, err := buildWatchers(topology, network, probeAmount)
	if err != nil {
		return err
	}
	fmt.Printf("watchers=%d expected_routes=%d bootstrap_started=true rpc_min_interval=%s\n", len(watchers), expectedRouteCount(watchers), *rpcMinInterval)
	bootstrapStarted := time.Now()
	feedCtx, cancelFeeds := context.WithCancel(ctx)
	defer cancelFeeds()
	if err := startAndAwaitWatchers(feedCtx, watchers, *bootstrapTimeout); err != nil {
		return err
	}
	fmt.Printf("watchers_ready=%d healthy=%d bootstrap_duration=%s\n", len(watchers), healthyCount(watchers), time.Since(bootstrapStarted))

	candidates, err := buildCandidates(watchers)
	if err != nil {
		return err
	}
	if len(candidates) != expectedRouteCount(watchers) {
		return fmt.Errorf("built %d local routes, expected %d", len(candidates), expectedRouteCount(watchers))
	}

	zeroRequest := settings.Request()
	kyberRequest := kyberswap.RouteRequest{
		Chain: kyberChain, TokenIn: settings.SellToken, TokenOut: settings.BuyToken,
		AmountIn: settings.SellAmount, Origin: settings.Taker,
	}
	localAmount, err := market.NewTokenAmount(inputTokenID, amountIn)
	if err != nil {
		return err
	}
	if *hourLocalProfile {
		profileSamples := 12
		if *samples > 0 {
			profileSamples = *samples
		}
		return runLongLocalProfile(
			ctx, watchers, topology, profileAmounts, profileSamples,
			*localProfileInterval, *localIterations, referenceGridDivisions, *fastGridDivisions,
		)
	}
	fmt.Printf("routes=%d providers_called_in_parallel=true pair_interval=%s client_side_rate_limiter=false\n", len(candidates), settings.Interval)
	fmt.Printf(
		"local_quote_source=verified_grid_multipool_split local_baseline=best_single_route local_state_source=fixed_live_snapshots local_iterations=%d reference_step=%s_%s reference_grid_divisions=%d reference_includes_fast_grid=true\n",
		*localIterations, formatBaseUnitsOrRaw(referenceStep, topology.Input.Decimals),
		strings.ReplaceAll(strings.TrimSpace(topology.Input.Symbol), " ", "_"), referenceGridDivisions,
	)
	fmt.Printf("local_fast_solver=bounded_dynamic_grid target_latency=50ms grid_divisions=%d\n", *fastGridDivisions)
	fmt.Println("delta_definition=first_provider_output-second_provider_output positive_means_first_provider_more_output")
	fmt.Println("broadcast=false signing=false approvals=false")

	timings := map[string]*timingGroup{
		"local_precise": newTimingGroup(), "local_fast": newTimingGroup(), "local_single": newTimingGroup(),
		"0x": newTimingGroup(), "kyberswap": newTimingGroup(),
	}
	stageTimings := map[string]*timingGroup{
		"local_snapshot": newTimingGroup(), "local_split_compute": newTimingGroup(),
		"local_fast_compute": newTimingGroup(), "local_single_compute": newTimingGroup(),
		"0x_http": newTimingGroup(), "0x_local": newTimingGroup(),
		"kyberswap_http": newTimingGroup(), "kyberswap_local": newTimingGroup(),
	}
	amounts := map[string][]*big.Int{
		"local_precise": {}, "local_fast": {}, "local_single": {}, "0x": {}, "kyberswap": {},
	}
	deltas := map[string][]*big.Int{
		"local_precise-local_fast": {}, "local_precise-local_single": {},
		"local_precise-0x": {}, "local_precise-kyberswap": {}, "kyberswap-0x": {},
	}
	nextStart := time.Now()
	for index := 0; index < settings.Samples; index++ {
		if err := waitUntil(ctx, nextStart); err != nil {
			return err
		}
		started := time.Now()
		localResult, zeroResult, kyberResult := quoteTriplet(
			ctx, candidates, watchers, topology, localAmount, *localIterations,
			referenceGridDivisions, *fastGridDivisions,
			zeroSource, zeroRequest, kyberSource, kyberRequest,
		)
		nextStart = started.Add(settings.Interval)

		fastResult := measurement{amount: localResult.fastAmount, err: localResult.fastErr}
		timings["local_precise"].add(localResult.duration, localResult.err)
		timings["local_fast"].add(localResult.snapshotDuration+localResult.fastComputeDuration, localResult.fastErr)
		timings["local_single"].add(localResult.snapshotDuration+localResult.singleComputeDuration, localResult.singleErr)
		timings["0x"].add(zeroResult.duration, zeroResult.err)
		timings["kyberswap"].add(kyberResult.duration, kyberResult.err)
		stageTimings["local_snapshot"].add(localResult.snapshotDuration, localResult.err)
		stageTimings["local_split_compute"].add(localResult.computeDuration, localResult.err)
		stageTimings["local_fast_compute"].add(localResult.fastComputeDuration, localResult.fastErr)
		stageTimings["local_single_compute"].add(localResult.singleComputeDuration, localResult.singleErr)
		stageTimings["0x_http"].add(zeroResult.httpDuration, zeroResult.err)
		stageTimings["0x_local"].add(nonNegative(zeroResult.duration-zeroResult.httpDuration), zeroResult.err)
		stageTimings["kyberswap_http"].add(kyberResult.httpDuration, kyberResult.err)
		stageTimings["kyberswap_local"].add(nonNegative(kyberResult.duration-kyberResult.httpDuration), kyberResult.err)
		if localResult.err == nil {
			amounts["local_precise"] = append(amounts["local_precise"], localResult.amount)
		}
		if localResult.fastErr == nil {
			amounts["local_fast"] = append(amounts["local_fast"], localResult.fastAmount)
		}
		if localResult.singleErr == nil {
			amounts["local_single"] = append(amounts["local_single"], localResult.singleAmount)
		}
		if zeroResult.err == nil {
			amounts["0x"] = append(amounts["0x"], zeroResult.amount)
		}
		if kyberResult.err == nil {
			amounts["kyberswap"] = append(amounts["kyberswap"], kyberResult.amount)
		}
		appendDelta(localResult, fastResult, func(value *big.Int) {
			deltas["local_precise-local_fast"] = append(deltas["local_precise-local_fast"], value)
		})
		appendDelta(
			localResult,
			measurement{amount: localResult.singleAmount, err: localResult.singleErr},
			func(value *big.Int) {
				deltas["local_precise-local_single"] = append(deltas["local_precise-local_single"], value)
			},
		)
		appendDelta(localResult, zeroResult, func(value *big.Int) {
			deltas["local_precise-0x"] = append(deltas["local_precise-0x"], value)
		})
		appendDelta(localResult, kyberResult, func(value *big.Int) {
			deltas["local_precise-kyberswap"] = append(deltas["local_precise-kyberswap"], value)
		})
		appendDelta(kyberResult, zeroResult, func(value *big.Int) {
			deltas["kyberswap-0x"] = append(deltas["kyberswap-0x"], value)
		})
		printSample(index+1, settings.BuyDecimals, settings.BuySymbol, localResult, zeroResult, kyberResult)
	}

	for _, provider := range []string{"local_precise", "local_fast", "local_single", "0x", "kyberswap"} {
		timings[provider].print(provider, "total")
		if err := printAmountSummary(provider, amounts[provider], settings.BuyDecimals, settings.BuySymbol); err != nil {
			return err
		}
	}
	for _, item := range []struct{ key, provider, stage string }{
		{"local_snapshot", "local_precise", "snapshot"},
		{"local_split_compute", "local_precise", "compute"},
		{"local_fast_compute", "local_fast", "compute"},
		{"local_single_compute", "local_single", "compute"},
		{"0x_http", "0x", "http"},
		{"0x_local", "0x", "local"},
		{"kyberswap_http", "kyberswap", "http"},
		{"kyberswap_local", "kyberswap", "local"},
	} {
		stageTimings[item.key].print(item.provider, item.stage)
	}
	for _, name := range []string{
		"local_precise-local_fast", "local_precise-local_single",
		"local_precise-0x", "local_precise-kyberswap", "kyberswap-0x",
	} {
		if err := printAmountSummary("delta("+name+")", deltas[name], settings.BuyDecimals, settings.BuySymbol); err != nil {
			return err
		}
	}
	return nil
}

func RunHourLocalProfile(parent context.Context, args []string) error {
	hourArgs := make([]string, 0, len(args)+1)
	hourArgs = append(hourArgs, "-hour-local-profile")
	hourArgs = append(hourArgs, args...)
	return Run(parent, hourArgs)
}

type profileAmount struct {
	label  string
	amount market.TokenAmount
}

func parseProfileAmounts(raw string, decimals uint8, referenceGridDivisions int) ([]profileAmount, error) {
	parsed, err := localquoteexperiment.ParseWholeTokenAmounts(raw, decimals)
	if err != nil {
		return nil, fmt.Errorf("parse local profile amounts: %w", err)
	}
	divisor := big.NewInt(int64(referenceGridDivisions))
	result := make([]profileAmount, len(parsed))
	for index, configured := range parsed {
		units := configured.BaseUnits()
		if new(big.Int).Rem(new(big.Int).Set(units), divisor).Sign() != 0 {
			return nil, fmt.Errorf(
				"local profile amount %s cannot be divided exactly into %d reference intervals",
				configured.Label(), referenceGridDivisions,
			)
		}
		amount, amountErr := market.NewTokenAmount(inputTokenID, units)
		if amountErr != nil {
			return nil, fmt.Errorf("local profile amount %s: %w", configured.Label(), amountErr)
		}
		result[index] = profileAmount{label: configured.Label(), amount: amount}
	}
	return result, nil
}

type longProfileStats struct {
	preciseTimings   *timingGroup
	fastTimings      *timingGroup
	preciseAmounts   []*big.Int
	fastAmounts      []*big.Int
	deltas           []*big.Int
	equalOutputs     int
	preciseBetter    int
	fastBetter       int
	preciseSplits    int
	fastSplits       int
	selectedFastGrid int
}

func newLongProfileStats(samples int) *longProfileStats {
	return &longProfileStats{
		preciseTimings: newTimingGroup(), fastTimings: newTimingGroup(),
		preciseAmounts: make([]*big.Int, 0, samples),
		fastAmounts:    make([]*big.Int, 0, samples),
		deltas:         make([]*big.Int, 0, samples),
	}
}

func (s *longProfileStats) add(local measurement) {
	s.preciseTimings.add(local.computeDuration, local.err)
	s.fastTimings.add(local.fastComputeDuration, local.fastErr)
	if local.err == nil {
		s.preciseAmounts = append(s.preciseAmounts, new(big.Int).Set(local.amount))
		if local.splitUsed > 1 {
			s.preciseSplits++
		}
		if local.splitSelectedGrid == "fast" {
			s.selectedFastGrid++
		}
	}
	if local.fastErr == nil {
		s.fastAmounts = append(s.fastAmounts, new(big.Int).Set(local.fastAmount))
		if local.fastUsed > 1 {
			s.fastSplits++
		}
	}
	if local.err != nil || local.fastErr != nil {
		return
	}
	delta := new(big.Int).Sub(new(big.Int).Set(local.amount), local.fastAmount)
	s.deltas = append(s.deltas, delta)
	switch delta.Sign() {
	case -1:
		s.fastBetter++
	case 0:
		s.equalOutputs++
	case 1:
		s.preciseBetter++
	}
}

func runLongLocalProfile(
	ctx context.Context,
	watchers []*watcher,
	topology topologyConfig,
	amounts []profileAmount,
	samples int,
	interval time.Duration,
	iterations int,
	referenceGridDivisions int,
	fastGridDivisions int,
) error {
	amountLabels := make([]string, len(amounts))
	resolutions := make([]string, len(amounts))
	stats := make([]*longProfileStats, len(amounts))
	for index, configured := range amounts {
		amountLabels[index] = configured.label
		resolution := new(big.Int).Quo(
			configured.amount.Units(), big.NewInt(int64(referenceGridDivisions)),
		)
		resolutions[index] = configured.label + ":" + formatBaseUnitsOrRaw(resolution, topology.Input.Decimals)
		stats[index] = newLongProfileStats(samples)
	}
	fmt.Printf(
		"mode=hour_local_profile providers_called=false samples=%d interval=%s first_measurement_after=%s scheduled_duration=%s watchers_remain_open=true amounts=%s amounts_run_concurrently=true\n",
		samples, interval, interval, time.Duration(samples)*interval, strings.Join(amountLabels, ","),
	)
	fmt.Printf(
		"snapshot_policy=one_fixed_immutable_snapshot_set_per_measurement reference_grid_divisions=%d reference_resolutions=%s fast_grid_divisions=%d reference_includes_fast_grid=true\n",
		referenceGridDivisions, strings.Join(resolutions, ","), fastGridDivisions,
	)

	schedule, err := localquoteexperiment.MeasurementSchedule(time.Now(), interval, samples)
	if err != nil {
		return err
	}
	for sampleIndex, scheduledAt := range schedule {
		if err := waitUntil(ctx, scheduledAt); err != nil {
			return err
		}
		captureStarted := time.Now()
		snapshots, captureErr := captureLocalSnapshots(watchers)
		snapshotDuration := time.Since(captureStarted)
		if captureErr != nil {
			fmt.Printf(
				"sample=%d scheduled_lag=%s snapshot_capture=%s snapshot_error=%s\n",
				sampleIndex+1, nonNegative(captureStarted.Sub(scheduledAt)), snapshotDuration,
				strings.ReplaceAll(captureErr.Error(), " ", "_"),
			)
			for index := range stats {
				failed := measurement{err: captureErr, fastErr: captureErr}
				stats[index].add(failed)
			}
			continue
		}

		results := make([]measurement, len(amounts))
		startedAt := make([]time.Time, len(amounts))
		start := make(chan struct{})
		var wait sync.WaitGroup
		wait.Add(len(amounts))
		for index, configured := range amounts {
			go func(index int, configured profileAmount) {
				defer wait.Done()
				<-start
				startedAt[index] = time.Now()
				results[index] = measureSplitProfiles(
					ctx, watchers, snapshots, topology, configured.amount, iterations,
					referenceGridDivisions, fastGridDivisions, startedAt[index],
				)
			}(index, configured)
		}
		close(start)
		wait.Wait()
		startSkew := maximumStartSkew(startedAt...)
		for index, local := range results {
			local.snapshotDuration = snapshotDuration
			fast := measurement{amount: local.fastAmount, err: local.fastErr}
			stats[index].add(local)
			fmt.Printf(
				"sample=%d amount_in=%s_%s scheduled_lag=%s amount_start_skew=%s snapshot_capture=%s precise_output=%s fast_output=%s delta(precise-fast)=%s precise_compute=%s fast_compute=%s precise_used_pools=%d fast_used_pools=%d precise_selected_grid=%s precise_grid_verified=%t fast_grid_verified=%t precise_plan=%s fast_plan=%s\n",
				sampleIndex+1, amounts[index].label,
				strings.ReplaceAll(strings.TrimSpace(topology.Input.Symbol), " ", "_"),
				nonNegative(captureStarted.Sub(scheduledAt)), startSkew, snapshotDuration,
				formatMeasurement(local, fmt.Sprint(topology.Output.Decimals), topology.Output.Symbol),
				formatMeasurement(fast, fmt.Sprint(topology.Output.Decimals), topology.Output.Symbol),
				formatDelta(local, fast, fmt.Sprint(topology.Output.Decimals), topology.Output.Symbol),
				local.computeDuration, local.fastComputeDuration,
				local.splitUsed, local.fastUsed, local.splitSelectedGrid,
				local.splitGridVerified, local.fastGridVerified,
				local.splitPlan, local.fastPlan,
			)
		}
	}

	for index, configured := range amounts {
		label := "amount_" + configured.label
		current := stats[index]
		current.preciseTimings.print("local_precise_"+label, "compute_with_included_fast_grid")
		current.fastTimings.print("local_fast_"+label, "compute")
		if err := printAmountSummary(
			"local_precise_"+label, current.preciseAmounts,
			fmt.Sprint(topology.Output.Decimals), topology.Output.Symbol,
		); err != nil {
			return err
		}
		if err := printAmountSummary(
			"local_fast_"+label, current.fastAmounts,
			fmt.Sprint(topology.Output.Decimals), topology.Output.Symbol,
		); err != nil {
			return err
		}
		if err := printAmountSummary(
			"delta(local_precise-local_fast)_"+label, current.deltas,
			fmt.Sprint(topology.Output.Decimals), topology.Output.Symbol,
		); err != nil {
			return err
		}
		fmt.Printf(
			"comparison_summary amount_in=%s attempts=%d compared=%d equal_outputs=%d precise_better=%d fast_better=%d precise_split_samples=%d fast_split_samples=%d precise_selected_fast_grid=%d\n",
			configured.label, samples, len(current.deltas), current.equalOutputs,
			current.preciseBetter, current.fastBetter, current.preciseSplits,
			current.fastSplits, current.selectedFastGrid,
		)
	}
	totalCompared := 0
	totalEqual := 0
	totalPreciseBetter := 0
	totalFastBetter := 0
	totalPreciseSplits := 0
	totalFastSplits := 0
	totalSelectedFast := 0
	for _, current := range stats {
		totalCompared += len(current.deltas)
		totalEqual += current.equalOutputs
		totalPreciseBetter += current.preciseBetter
		totalFastBetter += current.fastBetter
		totalPreciseSplits += current.preciseSplits
		totalFastSplits += current.fastSplits
		totalSelectedFast += current.selectedFastGrid
	}
	fmt.Printf(
		"comparison_summary scope=all_amounts amount_attempts=%d compared=%d equal_outputs=%d precise_better=%d fast_better=%d precise_split_samples=%d fast_split_samples=%d precise_selected_fast_grid=%d\n",
		samples*len(amounts), totalCompared, totalEqual, totalPreciseBetter,
		totalFastBetter, totalPreciseSplits, totalFastSplits, totalSelectedFast,
	)
	return nil
}

func dialNetwork(ctx context.Context, chainID int64, httpURL, wsURL string) (*evm.ReadOnlyNetwork, error) {
	delay := 500 * time.Millisecond
	var lastErr error
	for attempt := 1; attempt <= 5; attempt++ {
		network, err := evm.DialReadOnlyNetwork(ctx, "local-evm", "configured EVM chain", big.NewInt(chainID), httpURL, wsURL)
		if err == nil {
			return network, nil
		}
		lastErr = err
		if attempt == 5 {
			break
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		delay *= 2
	}
	return nil, fmt.Errorf("connect configured EVM chain after retries: %w", lastErr)
}

const (
	inputTokenID        market.TokenID = "input"
	intermediateTokenID market.TokenID = "intermediate"
	outputTokenID       market.TokenID = "output"
)

type tokenConfig struct {
	Address  string `json:"address"`
	Decimals uint8  `json:"decimals"`
	Symbol   string `json:"symbol"`
}

type poolConfig struct {
	Kind         string `json:"kind"`
	Address      string `json:"address"`
	PoolID       string `json:"pool_id"`
	Currency0    string `json:"currency0"`
	Currency1    string `json:"currency1"`
	Fee          uint32 `json:"fee"`
	TickSpacing  int32  `json:"tick_spacing"`
	MaxTickWords int    `json:"max_tick_words"`
}

type topologyConfig struct {
	ChainID      int64        `json:"chain_id"`
	PoolManager  string       `json:"pool_manager"`
	StateView    string       `json:"state_view"`
	Input        tokenConfig  `json:"input"`
	Intermediate tokenConfig  `json:"intermediate"`
	Output       tokenConfig  `json:"output"`
	Pools        []poolConfig `json:"pools"`
}

func loadTopology(path string) (topologyConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return topologyConfig{}, fmt.Errorf("open private pool topology %s: %w", path, err)
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	var config topologyConfig
	if err := decoder.Decode(&config); err != nil {
		return topologyConfig{}, fmt.Errorf("decode private pool topology: %w", err)
	}
	if config.ChainID <= 0 || len(config.Pools) == 0 {
		return topologyConfig{}, fmt.Errorf("pool topology requires a positive chain ID and pools")
	}
	for name, token := range map[string]tokenConfig{
		"input": config.Input, "intermediate": config.Intermediate, "output": config.Output,
	} {
		if !common.IsHexAddress(token.Address) || strings.TrimSpace(token.Symbol) == "" {
			return topologyConfig{}, fmt.Errorf("%s token requires an EVM address and symbol", name)
		}
		if token.Decimals > 36 {
			return topologyConfig{}, fmt.Errorf("%s token decimals exceed supported display precision", name)
		}
	}
	if equalAddress(config.Input.Address, config.Intermediate.Address) ||
		equalAddress(config.Input.Address, config.Output.Address) ||
		equalAddress(config.Intermediate.Address, config.Output.Address) {
		return topologyConfig{}, fmt.Errorf("topology tokens must be distinct")
	}
	return config, nil
}

func (c topologyConfig) validateAgainst(settings zeroxexperiment.Settings) error {
	if settings.ChainID != fmt.Sprint(c.ChainID) {
		return fmt.Errorf("provider chain ID differs from local topology")
	}
	if !equalAddress(settings.SellToken, c.Input.Address) || !equalAddress(settings.BuyToken, c.Output.Address) {
		return fmt.Errorf("provider token pair differs from local topology endpoints")
	}
	if settings.BuyDecimals != fmt.Sprint(c.Output.Decimals) {
		return fmt.Errorf("provider output decimals differ from local topology")
	}
	return nil
}

type watcher struct {
	index     int
	market    market.Market
	token0    market.TokenID
	token1    market.TokenID
	mirror    *marketstate.Mirror
	source    quoteport.Source
	feed      feedport.Feed
	validate  func(uniswapv3.Snapshot) error
	ready     chan struct{}
	readyOnce sync.Once
}

func buildWatchers(config topologyConfig, network evm.Network, amountIn *big.Int) ([]*watcher, error) {
	manager := common.HexToAddress(config.PoolManager)
	stateView := common.HexToAddress(config.StateView)
	addressToToken := map[common.Address]market.TokenID{
		common.HexToAddress(config.Input.Address):        inputTokenID,
		common.HexToAddress(config.Intermediate.Address): intermediateTokenID,
		common.HexToAddress(config.Output.Address):       outputTokenID,
	}
	result := make([]*watcher, 0, len(config.Pools))
	for index, configured := range config.Pools {
		currency0 := common.HexToAddress(configured.Currency0)
		currency1 := common.HexToAddress(configured.Currency1)
		token0, ok0 := addressToToken[currency0]
		token1, ok1 := addressToToken[currency1]
		if !ok0 || !ok1 || token0 == token1 {
			return nil, fmt.Errorf("pool %d currencies are outside configured token graph", index)
		}
		marketID := market.MarketID(fmt.Sprintf("pool-%d", index))
		sourceID := market.SourceID(fmt.Sprintf("watcher-%d", index))
		candidate := market.Market{
			ID: marketID, Pair: "local-pair", Chain: "local-chain", Path: market.PathID(fmt.Sprintf("pool-path-%d", index)),
			BaseToken: token0, QuoteToken: token1,
		}
		zeroForOne := inputDirection(token0, token1)
		probeAmount := new(big.Int).Set(amountIn)
		if token0 != inputTokenID && token1 != inputTokenID {
			probeAmount.Exp(big.NewInt(10), big.NewInt(int64(config.Intermediate.Decimals)), nil)
		}
		var venue evmlogs.Venue
		var source quoteport.Source
		var validate func(uniswapv3.Snapshot) error
		var err error
		switch configured.Kind {
		case "uniswap_v3":
			if !common.IsHexAddress(configured.Address) {
				return nil, fmt.Errorf("pool %d requires a V3 address", index)
			}
			adapter, adapterErr := uniswapv3.NewAdapter(uniswapv3.OnChainConfig{
				Pool: common.HexToAddress(configured.Address), MaxTickWords: configured.MaxTickWords,
				Probes: []uniswapv3.CoverageProbe{{ZeroForOne: zeroForOne, AmountIn: probeAmount}},
			})
			if adapterErr != nil {
				return nil, fmt.Errorf("pool %d: %w", index, adapterErr)
			}
			venue = adapter
			validate = func(state uniswapv3.Snapshot) error {
				info, ok := adapter.PoolInfo()
				if !ok || info.Token0 != currency0 || info.Token1 != currency1 || info.Fee != configured.Fee {
					return fmt.Errorf("V3 pool metadata differs from private topology")
				}
				if state.FeePips() != configured.Fee || state.TickSpacing() != configured.TickSpacing {
					return fmt.Errorf("V3 pool state differs from private topology")
				}
				return nil
			}
			source, err = uniswapv3.NewQuoter(sourceID+"/local", candidate, token0, token1)
		case "uniswap_v4":
			if manager == (common.Address{}) || stateView == (common.Address{}) || len(configured.PoolID) != 66 {
				return nil, fmt.Errorf("pool %d requires V4 deployment and pool ID", index)
			}
			adapter, adapterErr := uniswapv4.NewAdapter(uniswapv4.Config{
				PoolManager: manager, StateView: stateView, PoolID: common.HexToHash(configured.PoolID),
				Currency0: currency0, Currency1: currency1, Fee: configured.Fee,
				TickSpacing: configured.TickSpacing, MaxTickWords: configured.MaxTickWords,
				Probes: []uniswapv4.CoverageProbe{{ZeroForOne: zeroForOne, AmountIn: probeAmount}},
			})
			if adapterErr != nil {
				return nil, fmt.Errorf("pool %d: %w", index, adapterErr)
			}
			venue = adapter
			validate = func(state uniswapv3.Snapshot) error {
				if state.FeePips() != configured.Fee || state.TickSpacing() != configured.TickSpacing {
					return fmt.Errorf("V4 pool state differs from private topology")
				}
				return nil
			}
			source, err = uniswapv4.NewQuoter(sourceID+"/local", candidate, token0, token1)
		default:
			return nil, fmt.Errorf("pool %d has unsupported kind %q", index, configured.Kind)
		}
		if err != nil {
			return nil, fmt.Errorf("pool %d local quoter: %w", index, err)
		}
		mirror, err := marketstate.NewMirror(
			marketID, sourceID, uniswapv3.Reducer{},
			sourceorder.NewMonotonic(evmlogs.BlockPositionKind, false), time.Now,
		)
		if err != nil {
			return nil, err
		}
		feed, err := evmlogs.New(evmlogs.Config{
			Market: marketID, Source: sourceID, Network: network, Venue: venue,
			Clock: time.Now, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		if err != nil {
			return nil, err
		}
		result = append(result, &watcher{
			index: index, market: candidate, token0: token0, token1: token1,
			mirror: mirror, source: source, feed: feed, validate: validate, ready: make(chan struct{}),
		})
	}
	return result, nil
}

func inputDirection(token0, token1 market.TokenID) bool {
	switch {
	case token0 == inputTokenID:
		return true
	case token1 == inputTokenID:
		return false
	case token0 == intermediateTokenID:
		return true
	default:
		return false
	}
}

type watcherSink struct{ watcher *watcher }

func (s watcherSink) Publish(ctx context.Context, event market.MarketEvent) error {
	_, err := s.watcher.mirror.Apply(ctx, event)
	return err
}

func (s watcherSink) Reset(ctx context.Context, event market.MarketEvent) error {
	_, err := s.watcher.mirror.Reset(ctx, event)
	return err
}

func (s watcherSink) SetHealth(ctx context.Context, update feedport.HealthUpdate) error {
	if err := s.watcher.mirror.SetHealth(ctx, update); err != nil {
		return err
	}
	if update.Health == market.HealthHealthy {
		if _, ok := s.watcher.mirror.Current(); ok {
			s.watcher.readyOnce.Do(func() { close(s.watcher.ready) })
		}
	}
	return nil
}

func startAndAwaitWatchers(parent context.Context, watchers []*watcher, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	errs := make(chan error, len(watchers))
	for _, current := range watchers {
		go func(current *watcher) {
			if err := current.feed.Run(parent, watcherSink{watcher: current}); err != nil && !errors.Is(err, context.Canceled) {
				select {
				case errs <- fmt.Errorf("watcher %d: %w", current.index, err):
				default:
				}
			}
		}(current)
	}
	for _, current := range watchers {
		select {
		case <-current.ready:
		case err := <-errs:
			return err
		case <-ctx.Done():
			return fmt.Errorf("watchers did not all initialize: %w", ctx.Err())
		}
	}
	for _, current := range watchers {
		snapshot, ok := current.mirror.Current()
		if !ok || snapshot.Metadata().Health != market.HealthHealthy {
			return fmt.Errorf("watcher %d is not healthy after bootstrap", current.index)
		}
		state, ok := snapshot.Data().(uniswapv3.Snapshot)
		if !ok || state.FeePips() == 0 {
			return fmt.Errorf("watcher %d published incompatible concentrated-liquidity state", current.index)
		}
		if err := current.validate(state); err != nil {
			return fmt.Errorf("watcher %d: %w", current.index, err)
		}
	}
	return nil
}

type localCandidate struct {
	marketID market.MarketID
	source   *route.Source
	watches  []*watcher
}

func buildCandidates(watchers []*watcher) ([]localCandidate, error) {
	pools := make([]localquoteexperiment.Pool, len(watchers))
	for index, current := range watchers {
		pools[index] = localquoteexperiment.Pool{Token0: string(current.token0), Token1: string(current.token1)}
	}
	definitions, err := localquoteexperiment.BuildRoutes(
		pools, string(inputTokenID), string(intermediateTokenID), string(outputTokenID),
	)
	if err != nil {
		return nil, err
	}
	paths := make([][]*watcher, len(definitions))
	for index, definition := range definitions {
		for _, poolIndex := range definition.PoolIndexes {
			paths[index] = append(paths[index], watchers[poolIndex])
		}
	}
	result := make([]localCandidate, 0, len(paths))
	for index, path := range paths {
		routeID := market.MarketID(fmt.Sprintf("route-%d", index))
		candidate := market.Market{
			ID: routeID, Pair: "local-pair", Chain: "local-chain",
			Path:      market.PathID(fmt.Sprintf("route-path-%d", index)),
			BaseToken: inputTokenID, QuoteToken: outputTokenID,
		}
		hops := make([]route.Hop, len(path))
		for hopIndex, current := range path {
			in, out := inputTokenID, outputTokenID
			if len(path) == 2 && hopIndex == 0 {
				out = intermediateTokenID
			}
			if len(path) == 2 && hopIndex == 1 {
				in = intermediateTokenID
			}
			hops[hopIndex] = route.Hop{Market: current.market.ID, In: in, Out: out, Source: current.source}
		}
		source, err := route.NewUncached(market.SourceID(fmt.Sprintf("local-route-%d", index)), candidate, hops)
		if err != nil {
			return nil, err
		}
		result = append(result, localCandidate{marketID: routeID, source: source, watches: path})
	}
	return result, nil
}

func expectedRouteCount(watchers []*watcher) int {
	pools := make([]localquoteexperiment.Pool, len(watchers))
	for index, current := range watchers {
		pools[index] = localquoteexperiment.Pool{Token0: string(current.token0), Token1: string(current.token1)}
	}
	routes, _ := localquoteexperiment.BuildRoutes(
		pools, string(inputTokenID), string(intermediateTokenID), string(outputTokenID),
	)
	return len(routes)
}

func healthyCount(watchers []*watcher) int {
	count := 0
	for _, current := range watchers {
		if current.mirror.Health() == market.HealthHealthy {
			count++
		}
	}
	return count
}

type measurement struct {
	amount                *big.Int
	fastAmount            *big.Int
	singleAmount          *big.Int
	err                   error
	fastErr               error
	singleErr             error
	startedAt             time.Time
	duration              time.Duration
	httpDuration          time.Duration
	snapshotDuration      time.Duration
	computeDuration       time.Duration
	fastComputeDuration   time.Duration
	singleComputeDuration time.Duration
	batchDuration         time.Duration
	candidates            int
	candidateErrors       int
	selectedHops          int
	selectedWasCached     bool
	splitPlan             string
	splitUsed             int
	splitGloballyVerified bool
	splitGridVerified     bool
	splitGridDivisions    int
	splitInputResolution  *big.Int
	splitIncludedFast     bool
	splitSelectedGrid     string
	splitMetrics          sizing.SplitMetrics
	fastPlan              string
	fastUsed              int
	fastGloballyVerified  bool
	fastGridVerified      bool
	fastGridDivisions     int
	fastInputResolution   *big.Int
	fastMetrics           sizing.SplitMetrics
}

func quoteTriplet(
	ctx context.Context,
	candidates []localCandidate,
	watchers []*watcher,
	topology topologyConfig,
	amountIn market.TokenAmount,
	localIterations int,
	referenceGridDivisions int,
	fastGridDivisions int,
	zeroSource *zerox.Source,
	zeroRequest zerox.Request,
	kyberSource *kyberswap.Source,
	kyberRequest kyberswap.RouteRequest,
) (measurement, measurement, measurement) {
	var localResult, zeroResult, kyberResult measurement
	var wait sync.WaitGroup
	start := make(chan struct{})
	wait.Add(3)
	go func() {
		defer wait.Done()
		<-start
		localResult.startedAt = time.Now()
		localResult = measureLocal(
			ctx, candidates, watchers, topology, amountIn, localIterations,
			referenceGridDivisions, fastGridDivisions, localResult.startedAt,
		)
	}()
	go func() {
		defer wait.Done()
		<-start
		zeroResult.startedAt = time.Now()
		result, err := zeroSource.Price(ctx, zeroRequest)
		zeroResult.duration = time.Since(zeroResult.startedAt)
		zeroResult.httpDuration = result.HTTPDuration
		zeroResult.err = err
		if err == nil {
			zeroResult.amount, zeroResult.err = parseAmount(result.BuyAmount)
		}
	}()
	go func() {
		defer wait.Done()
		<-start
		kyberResult.startedAt = time.Now()
		result, err := kyberSource.Route(ctx, kyberRequest)
		kyberResult.duration = time.Since(kyberResult.startedAt)
		kyberResult.httpDuration = result.HTTPDuration
		kyberResult.err = err
		if err == nil {
			kyberResult.amount, kyberResult.err = parseAmount(result.AmountOut)
		}
	}()
	close(start)
	wait.Wait()
	return localResult, zeroResult, kyberResult
}

type preparedCandidate struct {
	candidate localCandidate
	snapshot  market.MarketSnapshot
}

func measureLocal(
	ctx context.Context,
	candidates []localCandidate,
	watchers []*watcher,
	topology topologyConfig,
	amountIn market.TokenAmount,
	iterations int,
	referenceGridDivisions int,
	fastGridDivisions int,
	started time.Time,
) measurement {
	result := measurement{startedAt: started, candidates: len(candidates)}
	snapshotStarted := time.Now()
	snapshots, err := captureLocalSnapshots(watchers)
	if err != nil {
		result.err = err
		result.fastErr = err
		result.singleErr = err
		result.snapshotDuration = time.Since(snapshotStarted)
		result.duration = result.snapshotDuration
		return result
	}
	preparedCandidates, candidateErrors := prepareCandidates(candidates, snapshots)
	snapshotDuration := time.Since(snapshotStarted)
	result = measureSplitProfiles(
		ctx, watchers, snapshots, topology, amountIn, iterations,
		referenceGridDivisions, fastGridDivisions, started,
	)
	result.candidates = len(candidates)
	result.candidateErrors = candidateErrors
	result.snapshotDuration = snapshotDuration

	singleStarted := time.Now()
	for iteration := 0; iteration < iterations; iteration++ {
		best, selectedHops, selectedCached, failures := bestSingleRoute(
			ctx, preparedCandidates, amountIn, started.UTC(),
		)
		if failures > result.candidateErrors {
			result.candidateErrors = failures
		}
		if best == nil {
			result.singleErr = fmt.Errorf("all %d local route candidates failed", len(candidates))
			break
		}
		if iteration == iterations-1 {
			result.singleAmount = best
			result.selectedHops = selectedHops
			result.selectedWasCached = selectedCached
		}
	}
	singleBatch := time.Since(singleStarted)
	result.singleComputeDuration = singleBatch / time.Duration(iterations)
	result.duration = result.snapshotDuration + result.computeDuration
	result.batchDuration += singleBatch
	return result
}

func measureSplitProfiles(
	ctx context.Context,
	watchers []*watcher,
	snapshots localSnapshotSet,
	topology topologyConfig,
	amountIn market.TokenAmount,
	iterations int,
	referenceGridDivisions int,
	fastGridDivisions int,
	started time.Time,
) measurement {
	result := measurement{startedAt: started}
	// Run the latency-targeted profile first. The verified reference allocates
	// substantially more temporary state; running fast after it would measure
	// garbage-collection pressure from the reference rather than fast latency.
	fastStarted := time.Now()
	var fast sizing.TwoStageSplitResult
	fastAttempts := 0
	var fastErr error
	for iteration := 0; iteration < iterations; iteration++ {
		fastAttempts++
		fast, fastErr = optimizeLocalGridSplit(
			ctx, watchers, snapshots, amountIn.Units(), started.UTC(), fastGridDivisions,
		)
		if fastErr != nil {
			break
		}
	}
	fastBatch := time.Since(fastStarted)
	result.fastComputeDuration = fastBatch / time.Duration(fastAttempts)
	if fastErr != nil {
		result.fastErr = fastErr
	} else {
		result.fastAmount = fast.TotalOutput
		result.fastMetrics = fast.Metrics
		result.fastGloballyVerified = fast.GloballyVerified
		result.fastGridVerified = fast.GridVerified
		result.fastGridDivisions = fast.GridDivisions
		result.fastInputResolution = fast.InputResolution
		result.fastUsed = usedAllocations(fast.Direct, fast.FirstStage, fast.SecondStage)
		result.fastPlan, result.fastErr = formatLocalSplitPlan(fast, topology)
	}

	splitStarted := time.Now()
	var split sizing.TwoStageSplitResult
	splitAttempts := 0
	var splitErr error
	for iteration := 0; iteration < iterations; iteration++ {
		splitAttempts++
		split, splitErr = optimizeLocalReferenceGrid(
			ctx, watchers, snapshots, amountIn.Units(), started.UTC(), referenceGridDivisions,
		)
		if splitErr != nil {
			break
		}
	}
	splitBatch := time.Since(splitStarted)
	result.computeDuration = (splitBatch + fastBatch) / time.Duration(splitAttempts)
	switch {
	case splitErr != nil:
		result.err = splitErr
	case fastErr != nil:
		result.err = fmt.Errorf("evaluate included fast grid: %w", fastErr)
	case !split.GridVerified:
		result.err = fmt.Errorf("reference grid did not produce exhaustive grid verification")
	case !fast.GridVerified:
		result.err = fmt.Errorf("included fast grid did not produce exhaustive grid verification")
	default:
		selected := split
		result.splitSelectedGrid = "reference"
		if fast.TotalOutput.Cmp(split.TotalOutput) > 0 {
			selected = fast
			result.splitSelectedGrid = "fast"
		}
		result.amount = selected.TotalOutput
		result.splitMetrics = addSplitMetrics(split.Metrics, fast.Metrics)
		result.splitGloballyVerified = split.GloballyVerified && fast.GloballyVerified
		result.splitGridVerified = split.GridVerified && fast.GridVerified
		result.splitGridDivisions = split.GridDivisions
		result.splitInputResolution = split.InputResolution
		result.splitIncludedFast = true
		result.splitUsed = usedAllocations(selected.Direct, selected.FirstStage, selected.SecondStage)
		result.splitPlan, result.err = formatLocalSplitPlan(selected, topology)
		if result.err == nil && result.amount.Cmp(fast.TotalOutput) < 0 {
			result.err = fmt.Errorf("verified reference output is below included fast-grid output")
		}
	}
	result.duration = result.computeDuration
	result.batchDuration = splitBatch + fastBatch
	return result
}

func bestSingleRoute(
	ctx context.Context,
	preparedCandidates []preparedCandidate,
	amountIn market.TokenAmount,
	quotedAt time.Time,
) (*big.Int, int, bool, int) {
	var best *big.Int
	var selectedHops int
	var selectedCached bool
	var candidateErrors int
	for _, prepared := range preparedCandidates {
		quoted, err := prepared.candidate.source.Quote(ctx, quoteport.Input{
			Snapshot: prepared.snapshot, TokenIn: inputTokenID, TokenOut: outputTokenID,
			AmountIn: amountIn, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: quotedAt,
		})
		if err != nil {
			candidateErrors++
			continue
		}
		amount := quoted.AmountOut.Units()
		if best == nil || amount.Cmp(best) > 0 {
			best = amount
			timing := prepared.candidate.source.LastTiming()
			selectedHops = len(timing.Hops)
			selectedCached = timing.Cached
		}
	}
	return best, selectedHops, selectedCached, candidateErrors
}

func prepareCandidates(candidates []localCandidate, snapshots localSnapshotSet) ([]preparedCandidate, int) {
	prepared := make([]preparedCandidate, 0, len(candidates))
	var candidateErrors int
	for _, candidate := range candidates {
		children := make([]market.MarketSnapshot, len(candidate.watches))
		healthy := true
		for index, current := range candidate.watches {
			snapshot, ok := snapshots[current]
			if !ok || snapshot.Metadata().Health != market.HealthHealthy {
				healthy = false
				break
			}
			children[index] = snapshot
		}
		if !healthy {
			candidateErrors++
			continue
		}
		bundle, err := market.NewSnapshotBundle(candidate.marketID, children)
		if err != nil {
			candidateErrors++
			continue
		}
		now := time.Now().UTC()
		snapshot, err := market.NewMarketSnapshot(market.SnapshotMetadata{
			Market: candidate.marketID, Source: candidate.source.ID(), Version: bundle.Version(),
			Finality: market.FinalityPreconfirmed, ReceivedAt: now, AppliedAt: now,
			Health: market.HealthHealthy, HealthChangedAt: now, StateHash: bundle.Hash(),
		}, bundle)
		if err != nil {
			candidateErrors++
			continue
		}
		prepared = append(prepared, preparedCandidate{candidate: candidate, snapshot: snapshot})
	}
	return prepared, candidateErrors
}

func parseAmount(raw string) (*big.Int, error) {
	value, ok := new(big.Int).SetString(raw, 10)
	if !ok || value.Sign() < 0 {
		return nil, fmt.Errorf("provider returned an invalid output amount")
	}
	return value, nil
}

func tenthTokenReferenceGrid(totalInput *big.Int, decimals uint8) (int, *big.Int, error) {
	if totalInput == nil || totalInput.Sign() <= 0 || decimals == 0 {
		return 0, nil, fmt.Errorf("the verified reference requires a positive amount and at least one input-token decimal")
	}
	step := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals-1)), nil)
	divisions, remainder := new(big.Int).QuoRem(totalInput, step, new(big.Int))
	if remainder.Sign() != 0 {
		return 0, nil, fmt.Errorf("input amount must be divisible by the 0.1-token verified reference step")
	}
	if !divisions.IsInt64() || divisions.Int64() < 2 || divisions.Int64() > 1024 {
		return 0, nil, fmt.Errorf(
			"0.1-token verified reference requires between 2 and 1024 divisions; got %s",
			divisions,
		)
	}
	return int(divisions.Int64()), step, nil
}

func formatBaseUnitsOrRaw(value *big.Int, decimals uint8) string {
	formatted, err := okxexperiment.FormatBaseUnits(value.String(), fmt.Sprint(decimals))
	if err != nil {
		return value.String()
	}
	return formatted
}

func addSplitMetrics(left, right sizing.SplitMetrics) sizing.SplitMetrics {
	return sizing.SplitMetrics{
		CurveEvaluations:     left.CurveEvaluations + right.CurveEvaluations,
		CurveCacheHits:       left.CurveCacheHits + right.CurveCacheHits,
		ObjectiveEvaluations: left.ObjectiveEvaluations + right.ObjectiveEvaluations,
		ObjectiveCacheHits:   left.ObjectiveCacheHits + right.ObjectiveCacheHits,
		SecondStageSolves:    left.SecondStageSolves + right.SecondStageSolves,
		CoordinateSweeps:     left.CoordinateSweeps + right.CoordinateSweeps,
	}
}

func appendDelta(first, second measurement, appendValue func(*big.Int)) {
	if first.err == nil && second.err == nil && first.amount != nil && second.amount != nil {
		appendValue(new(big.Int).Sub(new(big.Int).Set(first.amount), second.amount))
	}
}

func printSample(index int, decimals, symbol string, local, zero, kyber measurement) {
	startSkew := maximumStartSkew(local.startedAt, zero.startedAt, kyber.startedAt)
	fast := measurement{amount: local.fastAmount, err: local.fastErr}
	single := measurement{amount: local.singleAmount, err: local.singleErr}
	fmt.Printf(
		"sample=%d start_skew=%s local_precise_output=%s local_fast_output=%s delta(local_precise-local_fast)=%s local_single_output=%s delta(local_precise-local_single)=%s local_precise_total=%s local_precise_compute=%s local_fast_total=%s local_fast_compute=%s local_single_compute=%s local_snapshot=%s local_compute_batch=%s local_candidates=%d local_candidate_errors=%d local_single_selected_hops=%d local_single_cached=%t local_precise_used_pools=%d local_precise_grid_verified=%t local_precise_globally_verified=%t local_precise_grid_divisions=%d local_precise_input_resolution=%s local_precise_included_fast=%t local_precise_selected_grid=%s local_precise_curve_evaluations=%d local_precise_curve_cache_hits=%d local_precise_objective_evaluations=%d local_precise_objective_cache_hits=%d local_precise_second_stage_solves=%d local_precise_coordinate_sweeps=%d local_precise_plan=%s local_fast_used_pools=%d local_fast_grid_verified=%t local_fast_globally_verified=%t local_fast_grid_divisions=%d local_fast_input_resolution=%s local_fast_curve_evaluations=%d local_fast_curve_cache_hits=%d local_fast_objective_evaluations=%d local_fast_second_stage_solves=%d local_fast_plan=%s zerox_output=%s zerox_total=%s zerox_http=%s zerox_local=%s kyberswap_output=%s kyberswap_total=%s kyberswap_http=%s kyberswap_local=%s delta(local_precise-0x)=%s delta(local_precise-kyberswap)=%s delta(kyberswap-0x)=%s\n",
		index, startSkew,
		formatMeasurement(local, decimals, symbol), formatMeasurement(fast, decimals, symbol),
		formatDelta(local, fast, decimals, symbol),
		formatMeasurement(single, decimals, symbol),
		formatDelta(local, single, decimals, symbol),
		local.duration, local.computeDuration,
		local.snapshotDuration+local.fastComputeDuration, local.fastComputeDuration,
		local.singleComputeDuration, local.snapshotDuration, local.batchDuration,
		local.candidates, local.candidateErrors, local.selectedHops, local.selectedWasCached,
		local.splitUsed, local.splitGridVerified, local.splitGloballyVerified,
		local.splitGridDivisions, formatOptionalInt(local.splitInputResolution),
		local.splitIncludedFast, local.splitSelectedGrid,
		local.splitMetrics.CurveEvaluations, local.splitMetrics.CurveCacheHits,
		local.splitMetrics.ObjectiveEvaluations, local.splitMetrics.ObjectiveCacheHits,
		local.splitMetrics.SecondStageSolves, local.splitMetrics.CoordinateSweeps,
		local.splitPlan,
		local.fastUsed, local.fastGridVerified, local.fastGloballyVerified,
		local.fastGridDivisions, formatOptionalInt(local.fastInputResolution),
		local.fastMetrics.CurveEvaluations, local.fastMetrics.CurveCacheHits,
		local.fastMetrics.ObjectiveEvaluations, local.fastMetrics.SecondStageSolves,
		local.fastPlan,
		formatMeasurement(zero, decimals, symbol), zero.duration, zero.httpDuration, nonNegative(zero.duration-zero.httpDuration),
		formatMeasurement(kyber, decimals, symbol), kyber.duration, kyber.httpDuration, nonNegative(kyber.duration-kyber.httpDuration),
		formatDelta(local, zero, decimals, symbol),
		formatDelta(local, kyber, decimals, symbol),
		formatDelta(kyber, zero, decimals, symbol),
	)
}

func formatOptionalInt(value *big.Int) string {
	if value == nil {
		return "n/a"
	}
	return value.String()
}

func formatMeasurement(value measurement, decimals, symbol string) string {
	if value.err != nil {
		return "error=" + strings.ReplaceAll(value.err.Error(), " ", "_")
	}
	formatted, err := okxexperiment.FormatBaseUnits(value.amount.String(), decimals)
	if err != nil {
		return "format_error"
	}
	return formatted + unitSuffix(symbol)
}

func formatDelta(first, second measurement, decimals, symbol string) string {
	if first.err != nil || second.err != nil || first.amount == nil || second.amount == nil {
		return "n/a"
	}
	delta := new(big.Int).Sub(new(big.Int).Set(first.amount), second.amount)
	formatted, err := okxexperiment.FormatBaseUnits(new(big.Int).Abs(delta).String(), decimals)
	if err != nil {
		return "format_error"
	}
	if delta.Sign() < 0 {
		formatted = "-" + formatted
	} else if delta.Sign() > 0 {
		formatted = "+" + formatted
	}
	return formatted + unitSuffix(symbol)
}

func maximumStartSkew(values ...time.Time) time.Duration {
	minimum, maximum := values[0], values[0]
	for _, value := range values[1:] {
		if value.Before(minimum) {
			minimum = value
		}
		if value.After(maximum) {
			maximum = value
		}
	}
	return maximum.Sub(minimum)
}

type timingGroup struct {
	attempts  int
	successes int
	errors    int
	all       []time.Duration
	success   []time.Duration
}

func newTimingGroup() *timingGroup { return &timingGroup{} }

func (g *timingGroup) add(duration time.Duration, err error) {
	g.attempts++
	g.all = append(g.all, duration)
	if err != nil {
		g.errors++
		return
	}
	g.successes++
	g.success = append(g.success, duration)
}

func (g *timingGroup) print(provider, stage string) {
	all := durationStatsOf(g.all)
	success := durationStatsOf(g.success)
	fmt.Printf(
		"summary provider=%s stage=%s attempts=%d successes=%d errors=%d all_min=%s all_mean=%s all_p50=%s all_p95=%s all_p99=%s all_max=%s success_min=%s success_mean=%s success_p50=%s success_p95=%s success_p99=%s success_max=%s\n",
		provider, stage, g.attempts, g.successes, g.errors,
		all.min, all.mean, all.p50, all.p95, all.p99, all.max,
		success.min, success.mean, success.p50, success.p95, success.p99, success.max,
	)
}

type durationStats struct{ min, mean, p50, p95, p99, max time.Duration }

func durationStatsOf(values []time.Duration) durationStats {
	if len(values) == 0 {
		return durationStats{}
	}
	ordered := append([]time.Duration(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	var total time.Duration
	for _, value := range ordered {
		total += value
	}
	return durationStats{
		min: ordered[0], mean: total / time.Duration(len(ordered)),
		p50: percentileDuration(ordered, 0.50), p95: percentileDuration(ordered, 0.95),
		p99: percentileDuration(ordered, 0.99), max: ordered[len(ordered)-1],
	}
}

func percentileDuration(ordered []time.Duration, percentile float64) time.Duration {
	if len(ordered) == 0 {
		return 0
	}
	index := int(math.Ceil(percentile*float64(len(ordered)))) - 1
	if index < 0 {
		index = 0
	}
	return ordered[index]
}

func printAmountSummary(label string, values []*big.Int, decimals, symbol string) error {
	if len(values) == 0 {
		fmt.Printf("amount_summary provider=%s samples=0\n", label)
		return nil
	}
	ordered := make([]*big.Int, len(values))
	total := new(big.Int)
	for index, value := range values {
		ordered[index] = new(big.Int).Set(value)
		total.Add(total, value)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Cmp(ordered[j]) < 0 })
	mean := new(big.Rat).SetFrac(total, big.NewInt(int64(len(ordered))))
	minimum, err := formatRatBaseUnits(new(big.Rat).SetInt(ordered[0]), decimals)
	if err != nil {
		return err
	}
	meanText, err := formatRatBaseUnits(mean, decimals)
	if err != nil {
		return err
	}
	p50, err := formatRatBaseUnits(new(big.Rat).SetInt(ordered[percentileIndex(len(ordered), 0.50)]), decimals)
	if err != nil {
		return err
	}
	maximum, err := formatRatBaseUnits(new(big.Rat).SetInt(ordered[len(ordered)-1]), decimals)
	if err != nil {
		return err
	}
	fmt.Printf("amount_summary provider=%s samples=%d min=%s mean=%s p50=%s max=%s unit=%s\n", label, len(ordered), minimum, meanText, p50, maximum, symbol)
	return nil
}

func formatRatBaseUnits(value *big.Rat, decimals string) (string, error) {
	scale, ok := new(big.Int).SetString(decimals, 10)
	if !ok || !scale.IsInt64() || scale.Sign() < 0 {
		return "", fmt.Errorf("invalid output decimals")
	}
	denominator := new(big.Int).Exp(big.NewInt(10), scale, nil)
	scaled := new(big.Rat).Quo(value, new(big.Rat).SetInt(denominator))
	text := scaled.FloatString(int(scale.Int64()) + 6)
	text = strings.TrimRight(strings.TrimRight(text, "0"), ".")
	if text == "" || text == "-0" {
		return "0", nil
	}
	return text, nil
}

func percentileIndex(length int, percentile float64) int {
	index := int(math.Ceil(percentile*float64(length))) - 1
	if index < 0 {
		return 0
	}
	return index
}

func waitUntil(ctx context.Context, target time.Time) error {
	delay := time.Until(target)
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nonNegative(value time.Duration) time.Duration {
	if value < 0 {
		return 0
	}
	return value
}

func unitSuffix(symbol string) string {
	if strings.TrimSpace(symbol) == "" {
		return ""
	}
	return "_" + strings.TrimSpace(symbol)
}

func equalAddress(left, right string) bool {
	return common.IsHexAddress(left) && common.IsHexAddress(right) &&
		common.HexToAddress(left) == common.HexToAddress(right)
}
