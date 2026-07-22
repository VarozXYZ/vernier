// Command research runs the deterministic synthetic Research demonstration.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/adapters/chain/solana"
	sqlitepersistence "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	persistence "github.com/VarozXYZ/vernier/ports/persistence"
	"github.com/VarozXYZ/vernier/runtime/configuration"
	"github.com/VarozXYZ/vernier/runtime/livecompare"
	"github.com/VarozXYZ/vernier/runtime/observability"
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
	if len(args) > 0 && args[0] == "windows" {
		return runWindows(ctx, args[1:], stdout, stderr)
	}
	return runSynthetic(ctx, args, stdout, stderr)
}

func runCompareLive(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("research compare-live", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "examples/setups/virtual/vernier.yaml", "path to YAML configuration manifest")
	envPath := flags.String("env-file", ".env", "path to local environment file")
	format := flags.String("format", "text", "output format: text or json (jsonl in stream mode)")
	stream := flags.Bool("stream", true, "continuously evaluate both pools from WebSocket log feeds (use --stream=false for one snapshot)")
	updates := flags.Int("updates", 0, "reports to emit in stream mode; zero runs until canceled")
	logLevel := flags.String("log-level", "info", "diagnostic log level: debug, info, warn, or error")
	calculations := flags.String("calculations", "summary", "calculation output: summary or full")
	opportunityStorePath := flags.String("opportunity-store", ".vernier/opportunities.sqlite", "SQLite opportunity-window store; empty disables persistence")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	validFormat := *format == "text" || (!*stream && *format == "json") || (*stream && *format == "jsonl")
	if flags.NArg() != 0 || !validFormat || *updates < 0 || (*calculations != string(livecompare.CalculationSummary) && *calculations != string(livecompare.CalculationFull)) {
		fmt.Fprintln(stderr, "research compare-live: invalid arguments")
		return 2
	}
	logger, err := observability.NewLogger(stderr, *logLevel)
	if err != nil {
		fmt.Fprintf(stderr, "research compare-live: %v\n", err)
		return 2
	}
	logger.Info("loading live configuration", "path", *configPath, "mode", map[bool]string{true: "stream", false: "point_in_time"}[*stream])
	if err := loadEnvFile(*envPath, os.LookupEnv, os.Setenv); err != nil {
		fmt.Fprintln(stderr, "research compare-live: cannot load local environment")
		return 2
	}
	config, err := configuration.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "research compare-live: %v\n", err)
		return 2
	}
	logger.Info("configuration loaded", "chains", len(config.Chains), "markets", len(config.Markets), "config_hash", config.Hash)
	endpoints, err := config.ResolveEndpoints(os.LookupEnv)
	if err != nil {
		fmt.Fprintf(stderr, "research compare-live: %v\n", err)
		return 2
	}
	networks := make(livecompare.Networks, len(config.Chains))
	solanaNetworks := make(livecompare.SolanaNetworks)
	for id, profile := range config.Chains {
		logger.Info("dialing network", "chain", id, "label", profile.Label)
		if profile.Kind == "solana" {
			network, dialErr := solana.DialReadOnlyNetwork(ctx, profile.ID, profile.Label, endpoints[id+".http"], endpoints[id+".websocket"])
			if dialErr != nil {
				fmt.Fprintf(stderr, "research compare-live: %v\n", dialErr)
				return 1
			}
			solanaNetworks[id] = network
			logger.Info("network ready", "chain", id)
			continue
		}
		wsEndpoint := endpoints[id+".websocket"]
		if wsEndpoint == "" {
			wsEndpoint = endpoints[id]
		}
		network, dialErr := evm.DialReadOnlyNetwork(ctx, profile.ID, profile.Label, profile.ChainID, endpoints[id], wsEndpoint)
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
		logger.Info("network ready", "chain", id)
	}
	defer func() {
		for _, network := range networks {
			if closer, ok := network.(interface{ Close() }); ok {
				closer.Close()
			}
		}
		for _, network := range solanaNetworks {
			network.Close()
		}
	}()
	runner, err := livecompare.New(config, networks, livecompare.Options{LookupEnv: os.LookupEnv, Logger: logger, SolanaNetworks: solanaNetworks})
	if err != nil {
		fmt.Fprintf(stderr, "research compare-live: invalid composition: %v\n", err)
		return 2
	}
	if *stream {
		var outputMu sync.Mutex
		var opportunityStore persistence.OpportunityStore
		if strings.TrimSpace(*opportunityStorePath) != "" {
			opportunityStore, err = sqlitepersistence.Open(*opportunityStorePath)
			if err != nil {
				fmt.Fprintf(stderr, "research compare-live: open opportunity store: %v\n", err)
				return 1
			}
			defer opportunityStore.Close()
			logger.Info("opportunity store opened", "path", *opportunityStorePath)
		}
		err = runner.RunStream(ctx, livecompare.StreamOptions{
			Updates: *updates, OpportunityStore: opportunityStore,
			OnReport: func(report livecompare.Report) error {
				outputMu.Lock()
				defer outputMu.Unlock()
				options := livecompare.OutputOptions{Calculations: livecompare.CalculationDetail(*calculations)}
				if *format == "jsonl" {
					return livecompare.WriteJSONLineWithOptions(stdout, report, options)
				}
				return livecompare.WriteTextWithOptions(stdout, report, options)
			},
			OnReference: func(report livecompare.ReferenceReport) error {
				outputMu.Lock()
				defer outputMu.Unlock()
				if *format == "jsonl" {
					return livecompare.WriteReferenceJSONLine(stdout, report)
				}
				return livecompare.WriteReferenceText(stdout, report)
			},
		})
	} else {
		var report livecompare.Report
		report, err = runner.Run(ctx)
		if err == nil {
			options := livecompare.OutputOptions{Calculations: livecompare.CalculationDetail(*calculations)}
			if *format == "json" {
				err = livecompare.WriteJSONWithOptions(stdout, report, options)
			} else {
				err = livecompare.WriteTextWithOptions(stdout, report, options)
			}
		}
	}
	if err != nil {
		fmt.Fprintf(stderr, "research compare-live: run failed: %v\n", err)
		return 1
	}
	return 0
}

func runWindows(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("research windows", flag.ContinueOnError)
	flags.SetOutput(stderr)
	storePath := flags.String("store", ".vernier/opportunities.sqlite", "SQLite opportunity-window store")
	format := flags.String("format", "text", "output format: text or json")
	runID := flags.String("run", "", "filter by research run ID")
	strategyID := flags.String("strategy", "", "filter by strategy ID")
	status := flags.String("status", "", "filter by status: open, closed, or failed")
	limit := flags.Int("limit", 100, "maximum number of windows")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 || (*format != "text" && *format != "json") || *limit <= 0 {
		fmt.Fprintln(stderr, "research windows: invalid arguments")
		return 2
	}
	if *status != "" && *status != string(arbitrage.WindowStatusOpen) && *status != string(arbitrage.WindowStatusClosed) && *status != string(arbitrage.WindowStatusFailed) {
		fmt.Fprintf(stderr, "research windows: invalid status %q\n", *status)
		return 2
	}
	if _, err := os.Stat(*storePath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if *format == "json" {
				if err := writeWindowsJSON(stdout, []arbitrage.WindowRecord{}); err != nil {
					fmt.Fprintf(stderr, "research windows: write report: %v\n", err)
					return 1
				}
				return 0
			}
			_, writeErr := fmt.Fprintln(stdout, "Opportunity windows\nwindows: 0")
			if writeErr != nil {
				fmt.Fprintf(stderr, "research windows: write report: %v\n", writeErr)
				return 1
			}
			return 0
		}
		fmt.Fprintf(stderr, "research windows: stat store: %v\n", err)
		return 1
	}
	store, err := sqlitepersistence.Open(*storePath)
	if err != nil {
		fmt.Fprintf(stderr, "research windows: open store: %v\n", err)
		return 1
	}
	defer store.Close()
	records, err := store.ListWindows(ctx, arbitrage.WindowQuery{
		Run: arbitrage.ResearchRunID(*runID), Strategy: arbitrage.StrategyID(*strategyID),
		Status: arbitrage.WindowStatus(*status), Limit: *limit,
	})
	if err != nil {
		fmt.Fprintf(stderr, "research windows: list store: %v\n", err)
		return 1
	}
	if *format == "json" {
		if err := writeWindowsJSON(stdout, records); err != nil {
			fmt.Fprintf(stderr, "research windows: write report: %v\n", err)
			return 1
		}
	} else if err := writeWindowsText(stdout, records); err != nil {
		fmt.Fprintf(stderr, "research windows: write report: %v\n", err)
		return 1
	}
	return 0
}

type windowOutput struct {
	ID                string                    `json:"id"`
	Run               string                    `json:"run"`
	Strategy          string                    `json:"strategy"`
	ConfigHash        string                    `json:"config_hash"`
	BuyMarket         string                    `json:"buy_market"`
	SellMarket        string                    `json:"sell_market"`
	Status            string                    `json:"status"`
	Classification    string                    `json:"classification"`
	OpenedAt          string                    `json:"opened_at"`
	FirstProfitableAt string                    `json:"first_profitable_at"`
	LastProfitableAt  string                    `json:"last_profitable_at"`
	ClosedAt          string                    `json:"closed_at,omitempty"`
	Duration          string                    `json:"duration"`
	CloseReason       string                    `json:"close_reason,omitempty"`
	Degraded          bool                      `json:"degraded"`
	Trigger           *windowTriggerOutput      `json:"trigger,omitempty"`
	Best              windowCandidateOutput     `json:"best"`
	Observations      []windowObservationOutput `json:"observations"`
}

type windowTriggerOutput struct {
	Market         string `json:"market"`
	Source         string `json:"source"`
	PositionKind   string `json:"position_kind,omitempty"`
	PositionValue  uint64 `json:"position_value,omitempty"`
	ReferenceKind  string `json:"reference_kind,omitempty"`
	ReferenceValue string `json:"reference_value,omitempty"`
	At             string `json:"at"`
}

type windowCandidateOutput struct {
	Size      string `json:"size"`
	SizeAsset string `json:"size_asset"`
	GrossPnL  string `json:"gross_pnl"`
	NetPnL    string `json:"net_pnl"`
	Cost      string `json:"cost"`
	Asset     string `json:"asset"`
}

type windowObservationOutput struct {
	ID             string                 `json:"id"`
	Evaluation     string                 `json:"evaluation"`
	ObservedAt     string                 `json:"observed_at"`
	Classification string                 `json:"classification"`
	Best           bool                   `json:"best"`
	Candidate      *windowCandidateOutput `json:"candidate,omitempty"`
}

func writeWindowsJSON(writer io.Writer, records []arbitrage.WindowRecord) error {
	payload := make([]windowOutput, 0, len(records))
	for _, record := range records {
		payload = append(payload, newWindowOutput(record))
	}
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	return encoder.Encode(payload)
}

func writeWindowsText(writer io.Writer, records []arbitrage.WindowRecord) error {
	if _, err := fmt.Fprintf(writer, "Opportunity windows\nwindows: %d\n", len(records)); err != nil {
		return err
	}
	for _, record := range records {
		window := record.Window
		if _, err := fmt.Fprintf(writer, "window: %s %s %s->%s classification=%s opened=%s closed=%s duration=%s best_size=%s %s best_net=%s %s reason=%s degraded=%t\n",
			window.ID, window.Status, window.Direction.BuyMarket, window.Direction.SellMarket, window.Classification,
			window.OpenedAt.UTC().Format(time.RFC3339Nano), formatOptionalTime(window.ClosedAt), window.Duration(),
			window.Best.Size.Decimal(8), window.Best.Size.Asset(), window.Best.NetPnL.Decimal(8), window.Best.NetPnL.Asset(), window.CloseReason, window.Degraded); err != nil {
			return err
		}
		for _, observation := range record.Observations {
			if _, err := fmt.Fprintf(writer, "  best: %s evaluation=%s at=%s net=%s %s size=%s %s\n", observation.ID, observation.Evaluation, observation.ObservedAt.UTC().Format(time.RFC3339Nano), observation.Candidate.NetPnL.Decimal(8), observation.Candidate.NetPnL.Asset(), observation.Candidate.Size.Decimal(8), observation.Candidate.Size.Asset()); err != nil {
				return err
			}
		}
	}
	return nil
}

func newWindowOutput(record arbitrage.WindowRecord) windowOutput {
	window := record.Window
	output := windowOutput{
		ID: string(window.ID), Run: string(window.Run), Strategy: string(window.Strategy), ConfigHash: window.ConfigHash,
		BuyMarket: string(window.Direction.BuyMarket), SellMarket: string(window.Direction.SellMarket), Status: string(window.Status),
		Classification: string(window.Classification), OpenedAt: window.OpenedAt.UTC().Format(time.RFC3339Nano),
		FirstProfitableAt: window.FirstProfitableAt.UTC().Format(time.RFC3339Nano), LastProfitableAt: window.LastProfitableAt.UTC().Format(time.RFC3339Nano),
		ClosedAt: optionalJSONTime(window.ClosedAt), Duration: window.Duration().String(), CloseReason: window.CloseReason, Degraded: window.Degraded,
		Best: newWindowCandidateOutput(window.Best), Observations: make([]windowObservationOutput, 0, len(record.Observations)),
	}
	if window.HasTrigger {
		output.Trigger = &windowTriggerOutput{Market: string(window.Trigger.Market), Source: string(window.Trigger.Source), PositionKind: string(window.Trigger.Position.Kind), PositionValue: window.Trigger.Position.Value, ReferenceKind: string(window.Trigger.Reference.Kind), ReferenceValue: window.Trigger.Reference.Value, At: window.Trigger.At.UTC().Format(time.RFC3339Nano)}
	}
	for _, observation := range record.Observations {
		item := windowObservationOutput{ID: observation.ID, Evaluation: string(observation.Evaluation), ObservedAt: observation.ObservedAt.UTC().Format(time.RFC3339Nano), Classification: string(observation.Classification), Best: observation.Best}
		if observation.HasCandidate {
			candidate := newWindowCandidateOutput(observation.Candidate)
			item.Candidate = &candidate
		}
		output.Observations = append(output.Observations, item)
	}
	return output
}

func newWindowCandidateOutput(candidate arbitrage.WindowCandidate) windowCandidateOutput {
	return windowCandidateOutput{Size: candidate.Size.String(), SizeAsset: string(candidate.Size.Asset()), GrossPnL: candidate.GrossPnL.String(), NetPnL: candidate.NetPnL.String(), Cost: candidate.Cost.String(), Asset: string(candidate.NetPnL.Asset())}
}

func formatOptionalTime(value time.Time) string {
	if value.IsZero() {
		return "-"
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func optionalJSONTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
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
	fixturePath := flags.String("fixture", "examples/synthetic/two-market.yaml", "path to the experimental YAML fixture")
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
	configPath := flags.String("config", "config/local/vernier.yaml", "path to YAML configuration manifest")
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
	bundle, err := configuration.LoadConfig(*configPath)
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
