// Command live starts the signer-enabled Vernier runtime. The public tree
// provides the generic vertical; a private ignored file registers the
// setup-specific route and contract composition.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	sqlitestore "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	"github.com/VarozXYZ/vernier/internal/buildinfo"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	"github.com/VarozXYZ/vernier/runtime/configuration"
	"github.com/VarozXYZ/vernier/runtime/livecanary"
	"github.com/VarozXYZ/vernier/runtime/observability"
)

type armedRuntime interface {
	Run(context.Context) error
	Close() error
}

type refuelRuntime interface {
	RefuelOnce(
		context.Context,
		market.ChainID,
		bool,
	) (executionport.RefuelRecord, error)
}

type runMode string

const (
	modeDryRun      runMode = "dry-run"
	modeArmed       runMode = "armed"
	modeCostObserve runMode = "cost-observe"
)

type privateFactory func(context.Context, configuration.ParsedLiveConfig, configuration.LookupEnv, runMode) (armedRuntime, error)

var composePrivate privateFactory

// An ignored setup-specific Go file assigns composePrivate from init. Keeping
// the hook package-private prevents it becoming a public plugin API while
// retaining normal compile-time composition.

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 1 &&
		(args[0] == "--version" || args[0] == "version") {
		fmt.Fprintln(stdout, buildinfo.Summary())
		return 0
	}
	flags := flag.NewFlagSet("live", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "config/live/vernier.yaml", "private modular Live manifest")
	envPath := flags.String("env-file", ".env.test", "local environment file")
	arm := flags.Bool("arm", false, "enable signer and broadcast capabilities")
	dryRun := flags.Bool("dry-run", false, "validate executable artifacts without signer, persistence, or broadcast")
	costObserve := flags.Bool(
		"cost-observe",
		false,
		"run Research and warm complete-flow costs without alerts or broadcast",
	)
	confirmCanary := flags.String(
		"confirm-canary-input",
		"",
		"must exactly match the configured sequential canary input when armed",
	)
	confirmLive := flags.String(
		"confirm-live-input",
		"",
		"must exactly match the configured sequential Live input when armed",
	)
	forceCanaryDirection := flags.String(
		"force-canary-direction",
		"",
		"execute one forced canary cycle: solana-to-evm or evm-to-solana",
	)
	acknowledgeReconciled := flags.String(
		"acknowledge-reconciled-operation",
		"",
		"close one manually reconciled operation barrier by exact operation ID",
	)
	refuelOnce := flags.String(
		"refuel-once",
		"",
		"build and simulate one gas refuel for solana or polygon; add -arm to broadcast",
	)
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "live: positional arguments are not supported")
		return 2
	}
	if boolCount(*arm, *dryRun, *costObserve) > 1 {
		fmt.Fprintln(stderr, "live: -arm, -dry-run, and -cost-observe are mutually exclusive")
		return 2
	}
	if strings.TrimSpace(*acknowledgeReconciled) != "" &&
		(*arm || *dryRun || *costObserve ||
			strings.TrimSpace(*confirmCanary) != "" ||
			strings.TrimSpace(*confirmLive) != "" ||
			strings.TrimSpace(*forceCanaryDirection) != "" ||
			strings.TrimSpace(*refuelOnce) != "") {
		fmt.Fprintln(
			stderr,
			"live: -acknowledge-reconciled-operation cannot be combined with execution flags",
		)
		return 2
	}
	if !*arm && strings.TrimSpace(*confirmCanary) != "" {
		fmt.Fprintln(stderr, "live: -confirm-canary-input requires -arm")
		return 2
	}
	if !*arm && strings.TrimSpace(*confirmLive) != "" {
		fmt.Fprintln(stderr, "live: -confirm-live-input requires -arm")
		return 2
	}
	if strings.TrimSpace(*confirmCanary) != "" &&
		strings.TrimSpace(*confirmLive) != "" {
		fmt.Fprintln(
			stderr,
			"live: -confirm-canary-input and -confirm-live-input are mutually exclusive",
		)
		return 2
	}
	forcedDirection, err := livecanary.ParseForcedCanaryDirection(
		*forceCanaryDirection,
	)
	if err != nil {
		fmt.Fprintf(stderr, "live: %v\n", err)
		return 2
	}
	if forcedDirection != "" && !*arm {
		fmt.Fprintln(stderr, "live: -force-canary-direction requires -arm")
		return 2
	}
	refuelChain := market.ChainID(strings.ToLower(strings.TrimSpace(*refuelOnce)))
	if refuelChain != "" &&
		refuelChain != market.ChainID("solana") &&
		refuelChain != market.ChainID("polygon") {
		fmt.Fprintln(stderr, "live: -refuel-once must be solana or polygon")
		return 2
	}
	if refuelChain != "" &&
		(*dryRun || *costObserve || forcedDirection != "" ||
			strings.TrimSpace(*confirmCanary) != "" ||
			strings.TrimSpace(*confirmLive) != "") {
		fmt.Fprintln(
			stderr,
			"live: -refuel-once cannot be combined with execution or observation flags",
		)
		return 2
	}
	if err := configuration.LoadEnvFile(*envPath, os.LookupEnv, os.Setenv); err != nil {
		fmt.Fprintln(stderr, "live: cannot load local environment")
		return 2
	}
	config, err := configuration.LoadLiveConfig(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "live: %v\n", err)
		return 2
	}
	if operationID := strings.TrimSpace(*acknowledgeReconciled); operationID != "" {
		store, openErr := sqlitestore.OpenSequentialLive(config.OperationalStorePath)
		if openErr != nil {
			fmt.Fprintf(stderr, "live: open operational journal: %v\n", openErr)
			return 1
		}
		defer store.Close()
		if acknowledgeErr := store.AcknowledgeManualReconciliation(
			ctx,
			execution.OperationID(operationID),
		); acknowledgeErr != nil {
			fmt.Fprintf(stderr, "live: %v\n", acknowledgeErr)
			return 1
		}
		fmt.Fprintf(
			stdout,
			"live_reconciliation operation=%s state=%s journal=%s\n",
			operationID,
			execution.SequentialReconciledManually,
			config.OperationalStorePath,
		)
		return 0
	}
	if refuelChain != "" {
		store, openErr := sqlitestore.OpenSequentialLive(
			config.OperationalStorePath,
		)
		if openErr != nil {
			fmt.Fprintf(stderr, "live: open operational journal: %v\n", openErr)
			return 1
		}
		active, found, activeErr := store.ActiveSequentialOperation(ctx)
		_ = store.Close()
		if activeErr != nil {
			fmt.Fprintf(stderr, "live: inspect operational barrier: %v\n", activeErr)
			return 1
		}
		if found {
			fmt.Fprintf(
				stderr,
				"live: operation %s is %s; run automatic recovery before refueling manually\n",
				active.ID,
				active.State,
			)
			return 1
		}
	}
	if *arm && refuelChain == "" &&
		config.ExecutionMode == "sequential_bridge_canary" {
		expected := ""
		if config.CanaryInput != nil {
			expected = config.CanaryInput.RatString()
		}
		if strings.TrimSpace(*confirmCanary) != expected || expected == "" {
			fmt.Fprintf(
				stderr,
				"live: -arm requires -confirm-canary-input %s\n",
				expected,
			)
			return 2
		}
	}
	if *arm && refuelChain == "" &&
		config.ExecutionMode == "sequential_bridge_live" {
		expected := ""
		if config.ExecutionInput != nil {
			expected = config.ExecutionInput.RatString()
		}
		if strings.TrimSpace(*confirmLive) != expected || expected == "" {
			fmt.Fprintf(
				stderr,
				"live: -arm requires -confirm-live-input %s\n",
				expected,
			)
			return 2
		}
	}
	if forcedDirection != "" &&
		config.ExecutionMode != "sequential_bridge_canary" {
		fmt.Fprintln(
			stderr,
			"live: -force-canary-direction is only available for sequential_bridge_canary",
		)
		return 2
	}
	mode := runMode("")
	if *arm {
		mode = modeArmed
	} else if refuelChain != "" {
		// Refuel preview needs signer-backed transaction construction and
		// simulation, but RefuelOnce still keeps broadcast disarmed.
		mode = modeArmed
	} else if *costObserve {
		mode = modeCostObserve
	} else if *dryRun {
		mode = modeDryRun
	}
	if err := validateEnvironment(config, os.LookupEnv, mode); err != nil {
		fmt.Fprintf(stderr, "live: %v\n", err)
		return 2
	}
	if mode == "" {
		fmt.Fprintln(stdout, "Live configuration and environment are valid; broadcast remains disarmed.")
		return 0
	}
	var runtime armedRuntime
	if config.ExecutionMode == "sequential_bridge_canary" ||
		config.ExecutionMode == "sequential_bridge_live" {
		if mode == modeDryRun {
			fmt.Fprintln(
				stdout,
				"Sequential Live configuration is valid; artifact construction requires an armed qualified opportunity and remains disabled.",
			)
			return 0
		}
		researchConfig, loadErr := configuration.LoadConfig(*configPath)
		if loadErr != nil {
			fmt.Fprintf(stderr, "live: %v\n", loadErr)
			return 2
		}
		logger, loggerErr := observability.NewLogger(stderr, "info")
		if loggerErr != nil {
			fmt.Fprintf(stderr, "live: create logger: %v\n", loggerErr)
			return 1
		}
		runtime, err = livecanary.ComposeArmed(ctx, livecanary.ComposeConfig{
			ManifestPath:     *configPath,
			Research:         researchConfig,
			Live:             config,
			LookupEnv:        os.LookupEnv,
			Logger:           logger,
			Output:           stdout,
			ObserveCostsOnly: mode == modeCostObserve,
			RefuelOnly:       refuelChain != "",
			ForcedCanary:     forcedDirection,
		})
	} else {
		if composePrivate == nil {
			fmt.Fprintln(stderr, "live: private setup composition is not installed")
			return 2
		}
		runtime, err = composePrivate(ctx, config, os.LookupEnv, mode)
	}
	if err != nil {
		fmt.Fprintf(stderr, "live: compose runtime: %v\n", err)
		return 1
	}
	defer runtime.Close()
	if refuelChain != "" {
		refueler, ok := runtime.(refuelRuntime)
		if !ok {
			fmt.Fprintln(stderr, "live: runtime does not support gas refuel")
			return 1
		}
		record, refuelErr := refueler.RefuelOnce(ctx, refuelChain, *arm)
		if refuelErr != nil {
			fmt.Fprintf(stderr, "live: refuel %s: %v\n", refuelChain, refuelErr)
			return 1
		}
		action := "simulated"
		if *arm {
			action = "completed"
		}
		fmt.Fprintf(
			stdout,
			"live_refuel chain=%s action=%s input=%s output=%s fee=%s tx=%s\n",
			refuelChain, action, record.Input.String(),
			record.NativeReceived.String(), record.Fee.String(),
			record.Identity.Hash,
		)
		return 0
	}
	if err := runtime.Run(ctx); err != nil {
		fmt.Fprintf(stderr, "live: runtime failed: %v\n", err)
		return 1
	}
	return 0
}

func validateEnvironment(config configuration.ParsedLiveConfig, lookup configuration.LookupEnv, mode runMode) error {
	if _, err := config.ResolveEndpoints(lookup); err != nil {
		return err
	}
	for chainID, account := range config.Accounts {
		required := make([]string, 0, 4)
		if account.PublicAddressEnv != "" {
			required = append(required, account.PublicAddressEnv)
		}
		if mode == modeArmed || mode == modeCostObserve {
			required = append(required, account.SignerEnv)
		}
		if (mode == modeArmed || mode == modeCostObserve) && account.SenderURLEnv != "" {
			required = append(required, account.SenderURLEnv)
		}
		if (mode == modeArmed || mode == modeCostObserve) && account.FanoutRPCURLEnv != "" {
			required = append(required, account.FanoutRPCURLEnv)
		}
		if account.ContractAddressEnv != "" {
			required = append(required, account.ContractAddressEnv)
		}
		for _, name := range required {
			value, ok := lookup(name)
			if !ok || strings.TrimSpace(value) == "" {
				return fmt.Errorf("required Live environment for chain %q is unset", chainID)
			}
		}
	}
	return nil
}

func boolCount(values ...bool) int {
	count := 0
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}
