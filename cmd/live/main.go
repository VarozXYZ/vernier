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

	"github.com/VarozXYZ/vernier/internal/buildinfo"
	"github.com/VarozXYZ/vernier/runtime/configuration"
)

type armedRuntime interface {
	Run(context.Context) error
	Close() error
}

type runMode string

const (
	modeDryRun runMode = "dry-run"
	modeArmed  runMode = "armed"
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
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "live: positional arguments are not supported")
		return 2
	}
	if *arm && *dryRun {
		fmt.Fprintln(stderr, "live: -arm and -dry-run are mutually exclusive")
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
	mode := runMode("")
	if *arm {
		mode = modeArmed
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
	if composePrivate == nil {
		fmt.Fprintln(stderr, "live: private setup composition is not installed")
		return 2
	}
	runtime, err := composePrivate(ctx, config, os.LookupEnv, mode)
	if err != nil {
		fmt.Fprintf(stderr, "live: compose runtime: %v\n", err)
		return 1
	}
	defer runtime.Close()
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
		required := []string{account.PublicAddressEnv}
		if mode == modeArmed {
			required = append(required, account.SignerEnv)
		}
		if mode == modeArmed && account.SenderURLEnv != "" {
			required = append(required, account.SenderURLEnv)
		}
		if mode == modeArmed && account.FanoutRPCURLEnv != "" {
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
