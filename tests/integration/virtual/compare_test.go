package virtual_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/runtime/configuration"
	"github.com/VarozXYZ/vernier/runtime/livecompare"
	runtimeresearch "github.com/VarozXYZ/vernier/runtime/research"
)

func TestPublicSetupMatchesVenueReferences(t *testing.T) {
	for _, name := range []string{"VERNIER_ROBINHOOD_WS_URL", "VERNIER_BASE_WS_URL"} {
		if os.Getenv(name) == "" {
			t.Skip("public VIRTUAL integration requires configured RPC endpoints")
		}
	}
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	config, err := configuration.LoadConfig(filepath.Join(root, "examples", "setups", "virtual", "vernier.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	endpoints, err := config.ResolveEndpoints(os.LookupEnv)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	networks := make(livecompare.Networks, len(config.Chains))
	for id, profile := range config.Chains {
		network, dialErr := evm.DialReadOnlyNetwork(ctx, profile.ID, profile.Label, profile.ChainID, endpoints[id], endpoints[id])
		if dialErr != nil {
			closeNetworks(networks)
			t.Fatal(dialErr)
		}
		networks[id] = network
	}
	defer closeNetworks(networks)
	runner, err := livecompare.New(config, networks, livecompare.Options{LookupEnv: os.LookupEnv})
	if err != nil {
		t.Fatal(err)
	}
	report, err := runner.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Research.Status != runtimeresearch.StatusHealthy || len(report.Research.Opportunities) != 2 || len(report.Parity) != 40 {
		t.Fatalf("unexpected VIRTUAL report summary: status=%s opportunities=%d parity=%d", report.Research.Status, len(report.Research.Opportunities), len(report.Parity))
	}
	for _, evidence := range report.Parity {
		if !evidence.Matches {
			t.Fatalf("local quote differs from reference: %+v", evidence)
		}
	}
}

func closeNetworks(networks livecompare.Networks) {
	for _, network := range networks {
		if closer, ok := network.(interface{ Close() }); ok {
			closer.Close()
		}
	}
}
