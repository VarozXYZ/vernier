package observev3_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VarozXYZ/vernier/runtime/configuration"
	"github.com/VarozXYZ/vernier/runtime/observev3"
)

const observeManifest = `schema_version: 1
topology: topology.yaml
policy: policy.yaml
active_research: observe
`

const observeTopology = `schema_version: 1
chains:
  ethereum: {kind: evm, label: Ethereum, chain_id: "1", rpc_url_env: ETHEREUM_RPC}
assets:
  asset_x: {symbol: ASSET_X}
  weth: {symbol: WETH}
  usd: {symbol: USD}
tokens:
  token_0: {asset: asset_x, chain: ethereum, address: "0x3000000000000000000000000000000000000003", decimals: 0, symbol: ASSET_X}
  token_1: {asset: weth, chain: ethereum, address: "0x4000000000000000000000000000000000000004", decimals: 6, symbol: WETH}
venues:
  canonical_v3:
    kind: uniswap_v3
    chain: ethereum
    pool_address: "0x1000000000000000000000000000000000000001"
    reference_address: "0x2000000000000000000000000000000000000002"
    max_tick_words: 64
  canonical_v2:
    kind: uniswap_v2
    chain: ethereum
    pool_address: "0x5000000000000000000000000000000000000005"
    factory_address: "0x6000000000000000000000000000000000000006"
    reference_address: "0x7000000000000000000000000000000000000007"
    fee_bps: 30
markets:
  local_market: {venue: canonical_v3, base_token: token_0, quote_token: token_1}
  other_market: {venue: canonical_v2, base_token: token_0, quote_token: token_1}
price_sources:
  weth_usd:
    base_asset: weth
    quote_asset: usd
    primary: {kind: coingecko, coin_id: weth, currency: usd}
    fallback: {kind: chainlink, chain: ethereum, feed_address: "0x8000000000000000000000000000000000000008"}
`

const observePolicy = `schema_version: 1
setups:
  setup: {markets: [local_market, other_market]}
research:
  observe:
    run_id: observe-run
    setup: setup
    inventory_mode: prepositioned
    price_source: weth_usd
    fixed_cost: {asset: usd, amount: "0.5"}
    min_net_profit: "0"
    sizing: {kind: linear_range, asset: base, min: "1", max: "1000000", samples: 2}
`

func TestConfigUsesSharedYAMLAndDerivesBothDirections(t *testing.T) {
	config := observerConfig(t)
	if config.NetworkAdapter != "ethereum" || config.VenueAdapter != "uniswap-v3" || len(config.Hash) != 64 ||
		len(config.QuoteInputs) != 2 || config.QuoteInputs[0].Amount != "1000000" || config.QuoteInputs[1].Amount != "1000000" {
		t.Fatalf("unexpected observer configuration: %+v", config)
	}
	path := writeObserverFiles(t, strings.Replace(observeTopology, "schema_version: 1", "schema_version: 1\nunexpected: true", 1))
	if _, err := configuration.LoadConfig(path); err == nil {
		t.Fatal("unknown YAML field was accepted")
	}
}

func TestConfigUsesQuoteBudgetForQuoteSizing(t *testing.T) {
	policy := strings.Replace(observePolicy, "asset: base", "asset: quote", 1)
	directory := t.TempDir()
	for name, data := range map[string]string{"vernier.yaml": observeManifest, "topology.yaml": observeTopology, "policy.yaml": policy} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	bundle, err := configuration.LoadConfig(filepath.Join(directory, "vernier.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	config, err := observev3.FromConfig(bundle, "local_market")
	if err != nil {
		t.Fatal(err)
	}
	if config.QuoteInputs[0].Amount != "1000000000000" || config.QuoteInputs[1].Amount != "1000000000000" {
		t.Fatalf("quote sizing did not drive observer probes: %+v", config.QuoteInputs)
	}
}

func TestEndpointResolutionDoesNotExposeValues(t *testing.T) {
	config := observerConfig(t)
	secret := "https://user:password@example.invalid"
	_, err := config.ResolveEndpoints(func(string) (string, bool) { return secret, false })
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("endpoint error leaked a configured value: %v", err)
	}
}

func observerConfig(t *testing.T) observev3.Config {
	t.Helper()
	bundle, err := configuration.LoadConfig(writeObserverFiles(t, observeTopology))
	if err != nil {
		t.Fatal(err)
	}
	config, err := observev3.FromConfig(bundle, "local_market")
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func writeObserverFiles(t *testing.T, topology string) string {
	t.Helper()
	directory := t.TempDir()
	for name, data := range map[string]string{"vernier.yaml": observeManifest, "topology.yaml": topology, "policy.yaml": observePolicy} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return filepath.Join(directory, "vernier.yaml")
}
