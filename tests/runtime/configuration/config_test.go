package configuration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VarozXYZ/vernier/runtime/configuration"
)

const manifestYAML = `schema_version: 1
topology: topology.yaml
policy: policy.yaml
active_research: live
`

const topologyYAML = `schema_version: 1
chains:
  chain_a: {kind: evm, label: Chain A, chain_id: "1", rpc_url_env: CHAIN_A_RPC}
  chain_b: {kind: evm, label: Chain B, chain_id: "8453", rpc_url_env: CHAIN_B_RPC}
assets:
  virtual: {symbol: VIRTUAL}
  weth: {symbol: WETH}
  usd: {symbol: USD}
tokens:
  virtual_a: {asset: virtual, chain: chain_a, address: "0x0000000000000000000000000000000000000001", decimals: 18, symbol: VIRTUAL}
  weth_a: {asset: weth, chain: chain_a, address: "0x0000000000000000000000000000000000000002", decimals: 18, symbol: WETH}
  virtual_b: {asset: virtual, chain: chain_b, address: "0x0000000000000000000000000000000000000003", decimals: 18, symbol: VIRTUAL}
  weth_b: {asset: weth, chain: chain_b, address: "0x0000000000000000000000000000000000000004", decimals: 18, symbol: WETH}
venues:
  venue_a:
    kind: uniswap_v2
    chain: chain_a
    pool_address: "0x0000000000000000000000000000000000000005"
    factory_address: "0x0000000000000000000000000000000000000006"
    reference_address: "0x0000000000000000000000000000000000000007"
    fee_bps: 30
  venue_b:
    kind: aerodrome_slipstream
    chain: chain_b
    pool_address: "0x0000000000000000000000000000000000000008"
    factory_address: "0x0000000000000000000000000000000000000009"
    reference_address: "0x000000000000000000000000000000000000000a"
    max_tick_words: 16
markets:
  market_a: {venue: venue_a, base_token: virtual_a, quote_token: weth_a}
  market_b: {venue: venue_b, base_token: virtual_b, quote_token: weth_b}
price_sources:
  weth_usd:
    base_asset: weth
    quote_asset: usd
    primary: {kind: coingecko, coin_id: weth, currency: usd, api_key_env: COINGECKO_KEY, api_key_kind: demo}
    fallback: {kind: chainlink, chain: chain_b, feed_address: "0x000000000000000000000000000000000000000b"}
`

const policyYAML = `schema_version: 1
setups:
  cross_chain: {markets: [market_a, market_b]}
research:
  live:
    run_id: live-run
    setup: cross_chain
    inventory_mode: prepositioned
    price_source: weth_usd
    fixed_cost: {asset: usd, amount: "0.5"}
    min_net_profit: "0"
    sizing: {kind: linear_range, min: "100", max: "5000", samples: 10}
`

func TestLoadConfigResolvesModularYAMLExactly(t *testing.T) {
	path := writeConfig(t, manifestYAML, topologyYAML, policyYAML)
	config, err := configuration.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.FixedCost.RatString() != "1/2" || config.SizingAsset != "quote" || config.MinimumSize.RatString() != "100" ||
		config.MaximumSize.RatString() != "5000" || config.SizeSamples != 10 || len(config.Hash) != 64 ||
		len(config.Chains) != 2 || config.Markets[0].Venue.Kind != "uniswap_v2" {
		t.Fatalf("unexpected parsed configuration: %+v", config)
	}
	endpoints, err := config.ResolveEndpoints(func(name string) (string, bool) { return "wss://" + strings.ToLower(name), true })
	if err != nil || endpoints["chain_a"] == endpoints["chain_b"] {
		t.Fatalf("unexpected endpoints: %+v, %v", endpoints, err)
	}
}

func TestLoadConfigRejectsUnknownFieldsAndBrokenReferences(t *testing.T) {
	for name, topology := range map[string]string{
		"unknown field":  strings.Replace(topologyYAML, "schema_version: 1", "schema_version: 1\nunknown: true", 1),
		"unknown market": strings.Replace(topologyYAML, "market_a: {venue: venue_a", "market_a: {venue: missing", 1),
		"wrong asset":    strings.Replace(topologyYAML, "quote_asset: usd", "quote_asset: weth", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := configuration.LoadConfig(writeConfig(t, manifestYAML, topology, policyYAML)); err == nil {
				t.Fatal("invalid configuration was accepted")
			}
		})
	}
}

func TestLoadConfigRejectsUnsupportedSizingAsset(t *testing.T) {
	policy := strings.Replace(policyYAML, "sizing: {kind: linear_range, min: \"100\", max: \"5000\", samples: 10}", "sizing: {kind: linear_range, asset: notional, min: \"100\", max: \"5000\", samples: 10}", 1)
	if _, err := configuration.LoadConfig(writeConfig(t, manifestYAML, topologyYAML, policy)); err == nil {
		t.Fatal("unsupported sizing asset was accepted")
	}
}

func TestConfigurationHashIgnoresYAMLFormatting(t *testing.T) {
	first, err := configuration.LoadConfig(writeConfig(t, manifestYAML, topologyYAML, policyYAML))
	if err != nil {
		t.Fatal(err)
	}
	secondTopology := strings.Replace(topologyYAML, "schema_version: 1\n", "# comment\nschema_version: 1\n\n", 1)
	second, err := configuration.LoadConfig(writeConfig(t, manifestYAML, secondTopology, policyYAML))
	if err != nil {
		t.Fatal(err)
	}
	if first.Hash != second.Hash {
		t.Fatal("semantic hash changed because of YAML formatting")
	}
}

func TestPublicVirtualSetupResolves(t *testing.T) {
	path := filepath.Join("..", "..", "..", "examples", "setups", "virtual", "vernier.yaml")
	config, err := configuration.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.ResearchID != "virtual_cross_chain" || config.SetupID != "virtual_wealth" ||
		config.Chains["robinhood"].ChainID.String() != "4663" || config.Chains["base"].ChainID.String() != "8453" ||
		config.Markets[0].Venue.Kind != "uniswap_v2" || config.Markets[1].Venue.Kind != "aerodrome_volatile" ||
		config.Markets[1].Venue.FeeBPS != 100 || !strings.EqualFold(config.Markets[1].Venue.Pool.Hex(), "0x21594b992f68495dd28d605834b58889d0a727c7") ||
		config.SizingAsset != "quote" || config.MinimumSize.RatString() != "1/100" || config.MaximumSize.RatString() != "1" || config.SizeSamples != 5 {
		t.Fatalf("unexpected public VIRTUAL setup: %+v", config)
	}
}

func writeConfig(t *testing.T, manifest, topology, policy string) string {
	t.Helper()
	directory := t.TempDir()
	for name, data := range map[string]string{"vernier.yaml": manifest, "topology.yaml": topology, "policy.yaml": policy} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return filepath.Join(directory, "vernier.yaml")
}
