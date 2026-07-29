package configuration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/runtime/configuration"
)

func TestLoadLiveConfigResolvesExecutionPolicy(t *testing.T) {
	manifest := writeLiveConfig(t, "0.05")
	config, err := configuration.LoadLiveConfig(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if config.LiveID != "synthetic_live" || config.RunID != "synthetic-run" ||
		config.BuildToBroadcastTimeout != 250*time.Millisecond || config.EVMDeadline != 20*time.Second {
		t.Fatalf("unexpected resolved identity or timeouts: %+v", config)
	}
	if config.SQLiteSynchronous != "FULL" || config.ExecutionCost.RatString() != "1/20" ||
		config.MaximumExecutionCost.RatString() != "1/2" ||
		config.MaximumBaseExposure.RatString() != "500" ||
		config.ReturnBridgeSafetyMargin.RatString() != "1/10" {
		t.Fatalf("unexpected durability or cost: sync=%s cost=%s", config.SQLiteSynchronous, config.ExecutionCost)
	}
	if len(config.Accounts) != 2 || len(config.Inventory) != 4 {
		t.Fatalf("accounts=%d inventory=%d", len(config.Accounts), len(config.Inventory))
	}
}

func TestLoadLiveConfigRejectsZeroExecutionCost(t *testing.T) {
	manifest := writeLiveConfig(t, "0")
	_, err := configuration.LoadLiveConfig(manifest)
	if err == nil || !strings.Contains(err.Error(), "live execution cost must be positive") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadLiveConfigPromotesSequentialExecutionAtDiscoveryNotional(t *testing.T) {
	manifest := writeSequentialLiveConfig(t, "100")
	config, err := configuration.LoadLiveConfig(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if config.ExecutionMode != "sequential_bridge_live" ||
		config.Notional.RatString() != "100" ||
		config.ExecutionInput.RatString() != "100" ||
		config.CanaryInput != nil ||
		config.MaxOperationsPerRun != 1 {
		t.Fatalf("unexpected sequential Live sizing: %+v", config)
	}
	if config.ExecutionCost.Sign() != 0 {
		t.Fatalf("dynamic sequential execution cost=%s, want zero placeholder", config.ExecutionCost)
	}
	if config.BaseTransferSource.Kind != "synthetic-base-transfer" ||
		config.QuoteTransferSource.Kind != "synthetic-quote-transfer" {
		t.Fatalf("transfer capabilities were not resolved: %+v", config)
	}
	if config.Accounts["chain_a"].FanoutRPCURLEnv != "CHAIN_A_FANOUT" ||
		config.Accounts["chain_b"].FanoutRPCURLEnv != "CHAIN_B_FANOUT" {
		t.Fatalf("broadcast transports were not resolved: %+v", config.Accounts)
	}
}

func TestLoadLiveConfigRejectsSequentialExecutionBelowDiscoveryNotional(t *testing.T) {
	manifest := writeSequentialLiveConfig(t, "1")
	_, err := configuration.LoadLiveConfig(manifest)
	if err == nil || !strings.Contains(
		err.Error(),
		"live execution input must equal discovery notional",
	) {
		t.Fatalf("error=%v", err)
	}
}

func TestLoadLiveConfigRejectsUnknownTransferSource(t *testing.T) {
	manifest := writeSequentialLiveConfig(t, "100")
	policyPath := filepath.Join(filepath.Dir(manifest), "policy.yaml")
	data, err := os.ReadFile(policyPath)
	if err != nil {
		t.Fatal(err)
	}
	invalid := strings.Replace(
		string(data),
		"base_transfer_source: transfer_base",
		"base_transfer_source: missing_transfer",
		1,
	)
	if err := os.WriteFile(policyPath, []byte(invalid), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = configuration.LoadLiveConfig(manifest)
	if err == nil || !strings.Contains(
		err.Error(),
		"sequential transfer and confirmation policy is incomplete",
	) {
		t.Fatalf("error=%v", err)
	}
}

func writeLiveConfig(t *testing.T, cost string) string {
	t.Helper()
	directory := t.TempDir()
	manifest := `schema_version: 1
topology: topology.yaml
policy: policy.yaml
active_live: synthetic_live
`
	topology := `schema_version: 1
chains:
  chain_a:
    kind: solana
    label: Synthetic A
    http_url_env: CHAIN_A_HTTP
    websocket_url_env: CHAIN_A_WS
  chain_b:
    kind: evm
    label: Synthetic B
    chain_id: "12345"
    rpc_url_env: CHAIN_B_RPC
assets:
  base: {symbol: BASE}
  quote: {symbol: QUOTE}
tokens:
  base_a: {asset: base, chain: chain_a, address: "Base111111111111111111111111111111111", decimals: 6, symbol: BASE}
  quote_a: {asset: quote, chain: chain_a, address: "Quote11111111111111111111111111111111", decimals: 6, symbol: QUOTE}
  base_b: {asset: base, chain: chain_b, address: "0x1111111111111111111111111111111111111111", decimals: 18, symbol: BASE}
  quote_b: {asset: quote, chain: chain_b, address: "0x2222222222222222222222222222222222222222", decimals: 6, symbol: QUOTE}
venues:
  venue_a:
    kind: orca_whirlpool
    chain: chain_a
    pool_address: "Pool111111111111111111111111111111111"
    fee_bps: 30
  venue_b:
    kind: uniswap_v2
    chain: chain_b
    pool_address: "0x3333333333333333333333333333333333333333"
    factory_address: "0x4444444444444444444444444444444444444444"
    reference_address: "0x5555555555555555555555555555555555555555"
    fee_bps: 30
markets:
  market_a: {venue: venue_a, base_token: base_a, quote_token: quote_a, quote_source: jupiter}
  market_b: {venue: venue_b, base_token: base_b, quote_token: quote_b}
quote_sources:
  jupiter:
    kind: jupiter
    base_url: https://example.invalid
    taker_env: SOLANA_PUBLIC_KEY
    api_key_env: JUPITER_API_KEYS
    slippage_bps: 30
    max_accounts: 64
`
	policy := `schema_version: 1
setups:
  synthetic_setup: {markets: [market_a, market_b]}
live:
  synthetic_live:
    run_id: synthetic-run
    setup: synthetic_setup
    inventory_mode: prefunded_live
    notional: {asset: quote, amount: "100"}
    execution_cost: {asset: quote, amount: "` + cost + `"}
    max_execution_cost: {asset: quote, amount: "0.50"}
    max_base_exposure: {asset: base, amount: "500"}
    min_net_profit: {asset: quote, amount: "0.20"}
    return_bridge_safety_margin: {asset: quote, amount: "0.10"}
    fee_cache_max_age_ms: 2000
    slippage_bps: 30
    tip_lamports: "200000"
    compute_unit_price_percentile: "75"
    compute_unit_limit: 1200000
    blockhash_slots_to_expiry: 20
    build_to_broadcast_timeout_ms: 250
    evm_deadline_seconds: 20
    operational_store: {path: ".vernier/live.sqlite", synchronous: FULL}
    accounts:
      chain_a: {id: account_a, public_address_env: CHAIN_A_ADDRESS, signer_env: CHAIN_A_SIGNER, sender_url_env: HELIUS_SENDER_URL}
      chain_b: {id: account_b, public_address_env: CHAIN_B_ADDRESS, signer_env: CHAIN_B_SIGNER, fanout_rpc_urls_env: CHAIN_B_FANOUT, contract_address_env: CHAIN_B_CONTRACT}
    inventory:
      - {chain: chain_a, account: account_a, token: base_a, amount: "100000"}
      - {chain: chain_a, account: account_a, token: quote_a, amount: "10000"}
      - {chain: chain_b, account: account_b, token: base_b, amount: "100000"}
      - {chain: chain_b, account: account_b, token: quote_b, amount: "10000"}
`
	for name, data := range map[string]string{
		"vernier.yaml": manifest, "topology.yaml": topology, "policy.yaml": policy,
	} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return filepath.Join(directory, "vernier.yaml")
}

func writeSequentialLiveConfig(t *testing.T, executionInput string) string {
	t.Helper()
	directory := t.TempDir()
	files := map[string]string{
		"vernier.yaml": `schema_version: 1
topology: topology.yaml
policy: policy.yaml
active_live: synthetic_sequential_live
`,
		"topology.yaml": `schema_version: 1
chains:
  chain_a:
    kind: evm
    label: Synthetic A
    chain_id: "12345"
    rpc_url_env: CHAIN_A_RPC
  chain_b:
    kind: evm
    label: Synthetic B
    chain_id: "67890"
    rpc_url_env: CHAIN_B_RPC
assets:
  base: {symbol: BASE}
  quote: {symbol: QUOTE}
tokens:
  base_a: {asset: base, chain: chain_a, address: "0x1111111111111111111111111111111111111111", decimals: 18, symbol: BASE}
  quote_a: {asset: quote, chain: chain_a, address: "0x2222222222222222222222222222222222222222", decimals: 6, symbol: QUOTE}
  base_b: {asset: base, chain: chain_b, address: "0x3333333333333333333333333333333333333333", decimals: 18, symbol: BASE}
  quote_b: {asset: quote, chain: chain_b, address: "0x4444444444444444444444444444444444444444", decimals: 6, symbol: QUOTE}
pools:
  pool_a: {chain: chain_a, kind: uniswap_v3, address: "0x5555555555555555555555555555555555555555"}
  pool_b: {chain: chain_b, kind: uniswap_v3, address: "0x6666666666666666666666666666666666666666"}
quote_sources:
  source_a:
    kind: kyberswap
    base_url: https://example.invalid
    chain_slug: synthetic-a
    client_id_env: CHAIN_A_CLIENT_ID
    slippage_bps: 10
  source_b:
    kind: kyberswap
    base_url: https://example.invalid
    chain_slug: synthetic-b
    client_id_env: CHAIN_B_CLIENT_ID
    slippage_bps: 10
transfer_sources:
  transfer_base:
    kind: synthetic-base-transfer
    profile: synthetic-base.yaml
  transfer_quote:
    kind: synthetic-quote-transfer
    api_key_env: TRANSFER_QUOTE_API_KEY
    integrator_id_env: TRANSFER_QUOTE_INTEGRATOR
markets:
  market_a:
    chain: chain_a
    base_token: base_a
    quote_token: quote_a
    quote_source: source_a
    trigger_pools: [pool_a]
  market_b:
    chain: chain_b
    base_token: base_b
    quote_token: quote_b
    quote_source: source_b
    trigger_pools: [pool_b]
`,
		"policy.yaml": `schema_version: 1
setups:
  synthetic_setup: {markets: [market_a, market_b]}
live:
  synthetic_sequential_live:
    run_id: synthetic-sequential-run
    setup: synthetic_setup
    inventory_mode: prefunded_live
    execution_mode: sequential_bridge_live
    notional: {asset: quote, amount: "100"}
    execution_input: {asset: quote, amount: "` + executionInput + `"}
    max_operations_per_run: 1
    base_transfer_source: transfer_base
    quote_transfer_source: transfer_quote
    confirmation_timeout_seconds: 600
    max_execution_cost: {asset: quote, amount: "10"}
    max_base_exposure: {asset: base, amount: "1000"}
    min_net_profit: {asset: quote, amount: "1"}
    return_bridge_safety_margin: {asset: quote, amount: "0"}
    fee_cache_max_age_ms: 5000
    cost_refresh_interval_ms: 15000
    cost_cache_ttl_ms: 60000
    slippage_bps: 10
    build_to_broadcast_timeout_ms: 2000
    evm_deadline_seconds: 120
    operational_store: {path: ".runtime/live.sqlite", synchronous: FULL}
    accounts:
      chain_a: {id: account_a, signer_env: CHAIN_A_SIGNER, fanout_rpc_urls_env: CHAIN_A_FANOUT}
      chain_b: {id: account_b, signer_env: CHAIN_B_SIGNER, fanout_rpc_urls_env: CHAIN_B_FANOUT}
    inventory:
      - {chain: chain_a, account: account_a, token: quote_a, amount: "1000"}
      - {chain: chain_b, account: account_b, token: quote_b, amount: "1000"}
`,
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return filepath.Join(directory, "vernier.yaml")
}
