package configuration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VarozXYZ/vernier/runtime/configuration"
)

const manifestYAML = `schema_version: 2
topology: topology.yaml
policy: policy.yaml
active_research: live
`

const topologyYAML = `schema_version: 2
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
  market_b: {venue: venue_b, base_token: virtual_b, quote_token: weth_b, reference_quote: external}
quote_sources:
  external: {kind: jupiter, taker_env: PUBLIC_TAKER, slippage_bps: 50, max_accounts: 32}
price_sources:
  weth_usd:
    base_asset: weth
    quote_asset: usd
    primary: {kind: coingecko, coin_id: weth, currency: usd, api_key_env: COINGECKO_KEY, api_key_kind: demo}
    fallback: {kind: chainlink, chain: chain_b, feed_address: "0x000000000000000000000000000000000000000b"}
`

const policyYAML = `schema_version: 2
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
		len(config.Chains) != 2 || config.Markets[0].Venue.Kind != "uniswap_v2" || config.Markets[1].ReferenceQuote != "external" || config.QuoteSources["external"].TakerEnv != "PUBLIC_TAKER" {
		t.Fatalf("unexpected parsed configuration: %+v", config)
	}
	endpoints, err := config.ResolveEndpoints(func(name string) (string, bool) { return "wss://" + strings.ToLower(name), true })
	if err != nil || endpoints["chain_a"] == endpoints["chain_b"] {
		t.Fatalf("unexpected endpoints: %+v, %v", endpoints, err)
	}
}

func TestLoadConfigResolvesStateOnlyPoolTriggerPolicy(t *testing.T) {
	topology := strings.Replace(topologyYAML, "markets:\n", `pools:
  auxiliary_pool: {venue: venue_a, chain: chain_a, address: "0x0000000000000000000000000000000000000005", suppress_evaluation_trigger: true}
paths:
  auxiliary_path:
    chain: chain_a
    hops: [{pool: auxiliary_pool, token_in: virtual_a, token_out: weth_a}]
markets:
`, 1)
	topology = strings.Replace(topology,
		"market_a: {venue: venue_a, base_token: virtual_a, quote_token: weth_a}",
		"market_a: {path: auxiliary_path, base_token: virtual_a, quote_token: weth_a}", 1)
	config, err := configuration.LoadConfig(writeConfig(t, manifestYAML, topology, policyYAML))
	if err != nil {
		t.Fatal(err)
	}
	if len(config.Markets[0].Path) != 1 || !config.Markets[0].Path[0].SuppressEvaluationTrigger {
		t.Fatalf("state-only trigger policy was not resolved: %+v", config.Markets[0].Path)
	}
}

func TestLoadConfigResolvesExplicitV3CoverageProbes(t *testing.T) {
	topology := strings.Replace(topologyYAML, "markets:\n", `pools:
  v3_pool: {venue: venue_b, chain: chain_b, address: "0x0000000000000000000000000000000000000008", coverage_probe_in: "1500", coverage_probe_out: "3"}
paths:
  v3_path:
    chain: chain_b
    hops: [{pool: v3_pool, token_in: virtual_b, token_out: weth_b}]
markets:
`, 1)
	topology = strings.Replace(topology,
		"market_b: {venue: venue_b, base_token: virtual_b, quote_token: weth_b, reference_quote: external}",
		"market_b: {path: v3_path, base_token: virtual_b, quote_token: weth_b, reference_quote: external}", 1)
	config, err := configuration.LoadConfig(writeConfig(t, manifestYAML, topology, policyYAML))
	if err != nil {
		t.Fatal(err)
	}
	hop := config.Markets[1].Path[0]
	if hop.CoverageProbeIn == nil || hop.CoverageProbeIn.RatString() != "1500" ||
		hop.CoverageProbeOut == nil || hop.CoverageProbeOut.RatString() != "3" {
		t.Fatalf("unexpected V3 coverage probes: %+v", hop)
	}

	invalid := strings.Replace(topology, `coverage_probe_out: "3"`, `coverage_probe_out: ""`, 1)
	if _, err = configuration.LoadConfig(writeConfig(t, manifestYAML, invalid, policyYAML)); err == nil {
		t.Fatal("one-sided V3 coverage probes were accepted")
	}
}

func TestLoadConfigRejectsUnknownFieldsAndBrokenReferences(t *testing.T) {
	for name, topology := range map[string]string{
		"unknown field":  strings.Replace(topologyYAML, "schema_version: 2", "schema_version: 2\nunknown: true", 1),
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

func TestLoadConfigRejectsSchemaV1Explicitly(t *testing.T) {
	manifest := strings.Replace(
		manifestYAML, "schema_version: 2", "schema_version: 1", 1,
	)
	_, err := configuration.LoadConfig(
		writeConfig(t, manifest, topologyYAML, policyYAML),
	)
	if err == nil ||
		!strings.Contains(err.Error(), "schema v2 is required") {
		t.Fatalf("error=%v", err)
	}
}

func TestLoadConfigRejectsUnsupportedSizingAsset(t *testing.T) {
	policy := strings.Replace(policyYAML, "sizing: {kind: linear_range, min: \"100\", max: \"5000\", samples: 10}", "sizing: {kind: linear_range, asset: notional, min: \"100\", max: \"5000\", samples: 10}", 1)
	if _, err := configuration.LoadConfig(writeConfig(t, manifestYAML, topologyYAML, policy)); err == nil {
		t.Fatal("unsupported sizing asset was accepted")
	}
}

func TestLoadConfigResolvesDiscreteSizingGrid(t *testing.T) {
	policy := strings.Replace(policyYAML,
		`sizing: {kind: linear_range, min: "100", max: "5000", samples: 10}`,
		`sizing: {kind: discrete, values: ["250", "500", "750", "1000"]}`, 1)
	config, err := configuration.LoadConfig(writeConfig(t, manifestYAML, topologyYAML, policy))
	if err != nil {
		t.Fatal(err)
	}
	if config.SizingKind != "discrete" || len(config.Sizes) != 4 ||
		config.MinimumSize.RatString() != "250" || config.MaximumSize.RatString() != "1000" {
		t.Fatalf("unexpected discrete sizing: %+v", config.Sizes)
	}
	for index, want := range []string{"250", "500", "750", "1000"} {
		if got := config.Sizes[index].RatString(); got != want {
			t.Fatalf("size[%d]=%s want=%s", index, got, want)
		}
	}

	invalid := strings.Replace(policy, `"500", "750"`, `"750", "500"`, 1)
	if _, err := configuration.LoadConfig(writeConfig(t, manifestYAML, topologyYAML, invalid)); err == nil ||
		!strings.Contains(err.Error(), "strictly increasing") {
		t.Fatalf("unordered discrete sizes accepted: %v", err)
	}
}

func TestLoadConfigResolvesFixedAndPercentageProfitThreshold(t *testing.T) {
	policy := strings.Replace(policyYAML,
		"min_net_profit: \"0\"",
		"min_net_profit: \"0\"\n    profit_threshold:\n      kind: max_fixed_and_input_bps\n      fixed: {asset: weth, amount: \"0.3\"}\n      bps: 5\n    tracking_mode: fixed_candidate\n    tracking_queue_capacity: 4096",
		1,
	)
	config, err := configuration.LoadConfig(writeConfig(t, manifestYAML, topologyYAML, policy))
	if err != nil {
		t.Fatal(err)
	}
	if config.ProfitThresholdKind != "max_fixed_and_input_bps" || config.ProfitThresholdFixed.RatString() != "3/10" ||
		config.ProfitThresholdBPS != 5 || config.TrackingMode != "fixed_candidate" || config.TrackingQueueCapacity != 4096 {
		t.Fatalf("unexpected threshold/tracking configuration: %+v", config)
	}

	invalid := strings.Replace(policy, "min_net_profit: \"0\"", "min_net_profit: \"0.1\"", 1)
	if _, err := configuration.LoadConfig(writeConfig(t, manifestYAML, topologyYAML, invalid)); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("legacy and percentage thresholds were both accepted: %v", err)
	}
}

func TestConfigurationHashIgnoresYAMLFormatting(t *testing.T) {
	first, err := configuration.LoadConfig(writeConfig(t, manifestYAML, topologyYAML, policyYAML))
	if err != nil {
		t.Fatal(err)
	}
	secondTopology := strings.Replace(topologyYAML, "schema_version: 2\n", "# comment\nschema_version: 2\n\n", 1)
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

func TestLoadConfigResolvesSolanaAndMultiHopPaths(t *testing.T) {
	manifest := `schema_version: 2
topology: topology.yaml
policy: policy.yaml
active_research: route
`
	topology := `schema_version: 2
chains:
  robinhood: {kind: evm, label: Robinhood, chain_id: "4663", rpc_url_env: RH_RPC}
  solana: {kind: solana, label: Solana, chain_id: solana, http_url_env: SOL_HTTP, websocket_url_env: SOL_WS}
assets:
  asset_a: {symbol: ASSET_A}
  sol: {symbol: SOL}
  asset_b: {symbol: ASSET_B}
  usd: {symbol: USD}
tokens:
  asset_a_rh: {asset: asset_a, chain: robinhood, address: "0x0000000000000000000000000000000000000001", decimals: 18, symbol: ASSET_A}
  weth_rh: {asset: sol, chain: robinhood, address: "0x0000000000000000000000000000000000000002", decimals: 18, symbol: WETH}
  asset_b_rh: {asset: asset_b, chain: robinhood, address: "0x0000000000000000000000000000000000000003", decimals: 6, symbol: ASSET_B}
  asset_a_sol: {asset: asset_a, chain: solana, address: AssetASynthetic111111111111111111111111, decimals: 9, symbol: ASSET_A}
  sol_sol: {asset: sol, chain: solana, address: SolSynthetic111111111111111111111111111, decimals: 9, symbol: SOL}
  asset_b_sol: {asset: asset_b, chain: solana, address: AssetBSynthetic111111111111111111111111, decimals: 6, symbol: ASSET_B}
venues:
  uniswap: {kind: uniswap_v3, chain: robinhood, pool_address: "0x0000000000000000000000000000000000000004", reference_address: "0x0000000000000000000000000000000000000005"}
  meteora: {kind: meteora_dlmm, chain: solana, pool_address: MeteoraVenueSynthetic1111111111111111, reference_address: ""}
  orca: {kind: orca_whirlpool, chain: solana, pool_address: OrcaVenueSynthetic111111111111111111, reference_address: ""}
pools:
  rh_asset_a_weth: {venue: uniswap, chain: robinhood, address: "0x0000000000000000000000000000000000000004"}
  rh_weth_asset_b: {venue: uniswap, chain: robinhood, address: "0x0000000000000000000000000000000000000006"}
  sol_asset_a: {venue: meteora, chain: solana, address: MeteoraPoolSynthetic11111111111111111111}
  sol_asset_b: {venue: orca, chain: solana, address: OrcaPoolSynthetic1111111111111111111111}
paths:
  rh_path:
    chain: robinhood
    hops: [{pool: rh_asset_a_weth, token_in: asset_a_rh, token_out: weth_rh}, {pool: rh_weth_asset_b, token_in: weth_rh, token_out: asset_b_rh}]
  sol_path:
    chain: solana
    hops: [{pool: sol_asset_a, token_in: asset_a_sol, token_out: sol_sol}, {pool: sol_asset_b, token_in: sol_sol, token_out: asset_b_sol}]
markets:
  rh: {path: rh_path, base_token: asset_a_rh, quote_token: asset_b_rh}
  sol: {path: sol_path, base_token: asset_a_sol, quote_token: asset_b_sol, reference_quote: external}
quote_sources:
  external: {kind: jupiter, taker_env: PUBLIC_TAKER, slippage_bps: 50, max_accounts: 64}
price_sources:
  asset_b_usd: {base_asset: asset_b, quote_asset: usd, primary: {kind: coingecko, coin_id: usd-coin, currency: usd}, fallback: {kind: chainlink, chain: robinhood, feed_address: "0x0000000000000000000000000000000000000007"}}
`
	policy := `schema_version: 2
setups: {route_setup: {markets: [rh, sol]}}
research: {route: {run_id: route, setup: route_setup, inventory_mode: prepositioned, price_source: asset_b_usd, fixed_cost: {asset: usd, amount: "0.5"}, min_net_profit: "0", sizing: {kind: linear_range, asset: quote, min: "100", max: "5000", samples: 10}}}
`
	config, err := configuration.LoadConfig(writeConfig(t, manifest, topology, policy))
	if err != nil {
		t.Fatal(err)
	}
	if config.Chains["solana"].Kind != "solana" || config.Chains["solana"].HTTPURLEnv != "SOL_HTTP" || len(config.Markets[0].Path) != 2 || len(config.Markets[1].Path) != 2 || config.Markets[1].ReferenceQuote != "external" || config.QuoteSources["external"].Kind != "jupiter" {
		t.Fatalf("unexpected cross-chain config: %+v", config)
	}
}

func TestLoadConfigResolvesTwoRemoteMarketsAndTriggerOnlyPools(t *testing.T) {
	manifest := `schema_version: 2
topology: topology.yaml
policy: policy.yaml
active_research: remote
`
	topology := `schema_version: 2
chains:
  synthetic_sol: {kind: solana, label: Synthetic Solana, http_url_env: SYNTH_SOL_HTTP, websocket_url_env: SYNTH_SOL_WS}
  synthetic_evm: {kind: evm, label: Synthetic EVM, chain_id: "137", http_url_env: SYNTH_EVM_HTTP, websocket_url_env: SYNTH_EVM_WS}
assets:
  base: {symbol: BASE}
  quote: {symbol: QUOTE}
tokens:
  base_sol: {asset: base, chain: synthetic_sol, address: BaseSynthetic111111111111111111111111111, decimals: 9, symbol: BASE}
  quote_sol: {asset: quote, chain: synthetic_sol, address: QuoteSynthetic11111111111111111111111111, decimals: 6, symbol: QUOTE}
  base_evm: {asset: base, chain: synthetic_evm, address: "0x0000000000000000000000000000000000000001", decimals: 18, symbol: BASE}
  quote_evm: {asset: quote, chain: synthetic_evm, address: "0x0000000000000000000000000000000000000002", decimals: 6, symbol: QUOTE}
pools:
  sol_a: {chain: synthetic_sol, kind: raydium_clmm, address: PoolSynthetic111111111111111111111111111}
  sol_b: {chain: synthetic_sol, kind: orca_whirlpool, address: PoolSynthetic222222222222222222222222222}
  evm_a: {chain: synthetic_evm, kind: uniswap_v3, address: "0x0000000000000000000000000000000000000003"}
quote_sources:
  synthetic_jupiter: {kind: jupiter, quote_path: /swap/v2/order, expected_mode: manual, taker_env: SYNTH_TAKER, api_key_env: SYNTH_JUP_KEYS, slippage_bps: 5, swap_mode: ExactIn, priority_fee_lamports: 1000000, broadcast_fee_type: maxCap, use_wsol: false, client_platform: synthetic.web}
  synthetic_kyber: {kind: kyberswap, chain_slug: polygon, client_id_env: SYNTH_KYBER_ID}
markets:
  sol: {chain: synthetic_sol, base_token: base_sol, quote_token: quote_sol, quote_source: synthetic_jupiter, trigger_pools: [sol_a, sol_b]}
  evm: {chain: synthetic_evm, base_token: base_evm, quote_token: quote_evm, quote_source: synthetic_kyber, trigger_pools: [evm_a]}
`
	policy := `schema_version: 2
setups: {remote_pair: {markets: [sol, evm]}}
research:
  remote:
    run_id: synthetic-remote
    setup: remote_pair
    inventory_mode: prepositioned
    fixed_cost: {asset: quote, amount: "1"}
    min_net_profit: "1"
    sizing: {kind: fixed, asset: quote, amount: "750"}
    evaluation_mode: best_buy_opposite_sell
    idle_evaluation_interval_ms: 15000
    window_qualification: policy_qualified
    retry: {attempts: 1, delay_ms: 100}
    telegram: {enabled: true, bot_token_env: SYNTH_TELEGRAM_TOKEN, chat_id_env: SYNTH_TELEGRAM_CHAT}
`
	config, err := configuration.LoadConfig(writeConfig(t, manifest, topology, policy))
	if err != nil {
		t.Fatal(err)
	}
	if config.EvaluationMode != "best_buy_opposite_sell" || config.SizingKind != "fixed" ||
		config.MinimumSize.RatString() != "750" || config.MaximumSize.RatString() != "750" ||
		config.FixedCost.RatString() != "1" || config.MinimumNet.RatString() != "1" ||
		config.IdleEvaluationInterval.Milliseconds() != 15000 || config.WindowQualification != "policy_qualified" ||
		len(config.Markets[0].TriggerPools)+len(config.Markets[1].TriggerPools) != 3 ||
		config.Markets[0].Base.Token.Decimals != 9 || config.Markets[1].Base.Token.Decimals != 18 ||
		config.QuoteSources["synthetic_kyber"].ChainSlug != "polygon" ||
		config.QuoteSources["synthetic_jupiter"].QuotePath != "/swap/v2/order" ||
		config.QuoteSources["synthetic_jupiter"].ExpectedMode != "manual" ||
		config.QuoteSources["synthetic_jupiter"].TakerEnv != "SYNTH_TAKER" ||
		config.QuoteSources["synthetic_jupiter"].SlippageBPS != 5 ||
		config.QuoteSources["synthetic_jupiter"].SwapMode != "ExactIn" ||
		config.QuoteSources["synthetic_jupiter"].PriorityFeeLamports != 1_000_000 ||
		config.QuoteSources["synthetic_jupiter"].BroadcastFeeType != "maxCap" ||
		config.QuoteSources["synthetic_jupiter"].UseWSOL == nil ||
		*config.QuoteSources["synthetic_jupiter"].UseWSOL ||
		config.QuoteSources["synthetic_jupiter"].ClientPlatform != "synthetic.web" {
		t.Fatalf("unexpected remote configuration: %+v", config)
	}
	if config.PriceSource.Base != "quote" || config.PriceSource.Quote != "quote" {
		t.Fatalf("same-asset fixed cost should use parity: %+v", config.PriceSource)
	}
}

func TestLoadConfigResolvesSyntheticPrefundedHybridAndRejectsIncompleteValidation(t *testing.T) {
	manifest := "schema_version: 2\ntopology: topology.yaml\npolicy: policy.yaml\nactive_research: hybrid\n"
	topology := `schema_version: 2
chains:
  local: {kind: evm, label: Local, chain_id: "1", http_url_env: LOCAL_HTTP, websocket_url_env: LOCAL_WS}
  remote: {kind: evm, label: Remote, chain_id: "2", http_url_env: REMOTE_HTTP, websocket_url_env: REMOTE_WS}
assets: {base: {symbol: BASE}, quote: {symbol: QUOTE}}
tokens:
  base_local: {asset: base, chain: local, address: "0x0000000000000000000000000000000000000001", decimals: 18, symbol: BASE}
  quote_local: {asset: quote, chain: local, address: "0x0000000000000000000000000000000000000002", decimals: 6, symbol: QUOTE}
  base_remote: {asset: base, chain: remote, address: "0x0000000000000000000000000000000000000003", decimals: 18, symbol: BASE}
  quote_remote: {asset: quote, chain: remote, address: "0x0000000000000000000000000000000000000004", decimals: 18, symbol: QUOTE}
venues:
  local_v3: {kind: pancakeswap_v3, chain: local, pool_address: "0x0000000000000000000000000000000000000005"}
pools:
  local_pool: {venue: local_v3, chain: local, address: "0x0000000000000000000000000000000000000005"}
  remote_trigger: {chain: remote, kind: uniswap_v3, address: "0x0000000000000000000000000000000000000006"}
paths:
  local_path: {chain: local, hops: [{pool: local_pool, token_in: base_local, token_out: quote_local}]}
quote_sources:
  remote_aggregator: {kind: kyberswap, chain_slug: synthetic, client_id_env: SYNTH_CLIENT, slippage_bps: 10}
markets:
  local_market: {path: local_path, base_token: base_local, quote_token: quote_local}
  remote_market: {chain: remote, base_token: base_remote, quote_token: quote_remote, quote_source: remote_aggregator, trigger_pools: [remote_trigger]}
`
	policy := `schema_version: 2
setups: {hybrid_pair: {markets: [local_market, remote_market]}}
research:
  hybrid:
    run_id: synthetic-hybrid
    setup: hybrid_pair
    inventory_mode: prepositioned
    fixed_cost: {asset: quote, amount: "1"}
    min_net_profit: "0.75"
    sizing: {kind: fixed, asset: quote, amount: "500"}
    evaluation_mode: prefunded_parallel
    tracking_mode: fixed_candidate
    executable_validation:
      enabled: true
      evm_sender_env: SYNTH_SENDER
      evm_token_slots: {base_remote: {balance_slot: 1, allowance_slot: 2}, quote_remote: {balance_slot: 3, allowance_slot: 4}}
`
	config, err := configuration.LoadConfig(writeConfig(t, manifest, topology, policy))
	if err != nil {
		t.Fatal(err)
	}
	if config.EvaluationMode != "prefunded_parallel" || !config.ExecutableValidationEnabled || config.QuoteSources["remote_aggregator"].ChainSlug != "synthetic" {
		t.Fatalf("unexpected hybrid config: %+v", config)
	}
	for name, invalid := range map[string]string{
		"sender":      strings.Replace(policy, "evm_sender_env: SYNTH_SENDER", "evm_sender_env: ''", 1),
		"slots":       strings.Replace(policy, "balance_slot: 1, allowance_slot: 2", "balance_slot: 1, allowance_slot: 1", 1),
		"tracking":    strings.Replace(policy, "tracking_mode: fixed_candidate", "tracking_mode: window_reselect", 1),
		"token slots": strings.Replace(policy, ", quote_remote: {balance_slot: 3, allowance_slot: 4}", "", 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := configuration.LoadConfig(writeConfig(t, manifest, topology, invalid)); err == nil {
				t.Fatal("incomplete hybrid configuration was accepted")
			}
		})
	}
	if _, err := configuration.LoadConfig(writeConfig(t, manifest, strings.Replace(topology, "chain_slug: synthetic", "chain_slug: ''", 1), policy)); err == nil {
		t.Fatal("empty chain slug was accepted")
	}
	if _, err := configuration.LoadConfig(writeConfig(t, manifest, strings.Replace(topology, "trigger_pools: [remote_trigger]", "trigger_pools: []", 1), policy)); err == nil {
		t.Fatal("empty trigger set was accepted")
	}
}

func TestLoadConfigRejectsPrimaryAndReferenceQuoteTogether(t *testing.T) {
	topology := strings.Replace(
		topologyYAML,
		"market_b: {venue: venue_b, base_token: virtual_b, quote_token: weth_b, reference_quote: external}",
		"market_b: {venue: venue_b, base_token: virtual_b, quote_token: weth_b, quote_source: external, reference_quote: external}",
		1,
	)
	if _, err := configuration.LoadConfig(writeConfig(t, manifestYAML, topology, policyYAML)); err == nil {
		t.Fatal("market with primary and reference quote sources was accepted")
	}
}

func TestLoadConfigResolvesQuoteConversionBridgeTokenAndRejectsUnknownSelection(t *testing.T) {
	topology := strings.Replace(
		topologyYAML,
		"  virtual_b: {asset: virtual, chain: chain_b, address: \"0x0000000000000000000000000000000000000003\", decimals: 18, symbol: VIRTUAL}",
		"  virtual_b: {asset: virtual, chain: chain_b, address: \"0x0000000000000000000000000000000000000003\", decimals: 18, symbol: VIRTUAL}\n  virtual_bridge_b: {asset: virtual, chain: chain_b, address: \"0x000000000000000000000000000000000000000c\", decimals: 6, symbol: VIRTUAL}",
		1,
	)
	topology = strings.Replace(
		topology,
		"  external: {kind: jupiter, taker_env: PUBLIC_TAKER, slippage_bps: 50, max_accounts: 32}",
		"  external: {kind: jupiter, taker_env: PUBLIC_TAKER, slippage_bps: 50, max_accounts: 32}\n  bridge_fx: {kind: kyberswap, chain_slug: synthetic, client_id_env: SYNTHETIC_CLIENT}\nquote_conversions:\n  synthetic_fx: {chain: chain_b, source: bridge_fx, token_a: virtual_b, token_b: virtual_bridge_b, bridge_token: virtual_bridge_b, amount: \"500\", refresh_interval_ms: 1000, ttl_ms: 3000}",
		1,
	)

	config, err := configuration.LoadConfig(writeConfig(t, manifestYAML, topology, policyYAML))
	if err != nil {
		t.Fatal(err)
	}
	conversion := config.QuoteConversions["synthetic_fx"]
	if conversion.BridgeToken == nil || conversion.BridgeToken.Token.ID != "virtual_bridge_b" {
		t.Fatalf("unexpected bridge token: %+v", conversion.BridgeToken)
	}

	invalid := strings.Replace(topology, "bridge_token: virtual_bridge_b", "bridge_token: weth_b", 1)
	if _, err := configuration.LoadConfig(writeConfig(t, manifestYAML, invalid, policyYAML)); err == nil ||
		!strings.Contains(err.Error(), "bridge token must be token_a or token_b") {
		t.Fatalf("unknown bridge-token selection was accepted: %v", err)
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
