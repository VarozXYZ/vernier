package livecompare_test

import (
	"strings"
	"testing"

	"github.com/VarozXYZ/vernier/runtime/livecompare"
)

const validConfig = `{
  "schema_version": 1,
  "run_id": "live",
  "inventory_mode": "prepositioned",
  "fixed_cost_usd": "0.5",
  "sizes": ["100", "5000"],
  "robinhood": {
    "network_adapter": "robinhood",
    "venue_adapter": "uniswap-v2",
    "rpc_url_env": "ROBINHOOD_RPC",
    "pool_address": "0x0000000000000000000000000000000000000001",
    "factory_address": "0x0000000000000000000000000000000000000002",
    "router_address": "0x0000000000000000000000000000000000000003",
    "base_token_address": "0x0000000000000000000000000000000000000004",
    "quote_token_address": "0x0000000000000000000000000000000000000005",
    "fee_bps": 30
  },
  "base": {
    "network_adapter": "base",
    "venue_adapter": "aerodrome-slipstream",
    "rpc_url_env": "BASE_RPC",
    "pool_address": "0x0000000000000000000000000000000000000006",
    "factory_address": "0x0000000000000000000000000000000000000007",
    "quoter_address": "0x0000000000000000000000000000000000000008",
    "base_token_address": "0x0000000000000000000000000000000000000009",
    "quote_token_address": "0x000000000000000000000000000000000000000a",
    "max_tick_words": 16
  },
  "weth_usd_feed": "0x000000000000000000000000000000000000000b"
}`

func TestParseConfigKeepsOperationalValuesPrivateAndExact(t *testing.T) {
	config, err := livecompare.ParseConfig([]byte(validConfig))
	if err != nil {
		t.Fatal(err)
	}
	if config.FixedCost.RatString() != "1/2" || len(config.SizeValues) != 2 ||
		config.SizeValues[1].RatString() != "5000" || len(config.Hash) != 64 {
		t.Fatalf("unexpected parsed configuration: %+v", config)
	}
	endpoints, err := config.ResolveEndpoints(func(name string) (string, bool) {
		return "wss://" + strings.ToLower(name), true
	})
	if err != nil || endpoints.Robinhood == endpoints.Base {
		t.Fatalf("unexpected endpoints: %+v, %v", endpoints, err)
	}
}

func TestParseConfigRejectsUnsafeComposition(t *testing.T) {
	cases := []string{
		strings.Replace(validConfig, `"prepositioned"`, `"bridged"`, 1),
		strings.Replace(validConfig, `"sizes": ["100", "5000"]`, `"sizes": ["5000", "100"]`, 1),
		strings.Replace(validConfig, `"uniswap-v2"`, `"unknown"`, 1),
		strings.Replace(validConfig, `"schema_version": 1,`, `"schema_version": 1, "unknown": true,`, 1),
	}
	for _, data := range cases {
		if _, err := livecompare.ParseConfig([]byte(data)); err == nil {
			t.Fatalf("expected rejection for %s", data)
		}
	}
}
