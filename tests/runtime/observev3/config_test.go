package observev3_test

import (
	"strings"
	"testing"

	"github.com/VarozXYZ/vernier/runtime/observev3"
)

const validConfig = `{
  "schema_version": 1,
  "network_adapter": "ethereum",
  "venue_adapter": "uniswap-v3",
  "market_id": "local-market",
  "pool_address": "0x1000000000000000000000000000000000000001",
  "quoter_v2_address": "0x2000000000000000000000000000000000000002",
  "http_url_env": "VERNIER_ETHEREUM_HTTP_URL",
  "ws_url_env": "VERNIER_ETHEREUM_WS_URL",
  "token0_id": "token-0",
  "token1_id": "token-1",
  "quote_inputs": [
    {"token_in": "token-0", "amount": "1000000"},
    {"token_in": "token-1", "amount": "1000000"}
  ],
  "max_tick_words": 64
}`

func TestConfigIsStrictAndRequiresBothDirections(t *testing.T) {
	config, err := observev3.ParseConfig([]byte(validConfig))
	if err != nil {
		t.Fatal(err)
	}
	if config.NetworkAdapter != "ethereum" || config.VenueAdapter != "uniswap-v3" || len(config.Hash) != 64 {
		t.Fatalf("unexpected parsed configuration: %+v", config)
	}
	unknown := strings.Replace(validConfig, `"schema_version": 1,`, `"schema_version": 1, "unexpected": true,`, 1)
	if _, err := observev3.ParseConfig([]byte(unknown)); err == nil {
		t.Fatal("unknown field was accepted")
	}
	oneDirection := strings.Replace(validConfig, `{"token_in": "token-1", "amount": "1000000"}`, `{"token_in": "token-0", "amount": "1000000"}`, 1)
	if _, err := observev3.ParseConfig([]byte(oneDirection)); err == nil {
		t.Fatal("configuration without both directions was accepted")
	}
}

func TestEndpointResolutionDoesNotExposeValues(t *testing.T) {
	config, err := observev3.ParseConfig([]byte(validConfig))
	if err != nil {
		t.Fatal(err)
	}
	secret := "https://user:password@example.invalid"
	_, err = config.ResolveEndpoints(func(name string) (string, bool) {
		if name == config.HTTPURLEnv {
			return secret, true
		}
		return "", false
	})
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("endpoint error leaked a configured value: %v", err)
	}
}
