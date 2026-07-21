// Package research composes the deterministic Research runtime.
package research

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"gopkg.in/yaml.v3"
)

// Fixture is the experimental schema used by the synthetic Research demo.
// It is test data, not Vernier's stable public configuration API.
type Fixture struct {
	SchemaVersion int               `yaml:"schema_version"`
	RunID         string            `yaml:"run_id"`
	FixedCost     string            `yaml:"fixed_cost"`
	Catalog       CatalogFixture    `yaml:"catalog"`
	Setup         SetupFixture      `yaml:"setup"`
	Strategies    []StrategyFixture `yaml:"strategies"`
	Feeds         []FeedFixture     `yaml:"feeds"`
}

type CatalogFixture struct {
	Chains  []ChainFixture  `yaml:"chains"`
	Assets  []AssetFixture  `yaml:"assets"`
	Tokens  []TokenFixture  `yaml:"tokens"`
	Venues  []VenueFixture  `yaml:"venues"`
	Pairs   []PairFixture   `yaml:"pairs"`
	Pools   []PoolFixture   `yaml:"pools"`
	Paths   []PathFixture   `yaml:"paths"`
	Markets []MarketFixture `yaml:"markets"`
}

type ChainFixture struct {
	ID string `yaml:"id"`
}

type AssetFixture struct {
	ID     string `yaml:"id"`
	Symbol string `yaml:"symbol"`
}

type TokenFixture struct {
	ID       string `yaml:"id"`
	Asset    string `yaml:"asset"`
	Chain    string `yaml:"chain"`
	Decimals uint8  `yaml:"decimals"`
	Symbol   string `yaml:"symbol"`
}

type VenueFixture struct {
	ID string `yaml:"id"`
}

type PairFixture struct {
	ID         string `yaml:"id"`
	BaseAsset  string `yaml:"base_asset"`
	QuoteAsset string `yaml:"quote_asset"`
}

type PoolFixture struct {
	ID      string   `yaml:"id"`
	Venue   string   `yaml:"venue"`
	Chain   string   `yaml:"chain"`
	Tokens  []string `yaml:"tokens"`
	Adapter string   `yaml:"adapter"`
}

type HopFixture struct {
	Pool     string `yaml:"pool"`
	TokenIn  string `yaml:"token_in"`
	TokenOut string `yaml:"token_out"`
}

type PathFixture struct {
	ID    string       `yaml:"id"`
	Chain string       `yaml:"chain"`
	Hops  []HopFixture `yaml:"hops"`
}

type MarketFixture struct {
	ID         string `yaml:"id"`
	Pair       string `yaml:"pair"`
	Chain      string `yaml:"chain"`
	Path       string `yaml:"path"`
	BaseToken  string `yaml:"base_token"`
	QuoteToken string `yaml:"quote_token"`
}

type SetupFixture struct {
	ID      string   `yaml:"id"`
	Pair    string   `yaml:"pair"`
	Markets []string `yaml:"markets"`
}

type StrategyFixture struct {
	ID        string   `yaml:"id"`
	Sizes     []string `yaml:"sizes"`
	Threshold string   `yaml:"threshold"`
}

type FeedFixture struct {
	Market     string             `yaml:"market"`
	Source     string             `yaml:"source"`
	Events     []EventFixture     `yaml:"events"`
	Disconnect *DisconnectFixture `yaml:"disconnect,omitempty"`
}

type EventFixture struct {
	BlockNumber          *uint64       `yaml:"block_number,omitempty"`
	Finality             string        `yaml:"finality"`
	SourceTime           string        `yaml:"source_time,omitempty"`
	ReceivedAt           string        `yaml:"received_at"`
	AppliedAt            string        `yaml:"applied_at"`
	EvaluationStartedAt  string        `yaml:"evaluation_started_at"`
	EvaluationFinishedAt string        `yaml:"evaluation_finished_at"`
	BaseReserve          string        `yaml:"base_reserve,omitempty"`
	QuoteReserve         string        `yaml:"quote_reserve,omitempty"`
	FeeBPS               uint16        `yaml:"fee_bps,omitempty"`
	SqrtPriceX96         string        `yaml:"sqrt_price_x96,omitempty"`
	Tick                 *int32        `yaml:"tick,omitempty"`
	Liquidity            string        `yaml:"liquidity,omitempty"`
	FeePips              *uint32       `yaml:"fee_pips,omitempty"`
	TickSpacing          *int32        `yaml:"tick_spacing,omitempty"`
	InitializedTicks     []TickFixture `yaml:"initialized_ticks,omitempty"`
}

type TickFixture struct {
	Index          int32  `yaml:"index"`
	LiquidityGross string `yaml:"liquidity_gross"`
	LiquidityNet   string `yaml:"liquidity_net"`
}

type DisconnectFixture struct {
	Reason               string `yaml:"reason"`
	ObservedAt           string `yaml:"observed_at"`
	EvaluationStartedAt  string `yaml:"evaluation_started_at"`
	EvaluationFinishedAt string `yaml:"evaluation_finished_at"`
}

func ParseFixture(data []byte) (Fixture, string, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var fixture Fixture
	if err := decoder.Decode(&fixture); err != nil {
		return Fixture{}, "", fmt.Errorf("decode fixture: %w", err)
	}
	if err := ensureEOF(decoder); err != nil {
		return Fixture{}, "", err
	}
	if fixture.SchemaVersion != 1 {
		return Fixture{}, "", fmt.Errorf("unsupported fixture schema version %d", fixture.SchemaVersion)
	}
	hash := sha256.Sum256(data)
	return fixture, hex.EncodeToString(hash[:]), nil
}

func ensureEOF(decoder *yaml.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("fixture contains multiple YAML documents")
		}
		return fmt.Errorf("decode trailing fixture data: %w", err)
	}
	return nil
}
