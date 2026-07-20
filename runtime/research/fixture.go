// Package research composes the deterministic Research runtime.
package research

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
)

// Fixture is the experimental schema used by the synthetic Research demo.
// It is test data, not Vernier's stable public configuration API.
type Fixture struct {
	SchemaVersion  int               `json:"schema_version"`
	RunID          string            `json:"run_id"`
	MaxSnapshotAge string            `json:"max_snapshot_age"`
	FixedCost      string            `json:"fixed_cost"`
	Catalog        CatalogFixture    `json:"catalog"`
	Setup          SetupFixture      `json:"setup"`
	Strategies     []StrategyFixture `json:"strategies"`
	Feeds          []FeedFixture     `json:"feeds"`
}

type CatalogFixture struct {
	Chains  []ChainFixture  `json:"chains"`
	Assets  []AssetFixture  `json:"assets"`
	Tokens  []TokenFixture  `json:"tokens"`
	Venues  []VenueFixture  `json:"venues"`
	Pairs   []PairFixture   `json:"pairs"`
	Pools   []PoolFixture   `json:"pools"`
	Paths   []PathFixture   `json:"paths"`
	Markets []MarketFixture `json:"markets"`
}

type ChainFixture struct {
	ID string `json:"id"`
}

type AssetFixture struct {
	ID     string `json:"id"`
	Symbol string `json:"symbol"`
}

type TokenFixture struct {
	ID       string `json:"id"`
	Asset    string `json:"asset"`
	Chain    string `json:"chain"`
	Decimals uint8  `json:"decimals"`
	Symbol   string `json:"symbol"`
}

type VenueFixture struct {
	ID string `json:"id"`
}

type PairFixture struct {
	ID         string `json:"id"`
	BaseAsset  string `json:"base_asset"`
	QuoteAsset string `json:"quote_asset"`
}

type PoolFixture struct {
	ID      string   `json:"id"`
	Venue   string   `json:"venue"`
	Chain   string   `json:"chain"`
	Tokens  []string `json:"tokens"`
	Adapter string   `json:"adapter"`
}

type HopFixture struct {
	Pool     string `json:"pool"`
	TokenIn  string `json:"token_in"`
	TokenOut string `json:"token_out"`
}

type PathFixture struct {
	ID    string       `json:"id"`
	Chain string       `json:"chain"`
	Hops  []HopFixture `json:"hops"`
}

type MarketFixture struct {
	ID         string `json:"id"`
	Pair       string `json:"pair"`
	Chain      string `json:"chain"`
	Path       string `json:"path"`
	BaseToken  string `json:"base_token"`
	QuoteToken string `json:"quote_token"`
}

type SetupFixture struct {
	ID      string   `json:"id"`
	Pair    string   `json:"pair"`
	Markets []string `json:"markets"`
}

type StrategyFixture struct {
	ID        string   `json:"id"`
	Sizes     []string `json:"sizes"`
	Threshold string   `json:"threshold"`
}

type FeedFixture struct {
	Market string         `json:"market"`
	Source string         `json:"source"`
	Events []EventFixture `json:"events"`
}

type EventFixture struct {
	Sequence             uint64 `json:"sequence"`
	Finality             string `json:"finality"`
	SourceTime           string `json:"source_time,omitempty"`
	ReceivedAt           string `json:"received_at"`
	AppliedAt            string `json:"applied_at"`
	EvaluationStartedAt  string `json:"evaluation_started_at"`
	EvaluationFinishedAt string `json:"evaluation_finished_at"`
	BaseReserve          string `json:"base_reserve"`
	QuoteReserve         string `json:"quote_reserve"`
	FeeBPS               uint16 `json:"fee_bps"`
}

func ParseFixture(data []byte) (Fixture, string, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
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

func ensureEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("fixture contains multiple JSON values")
		}
		return fmt.Errorf("decode trailing fixture data: %w", err)
	}
	return nil
}
