// Package observev3 composes the read-only Ethereum and Uniswap V3 research
// vertical.
package observev3

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

const (
	NetworkAdapterID = "ethereum"
	VenueAdapterID   = "uniswap-v3"
)

var environmentName = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

type QuoteInput struct {
	TokenIn string `json:"token_in"`
	Amount  string `json:"amount"`
}

type Config struct {
	SchemaVersion   int          `json:"schema_version"`
	NetworkAdapter  string       `json:"network_adapter"`
	VenueAdapter    string       `json:"venue_adapter"`
	MarketID        string       `json:"market_id"`
	PoolAddress     string       `json:"pool_address"`
	QuoterV2Address string       `json:"quoter_v2_address"`
	HTTPURLEnv      string       `json:"http_url_env"`
	WSURLEnv        string       `json:"ws_url_env"`
	Token0ID        string       `json:"token0_id"`
	Token1ID        string       `json:"token1_id"`
	QuoteInputs     []QuoteInput `json:"quote_inputs"`
	MaxTickWords    int          `json:"max_tick_words"`
}

type ParsedConfig struct {
	Config
	Hash       string
	Pool       common.Address
	QuoterV2   common.Address
	ProbeInput []*big.Int
}

type Endpoints struct {
	HTTP string
	WS   string
}

type LookupEnv func(string) (string, bool)

func ParseConfig(data []byte) (ParsedConfig, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var config Config
	if err := decoder.Decode(&config); err != nil {
		return ParsedConfig{}, fmt.Errorf("decode observe-v3 configuration: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return ParsedConfig{}, err
	}
	if config.SchemaVersion != 1 {
		return ParsedConfig{}, fmt.Errorf("unsupported observe-v3 schema version %d", config.SchemaVersion)
	}
	if config.NetworkAdapter != NetworkAdapterID || config.VenueAdapter != VenueAdapterID {
		return ParsedConfig{}, fmt.Errorf("unsupported adapter composition %q + %q", config.NetworkAdapter, config.VenueAdapter)
	}
	if strings.TrimSpace(config.MarketID) == "" || strings.TrimSpace(config.Token0ID) == "" ||
		strings.TrimSpace(config.Token1ID) == "" || config.Token0ID == config.Token1ID {
		return ParsedConfig{}, fmt.Errorf("market ID and distinct token IDs are required")
	}
	if !common.IsHexAddress(config.PoolAddress) || !common.IsHexAddress(config.QuoterV2Address) {
		return ParsedConfig{}, fmt.Errorf("pool and QuoterV2 must be hexadecimal addresses")
	}
	pool := common.HexToAddress(config.PoolAddress)
	quoter := common.HexToAddress(config.QuoterV2Address)
	if pool == (common.Address{}) || quoter == (common.Address{}) {
		return ParsedConfig{}, fmt.Errorf("pool and QuoterV2 addresses cannot be zero")
	}
	if !environmentName.MatchString(config.HTTPURLEnv) || !environmentName.MatchString(config.WSURLEnv) ||
		config.HTTPURLEnv == config.WSURLEnv {
		return ParsedConfig{}, fmt.Errorf("distinct valid HTTP and WebSocket environment variable names are required")
	}
	if config.MaxTickWords == 0 {
		config.MaxTickWords = 64
	}
	if config.MaxTickWords < 1 || config.MaxTickWords > 512 {
		return ParsedConfig{}, fmt.Errorf("max_tick_words must be between 1 and 512")
	}
	if len(config.QuoteInputs) < 2 {
		return ParsedConfig{}, fmt.Errorf("quote_inputs requires both token directions")
	}
	amounts := make([]*big.Int, len(config.QuoteInputs))
	directions := map[string]bool{config.Token0ID: false, config.Token1ID: false}
	for index, input := range config.QuoteInputs {
		if _, supported := directions[input.TokenIn]; !supported {
			return ParsedConfig{}, fmt.Errorf("quote input %d has unknown token %q", index, input.TokenIn)
		}
		amount, ok := new(big.Int).SetString(input.Amount, 10)
		if !ok || amount.Sign() <= 0 || amount.BitLen() > 256 {
			return ParsedConfig{}, fmt.Errorf("quote input %d amount must be a positive uint256 decimal", index)
		}
		amounts[index] = amount
		directions[input.TokenIn] = true
	}
	if !directions[config.Token0ID] || !directions[config.Token1ID] {
		return ParsedConfig{}, fmt.Errorf("quote_inputs requires at least one probe per direction")
	}
	sum := sha256.Sum256(data)
	return ParsedConfig{
		Config: config, Hash: hex.EncodeToString(sum[:]), Pool: pool, QuoterV2: quoter, ProbeInput: amounts,
	}, nil
}

func (c ParsedConfig) ResolveEndpoints(lookup LookupEnv) (Endpoints, error) {
	if lookup == nil {
		return Endpoints{}, fmt.Errorf("environment lookup is required")
	}
	httpURL, httpFound := lookup(c.HTTPURLEnv)
	wsURL, wsFound := lookup(c.WSURLEnv)
	if !httpFound || strings.TrimSpace(httpURL) == "" {
		return Endpoints{}, fmt.Errorf("required HTTP endpoint environment variable is unset")
	}
	if !wsFound || strings.TrimSpace(wsURL) == "" {
		return Endpoints{}, fmt.Errorf("required WebSocket endpoint environment variable is unset")
	}
	return Endpoints{HTTP: httpURL, WS: wsURL}, nil
}

func requireEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return fmt.Errorf("decode observe-v3 configuration trailer: %w", err)
	}
	return fmt.Errorf("observe-v3 configuration contains multiple JSON values")
}
