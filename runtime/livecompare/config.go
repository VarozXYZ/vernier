// Package livecompare composes a read-only, point-in-time comparison of a
// canonical Uniswap V2 market and an Aerodrome Slipstream market.
package livecompare

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
	RobinhoodNetworkAdapter = "robinhood"
	RobinhoodVenueAdapter   = "uniswap-v2"
	BaseNetworkAdapter      = "base"
	BaseVenueAdapter        = "aerodrome-slipstream"
)

var environmentName = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

type Config struct {
	SchemaVersion int            `json:"schema_version"`
	RunID         string         `json:"run_id"`
	InventoryMode string         `json:"inventory_mode"`
	FixedCostUSD  string         `json:"fixed_cost_usd"`
	Sizes         []string       `json:"sizes"`
	Robinhood     V2MarketConfig `json:"robinhood"`
	Base          CLMarketConfig `json:"base"`
	WETHUSDFeed   string         `json:"weth_usd_feed"`
}

type V2MarketConfig struct {
	NetworkAdapter    string `json:"network_adapter"`
	VenueAdapter      string `json:"venue_adapter"`
	RPCURLEnv         string `json:"rpc_url_env"`
	PoolAddress       string `json:"pool_address"`
	FactoryAddress    string `json:"factory_address"`
	RouterAddress     string `json:"router_address"`
	BaseTokenAddress  string `json:"base_token_address"`
	QuoteTokenAddress string `json:"quote_token_address"`
	FeeBPS            uint16 `json:"fee_bps"`
	RPCMinIntervalMS  int    `json:"rpc_min_interval_ms"`
}

type CLMarketConfig struct {
	NetworkAdapter    string `json:"network_adapter"`
	VenueAdapter      string `json:"venue_adapter"`
	RPCURLEnv         string `json:"rpc_url_env"`
	PoolAddress       string `json:"pool_address"`
	FactoryAddress    string `json:"factory_address"`
	QuoterAddress     string `json:"quoter_address"`
	BaseTokenAddress  string `json:"base_token_address"`
	QuoteTokenAddress string `json:"quote_token_address"`
	MaxTickWords      int    `json:"max_tick_words"`
	RPCMinIntervalMS  int    `json:"rpc_min_interval_ms"`
}

type ParsedConfig struct {
	Config
	Hash string

	FixedCost  *big.Rat
	SizeValues []*big.Rat

	RobinhoodPool       common.Address
	RobinhoodFactory    common.Address
	RobinhoodRouter     common.Address
	RobinhoodBaseToken  common.Address
	RobinhoodQuoteToken common.Address

	BasePool       common.Address
	BaseFactory    common.Address
	BaseQuoter     common.Address
	BaseBaseToken  common.Address
	BaseQuoteToken common.Address
	PriceFeed      common.Address
}

type Endpoints struct {
	Robinhood string
	Base      string
}

type LookupEnv func(string) (string, bool)

func ParseConfig(data []byte) (ParsedConfig, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var config Config
	if err := decoder.Decode(&config); err != nil {
		return ParsedConfig{}, fmt.Errorf("decode live comparison configuration: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return ParsedConfig{}, err
	}
	if config.SchemaVersion != 1 {
		return ParsedConfig{}, fmt.Errorf("unsupported live comparison schema version %d", config.SchemaVersion)
	}
	if strings.TrimSpace(config.RunID) == "" || config.InventoryMode != "prepositioned" {
		return ParsedConfig{}, fmt.Errorf("run_id and prepositioned inventory mode are required")
	}
	if config.Robinhood.NetworkAdapter != RobinhoodNetworkAdapter ||
		config.Robinhood.VenueAdapter != RobinhoodVenueAdapter ||
		config.Base.NetworkAdapter != BaseNetworkAdapter ||
		config.Base.VenueAdapter != BaseVenueAdapter {
		return ParsedConfig{}, fmt.Errorf("unsupported live adapter composition")
	}
	if !environmentName.MatchString(config.Robinhood.RPCURLEnv) ||
		!environmentName.MatchString(config.Base.RPCURLEnv) ||
		config.Robinhood.RPCURLEnv == config.Base.RPCURLEnv {
		return ParsedConfig{}, fmt.Errorf("distinct valid RPC environment variable names are required")
	}
	fixedCost, ok := new(big.Rat).SetString(config.FixedCostUSD)
	if !ok || fixedCost.Sign() < 0 {
		return ParsedConfig{}, fmt.Errorf("fixed_cost_usd must be non-negative")
	}
	if len(config.Sizes) == 0 {
		return ParsedConfig{}, fmt.Errorf("at least one positive VIRTUAL size is required")
	}
	sizes := make([]*big.Rat, len(config.Sizes))
	var previous *big.Rat
	for index, text := range config.Sizes {
		value, valid := new(big.Rat).SetString(text)
		if !valid || value.Sign() <= 0 || previous != nil && value.Cmp(previous) <= 0 {
			return ParsedConfig{}, fmt.Errorf("sizes must be strictly increasing positive quantities")
		}
		sizes[index] = value
		previous = value
	}
	if config.Robinhood.FeeBPS == 0 || config.Robinhood.FeeBPS >= 10_000 {
		return ParsedConfig{}, fmt.Errorf("robinhood fee_bps must be between 1 and 9999")
	}
	if config.Robinhood.RPCMinIntervalMS < 0 || config.Robinhood.RPCMinIntervalMS > 10_000 ||
		config.Base.RPCMinIntervalMS < 0 || config.Base.RPCMinIntervalMS > 10_000 {
		return ParsedConfig{}, fmt.Errorf("rpc minimum intervals must be between 0 and 10000 milliseconds")
	}
	if config.Base.MaxTickWords == 0 {
		config.Base.MaxTickWords = 64
	}
	if config.Base.MaxTickWords < 1 || config.Base.MaxTickWords > 512 {
		return ParsedConfig{}, fmt.Errorf("base max_tick_words must be between 1 and 512")
	}

	addresses := []string{
		config.Robinhood.PoolAddress, config.Robinhood.FactoryAddress, config.Robinhood.RouterAddress,
		config.Robinhood.BaseTokenAddress, config.Robinhood.QuoteTokenAddress,
		config.Base.PoolAddress, config.Base.FactoryAddress, config.Base.QuoterAddress,
		config.Base.BaseTokenAddress, config.Base.QuoteTokenAddress, config.WETHUSDFeed,
	}
	parsed := make([]common.Address, len(addresses))
	for index, value := range addresses {
		if !common.IsHexAddress(value) {
			return ParsedConfig{}, fmt.Errorf("configuration address %d is not hexadecimal", index)
		}
		parsed[index] = common.HexToAddress(value)
		if parsed[index] == (common.Address{}) {
			return ParsedConfig{}, fmt.Errorf("configuration address %d cannot be zero", index)
		}
	}
	if parsed[3] == parsed[4] || parsed[8] == parsed[9] {
		return ParsedConfig{}, fmt.Errorf("market endpoints must be distinct")
	}
	sum := sha256.Sum256(data)
	return ParsedConfig{
		Config: config, Hash: hex.EncodeToString(sum[:]), FixedCost: fixedCost, SizeValues: sizes,
		RobinhoodPool: parsed[0], RobinhoodFactory: parsed[1], RobinhoodRouter: parsed[2],
		RobinhoodBaseToken: parsed[3], RobinhoodQuoteToken: parsed[4],
		BasePool: parsed[5], BaseFactory: parsed[6], BaseQuoter: parsed[7],
		BaseBaseToken: parsed[8], BaseQuoteToken: parsed[9], PriceFeed: parsed[10],
	}, nil
}

func (c ParsedConfig) ResolveEndpoints(lookup LookupEnv) (Endpoints, error) {
	if lookup == nil {
		return Endpoints{}, fmt.Errorf("environment lookup is required")
	}
	robinhood, robinhoodOK := lookup(c.Robinhood.RPCURLEnv)
	base, baseOK := lookup(c.Base.RPCURLEnv)
	if !robinhoodOK || strings.TrimSpace(robinhood) == "" {
		return Endpoints{}, fmt.Errorf("required Robinhood RPC endpoint is unset")
	}
	if !baseOK || strings.TrimSpace(base) == "" {
		return Endpoints{}, fmt.Errorf("required Base RPC endpoint is unset")
	}
	return Endpoints{Robinhood: robinhood, Base: base}, nil
}

func requireEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return fmt.Errorf("decode live comparison configuration trailer: %w", err)
	}
	return fmt.Errorf("live comparison configuration contains multiple JSON values")
}
