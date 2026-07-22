// Package configuration loads and resolves Vernier's modular YAML.
package configuration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"gopkg.in/yaml.v3"

	"github.com/VarozXYZ/vernier/domain/market"
)

const schemaVersion = 1

var environmentName = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

type Manifest struct {
	SchemaVersion  int    `yaml:"schema_version"`
	Topology       string `yaml:"topology"`
	Policy         string `yaml:"policy"`
	ActiveResearch string `yaml:"active_research"`
}

type Topology struct {
	SchemaVersion int                          `yaml:"schema_version"`
	Chains        map[string]ChainConfig       `yaml:"chains"`
	Assets        map[string]AssetConfig       `yaml:"assets"`
	Tokens        map[string]TokenConfig       `yaml:"tokens"`
	Venues        map[string]VenueConfig       `yaml:"venues"`
	Pools         map[string]PoolConfig        `yaml:"pools"`
	Paths         map[string]PathConfig        `yaml:"paths"`
	Markets       map[string]MarketConfig      `yaml:"markets"`
	PriceSources  map[string]PriceSourceConfig `yaml:"price_sources"`
	QuoteSources  map[string]QuoteSourceConfig `yaml:"quote_sources"`
}

type Policy struct {
	SchemaVersion int                       `yaml:"schema_version"`
	Setups        map[string]SetupConfig    `yaml:"setups"`
	Research      map[string]ResearchConfig `yaml:"research"`
}

type ChainConfig struct {
	Kind             string `yaml:"kind"`
	Label            string `yaml:"label"`
	ChainID          string `yaml:"chain_id"`
	RPCURLEnv        string `yaml:"rpc_url_env"`
	HTTPURLEnv       string `yaml:"http_url_env"`
	WebSocketURLEnv  string `yaml:"websocket_url_env"`
	RPCMinIntervalMS int    `yaml:"rpc_min_interval_ms"`
}

type AssetConfig struct {
	Symbol string `yaml:"symbol"`
}

type TokenConfig struct {
	Asset    string `yaml:"asset"`
	Chain    string `yaml:"chain"`
	Address  string `yaml:"address"`
	Decimals uint8  `yaml:"decimals"`
	Symbol   string `yaml:"symbol"`
}

type VenueConfig struct {
	Kind             string `yaml:"kind"`
	Chain            string `yaml:"chain"`
	PoolAddress      string `yaml:"pool_address"`
	FactoryAddress   string `yaml:"factory_address"`
	ReferenceAddress string `yaml:"reference_address"`
	FeeBPS           uint16 `yaml:"fee_bps"`
	Stable           bool   `yaml:"stable"`
	MaxTickWords     int    `yaml:"max_tick_words"`
}

// PoolConfig separates a concrete pool address from a reusable venue
// protocol profile. Existing configurations may continue to put pool_address
// on the venue; paths should use this type instead.
type PoolConfig struct {
	Venue            string `yaml:"venue"`
	Chain            string `yaml:"chain"`
	Address          string `yaml:"address"`
	ReferenceAddress string `yaml:"reference_address"`
	FeeBPS           uint16 `yaml:"fee_bps"`
}

type PathConfig struct {
	Chain string          `yaml:"chain"`
	Hops  []PathHopConfig `yaml:"hops"`
}

type PathHopConfig struct {
	Pool     string `yaml:"pool"`
	TokenIn  string `yaml:"token_in"`
	TokenOut string `yaml:"token_out"`
}

type MarketConfig struct {
	Venue      string `yaml:"venue"`
	Path       string `yaml:"path"`
	BaseToken  string `yaml:"base_token"`
	QuoteToken string `yaml:"quote_token"`
}

type QuoteSourceConfig struct {
	Kind        string `yaml:"kind"`
	BaseURL     string `yaml:"base_url"`
	TakerEnv    string `yaml:"taker_env"`
	APIKeyEnv   string `yaml:"api_key_env"`
	SlippageBPS uint16 `yaml:"slippage_bps"`
	MaxAccounts uint16 `yaml:"max_accounts"`
}

type PriceSourceConfig struct {
	BaseAsset  string         `yaml:"base_asset"`
	QuoteAsset string         `yaml:"quote_asset"`
	Primary    ProviderConfig `yaml:"primary"`
	Fallback   ProviderConfig `yaml:"fallback"`
}

type ProviderConfig struct {
	Kind        string `yaml:"kind"`
	CoinID      string `yaml:"coin_id"`
	Currency    string `yaml:"currency"`
	APIKeyEnv   string `yaml:"api_key_env"`
	APIKeyKind  string `yaml:"api_key_kind"`
	BaseURL     string `yaml:"base_url"`
	Chain       string `yaml:"chain"`
	FeedAddress string `yaml:"feed_address"`
}

type SetupConfig struct {
	Markets []string `yaml:"markets"`
}

type ResearchConfig struct {
	RunID         string       `yaml:"run_id"`
	Setup         string       `yaml:"setup"`
	InventoryMode string       `yaml:"inventory_mode"`
	PriceSource   string       `yaml:"price_source"`
	FixedCost     AmountConfig `yaml:"fixed_cost"`
	MinNetProfit  string       `yaml:"min_net_profit"`
	Sizing        SizingConfig `yaml:"sizing"`
}

type AmountConfig struct {
	Asset  string `yaml:"asset"`
	Amount string `yaml:"amount"`
}

type SizingConfig struct {
	Kind    string `yaml:"kind"`
	Asset   string `yaml:"asset"`
	Minimum string `yaml:"min"`
	Maximum string `yaml:"max"`
	Samples int    `yaml:"samples"`
}

type ResolvedChain struct {
	ID              string
	Label           string
	Kind            string
	ChainID         *big.Int
	RPCURLEnv       string
	HTTPURLEnv      string
	WebSocketURLEnv string
	RPCMinInterval  time.Duration
}

type ResolvedToken struct {
	Token       market.Token
	Address     common.Address
	AddressText string
}

type ResolvedVenue struct {
	ID           string
	Kind         string
	Chain        string
	Pool         common.Address
	PoolText     string
	Factory      common.Address
	Reference    common.Address
	FeeBPS       uint16
	Stable       bool
	MaxTickWords int
}

type ResolvedMarket struct {
	ID    market.MarketID
	Venue ResolvedVenue
	Base  ResolvedToken
	Quote ResolvedToken
	Path  []ResolvedHop
}

type ResolvedHop struct {
	Pool  string
	Venue ResolvedVenue
	In    ResolvedToken
	Out   ResolvedToken
}

type ResolvedPriceSource struct {
	ID       market.SourceID
	Base     market.AssetID
	Quote    market.AssetID
	Primary  ProviderConfig
	Fallback ProviderConfig
}

type ParsedConfig struct {
	Hash          string
	ResearchID    string
	RunID         string
	SetupID       string
	InventoryMode string
	Assets        map[market.AssetID]market.Asset
	Chains        map[string]ResolvedChain
	Markets       [2]ResolvedMarket
	PriceSource   ResolvedPriceSource
	FixedCost     *big.Rat
	MinimumSize   *big.Rat
	MaximumSize   *big.Rat
	SizeSamples   int
	SizingAsset   string
	MinimumNet    *big.Rat
}

type LookupEnv func(string) (string, bool)

func LoadConfig(path string) (ParsedConfig, error) {
	manifestData, err := os.ReadFile(path)
	if err != nil {
		return ParsedConfig{}, fmt.Errorf("read configuration manifest: %w", err)
	}
	var manifest Manifest
	if err := decodeYAML(manifestData, &manifest); err != nil {
		return ParsedConfig{}, fmt.Errorf("decode configuration manifest: %w", err)
	}
	if manifest.SchemaVersion != schemaVersion || strings.TrimSpace(manifest.Topology) == "" ||
		strings.TrimSpace(manifest.Policy) == "" || strings.TrimSpace(manifest.ActiveResearch) == "" {
		return ParsedConfig{}, fmt.Errorf("manifest requires schema version, topology, policy, and active research")
	}
	directory := filepath.Dir(path)
	topologyData, err := os.ReadFile(filepath.Join(directory, manifest.Topology))
	if err != nil {
		return ParsedConfig{}, fmt.Errorf("read topology: %w", err)
	}
	policyData, err := os.ReadFile(filepath.Join(directory, manifest.Policy))
	if err != nil {
		return ParsedConfig{}, fmt.Errorf("read policy: %w", err)
	}
	var topology Topology
	if err := decodeYAML(topologyData, &topology); err != nil {
		return ParsedConfig{}, fmt.Errorf("decode topology: %w", err)
	}
	var policy Policy
	if err := decodeYAML(policyData, &policy); err != nil {
		return ParsedConfig{}, fmt.Errorf("decode policy: %w", err)
	}
	if topology.SchemaVersion != schemaVersion || policy.SchemaVersion != schemaVersion {
		return ParsedConfig{}, fmt.Errorf("topology and policy schema versions must be %d", schemaVersion)
	}
	return resolve(manifest, topology, policy)
}

func (c ParsedConfig) ResolveEndpoints(lookup LookupEnv) (map[string]string, error) {
	if lookup == nil {
		return nil, fmt.Errorf("environment lookup is required")
	}
	endpoints := make(map[string]string, len(c.Chains)*2)
	for id, chain := range c.Chains {
		name := chain.RPCURLEnv
		if name == "" {
			name = chain.HTTPURLEnv
		}
		value, ok := lookup(name)
		if !ok || strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("required endpoint for chain %q is unset", id)
		}
		endpoints[id] = value
		if chain.HTTPURLEnv != "" {
			value, ok = lookup(chain.HTTPURLEnv)
			if !ok || strings.TrimSpace(value) == "" {
				return nil, fmt.Errorf("required HTTP endpoint for chain %q is unset", id)
			}
			endpoints[id+".http"] = value
		}
		if chain.WebSocketURLEnv != "" {
			value, ok = lookup(chain.WebSocketURLEnv)
			if !ok || strings.TrimSpace(value) == "" {
				return nil, fmt.Errorf("required WebSocket endpoint for chain %q is unset", id)
			}
			endpoints[id+".websocket"] = value
		}
	}
	return endpoints, nil
}

func resolve(manifest Manifest, topology Topology, policy Policy) (ParsedConfig, error) {
	research, ok := policy.Research[manifest.ActiveResearch]
	if !ok || strings.TrimSpace(research.RunID) == "" || research.InventoryMode != "prepositioned" {
		return ParsedConfig{}, fmt.Errorf("active research requires a run ID and prepositioned inventory")
	}
	setup, ok := policy.Setups[research.Setup]
	if !ok || len(setup.Markets) != 2 || setup.Markets[0] == setup.Markets[1] {
		return ParsedConfig{}, fmt.Errorf("active research setup requires two distinct markets")
	}
	assets := make(map[market.AssetID]market.Asset, len(topology.Assets))
	for id, config := range topology.Assets {
		if strings.TrimSpace(id) == "" || strings.TrimSpace(config.Symbol) == "" {
			return ParsedConfig{}, fmt.Errorf("assets require IDs and symbols")
		}
		assets[market.AssetID(id)] = market.Asset{ID: market.AssetID(id), Symbol: config.Symbol}
	}
	chains := make(map[string]ResolvedChain)
	markets := [2]ResolvedMarket{}
	for index, id := range setup.Markets {
		resolved, chain, err := resolveMarket(id, topology, assets)
		if err != nil {
			return ParsedConfig{}, err
		}
		markets[index] = resolved
		chains[chain.ID] = chain
	}
	if markets[0].Base.Token.Asset != markets[1].Base.Token.Asset || markets[0].Quote.Token.Asset != markets[1].Quote.Token.Asset {
		return ParsedConfig{}, fmt.Errorf("setup markets must share base and quote assets")
	}
	priceConfig, ok := topology.PriceSources[research.PriceSource]
	if !ok || market.AssetID(priceConfig.BaseAsset) != markets[0].Quote.Token.Asset ||
		market.AssetID(priceConfig.QuoteAsset) != market.AssetID(research.FixedCost.Asset) {
		return ParsedConfig{}, fmt.Errorf("price source must convert market quote asset into fixed-cost asset")
	}
	if err := validateProvider(priceConfig.Primary, topology.Chains); err != nil {
		return ParsedConfig{}, fmt.Errorf("primary price provider: %w", err)
	}
	if err := validateProvider(priceConfig.Fallback, topology.Chains); err != nil {
		return ParsedConfig{}, fmt.Errorf("fallback price provider: %w", err)
	}
	if priceConfig.Primary.Kind != "coingecko" || priceConfig.Fallback.Kind != "chainlink" {
		return ParsedConfig{}, fmt.Errorf("price source requires CoinGecko primary and Chainlink fallback")
	}
	for _, provider := range []ProviderConfig{priceConfig.Primary, priceConfig.Fallback} {
		if provider.Chain != "" {
			chain, err := resolveChain(provider.Chain, topology.Chains[provider.Chain])
			if err != nil {
				return ParsedConfig{}, err
			}
			chains[chain.ID] = chain
		}
	}
	fixedCost, err := positiveOrZero(research.FixedCost.Amount, "fixed cost")
	if err != nil {
		return ParsedConfig{}, err
	}
	minimum, err := positive(research.Sizing.Minimum, "minimum size")
	if err != nil {
		return ParsedConfig{}, err
	}
	maximum, err := positive(research.Sizing.Maximum, "maximum size")
	sizingAsset := strings.TrimSpace(research.Sizing.Asset)
	if sizingAsset == "" {
		sizingAsset = "quote"
	}
	if sizingAsset != "base" && sizingAsset != "quote" {
		return ParsedConfig{}, fmt.Errorf("sizing asset must be base or quote")
	}
	if err != nil || maximum.Cmp(minimum) <= 0 || research.Sizing.Kind != "linear_range" || research.Sizing.Samples < 2 {
		return ParsedConfig{}, fmt.Errorf("linear sizing requires increasing positive bounds and at least two samples")
	}
	minimumNet, err := positiveOrZero(research.MinNetProfit, "minimum net profit")
	if err != nil {
		return ParsedConfig{}, err
	}
	bundle := struct {
		Manifest Manifest
		Topology Topology
		Policy   Policy
	}{manifest, topology, policy}
	canonical, err := json.Marshal(bundle)
	if err != nil {
		return ParsedConfig{}, fmt.Errorf("canonicalize configuration: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return ParsedConfig{
		Hash: hex.EncodeToString(sum[:]), ResearchID: manifest.ActiveResearch, RunID: research.RunID,
		SetupID: research.Setup, InventoryMode: research.InventoryMode, Assets: assets, Chains: chains, Markets: markets,
		PriceSource: ResolvedPriceSource{ID: market.SourceID(research.PriceSource), Base: market.AssetID(priceConfig.BaseAsset), Quote: market.AssetID(priceConfig.QuoteAsset), Primary: priceConfig.Primary, Fallback: priceConfig.Fallback},
		FixedCost:   fixedCost, MinimumSize: minimum, MaximumSize: maximum, SizeSamples: research.Sizing.Samples, SizingAsset: sizingAsset, MinimumNet: minimumNet,
	}, nil
}

func resolveMarket(id string, topology Topology, assets map[market.AssetID]market.Asset) (ResolvedMarket, ResolvedChain, error) {
	config, ok := topology.Markets[id]
	if !ok {
		return ResolvedMarket{}, ResolvedChain{}, fmt.Errorf("unknown market %q", id)
	}
	if config.Path != "" {
		path, ok := topology.Paths[config.Path]
		if !ok {
			return ResolvedMarket{}, ResolvedChain{}, fmt.Errorf("market %q references unknown path %q", id, config.Path)
		}
		resolvedPath, chain, err := resolvePath(config.Path, path, topology, assets)
		if err != nil {
			return ResolvedMarket{}, ResolvedChain{}, err
		}
		baseID, quoteID := config.BaseToken, config.QuoteToken
		if baseID == "" {
			baseID = path.Hops[0].TokenIn
		}
		if quoteID == "" {
			quoteID = path.Hops[len(path.Hops)-1].TokenOut
		}
		base, err := resolveToken(baseID, topology.Tokens[baseID], chain.ID, assets)
		if err != nil {
			return ResolvedMarket{}, ResolvedChain{}, err
		}
		quote, err := resolveToken(quoteID, topology.Tokens[quoteID], chain.ID, assets)
		if err != nil || base.Token.ID == quote.Token.ID || base.Token.Asset == quote.Token.Asset {
			return ResolvedMarket{}, ResolvedChain{}, fmt.Errorf("market %q requires distinct valid endpoints", id)
		}
		return ResolvedMarket{ID: market.MarketID(id), Venue: resolvedPath[0].Venue, Base: base, Quote: quote, Path: resolvedPath}, chain, nil
	}
	venueConfig, ok := topology.Venues[config.Venue]
	if !ok {
		return ResolvedMarket{}, ResolvedChain{}, fmt.Errorf("market %q references unknown venue", id)
	}
	chain, err := resolveChain(venueConfig.Chain, topology.Chains[venueConfig.Chain])
	if err != nil {
		return ResolvedMarket{}, ResolvedChain{}, err
	}
	base, err := resolveToken(config.BaseToken, topology.Tokens[config.BaseToken], venueConfig.Chain, assets)
	if err != nil {
		return ResolvedMarket{}, ResolvedChain{}, err
	}
	quote, err := resolveToken(config.QuoteToken, topology.Tokens[config.QuoteToken], venueConfig.Chain, assets)
	if err != nil || base.Token.ID == quote.Token.ID || base.Token.Asset == quote.Token.Asset {
		return ResolvedMarket{}, ResolvedChain{}, fmt.Errorf("market %q requires distinct valid endpoints", id)
	}
	venue, err := resolveVenue(config.Venue, venueConfig)
	if err != nil {
		return ResolvedMarket{}, ResolvedChain{}, err
	}
	return ResolvedMarket{ID: market.MarketID(id), Venue: venue, Base: base, Quote: quote, Path: []ResolvedHop{{Pool: config.Venue, Venue: venue, In: base, Out: quote}}}, chain, nil
}

func resolvePath(id string, config PathConfig, topology Topology, assets map[market.AssetID]market.Asset) ([]ResolvedHop, ResolvedChain, error) {
	if config.Chain == "" || len(config.Hops) == 0 {
		return nil, ResolvedChain{}, fmt.Errorf("path %q requires a chain and hops", id)
	}
	chain, err := resolveChain(config.Chain, topology.Chains[config.Chain])
	if err != nil {
		return nil, ResolvedChain{}, err
	}
	result := make([]ResolvedHop, 0, len(config.Hops))
	var previous market.TokenID
	for index, hop := range config.Hops {
		pool, ok := topology.Pools[hop.Pool]
		if !ok {
			return nil, ResolvedChain{}, fmt.Errorf("path %q references unknown pool %q", id, hop.Pool)
		}
		if pool.Chain != "" && pool.Chain != config.Chain {
			return nil, ResolvedChain{}, fmt.Errorf("pool %q belongs to chain %q, path uses %q", hop.Pool, pool.Chain, config.Chain)
		}
		venueConfig, ok := topology.Venues[pool.Venue]
		if !ok {
			return nil, ResolvedChain{}, fmt.Errorf("pool %q references unknown venue", hop.Pool)
		}
		venueConfig.Chain = config.Chain
		if pool.Address != "" {
			venueConfig.PoolAddress = pool.Address
		}
		if pool.ReferenceAddress != "" {
			venueConfig.ReferenceAddress = pool.ReferenceAddress
		}
		if pool.FeeBPS != 0 {
			venueConfig.FeeBPS = pool.FeeBPS
		}
		venue, err := resolveVenue(pool.Venue, venueConfig)
		if err != nil {
			return nil, ResolvedChain{}, err
		}
		in, err := resolveToken(hop.TokenIn, topology.Tokens[hop.TokenIn], config.Chain, assets)
		if err != nil {
			return nil, ResolvedChain{}, err
		}
		out, err := resolveToken(hop.TokenOut, topology.Tokens[hop.TokenOut], config.Chain, assets)
		if err != nil {
			return nil, ResolvedChain{}, err
		}
		if in.Token.ID == out.Token.ID || in.Token.Asset == out.Token.Asset {
			return nil, ResolvedChain{}, fmt.Errorf("path %q hop %d requires distinct tokens", id, index)
		}
		if previous != "" && previous != in.Token.ID {
			return nil, ResolvedChain{}, fmt.Errorf("path %q is discontinuous at hop %d", id, index)
		}
		previous = out.Token.ID
		result = append(result, ResolvedHop{Pool: hop.Pool, Venue: venue, In: in, Out: out})
	}
	return result, chain, nil
}

func resolveChain(id string, config ChainConfig) (ResolvedChain, error) {
	chainID, ok := new(big.Int).SetString(config.ChainID, 10)
	if id == "" || (config.Kind != "evm" && config.Kind != "solana") || strings.TrimSpace(config.Label) == "" || config.RPCMinIntervalMS < 0 || config.RPCMinIntervalMS > 10_000 {
		return ResolvedChain{}, fmt.Errorf("chain %q has invalid profile", id)
	}
	if config.Kind == "evm" && (!ok || chainID.Sign() <= 0) {
		return ResolvedChain{}, fmt.Errorf("chain %q has invalid EVM chain ID", id)
	}
	if config.Kind == "solana" {
		chainID = new(big.Int)
	}
	httpEnv, wsEnv := config.HTTPURLEnv, config.WebSocketURLEnv
	if config.RPCURLEnv != "" && httpEnv == "" && wsEnv == "" {
		wsEnv = config.RPCURLEnv
	}
	if config.Kind == "solana" && (httpEnv == "" || wsEnv == "") {
		return ResolvedChain{}, fmt.Errorf("solana chain %q requires separate HTTP and WebSocket endpoints", id)
	}
	for name, value := range map[string]string{"RPC": config.RPCURLEnv, "HTTP": httpEnv, "WebSocket": wsEnv} {
		if value != "" && !environmentName.MatchString(value) {
			return ResolvedChain{}, fmt.Errorf("chain %q has invalid %s endpoint environment", id, name)
		}
	}
	return ResolvedChain{ID: id, Label: config.Label, Kind: config.Kind, ChainID: chainID, RPCURLEnv: config.RPCURLEnv, HTTPURLEnv: httpEnv, WebSocketURLEnv: wsEnv, RPCMinInterval: time.Duration(config.RPCMinIntervalMS) * time.Millisecond}, nil
}

func resolveToken(id string, config TokenConfig, chain string, assets map[market.AssetID]market.Asset) (ResolvedToken, error) {
	asset := market.AssetID(config.Asset)
	tokenAddress := common.Address{}
	if id == "" || config.Chain != chain || config.Decimals > 36 || strings.TrimSpace(config.Symbol) == "" || strings.TrimSpace(config.Address) == "" {
		return ResolvedToken{}, fmt.Errorf("token %q has invalid configuration", id)
	}
	if _, ok := assets[asset]; !ok {
		return ResolvedToken{}, fmt.Errorf("token %q references unknown asset", id)
	}
	if common.IsHexAddress(config.Address) {
		var err error
		tokenAddress, err = address(config.Address)
		if err != nil {
			return ResolvedToken{}, fmt.Errorf("token %q has invalid EVM address", id)
		}
	} else if len(config.Address) < 32 {
		return ResolvedToken{}, fmt.Errorf("token %q has invalid public key", id)
	}
	return ResolvedToken{Token: market.Token{ID: market.TokenID(id), Asset: asset, Chain: market.ChainID(chain), Decimals: config.Decimals, Symbol: config.Symbol}, Address: tokenAddress, AddressText: config.Address}, nil
}

func resolveVenue(id string, config VenueConfig) (ResolvedVenue, error) {
	if config.Kind != "uniswap_v2" && config.Kind != "uniswap_v3" && config.Kind != "aerodrome_slipstream" && config.Kind != "aerodrome_volatile" && config.Kind != "meteora_dlmm" && config.Kind != "orca_whirlpool" {
		return ResolvedVenue{}, fmt.Errorf("venue %q has unsupported kind %q", id, config.Kind)
	}
	pool := common.Address{}
	poolText := strings.TrimSpace(config.PoolAddress)
	var err error
	if common.IsHexAddress(poolText) {
		pool, err = address(poolText)
	} else if config.Kind != "meteora_dlmm" && config.Kind != "orca_whirlpool" {
		err = fmt.Errorf("address is not hexadecimal")
	}
	if err != nil || poolText == "" {
		return ResolvedVenue{}, fmt.Errorf("venue %q pool: invalid address", id)
	}
	factory := common.Address{}
	if config.Kind != "uniswap_v3" && config.Kind != "meteora_dlmm" && config.Kind != "orca_whirlpool" || config.FactoryAddress != "" {
		factory, err = address(config.FactoryAddress)
		if err != nil {
			return ResolvedVenue{}, fmt.Errorf("venue %q factory: %w", id, err)
		}
	}
	reference := common.Address{}
	if config.ReferenceAddress != "" {
		reference, err = address(config.ReferenceAddress)
		if err != nil {
			return ResolvedVenue{}, fmt.Errorf("venue %q reference: %w", id, err)
		}
	}
	if (config.Kind == "uniswap_v2" || config.Kind == "aerodrome_volatile") && (config.FeeBPS == 0 || config.FeeBPS >= 10_000) {
		return ResolvedVenue{}, fmt.Errorf("venue %q requires a valid fee", id)
	}
	if config.Kind != "aerodrome_volatile" && config.Stable {
		return ResolvedVenue{}, fmt.Errorf("venue %q stable flag is only valid for Aerodrome volatile profiles", id)
	}
	if config.Kind == "aerodrome_volatile" && config.Stable {
		return ResolvedVenue{}, fmt.Errorf("venue %q is volatile and cannot set stable: true", id)
	}
	if config.Kind == "uniswap_v3" || config.Kind == "aerodrome_slipstream" || config.Kind == "orca_whirlpool" {
		if config.MaxTickWords == 0 {
			config.MaxTickWords = 64
		}
		if config.MaxTickWords < 1 || config.MaxTickWords > 512 {
			return ResolvedVenue{}, fmt.Errorf("venue %q has invalid tick coverage", id)
		}
	}
	return ResolvedVenue{ID: id, Kind: config.Kind, Chain: config.Chain, Pool: pool, PoolText: poolText, Factory: factory, Reference: reference, FeeBPS: config.FeeBPS, Stable: config.Stable, MaxTickWords: config.MaxTickWords}, nil
}

func validateProvider(config ProviderConfig, chains map[string]ChainConfig) error {
	switch config.Kind {
	case "coingecko":
		if strings.TrimSpace(config.CoinID) == "" || strings.TrimSpace(config.Currency) == "" ||
			config.APIKeyEnv != "" && !environmentName.MatchString(config.APIKeyEnv) ||
			config.APIKeyKind != "" && config.APIKeyKind != "demo" && config.APIKeyKind != "pro" {
			return fmt.Errorf("invalid CoinGecko provider")
		}
	case "chainlink":
		if _, ok := chains[config.Chain]; !ok {
			return fmt.Errorf("unknown Chainlink chain %q", config.Chain)
		}
		if _, err := address(config.FeedAddress); err != nil {
			return fmt.Errorf("invalid Chainlink feed")
		}
	default:
		return fmt.Errorf("unsupported provider kind %q", config.Kind)
	}
	return nil
}

func decodeYAML(data []byte, target any) error {
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	decoder.KnownFields(true)
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err == nil {
		return fmt.Errorf("multiple YAML documents are not allowed")
	} else if err != io.EOF {
		return fmt.Errorf("decode YAML trailer: %w", err)
	}
	return nil
}

func address(text string) (common.Address, error) {
	if !common.IsHexAddress(text) {
		return common.Address{}, fmt.Errorf("address is not hexadecimal")
	}
	value := common.HexToAddress(text)
	if value == (common.Address{}) {
		return common.Address{}, fmt.Errorf("address cannot be zero")
	}
	return value, nil
}

func positive(text, name string) (*big.Rat, error) {
	value, ok := new(big.Rat).SetString(text)
	if !ok || value.Sign() <= 0 {
		return nil, fmt.Errorf("%s must be positive", name)
	}
	return value, nil
}

func positiveOrZero(text, name string) (*big.Rat, error) {
	value, ok := new(big.Rat).SetString(text)
	if !ok || value.Sign() < 0 {
		return nil, fmt.Errorf("%s must be non-negative", name)
	}
	return value, nil
}
