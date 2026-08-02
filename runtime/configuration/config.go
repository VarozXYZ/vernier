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

const schemaVersion = 2

var environmentName = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

type Manifest struct {
	SchemaVersion  int    `yaml:"schema_version"`
	Topology       string `yaml:"topology"`
	Policy         string `yaml:"policy"`
	ActiveResearch string `yaml:"active_research"`
	ActiveLive     string `yaml:"active_live"`
}

type Topology struct {
	SchemaVersion   int                             `yaml:"schema_version"`
	Chains          map[string]ChainConfig          `yaml:"chains"`
	Assets          map[string]AssetConfig          `yaml:"assets"`
	Tokens          map[string]TokenConfig          `yaml:"tokens"`
	Venues          map[string]VenueConfig          `yaml:"venues"`
	Pools           map[string]PoolConfig           `yaml:"pools"`
	Paths           map[string]PathConfig           `yaml:"paths"`
	Markets         map[string]MarketConfig         `yaml:"markets"`
	PriceSources    map[string]PriceSourceConfig    `yaml:"price_sources"`
	QuoteSources    map[string]QuoteSourceConfig    `yaml:"quote_sources"`
	TransferSources map[string]TransferSourceConfig `yaml:"transfer_sources"`
}

type Policy struct {
	SchemaVersion     int                               `yaml:"schema_version"`
	Setups            map[string]SetupConfig            `yaml:"setups"`
	Research          map[string]ResearchConfig         `yaml:"research"`
	ExecutionPolicies map[string]ExecutionPolicyConfig  `yaml:"execution_policies"`
	InventoryProfiles map[string]InventoryProfileConfig `yaml:"inventory_profiles"`
	Live              map[string]LiveConfig             `yaml:"live"`
}

type ExecutionPolicyConfig struct {
	Kind                string `yaml:"kind"`
	ExitPolicy          string `yaml:"exit_policy"`
	InventoryRestore    string `yaml:"inventory_restore"`
	BaseTransferSource  string `yaml:"base_transfer_source"`
	QuoteTransferSource string `yaml:"quote_transfer_source"`
}

type InventoryProfileConfig struct {
	Kind     string                   `yaml:"kind"`
	Balances []InventoryBalanceConfig `yaml:"balances"`
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
	Kind             string `yaml:"kind"`
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
	Chain          string   `yaml:"chain"`
	Venue          string   `yaml:"venue"`
	Path           string   `yaml:"path"`
	BaseToken      string   `yaml:"base_token"`
	QuoteToken     string   `yaml:"quote_token"`
	QuoteSource    string   `yaml:"quote_source"`
	TriggerPools   []string `yaml:"trigger_pools"`
	ReferenceQuote string   `yaml:"reference_quote"`
}

type QuoteSourceConfig struct {
	Kind                string `yaml:"kind"`
	BaseURL             string `yaml:"base_url"`
	QuotePath           string `yaml:"quote_path"`
	ExpectedMode        string `yaml:"expected_mode"`
	TakerEnv            string `yaml:"taker_env"`
	APIKeyEnv           string `yaml:"api_key_env"`
	ClientIDEnv         string `yaml:"client_id_env"`
	ChainSlug           string `yaml:"chain_slug"`
	SlippageBPS         uint16 `yaml:"slippage_bps"`
	MaxAccounts         uint16 `yaml:"max_accounts"`
	SwapMode            string `yaml:"swap_mode"`
	PriorityFeeLamports uint64 `yaml:"priority_fee_lamports"`
	BroadcastFeeType    string `yaml:"broadcast_fee_type"`
	UseWSOL             *bool  `yaml:"use_wsol"`
	ExcludeDexes        string `yaml:"exclude_dexes"`
	ExcludeRouters      string `yaml:"exclude_routers"`
	ClientPlatform      string `yaml:"client_platform"`
}

// TransferSourceConfig selects a compiled transfer adapter and supplies only
// its external configuration. The resolver deliberately does not enumerate
// provider kinds: composition owns the registry of capabilities available in
// a particular binary.
type TransferSourceConfig struct {
	Kind            string `yaml:"kind"`
	BaseURL         string `yaml:"base_url"`
	Profile         string `yaml:"profile"`
	APIKeyEnv       string `yaml:"api_key_env"`
	IntegratorIDEnv string `yaml:"integrator_id_env"`
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
	RunID                    string                `yaml:"run_id"`
	Setup                    string                `yaml:"setup"`
	InventoryMode            string                `yaml:"inventory_mode"`
	PriceSource              string                `yaml:"price_source"`
	FixedCost                AmountConfig          `yaml:"fixed_cost"`
	MinNetProfit             string                `yaml:"min_net_profit"`
	ProfitThreshold          ProfitThresholdConfig `yaml:"profit_threshold"`
	Sizing                   SizingConfig          `yaml:"sizing"`
	EvaluationMode           string                `yaml:"evaluation_mode"`
	TrackingMode             string                `yaml:"tracking_mode"`
	TrackingQueueCapacity    int                   `yaml:"tracking_queue_capacity"`
	IdleEvaluationIntervalMS int                   `yaml:"idle_evaluation_interval_ms"`
	WindowQualification      string                `yaml:"window_qualification"`
	Retry                    RetryConfig           `yaml:"retry"`
	Telegram                 TelegramConfig        `yaml:"telegram"`
	Simulation               SimulationConfig      `yaml:"simulation"`
}

// SimulationConfig enables post-qualification, read-only transaction
// simulations. It contains only environment-variable names and public
// policy; balances, keys and addresses remain outside tracked YAML.
type SimulationConfig struct {
	Enabled            bool                            `yaml:"enabled"`
	IntervalMS         int                             `yaml:"interval_ms"`
	SolanaOwnerEnv     string                          `yaml:"solana_owner_env"`
	EVMOwnerEnv        string                          `yaml:"evm_owner_env"`
	EVMRouterEnv       string                          `yaml:"evm_router_env"`
	EVMBalanceSlot     uint64                          `yaml:"evm_balance_slot"`
	EVMAllowanceSlot   uint64                          `yaml:"evm_allowance_slot"`
	EVMGasLimit        uint64                          `yaml:"evm_gas_limit"`
	SolanaComputeLimit uint32                          `yaml:"solana_compute_limit"`
	EVMTokenSlots      map[string]SimulationTokenSlots `yaml:"evm_token_slots"`
}

type SimulationTokenSlots struct {
	BalanceSlot   uint64 `yaml:"balance_slot"`
	AllowanceSlot uint64 `yaml:"allowance_slot"`
}

type ProfitThresholdConfig struct {
	Kind  string       `yaml:"kind"`
	Fixed AmountConfig `yaml:"fixed"`
	BPS   uint16       `yaml:"bps"`
}

type RetryConfig struct {
	Attempts int `yaml:"attempts"`
	DelayMS  int `yaml:"delay_ms"`
}

type TelegramConfig struct {
	Enabled     bool   `yaml:"enabled"`
	BotTokenEnv string `yaml:"bot_token_env"`
	ChatIDEnv   string `yaml:"chat_id_env"`
}

type LiveConfig struct {
	RunID                         string                       `yaml:"run_id"`
	Setup                         string                       `yaml:"setup"`
	RunTier                       string                       `yaml:"run_tier"`
	ExecutionPolicy               string                       `yaml:"execution_policy"`
	InventoryProfile              string                       `yaml:"inventory_profile"`
	InventoryMode                 string                       `yaml:"inventory_mode"`
	ExecutionMode                 string                       `yaml:"execution_mode"`
	Notional                      AmountConfig                 `yaml:"notional"`
	CanaryInput                   AmountConfig                 `yaml:"canary_input"`
	ExecutionInput                AmountConfig                 `yaml:"execution_input"`
	MaxOperationsPerRun           int                          `yaml:"max_operations_per_run"`
	BaseTransferSource            string                       `yaml:"base_transfer_source"`
	QuoteTransferSource           string                       `yaml:"quote_transfer_source"`
	BaseBridgeProvider            string                       `yaml:"base_bridge_provider"`
	QuoteBridgeProvider           string                       `yaml:"quote_bridge_provider"`
	BaseBridgeProfile             string                       `yaml:"base_bridge_profile"`
	ConfirmationTimeoutSeconds    int                          `yaml:"confirmation_timeout_seconds"`
	ExecutionCost                 AmountConfig                 `yaml:"execution_cost"`
	MaxExecutionCost              AmountConfig                 `yaml:"max_execution_cost"`
	MaxBaseExposure               AmountConfig                 `yaml:"max_base_exposure"`
	MinNetProfit                  AmountConfig                 `yaml:"min_net_profit"`
	ReturnBridgeSafetyMargin      AmountConfig                 `yaml:"return_bridge_safety_margin"`
	FeeCacheMaxAgeMS              int                          `yaml:"fee_cache_max_age_ms"`
	CostRefreshIntervalMS         int                          `yaml:"cost_refresh_interval_ms"`
	CostCacheTTLMS                int                          `yaml:"cost_cache_ttl_ms"`
	CostCalibrationStore          string                       `yaml:"cost_calibration_store"`
	QuoteTransferCalibrationStore string                       `yaml:"quote_transfer_calibration_store"`
	AcrossCostCalibrationStore    string                       `yaml:"across_cost_calibration_store"`
	SlippageBPS                   uint16                       `yaml:"slippage_bps"`
	TipLamports                   string                       `yaml:"tip_lamports"`
	ComputeUnitPricePercentile    string                       `yaml:"compute_unit_price_percentile"`
	ComputeUnitLimit              uint32                       `yaml:"compute_unit_limit"`
	MaxPriorityFeeLamports        string                       `yaml:"max_priority_fee_lamports"`
	BlockhashSlotsToExpiry        uint16                       `yaml:"blockhash_slots_to_expiry"`
	BuildToBroadcastTimeoutMS     int                          `yaml:"build_to_broadcast_timeout_ms"`
	EVMDeadlineSeconds            int                          `yaml:"evm_deadline_seconds"`
	EVMGas                        EVMGasConfig                 `yaml:"evm_gas"`
	DynamicSlippage               DynamicSlippageConfig        `yaml:"dynamic_slippage"`
	ExitValidationAttempts        int                          `yaml:"exit_validation_attempts"`
	ExitValidationRetryDelayMS    int                          `yaml:"exit_validation_retry_delay_ms"`
	OperationalStore              OperationalStoreConfig       `yaml:"operational_store"`
	Accounts                      map[string]LiveAccountConfig `yaml:"accounts"`
	Inventory                     []InventoryBalanceConfig     `yaml:"inventory"`
	BalanceTracking               BalanceTrackingConfig        `yaml:"balance_tracking"`
	GasRefuel                     GasRefuelConfig              `yaml:"gas_refuel"`
}

type DynamicSlippageConfig struct {
	Enabled bool   `yaml:"enabled"`
	MaxBPS  uint16 `yaml:"max_bps"`
}

type EVMGasConfig struct {
	ExecutionMode           string `yaml:"execution_mode"`
	ExecutionFixedLimit     uint64 `yaml:"execution_fixed_limit"`
	EstimationMultiplierBPS uint64 `yaml:"estimation_multiplier_bps"`
	CostMode                string `yaml:"cost_mode"`
	CostFixedLimit          uint64 `yaml:"cost_fixed_limit"`
}

type GasRefuelConfig struct {
	Enabled         bool          `yaml:"enabled"`
	ThresholdUSD    string        `yaml:"threshold_usd"`
	TargetUSD       string        `yaml:"target_usd"`
	PollSeconds     int           `yaml:"poll_seconds"`
	CooldownSeconds int           `yaml:"cooldown_seconds"`
	SlippageBPS     uint16        `yaml:"slippage_bps"`
	MaxUSDC         string        `yaml:"max_usdc"`
	EVMGas          *EVMGasConfig `yaml:"evm_gas"`
}

type BalanceTrackingConfig struct {
	PollSeconds          int `yaml:"poll_seconds"`
	AlertIntervalSeconds int `yaml:"alert_interval_seconds"`
}

type OperationalStoreConfig struct {
	Path        string `yaml:"path"`
	Synchronous string `yaml:"synchronous"`
}

type LiveAccountConfig struct {
	ID                         string                    `yaml:"id"`
	PublicAddressEnv           string                    `yaml:"public_address_env"`
	SignerEnv                  string                    `yaml:"signer_env"`
	SellPreflightAddressEnv    string                    `yaml:"sell_preflight_address_env"`
	SellPreflightStateOverride *ERC20StateOverrideConfig `yaml:"sell_preflight_state_override"`
	SenderURLEnv               string                    `yaml:"sender_url_env"`
	FanoutRPCURLEnv            string                    `yaml:"fanout_rpc_urls_env"`
	ContractAddressEnv         string                    `yaml:"contract_address_env"`
}

type ERC20StateOverrideConfig struct {
	BalanceSlot   uint64 `yaml:"balance_slot"`
	AllowanceSlot uint64 `yaml:"allowance_slot"`
}

type InventoryBalanceConfig struct {
	Chain         string `yaml:"chain"`
	Account       string `yaml:"account"`
	Token         string `yaml:"token"`
	Amount        string `yaml:"amount"`
	AllocationCap string `yaml:"allocation_cap"`
	Target        string `yaml:"target"`
	Buffer        string `yaml:"buffer"`
}

type AmountConfig struct {
	Asset  string `yaml:"asset"`
	Amount string `yaml:"amount"`
}

type SizingConfig struct {
	Kind    string `yaml:"kind"`
	Asset   string `yaml:"asset"`
	Amount  string `yaml:"amount"`
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
	ID             market.MarketID
	Chain          string
	Venue          ResolvedVenue
	Base           ResolvedToken
	Quote          ResolvedToken
	Path           []ResolvedHop
	QuoteSource    string
	TriggerPools   []ResolvedTriggerPool
	ReferenceQuote string
}

type ResolvedHop struct {
	Pool  string
	Venue ResolvedVenue
	In    ResolvedToken
	Out   ResolvedToken
}

// ResolvedTriggerPool is a configured event source for a remotely quoted
// market. It intentionally carries no protocol state: activity at the pool
// only advances the market's causal quote generation.
type ResolvedTriggerPool struct {
	ID      string
	Chain   string
	Kind    string
	Address string
}

type ResolvedPriceSource struct {
	ID       market.SourceID
	Base     market.AssetID
	Quote    market.AssetID
	Primary  ProviderConfig
	Fallback ProviderConfig
}

type ResolvedQuoteSource struct {
	ID                  string
	Kind                string
	BaseURL             string
	QuotePath           string
	ExpectedMode        string
	TakerEnv            string
	APIKeyEnv           string
	ClientIDEnv         string
	ChainSlug           string
	SlippageBPS         uint16
	MaxAccounts         uint16
	SwapMode            string
	PriorityFeeLamports uint64
	BroadcastFeeType    string
	UseWSOL             *bool
	ExcludeDexes        string
	ExcludeRouters      string
	ClientPlatform      string
}

type ResolvedTransferSource struct {
	ID              string
	Kind            string
	BaseURL         string
	Profile         string
	APIKeyEnv       string
	IntegratorIDEnv string
}

type ParsedConfig struct {
	Hash                         string
	ResearchID                   string
	RunID                        string
	SetupID                      string
	InventoryMode                string
	Assets                       map[market.AssetID]market.Asset
	Chains                       map[string]ResolvedChain
	Markets                      [2]ResolvedMarket
	PriceSource                  ResolvedPriceSource
	QuoteSources                 map[string]ResolvedQuoteSource
	FixedCost                    *big.Rat
	SizingKind                   string
	MinimumSize                  *big.Rat
	MaximumSize                  *big.Rat
	SizeSamples                  int
	SizingAsset                  string
	MinimumNet                   *big.Rat
	ProfitThresholdKind          string
	ProfitThresholdFixed         *big.Rat
	ProfitThresholdBPS           uint16
	EvaluationMode               string
	TrackingMode                 string
	TrackingQueueCapacity        int
	IdleEvaluationInterval       time.Duration
	WindowQualification          string
	RetryAttempts                int
	RetryDelay                   time.Duration
	TelegramEnabled              bool
	TelegramBotTokenEnv          string
	TelegramChatIDEnv            string
	SimulationEnabled            bool
	SimulationInterval           time.Duration
	SimulationSolanaOwnerEnv     string
	SimulationEVMOwnerEnv        string
	SimulationEVMRouterEnv       string
	SimulationEVMBalanceSlot     uint64
	SimulationEVMAllowanceSlot   uint64
	SimulationEVMGasLimit        uint64
	SimulationSolanaComputeLimit uint32
	SimulationEVMTokenSlots      map[market.TokenID]SimulationTokenSlots
}

type ResolvedLiveAccount struct {
	ID                         string
	Chain                      string
	PublicAddressEnv           string
	SignerEnv                  string
	SellPreflightAddressEnv    string
	SellPreflightStateOverride *ERC20StateOverrideConfig
	SenderURLEnv               string
	FanoutRPCURLEnv            string
	ContractAddressEnv         string
}

type ResolvedInventoryBalance struct {
	Chain         string
	Account       string
	Token         market.Token
	AllocationCap *big.Rat
	Target        *big.Rat
	Buffer        *big.Rat
	// Amount is retained as an immutable alias for AllocationCap while
	// callers migrate to wallet-backed observations.
	Amount *big.Rat
}

type ParsedLiveConfig struct {
	Hash                          string
	LiveID                        string
	RunID                         string
	SetupID                       string
	Assets                        map[market.AssetID]market.Asset
	Chains                        map[string]ResolvedChain
	Markets                       [2]ResolvedMarket
	QuoteSources                  map[string]ResolvedQuoteSource
	TransferSources               map[string]ResolvedTransferSource
	ExecutionPolicyID             string
	ExecutionPolicyKind           string
	InventoryProfileID            string
	InventoryKind                 string
	RunTier                       string
	ExecutionMode                 string
	Notional                      *big.Rat
	CanaryInput                   *big.Rat
	ExecutionInput                *big.Rat
	MaxOperationsPerRun           int
	BaseTransferSource            ResolvedTransferSource
	QuoteTransferSource           ResolvedTransferSource
	BaseBridgeProvider            string
	QuoteBridgeProvider           string
	BaseBridgeProfile             string
	ConfirmationTimeout           time.Duration
	ExecutionCost                 *big.Rat
	MaximumExecutionCost          *big.Rat
	MaximumBaseExposure           *big.Rat
	MinimumNet                    *big.Rat
	ReturnBridgeSafetyMargin      *big.Rat
	FeeCacheMaxAge                time.Duration
	CostRefreshInterval           time.Duration
	CostCacheTTL                  time.Duration
	CostCalibrationStore          string
	QuoteTransferCalibrationStore string
	AcrossCostCalibrationStore    string
	SlippageBPS                   uint16
	TipLamports                   string
	ComputeUnitPricePercentile    string
	ComputeUnitLimit              uint32
	MaxPriorityFeeLamports        string
	BlockhashSlotsToExpiry        uint16
	BuildToBroadcastTimeout       time.Duration
	EVMDeadline                   time.Duration
	EVMGas                        ResolvedEVMGasPolicy
	DynamicSlippage               ResolvedDynamicSlippage
	ExitValidationAttempts        int
	ExitValidationRetryDelay      time.Duration
	OperationalStorePath          string
	SQLiteSynchronous             string
	Accounts                      map[string]ResolvedLiveAccount
	Inventory                     []ResolvedInventoryBalance
	BalancePollInterval           time.Duration
	BalanceAlertInterval          time.Duration
	GasRefuel                     ResolvedGasRefuel
}

type ResolvedDynamicSlippage struct {
	Enabled bool
	MaxBPS  uint16
}

type ResolvedEVMGasPolicy struct {
	ExecutionMode           string
	ExecutionFixedLimit     uint64
	EstimationMultiplierBPS uint64
	CostMode                string
	CostFixedLimit          uint64
}

type ResolvedGasRefuel struct {
	Enabled      bool
	ThresholdUSD *big.Rat
	TargetUSD    *big.Rat
	PollInterval time.Duration
	Cooldown     time.Duration
	SlippageBPS  uint16
	MaxUSDC      *big.Rat
	EVMGas       ResolvedEVMGasPolicy
}

type LookupEnv func(string) (string, bool)

func LoadConfig(path string) (ParsedConfig, error) {
	manifest, topology, policy, err := loadDocuments(path)
	if err != nil {
		return ParsedConfig{}, err
	}
	if strings.TrimSpace(manifest.ActiveResearch) == "" {
		return ParsedConfig{}, fmt.Errorf("manifest requires an active research")
	}
	return resolve(manifest, topology, policy)
}

func LoadLiveConfig(path string) (ParsedLiveConfig, error) {
	manifest, topology, policy, err := loadDocuments(path)
	if err != nil {
		return ParsedLiveConfig{}, err
	}
	if strings.TrimSpace(manifest.ActiveLive) == "" {
		return ParsedLiveConfig{}, fmt.Errorf("manifest requires an active live profile")
	}
	return resolveLive(manifest, topology, policy)
}

func loadDocuments(path string) (Manifest, Topology, Policy, error) {
	manifestData, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, Topology{}, Policy{}, fmt.Errorf("read configuration manifest: %w", err)
	}
	var manifest Manifest
	if err := decodeYAML(manifestData, &manifest); err != nil {
		return Manifest{}, Topology{}, Policy{}, fmt.Errorf("decode configuration manifest: %w", err)
	}
	if manifest.SchemaVersion != schemaVersion {
		return Manifest{}, Topology{}, Policy{}, fmt.Errorf(
			"unsupported configuration schema version %d; schema v2 is required",
			manifest.SchemaVersion,
		)
	}
	if strings.TrimSpace(manifest.Topology) == "" ||
		strings.TrimSpace(manifest.Policy) == "" {
		return Manifest{}, Topology{}, Policy{}, fmt.Errorf("manifest requires topology and policy")
	}
	directory := filepath.Dir(path)
	topologyData, err := os.ReadFile(filepath.Join(directory, manifest.Topology))
	if err != nil {
		return Manifest{}, Topology{}, Policy{}, fmt.Errorf("read topology: %w", err)
	}
	policyData, err := os.ReadFile(filepath.Join(directory, manifest.Policy))
	if err != nil {
		return Manifest{}, Topology{}, Policy{}, fmt.Errorf("read policy: %w", err)
	}
	var topology Topology
	if err := decodeYAML(topologyData, &topology); err != nil {
		return Manifest{}, Topology{}, Policy{}, fmt.Errorf("decode topology: %w", err)
	}
	var policy Policy
	if err := decodeYAML(policyData, &policy); err != nil {
		return Manifest{}, Topology{}, Policy{}, fmt.Errorf("decode policy: %w", err)
	}
	if topology.SchemaVersion != schemaVersion || policy.SchemaVersion != schemaVersion {
		return Manifest{}, Topology{}, Policy{}, fmt.Errorf("topology and policy schema versions must be %d", schemaVersion)
	}
	return manifest, topology, policy, nil
}

func (c ParsedConfig) ResolveEndpoints(lookup LookupEnv) (map[string]string, error) {
	return resolveEndpoints(c.Chains, lookup)
}

func (c ParsedLiveConfig) ResolveEndpoints(lookup LookupEnv) (map[string]string, error) {
	return resolveEndpoints(c.Chains, lookup)
}

func resolveEndpoints(chains map[string]ResolvedChain, lookup LookupEnv) (map[string]string, error) {
	if lookup == nil {
		return nil, fmt.Errorf("environment lookup is required")
	}
	endpoints := make(map[string]string, len(chains)*2)
	for id, chain := range chains {
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
	quoteSources := make(map[string]ResolvedQuoteSource, len(topology.QuoteSources))
	for id, config := range topology.QuoteSources {
		resolved, err := resolveQuoteSource(id, config)
		if err != nil {
			return ParsedConfig{}, err
		}
		quoteSources[id] = resolved
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
	for _, resolved := range markets {
		if resolved.QuoteSource != "" && resolved.ReferenceQuote != "" {
			return ParsedConfig{}, fmt.Errorf("market %q cannot configure both primary and reference quote sources", resolved.ID)
		}
		if resolved.ReferenceQuote != "" {
			if _, ok := quoteSources[resolved.ReferenceQuote]; !ok {
				return ParsedConfig{}, fmt.Errorf("market %q references unknown quote source %q", resolved.ID, resolved.ReferenceQuote)
			}
		}
		if resolved.QuoteSource != "" {
			source, ok := quoteSources[resolved.QuoteSource]
			if !ok {
				return ParsedConfig{}, fmt.Errorf("market %q references unknown quote source %q", resolved.ID, resolved.QuoteSource)
			}
			chain := chains[resolved.Chain]
			if source.Kind == "jupiter" && chain.Kind != "solana" {
				return ParsedConfig{}, fmt.Errorf("jupiter market %q must use a Solana chain", resolved.ID)
			}
			if source.Kind == "kyberswap" && chain.Kind != "evm" {
				return ParsedConfig{}, fmt.Errorf("KyberSwap market %q must use an EVM chain", resolved.ID)
			}
			if len(resolved.TriggerPools) == 0 {
				return ParsedConfig{}, fmt.Errorf("market %q remote quotes require trigger pools", resolved.ID)
			}
		}
	}
	if markets[0].Base.Token.Asset != markets[1].Base.Token.Asset || markets[0].Quote.Token.Asset != markets[1].Quote.Token.Asset {
		return ParsedConfig{}, fmt.Errorf("setup markets must share base and quote assets")
	}
	var priceSource ResolvedPriceSource
	fixedCostAsset := market.AssetID(research.FixedCost.Asset)
	if fixedCostAsset != markets[0].Quote.Token.Asset {
		priceConfig, ok := topology.PriceSources[research.PriceSource]
		if !ok || market.AssetID(priceConfig.BaseAsset) != markets[0].Quote.Token.Asset ||
			market.AssetID(priceConfig.QuoteAsset) != fixedCostAsset {
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
		priceSource = ResolvedPriceSource{
			ID: market.SourceID(research.PriceSource), Base: market.AssetID(priceConfig.BaseAsset),
			Quote: market.AssetID(priceConfig.QuoteAsset), Primary: priceConfig.Primary, Fallback: priceConfig.Fallback,
		}
	} else {
		priceSource = ResolvedPriceSource{
			ID: market.SourceID("fixed-cost-parity"), Base: fixedCostAsset, Quote: fixedCostAsset,
		}
	}
	fixedCost, err := positiveOrZero(research.FixedCost.Amount, "fixed cost")
	if err != nil {
		return ParsedConfig{}, err
	}
	sizingAsset := strings.TrimSpace(research.Sizing.Asset)
	if sizingAsset == "" {
		sizingAsset = "quote"
	}
	if sizingAsset != "base" && sizingAsset != "quote" {
		return ParsedConfig{}, fmt.Errorf("sizing asset must be base or quote")
	}
	var minimum, maximum *big.Rat
	sizeSamples := research.Sizing.Samples
	sizingKind := strings.TrimSpace(research.Sizing.Kind)
	switch sizingKind {
	case "fixed":
		if strings.TrimSpace(research.Sizing.Minimum) != "" || strings.TrimSpace(research.Sizing.Maximum) != "" || research.Sizing.Samples != 0 {
			return ParsedConfig{}, fmt.Errorf("fixed sizing requires amount and forbids range bounds and samples")
		}
		amount, amountErr := positive(research.Sizing.Amount, "fixed sizing amount")
		if amountErr != nil {
			return ParsedConfig{}, amountErr
		}
		minimum = new(big.Rat).Set(amount)
		maximum = new(big.Rat).Set(amount)
		sizeSamples = 1
	case "linear_range":
		if strings.TrimSpace(research.Sizing.Amount) != "" {
			return ParsedConfig{}, fmt.Errorf("linear sizing forbids a fixed amount")
		}
		var rangeErr error
		minimum, rangeErr = positive(research.Sizing.Minimum, "minimum size")
		if rangeErr != nil {
			return ParsedConfig{}, rangeErr
		}
		maximum, rangeErr = positive(research.Sizing.Maximum, "maximum size")
		if rangeErr != nil || maximum.Cmp(minimum) <= 0 || research.Sizing.Samples < 2 {
			return ParsedConfig{}, fmt.Errorf("linear sizing requires increasing positive bounds and at least two samples")
		}
	default:
		return ParsedConfig{}, fmt.Errorf("sizing kind must be fixed or linear_range")
	}
	minimumNet, err := positiveOrZero(research.MinNetProfit, "minimum net profit")
	if err != nil {
		return ParsedConfig{}, err
	}
	thresholdKind := strings.TrimSpace(research.ProfitThreshold.Kind)
	thresholdFixed := new(big.Rat)
	thresholdBPS := research.ProfitThreshold.BPS
	if thresholdKind != "" {
		if thresholdKind != "max_fixed_and_input_bps" {
			return ParsedConfig{}, fmt.Errorf("unsupported research profit threshold kind %q", thresholdKind)
		}
		if minimumNet.Sign() != 0 {
			return ParsedConfig{}, fmt.Errorf("research profit_threshold and non-zero min_net_profit are mutually exclusive")
		}
		if market.AssetID(strings.TrimSpace(research.ProfitThreshold.Fixed.Asset)) != markets[0].Quote.Token.Asset {
			return ParsedConfig{}, fmt.Errorf("research fixed profit threshold must use quote asset %q", markets[0].Quote.Token.Asset)
		}
		thresholdFixed, err = positive(research.ProfitThreshold.Fixed.Amount, "fixed profit threshold")
		if err != nil {
			return ParsedConfig{}, err
		}
		if thresholdBPS == 0 || thresholdBPS > 10_000 {
			return ParsedConfig{}, fmt.Errorf("research input profit threshold BPS must be between 1 and 10000")
		}
	}
	evaluationMode := strings.TrimSpace(research.EvaluationMode)
	if evaluationMode == "" {
		evaluationMode = "two_market"
	}
	if evaluationMode != "two_market" && evaluationMode != "best_buy_opposite_sell" {
		return ParsedConfig{}, fmt.Errorf("unsupported research evaluation mode %q", evaluationMode)
	}
	trackingMode := strings.TrimSpace(research.TrackingMode)
	if trackingMode == "" {
		trackingMode = "window_reselect"
	}
	if trackingMode != "window_reselect" && trackingMode != "fixed_candidate" {
		return ParsedConfig{}, fmt.Errorf("unsupported research tracking mode %q", trackingMode)
	}
	trackingQueueCapacity := research.TrackingQueueCapacity
	if trackingQueueCapacity == 0 {
		trackingQueueCapacity = 4096
	}
	if trackingQueueCapacity < 1 {
		return ParsedConfig{}, fmt.Errorf("research tracking queue capacity must be positive")
	}
	windowQualification := strings.TrimSpace(research.WindowQualification)
	if windowQualification == "" {
		windowQualification = "economic"
	}
	if windowQualification != "economic" && windowQualification != "policy_qualified" {
		return ParsedConfig{}, fmt.Errorf("unsupported window qualification %q", windowQualification)
	}
	if research.IdleEvaluationIntervalMS < 0 {
		return ParsedConfig{}, fmt.Errorf("idle evaluation interval cannot be negative")
	}
	idleInterval := time.Duration(research.IdleEvaluationIntervalMS) * time.Millisecond
	retryAttempts := research.Retry.Attempts
	if retryAttempts == 0 {
		retryAttempts = 1
	}
	if research.Retry.Attempts < 0 || retryAttempts > 1 || research.Retry.DelayMS < 0 {
		return ParsedConfig{}, fmt.Errorf("research retry policy is invalid")
	}
	retryDelay := time.Duration(research.Retry.DelayMS) * time.Millisecond
	if retryDelay == 0 {
		retryDelay = 100 * time.Millisecond
	}
	if research.Telegram.Enabled &&
		(!environmentName.MatchString(research.Telegram.BotTokenEnv) ||
			!environmentName.MatchString(research.Telegram.ChatIDEnv)) {
		return ParsedConfig{}, fmt.Errorf("telegram alert environment is invalid")
	}
	simulationInterval := time.Duration(research.Simulation.IntervalMS) * time.Millisecond
	if research.Simulation.Enabled {
		if research.Simulation.IntervalMS == 0 {
			simulationInterval = time.Second
		}
		if simulationInterval < time.Second {
			return ParsedConfig{}, fmt.Errorf("research simulation interval must be at least one second")
		}
		for field, value := range map[string]string{
			"solana_owner_env": research.Simulation.SolanaOwnerEnv,
			"evm_owner_env":    research.Simulation.EVMOwnerEnv,
			"evm_router_env":   research.Simulation.EVMRouterEnv,
		} {
			if !environmentName.MatchString(strings.TrimSpace(value)) {
				return ParsedConfig{}, fmt.Errorf("research simulation %s is invalid", field)
			}
		}
		if research.Simulation.EVMBalanceSlot == research.Simulation.EVMAllowanceSlot {
			return ParsedConfig{}, fmt.Errorf("research simulation EVM balance and allowance slots must be distinct")
		}
		for token, slots := range research.Simulation.EVMTokenSlots {
			if strings.TrimSpace(token) == "" || slots.BalanceSlot == slots.AllowanceSlot {
				return ParsedConfig{}, fmt.Errorf("research simulation token slot configuration is invalid for %q", token)
			}
		}
		if research.Simulation.EVMGasLimit == 0 {
			return ParsedConfig{}, fmt.Errorf("research simulation EVM gas limit must be positive")
		}
		if research.Simulation.SolanaComputeLimit == 0 {
			research.Simulation.SolanaComputeLimit = 1_400_000
		}
	} else {
		simulationInterval = 0
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
		SetupID: research.Setup, InventoryMode: research.InventoryMode, Assets: assets, Chains: chains, Markets: markets, QuoteSources: quoteSources,
		PriceSource: priceSource,
		FixedCost:   fixedCost, SizingKind: sizingKind, MinimumSize: minimum, MaximumSize: maximum,
		SizeSamples: sizeSamples, SizingAsset: sizingAsset, MinimumNet: minimumNet,
		ProfitThresholdKind: thresholdKind, ProfitThresholdFixed: thresholdFixed,
		ProfitThresholdBPS: thresholdBPS,
		EvaluationMode:     evaluationMode, TrackingMode: trackingMode, TrackingQueueCapacity: trackingQueueCapacity,
		IdleEvaluationInterval: idleInterval,
		WindowQualification:    windowQualification, RetryAttempts: retryAttempts, RetryDelay: retryDelay,
		TelegramEnabled: research.Telegram.Enabled, TelegramBotTokenEnv: research.Telegram.BotTokenEnv,
		TelegramChatIDEnv: research.Telegram.ChatIDEnv,
		SimulationEnabled: research.Simulation.Enabled, SimulationInterval: simulationInterval,
		SimulationSolanaOwnerEnv:     research.Simulation.SolanaOwnerEnv,
		SimulationEVMOwnerEnv:        research.Simulation.EVMOwnerEnv,
		SimulationEVMRouterEnv:       research.Simulation.EVMRouterEnv,
		SimulationEVMBalanceSlot:     research.Simulation.EVMBalanceSlot,
		SimulationEVMAllowanceSlot:   research.Simulation.EVMAllowanceSlot,
		SimulationEVMGasLimit:        research.Simulation.EVMGasLimit,
		SimulationSolanaComputeLimit: research.Simulation.SolanaComputeLimit,
		SimulationEVMTokenSlots: func() map[market.TokenID]SimulationTokenSlots {
			result := make(map[market.TokenID]SimulationTokenSlots, len(research.Simulation.EVMTokenSlots))
			for token, slots := range research.Simulation.EVMTokenSlots {
				result[market.TokenID(token)] = slots
			}
			return result
		}(),
	}, nil
}

func resolveLive(manifest Manifest, topology Topology, policy Policy) (ParsedLiveConfig, error) {
	config, ok := policy.Live[manifest.ActiveLive]
	if !ok || strings.TrimSpace(config.RunID) == "" {
		return ParsedLiveConfig{}, fmt.Errorf("active live profile requires a run ID")
	}
	executionPolicyID := strings.TrimSpace(config.ExecutionPolicy)
	inventoryProfileID := strings.TrimSpace(config.InventoryProfile)
	executionPolicy, policyOK := policy.ExecutionPolicies[executionPolicyID]
	inventoryProfile, inventoryOK := policy.InventoryProfiles[inventoryProfileID]
	if executionPolicyID != "" || inventoryProfileID != "" {
		if !policyOK || !inventoryOK {
			return ParsedLiveConfig{}, fmt.Errorf(
				"active live profile references an unknown execution policy or inventory profile",
			)
		}
		config.ExecutionMode = strings.TrimSpace(executionPolicy.Kind)
		config.InventoryMode = strings.TrimSpace(inventoryProfile.Kind)
		config.BaseTransferSource = executionPolicy.BaseTransferSource
		config.QuoteTransferSource = executionPolicy.QuoteTransferSource
		config.Inventory = append(
			[]InventoryBalanceConfig(nil), inventoryProfile.Balances...,
		)
		switch config.ExecutionMode {
		case "transported_sequential":
			if executionPolicy.ExitPolicy !=
				"post_bridge_destination_with_return_fallback" {
				return ParsedLiveConfig{}, fmt.Errorf(
					"transported execution policy has an invalid exit_policy",
				)
			}
		case "prefunded_sequential":
			if executionPolicy.ExitPolicy !=
				"destination_first_origin_circuit_breaker" ||
				executionPolicy.InventoryRestore != "immediate_ordered" {
				return ParsedLiveConfig{}, fmt.Errorf(
					"prefunded sequential policy requires destination-first exit and immediate ordered restoration",
				)
			}
		case "prefunded_parallel":
		default:
			return ParsedLiveConfig{}, fmt.Errorf(
				"unsupported execution policy kind %q", config.ExecutionMode,
			)
		}
	}
	executionMode := strings.TrimSpace(config.ExecutionMode)
	if executionMode == "" {
		if config.InventoryMode == "prefunded_live" {
			executionMode = "prefunded_parallel"
		} else {
			return ParsedLiveConfig{}, fmt.Errorf(
				"active live profile requires an execution_policy",
			)
		}
	}
	// Old execution names are accepted only inside a schema-v2 document while
	// private deployments transition their non-public configuration. Schema v1
	// is rejected before resolution.
	if executionMode == "sequential_bridge_canary" {
		executionMode = "transported_sequential"
		if config.RunTier == "" {
			config.RunTier = "canary"
		}
	}
	if executionMode == "sequential_bridge_live" {
		executionMode = "transported_sequential"
		if config.RunTier == "" {
			config.RunTier = "live"
		}
	}
	runTier := strings.TrimSpace(config.RunTier)
	if runTier == "" {
		runTier = "live"
	}
	if runTier != "canary" && runTier != "live" {
		return ParsedLiveConfig{}, fmt.Errorf("live run_tier must be canary or live")
	}
	sequential := executionMode == "transported_sequential" ||
		executionMode == "prefunded_sequential" ||
		(executionMode == "prefunded_parallel" && executionPolicyID != "")
	sequentialCanary := sequential && runTier == "canary"
	sequentialLive := sequential && runTier == "live"
	if executionMode != "prefunded_parallel" && !sequential {
		return ParsedLiveConfig{}, fmt.Errorf("unsupported live execution mode %q", executionMode)
	}
	if executionMode == "transported_sequential" &&
		config.InventoryMode != "transported" &&
		config.InventoryMode != "prefunded_live" {
		return ParsedLiveConfig{}, fmt.Errorf(
			"transported sequential execution requires transported inventory",
		)
	}
	if (executionMode == "prefunded_sequential" ||
		executionMode == "prefunded_parallel") &&
		config.InventoryMode != "prefunded" &&
		config.InventoryMode != "prefunded_live" {
		return ParsedLiveConfig{}, fmt.Errorf(
			"prefunded execution requires prefunded inventory",
		)
	}
	setup, ok := policy.Setups[config.Setup]
	if !ok || len(setup.Markets) != 2 || setup.Markets[0] == setup.Markets[1] {
		return ParsedLiveConfig{}, fmt.Errorf("active live setup requires two distinct markets")
	}
	assets := make(map[market.AssetID]market.Asset, len(topology.Assets))
	for id, candidate := range topology.Assets {
		if strings.TrimSpace(id) == "" || strings.TrimSpace(candidate.Symbol) == "" {
			return ParsedLiveConfig{}, fmt.Errorf("assets require IDs and symbols")
		}
		assets[market.AssetID(id)] = market.Asset{ID: market.AssetID(id), Symbol: candidate.Symbol}
	}
	quoteSources := make(map[string]ResolvedQuoteSource, len(topology.QuoteSources))
	for id, candidate := range topology.QuoteSources {
		resolved, err := resolveQuoteSource(id, candidate)
		if err != nil {
			return ParsedLiveConfig{}, err
		}
		quoteSources[id] = resolved
	}
	transferSources := make(
		map[string]ResolvedTransferSource,
		len(topology.TransferSources),
	)
	for id, candidate := range topology.TransferSources {
		resolved, err := resolveTransferSource(id, candidate)
		if err != nil {
			return ParsedLiveConfig{}, err
		}
		transferSources[id] = resolved
	}
	chains := make(map[string]ResolvedChain)
	markets := [2]ResolvedMarket{}
	remoteMarkets := 0
	for index, id := range setup.Markets {
		resolved, chain, err := resolveMarket(id, topology, assets)
		if err != nil {
			return ParsedLiveConfig{}, err
		}
		if resolved.QuoteSource != "" {
			if _, exists := quoteSources[resolved.QuoteSource]; !exists {
				return ParsedLiveConfig{}, fmt.Errorf("market %q references unknown quote source %q", resolved.ID, resolved.QuoteSource)
			}
			if len(resolved.TriggerPools) == 0 ||
				!sequential && chain.Kind != "solana" {
				return ParsedLiveConfig{}, fmt.Errorf("live remote market %q requires compatible trigger pools", resolved.ID)
			}
			remoteMarkets++
		}
		markets[index], chains[chain.ID] = resolved, chain
	}
	if !sequential && remoteMarkets != 1 {
		return ParsedLiveConfig{}, fmt.Errorf("initial live vertical requires exactly one event-refreshed remote market")
	}
	if sequential && remoteMarkets != 2 {
		return ParsedLiveConfig{}, fmt.Errorf("sequential bridge execution requires two event-refreshed remote markets")
	}
	if markets[0].Base.Token.Asset != markets[1].Base.Token.Asset ||
		markets[0].Quote.Token.Asset != markets[1].Quote.Token.Asset {
		return ParsedLiveConfig{}, fmt.Errorf("live setup markets must share base and quote assets")
	}
	executionCostAsset := market.AssetID(config.ExecutionCost.Asset)
	if market.AssetID(config.Notional.Asset) != markets[0].Quote.Token.Asset ||
		(!sequential || executionCostAsset != "") &&
			executionCostAsset != markets[0].Quote.Token.Asset ||
		market.AssetID(config.MaxExecutionCost.Asset) != markets[0].Quote.Token.Asset ||
		market.AssetID(config.MaxBaseExposure.Asset) != markets[0].Base.Token.Asset ||
		market.AssetID(config.MinNetProfit.Asset) != markets[0].Quote.Token.Asset {
		return ParsedLiveConfig{}, fmt.Errorf("live economic limits use inconsistent setup assets")
	}
	notional, err := positive(config.Notional.Amount, "live notional")
	if err != nil {
		return ParsedLiveConfig{}, err
	}
	var canaryInput *big.Rat
	var executionInput *big.Rat
	if sequentialCanary {
		if market.AssetID(config.CanaryInput.Asset) != markets[0].Quote.Token.Asset {
			return ParsedLiveConfig{}, fmt.Errorf("live canary input must use the setup quote asset")
		}
		canaryInput, err = positive(config.CanaryInput.Amount, "live canary input")
		if err != nil {
			return ParsedLiveConfig{}, err
		}
		if canaryInput.Cmp(notional) > 0 {
			return ParsedLiveConfig{}, fmt.Errorf("live canary input cannot exceed discovery notional")
		}
		executionInput = new(big.Rat).Set(canaryInput)
	}
	if sequentialLive {
		if market.AssetID(config.ExecutionInput.Asset) != markets[0].Quote.Token.Asset {
			return ParsedLiveConfig{}, fmt.Errorf("live execution input must use the setup quote asset")
		}
		executionInput, err = positive(config.ExecutionInput.Amount, "live execution input")
		if err != nil {
			return ParsedLiveConfig{}, err
		}
		if executionInput.Cmp(notional) != 0 {
			return ParsedLiveConfig{}, fmt.Errorf("live execution input must equal discovery notional")
		}
	}
	var baseTransfer, quoteTransfer ResolvedTransferSource
	genericTransfers := false
	legacyTransfers := false
	if sequential {
		var baseOK, quoteOK bool
		baseTransfer, baseOK = transferSources[strings.TrimSpace(
			config.BaseTransferSource,
		)]
		quoteTransfer, quoteOK = transferSources[strings.TrimSpace(
			config.QuoteTransferSource,
		)]
		genericTransfers = baseOK && quoteOK
		legacyTransfers = config.BaseBridgeProvider == "wormhole_ntt" &&
			config.QuoteBridgeProvider == "across_cctp" &&
			strings.TrimSpace(config.BaseBridgeProfile) != ""
		if config.MaxOperationsPerRun != 1 {
			return ParsedLiveConfig{}, fmt.Errorf("initial sequential execution requires max_operations_per_run: 1")
		}
		if (!genericTransfers && !legacyTransfers) ||
			config.ConfirmationTimeoutSeconds <= 0 {
			return ParsedLiveConfig{}, fmt.Errorf("sequential bridge and confirmation policy is incomplete")
		}
	}
	var executionCost *big.Rat
	if sequential && strings.TrimSpace(config.ExecutionCost.Amount) == "" {
		executionCost = new(big.Rat)
	} else {
		executionCost, err = positive(config.ExecutionCost.Amount, "live execution cost")
		if err != nil {
			return ParsedLiveConfig{}, err
		}
	}
	maximumExecutionCost, err := positive(config.MaxExecutionCost.Amount, "live maximum execution cost")
	if err != nil {
		return ParsedLiveConfig{}, err
	}
	if executionCost.Cmp(maximumExecutionCost) > 0 {
		return ParsedLiveConfig{}, fmt.Errorf("live execution cost exceeds its configured maximum")
	}
	maximumBaseExposure, err := positive(config.MaxBaseExposure.Amount, "live maximum base exposure")
	if err != nil {
		return ParsedLiveConfig{}, err
	}
	minimumNet, err := positiveOrZero(config.MinNetProfit.Amount, "live minimum net profit")
	if err != nil {
		return ParsedLiveConfig{}, err
	}
	returnSafetyMargin := new(big.Rat)
	if strings.TrimSpace(config.ReturnBridgeSafetyMargin.Amount) != "" {
		if market.AssetID(config.ReturnBridgeSafetyMargin.Asset) !=
			markets[0].Quote.Token.Asset {
			return ParsedLiveConfig{}, fmt.Errorf(
				"live return bridge safety margin must use the quote asset",
			)
		}
		returnSafetyMargin, err = positiveOrZero(
			config.ReturnBridgeSafetyMargin.Amount,
			"live return bridge safety margin",
		)
		if err != nil {
			return ParsedLiveConfig{}, err
		}
	}
	if config.SlippageBPS == 0 || config.SlippageBPS > 10_000 {
		return ParsedLiveConfig{}, fmt.Errorf("live slippage must be between 1 and 10000 basis points")
	}
	hasSolana := false
	hasEVM := false
	for _, chain := range chains {
		hasSolana = hasSolana || chain.Kind == "solana"
		hasEVM = hasEVM || chain.Kind == "evm"
	}
	evmGas := ResolvedEVMGasPolicy{}
	if hasEVM {
		evmGas, err = resolveEVMGasPolicy(config.EVMGas)
		if err != nil {
			return ParsedLiveConfig{}, err
		}
	}
	dynamicSlippage := ResolvedDynamicSlippage{
		Enabled: config.DynamicSlippage.Enabled,
		MaxBPS:  config.DynamicSlippage.MaxBPS,
	}
	if dynamicSlippage.Enabled && dynamicSlippage.MaxBPS == 0 {
		dynamicSlippage.MaxBPS = 500
	}
	if dynamicSlippage.MaxBPS > 2_000 {
		return ParsedLiveConfig{}, fmt.Errorf(
			"live dynamic slippage max_bps must be <= 2000",
		)
	}
	exitValidationAttempts := config.ExitValidationAttempts
	if exitValidationAttempts == 0 {
		exitValidationAttempts = 15
	}
	if exitValidationAttempts < 1 || exitValidationAttempts > 30 {
		return ParsedLiveConfig{}, fmt.Errorf(
			"live exit_validation_attempts must be between 1 and 30",
		)
	}
	exitValidationRetryDelay := time.Duration(
		config.ExitValidationRetryDelayMS,
	) * time.Millisecond
	if exitValidationRetryDelay == 0 {
		exitValidationRetryDelay = 100 * time.Millisecond
	}
	if exitValidationRetryDelay < 0 || exitValidationRetryDelay > 5*time.Second {
		return ParsedLiveConfig{}, fmt.Errorf(
			"live exit_validation_retry_delay_ms must be between 0 and 5000",
		)
	}
	if hasSolana {
		tip, ok := new(big.Int).SetString(config.TipLamports, 10)
		if !ok || tip.Sign() <= 0 {
			return ParsedLiveConfig{}, fmt.Errorf(
				"live tip_lamports must be a positive integer",
			)
		}
		if strings.TrimSpace(config.ComputeUnitPricePercentile) == "" ||
			config.ComputeUnitLimit == 0 ||
			config.ComputeUnitLimit > 1_400_000 ||
			config.BlockhashSlotsToExpiry == 0 ||
			config.BlockhashSlotsToExpiry > 300 {
			return ParsedLiveConfig{}, fmt.Errorf(
				"live Solana transaction policy is incomplete",
			)
		}
		if strings.TrimSpace(config.MaxPriorityFeeLamports) == "" {
			config.MaxPriorityFeeLamports = "18446744073709551615"
		}
		maxPriorityFee, ok := new(big.Int).SetString(config.MaxPriorityFeeLamports, 10)
		if !ok || maxPriorityFee.Sign() <= 0 || !maxPriorityFee.IsUint64() {
			return ParsedLiveConfig{}, fmt.Errorf(
				"live max_priority_fee_lamports must be a positive uint64 integer",
			)
		}
	}
	if config.FeeCacheMaxAgeMS <= 0 ||
		config.BuildToBroadcastTimeoutMS <= 0 ||
		hasEVM && config.EVMDeadlineSeconds <= 0 {
		return ParsedLiveConfig{}, fmt.Errorf("live transaction validity policy is invalid")
	}
	if config.CostRefreshIntervalMS == 0 {
		config.CostRefreshIntervalMS = 15_000
	}
	if config.BalanceTracking.PollSeconds == 0 {
		config.BalanceTracking.PollSeconds = 60
	}
	if config.BalanceTracking.AlertIntervalSeconds == 0 {
		config.BalanceTracking.AlertIntervalSeconds = 300
	}
	if config.BalanceTracking.PollSeconds < 1 ||
		config.BalanceTracking.PollSeconds > 3600 ||
		config.BalanceTracking.AlertIntervalSeconds < 1 {
		return ParsedLiveConfig{}, fmt.Errorf("live balance tracking policy is invalid")
	}
	if config.CostCacheTTLMS == 0 {
		config.CostCacheTTLMS = 60_000
	}
	if config.CostRefreshIntervalMS < 1_000 ||
		config.CostCacheTTLMS < config.CostRefreshIntervalMS {
		return ParsedLiveConfig{}, fmt.Errorf("live complete-flow cost cache policy is invalid")
	}
	refuel := ResolvedGasRefuel{
		Enabled: config.GasRefuel.Enabled,
		EVMGas:  evmGas,
	}
	if refuel.Enabled {
		if hasEVM && config.GasRefuel.EVMGas != nil {
			refuel.EVMGas, err = resolveEVMGasPolicy(
				*config.GasRefuel.EVMGas,
			)
			if err != nil {
				return ParsedLiveConfig{}, fmt.Errorf(
					"live gas refuel EVM gas policy: %w",
					err,
				)
			}
		}
		if strings.TrimSpace(config.GasRefuel.ThresholdUSD) == "" {
			config.GasRefuel.ThresholdUSD = "5"
		}
		if strings.TrimSpace(config.GasRefuel.TargetUSD) == "" {
			config.GasRefuel.TargetUSD = "15"
		}
		if strings.TrimSpace(config.GasRefuel.MaxUSDC) == "" {
			config.GasRefuel.MaxUSDC = "20"
		}
		if config.GasRefuel.PollSeconds == 0 {
			config.GasRefuel.PollSeconds = 300
		}
		if config.GasRefuel.CooldownSeconds == 0 {
			config.GasRefuel.CooldownSeconds = 900
		}
		if config.GasRefuel.SlippageBPS == 0 {
			config.GasRefuel.SlippageBPS = 20
		}
		refuel.ThresholdUSD, err = positive(
			config.GasRefuel.ThresholdUSD,
			"live gas refuel threshold",
		)
		if err != nil {
			return ParsedLiveConfig{}, err
		}
		refuel.TargetUSD, err = positive(
			config.GasRefuel.TargetUSD,
			"live gas refuel target",
		)
		if err != nil {
			return ParsedLiveConfig{}, err
		}
		refuel.MaxUSDC, err = positive(
			config.GasRefuel.MaxUSDC,
			"live gas refuel maximum",
		)
		if err != nil {
			return ParsedLiveConfig{}, err
		}
		refuel.PollInterval =
			time.Duration(config.GasRefuel.PollSeconds) * time.Second
		refuel.Cooldown =
			time.Duration(config.GasRefuel.CooldownSeconds) * time.Second
		refuel.SlippageBPS = config.GasRefuel.SlippageBPS
		if refuel.TargetUSD.Cmp(refuel.ThresholdUSD) <= 0 ||
			refuel.PollInterval < time.Minute ||
			refuel.Cooldown < refuel.PollInterval ||
			refuel.SlippageBPS > 10_000 {
			return ParsedLiveConfig{},
				fmt.Errorf("live gas refuel policy is invalid")
		}
	}
	synchronous := strings.ToUpper(strings.TrimSpace(config.OperationalStore.Synchronous))
	if synchronous == "" {
		synchronous = "FULL"
	}
	if strings.TrimSpace(config.OperationalStore.Path) == "" || synchronous != "FULL" && synchronous != "NORMAL" {
		return ParsedLiveConfig{}, fmt.Errorf("live operational store requires path and FULL or NORMAL synchronous mode")
	}
	accounts := make(map[string]ResolvedLiveAccount, len(config.Accounts))
	for chainID := range chains {
		account, exists := config.Accounts[chainID]
		chain := chains[chainID]
		if !exists || strings.TrimSpace(account.ID) == "" ||
			!environmentName.MatchString(account.SignerEnv) {
			return ParsedLiveConfig{}, fmt.Errorf(
				"live chain %q requires account ID and signer environment",
				chainID,
			)
		}
		for _, candidate := range []string{
			account.PublicAddressEnv,
			account.SellPreflightAddressEnv,
			account.SenderURLEnv,
			account.FanoutRPCURLEnv,
			account.ContractAddressEnv,
		} {
			if candidate != "" && !environmentName.MatchString(candidate) {
				return ParsedLiveConfig{}, fmt.Errorf(
					"live account %q has an invalid environment name",
					account.ID,
				)
			}
		}
		if account.SenderURLEnv == "" && account.FanoutRPCURLEnv == "" {
			return ParsedLiveConfig{}, fmt.Errorf(
				"live account %q requires a broadcast transport",
				account.ID,
			)
		}
		if account.SellPreflightStateOverride != nil &&
			account.SellPreflightStateOverride.BalanceSlot ==
				account.SellPreflightStateOverride.AllowanceSlot {
			return ParsedLiveConfig{}, fmt.Errorf(
				"live account %q requires distinct state override slots",
				account.ID,
			)
		}
		if legacyTransfers &&
			executionMode == "transported_sequential" {
			switch chain.Kind {
			case "solana":
				if !environmentName.MatchString(account.SellPreflightAddressEnv) ||
					account.SellPreflightStateOverride != nil {
					return ParsedLiveConfig{}, fmt.Errorf(
						"live Solana chain %q requires a sell preflight address environment",
						chainID,
					)
				}
				if !environmentName.MatchString(account.SenderURLEnv) ||
					account.FanoutRPCURLEnv != "" ||
					account.ContractAddressEnv != "" {
					return ParsedLiveConfig{}, fmt.Errorf(
						"live Solana account %q requires only sender_url_env",
						account.ID,
					)
				}
			case "evm":
				if account.SellPreflightStateOverride == nil ||
					account.SellPreflightAddressEnv != "" {
					return ParsedLiveConfig{}, fmt.Errorf(
						"live EVM chain %q requires an ERC-20 sell preflight state override",
						chainID,
					)
				}
				if !environmentName.MatchString(account.FanoutRPCURLEnv) ||
					account.SenderURLEnv != "" {
					return ParsedLiveConfig{}, fmt.Errorf(
						"live EVM account %q has incomplete fanout environments",
						account.ID,
					)
				}
			default:
				return ParsedLiveConfig{}, fmt.Errorf(
					"live chain %q has unsupported kind %q",
					chainID,
					chain.Kind,
				)
			}
		} else if !sequential && !environmentName.MatchString(account.PublicAddressEnv) {
			return ParsedLiveConfig{}, fmt.Errorf(
				"live chain %q requires a public address environment",
				chainID,
			)
		}
		accounts[chainID] = ResolvedLiveAccount{
			ID: account.ID, Chain: chainID, PublicAddressEnv: account.PublicAddressEnv,
			SignerEnv:                  account.SignerEnv,
			SellPreflightAddressEnv:    account.SellPreflightAddressEnv,
			SellPreflightStateOverride: account.SellPreflightStateOverride,
			SenderURLEnv:               account.SenderURLEnv, FanoutRPCURLEnv: account.FanoutRPCURLEnv,
			ContractAddressEnv: account.ContractAddressEnv,
		}
	}
	tokenByID := make(map[string]market.Token)
	for _, configuredMarket := range markets {
		tokenByID[string(configuredMarket.Base.Token.ID)] = configuredMarket.Base.Token
		tokenByID[string(configuredMarket.Quote.Token.ID)] = configuredMarket.Quote.Token
	}
	inventoryBalances := make([]ResolvedInventoryBalance, 0, len(config.Inventory))
	covered := make(map[string]bool)
	for _, balance := range config.Inventory {
		account, accountOK := accounts[balance.Chain]
		token, tokenOK := tokenByID[balance.Token]
		accountID := strings.TrimSpace(balance.Account)
		if accountID == "" && accountOK {
			accountID = account.ID
		}
		capText := strings.TrimSpace(balance.AllocationCap)
		if capText == "" {
			capText = balance.Amount
		}
		allocationCap, capOK := new(big.Rat).SetString(capText)
		target, targetOK := new(big.Rat).SetString(balance.Target)
		if strings.TrimSpace(balance.Target) == "" {
			target = new(big.Rat).Set(allocationCap)
			targetOK = capOK
		}
		buffer, bufferOK := new(big.Rat).SetString(balance.Buffer)
		if strings.TrimSpace(balance.Buffer) == "" {
			buffer = new(big.Rat)
			bufferOK = true
		}
		if !accountOK || account.ID != accountID || !tokenOK || token.Chain != market.ChainID(balance.Chain) ||
			!capOK || allocationCap.Sign() <= 0 ||
			!targetOK || target.Sign() < 0 || target.Cmp(allocationCap) > 0 ||
			!bufferOK || buffer.Sign() < 0 || buffer.Cmp(allocationCap) >= 0 {
			return ParsedLiveConfig{}, fmt.Errorf("live inventory balance is invalid")
		}
		if account.ID != accountID {
			return ParsedLiveConfig{}, fmt.Errorf("live inventory account is invalid")
		}
		key := balance.Chain + "/" + accountID + "/" + balance.Token
		if covered[key] {
			return ParsedLiveConfig{}, fmt.Errorf("live inventory repeats balance %q", key)
		}
		covered[key] = true
		inventoryBalances = append(inventoryBalances, ResolvedInventoryBalance{
			Chain: balance.Chain, Account: accountID, Token: token,
			AllocationCap: allocationCap, Target: target, Buffer: buffer,
			Amount: allocationCap,
		})
	}
	for _, configuredMarket := range markets {
		account := accounts[configuredMarket.Chain]
		requiredTokens := []market.Token{
			configuredMarket.Base.Token,
			configuredMarket.Quote.Token,
		}
		if executionMode == "transported_sequential" {
			requiredTokens = []market.Token{configuredMarket.Quote.Token}
		}
		for _, token := range requiredTokens {
			key := configuredMarket.Chain + "/" + account.ID + "/" + string(token.ID)
			if !covered[key] {
				return ParsedLiveConfig{}, fmt.Errorf("live inventory is missing prefunded token %q", token.ID)
			}
		}
	}
	bundle := struct {
		Manifest Manifest
		Topology Topology
		Policy   Policy
	}{manifest, topology, policy}
	canonical, err := json.Marshal(bundle)
	if err != nil {
		return ParsedLiveConfig{}, err
	}
	sum := sha256.Sum256(canonical)
	return ParsedLiveConfig{
		Hash: hex.EncodeToString(sum[:]), LiveID: manifest.ActiveLive, RunID: config.RunID,
		SetupID: config.Setup, Assets: assets, Chains: chains, Markets: markets,
		QuoteSources: quoteSources, TransferSources: transferSources,
		ExecutionPolicyID:   executionPolicyID,
		ExecutionPolicyKind: executionMode,
		InventoryProfileID:  inventoryProfileID,
		InventoryKind:       config.InventoryMode,
		RunTier:             runTier,
		ExecutionMode:       executionMode, Notional: notional, CanaryInput: canaryInput,
		ExecutionInput:       executionInput,
		MaxOperationsPerRun:  config.MaxOperationsPerRun,
		BaseTransferSource:   baseTransfer,
		QuoteTransferSource:  quoteTransfer,
		BaseBridgeProvider:   config.BaseBridgeProvider,
		QuoteBridgeProvider:  config.QuoteBridgeProvider,
		BaseBridgeProfile:    strings.TrimSpace(config.BaseBridgeProfile),
		ConfirmationTimeout:  time.Duration(config.ConfirmationTimeoutSeconds) * time.Second,
		ExecutionCost:        executionCost,
		MaximumExecutionCost: maximumExecutionCost, MaximumBaseExposure: maximumBaseExposure,
		MinimumNet:               minimumNet,
		ReturnBridgeSafetyMargin: returnSafetyMargin,
		FeeCacheMaxAge:           time.Duration(config.FeeCacheMaxAgeMS) * time.Millisecond,
		CostRefreshInterval:      time.Duration(config.CostRefreshIntervalMS) * time.Millisecond,
		CostCacheTTL:             time.Duration(config.CostCacheTTLMS) * time.Millisecond,
		CostCalibrationStore:     strings.TrimSpace(config.CostCalibrationStore),
		QuoteTransferCalibrationStore: strings.TrimSpace(
			config.QuoteTransferCalibrationStore,
		),
		AcrossCostCalibrationStore: strings.TrimSpace(config.AcrossCostCalibrationStore),
		SlippageBPS:                config.SlippageBPS, TipLamports: config.TipLamports,
		ComputeUnitPricePercentile: config.ComputeUnitPricePercentile, ComputeUnitLimit: config.ComputeUnitLimit,
		MaxPriorityFeeLamports:   config.MaxPriorityFeeLamports,
		BlockhashSlotsToExpiry:   config.BlockhashSlotsToExpiry,
		BuildToBroadcastTimeout:  time.Duration(config.BuildToBroadcastTimeoutMS) * time.Millisecond,
		EVMDeadline:              time.Duration(config.EVMDeadlineSeconds) * time.Second,
		EVMGas:                   evmGas,
		DynamicSlippage:          dynamicSlippage,
		ExitValidationAttempts:   exitValidationAttempts,
		ExitValidationRetryDelay: exitValidationRetryDelay,
		OperationalStorePath:     config.OperationalStore.Path, SQLiteSynchronous: synchronous,
		Accounts: accounts, Inventory: inventoryBalances,
		BalancePollInterval:  time.Duration(config.BalanceTracking.PollSeconds) * time.Second,
		BalanceAlertInterval: time.Duration(config.BalanceTracking.AlertIntervalSeconds) * time.Second,
		GasRefuel:            refuel,
	}, nil
}

func resolveEVMGasPolicy(
	config EVMGasConfig,
) (ResolvedEVMGasPolicy, error) {
	executionMode := strings.TrimSpace(config.ExecutionMode)
	if executionMode == "" {
		executionMode = "estimate"
	}
	if executionMode != "estimate" && executionMode != "fixed" {
		return ResolvedEVMGasPolicy{}, fmt.Errorf(
			"live EVM gas execution_mode must be estimate or fixed",
		)
	}
	if config.EstimationMultiplierBPS == 0 {
		config.EstimationMultiplierBPS = 12_000
	}
	if config.EstimationMultiplierBPS < 10_000 {
		return ResolvedEVMGasPolicy{}, fmt.Errorf(
			"live EVM gas estimation_multiplier_bps cannot reduce estimated gas",
		)
	}
	if executionMode == "fixed" && config.ExecutionFixedLimit == 0 {
		return ResolvedEVMGasPolicy{}, fmt.Errorf(
			"live EVM fixed execution gas requires execution_fixed_limit",
		)
	}
	costMode := strings.TrimSpace(config.CostMode)
	if costMode == "" {
		costMode = "estimated"
	}
	switch costMode {
	case "estimated":
		if executionMode != "estimate" {
			return ResolvedEVMGasPolicy{}, fmt.Errorf(
				"live EVM estimated cost gas requires estimated execution gas",
			)
		}
	case "transaction_limit":
	case "fixed":
		if config.CostFixedLimit == 0 {
			return ResolvedEVMGasPolicy{}, fmt.Errorf(
				"live EVM fixed cost gas requires cost_fixed_limit",
			)
		}
	default:
		return ResolvedEVMGasPolicy{}, fmt.Errorf(
			"live EVM gas cost_mode must be estimated, transaction_limit, or fixed",
		)
	}
	return ResolvedEVMGasPolicy{
		ExecutionMode:           executionMode,
		ExecutionFixedLimit:     config.ExecutionFixedLimit,
		EstimationMultiplierBPS: config.EstimationMultiplierBPS,
		CostMode:                costMode,
		CostFixedLimit:          config.CostFixedLimit,
	}, nil
}

func resolveMarket(id string, topology Topology, assets map[market.AssetID]market.Asset) (ResolvedMarket, ResolvedChain, error) {
	config, ok := topology.Markets[id]
	if !ok {
		return ResolvedMarket{}, ResolvedChain{}, fmt.Errorf("unknown market %q", id)
	}
	if config.QuoteSource != "" && config.Chain != "" {
		if config.Venue != "" || config.Path != "" {
			return ResolvedMarket{}, ResolvedChain{}, fmt.Errorf("remote market %q requires chain and quote_source without venue or path", id)
		}
		chain, err := resolveChain(config.Chain, topology.Chains[config.Chain])
		if err != nil {
			return ResolvedMarket{}, ResolvedChain{}, err
		}
		base, err := resolveToken(config.BaseToken, topology.Tokens[config.BaseToken], chain.ID, assets)
		if err != nil {
			return ResolvedMarket{}, ResolvedChain{}, err
		}
		quote, err := resolveToken(config.QuoteToken, topology.Tokens[config.QuoteToken], chain.ID, assets)
		if err != nil || base.Token.ID == quote.Token.ID || base.Token.Asset == quote.Token.Asset {
			return ResolvedMarket{}, ResolvedChain{}, fmt.Errorf("market %q requires distinct valid endpoints", id)
		}
		triggers, err := resolveTriggerPools(config.TriggerPools, nil, chain.ID, topology)
		if err != nil {
			return ResolvedMarket{}, ResolvedChain{}, fmt.Errorf("market %q trigger pools: %w", id, err)
		}
		if len(triggers) == 0 {
			return ResolvedMarket{}, ResolvedChain{}, fmt.Errorf("remote market %q requires trigger pools", id)
		}
		for _, trigger := range triggers {
			if strings.TrimSpace(trigger.Kind) == "" {
				return ResolvedMarket{}, ResolvedChain{}, fmt.Errorf("remote market %q trigger pool %q requires kind", id, trigger.ID)
			}
			if chain.Kind == "evm" && !common.IsHexAddress(trigger.Address) {
				return ResolvedMarket{}, ResolvedChain{}, fmt.Errorf("trigger pool %q has invalid EVM address", trigger.ID)
			}
			if chain.Kind == "solana" && len(trigger.Address) < 32 {
				return ResolvedMarket{}, ResolvedChain{}, fmt.Errorf("trigger pool %q has invalid Solana public key", trigger.ID)
			}
		}
		return ResolvedMarket{
			ID: market.MarketID(id), Chain: chain.ID, Base: base, Quote: quote,
			QuoteSource: config.QuoteSource, TriggerPools: triggers, ReferenceQuote: config.ReferenceQuote,
		}, chain, nil
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
		triggers, err := resolveTriggerPools(config.TriggerPools, resolvedPath, chain.ID, topology)
		if err != nil {
			return ResolvedMarket{}, ResolvedChain{}, fmt.Errorf("market %q trigger pools: %w", id, err)
		}
		if config.QuoteSource == "" && len(config.TriggerPools) > 0 {
			return ResolvedMarket{}, ResolvedChain{}, fmt.Errorf("market %q trigger pools require quote_source", id)
		}
		return ResolvedMarket{
			ID: market.MarketID(id), Chain: chain.ID, Venue: resolvedPath[0].Venue,
			Base: base, Quote: quote, Path: resolvedPath, QuoteSource: config.QuoteSource,
			TriggerPools: triggers, ReferenceQuote: config.ReferenceQuote,
		}, chain, nil
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
	path := []ResolvedHop{{Pool: config.Venue, Venue: venue, In: base, Out: quote}}
	triggers, err := resolveTriggerPools(config.TriggerPools, path, chain.ID, topology)
	if err != nil {
		return ResolvedMarket{}, ResolvedChain{}, fmt.Errorf("market %q trigger pools: %w", id, err)
	}
	if config.QuoteSource == "" && len(config.TriggerPools) > 0 {
		return ResolvedMarket{}, ResolvedChain{}, fmt.Errorf("market %q trigger pools require quote_source", id)
	}
	return ResolvedMarket{
		ID: market.MarketID(id), Chain: chain.ID, Venue: venue, Base: base, Quote: quote,
		Path: path, QuoteSource: config.QuoteSource, TriggerPools: triggers, ReferenceQuote: config.ReferenceQuote,
	}, chain, nil
}

func resolveTriggerPools(configured []string, path []ResolvedHop, chain string, topology Topology) ([]ResolvedTriggerPool, error) {
	ids := append([]string(nil), configured...)
	if len(ids) == 0 {
		for _, hop := range path {
			ids = append(ids, hop.Pool)
		}
	}
	result := make([]ResolvedTriggerPool, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			return nil, fmt.Errorf("pool ID cannot be empty")
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, fmt.Errorf("duplicate pool %q", id)
		}
		seen[id] = struct{}{}
		pool, ok := topology.Pools[id]
		if !ok {
			// Legacy one-pool markets identify their pool through the venue.
			for _, hop := range path {
				if hop.Pool == id && hop.Venue.PoolText != "" {
					result = append(result, ResolvedTriggerPool{
						ID: id, Chain: chain, Kind: hop.Venue.Kind, Address: hop.Venue.PoolText,
					})
					ok = true
					break
				}
			}
			if !ok {
				return nil, fmt.Errorf("unknown pool %q", id)
			}
			continue
		}
		poolChain := pool.Chain
		if poolChain == "" {
			poolChain = chain
		}
		if poolChain != chain {
			return nil, fmt.Errorf("pool %q belongs to chain %q, market uses %q", id, poolChain, chain)
		}
		address := strings.TrimSpace(pool.Address)
		if address == "" {
			venue, venueOK := topology.Venues[pool.Venue]
			if !venueOK {
				return nil, fmt.Errorf("pool %q references unknown venue", id)
			}
			address = strings.TrimSpace(venue.PoolAddress)
		}
		if address == "" {
			return nil, fmt.Errorf("pool %q has no address", id)
		}
		kind := strings.TrimSpace(pool.Kind)
		if kind == "" && pool.Venue != "" {
			if venue, venueOK := topology.Venues[pool.Venue]; venueOK {
				kind = venue.Kind
			}
		}
		result = append(result, ResolvedTriggerPool{ID: id, Chain: poolChain, Kind: kind, Address: address})
	}
	return result, nil
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
	if config.Kind != "uniswap_v2" && config.Kind != "uniswap_v3" && config.Kind != "pancakeswap_v3" && config.Kind != "aerodrome_slipstream" && config.Kind != "aerodrome_volatile" && config.Kind != "meteora_dlmm" && config.Kind != "orca_whirlpool" {
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
	if config.Kind != "uniswap_v3" && config.Kind != "pancakeswap_v3" && config.Kind != "meteora_dlmm" && config.Kind != "orca_whirlpool" || config.FactoryAddress != "" {
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
	if config.Kind == "uniswap_v3" || config.Kind == "pancakeswap_v3" || config.Kind == "aerodrome_slipstream" || config.Kind == "orca_whirlpool" {
		if config.MaxTickWords == 0 {
			config.MaxTickWords = 64
		}
		if config.MaxTickWords < 1 || config.MaxTickWords > 512 {
			return ResolvedVenue{}, fmt.Errorf("venue %q has invalid tick coverage", id)
		}
	}
	return ResolvedVenue{ID: id, Kind: config.Kind, Chain: config.Chain, Pool: pool, PoolText: poolText, Factory: factory, Reference: reference, FeeBPS: config.FeeBPS, Stable: config.Stable, MaxTickWords: config.MaxTickWords}, nil
}

func resolveQuoteSource(id string, config QuoteSourceConfig) (ResolvedQuoteSource, error) {
	if strings.TrimSpace(id) == "" {
		return ResolvedQuoteSource{}, fmt.Errorf("quote source ID is required")
	}
	switch config.Kind {
	case "jupiter":
		if config.TakerEnv != "" && !environmentName.MatchString(config.TakerEnv) ||
			config.ClientIDEnv != "" || config.ChainSlug != "" {
			return ResolvedQuoteSource{}, fmt.Errorf("quote source %q has invalid Jupiter profile", id)
		}
		if config.QuotePath != "" &&
			(!strings.HasPrefix(config.QuotePath, "/") || strings.ContainsAny(config.QuotePath, "?#")) {
			return ResolvedQuoteSource{}, fmt.Errorf("quote source %q has invalid Jupiter quote path", id)
		}
		config.ExpectedMode = strings.ToLower(strings.TrimSpace(config.ExpectedMode))
		if config.ExpectedMode != "" && config.ExpectedMode != "manual" && config.ExpectedMode != "ultra" {
			return ResolvedQuoteSource{}, fmt.Errorf("quote source %q has invalid Jupiter expected mode", id)
		}
		if config.ExpectedMode != "" && config.QuotePath == "" {
			return ResolvedQuoteSource{}, fmt.Errorf("quote source %q expected mode requires an explicit quote path", id)
		}
		config.SwapMode = strings.TrimSpace(config.SwapMode)
		if config.SwapMode != "" && config.SwapMode != "ExactIn" {
			return ResolvedQuoteSource{}, fmt.Errorf("quote source %q has invalid Jupiter swap mode", id)
		}
		config.BroadcastFeeType = strings.TrimSpace(config.BroadcastFeeType)
		if config.BroadcastFeeType != "" && config.BroadcastFeeType != "maxCap" &&
			config.BroadcastFeeType != "exactFee" {
			return ResolvedQuoteSource{}, fmt.Errorf("quote source %q has invalid Jupiter broadcast fee type", id)
		}
		config.ExcludeDexes = strings.TrimSpace(config.ExcludeDexes)
		config.ExcludeRouters = strings.TrimSpace(config.ExcludeRouters)
		config.ClientPlatform = strings.TrimSpace(config.ClientPlatform)
	case "kyberswap":
		if !environmentName.MatchString(config.ClientIDEnv) || strings.TrimSpace(config.ChainSlug) == "" ||
			config.TakerEnv != "" || config.APIKeyEnv != "" || config.MaxAccounts != 0 ||
			config.QuotePath != "" || config.ExpectedMode != "" || config.SwapMode != "" ||
			config.PriorityFeeLamports != 0 || config.BroadcastFeeType != "" || config.UseWSOL != nil ||
			config.ExcludeDexes != "" || config.ExcludeRouters != "" || config.ClientPlatform != "" {
			return ResolvedQuoteSource{}, fmt.Errorf("quote source %q has invalid KyberSwap profile", id)
		}
	default:
		return ResolvedQuoteSource{}, fmt.Errorf("quote source %q has unsupported kind %q", id, config.Kind)
	}
	if config.APIKeyEnv != "" && !environmentName.MatchString(config.APIKeyEnv) {
		return ResolvedQuoteSource{}, fmt.Errorf("quote source %q has invalid API key environment", id)
	}
	if config.SlippageBPS > 10_000 || config.MaxAccounts > 256 {
		return ResolvedQuoteSource{}, fmt.Errorf("quote source %q has invalid request limits", id)
	}
	return ResolvedQuoteSource{
		ID: id, Kind: config.Kind, BaseURL: config.BaseURL, QuotePath: config.QuotePath,
		ExpectedMode: config.ExpectedMode, TakerEnv: config.TakerEnv,
		APIKeyEnv: config.APIKeyEnv, ClientIDEnv: config.ClientIDEnv, ChainSlug: config.ChainSlug,
		SlippageBPS: config.SlippageBPS, MaxAccounts: config.MaxAccounts,
		SwapMode: config.SwapMode, PriorityFeeLamports: config.PriorityFeeLamports,
		BroadcastFeeType: config.BroadcastFeeType, UseWSOL: config.UseWSOL,
		ExcludeDexes: config.ExcludeDexes, ExcludeRouters: config.ExcludeRouters,
		ClientPlatform: config.ClientPlatform,
	}, nil
}

func resolveTransferSource(
	id string,
	config TransferSourceConfig,
) (ResolvedTransferSource, error) {
	id = strings.TrimSpace(id)
	config.Kind = strings.TrimSpace(config.Kind)
	config.BaseURL = strings.TrimSpace(config.BaseURL)
	config.Profile = strings.TrimSpace(config.Profile)
	config.APIKeyEnv = strings.TrimSpace(config.APIKeyEnv)
	config.IntegratorIDEnv = strings.TrimSpace(config.IntegratorIDEnv)
	if id == "" || config.Kind == "" {
		return ResolvedTransferSource{}, fmt.Errorf(
			"transfer sources require an ID and adapter kind",
		)
	}
	for _, candidate := range []string{
		config.APIKeyEnv,
		config.IntegratorIDEnv,
	} {
		if candidate != "" && !environmentName.MatchString(candidate) {
			return ResolvedTransferSource{}, fmt.Errorf(
				"transfer source %q has an invalid environment name",
				id,
			)
		}
	}
	return ResolvedTransferSource{
		ID: id, Kind: config.Kind, BaseURL: config.BaseURL,
		Profile: config.Profile, APIKeyEnv: config.APIKeyEnv,
		IntegratorIDEnv: config.IntegratorIDEnv,
	}, nil
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
