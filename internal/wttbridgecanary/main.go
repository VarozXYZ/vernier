// Package wttbridgecanary provides a deliberately narrow, durable manual WTT
// entrypoint. It is read-only unless both arm barriers are supplied.
package wttbridgecanary

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"gopkg.in/yaml.v3"

	evmadapter "github.com/VarozXYZ/vernier/adapters/chain/evm"
	"github.com/VarozXYZ/vernier/adapters/crosschain/wormholentt"
	"github.com/VarozXYZ/vernier/adapters/crosschain/wormholewtt"
	sqlitestore "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	crosschainport "github.com/VarozXYZ/vernier/ports/crosschain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	"github.com/VarozXYZ/vernier/runtime/configuration"
)

const defaultStorePath = ".vernier/wtt-bridge-canary.sqlite"

var publicGuardianEndpoints = []string{
	"https://wormhole-v2-mainnet-api.mcf.rocks",
	"https://wormhole-v2-mainnet-api.chainlayer.network",
	"https://wormhole-v2-mainnet-api.staking.fund",
	"https://api.wormholescan.io/api/v1/vaas",
}

type profile struct {
	SchemaVersion  int                     `yaml:"schema_version"`
	PollIntervalMS int                     `yaml:"poll_interval_ms"`
	Chains         map[string]profileChain `yaml:"chains"`
}

type profileChain struct {
	WormholeChainID uint16 `yaml:"wormhole_chain_id"`
	CoreBridge      string `yaml:"core_bridge"`
	TokenBridge     string `yaml:"token_bridge"`
}

type composed struct {
	service     *wormholewtt.LiveService
	chains      map[string]wormholewtt.LiveChain
	markets     map[string]configuration.ResolvedMarket
	clients     []*ethclient.Client
	managers    map[string]*evmadapter.TxManager
	config      configuration.ParsedLiveConfig
	store       *sqlitestore.SequentialLiveStore
	storePath   string
	owner       common.Address
	poll        time.Duration
	timeout     time.Duration
	closeCalled bool
}

func (c *composed) close() {
	if c == nil || c.closeCalled {
		return
	}
	c.closeCalled = true
	if c.store != nil {
		_ = c.store.Close()
	}
	for _, client := range c.clients {
		client.Close()
	}
}

func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("wtt-bridge-canary", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "private modular Live manifest")
	envPath := flags.String("env-file", "", "isolated local environment file")
	source := flags.String("source", "", "configured source chain ID")
	destination := flags.String("destination", "", "configured destination chain ID")
	amount := flags.String("amount", "", "exact whole-token decimal amount")
	confirmAmount := flags.String("confirm-amount", "", "must exactly match --amount when armed")
	arm := flags.Bool("arm", false, "enable source and redeem broadcasts")
	resume := flags.String("resume-operation", "", "resume one exact durable operation")
	storePath := flags.String("store", defaultStorePath, "WAL FULL operation journal")
	timeout := flags.Duration("confirmation-timeout", 0, "source, VAA, and redeem timeout")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "wtt-bridge-canary: positional arguments are not supported")
		return 2
	}
	resumeID := strings.TrimSpace(*resume)
	if strings.TrimSpace(*configPath) == "" || strings.TrimSpace(*envPath) == "" {
		fmt.Fprintln(stderr, "wtt-bridge-canary: --config and --env-file are required")
		return 2
	}
	if resumeID != "" {
		if !*arm || *source != "" || *destination != "" || *amount != "" || *confirmAmount != "" {
			fmt.Fprintln(stderr, "wtt-bridge-canary: --resume-operation requires --arm and cannot be combined with transfer fields")
			return 2
		}
	} else {
		if strings.TrimSpace(*source) == "" || strings.TrimSpace(*destination) == "" || *source == *destination {
			fmt.Fprintln(stderr, "wtt-bridge-canary: distinct --source and --destination values are required")
			return 2
		}
		parsedAmount, ok := new(big.Rat).SetString(strings.TrimSpace(*amount))
		if !ok || parsedAmount.Sign() <= 0 || strings.Contains(*amount, "/") {
			fmt.Fprintln(stderr, "wtt-bridge-canary: --amount must be a positive exact decimal")
			return 2
		}
		if *arm && *confirmAmount != *amount {
			fmt.Fprintln(stderr, "wtt-bridge-canary: --confirm-amount must exactly match --amount when armed")
			return 2
		}
		if !*arm && *confirmAmount != "" {
			fmt.Fprintln(stderr, "wtt-bridge-canary: --confirm-amount requires --arm")
			return 2
		}
	}

	lookup, err := configuration.ReadIsolatedEnvFile(*envPath)
	if err != nil {
		fmt.Fprintln(stderr, "wtt-bridge-canary: cannot load isolated environment file")
		return 2
	}
	config, err := configuration.LoadLiveConfig(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "wtt-bridge-canary: %v\n", err)
		return 2
	}
	if config.BaseTransferSource.Kind != "wormhole_wtt" || strings.TrimSpace(config.BaseTransferSource.Profile) == "" {
		fmt.Fprintln(stderr, "wtt-bridge-canary: active Live profile does not configure Wormhole WTT")
		return 2
	}
	if *timeout <= 0 {
		*timeout = config.ConfirmationTimeout
	}
	if *timeout <= 0 {
		fmt.Fprintln(stderr, "wtt-bridge-canary: confirmation timeout must be positive")
		return 2
	}

	bridge, err := compose(ctx, *configPath, config, lookup, *storePath, *timeout, stdout)
	if err != nil {
		fmt.Fprintf(stderr, "wtt-bridge-canary: %v\n", err)
		return 1
	}
	defer bridge.close()

	if resumeID != "" {
		if err := bridge.resume(ctx, execution.OperationID(resumeID), stdout); err != nil {
			fmt.Fprintf(stderr, "wtt-bridge-canary: %v\n", err)
			return 1
		}
		return 0
	}

	request, err := bridge.request(*source, *destination, *amount, "")
	if err != nil {
		fmt.Fprintf(stderr, "wtt-bridge-canary: %v\n", err)
		return 2
	}
	if err := bridge.preflight(ctx, request); err != nil {
		fmt.Fprintf(stderr, "wtt-bridge-canary: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "wtt_preflight status=ready source=%s destination=%s amount=%s input_units=%s owner=%s broadcast=%t\n",
		*source, *destination, *amount, request.Input.Units(), bridge.owner.Hex(), *arm)
	if !*arm {
		return 0
	}
	if err := bridge.execute(ctx, request, stdout); err != nil {
		fmt.Fprintf(stderr, "wtt-bridge-canary: %v\n", err)
		return 1
	}
	return 0
}

func compose(ctx context.Context, manifestPath string, config configuration.ParsedLiveConfig,
	lookup configuration.LookupEnv, storePath string, timeout time.Duration, output io.Writer,
) (*composed, error) {
	endpoints, err := config.ResolveEndpoints(lookup)
	if err != nil {
		return nil, err
	}
	profilePath := filepath.Join(filepath.Dir(manifestPath), config.BaseTransferSource.Profile)
	deployments, poll, err := loadProfile(profilePath)
	if err != nil {
		return nil, fmt.Errorf("load WTT profile: %w", err)
	}
	guardian, err := wormholentt.NewGuardianClient(wormholentt.GuardianClientConfig{
		Endpoints: publicGuardianEndpoints, PollInterval: poll, Clock: time.Now,
	})
	if err != nil {
		return nil, err
	}
	result := &composed{chains: make(map[string]wormholewtt.LiveChain, 2), markets: make(map[string]configuration.ResolvedMarket, 2),
		managers: make(map[string]*evmadapter.TxManager, 2),
		config:   config, storePath: storePath, poll: poll, timeout: timeout}
	fail := func(cause error) (*composed, error) { result.close(); return nil, cause }

	var owner common.Address
	for _, configured := range config.Markets {
		chainID := configured.Chain
		chain := config.Chains[chainID]
		if chain.Kind != "evm" {
			return fail(fmt.Errorf("WTT manual canary requires EVM chains"))
		}
		deployment, ok := deployments[chainID]
		if !ok {
			return fail(fmt.Errorf("WTT deployment for chain %q is unavailable", chainID))
		}
		account, ok := config.Accounts[chainID]
		if !ok {
			return fail(fmt.Errorf("live account for chain %q is unavailable", chainID))
		}
		keyText, ok := lookup(account.SignerEnv)
		if !ok || strings.TrimSpace(keyText) == "" {
			return fail(fmt.Errorf("signer for chain %q is unavailable in the isolated environment", chainID))
		}
		privateKey, err := parsePrivateKey(keyText)
		if err != nil {
			return fail(fmt.Errorf("signer for chain %q is invalid", chainID))
		}
		derived := crypto.PubkeyToAddress(privateKey.PublicKey)
		publicText, hasPublic := lookup(account.PublicAddressEnv)
		if hasPublic && strings.TrimSpace(publicText) != "" &&
			(!common.IsHexAddress(strings.TrimSpace(publicText)) || common.HexToAddress(publicText) != derived) {
			return fail(fmt.Errorf("configured public address for chain %q does not match its signer", chainID))
		}
		if owner != (common.Address{}) && owner != derived {
			return fail(fmt.Errorf("WTT manual canary requires the same EVM signer on both chains"))
		}
		owner = derived
		httpURL := endpoints[chainID+".http"]
		if httpURL == "" {
			httpURL = endpoints[chainID]
		}
		client, err := ethclient.DialContext(ctx, httpURL)
		if err != nil {
			return fail(fmt.Errorf("dial chain %q: %w", chainID, err))
		}
		result.clients = append(result.clients, client)
		manager, err := evmadapter.NewTxManager(evmadapter.TxManagerConfig{
			Chain: market.ChainID(chainID), Account: execution.AccountID(account.ID), ChainID: chain.ChainID,
			PrivateKey: privateKey, Primary: client, Simulator: client, Fanout: map[string]evmadapter.TxClient{"primary": client},
			DefaultGasLimit: 1_500_000, Clock: time.Now, FeeRefreshInterval: config.FeeCacheMaxAge,
		})
		if err != nil {
			return fail(err)
		}
		if err := manager.Warm(ctx); err != nil {
			return fail(fmt.Errorf("warm TxManager for chain %q: %w", chainID, err))
		}
		result.managers[chainID] = manager
		if !allowedWTTSpender(config, market.ChainID(chainID), configured.Base.Token.ID, deployment.TokenBridge) {
			return fail(fmt.Errorf("WTT token bridge for chain %q is not in the exact approval allowlist", chainID))
		}
		native, ok := config.NativeAssets[chainID]
		if !ok || native.Asset == "" {
			return fail(fmt.Errorf("native asset for chain %q is unavailable", chainID))
		}
		result.chains[chainID] = wormholewtt.LiveChain{
			ID: market.ChainID(chainID), WormholeID: deployment.WormholeChainID,
			CoreBridge: deployment.CoreBridge, TokenBridge: deployment.TokenBridge,
			Token: configured.Base.Token, TokenAddress: configured.Base.Address,
			Owner: derived, Manager: manager, Client: client, NativeAsset: native.Asset,
		}
		result.markets[chainID] = configured
	}
	result.owner = owner
	store, err := sqlitestore.OpenSequentialLive(storePath)
	if err != nil {
		return fail(err)
	}
	result.store = store
	service, err := wormholewtt.NewLiveService(wormholewtt.LiveServiceConfig{
		Chains: map[market.ChainID]wormholewtt.LiveChain{
			market.ChainID(config.Markets[0].Chain): result.chains[config.Markets[0].Chain],
			market.ChainID(config.Markets[1].Chain): result.chains[config.Markets[1].Chain],
		},
		Attestations: guardian, Clock: time.Now, PollInterval: poll, Timeout: timeout,
		Trace: func(phase string) { fmt.Fprintf(output, "wtt_phase=%s\n", phase) },
	})
	if err != nil {
		return fail(err)
	}
	result.service = service
	return result, nil
}

func (c *composed) request(source, destination, amount, planID string) (execution.SequentialStageRequest, error) {
	sourceMarket, sourceOK := c.markets[source]
	destinationMarket, destinationOK := c.markets[destination]
	if !sourceOK || !destinationOK || source == destination {
		return execution.SequentialStageRequest{}, fmt.Errorf("source and destination must name the two configured chains")
	}
	units, err := amountUnits(amount, sourceMarket.Base.Token.Decimals)
	if err != nil {
		return execution.SequentialStageRequest{}, err
	}
	input, _ := market.NewTokenAmount(sourceMarket.Base.Token.ID, units)
	if planID == "" {
		planID = "wtt-manual-preview"
	}
	request := execution.SequentialStageRequest{
		Plan: execution.PlanID(planID),
		Stage: execution.SequentialStagePlan{Ordinal: 1, Stage: execution.StageBridgeBase,
			SourceChain: market.ChainID(source), DestinationChain: market.ChainID(destination),
			InputToken: sourceMarket.Base.Token.ID, OutputToken: destinationMarket.Base.Token.ID},
		Input: input,
	}
	return request, nil
}

func (c *composed) preflight(ctx context.Context, request execution.SequentialStageRequest) error {
	source := c.chains[string(request.Stage.SourceChain)]
	destination := c.chains[string(request.Stage.DestinationChain)]
	transferable, dust, err := wormholewtt.TrimTransferAmount(request.Input.Units(), source.Token.Decimals)
	if err != nil {
		return err
	}
	if dust.Sign() != 0 {
		return fmt.Errorf("amount produces %s source units of WTT precision dust; choose an exact 8-decimal amount", dust)
	}
	balance, err := erc20Uint(ctx, source.Client, source.TokenAddress, "balanceOf(address)", source.Owner)
	if err != nil {
		return fmt.Errorf("read source token balance: %w", err)
	}
	if balance.Cmp(transferable) < 0 {
		return fmt.Errorf("insufficient confirmed source balance: have %s units, need %s", balance, transferable)
	}
	allowance, err := erc20Allowance(ctx, source.Client, source.TokenAddress, source.Owner, source.TokenBridge)
	if err != nil {
		return fmt.Errorf("read WTT allowance: %w", err)
	}
	if allowance.Cmp(transferable) < 0 {
		return fmt.Errorf("insufficient WTT allowance: have %s units, need %s; run the armed Live approval bootstrap first", allowance, transferable)
	}
	messageFee, _, err := c.service.MessageFee(ctx, source.ID)
	if err != nil {
		return fmt.Errorf("read WTT message fee: %w", err)
	}
	if err := c.requireNativeBalance(ctx, source, 500_000, messageFee); err != nil {
		return err
	}
	if err := c.requireNativeBalance(ctx, destination, 1_500_000, new(big.Int)); err != nil {
		return err
	}
	return nil
}

func (c *composed) requireNativeBalance(ctx context.Context, chain wormholewtt.LiveChain, gas uint64, value *big.Int) error {
	manager := c.managers[string(chain.ID)]
	fees, ok := manager.FeeSnapshot()
	if !ok || fees.FeeCap == nil || fees.FeeCap.Sign() <= 0 {
		return fmt.Errorf("fee cache for chain %q is unavailable", chain.ID)
	}
	required := new(big.Int).Mul(new(big.Int).SetUint64(gas), fees.FeeCap)
	required.Add(required, value)
	balance, err := chain.Client.BalanceAt(ctx, chain.Owner, nil)
	if err != nil {
		return fmt.Errorf("read native balance for chain %q: %w", chain.ID, err)
	}
	if balance.Cmp(required) < 0 {
		return fmt.Errorf("insufficient native balance on chain %q: have %s wei, conservative requirement %s wei", chain.ID, balance, required)
	}
	return nil
}

func (c *composed) execute(ctx context.Context, request execution.SequentialStageRequest, output io.Writer) error {
	active, found, err := c.store.ActiveSequentialOperation(ctx)
	if err != nil {
		return err
	}
	if found {
		return fmt.Errorf("operation %s is %s; resume it with --resume-operation %s --arm", active.ID, active.State, active.ID)
	}
	id, err := operationID()
	if err != nil {
		return err
	}
	request.Operation = id
	request.Plan = execution.PlanID("wtt-manual/" + string(id))
	now := time.Now().UTC()
	operation := execution.SequentialOperation{ID: id, Plan: request.Plan, OpportunityID: "manual-wtt-transfer",
		ConfigHash: c.config.Hash, State: execution.SequentialRunning, CurrentAmount: request.Input, StartedAt: now, UpdatedAt: now}
	if err := c.store.CreateSequentialOperation(ctx, operation); err != nil {
		return err
	}
	fmt.Fprintf(output, "wtt_operation id=%s state=running journal=%s\n", id, c.storePath)
	transferCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	result, err := c.service.Transfer(transferCtx, request, c.store)
	if err != nil {
		return c.finishFailure(ctx, id, err)
	}
	return c.complete(ctx, request, resultToSettlement(request, result), output)
}

func (c *composed) resume(ctx context.Context, id execution.OperationID, output io.Writer) error {
	active, found, err := c.store.ActiveSequentialOperation(ctx)
	if err != nil {
		return err
	}
	if !found || active.ID != id {
		return fmt.Errorf("active operation %s was not found in %s", id, c.storePath)
	}
	if active.ConfigHash != c.config.Hash {
		return fmt.Errorf("operation %s belongs to another configuration hash", id)
	}
	if active.CurrentStage == 1 {
		if err := c.store.FinishSequentialOperation(ctx, id, execution.SequentialCompleted, nil); err != nil {
			return err
		}
		fmt.Fprintf(output, "wtt_result operation=%s state=completed recovery=journal_finalize\n", id)
		return nil
	}
	if active.CurrentStage != 0 {
		return fmt.Errorf("operation %s has unsupported durable stage %d", id, active.CurrentStage)
	}
	var source, destination string
	for chain, configured := range c.markets {
		if configured.Base.Token.ID == active.CurrentAmount.Token() {
			source = chain
		} else {
			destination = chain
		}
	}
	if source == "" || destination == "" {
		return fmt.Errorf("operation %s token does not identify a configured WTT direction", id)
	}
	request := execution.SequentialStageRequest{Operation: id, Plan: active.Plan,
		Stage: execution.SequentialStagePlan{Ordinal: 1, Stage: execution.StageBridgeBase,
			SourceChain: market.ChainID(source), DestinationChain: market.ChainID(destination),
			InputToken: c.markets[source].Base.Token.ID, OutputToken: c.markets[destination].Base.Token.ID},
		Input: active.CurrentAmount}
	records, err := c.store.SequentialTransactions(ctx, id)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		if err := c.preflight(ctx, request); err != nil {
			return fmt.Errorf("repeat source preflight: %w", err)
		}
	} else if !hasPhase(records, "wtt_redeem") {
		if err := c.requireNativeBalance(ctx, c.chains[destination], 1_500_000, new(big.Int)); err != nil {
			return err
		}
	}
	if active.State == execution.SequentialRecoveryBlocked {
		if err := c.store.RetryBlockedSequentialRecovery(ctx, id); err != nil {
			return err
		}
	}
	fmt.Fprintf(output, "wtt_operation id=%s state=recovering durable_transactions=%d\n", id, len(records))
	transferCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	result, err := c.service.RecoverTransfer(transferCtx, request, records, c.store)
	if err != nil {
		return c.finishFailure(ctx, id, err)
	}
	return c.complete(ctx, request, resultToSettlement(request, result), output)
}

func hasPhase(records []executionport.SequentialTransactionRecord, phase string) bool {
	for _, record := range records {
		if record.Phase == phase {
			return true
		}
	}
	return false
}

func (c *composed) finishFailure(ctx context.Context, id execution.OperationID, cause error) error {
	state := execution.SequentialRecoveryBlocked
	disposition := executionport.ErrorDisposition(cause)
	if disposition == executionport.DispositionRejected || disposition == executionport.DispositionConfirmedFailure {
		state = execution.SequentialAborted
	}
	if err := c.store.FinishSequentialOperation(context.WithoutCancel(ctx), id, state, cause); err != nil {
		return fmt.Errorf("operation %s failed and its terminal state could not be persisted: %w", id, err)
	}
	if state == execution.SequentialAborted {
		return fmt.Errorf("operation %s ended without an uncertain effect: %w", id, cause)
	}
	return fmt.Errorf("operation %s requires recovery: %w; resume with --resume-operation %s --arm", id, cause, id)
}

func (c *composed) complete(ctx context.Context, request execution.SequentialStageRequest,
	settlement execution.SequentialStageSettlement, output io.Writer,
) error {
	if err := c.store.RecordStageSettlement(context.WithoutCancel(ctx), settlement); err != nil {
		return fmt.Errorf("persist WTT settlement: %w", err)
	}
	if err := c.store.FinishSequentialOperation(context.WithoutCancel(ctx), request.Operation, execution.SequentialCompleted, nil); err != nil {
		return fmt.Errorf("finish WTT operation: %w", err)
	}
	fmt.Fprintf(output, "wtt_result operation=%s state=completed source_tx=%s destination_tx=%s input_units=%s output_units=%s evidence=%s\n",
		request.Operation, settlement.SourceIdentity.Hash, settlement.DestinationIdentity.Hash,
		settlement.ActualInput.Units(), settlement.ActualOutput.Units(), settlement.Evidence)
	return nil
}

func resultToSettlement(request execution.SequentialStageRequest, result crosschainport.LiveTransferResult) execution.SequentialStageSettlement {
	destination := result.DestinationIdentity
	return execution.SequentialStageSettlement{
		Request: request, ActualInput: result.ActualInput, ActualOutput: result.ActualOutput,
		Costs: result.Costs, SourceIdentity: result.SourceIdentity, DestinationIdentity: &destination,
		DestinationBalanceBefore: result.DestinationBalanceBefore,
		DestinationBalanceAfter:  result.DestinationBalanceAfter,
		ObservedAt:               result.ObservedAt, Evidence: result.Evidence,
	}
}

func parsePrivateKey(value string) (*ecdsa.PrivateKey, error) {
	return crypto.HexToECDSA(strings.TrimPrefix(strings.TrimSpace(value), "0x"))
}

func amountUnits(text string, decimals uint8) (*big.Int, error) {
	value, ok := new(big.Rat).SetString(strings.TrimSpace(text))
	if !ok || value.Sign() <= 0 {
		return nil, fmt.Errorf("amount must be a positive exact decimal")
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	scaled := new(big.Rat).Mul(value, new(big.Rat).SetInt(scale))
	if scaled.Denom().Cmp(big.NewInt(1)) != 0 {
		return nil, fmt.Errorf("amount has more than %d decimal places", decimals)
	}
	return new(big.Int).Set(scaled.Num()), nil
}

func operationID() (execution.OperationID, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return execution.OperationID("manual-wtt-" + hex.EncodeToString(raw)), nil
}

func allowedWTTSpender(config configuration.ParsedLiveConfig, chain market.ChainID, token market.TokenID, spender common.Address) bool {
	for _, candidate := range config.ApprovalSpenders {
		if candidate.Chain == chain && candidate.Token == token && candidate.Spender == spender && candidate.Purpose == "wormhole_wtt" {
			return true
		}
	}
	return false
}

func erc20Uint(ctx context.Context, client *ethclient.Client, token common.Address, signature string, address common.Address) (*big.Int, error) {
	selector := crypto.Keccak256([]byte(signature))[:4]
	data := make([]byte, 4+32)
	copy(data, selector)
	copy(data[4+12:], address.Bytes())
	raw, err := client.CallContract(ctx, geth.CallMsg{To: &token, Data: data}, nil)
	if err != nil {
		return nil, err
	}
	if len(raw) < 32 {
		return nil, fmt.Errorf("ERC-20 call returned truncated data")
	}
	return new(big.Int).SetBytes(raw[len(raw)-32:]), nil
}

func erc20Allowance(ctx context.Context, client *ethclient.Client, token, owner, spender common.Address) (*big.Int, error) {
	selector := crypto.Keccak256([]byte("allowance(address,address)"))[:4]
	data := make([]byte, 4+64)
	copy(data, selector)
	copy(data[4+12:4+32], owner.Bytes())
	copy(data[4+32+12:], spender.Bytes())
	raw, err := client.CallContract(ctx, geth.CallMsg{To: &token, Data: data}, nil)
	if err != nil {
		return nil, err
	}
	if len(raw) < 32 {
		return nil, fmt.Errorf("ERC-20 allowance returned truncated data")
	}
	return new(big.Int).SetBytes(raw[len(raw)-32:]), nil
}

func loadProfile(path string) (map[string]struct {
	WormholeChainID uint16
	CoreBridge      common.Address
	TokenBridge     common.Address
}, time.Duration, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}
	var decoded profile
	if err := yaml.Unmarshal(raw, &decoded); err != nil {
		return nil, 0, err
	}
	if decoded.SchemaVersion != 1 || len(decoded.Chains) != 2 || decoded.PollIntervalMS <= 0 {
		return nil, 0, fmt.Errorf("WTT profile is incomplete")
	}
	result := make(map[string]struct {
		WormholeChainID uint16
		CoreBridge      common.Address
		TokenBridge     common.Address
	}, len(decoded.Chains))
	for id, chain := range decoded.Chains {
		if id == "" || chain.WormholeChainID == 0 || !common.IsHexAddress(chain.CoreBridge) || !common.IsHexAddress(chain.TokenBridge) ||
			common.HexToAddress(chain.CoreBridge) == (common.Address{}) || common.HexToAddress(chain.TokenBridge) == (common.Address{}) {
			return nil, 0, fmt.Errorf("WTT chain profile %q is invalid", id)
		}
		result[id] = struct {
			WormholeChainID uint16
			CoreBridge      common.Address
			TokenBridge     common.Address
		}{chain.WormholeChainID, common.HexToAddress(chain.CoreBridge), common.HexToAddress(chain.TokenBridge)}
	}
	return result, time.Duration(decoded.PollIntervalMS) * time.Millisecond, nil
}

var _ executionport.SequentialJournal = (*sqlitestore.SequentialLiveStore)(nil)
