package livecanary

import (
	"context"
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	gethcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	solanago "github.com/gagliardetto/solana-go"

	evmadapter "github.com/VarozXYZ/vernier/adapters/chain/evm"
	solanaadapter "github.com/VarozXYZ/vernier/adapters/chain/solana"
	"github.com/VarozXYZ/vernier/adapters/crosschain/across"
	kyberexecution "github.com/VarozXYZ/vernier/adapters/execution/kyberswap"
	telegramnotification "github.com/VarozXYZ/vernier/adapters/notification/telegram"
	sqlitestore "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	"github.com/VarozXYZ/vernier/adapters/quote/jupiter"
	kyberquote "github.com/VarozXYZ/vernier/adapters/quote/kyberswap"
	"github.com/VarozXYZ/vernier/core/saga"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	"github.com/VarozXYZ/vernier/internal/acrossbridgecanary"
	"github.com/VarozXYZ/vernier/internal/nttmanualcanary"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	notificationport "github.com/VarozXYZ/vernier/ports/notification"
	"github.com/VarozXYZ/vernier/runtime/configuration"
	"github.com/VarozXYZ/vernier/runtime/livecompare"
)

const jupiterBuildBaseURL = "https://api.jup.ag"

const (
	solanaNativeTokenID    market.TokenID = "live_native_solana"
	evmNativeTokenID       market.TokenID = "live_native_evm"
	solanaNativeMint                      = "So11111111111111111111111111111111111111112"
	evmNativePseudoAddress                = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"
)

type ComposeConfig struct {
	ManifestPath     string
	Research         configuration.ParsedConfig
	Live             configuration.ParsedLiveConfig
	LookupEnv        configuration.LookupEnv
	Logger           *slog.Logger
	Output           io.Writer
	ObserveCostsOnly bool
	RefuelOnly       bool
	ForcedCanary     ForcedCanaryDirection
}

// ComposeArmed builds the signer-enabled sequential runtime. Normal execution
// cannot broadcast until Run receives a qualified opportunity; the explicit
// RefuelOnce path has its own arm barrier.
func ComposeArmed(ctx context.Context, config ComposeConfig) (_ *Runtime, err error) {
	if strings.TrimSpace(config.ManifestPath) == "" || config.LookupEnv == nil ||
		config.Output == nil ||
		(config.Live.ExecutionPolicyKind !=
			string(execution.PolicyTransportedSequential) &&
			config.Live.ExecutionPolicyKind !=
				string(execution.PolicyPrefundedSequential)) {
		return nil, fmt.Errorf("sequential Live composition configuration is incomplete")
	}
	if config.ForcedCanary != "" &&
		config.Live.RunTier != "canary" {
		return nil, fmt.Errorf(
			"forced canary execution requires sequential_bridge_canary",
		)
	}
	config.Output = &synchronizedWriter{delegate: config.Output}
	if config.Research.SetupID != config.Live.SetupID ||
		config.Research.Hash != config.Live.Hash {
		return nil, fmt.Errorf("research and Live configurations do not describe the same manifest")
	}
	endpoints, err := config.Research.ResolveEndpoints(config.LookupEnv)
	if err != nil {
		return nil, err
	}
	var cleanup []func()
	keep := false
	defer func() {
		if keep {
			return
		}
		for index := len(cleanup) - 1; index >= 0; index-- {
			cleanup[index]()
		}
	}()

	networks := make(livecompare.Networks)
	solanaNetworks := make(livecompare.SolanaNetworks)
	var solanaNetwork *solanaadapter.ReadOnlyNetwork
	var evmNetwork *evmadapter.ReadOnlyNetwork
	var solanaChain, evmChain string
	for id, profile := range config.Research.Chains {
		switch profile.Kind {
		case "solana":
			network, dialErr := solanaadapter.DialReadOnlyNetwork(
				ctx, profile.ID, profile.Label,
				endpoints[id+".http"], endpoints[id+".websocket"],
			)
			if dialErr != nil {
				return nil, dialErr
			}
			solanaNetworks[id] = network
			solanaNetwork, solanaChain = network, id
			cleanup = append(cleanup, network.Close)
		case "evm":
			network, dialErr := evmadapter.DialReadOnlyNetwork(
				ctx, profile.ID, profile.Label, profile.ChainID,
				endpoints[id], endpoints[id+".websocket"],
			)
			if dialErr != nil {
				return nil, dialErr
			}
			networks[id] = network
			evmNetwork, evmChain = network, id
			cleanup = append(cleanup, network.Close)
		default:
			return nil, fmt.Errorf("unsupported Live chain kind %q", profile.Kind)
		}
	}
	if solanaNetwork == nil || evmNetwork == nil {
		return nil, fmt.Errorf("sequential Live requires one Solana and one EVM chain")
	}

	runnerOptions := livecompare.Options{
		LookupEnv: config.LookupEnv, Logger: config.Logger,
		SolanaNetworks: solanaNetworks,
	}
	var liveNotifier *LiveNotifier
	if config.Research.TelegramEnabled && !config.ObserveCostsOnly {
		botToken, tokenErr := requiredEnv(
			config.LookupEnv, config.Research.TelegramBotTokenEnv,
		)
		if tokenErr != nil {
			return nil, tokenErr
		}
		chatID, chatErr := requiredEnv(
			config.LookupEnv, config.Research.TelegramChatIDEnv,
		)
		if chatErr != nil {
			return nil, chatErr
		}
		telegramSender, senderErr := telegramnotification.New(
			telegramnotification.Config{BotToken: botToken, ChatID: chatID},
		)
		if senderErr != nil {
			return nil, senderErr
		}
		// The execution observer folds opportunity discovery and every
		// subsequent action into one editable Telegram message.
		runnerOptions.OpeningAlerts = discardOpeningSender{}
		runnerOptions.ConfigurationAlerts = telegramSender
		liveNotifier, err = NewLiveNotifier(telegramSender, config.Logger)
		if err != nil {
			return nil, err
		}
		cleanup = append(cleanup, liveNotifier.Close)
	}
	solanaMarket, evmMarket, err := splitMarkets(
		config.Research, solanaChain, evmChain,
	)
	if err != nil {
		return nil, err
	}
	var forcedDirection *arbitrage.Direction
	if config.ForcedCanary != "" {
		resolved, resolveErr := config.ForcedCanary.Resolve(
			solanaMarket.ID,
			evmMarket.ID,
		)
		if resolveErr != nil {
			return nil, resolveErr
		}
		forcedDirection = &resolved
	}
	costValuator, err := composeCostValuator(
		ctx, config, solanaMarket, evmMarket,
	)
	if err != nil {
		return nil, err
	}
	var progressObserver executionport.SequentialObserver
	if liveNotifier != nil {
		progressObserver, err = NewProgressObserver(
			liveNotifier,
			map[market.TokenID]market.Token{
				solanaMarket.Base.Token.ID:  solanaMarket.Base.Token,
				solanaMarket.Quote.Token.ID: solanaMarket.Quote.Token,
				evmMarket.Base.Token.ID:     evmMarket.Base.Token,
				evmMarket.Quote.Token.ID:    evmMarket.Quote.Token,
			},
			map[market.ChainID]configuration.ResolvedChain{
				market.ChainID(solanaChain): config.Research.Chains[solanaChain],
				market.ChainID(evmChain):    config.Research.Chains[evmChain],
			},
			time.Now,
		)
		if err != nil {
			return nil, err
		}
	}
	solanaAccount, ok := config.Live.Accounts[solanaChain]
	if !ok {
		return nil, fmt.Errorf("solana Live account is not configured")
	}
	evmAccount, ok := config.Live.Accounts[evmChain]
	if !ok {
		return nil, fmt.Errorf("EVM Live account is not configured")
	}

	solanaKeyText, err := requiredEnv(config.LookupEnv, solanaAccount.SignerEnv)
	if err != nil {
		return nil, err
	}
	solanaKey, err := parseSolanaPrivateKey(solanaKeyText)
	if err != nil {
		return nil, err
	}
	evmKeyText, err := requiredEnv(config.LookupEnv, evmAccount.SignerEnv)
	if err != nil {
		return nil, err
	}
	evmKey, err := parseEVMPrivateKey(evmKeyText)
	if err != nil {
		return nil, err
	}
	evmSender := gethcrypto.PubkeyToAddress(evmKey.PublicKey)
	solanaBinding, err := composeSolanaSwap(
		ctx, config, solanaMarket, solanaAccount, solanaKey, solanaNetwork,
	)
	if err != nil {
		return nil, err
	}
	evmBinding, evmClosers, err := composeEVMSwap(
		ctx, config, evmMarket, evmAccount, evmKey, evmSender,
		endpoints[evmChain],
	)
	if err != nil {
		return nil, err
	}
	cleanup = append(cleanup, evmClosers...)
	var solanaSellPreflight, evmSellPreflight SellPreflight
	if config.Live.ExecutionPolicyKind ==
		string(execution.PolicyTransportedSequential) {
		solanaPreflightAddressText, preflightErr := requiredEnv(
			config.LookupEnv,
			solanaAccount.SellPreflightAddressEnv,
		)
		if preflightErr != nil {
			return nil, preflightErr
		}
		solanaPreflightAddress, preflightErr :=
			solanago.PublicKeyFromBase58(solanaPreflightAddressText)
		if preflightErr != nil {
			return nil, fmt.Errorf("invalid Solana sell preflight address")
		}
		solanaSellPreflight, preflightErr = composeSolanaSellPreflight(
			ctx, config, solanaMarket, solanaAccount, solanaKey,
			solanaPreflightAddress, solanaNetwork,
		)
		if preflightErr != nil {
			return nil, fmt.Errorf(
				"compose Solana sell preflight: %w", preflightErr,
			)
		}
		var evmPreflightClosers []func()
		evmSellPreflight, evmPreflightClosers, preflightErr =
			composeEVMSellPreflight(
				ctx, config, evmMarket, evmAccount, evmSender,
				endpoints[evmChain],
			)
		if preflightErr != nil {
			return nil, fmt.Errorf(
				"compose EVM sell preflight: %w", preflightErr,
			)
		}
		cleanup = append(cleanup, evmPreflightClosers...)
	}

	journal, err := sqlitestore.OpenSequentialLive(config.Live.OperationalStorePath)
	if err != nil {
		return nil, err
	}
	cleanup = append(cleanup, func() { _ = journal.Close() })

	acrossClient, err := across.New(across.Config{
		APIKey:       mustLookup(config.LookupEnv, "ACROSS_API_KEY"),
		IntegratorID: mustLookup(config.LookupEnv, "ACROSS_INTEGRATOR_ID"),
	})
	if err != nil {
		return nil, err
	}
	accounts := map[market.ChainID]execution.AccountID{
		market.ChainID(solanaChain): execution.AccountID(solanaAccount.ID),
		market.ChainID(evmChain):    execution.AccountID(evmAccount.ID),
	}
	storeStem := strings.TrimSuffix(config.Live.OperationalStorePath, filepath.Ext(config.Live.OperationalStorePath))
	quoteBridge, err := acrossbridgecanary.NewLiveService(
		acrossbridgecanary.LiveServiceConfig{
			Configuration: config.Research, Client: acrossClient,
			StorePath: storeStem + "-across.sqlite",
			Timeout:   config.Live.ConfirmationTimeout, Accounts: accounts,
			NonceCoordinator: evmBinding.NonceCoordinator,
			NativeAssets: map[market.ChainID]market.AssetID{
				market.ChainID(solanaChain): "sol",
				market.ChainID(evmChain):    "pol",
			},
			Output: config.Output,
		},
	)
	if err != nil {
		return nil, err
	}
	if err := quoteBridge.Warm(ctx); err != nil {
		return nil, fmt.Errorf("warm Across destination trackers: %w", err)
	}
	cleanup = append(cleanup, quoteBridge.Close)
	baseBridge, err := nttmanualcanary.NewLiveService(
		nttmanualcanary.LiveServiceConfig{
			ProfilePath: filepath.Join(
				filepath.Dir(config.ManifestPath), config.Live.BaseBridgeProfile,
			),
			StorePath:   storeStem + "-ntt.sqlite",
			SolanaChain: market.ChainID(solanaChain),
			EVMChain:    market.ChainID(evmChain), Accounts: accounts,
			NonceCoordinator:  evmBinding.NonceCoordinator,
			SolanaNativeAsset: "sol", EVMNativeAsset: "pol",
			ConfirmationTimeout: config.Live.ConfirmationTimeout,
			TokenDecimals: map[market.ChainID]uint8{
				market.ChainID(solanaChain): solanaMarket.Base.Token.Decimals,
				market.ChainID(evmChain):    evmMarket.Base.Token.Decimals,
			},
			Output: config.Output,
		},
	)
	if err != nil {
		return nil, err
	}

	if strings.TrimSpace(config.Live.CostCalibrationStore) == "" {
		return nil, fmt.Errorf(
			"complete-flow cost estimation requires cost_calibration_store",
		)
	}
	nttCalibration, err := sqlitestore.OpenNTTCanary(
		config.Live.CostCalibrationStore,
	)
	if err != nil {
		return nil, fmt.Errorf("open NTT cost calibration: %w", err)
	}
	cleanup = append(cleanup, func() { _ = nttCalibration.Close() })
	if strings.TrimSpace(config.Live.AcrossCostCalibrationStore) == "" {
		return nil, fmt.Errorf(
			"complete-flow cost estimation requires across_cost_calibration_store",
		)
	}
	acrossCalibration, err := sqlitestore.OpenAcrossCanary(
		config.Live.AcrossCostCalibrationStore,
	)
	if err != nil {
		return nil, fmt.Errorf("open Across cost calibration: %w", err)
	}
	cleanup = append(cleanup, func() { _ = acrossCalibration.Close() })
	evmFeeSource, ok := evmBinding.TxManager.(cachedEVMFeeSource)
	if !ok {
		return nil, fmt.Errorf("EVM manager does not expose its background fee cache")
	}
	flowRefresh, err := NewObservedFlowCostRefresh(
		ObservedFlowCostRefreshConfig{
			Markets: map[market.MarketID]configuration.ResolvedMarket{
				solanaMarket.ID: solanaMarket,
				evmMarket.ID:    evmMarket,
			},
			Bindings: map[market.MarketID]SwapBinding{
				solanaMarket.ID: solanaBinding,
				evmMarket.ID:    evmBinding,
			},
			Chains: config.Research.Chains, Valuator: costValuator,
			NTTCalibration: nttCalibration, Across: quoteBridge,
			AcrossCalibration: acrossCalibration,
			EVMFees:           evmFeeSource,
			SolanaFees:        solanaNetwork,
			NativeAssets: map[market.ChainID]market.AssetID{
				market.ChainID(solanaChain): "sol",
				market.ChainID(evmChain):    "pol",
			},
			NativeDecimals: map[market.ChainID]uint8{
				market.ChainID(solanaChain): 9,
				market.ChainID(evmChain):    18,
			},
			Clock: time.Now,
		},
	)
	if err != nil {
		return nil, err
	}
	directions := []arbitrage.Direction{
		{BuyMarket: solanaMarket.ID, SellMarket: evmMarket.ID},
		{BuyMarket: evmMarket.ID, SellMarket: solanaMarket.ID},
	}
	runtimeGate := NewRuntimeGate()
	reevaluate := make(chan time.Time, 1)
	flowCosts, err := NewFlowCostOracle(FlowCostOracleConfig{
		Directions: directions,
		QuoteAsset: solanaMarket.Quote.Token.Asset,
		Refresh:    flowRefresh, RefreshInterval: config.Live.CostRefreshInterval,
		TTL: config.Live.CostCacheTTL, Clock: time.Now, Logger: config.Logger,
		Gate: runtimeGate,
		OnReady: func() {
			if !runtimeGate.EvaluationAllowed() {
				return
			}
			select {
			case reevaluate <- time.Now().UTC():
			default:
			}
		},
	})
	if err != nil {
		return nil, err
	}
	if forcedDirection == nil && !config.RefuelOnly {
		go flowCosts.Run(ctx)
		runnerOptions.DirectionalCosts = flowCosts
	}
	runner, err := livecompare.New(config.Research, networks, runnerOptions)
	if err != nil {
		return nil, err
	}

	swapDriver := &SwapDriver{
		Bindings: map[market.MarketID]SwapBinding{
			solanaMarket.ID: solanaBinding,
			evmMarket.ID:    evmBinding,
		},
		SellPreflights: map[market.MarketID]SellPreflight{},
		TokenDecimals: map[market.TokenID]uint8{
			solanaMarket.Base.Token.ID:  solanaMarket.Base.Token.Decimals,
			solanaMarket.Quote.Token.ID: solanaMarket.Quote.Token.Decimals,
			evmMarket.Base.Token.ID:     evmMarket.Base.Token.Decimals,
			evmMarket.Quote.Token.ID:    evmMarket.Quote.Token.Decimals,
		},
		BridgePrecision: 8,
		QuoteAsset:      solanaMarket.Quote.Token.Asset,
		MinimumNet:      config.Live.MinimumNet,
		ReturnMargin:    config.Live.ReturnBridgeSafetyMargin,
		ExitCosts:       flowCosts,
		DynamicSlippage: DynamicSlippagePolicy{
			Enabled: config.Live.DynamicSlippage.Enabled,
			MaxBPS:  config.Live.DynamicSlippage.MaxBPS,
		},
		ExitValidationAttempts:   config.Live.ExitValidationAttempts,
		ExitValidationRetryDelay: config.Live.ExitValidationRetryDelay,
		FallbackAfter:            config.Live.ConfirmationTimeout,
		ArtifactMaxAge:           config.Live.BuildToBroadcastTimeout,
		Output:                   config.Output, Costs: costValuator,
	}
	if solanaSellPreflight != nil {
		swapDriver.SellPreflights[solanaMarket.ID] = solanaSellPreflight
	}
	if evmSellPreflight != nil {
		swapDriver.SellPreflights[evmMarket.ID] = evmSellPreflight
	}
	drivers := executionport.DriverSet{
		Buy: swapDriver,
		BridgeBase: &BridgeDriver{
			Stage: execution.StageBridgeBase, Provider: baseBridge,
			Costs: costValuator,
		},
		Sell: swapDriver,
		BridgeQuoteReturn: &BridgeDriver{
			Stage: execution.StageBridgeQuoteReturn, Provider: quoteBridge,
			Costs: costValuator,
		},
		ExitSelector: swapDriver,
	}
	executor, err := saga.NewSequentialExecutorWithObserver(
		journal,
		drivers,
		time.Now,
		progressObserver,
	)
	if err != nil {
		return nil, err
	}
	recovery, err := saga.NewSequentialRecoveryCoordinator(
		saga.SequentialRecoveryConfig{
			Journal: journal, RecoveryJournal: journal,
			Drivers: drivers, Clock: time.Now,
			Observer:         NewRecoveryObserver(liveNotifier, time.Now),
			UncertainTimeout: 10 * time.Minute,
			CostValuator:     costValuator,
		},
	)
	if err != nil {
		return nil, err
	}
	var refuelService *RefuelService
	if config.Live.GasRefuel.Enabled {
		solanaRefuel, refuelErr := NewSwapRefuelExecutor(
			SwapRefuelExecutorConfig{
				Chain:       market.ChainID(solanaChain),
				Market:      solanaMarket.ID,
				Account:     solanaBinding.Account,
				QuoteToken:  solanaMarket.Quote.Token,
				NativeToken: solanaBinding.NativeToken,
				NativeAsset: solanaBinding.NativeToken.Asset,
				Binding:     solanaBinding,
				Network:     solanaBinding.RefuelNetwork,
				Prices:      costValuator, Clock: time.Now,
				ConfirmTimeout: config.Live.ConfirmationTimeout,
			},
		)
		if refuelErr != nil {
			return nil, refuelErr
		}
		evmRefuel, refuelErr := NewSwapRefuelExecutor(
			SwapRefuelExecutorConfig{
				Chain:       market.ChainID(evmChain),
				Market:      evmMarket.ID,
				Account:     evmBinding.Account,
				QuoteToken:  evmMarket.Quote.Token,
				NativeToken: evmBinding.NativeToken,
				NativeAsset: evmBinding.NativeToken.Asset,
				Binding:     evmBinding,
				Network:     evmBinding.RefuelNetwork,
				Prices:      costValuator, Clock: time.Now,
				ConfirmTimeout: config.Live.ConfirmationTimeout,
			},
		)
		if refuelErr != nil {
			return nil, refuelErr
		}
		refuelService, err = NewRefuelService(
			config.Live.GasRefuel,
			runtimeGate,
			journal,
			[]executionport.RefuelExecutor{solanaRefuel, evmRefuel},
			liveNotifier,
			time.Now,
		)
		if err != nil {
			return nil, err
		}
		recovery.SetEmergencyRefuel(refuelService.EmergencyRefuel)
	}
	executionUnits, err := amountUnits(
		config.Live.ExecutionInput,
		solanaMarket.Quote.Token.Decimals,
	)
	if err != nil {
		return nil, fmt.Errorf("resolve Live execution input: %w", err)
	}
	operationLimit := config.Live.MaxOperationsPerRun
	if forcedDirection == nil {
		// Production Live is bounded to one active operation by Manager, but
		// it remains armed for subsequent opportunities after a successful
		// settlement. Forced canaries intentionally retain the configured
		// one-operation process limit.
		operationLimit = 0
	}
	manager, err := NewManagerWithLimit(
		ctx,
		Planner{
			MarketChains: map[market.MarketID]market.ChainID{
				solanaMarket.ID: market.ChainID(solanaChain),
				evmMarket.ID:    market.ChainID(evmChain),
			},
			ExecutionUnits: executionUnits,
			ExecutionPolicy: execution.ExecutionPolicyKind(
				config.Live.ExecutionPolicyKind,
			),
		},
		executor,
		operationLimit,
	)
	if err != nil {
		return nil, err
	}
	opportunityStore, err := sqlitestore.Open(storeStem + "-opportunities.sqlite")
	if err != nil {
		return nil, err
	}
	runtime, err := NewRuntimeWithGate(
		runner, manager, opportunityStore, cleanup, config.Output, liveNotifier,
		reevaluate, !config.ObserveCostsOnly, forcedDirection, runtimeGate,
	)
	if err != nil {
		_ = opportunityStore.Close()
		return nil, err
	}
	runtime.SetPostFlowRefresh(flowCosts.Warm)
	runtime.SetRecovery(recovery)
	runtime.SetRefuel(refuelService)
	keep = true
	return runtime, nil
}

type discardOpeningSender struct{}

func (discardOpeningSender) SendOpening(
	context.Context,
	notificationport.OpportunityOpening,
) error {
	return nil
}

func composeSolanaSwap(
	ctx context.Context,
	config ComposeConfig,
	configured configuration.ResolvedMarket,
	account configuration.ResolvedLiveAccount,
	privateKey solanago.PrivateKey,
	network *solanaadapter.ReadOnlyNetwork,
) (SwapBinding, error) {
	source, ok := config.Research.QuoteSources[configured.QuoteSource]
	if !ok || source.Kind != "jupiter" {
		return SwapBinding{}, fmt.Errorf("solana Live market requires Jupiter")
	}
	keysText, err := requiredEnv(config.LookupEnv, source.APIKeyEnv)
	if err != nil {
		return SwapBinding{}, err
	}
	keys := splitValues(keysText)
	if len(keys) == 0 {
		return SwapBinding{}, fmt.Errorf("jupiter API key list is empty")
	}
	mints := map[market.TokenID]string{
		configured.Base.Token.ID:  configured.Base.AddressText,
		configured.Quote.Token.ID: configured.Quote.AddressText,
		solanaNativeTokenID:       solanaNativeMint,
	}
	quoteClient, err := jupiter.NewQuoteSource(jupiter.QuoteConfig{
		ID:      market.SourceID("jupiter/live-exit-quote"),
		BaseURL: source.BaseURL, QuotePath: source.QuotePath,
		ExpectedMode: source.ExpectedMode, APIKeys: keys,
		SlippageBPS: source.SlippageBPS,
		Taker:       privateKey.PublicKey().String(), SwapMode: source.SwapMode,
		PriorityFeeLamports: source.PriorityFeeLamports,
		BroadcastFeeType:    source.BroadcastFeeType,
		UseWSOL:             source.UseWSOL,
		ExcludeDexes:        source.ExcludeDexes,
		ExcludeRouters:      source.ExcludeRouters,
		ClientPlatform:      source.ClientPlatform,
		Limiter:             jupiter.ImmediateLimiter{},
		Clock:               time.Now,
	})
	if err != nil {
		return SwapBinding{}, err
	}
	estimator := SwapQuoteEstimatorFunc(func(
		quoteCtx context.Context,
		input market.TokenAmount,
		output market.TokenID,
	) (market.TokenAmount, error) {
		inputMint, inputOK := mints[input.Token()]
		outputMint, outputOK := mints[output]
		if !inputOK || !outputOK {
			return market.TokenAmount{},
				fmt.Errorf("jupiter exit quote token mapping is unavailable")
		}
		result, quoteErr := quoteClient.Quote(
			quoteCtx,
			jupiter.QuoteRequest{
				InputMint: inputMint, OutputMint: outputMint,
				Amount: input.Units().String(),
			},
		)
		if quoteErr != nil {
			return market.TokenAmount{}, quoteErr
		}
		units, ok := new(big.Int).SetString(result.ToTokenAmount, 10)
		if !ok || units.Sign() <= 0 {
			return market.TokenAmount{},
				fmt.Errorf("jupiter exit quote output is invalid")
		}
		return market.NewTokenAmount(output, units)
	})
	validator, err := jupiter.NewBuildSource(jupiter.BuildConfig{
		ID: "jupiter/live-build", BaseURL: jupiterBuildBaseURL,
		Taker: privateKey.PublicKey().String(), APIKeys: keys,
		TokenMints: mints, SlippageBPS: config.Live.SlippageBPS,
		MaxAccounts:            source.MaxAccounts,
		ComputePricePercentile: config.Live.ComputeUnitPricePercentile,
		BlockhashSlotsToExpiry: config.Live.BlockhashSlotsToExpiry,
	})
	if err != nil {
		return SwapBinding{}, err
	}
	var refuelValidator executionport.Validator = validator
	if config.Live.GasRefuel.Enabled {
		refuelValidator, err = jupiter.NewBuildSource(jupiter.BuildConfig{
			ID: "jupiter/gas-refuel", BaseURL: jupiterBuildBaseURL,
			Taker: privateKey.PublicKey().String(), APIKeys: keys,
			TokenMints:             mints,
			SlippageBPS:            config.Live.GasRefuel.SlippageBPS,
			MaxAccounts:            source.MaxAccounts,
			ComputePricePercentile: config.Live.ComputeUnitPricePercentile,
			BlockhashSlotsToExpiry: config.Live.BlockhashSlotsToExpiry,
		})
		if err != nil {
			return SwapBinding{}, err
		}
	}
	decoder, err := solanaadapter.NewSPLBalanceDecoder(
		solanaadapter.SPLBalanceDecoderConfig{
			Owner: privateKey.PublicKey().String(), TokenMints: mints,
			NativeAsset: "sol", Clock: time.Now,
		},
	)
	if err != nil {
		return SwapBinding{}, err
	}
	confirmation, err := solanaadapter.NewConfirmationSource(
		solanaadapter.ConfirmationSourceConfig{
			AccountAddress: privateKey.PublicKey().String(),
			Subscriber:     network, Decoder: decoder, Clock: time.Now,
		},
	)
	if err != nil {
		return SwapBinding{}, err
	}
	var confirmationSource chainport.ConfirmationSource = confirmation
	if err := confirmation.Warm(ctx); err != nil {
		confirmationSource = nil
		if config.Logger != nil {
			config.Logger.Warn(
				"Solana transaction websocket unavailable; RPC confirmation fallback enabled",
				"error", err,
			)
		}
		fmt.Fprintf(
			config.Output,
			"live_warning component=solana_confirmation websocket=unavailable fallback=rpc reason=%q\n",
			err,
		)
	}
	senderURL, err := requiredEnv(config.LookupEnv, account.SenderURLEnv)
	if err != nil {
		return SwapBinding{}, err
	}
	tip, err := strconv.ParseUint(config.Live.TipLamports, 10, 64)
	if err != nil || tip == 0 {
		return SwapBinding{}, fmt.Errorf("live Helius tip is invalid")
	}
	maxPriorityFee, err := strconv.ParseUint(
		config.Live.MaxPriorityFeeLamports,
		10,
		64,
	)
	if err != nil || maxPriorityFee == 0 {
		return SwapBinding{}, fmt.Errorf("live Solana priority-fee cap is invalid")
	}
	manager, err := solanaadapter.NewTxManager(solanaadapter.TxManagerConfig{
		Chain:   market.ChainID(configured.Chain),
		Account: execution.AccountID(account.ID), PrivateKey: privateKey,
		SenderEndpoint: senderURL, SenderTipLamports: tip,
		ComputeUnitLimit:       config.Live.ComputeUnitLimit,
		MaxPriorityFeeLamports: maxPriorityFee,
		Reconciliation:         network, Simulator: network, FeeEstimator: network, Clock: time.Now,
		SettlementDecoder: decoder,
	})
	if err != nil {
		return SwapBinding{}, err
	}
	if err := manager.Warm(ctx); err != nil {
		return SwapBinding{}, fmt.Errorf("warm Helius Sender: %w", err)
	}
	return SwapBinding{
		Account: execution.AccountID(account.ID), Validator: validator,
		RefuelValidator: refuelValidator,
		Estimator:       estimator, TxManager: manager,
		Confirmation: confirmationSource,
		SpendableBalance: SpendableBalanceReaderFunc(func(
			balanceCtx context.Context,
			token market.TokenID,
		) (*big.Int, error) {
			mintText, exists := mints[token]
			if !exists {
				return nil, fmt.Errorf(
					"solana spendable token mapping is unavailable",
				)
			}
			mint, err := solanago.PublicKeyFromBase58(mintText)
			if err != nil {
				return nil, err
			}
			ata, _, err := solanago.FindAssociatedTokenAddress(
				privateKey.PublicKey(),
				mint,
			)
			if err != nil {
				return nil, err
			}
			accountState, err := network.ReadAccount(
				balanceCtx,
				ata.String(),
			)
			if err != nil {
				return nil, err
			}
			if len(accountState.Data) < 72 {
				return new(big.Int), nil
			}
			return new(big.Int).SetUint64(
				binary.LittleEndian.Uint64(accountState.Data[64:72]),
			), nil
		}),
		RefuelNetwork: SolanaRefuelNetwork{
			Network:                 network,
			Address:                 privateKey.PublicKey().String(),
			AdditionalDebitLamports: tip,
		},
		NativeToken: market.Token{
			ID: solanaNativeTokenID, Asset: "sol",
			Chain:    market.ChainID(configured.Chain),
			Decimals: 9, Symbol: "SOL",
		},
	}, nil
}

func composeSolanaSellPreflight(
	_ context.Context,
	config ComposeConfig,
	configured configuration.ResolvedMarket,
	account configuration.ResolvedLiveAccount,
	payer solanago.PrivateKey,
	reference solanago.PublicKey,
	network *solanaadapter.ReadOnlyNetwork,
) (SellPreflight, error) {
	source, ok := config.Research.QuoteSources[configured.QuoteSource]
	if !ok || source.Kind != "jupiter" {
		return nil, fmt.Errorf("solana sell preflight requires Jupiter")
	}
	keysText, err := requiredEnv(config.LookupEnv, source.APIKeyEnv)
	if err != nil {
		return nil, err
	}
	keys := splitValues(keysText)
	if len(keys) == 0 {
		return nil, fmt.Errorf("jupiter API key list is empty")
	}
	mints := map[market.TokenID]string{
		configured.Base.Token.ID:  configured.Base.AddressText,
		configured.Quote.Token.ID: configured.Quote.AddressText,
	}
	validator, err := jupiter.NewBuildSource(jupiter.BuildConfig{
		ID: "jupiter/sell-preflight", BaseURL: jupiterBuildBaseURL,
		Taker: reference.String(), Payer: payer.PublicKey().String(),
		APIKeys: keys, TokenMints: mints,
		SlippageBPS:            config.Live.SlippageBPS,
		MaxAccounts:            source.MaxAccounts,
		ComputePricePercentile: config.Live.ComputeUnitPricePercentile,
		BlockhashSlotsToExpiry: config.Live.BlockhashSlotsToExpiry,
	})
	if err != nil {
		return nil, err
	}
	tip, err := strconv.ParseUint(config.Live.TipLamports, 10, 64)
	if err != nil || tip == 0 {
		return nil, fmt.Errorf("live Helius tip is invalid")
	}
	maxPriorityFee, err := strconv.ParseUint(
		config.Live.MaxPriorityFeeLamports,
		10,
		64,
	)
	if err != nil || maxPriorityFee == 0 {
		return nil, fmt.Errorf("live Solana priority-fee cap is invalid")
	}
	tipAccount := solanaadapter.NextHeliusSenderTipAccount()
	preflightAccount := execution.AccountID(account.ID + "-sell-preflight")
	return SellPreflightFunc{
		Identity: reference.String(),
		Run: func(
			preflightCtx context.Context,
			request execution.SequentialStageRequest,
		) (executionport.Artifact, error) {
			validationRequest, requestErr := swapValidationRequest(
				request,
				preflightAccount,
				time.Now().UTC(),
			)
			if requestErr != nil {
				return executionport.Artifact{}, requestErr
			}
			artifact, validationErr := validator.Validate(
				preflightCtx,
				validationRequest,
			)
			for compactAttempts := 0; ; compactAttempts++ {
				if validationErr != nil {
					return executionport.Artifact{}, validationErr
				}
				raw, _, assembleErr :=
					solanaadapter.AssembleJupiterBuildForSimulation(
						artifact.Payload,
						payer,
						config.Live.ComputeUnitLimit,
						tipAccount,
						tip,
						maxPriorityFee,
					)
				if assembleErr != nil {
					return executionport.Artifact{}, assembleErr
				}
				if len(raw) <= 1232 {
					if simulationErr :=
						network.SimulateTransactionWithoutSignatureVerification(
							preflightCtx,
							raw,
						); simulationErr != nil {
						return executionport.Artifact{}, simulationErr
					}
					return artifact, nil
				}
				if compactAttempts >= 3 {
					return executionport.Artifact{}, fmt.Errorf(
						"solana sell preflight transaction is %d bytes",
						len(raw),
					)
				}
				artifact, validationErr = validator.ValidateCompact(
					preflightCtx,
					validationRequest,
					artifact,
				)
			}
		},
	}, nil
}

func composeEVMSwap(
	ctx context.Context,
	config ComposeConfig,
	configured configuration.ResolvedMarket,
	account configuration.ResolvedLiveAccount,
	privateKey *ecdsa.PrivateKey,
	sender common.Address,
	primaryURL string,
) (SwapBinding, []func(), error) {
	source, ok := config.Research.QuoteSources[configured.QuoteSource]
	if !ok || source.Kind != "kyberswap" {
		return SwapBinding{}, nil, fmt.Errorf("EVM Live market requires KyberSwap")
	}
	clientID, err := requiredEnv(config.LookupEnv, source.ClientIDEnv)
	if err != nil {
		return SwapBinding{}, nil, err
	}
	quoteSource, err := kyberquote.New(kyberquote.Config{
		BaseURL: source.BaseURL, ClientID: clientID,
	})
	if err != nil {
		return SwapBinding{}, nil, err
	}
	primary, err := ethclient.DialContext(ctx, primaryURL)
	if err != nil {
		return SwapBinding{}, nil, fmt.Errorf("dial EVM transaction endpoint: %w", err)
	}
	closers := []func(){primary.Close}
	fail := func(cause error) (SwapBinding, []func(), error) {
		for index := len(closers) - 1; index >= 0; index-- {
			closers[index]()
		}
		return SwapBinding{}, nil, cause
	}
	tokens := map[market.TokenID]string{
		configured.Base.Token.ID:  configured.Base.AddressText,
		configured.Quote.Token.ID: configured.Quote.AddressText,
		evmNativeTokenID:          evmNativePseudoAddress,
	}
	validator, err := kyberexecution.New(kyberexecution.Config{
		ID: "kyberswap/live-build", ChainSlug: source.ChainSlug,
		Sender: sender, TokenAddresses: tokens,
		SlippageBPS:                config.Live.SlippageBPS,
		GasExecutionMode:           config.Live.EVMGas.ExecutionMode,
		FixedExecutionGasLimit:     config.Live.EVMGas.ExecutionFixedLimit,
		GasEstimationMultiplierBPS: config.Live.EVMGas.EstimationMultiplierBPS,
		GasCostMode:                config.Live.EVMGas.CostMode,
		FixedCostGasLimit:          config.Live.EVMGas.CostFixedLimit,
		Source:                     quoteSource,
		Simulator:                  primary,
		Clock:                      time.Now,
	})
	if err != nil {
		return fail(err)
	}
	var refuelValidator executionport.Validator = validator
	if config.Live.GasRefuel.Enabled {
		refuelGas := config.Live.GasRefuel.EVMGas
		refuelValidator, err = kyberexecution.New(kyberexecution.Config{
			ID: "kyberswap/gas-refuel", ChainSlug: source.ChainSlug,
			Sender: sender, TokenAddresses: tokens,
			SlippageBPS:                config.Live.GasRefuel.SlippageBPS,
			GasExecutionMode:           refuelGas.ExecutionMode,
			FixedExecutionGasLimit:     refuelGas.ExecutionFixedLimit,
			GasEstimationMultiplierBPS: refuelGas.EstimationMultiplierBPS,
			GasCostMode:                refuelGas.CostMode,
			FixedCostGasLimit:          refuelGas.CostFixedLimit,
			Source:                     quoteSource,
			Simulator:                  primary,
			Clock:                      time.Now,
		})
		if err != nil {
			return fail(err)
		}
	}
	estimator := SwapQuoteEstimatorFunc(func(
		quoteCtx context.Context,
		input market.TokenAmount,
		output market.TokenID,
	) (market.TokenAmount, error) {
		inputAddress, inputOK := tokens[input.Token()]
		outputAddress, outputOK := tokens[output]
		if !inputOK || !outputOK {
			return market.TokenAmount{},
				fmt.Errorf("kyberswap exit quote token mapping is unavailable")
		}
		result, quoteErr := quoteSource.Route(
			quoteCtx,
			kyberquote.RouteRequest{
				Chain: source.ChainSlug, TokenIn: inputAddress,
				TokenOut: outputAddress, AmountIn: input.Units().String(),
				Origin: sender.Hex(),
			},
		)
		if quoteErr != nil {
			return market.TokenAmount{}, quoteErr
		}
		units, ok := new(big.Int).SetString(result.AmountOut, 10)
		if !ok || units.Sign() <= 0 {
			return market.TokenAmount{},
				fmt.Errorf("kyberswap exit quote output is invalid")
		}
		return market.NewTokenAmount(output, units)
	})
	decoder, err := evmadapter.NewERC20TransferReceiptDecoder(
		sender, tokens, time.Now, "pol",
	)
	if err != nil {
		return fail(err)
	}
	var confirmationSource chainport.ConfirmationSource
	chainConfig := config.Research.Chains[configured.Chain]
	websocketURL, websocketErr := requiredEnv(
		config.LookupEnv,
		chainConfig.WebSocketURLEnv,
	)
	if websocketErr == nil {
		confirmationNetwork, confirmationErr :=
			evmadapter.DialReadOnlyNetwork(
				ctx,
				string(configured.Chain),
				chainConfig.Label,
				chainConfig.ChainID,
				primaryURL,
				websocketURL,
			)
		if confirmationErr == nil {
			confirmation, sourceErr := evmadapter.NewConfirmationSource(
				evmadapter.ConfirmationSourceConfig{
					Network: confirmationNetwork,
					Decoder: decoder,
					Clock:   time.Now,
				},
			)
			if sourceErr == nil {
				sourceErr = confirmation.Warm(ctx)
			}
			if sourceErr == nil {
				confirmationSource = confirmation
				closers = append(closers, confirmationNetwork.Close)
			} else {
				confirmationNetwork.Close()
				websocketErr = sourceErr
			}
		} else {
			websocketErr = confirmationErr
		}
	}
	if websocketErr != nil {
		if config.Logger != nil {
			config.Logger.Warn(
				"EVM settlement websocket unavailable; parallel RPC receipt confirmation remains enabled",
				"reason", "connection_or_subscription_failed",
			)
		}
		fmt.Fprintf(
			config.Output,
			"live_warning component=evm_confirmation websocket=unavailable fallback=parallel_receipt_rpc reason=connection_or_subscription_failed\n",
		)
	}
	fanoutText := mustLookup(config.LookupEnv, account.FanoutRPCURLEnv)
	endpoints := distinctValues(
		append([]string{primaryURL}, splitValues(fanoutText)...),
	)
	fanout := make(map[string]evmadapter.TxClient, len(endpoints))
	for index, endpoint := range endpoints {
		label := fanoutEndpointLabel(index, endpoint)
		if endpoint == primaryURL {
			fanout[label] = primary
			continue
		}
		client, dialErr := ethclient.DialContext(ctx, endpoint)
		if dialErr != nil {
			return fail(fmt.Errorf("dial EVM fanout endpoint %d: %w", index, dialErr))
		}
		closers = append(closers, client.Close)
		fanout[label] = client
	}
	if len(fanout) == 0 {
		return fail(fmt.Errorf("EVM fanout endpoint list is empty"))
	}
	manager, err := evmadapter.NewTxManager(evmadapter.TxManagerConfig{
		Chain:      market.ChainID(configured.Chain),
		Account:    execution.AccountID(account.ID),
		ChainID:    config.Research.Chains[configured.Chain].ChainID,
		PrivateKey: privateKey, Primary: primary, Fanout: fanout,
		Simulator: primary, Clock: time.Now, ReceiptDecoder: decoder,
		FeeRefreshInterval: config.Live.FeeCacheMaxAge,
		OnFanoutResult: func(attempt evmadapter.FanoutAttempt) {
			status := "rejected"
			if attempt.Accepted {
				status = "accepted"
			}
			fmt.Fprintf(
				config.Output,
				"live_evm_fanout endpoint=%s status=%s error_class=%s already_known=%t latency=%s\n",
				attempt.Endpoint,
				status,
				attempt.ErrorClass,
				attempt.AlreadyKnown,
				attempt.Latency.Round(time.Microsecond),
			)
			if config.Logger != nil {
				config.Logger.Info(
					"EVM raw transaction fanout completed",
					"endpoint", attempt.Endpoint,
					"status", status,
					"error_class", attempt.ErrorClass,
					"already_known", attempt.AlreadyKnown,
					"latency", attempt.Latency,
				)
			}
		},
	})
	if err != nil {
		return fail(err)
	}
	if err := manager.Warm(ctx); err != nil {
		return fail(fmt.Errorf("warm EVM nonce and fees: %w", err))
	}
	return SwapBinding{
		Account: execution.AccountID(account.ID), Validator: validator,
		RefuelValidator: refuelValidator,
		Estimator:       estimator, TxManager: manager,
		Confirmation:     confirmationSource,
		NonceCoordinator: manager,
		SpendableBalance: SpendableBalanceReaderFunc(func(
			balanceCtx context.Context,
			token market.TokenID,
		) (*big.Int, error) {
			tokenText, exists := tokens[token]
			if !exists || !common.IsHexAddress(tokenText) {
				return nil, fmt.Errorf(
					"EVM spendable token mapping is unavailable",
				)
			}
			tokenAddress := common.HexToAddress(tokenText)
			selector := gethcrypto.Keccak256(
				[]byte("balanceOf(address)"),
			)[:4]
			payload := append(
				append([]byte(nil), selector...),
				common.LeftPadBytes(sender.Bytes(), 32)...,
			)
			raw, err := primary.CallContract(
				balanceCtx,
				geth.CallMsg{To: &tokenAddress, Data: payload},
				nil,
			)
			if err != nil {
				return nil, err
			}
			if len(raw) != 32 {
				return nil, fmt.Errorf(
					"EVM balance response has %d bytes",
					len(raw),
				)
			}
			return new(big.Int).SetBytes(raw), nil
		}),
		Allowance: AllowanceReaderFunc(func(
			allowanceCtx context.Context,
			token market.TokenID,
			spenderText string,
		) (*big.Int, error) {
			tokenText, exists := tokens[token]
			if !exists || !common.IsHexAddress(tokenText) ||
				!common.IsHexAddress(spenderText) {
				return nil, fmt.Errorf(
					"EVM allowance mapping is unavailable",
				)
			}
			tokenAddress := common.HexToAddress(tokenText)
			spender := common.HexToAddress(spenderText)
			selector := gethcrypto.Keccak256(
				[]byte("allowance(address,address)"),
			)[:4]
			payload := append(
				append(
					append([]byte(nil), selector...),
					common.LeftPadBytes(sender.Bytes(), 32)...,
				),
				common.LeftPadBytes(spender.Bytes(), 32)...,
			)
			raw, err := primary.CallContract(
				allowanceCtx,
				geth.CallMsg{To: &tokenAddress, Data: payload},
				nil,
			)
			if err != nil {
				return nil, err
			}
			if len(raw) != 32 {
				return nil, fmt.Errorf(
					"EVM allowance response has %d bytes",
					len(raw),
				)
			}
			return new(big.Int).SetBytes(raw), nil
		}),
		RefuelNetwork: EVMRefuelNetwork{
			Client:  primary,
			Address: sender,
		},
		NativeToken: market.Token{
			ID: evmNativeTokenID, Asset: "pol",
			Chain:    market.ChainID(configured.Chain),
			Decimals: 18, Symbol: "POL",
		},
	}, closers, nil
}

func composeEVMSellPreflight(
	ctx context.Context,
	config ComposeConfig,
	configured configuration.ResolvedMarket,
	account configuration.ResolvedLiveAccount,
	reference common.Address,
	primaryURL string,
) (SellPreflight, []func(), error) {
	source, ok := config.Research.QuoteSources[configured.QuoteSource]
	if !ok || source.Kind != "kyberswap" {
		return nil, nil, fmt.Errorf("EVM sell preflight requires KyberSwap")
	}
	clientID, err := requiredEnv(config.LookupEnv, source.ClientIDEnv)
	if err != nil {
		return nil, nil, err
	}
	quoteSource, err := kyberquote.New(kyberquote.Config{
		BaseURL:  source.BaseURL,
		ClientID: clientID,
	})
	if err != nil {
		return nil, nil, err
	}
	primary, err := ethclient.DialContext(ctx, primaryURL)
	if err != nil {
		return nil, nil, fmt.Errorf("dial EVM sell preflight endpoint: %w", err)
	}
	override := account.SellPreflightStateOverride
	if override == nil {
		primary.Close()
		return nil, nil, fmt.Errorf(
			"EVM sell preflight state override is not configured",
		)
	}
	simulator, err := evmadapter.NewERC20StateOverrideSimulator(
		evmadapter.ERC20StateOverrideSimulatorConfig{
			Client:        primary.Client(),
			Token:         configured.Base.Address,
			Owner:         reference,
			BalanceSlot:   override.BalanceSlot,
			AllowanceSlot: override.AllowanceSlot,
		},
	)
	if err != nil {
		primary.Close()
		return nil, nil, err
	}
	tokens := map[market.TokenID]string{
		configured.Base.Token.ID:  configured.Base.AddressText,
		configured.Quote.Token.ID: configured.Quote.AddressText,
	}
	validator, err := kyberexecution.New(kyberexecution.Config{
		ID:                         "kyberswap/sell-preflight",
		ChainSlug:                  source.ChainSlug,
		Sender:                     reference,
		TokenAddresses:             tokens,
		SlippageBPS:                config.Live.SlippageBPS,
		GasExecutionMode:           config.Live.EVMGas.ExecutionMode,
		FixedExecutionGasLimit:     config.Live.EVMGas.ExecutionFixedLimit,
		GasEstimationMultiplierBPS: config.Live.EVMGas.EstimationMultiplierBPS,
		GasCostMode:                config.Live.EVMGas.CostMode,
		FixedCostGasLimit:          config.Live.EVMGas.CostFixedLimit,
		Source:                     quoteSource,
		Simulator:                  simulator,
		Clock:                      time.Now,
	})
	if err != nil {
		primary.Close()
		return nil, nil, err
	}
	preflightAccount := execution.AccountID(account.ID + "-sell-preflight")
	return SellPreflightFunc{
		Identity: reference.Hex() + "/state-override",
		Run: func(
			preflightCtx context.Context,
			request execution.SequentialStageRequest,
		) (executionport.Artifact, error) {
			validationRequest, requestErr := swapValidationRequest(
				request,
				preflightAccount,
				time.Now().UTC(),
			)
			if requestErr != nil {
				return executionport.Artifact{}, requestErr
			}
			return validator.Validate(preflightCtx, validationRequest)
		},
	}, []func(){primary.Close}, nil
}

func splitMarkets(
	config configuration.ParsedConfig,
	solanaChain, evmChain string,
) (configuration.ResolvedMarket, configuration.ResolvedMarket, error) {
	var solanaMarket, evmMarket configuration.ResolvedMarket
	for _, configured := range config.Markets {
		switch configured.Chain {
		case solanaChain:
			solanaMarket = configured
		case evmChain:
			evmMarket = configured
		}
	}
	if solanaMarket.ID == "" || evmMarket.ID == "" {
		return solanaMarket, evmMarket, fmt.Errorf("live setup must contain one market per chain")
	}
	if solanaMarket.Quote.Token.Asset != evmMarket.Quote.Token.Asset ||
		solanaMarket.Quote.Token.Decimals != evmMarket.Quote.Token.Decimals {
		return solanaMarket, evmMarket, fmt.Errorf(
			"sequential canary requires equivalent quote tokens with equal decimals",
		)
	}
	return solanaMarket, evmMarket, nil
}

func amountUnits(amount *big.Rat, decimals uint8) (*big.Int, error) {
	if amount == nil || amount.Sign() <= 0 {
		return nil, fmt.Errorf("amount must be positive")
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	scaled := new(big.Rat).Mul(amount, new(big.Rat).SetInt(scale))
	if !scaled.IsInt() {
		return nil, fmt.Errorf("amount has more precision than the token supports")
	}
	return new(big.Int).Set(scaled.Num()), nil
}

func requiredEnv(lookup configuration.LookupEnv, name string) (string, error) {
	value, ok := lookup(strings.TrimSpace(name))
	if !ok || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("required Live environment %q is unset", name)
	}
	return strings.TrimSpace(value), nil
}

func mustLookup(lookup configuration.LookupEnv, name string) string {
	value, _ := lookup(name)
	return strings.TrimSpace(value)
}

func splitValues(value string) []string {
	fields := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\r'
	})
	result := make([]string, 0, len(fields))
	for _, field := range fields {
		if trimmed := strings.TrimSpace(field); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func distinctValues(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, duplicate := seen[value]; duplicate {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func fanoutEndpointLabel(index int, endpoint string) string {
	label := fmt.Sprintf("fanout-%d", index)
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || parsed.Hostname() == "" {
		return label
	}
	return label + ":" + strings.ToLower(parsed.Hostname())
}

func parseEVMPrivateKey(value string) (*ecdsa.PrivateKey, error) {
	key, err := gethcrypto.HexToECDSA(
		strings.TrimPrefix(strings.TrimSpace(value), "0x"),
	)
	if err != nil {
		return nil, fmt.Errorf("invalid EVM private key")
	}
	return key, nil
}

func parseSolanaPrivateKey(value string) (solanago.PrivateKey, error) {
	value = strings.TrimSpace(value)
	if parsed, err := solanago.PrivateKeyFromBase58(value); err == nil {
		return parsed, nil
	}
	var bytes []byte
	if json.Unmarshal([]byte(value), &bytes) == nil {
		return solanaPrivateKeyBytes(bytes)
	}
	if decoded, err := base64.StdEncoding.DecodeString(value); err == nil {
		return solanaPrivateKeyBytes(decoded)
	}
	return nil, fmt.Errorf("invalid Solana private key")
}

func solanaPrivateKeyBytes(value []byte) (solanago.PrivateKey, error) {
	if len(value) != 64 {
		return nil, fmt.Errorf("solana private key must contain 64 bytes")
	}
	return solanago.PrivateKey(append([]byte(nil), value...)), nil
}
