package livecanary

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"path/filepath"
	"strings"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	gethcrypto "github.com/ethereum/go-ethereum/crypto"

	evmadapter "github.com/VarozXYZ/vernier/adapters/chain/evm"
	acrossadapter "github.com/VarozXYZ/vernier/adapters/crosschain/across"
	"github.com/VarozXYZ/vernier/adapters/crosschain/wormholentt"
	"github.com/VarozXYZ/vernier/adapters/crosschain/wormholewtt"
	kyberexecution "github.com/VarozXYZ/vernier/adapters/execution/kyberswap"
	localexecution "github.com/VarozXYZ/vernier/adapters/execution/local"
	"github.com/VarozXYZ/vernier/adapters/market/aerodromeslipstream"
	"github.com/VarozXYZ/vernier/adapters/market/uniswapv3"
	telegramnotification "github.com/VarozXYZ/vernier/adapters/notification/telegram"
	sqlitestore "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	corelive "github.com/VarozXYZ/vernier/core/live"
	"github.com/VarozXYZ/vernier/core/saga"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/inventory"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	crosschainport "github.com/VarozXYZ/vernier/ports/crosschain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	notificationport "github.com/VarozXYZ/vernier/ports/notification"
	"github.com/VarozXYZ/vernier/runtime/configuration"
	"github.com/VarozXYZ/vernier/runtime/livecompare"
)

func composeEVMHybridArmed(ctx context.Context, config ComposeConfig) (_ *Runtime, err error) {
	endpoints, err := config.Research.ResolveEndpoints(config.LookupEnv)
	if err != nil {
		return nil, err
	}
	config.Output = &synchronizedWriter{delegate: config.Output}
	if err := validateExecutionSizing(config.Research, config.Live); err != nil {
		return nil, err
	}
	var cleanup []func()
	keep := false
	defer func() {
		if !keep {
			for index := len(cleanup) - 1; index >= 0; index-- {
				cleanup[index]()
			}
		}
	}()

	networks := make(livecompare.Networks, 2)
	for id, profile := range config.Research.Chains {
		fmt.Fprintf(config.Output, "live_startup phase=network_dial_started chain=%s\n", id)
		dialCtx, cancelDial := context.WithTimeout(ctx, 20*time.Second)
		network, dialErr := evmadapter.DialReadOnlyNetwork(dialCtx, profile.ID, profile.Label, profile.ChainID,
			endpoints[id], endpoints[id+".websocket"])
		cancelDial()
		if dialErr != nil {
			return nil, dialErr
		}
		fmt.Fprintf(config.Output, "live_startup phase=network_dial_ready chain=%s\n", id)
		networks[id] = network
		cleanup = append(cleanup, network.Close)
	}
	localMarket, remoteMarket, err := splitHybridMarkets(config.Research)
	if err != nil {
		return nil, err
	}
	markets := []configuration.ResolvedMarket{localMarket, remoteMarket}
	var forcedDirection *arbitrage.Direction
	if config.ForcedCanary != "" {
		resolved, resolveErr := config.ForcedCanary.Resolve(localMarket.ID, remoteMarket.ID)
		if resolveErr != nil {
			return nil, resolveErr
		}
		forcedDirection = &resolved
	}

	quoteFXBlocked := make(chan bool, 1)
	var balanceManager *BalanceManager
	runnerOptions := livecompare.Options{LookupEnv: config.LookupEnv, Logger: config.Logger,
		SuppressConfiguredNotifications: config.ObserveCostsOnly,
		QuoteConversionBlocked: func(blocked bool) {
			select {
			case quoteFXBlocked <- blocked:
			default:
				select {
				case <-quoteFXBlocked:
				default:
				}
				select {
				case quoteFXBlocked <- blocked:
				default:
				}
			}
		},
		ExecutableAllowedRouters: approvalDestinations(config.Live, remoteMarket.Chain, "kyberswap")}
	if config.Live.InventoryCapacityMode == "confirmed_balance" {
		runnerOptions.CandidateEligible = func(direction arbitrage.Direction, candidate arbitrage.Candidate) bool {
			return balanceManager != nil && balanceManager.CandidateEligible(direction, candidate)
		}
	}
	if remoteMarket.QuoteSource != "" && len(runnerOptions.ExecutableAllowedRouters) == 0 {
		return nil, fmt.Errorf("armed hybrid Live requires an exact KyberSwap router allowlist")
	}
	var liveNotifier *LiveNotifier
	if config.Research.TelegramEnabled && !config.ObserveCostsOnly {
		botToken, tokenErr := requiredEnv(config.LookupEnv, config.Research.TelegramBotTokenEnv)
		if tokenErr != nil {
			return nil, tokenErr
		}
		chatID, chatErr := requiredEnv(config.LookupEnv, config.Research.TelegramChatIDEnv)
		if chatErr != nil {
			return nil, chatErr
		}
		sender, senderErr := telegramnotification.New(telegramnotification.Config{
			BotToken: botToken, ChatID: chatID, SetupLabel: localMarket.Base.Token.Symbol,
		})
		if senderErr != nil {
			return nil, senderErr
		}
		runnerOptions.OpeningAlerts, runnerOptions.ConfigurationAlerts = discardOpeningSender{}, sender
		liveNotifier, err = NewLiveNotifier(sender, config.Logger)
		if err != nil {
			return nil, err
		}
		cleanup = append(cleanup, liveNotifier.Close)
	}
	if liveNotifier != nil && len(config.Research.QuoteConversions) > 0 {
		go monitorQuoteFXBlock(ctx, quoteFXBlocked, liveNotifier)
	}

	bindings := make(map[market.MarketID]SwapBinding, 2)
	accounts := make(map[market.ChainID]execution.AccountID, 2)
	marketByChain := make(map[market.ChainID]configuration.ResolvedMarket, 2)
	for _, configured := range markets {
		account, ok := config.Live.Accounts[configured.Chain]
		if !ok {
			return nil, fmt.Errorf("live account for %s is unavailable", configured.Chain)
		}
		keyText, keyErr := requiredEnv(config.LookupEnv, account.SignerEnv)
		if keyErr != nil {
			return nil, keyErr
		}
		privateKey, keyErr := parseEVMPrivateKey(keyText)
		if keyErr != nil {
			return nil, keyErr
		}
		owner := gethcrypto.PubkeyToAddress(privateKey.PublicKey)
		var binding SwapBinding
		var closers []func()
		if configured.ID == localMarket.ID || configured.SplitRoute != nil {
			binding, closers, err = composeLocalEVMSwap(ctx, config, configured, account, privateKey, owner, endpoints[configured.Chain])
		} else {
			binding, closers, err = composeEVMSwap(ctx, config, configured, account, privateKey, owner, endpoints[configured.Chain])
		}
		if err != nil {
			return nil, err
		}
		cleanup = append(cleanup, closers...)
		bindings[configured.ID] = binding
		chain := market.ChainID(configured.Chain)
		accounts[chain] = execution.AccountID(account.ID)
		marketByChain[chain] = configured
	}
	for id, binding := range bindings {
		binding.StartupAllowanceGuaranteed = true
		bindings[id] = binding
	}

	runnerConfig := config.Research
	if forcedDirection != nil {
		runnerConfig.MinimumSize = new(big.Rat).Set(config.Live.CanaryInput)
		runnerConfig.MaximumSize = new(big.Rat).Set(config.Live.CanaryInput)
		runnerConfig.SizingKind = "fixed"
		runnerConfig.SizeSamples = 1
		runnerConfig.Sizes = []*big.Rat{new(big.Rat).Set(config.Live.CanaryInput)}
	}
	origins := make(map[string]string, len(markets))
	for _, configured := range markets {
		origins[configured.Chain] = bindings[configured.ID].EVMAddress.Hex()
	}
	conversionBook, err := livecompare.StartQuoteConversions(ctx, runnerConfig, config.LookupEnv, origins, config.Logger)
	if err != nil {
		return nil, err
	}
	runnerOptions.QuoteConversions = conversionBook
	runner, err := livecompare.New(runnerConfig, networks, runnerOptions)
	if err != nil {
		return nil, err
	}
	localBinding := bindings[localMarket.ID]
	if config.Live.ExecutionPolicyKind == string(execution.PolicyPrefundedTriggerFirst) {
		validator, validatorErr := localexecution.New(localexecution.Config{Source: runner,
			Builder: localBinding.LocalBuilder, Clock: time.Now})
		if validatorErr != nil {
			return nil, validatorErr
		}
		localBinding.Validator, localBinding.RecoveryValidator = validator, validator
		localBinding.LatestSnapshot = func() (market.MarketSnapshot, bool) { return runner.LatestSnapshot(localMarket.ID) }
	}
	localBinding.SnapshotForQuote = runner.SnapshotForQuote
	localBinding.Estimator, err = composeHybridLocalRecoveryEstimator(localMarket, config.Live, networks[localMarket.Chain])
	if err != nil {
		return nil, err
	}
	bindings[localMarket.ID] = localBinding
	remoteBinding := bindings[remoteMarket.ID]
	if remoteMarket.SplitRoute != nil {
		if remoteBinding.LocalBuilder == nil {
			return nil, fmt.Errorf("split local builder is unavailable")
		}
		validator, validatorErr := localexecution.New(localexecution.Config{Source: runner,
			Builder: remoteBinding.LocalBuilder, Clock: time.Now})
		if validatorErr != nil {
			return nil, validatorErr
		}
		remoteBinding.Validator, remoteBinding.RecoveryValidator = validator, validator
		remoteBinding.SnapshotForQuote = runner.SnapshotForQuote
		remoteBinding.LatestSnapshot = func() (market.MarketSnapshot, bool) { return runner.LatestSnapshot(remoteMarket.ID) }
		bindings[remoteMarket.ID] = remoteBinding
	} else {
		validationSender, senderErr := requiredEnv(config.LookupEnv, config.Research.ExecutableValidationEVMSenderEnv)
		if senderErr != nil || !common.IsHexAddress(validationSender) ||
			common.HexToAddress(validationSender) != remoteBinding.EVMAddress {
			return nil, fmt.Errorf("armed hybrid Live requires the Research validation sender to equal the remote Live wallet")
		}
		remoteBinding.SnapshotForQuote = runner.SnapshotForQuote
		remoteBinding.RecoveryValidator = remoteBinding.Validator
		if validator, ok := remoteBinding.Validator.(*kyberexecution.Validator); ok {
			remoteBinding.RecoveryValidator = validator.FreshRouteValidator()
		}
		remoteBinding.Validator = RetainedArtifactValidator{Source: runner, AllowedDestinations: approvalDestinations(config.Live, remoteMarket.Chain, "kyberswap")}
		bindings[remoteMarket.ID] = remoteBinding
	}

	runtimeGate := NewRuntimeGate()
	nativeTokens := make(map[market.ChainID]market.Token, 2)
	balanceReaders := make(map[inventory.Key]PhysicalBalanceReader)
	for _, configured := range markets {
		nativeTokens[market.ChainID(configured.Chain)] = bindings[configured.ID].NativeToken
	}
	for _, configuredBalance := range config.Live.Inventory {
		chain := market.ChainID(configuredBalance.Chain)
		configured := marketByChain[chain]
		reader := bindings[configured.ID].SpendableBalance
		token := configuredBalance.Token.ID
		balanceReaders[inventory.Key{Chain: chain, Account: execution.AccountID(configuredBalance.Account), Token: token}] = func(readCtx context.Context) (*big.Int, error) {
			return reader.SpendableBalance(readCtx, token)
		}
	}
	for chain, native := range nativeTokens {
		chain, native := chain, native
		configured := marketByChain[chain]
		network := bindings[configured.ID].RefuelNetwork
		balanceReaders[inventory.Key{Chain: chain, Account: accounts[chain], Token: native.ID}] = func(readCtx context.Context) (*big.Int, error) { return network.NativeBalance(readCtx) }
	}
	balanceManager, err = NewBalanceManager(BalanceManagerConfig{Balances: config.Live.Inventory, Readers: balanceReaders,
		NativeTokens: nativeTokens, Accounts: accounts, MarketChains: map[market.MarketID]market.ChainID{
			localMarket.ID: market.ChainID(localMarket.Chain), remoteMarket.ID: market.ChainID(remoteMarket.Chain)},
		UseConfirmedBalanceCapacity: config.Live.InventoryCapacityMode == "confirmed_balance",
		Gate:                        runtimeGate, Notifier: liveNotifier, Logger: config.Logger,
		Output: config.Output, PollInterval: config.Live.BalancePollInterval, AlertInterval: config.Live.BalanceAlertInterval})
	if err != nil {
		return nil, err
	}
	if err := balanceManager.Warm(ctx); err != nil {
		return nil, err
	}
	for _, configured := range markets {
		binding := bindings[configured.ID]
		chain := market.ChainID(configured.Chain)
		binding.SpendableBalance = balanceManager.SpendableReader(chain)
		binding.BalanceSnapshot = func(token market.TokenID) (*big.Int, uint64, error) { return balanceManager.Snapshot(chain, token) }
		bindings[configured.ID] = binding
	}

	journal, err := sqlitestore.OpenSequentialLive(config.Live.OperationalStorePath)
	if err != nil {
		return nil, err
	}
	cleanup = append(cleanup, func() { _ = journal.Close() })
	if liveNotifier != nil {
		if err := liveNotifier.AttachOutbox(ctx, journal); err != nil {
			return nil, err
		}
		cleanup = append(cleanup, liveNotifier.Close)
	}
	if err := ensureHybridApprovals(ctx, config, markets, bindings, journal, !config.ObserveCostsOnly); err != nil {
		return nil, err
	}

	profiles, guardianURLs, pollMS, err := loadWTTProfile(filepath.Join(filepath.Dir(config.ManifestPath), config.Live.BaseTransferSource.Profile))
	if err != nil {
		return nil, err
	}
	guardian, err := wormholentt.NewGuardianClient(wormholentt.GuardianClientConfig{Endpoints: guardianURLs, PollInterval: time.Duration(pollMS) * time.Millisecond, Clock: time.Now})
	if err != nil {
		return nil, err
	}
	wttChains := make(map[market.ChainID]wormholewtt.LiveChain, 2)
	acrossChains := make(map[market.ChainID]acrossadapter.EVMLiveChain, 2)
	for _, configured := range markets {
		chain := market.ChainID(configured.Chain)
		profile, ok := profiles[configured.Chain]
		if !ok {
			return nil, fmt.Errorf("WTT deployment for %s is unavailable", configured.Chain)
		}
		binding := bindings[configured.ID]
		native := binding.NativeToken
		wttChains[chain] = wormholewtt.LiveChain{ID: chain, WormholeID: profile.WormholeChainID,
			CoreBridge: profile.CoreBridge, TokenBridge: profile.TokenBridge, Token: configured.Base.Token,
			TokenAddress: configured.Base.Address, Owner: binding.EVMAddress, Manager: binding.TxManager,
			Client: binding.EVMClient, NativeAsset: native.Asset}
		chainID := config.Research.Chains[configured.Chain].ChainID
		acrossToken := configured.Quote
		if _, transit, found := quoteConversionTokens(config.Live, configured); found {
			acrossToken = transit
		}
		acrossChains[chain] = acrossadapter.EVMLiveChain{ID: chain, ChainID: chainID.Uint64(), Token: acrossToken.Token,
			TokenAddress: acrossToken.Address, Owner: binding.EVMAddress, AllowedContracts: approvalDestinations(config.Live, configured.Chain, "across"),
			Manager: binding.TxManager, Client: binding.EVMClient, NativeAsset: native.Asset}
	}
	baseBridge, err := wormholewtt.NewLiveService(wormholewtt.LiveServiceConfig{Chains: wttChains, Attestations: guardian,
		Clock: time.Now, PollInterval: time.Duration(pollMS) * time.Millisecond, Timeout: config.Live.ConfirmationTimeout,
		Trace: func(phase string) { fmt.Fprintf(config.Output, "live_wtt phase=%s\n", phase) }})
	if err != nil {
		return nil, err
	}
	acrossClient, err := acrossadapter.New(acrossadapter.Config{BaseURL: config.Live.QuoteTransferSource.BaseURL,
		APIKey:       mustLookup(config.LookupEnv, config.Live.QuoteTransferSource.APIKeyEnv),
		IntegratorID: mustLookup(config.LookupEnv, config.Live.QuoteTransferSource.IntegratorIDEnv)})
	if err != nil {
		return nil, err
	}
	quoteBridge, err := acrossadapter.NewEVMLiveService(acrossadapter.EVMLiveServiceConfig{Client: acrossClient, Chains: acrossChains,
		Clock: time.Now, Timeout: config.Live.ConfirmationTimeout})
	if err != nil {
		return nil, err
	}
	var quoteTransfer crosschainport.RecoverableLiveTransferService = quoteBridge
	if operational, transit, found := quoteConversionTokens(config.Live, remoteMarket); found {
		quoteTransfer, err = NewConversionAwareTransfer(ConversionAwareTransfer{
			Bridge: quoteBridge, ConversionChain: market.ChainID(remoteMarket.Chain),
			OperationalToken: operational.Token.ID, TransitToken: transit.Token.ID,
			Market: remoteMarket.ID, Binding: bindings[remoteMarket.ID], SlippageBPS: config.Live.SecondLegSlippageBPS,
			Clock: time.Now, Timeout: config.Live.ConfirmationTimeout,
		})
		if err != nil {
			return nil, err
		}
	}
	fmt.Fprintln(config.Output, "live_startup phase=cost_valuator_warm_started")
	costValuator, err := composeEVMCostValuator(ctx, config, markets)
	if err != nil {
		return nil, err
	}
	fmt.Fprintln(config.Output, "live_startup phase=cost_valuator_warm_ready")
	reevaluate := make(chan time.Time, 1)
	var flowCosts *FlowCostOracle
	if config.Live.CostModel == "observed_complete_flow_evm" {
		feeSources := make(map[market.ChainID]cachedEVMFeeSource, 2)
		nativeAssets := make(map[market.ChainID]market.AssetID, 2)
		nativeDecimals := make(map[market.ChainID]uint8, 2)
		for _, configured := range markets {
			chain := market.ChainID(configured.Chain)
			fees, ok := bindings[configured.ID].TxManager.(cachedEVMFeeSource)
			if !ok {
				return nil, fmt.Errorf("EVM manager for %s has no fee calibration capability", configured.Chain)
			}
			feeSources[chain] = fees
			nativeAssets[chain] = bindings[configured.ID].NativeToken.Asset
			nativeDecimals[chain] = bindings[configured.ID].NativeToken.Decimals
		}
		bridgeTokens := make(map[market.ChainID]market.Token, len(acrossChains))
		for chain, profile := range acrossChains {
			bridgeTokens[chain] = profile.Token
		}
		var conversionChain market.ChainID
		if _, _, found := quoteConversionTokens(config.Live, remoteMarket); found {
			conversionChain = market.ChainID(remoteMarket.Chain)
		}
		swapGasProbes := make(map[market.MarketID]EVMObservedSwapGasProbe)
		remoteSwapGas := make(map[market.MarketID]uint64)
		if localMarket.SplitRoute != nil || bindings[localMarket.ID].LocalBuilder != nil {
			swapGasProbes[localMarket.ID] = localExecutorGasProbe(bindings[localMarket.ID])
		}
		if remoteMarket.SplitRoute != nil {
			swapGasProbes[remoteMarket.ID] = localExecutorGasProbe(bindings[remoteMarket.ID])
		} else {
			remoteSwapGas[remoteMarket.ID] = config.Live.EVMGas.CostFixedLimit
		}
		flowRefresh, refreshErr := NewEVMObservedFlowCostRefresh(EVMObservedFlowCostRefreshConfig{
			Markets:  map[market.MarketID]configuration.ResolvedMarket{localMarket.ID: localMarket, remoteMarket.ID: remoteMarket},
			Valuator: costValuator, Calibration: journal, Across: quoteBridge, WTT: baseBridge,
			Fees: feeSources, NativeAssets: nativeAssets, NativeDecimals: nativeDecimals,
			RemoteSwapGas: remoteSwapGas,
			SwapGasProbes: swapGasProbes,
			AcrossGasFloor: map[market.ChainID]uint64{
				market.ChainID(localMarket.Chain):  config.Live.EVMGas.CostFixedLimit,
				market.ChainID(remoteMarket.Chain): config.Live.EVMGas.CostFixedLimit,
			},
			QuoteConversions: conversionBook, BridgeTokens: bridgeTokens, ConversionChain: conversionChain,
			ConversionGas: config.Live.EVMGas.CostFixedLimit,
			OnSizeFailure: func(direction arbitrage.Direction, input market.AssetQuantity, err error) {
				config.Logger.Warn("complete-flow cost size refresh failed", "buy_market", direction.BuyMarket,
					"sell_market", direction.SellMarket, "input", input.Rat().RatString(), "error", safeFlowCostError(err))
			},
			CandidateEligible: func(direction arbitrage.Direction, candidate arbitrage.Candidate) bool {
				return config.Live.InventoryCapacityMode != "confirmed_balance" ||
					balanceManager.CandidateEligible(direction, candidate)
			},
			CalibrationLimit: 10, Clock: time.Now,
		})
		if refreshErr != nil {
			return nil, refreshErr
		}
		directions := []arbitrage.Direction{
			{BuyMarket: localMarket.ID, SellMarket: remoteMarket.ID},
			{BuyMarket: remoteMarket.ID, SellMarket: localMarket.ID},
		}
		var staleCostFallback market.AssetQuantity
		if config.Live.StaleCostFallback != nil {
			staleCostFallback, err = market.NewAssetQuantity(
				localMarket.Quote.Token.Asset,
				config.Live.StaleCostFallback,
			)
			if err != nil {
				return nil, err
			}
		}
		flowCosts, err = NewFlowCostOracle(FlowCostOracleConfig{
			Directions: directions, QuoteAsset: localMarket.Quote.Token.Asset, Refresh: flowRefresh,
			RefreshInterval: config.Live.CostRefreshInterval, TTL: config.Live.CostCacheTTL,
			StaleFallback: staleCostFallback,
			Clock:         time.Now, Logger: config.Logger,
			StaleAlertAfter: 30 * time.Second,
			OnReady: func() {
				select {
				case reevaluate <- time.Now().UTC():
				default:
				}
			},
			OnStale: func(cause error) {
				if liveNotifier != nil {
					liveNotifier.NotifyRuntime(notificationport.LiveRuntimeEvent{Kind: notificationport.LiveRuntimeCostCacheStale,
						Reason: cause.Error(), OccurredAt: time.Now().UTC()})
				}
			},
			OnRecovered: func() {
				if liveNotifier != nil {
					liveNotifier.NotifyRuntime(notificationport.LiveRuntimeEvent{Kind: notificationport.LiveRuntimeCostCacheRecovered,
						Reason: "directional complete-flow cache refreshed", OccurredAt: time.Now().UTC()})
				}
			},
		})
		if err != nil {
			return nil, err
		}
		if err := runner.SetDirectionalCosts(flowCosts); err != nil {
			return nil, err
		}
		go flowCosts.Run(ctx)
		fmt.Fprintln(config.Output, "live_startup phase=complete_flow_cost_oracle_ready")
	}

	var progressObserver *ProgressObserver
	if liveNotifier != nil {
		progressObserver, err = NewProgressObserver(liveNotifier, map[market.TokenID]market.Token{
			localMarket.Base.Token.ID: localMarket.Base.Token, localMarket.Quote.Token.ID: localMarket.Quote.Token,
			remoteMarket.Base.Token.ID: remoteMarket.Base.Token, remoteMarket.Quote.Token.ID: remoteMarket.Quote.Token,
		}, map[market.ChainID]configuration.ResolvedChain{
			market.ChainID(localMarket.Chain): config.Research.Chains[localMarket.Chain], market.ChainID(remoteMarket.Chain): config.Research.Chains[remoteMarket.Chain]}, time.Now)
		if err != nil {
			return nil, err
		}
	}
	swapDriver := &SwapDriver{Bindings: bindings, TokenDecimals: map[market.TokenID]uint8{
		localMarket.Base.Token.ID: localMarket.Base.Token.Decimals, localMarket.Quote.Token.ID: localMarket.Quote.Token.Decimals,
		remoteMarket.Base.Token.ID: remoteMarket.Base.Token.Decimals, remoteMarket.Quote.Token.ID: remoteMarket.Quote.Token.Decimals,
	}, BridgePrecision: 8, QuoteAsset: localMarket.Quote.Token.Asset, BaseAsset: localMarket.Base.Token.Asset,
		MinimumNet: config.Live.MinimumNet, DirectionalMinimumNet: config.Live.DirectionalMinimumNet,
		MaximumCost:            config.Live.MaximumExecutionCost,
		ReturnMargin:           config.Live.ReturnBridgeSafetyMargin,
		ExitCosts:              flowCosts,
		DynamicSlippage:        DynamicSlippagePolicy{Enabled: config.Live.DynamicSlippage.Enabled, MaxBPS: config.Live.DynamicSlippage.MaxBPS, HeadroomBPS: config.Live.FirstLegHeadroomBPS},
		ExitValidationAttempts: config.Live.ExitValidationAttempts, ExitValidationRetryDelay: config.Live.ExitValidationRetryDelay,
		FallbackAfter: config.Live.ConfirmationTimeout, ArtifactMaxAge: config.Live.BuildToBroadcastTimeout,
		Output: config.Output, Costs: costValuator, RecoveryJournal: journal}
	drivers := executionport.DriverSet{Buy: swapDriver,
		BridgeBase: &BridgeDriver{Stage: execution.StageBridgeBase, Provider: baseBridge, Costs: costValuator}, Sell: swapDriver,
		BridgeQuoteReturn: &BridgeDriver{Stage: execution.StageBridgeQuoteReturn, Provider: quoteTransfer, Costs: costValuator}, ExitSelector: swapDriver}
	observer := executionport.SequentialObserver(balanceProgressObserver{progress: progressObserver, balances: balanceManager})
	executor, err := saga.NewSequentialExecutorWithObserver(journal, drivers, time.Now, observer)
	if err != nil {
		return nil, err
	}
	fmt.Fprintln(config.Output, "live_startup phase=executor_ready")
	asyncRestorer, err := NewAsyncQuoteRestorer(AsyncQuoteRestorerConfig{Context: ctx, Journal: journal,
		Driver: drivers.BridgeQuoteReturn, Observer: observer, Clock: time.Now, OnCapacity: func() {
			select {
			case reevaluate <- time.Now().UTC():
			default:
			}
		}})
	if err != nil {
		return nil, err
	}
	fmt.Fprintln(config.Output, "live_startup phase=async_restorer_ready")
	executor.SetAsyncQuoteRestorer(asyncRestorer)
	cleanup = append(cleanup, asyncRestorer.Close)
	recoveryObserver := NewRecoveryObserver(liveNotifier, progressObserver, time.Now)
	recoveryObserver.SetBalanceManager(balanceManager)
	recovery, err := saga.NewSequentialRecoveryCoordinator(saga.SequentialRecoveryConfig{Journal: journal, RecoveryJournal: journal,
		Drivers: drivers, Clock: time.Now, Observer: tracingRecoveryObserver{delegate: recoveryObserver, output: config.Output}, UncertainTimeout: 10 * time.Minute, CostValuator: costValuator})
	if err != nil {
		return nil, err
	}
	fmt.Fprintln(config.Output, "live_startup phase=recovery_ready")
	executionInput := config.Live.ExecutionInput
	allowedExecutionInputs := config.Live.ExecutionInputs
	if len(allowedExecutionInputs) <= 1 {
		allowedExecutionInputs = nil
	}
	if forcedDirection != nil {
		executionInput = config.Live.CanaryInput
		allowedExecutionInputs = nil
	}
	limit := 0
	if forcedDirection != nil {
		limit = 1
	}
	manager, err := NewManagerWithLimit(ctx, Planner{MarketChains: map[market.MarketID]market.ChainID{
		localMarket.ID: market.ChainID(localMarket.Chain), remoteMarket.ID: market.ChainID(remoteMarket.Chain)},
		ExecutionAmount: new(big.Rat).Set(executionInput), AllowedExecutionAmounts: cloneBigRats(allowedExecutionInputs),
		ExecutionPolicy: execution.ExecutionPolicyKind(config.Live.ExecutionPolicyKind),
		BaseAsset:       localMarket.Base.Token.Asset, QuoteAsset: localMarket.Quote.Token.Asset, TokenDecimals: swapDriver.TokenDecimals}, executor, limit)
	if err != nil {
		return nil, err
	}
	fmt.Fprintln(config.Output, "live_startup phase=manager_ready")
	manager.SetAdmission(combinedAdmission{balances: balanceManager, restoration: asyncRestorer})
	storeStem := strings.TrimSuffix(config.Live.OperationalStorePath, filepath.Ext(config.Live.OperationalStorePath))
	opportunityStore, err := sqlitestore.Open(storeStem + "-opportunities.sqlite")
	if err != nil {
		return nil, err
	}
	fmt.Fprintln(config.Output, "live_startup phase=opportunity_store_ready")
	cleanup = append(cleanup, func() { _ = opportunityStore.Close() })
	runtime, err := NewRuntimeWithGate(runner, manager, opportunityStore, cleanup, config.Output, liveNotifier,
		reevaluate, !config.ObserveCostsOnly, forcedDirection, runtimeGate)
	if err != nil {
		return nil, err
	}
	runtime.SetRecovery(recovery)
	runtime.SetBalanceManager(balanceManager)
	runtime.SetRestorationJournal(journal)
	runtime.SetRestorationResumer(asyncRestorer)
	runtime.SetCapacitySource(asyncRestorer)
	keep = true
	fmt.Fprintln(config.Output, "live_startup phase=runtime_ready")
	return runtime, nil
}

func validateExecutionSizing(research configuration.ParsedConfig, live configuration.ParsedLiveConfig) error {
	if len(live.ExecutionInputs) == 0 {
		return fmt.Errorf("live execution sizing is unavailable")
	}
	researchSizes := research.Sizes
	if len(researchSizes) == 0 && research.MinimumSize != nil && research.MaximumSize != nil &&
		research.MinimumSize.Cmp(research.MaximumSize) == 0 {
		researchSizes = []*big.Rat{research.MinimumSize}
	}
	if len(researchSizes) != len(live.ExecutionInputs) {
		return fmt.Errorf("research and Live execution size grids differ")
	}
	for index := range researchSizes {
		if researchSizes[index] == nil || live.ExecutionInputs[index] == nil ||
			researchSizes[index].Cmp(live.ExecutionInputs[index]) != 0 {
			return fmt.Errorf("research and Live execution size grids differ")
		}
	}
	return nil
}

// quoteConversionTokens returns the operational market quote and the token
// transported by the bridge on the same chain. The topology is deliberately
// explicit; there is no provider-selected token or spender fallback.
func quoteConversionTokens(config configuration.ParsedLiveConfig,
	configured configuration.ResolvedMarket,
) (configuration.ResolvedToken, configuration.ResolvedToken, bool) {
	for _, conversion := range config.QuoteConversions {
		if conversion.Chain != configured.Chain || conversion.BridgeToken == nil {
			continue
		}
		switch configured.Quote.Token.ID {
		case conversion.TokenA.Token.ID:
			if conversion.BridgeToken.Token.ID != conversion.TokenA.Token.ID {
				return conversion.TokenA, *conversion.BridgeToken, true
			}
		case conversion.TokenB.Token.ID:
			if conversion.BridgeToken.Token.ID != conversion.TokenB.Token.ID {
				return conversion.TokenB, *conversion.BridgeToken, true
			}
		}
	}
	return configuration.ResolvedToken{}, configuration.ResolvedToken{}, false
}

func cloneBigRats(values []*big.Rat) []*big.Rat {
	result := make([]*big.Rat, len(values))
	for index, value := range values {
		if value != nil {
			result[index] = new(big.Rat).Set(value)
		}
	}
	return result
}

// localExecutorGasProbe rebuilds the exact local allocation and executes an
// eth_estimateGas simulation in the background cost worker. It never signs,
// reserves a nonce, persists an operation, or runs on the decision hot path.
func localExecutorGasProbe(binding SwapBinding) EVMObservedSwapGasProbe {
	return func(ctx context.Context, quote market.Quote) (uint64, error) {
		if binding.Validator == nil || binding.LatestSnapshot == nil || binding.EVMClient == nil ||
			binding.EVMAddress == (common.Address{}) || quote.AmountIn.IsZero() || quote.AmountOut.IsZero() {
			return 0, fmt.Errorf("local executor gas probe is unavailable")
		}
		snapshot, ok := binding.LatestSnapshot()
		if !ok || snapshot.Metadata().Health != market.HealthHealthy || snapshot.Metadata().Market != quote.Market {
			return 0, fmt.Errorf("local executor gas probe snapshot is unavailable")
		}
		leg := execution.Leg{ID: "cost-probe", Side: execution.LegSell,
			Chain: binding.NativeToken.Chain, Account: binding.Account, Market: quote.Market,
			Input: quote.AmountIn, ExpectedOutput: quote.AmountOut}
		artifact, err := binding.Validator.Validate(ctx, executionport.ValidationRequest{
			Operation: "periodic-cost-probe", Leg: leg, Discovery: quote,
			Snapshot: snapshot, RequestedAt: time.Now().UTC()})
		if err != nil {
			return 0, err
		}
		toText := strings.TrimSpace(artifact.Metadata["to"])
		if !common.IsHexAddress(toText) || len(artifact.Payload) < 4 {
			return 0, fmt.Errorf("local executor gas probe artifact is invalid")
		}
		to := common.HexToAddress(toText)
		return binding.EVMClient.EstimateGas(ctx, geth.CallMsg{
			From: binding.EVMAddress, To: &to, Data: artifact.Payload,
		})
	}
}

type tracingRecoveryObserver struct {
	delegate *RecoveryObserver
	output   io.Writer
}

func monitorQuoteFXBlock(ctx context.Context, updates <-chan bool, notifier *LiveNotifier) {
	var timer *time.Timer
	var timerC <-chan time.Time
	blocked, alerted := false, false
	stop := func() {
		if timer != nil && !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer, timerC = nil, nil
	}
	defer stop()
	for {
		select {
		case <-ctx.Done():
			return
		case value := <-updates:
			if value && !blocked {
				blocked = true
				timer = time.NewTimer(30 * time.Second)
				timerC = timer.C
			} else if !value && blocked {
				blocked = false
				stop()
				if alerted {
					notifier.NotifyRuntime(notificationport.LiveRuntimeEvent{Kind: notificationport.LiveRuntimeQuoteFXRecovered,
						Reason: "quote-token conversion cache refreshed", OccurredAt: time.Now().UTC()})
					alerted = false
				}
			}
		case <-timerC:
			timerC = nil
			if blocked && !alerted {
				notifier.NotifyRuntime(notificationport.LiveRuntimeEvent{Kind: notificationport.LiveRuntimeQuoteFXStale,
					Reason: "quote-token conversion cache blocked admission continuously", OccurredAt: time.Now().UTC()})
				alerted = true
			}
		}
	}
}

func (o tracingRecoveryObserver) RecoveryStarted(snapshot executionport.SequentialRecoverySnapshot) {
	fmt.Fprintln(o.output, "live_recovery phase=observer_started")
	o.delegate.RecoveryStarted(snapshot)
	fmt.Fprintln(o.output, "live_recovery phase=observer_ready")
}
func (o tracingRecoveryObserver) RecoveryAttempt(attempt executionport.SequentialRecoveryAttempt) {
	o.delegate.RecoveryAttempt(attempt)
}
func (o tracingRecoveryObserver) RecoveryCompleted(result executionport.SequentialResult) {
	o.delegate.RecoveryCompleted(result)
}

func (o tracingRecoveryObserver) RecoveryAborted(
	operation execution.SequentialOperation,
	result executionport.SequentialResult,
	err error,
) {
	o.delegate.RecoveryAborted(operation, result, err)
}
func (o tracingRecoveryObserver) RecoveryBlocked(operation execution.SequentialOperation, err error) {
	o.delegate.RecoveryBlocked(operation, err)
}

func composeHybridLocalRecoveryEstimator(configured configuration.ResolvedMarket, live configuration.ParsedLiveConfig, network evmadapter.Network) (SwapQuoteEstimator, error) {
	if network == nil {
		return nil, fmt.Errorf("hybrid local recovery network is unavailable")
	}
	tokens := map[market.TokenID]common.Address{
		configured.Base.Token.ID:  configured.Base.Address,
		configured.Quote.Token.ID: configured.Quote.Address,
	}
	var quote func(context.Context, common.Address, common.Address, *big.Int) (*big.Int, error)
	switch configured.Venue.Kind {
	case "aerodrome_slipstream":
		quoter, err := aerodromeslipstream.NewReferenceQuoter(configured.Venue.Reference)
		if err != nil {
			return nil, err
		}
		quote = func(ctx context.Context, tokenIn, tokenOut common.Address, amount *big.Int) (*big.Int, error) {
			block, err := network.CurrentBlock(ctx)
			if err != nil {
				return nil, err
			}
			return quoter.QuoteExactInputSingle(ctx, network, block, tokenIn, tokenOut, amount, live.LocalSwapTickSpacing)
		}
	case "pancakeswap_v3":
		quoter, err := uniswapv3.NewReferenceQuoter(configured.Venue.Reference)
		if err != nil {
			return nil, err
		}
		quote = func(ctx context.Context, tokenIn, tokenOut common.Address, amount *big.Int) (*big.Int, error) {
			block, err := network.CurrentBlock(ctx)
			if err != nil {
				return nil, err
			}
			return quoter.QuoteExactInputSingle(ctx, network, block, tokenIn, tokenOut, amount, live.LocalSwapFee)
		}
	default:
		return nil, fmt.Errorf("hybrid local recovery venue %q is unsupported", configured.Venue.Kind)
	}
	return SwapQuoteEstimatorFunc(func(ctx context.Context, input market.TokenAmount, output market.TokenID) (market.TokenAmount, error) {
		tokenIn, inputOK := tokens[input.Token()]
		tokenOut, outputOK := tokens[output]
		if !inputOK || !outputOK || input.IsZero() {
			return market.TokenAmount{}, fmt.Errorf("hybrid local recovery token mapping is unavailable")
		}
		units, err := quote(ctx, tokenIn, tokenOut, input.Units())
		if err != nil {
			return market.TokenAmount{}, err
		}
		return market.NewTokenAmount(output, units)
	}), nil
}

type combinedAdmission struct {
	balances    PlanAdmission
	restoration PlanAdmission
}

func (a combinedAdmission) Admit(plan execution.SequentialPlan) error {
	if a.restoration != nil {
		if err := a.restoration.Admit(plan); err != nil {
			return err
		}
	}
	if a.balances != nil {
		return a.balances.Admit(plan)
	}
	return nil
}

func splitHybridMarkets(config configuration.ParsedConfig) (configuration.ResolvedMarket, configuration.ResolvedMarket, error) {
	var local, remote configuration.ResolvedMarket
	for _, configured := range config.Markets {
		source, hasSource := config.QuoteSources[configured.QuoteSource]
		if hasSource && source.Kind == "kyberswap" {
			remote = configured
		} else {
			local = configured
		}
	}
	if remote.ID == "" {
		for _, configured := range config.Markets {
			if configured.SplitRoute != nil {
				remote = configured
			} else {
				local = configured
			}
		}
	}
	if local.ID == "" || remote.ID == "" || local.Base.Token.Asset != remote.Base.Token.Asset || local.Quote.Token.Asset != remote.Quote.Token.Asset {
		return local, remote, fmt.Errorf("hybrid EVM Live requires one local and one KyberSwap market with equivalent assets")
	}
	return local, remote, nil
}

func approvalDestinations(config configuration.ParsedLiveConfig, chain, purpose string) []common.Address {
	result := make([]common.Address, 0, 2)
	seen := make(map[common.Address]struct{})
	for _, item := range config.ApprovalSpenders {
		if item.Chain != market.ChainID(chain) || !strings.Contains(strings.ToLower(item.Purpose), purpose) {
			continue
		}
		if _, ok := seen[item.Spender]; !ok {
			seen[item.Spender] = struct{}{}
			result = append(result, item.Spender)
		}
	}
	return result
}

func ensureHybridApprovals(ctx context.Context, config ComposeConfig, markets []configuration.ResolvedMarket,
	bindings map[market.MarketID]SwapBinding, journal *sqlitestore.SequentialLiveStore, armed bool) error {
	byChain := make(map[string]configuration.ResolvedMarket, 2)
	for _, configured := range markets {
		byChain[configured.Chain] = configured
	}
	requirements := make([]corelive.AllowanceRequirement, 0, len(config.Live.ApprovalSpenders))
	revocations := make([]corelive.AllowanceRequirement, 0, len(config.Live.ApprovalRevocations))
	managerMap := make(map[market.ChainID]chainTxManager, 2)
	addresses := make(map[market.ChainID]map[market.TokenID]common.Address, 2)
	for _, configured := range markets {
		chain := market.ChainID(configured.Chain)
		managerMap[chain] = bindings[configured.ID].TxManager
		addresses[chain] = map[market.TokenID]common.Address{configured.Base.Token.ID: configured.Base.Address, configured.Quote.Token.ID: configured.Quote.Address}
	}
	for _, conversion := range config.Live.QuoteConversions {
		chain := market.ChainID(conversion.Chain)
		if addresses[chain] == nil {
			addresses[chain] = make(map[market.TokenID]common.Address)
		}
		addresses[chain][conversion.TokenA.Token.ID] = conversion.TokenA.Address
		addresses[chain][conversion.TokenB.Token.ID] = conversion.TokenB.Address
	}
	reader := allowanceBootstrapReader{bindings: bindings, markets: byChain}
	for _, item := range config.Live.ApprovalSpenders {
		requirements = append(requirements, corelive.AllowanceRequirement{
			Chain: market.ChainID(item.Chain), Token: market.TokenID(item.Token), Spender: item.Spender.Hex(), Purpose: item.Purpose})
	}
	for _, item := range config.Live.ApprovalRevocations {
		revocations = append(revocations, corelive.AllowanceRequirement{Chain: item.Chain, Token: item.Token,
			Spender: item.Spender.Hex(), Purpose: item.Purpose})
	}
	var writer corelive.ApprovalWriter
	if armed {
		writerManagers := make(map[market.ChainID]chainport.TxManager, len(managerMap))
		for id, manager := range managerMap {
			writerManagers[id] = manager
		}
		approvalWriter := &EVMApprovalWriter{Managers: writerManagers, TokenAddresses: addresses, Journal: journal,
			Clock: time.Now, Timeout: config.Live.ConfirmationTimeout}
		if err := approvalWriter.ReconcilePending(ctx); err != nil {
			return err
		}
		writer = approvalWriter
	}
	results, err := (corelive.AllowanceBootstrap{Reader: reader, Writer: writer,
		Requirements: requirements, Revocations: revocations, Clock: time.Now,
		VerifyTimeout: 10 * time.Second, VerifyInterval: 250 * time.Millisecond,
	}).Ensure(ctx, armed)
	if err != nil {
		return err
	}
	for _, result := range results {
		fmt.Fprintf(config.Output, "live_approval chain=%s token=%s purpose=%s changed=%t reset=%t\n",
			result.Requirement.Chain, result.Requirement.Token, result.Requirement.Purpose, result.Changed, result.Reset)
	}
	return nil
}

type chainTxManager = chainport.TxManager

type allowanceBootstrapReader struct {
	bindings map[market.MarketID]SwapBinding
	markets  map[string]configuration.ResolvedMarket
}

func (r allowanceBootstrapReader) Allowance(ctx context.Context, requirement corelive.AllowanceRequirement) (*big.Int, error) {
	configured, ok := r.markets[string(requirement.Chain)]
	if !ok {
		return nil, fmt.Errorf("allowance chain is unavailable")
	}
	binding := r.bindings[configured.ID]
	if binding.Allowance == nil {
		return nil, fmt.Errorf("allowance reader is unavailable")
	}
	return binding.Allowance.Allowance(ctx, requirement.Token, requirement.Spender)
}
