package livecanary

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	acrossadapter "github.com/VarozXYZ/vernier/adapters/crosschain/across"
	sqlitestore "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	"github.com/VarozXYZ/vernier/runtime/configuration"
)

type evmFlowCalibrationSource interface {
	LatestEVMFlowTransactions(context.Context, market.MarketID, market.MarketID, int, string, int) ([]sqlitestore.EVMFlowTransaction, error)
}

type evmAcrossCostSource interface {
	CostApproval(context.Context, market.ChainID, *big.Int) (acrossadapter.EVMCostApproval, error)
}

type evmWTTMessageFeeSource interface {
	MessageFee(context.Context, market.ChainID) (*big.Int, time.Time, error)
	EstimateTransferGas(context.Context, market.ChainID, market.ChainID, market.TokenAmount) (uint64, error)
	RedemptionGasFloor(market.ChainID) (uint64, error)
}

type quoteConversionCostSource interface {
	Snapshot(market.TokenID, market.TokenID, time.Time) (market.QuoteConversionSnapshot, bool)
}

type EVMObservedSwapGasProbe func(context.Context, market.Quote) (uint64, error)

type EVMObservedFlowCostRefreshConfig struct {
	Markets           map[market.MarketID]configuration.ResolvedMarket
	Valuator          *CostValuator
	Calibration       evmFlowCalibrationSource
	Across            evmAcrossCostSource
	WTT               evmWTTMessageFeeSource
	Fees              map[market.ChainID]EVMFeeCostSource
	NativeAssets      map[market.ChainID]market.AssetID
	NativeDecimals    map[market.ChainID]uint8
	RemoteSwapGas     map[market.MarketID]uint64
	SwapGasProbes     map[market.MarketID]EVMObservedSwapGasProbe
	AcrossGasFloor    map[market.ChainID]uint64
	QuoteConversions  quoteConversionCostSource
	BridgeTokens      map[market.ChainID]market.Token
	ConversionChain   market.ChainID
	ConversionGas     uint64
	CalibrationLimit  int
	CalibrationTTL    time.Duration
	Clock             func() time.Time
	OnSizeFailure     func(arbitrage.Direction, market.AssetQuantity, error)
	CandidateEligible func(arbitrage.Direction, arbitrage.Candidate) bool
	calibrationState  *evmGasCalibrationState
}

type evmGasCalibrationState struct {
	mu      sync.Mutex
	entries map[string]evmGasCalibrationEntry
}

type evmGasCalibrationEntry struct {
	gas      uint64
	err      error
	loadedAt time.Time
}

// NewEVMObservedFlowCostRefresh builds the EVM-to-EVM complete-flow estimator.
// It uses confirmed receipt gas as calibration, current fee/native-price
// caches, current Wormhole message fees, and a fresh read-only Across approval.
func NewEVMObservedFlowCostRefresh(config EVMObservedFlowCostRefreshConfig) (FlowCostRefresh, error) {
	if len(config.Markets) != 2 || config.Valuator == nil || config.Calibration == nil ||
		config.Across == nil || config.WTT == nil || len(config.Fees) != 2 ||
		len(config.NativeAssets) != 2 || len(config.NativeDecimals) != 2 {
		return nil, fmt.Errorf("EVM complete-flow cost refresh is incomplete")
	}
	if config.CalibrationLimit <= 0 {
		config.CalibrationLimit = 10
	}
	if config.CalibrationTTL <= 0 {
		config.CalibrationTTL = 5 * time.Minute
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	config.calibrationState = &evmGasCalibrationState{entries: make(map[string]evmGasCalibrationEntry)}
	return func(ctx context.Context, opportunities []arbitrage.Opportunity) ([]FlowCostEstimate, error) {
		usable := make([]arbitrage.Opportunity, 0, len(opportunities)*4)
		directions := make(map[arbitrage.Direction]bool, 2)
		for _, opportunity := range opportunities {
			for _, candidate := range opportunity.Candidates {
				if candidate.SellQuote.AmountOut.IsZero() {
					continue
				}
				if config.CandidateEligible != nil && !config.CandidateEligible(opportunity.Direction, candidate) {
					continue
				}
				if candidate.Input.Asset() == "" {
					configured, ok := config.Markets[opportunity.Direction.BuyMarket]
					if !ok {
						continue
					}
					input, inputErr := candidate.BuyQuote.AmountIn.ToAssetQuantity(configured.Quote.Token)
					if inputErr != nil {
						sell, sellOK := config.Markets[opportunity.Direction.SellMarket]
						if !sellOK {
							continue
						}
						input, inputErr = candidate.SellQuote.AmountOut.ToAssetQuantity(sell.Quote.Token)
						if inputErr != nil {
							continue
						}
					}
					candidate.Input = input
				}
				copy := opportunity
				copy.Candidates = []arbitrage.Candidate{candidate}
				copy.SelectedIndex = 0
				usable = append(usable, copy)
				directions[opportunity.Direction] = true
			}
		}
		if len(directions) != 2 {
			return nil, fmt.Errorf("EVM complete-flow cost refresh requires both observed directions")
		}
		wttSourceFloors := make(map[arbitrage.Direction]uint64, len(directions))
		for direction := range directions {
			index := smallestObservedInput(usable, direction)
			if index < 0 {
				return nil, fmt.Errorf("EVM complete-flow cost refresh has no WTT probe candidate for %s", direction)
			}
			buy := config.Markets[direction.BuyMarket]
			sell := config.Markets[direction.SellMarket]
			floor, probeErr := config.WTT.EstimateTransferGas(ctx, market.ChainID(buy.Chain), market.ChainID(sell.Chain),
				usable[index].Candidates[0].BuyQuote.AmountOut)
			if probeErr != nil {
				return nil, fmt.Errorf("simulate WTT source gas for %s: %w", buy.Chain, probeErr)
			}
			wttSourceFloors[direction] = floor
		}
		results := make([]FlowCostEstimate, len(usable))
		errs := make([]error, len(usable))
		var group sync.WaitGroup
		for index, opportunity := range usable {
			index, opportunity := index, opportunity
			group.Add(1)
			go func() {
				defer group.Done()
				results[index], errs[index] = estimateEVMObservedDirection(ctx, config, opportunity, wttSourceFloors[opportunity.Direction])
			}()
		}
		group.Wait()
		successful := make([]FlowCostEstimate, 0, len(results))
		successfulDirections := make(map[arbitrage.Direction]bool, 2)
		failures := make([]error, 0, len(errs))
		for index, err := range errs {
			if err != nil {
				if config.OnSizeFailure != nil {
					config.OnSizeFailure(usable[index].Direction, usable[index].Candidates[0].Input, err)
				}
				failures = append(failures, fmt.Errorf("%s input %s: %w",
					usable[index].Direction, usable[index].Candidates[0].Input, err))
				continue
			}
			successful = append(successful, results[index])
			successfulDirections[results[index].Direction] = true
		}
		if len(successfulDirections) != len(directions) {
			return nil, fmt.Errorf("EVM complete-flow cost refresh has no successful size for every direction: %w", errors.Join(failures...))
		}
		return successful, nil
	}, nil
}

func smallestObservedInput(opportunities []arbitrage.Opportunity, direction arbitrage.Direction) int {
	best := -1
	for index, opportunity := range opportunities {
		if opportunity.Direction != direction || len(opportunity.Candidates) == 0 {
			continue
		}
		if best < 0 || opportunity.Candidates[0].Input.Rat().Cmp(opportunities[best].Candidates[0].Input.Rat()) < 0 {
			best = index
		}
	}
	return best
}

func estimateEVMObservedDirection(ctx context.Context, config EVMObservedFlowCostRefreshConfig, opportunity arbitrage.Opportunity,
	wttSourceFloor uint64) (FlowCostEstimate, error) {
	direction := opportunity.Direction
	buy, buyOK := config.Markets[direction.BuyMarket]
	sell, sellOK := config.Markets[direction.SellMarket]
	if !buyOK || !sellOK {
		return FlowCostEstimate{}, fmt.Errorf("EVM cost direction references unavailable markets")
	}
	buyChain, sellChain := market.ChainID(buy.Chain), market.ChainID(sell.Chain)
	components := make([]FlowCostComponent, 0, 8)

	buyGasFloor := config.RemoteSwapGas[direction.BuyMarket]
	if probe := config.SwapGasProbes[direction.BuyMarket]; probe != nil {
		probed, probeErr := probe(ctx, opportunity.Candidates[0].BuyQuote)
		if probeErr != nil {
			return FlowCostEstimate{}, fmt.Errorf("simulate buy swap gas: %w", probeErr)
		}
		if probed > buyGasFloor {
			buyGasFloor = probed
		}
	}
	buySwap, err := calibratedEVMGasComponent(ctx, config, direction, 1, "swap", buyChain, "swap_buy", buyGasFloor)
	if err != nil {
		return FlowCostEstimate{}, err
	}
	components = append(components, buySwap)
	sellGasFloor := config.RemoteSwapGas[direction.SellMarket]
	if probe := config.SwapGasProbes[direction.SellMarket]; probe != nil {
		probed, probeErr := probe(ctx, opportunity.Candidates[0].SellQuote)
		if probeErr != nil {
			return FlowCostEstimate{}, fmt.Errorf("simulate sell swap gas: %w", probeErr)
		}
		if probed > sellGasFloor {
			sellGasFloor = probed
		}
	}
	sellSwap, err := calibratedEVMGasComponent(ctx, config, direction, 2, "swap", sellChain, "swap_sell", sellGasFloor)
	if err != nil {
		return FlowCostEstimate{}, err
	}
	components = append(components, sellSwap)

	wttSource, err := calibratedEVMGasComponent(ctx, config, direction, 3, "wtt_source", buyChain, "base_bridge_source", wttSourceFloor)
	if err != nil {
		return FlowCostEstimate{}, err
	}
	components = append(components, wttSource)
	wttRedeemFloor, err := config.WTT.RedemptionGasFloor(sellChain)
	if err != nil {
		return FlowCostEstimate{}, err
	}
	wttRedeem, err := calibratedEVMGasComponent(ctx, config, direction, 3, "wtt_redeem", sellChain, "base_bridge_redeem", wttRedeemFloor)
	if err != nil {
		return FlowCostEstimate{}, err
	}
	components = append(components, wttRedeem)
	messageFee, capturedAt, err := config.WTT.MessageFee(ctx, buyChain)
	if err != nil {
		return FlowCostEstimate{}, fmt.Errorf("read WTT message fee for %s: %w", buyChain, err)
	}
	messageComponent, err := valueEVMNativeCost(config, buyChain, messageFee, "base_bridge_message_fee", "wormhole_core_message_fee", capturedAt)
	if err != nil {
		return FlowCostEstimate{}, err
	}
	components = append(components, messageComponent)

	input := opportunity.Candidates[0].SellQuote.AmountOut
	bridgeInput := input
	if bridgeToken, ok := config.BridgeTokens[sellChain]; ok && bridgeToken.ID != input.Token() {
		snapshot, snapshotOK := config.QuoteConversions.Snapshot(input.Token(), bridgeToken.ID, config.Clock().UTC())
		if !snapshotOK {
			return FlowCostEstimate{}, fmt.Errorf("quote conversion cache for Across input is unavailable")
		}
		bridgeInput, err = convertTokenAmount(input, bridgeToken, snapshot)
		if err != nil {
			return FlowCostEstimate{}, err
		}
	}
	approval, err := config.Across.CostApproval(ctx, sellChain, bridgeInput.Units())
	if err != nil {
		return FlowCostEstimate{}, fmt.Errorf("estimate Across return cost for %s: %w", sellChain, err)
	}
	sourceBridgeToken := sell.Quote.Token
	if configured, ok := config.BridgeTokens[sellChain]; ok {
		sourceBridgeToken = configured
	}
	destinationBridgeToken := buy.Quote.Token
	if configured, ok := config.BridgeTokens[buyChain]; ok {
		destinationBridgeToken = configured
	}
	spread, err := acrossEVMSpread(sourceBridgeToken, destinationBridgeToken, approval)
	if err != nil {
		return FlowCostEstimate{}, err
	}
	components = append(components, FlowCostComponent{Kind: "quote_bridge_spread", Amount: spread,
		Evidence: "across_approval_expected_output", CapturedAt: approval.ObservedAt})
	acrossGas := approval.SourceGas
	if floor := config.AcrossGasFloor[sellChain]; floor > acrossGas {
		acrossGas = floor
	}
	calibratedAcross, calibrationErr := calibratedEVMGas(ctx, config, direction, 4, "across_source", sellChain)
	if calibrationErr == nil && calibratedAcross > acrossGas {
		acrossGas = calibratedAcross
	}
	if acrossGas == 0 {
		if calibrationErr != nil {
			return FlowCostEstimate{}, fmt.Errorf("across source gas is unavailable: %w", calibrationErr)
		}
		return FlowCostEstimate{}, fmt.Errorf("across source gas is unavailable")
	}
	acrossComponent, err := currentEVMGasComponent(config, sellChain, acrossGas, "quote_bridge_source", "across_approval_configured_or_real_high_water_gas")
	if err != nil {
		return FlowCostEstimate{}, err
	}
	components = append(components, acrossComponent)
	if config.ConversionChain != "" {
		phase := "quote_convert_destination"
		if sellChain == config.ConversionChain {
			phase = "quote_convert_source"
		}
		conversionComponent, conversionErr := calibratedEVMGasComponent(ctx, config, direction, 4,
			phase, config.ConversionChain, "quote_conversion_swap", config.ConversionGas)
		if conversionErr != nil {
			return FlowCostEstimate{}, conversionErr
		}
		components = append(components, conversionComponent)
	}
	return FlowCostEstimate{Direction: direction, Input: opportunity.Candidates[0].Input, Components: components}, nil
}

func calibratedEVMGasComponent(ctx context.Context, config EVMObservedFlowCostRefreshConfig, direction arbitrage.Direction, ordinal int, phase string, chain market.ChainID, kind string, floor uint64) (FlowCostComponent, error) {
	if floor > 0 {
		gas, calibrationErr := calibratedEVMGas(ctx, config, direction, ordinal, phase, chain)
		evidence := "simulated_or_configured_expected_gas+evm_fee_cache"
		if calibrationErr == nil && gas > floor {
			floor, evidence = gas, "real_high_water_gas+evm_fee_cache"
		}
		return currentEVMGasComponent(config, chain, floor, kind, evidence)
	}
	gas, err := calibratedEVMGas(ctx, config, direction, ordinal, phase, chain)
	if err != nil {
		return FlowCostComponent{}, err
	}
	if gas == 0 {
		return FlowCostComponent{}, fmt.Errorf("%s gas calibration is unavailable", kind)
	}
	return currentEVMGasComponent(config, chain, gas, kind, "real_high_water_gas+evm_fee_cache")
}

func calibratedEVMGas(ctx context.Context, config EVMObservedFlowCostRefreshConfig, direction arbitrage.Direction, ordinal int, phase string, chain market.ChainID) (uint64, error) {
	key := fmt.Sprintf("%s/%s/%d/%s/%s", direction.BuyMarket, direction.SellMarket, ordinal, phase, chain)
	now := config.Clock().UTC()
	config.calibrationState.mu.Lock()
	if cached, ok := config.calibrationState.entries[key]; ok && now.Sub(cached.loadedAt) < config.CalibrationTTL {
		config.calibrationState.mu.Unlock()
		return cached.gas, cached.err
	}
	config.calibrationState.mu.Unlock()

	transactions, err := config.Calibration.LatestEVMFlowTransactions(ctx, direction.BuyMarket, direction.SellMarket, ordinal, phase, config.CalibrationLimit)
	if err != nil {
		cacheEVMGasCalibration(config, key, 0, err, now)
		return 0, err
	}
	fees := config.Fees[chain]
	if fees == nil {
		err = fmt.Errorf("EVM fee source for %s is unavailable", chain)
		cacheEVMGasCalibration(config, key, 0, err, now)
		return 0, err
	}
	var maximum uint64
	var mu sync.Mutex
	var group sync.WaitGroup
	for _, transaction := range transactions {
		if transaction.Chain != chain {
			continue
		}
		identity := transaction.Identity
		group.Add(1)
		go func() {
			defer group.Done()
			gas, gasErr := fees.ConfirmedGasUsed(ctx, identity)
			if gasErr != nil {
				return
			}
			mu.Lock()
			if gas > maximum {
				maximum = gas
			}
			mu.Unlock()
		}()
	}
	group.Wait()
	if maximum == 0 {
		err = fmt.Errorf("no confirmed %s gas calibration for %s", phase, chain)
		cacheEVMGasCalibration(config, key, 0, err, now)
		return 0, err
	}
	cacheEVMGasCalibration(config, key, maximum, nil, now)
	return maximum, nil
}

func cacheEVMGasCalibration(config EVMObservedFlowCostRefreshConfig, key string, gas uint64, err error, at time.Time) {
	config.calibrationState.mu.Lock()
	config.calibrationState.entries[key] = evmGasCalibrationEntry{gas: gas, err: err, loadedAt: at}
	config.calibrationState.mu.Unlock()
}

func currentEVMGasComponent(config EVMObservedFlowCostRefreshConfig, chain market.ChainID, gas uint64, kind, evidence string) (FlowCostComponent, error) {
	feeSource := config.Fees[chain]
	if feeSource == nil {
		return FlowCostComponent{}, fmt.Errorf("EVM fee source for %s is unavailable", chain)
	}
	fees, ok := feeSource.FeeSnapshot()
	if !ok {
		return FlowCostComponent{}, fmt.Errorf("EVM fee cache for %s is unavailable", chain)
	}
	price := new(big.Int).Add(fees.BaseFee, fees.TipCap)
	units := new(big.Int).Mul(new(big.Int).SetUint64(gas), price)
	return valueEVMNativeCost(config, chain, units, kind, evidence, fees.CapturedAt)
}

func valueEVMNativeCost(config EVMObservedFlowCostRefreshConfig, chain market.ChainID, units *big.Int, kind, evidence string, capturedAt time.Time) (FlowCostComponent, error) {
	amount, err := tokenUnitsQuantity(config.NativeAssets[chain], units, config.NativeDecimals[chain])
	if err != nil {
		return FlowCostComponent{}, err
	}
	valued, err := config.Valuator.Value(execution.CostComponent{Kind: kind, Chain: chain, Amount: amount, Evidence: evidence})
	if err != nil {
		return FlowCostComponent{}, err
	}
	price, ok := config.Valuator.Price(config.NativeAssets[chain])
	if !ok {
		return FlowCostComponent{}, fmt.Errorf("native price cache for %s is unavailable", chain)
	}
	if price.CapturedAt.Before(capturedAt) {
		capturedAt = price.CapturedAt
	}
	return FlowCostComponent{Kind: kind, Amount: valued.QuoteValue, Evidence: valued.Evidence, CapturedAt: capturedAt.UTC()}, nil
}

func acrossEVMSpread(source, destination market.Token, approval acrossadapter.EVMCostApproval) (market.AssetQuantity, error) {
	input := new(big.Rat).SetFrac(approval.InputUnits, decimalScale(source.Decimals))
	output := new(big.Rat).SetFrac(approval.ExpectedOutputUnits, decimalScale(destination.Decimals))
	spread := new(big.Rat).Sub(input, output)
	if spread.Sign() < 0 {
		return market.AssetQuantity{}, fmt.Errorf("across expected output exceeds human input")
	}
	return market.NewAssetQuantity(source.Asset, spread)
}

func convertTokenAmount(input market.TokenAmount, output market.Token,
	snapshot market.QuoteConversionSnapshot) (market.TokenAmount, error) {
	if snapshot.Input.Token() != input.Token() || snapshot.Output.Token() != output.ID {
		return market.TokenAmount{}, fmt.Errorf("quote conversion snapshot does not match Across input")
	}
	// All arithmetic stays integer and rounds down so the cost oracle never
	// asks Across to approve more transit tokens than the conversion predicts.
	units := new(big.Int).Mul(input.Units(), snapshot.Output.Units())
	units.Quo(units, snapshot.Input.Units())
	if units.Sign() <= 0 {
		return market.TokenAmount{}, fmt.Errorf("quote conversion rounds Across input to zero")
	}
	return market.NewTokenAmount(output.ID, units)
}
