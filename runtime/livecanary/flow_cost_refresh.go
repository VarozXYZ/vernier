package livecanary

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	solanago "github.com/gagliardetto/solana-go"

	evmadapter "github.com/VarozXYZ/vernier/adapters/chain/evm"
	solanaadapter "github.com/VarozXYZ/vernier/adapters/chain/solana"
	sqlitestore "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	"github.com/VarozXYZ/vernier/internal/acrossbridgecanary"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	"github.com/VarozXYZ/vernier/runtime/configuration"
)

type artifactNetworkCostEstimator interface {
	EstimateArtifactNetworkCost(
		context.Context,
		executionport.Artifact,
	) (*big.Int, time.Time, error)
}

type detailedArtifactNetworkCostEstimator interface {
	EstimateArtifactNetworkCostDetails(
		context.Context,
		executionport.Artifact,
	) (solanaadapter.ArtifactNetworkCostEstimate, error)
}

type routeGasEstimator interface {
	EstimateNetworkGas(context.Context, market.TokenAmount, market.TokenID) (uint64, error)
}

type cachedEVMFeeSource interface {
	FeeSnapshot() (evmadapter.FeeSnapshot, bool)
	ConfirmedGasUsed(context.Context, string) (uint64, error)
}

type solanaMessageFeeSource interface {
	FeeForMessage(context.Context, string) (uint64, error)
	ConfirmedPayerDebit(context.Context, string) (uint64, error)
}

type nttCostCalibrationSource interface {
	LatestCostCalibration(context.Context, string, int) (sqlitestore.NTTCostCalibration, error)
	LatestCompletedTransactions(context.Context, string) ([]sqlitestore.NTTCanaryTransaction, time.Time, error)
}

type acrossCostApprovalSource interface {
	CostApproval(context.Context, market.ChainID, *big.Int) (acrossbridgecanary.CostApproval, error)
}

type acrossCostCalibrationSource interface {
	LatestCompletedSource(context.Context, string) (string, time.Time, error)
}

type observedFlowCostState struct {
	mu          sync.Mutex
	calibration map[string]sqlitestore.NTTCostCalibration
}

type ObservedFlowCostRefreshConfig struct {
	Markets           map[market.MarketID]configuration.ResolvedMarket
	Bindings          map[market.MarketID]SwapBinding
	Chains            map[string]configuration.ResolvedChain
	Valuator          *CostValuator
	NTTCalibration    nttCostCalibrationSource
	Across            acrossCostApprovalSource
	AcrossCalibration acrossCostCalibrationSource
	EVMFees           cachedEVMFeeSource
	SolanaFees        solanaMessageFeeSource
	NativeAssets      map[market.ChainID]market.AssetID
	NativeDecimals    map[market.ChainID]uint8
	Clock             func() time.Time
}

// NewObservedFlowCostRefresh builds the out-of-band estimator used by Live.
// It validates/simulates current swap artifacts, reprices measured NTT
// high-water resource usage, and requests a fresh Across approval. None of
// these effects run from the Research or execution hot path.
func NewObservedFlowCostRefresh(
	config ObservedFlowCostRefreshConfig,
) (FlowCostRefresh, error) {
	if len(config.Markets) != 2 || len(config.Bindings) != 2 ||
		len(config.Chains) != 2 || config.Valuator == nil ||
		config.NTTCalibration == nil || config.Across == nil ||
		config.AcrossCalibration == nil ||
		config.EVMFees == nil || config.SolanaFees == nil ||
		len(config.NativeAssets) != 2 || len(config.NativeDecimals) != 2 {
		return nil, fmt.Errorf("observed complete-flow cost refresh is incomplete")
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	state := &observedFlowCostState{
		calibration: make(map[string]sqlitestore.NTTCostCalibration, 2),
	}
	return func(
		ctx context.Context,
		opportunities []arbitrage.Opportunity,
	) ([]FlowCostEstimate, error) {
		usable := make([]arbitrage.Opportunity, 0, len(opportunities))
		for _, opportunity := range opportunities {
			if len(opportunity.Candidates) != 1 {
				continue
			}
			candidate := opportunity.Candidates[0]
			if candidate.BuyQuote.AmountIn.IsZero() ||
				candidate.SellQuote.AmountIn.IsZero() {
				continue
			}
			usable = append(usable, opportunity)
		}
		if len(usable) != 2 {
			return nil, fmt.Errorf("complete-flow cost refresh requires both observed routes")
		}
		results := make([]FlowCostEstimate, len(usable))
		errorsByIndex := make([]error, len(usable))
		var group sync.WaitGroup
		for index, opportunity := range usable {
			index, opportunity := index, opportunity
			group.Add(1)
			go func() {
				defer group.Done()
				results[index], errorsByIndex[index] =
					estimateObservedDirection(ctx, config, state, opportunity)
			}()
		}
		group.Wait()
		for _, err := range errorsByIndex {
			if err != nil {
				return nil, err
			}
		}
		return results, nil
	}, nil
}

func estimateObservedDirection(
	ctx context.Context,
	config ObservedFlowCostRefreshConfig,
	state *observedFlowCostState,
	opportunity arbitrage.Opportunity,
) (FlowCostEstimate, error) {
	candidate := opportunity.Candidates[0]
	type componentResult struct {
		components []FlowCostComponent
		err        error
	}
	results := make(chan componentResult, 4)
	go func() {
		component, err := estimateObservedSwap(
			ctx, config, opportunity.Direction.BuyMarket,
			execution.LegBuy, candidate.BuyQuote,
		)
		results <- componentResult{components: []FlowCostComponent{component}, err: err}
	}()
	go func() {
		component, err := estimateObservedSwap(
			ctx, config, opportunity.Direction.SellMarket,
			execution.LegSell, candidate.SellQuote,
		)
		results <- componentResult{components: []FlowCostComponent{component}, err: err}
	}()
	go func() {
		components, err := estimateObservedNTT(
			ctx, config, state, opportunity.Direction,
		)
		results <- componentResult{components: components, err: err}
	}()
	go func() {
		components, err := estimateObservedAcross(
			ctx, config, opportunity.Direction, candidate.SellQuote.AmountOut,
		)
		results <- componentResult{components: components, err: err}
	}()
	components := make([]FlowCostComponent, 0, 8)
	for index := 0; index < 4; index++ {
		result := <-results
		if result.err != nil {
			return FlowCostEstimate{}, result.err
		}
		components = append(components, result.components...)
	}
	return FlowCostEstimate{
		Direction: opportunity.Direction, Components: components,
	}, nil
}

func estimateObservedSwap(
	ctx context.Context,
	config ObservedFlowCostRefreshConfig,
	marketID market.MarketID,
	side execution.LegSide,
	discovery market.Quote,
) (FlowCostComponent, error) {
	binding, bindingOK := config.Bindings[marketID]
	configured, marketOK := config.Markets[marketID]
	if !bindingOK || !marketOK || binding.Validator == nil {
		return FlowCostComponent{}, fmt.Errorf(
			"complete-flow swap binding %s is unavailable", marketID,
		)
	}
	estimator, ok := binding.TxManager.(artifactNetworkCostEstimator)
	if !ok {
		return FlowCostComponent{}, fmt.Errorf(
			"complete-flow swap manager %s cannot estimate network cost", marketID,
		)
	}
	validationRequest := executionport.ValidationRequest{
		Operation: execution.OperationID("complete-flow-cost-probe"),
		Leg: execution.Leg{
			ID:   execution.StepID("complete-flow-cost/" + string(marketID)),
			Side: side, Chain: market.ChainID(configured.Chain),
			Account: binding.Account, Market: marketID,
			Input: discovery.AmountIn, ExpectedOutput: discovery.AmountOut,
		},
		Discovery: discovery, RequestedAt: config.Clock().UTC(),
	}
	artifact, err := binding.Validator.Validate(ctx, validationRequest)
	if err != nil {
		fallback, ok := binding.Validator.(routeGasEstimator)
		if !ok {
			return FlowCostComponent{}, fmt.Errorf(
				"estimate %s swap cost: %w", marketID, err,
			)
		}
		gas, fallbackErr := fallback.EstimateNetworkGas(
			ctx, discovery.AmountIn, discovery.AmountOut.Token(),
		)
		if fallbackErr != nil {
			return FlowCostComponent{}, fmt.Errorf(
				"estimate %s swap cost: build=%v route_gas=%w",
				marketID, err, fallbackErr,
			)
		}
		artifact = executionport.Artifact{
			Metadata: map[string]string{
				"estimated_gas": strconv.FormatUint(gas, 10),
			},
		}
	}
	nativeUnits, capturedAt, evidence, err := estimateArtifactNetworkCostWithEvidence(
		ctx,
		estimator,
		binding.Validator,
		validationRequest,
		artifact,
	)
	if err != nil {
		return FlowCostComponent{}, fmt.Errorf(
			"value %s swap network cost: %w", marketID, err,
		)
	}
	component, err := valueNativeFlowCost(
		config, market.ChainID(configured.Chain), nativeUnits,
		"swap_"+string(side), evidence, capturedAt,
	)
	if err != nil {
		return FlowCostComponent{}, err
	}
	return component, nil
}

// EstimateArtifactNetworkCostWithCompaction retries a Jupiter cost probe with
// progressively smaller account sets when the assembled transaction is too large.
func EstimateArtifactNetworkCostWithCompaction(
	ctx context.Context,
	estimator artifactNetworkCostEstimator,
	validator executionport.Validator,
	request executionport.ValidationRequest,
	artifact executionport.Artifact,
) (*big.Int, time.Time, error) {
	units, capturedAt, _, err := estimateArtifactNetworkCostWithEvidence(
		ctx,
		estimator,
		validator,
		request,
		artifact,
	)
	return units, capturedAt, err
}

func estimateArtifactNetworkCostWithEvidence(
	ctx context.Context,
	estimator artifactNetworkCostEstimator,
	validator executionport.Validator,
	request executionport.ValidationRequest,
	artifact executionport.Artifact,
) (*big.Int, time.Time, string, error) {
	if estimator == nil {
		return nil, time.Time{}, "", fmt.Errorf(
			"artifact network cost estimator is unavailable",
		)
	}
	for compactRebuilds := 0; ; compactRebuilds++ {
		var (
			units      *big.Int
			capturedAt time.Time
			evidence   = "provider_build+chain_fee_cache"
			err        error
		)
		if detailed, ok := estimator.(detailedArtifactNetworkCostEstimator); ok {
			var estimate solanaadapter.ArtifactNetworkCostEstimate
			estimate, err = detailed.EstimateArtifactNetworkCostDetails(
				ctx,
				artifact,
			)
			if err == nil {
				units = new(big.Int).Set(estimate.TotalLamports)
				capturedAt = estimate.CapturedAt
				evidence = fmt.Sprintf(
					"solana_fee base=%d priority=%d tip=%d cu_limit=%d "+
						"cu_price=%d provider_price=%d capped=%t",
					estimate.BaseFeeLamports,
					estimate.PriorityFeeLamports,
					estimate.SenderTipLamports,
					estimate.ComputeUnitLimit,
					estimate.EffectivePriceMicroLamports,
					estimate.ProviderPriceMicroLamports,
					estimate.PriorityFeeCapped,
				)
			}
		} else {
			units, capturedAt, err =
				estimator.EstimateArtifactNetworkCost(ctx, artifact)
		}
		if err == nil {
			return units, capturedAt, evidence, nil
		}
		var oversized *executionport.ArtifactTooLargeError
		compact, supportsCompact :=
			validator.(executionport.CompactValidator)
		if !errors.As(err, &oversized) ||
			!supportsCompact ||
			compactRebuilds >= 3 {
			return nil, time.Time{}, "", err
		}
		artifact, err = compact.ValidateCompact(ctx, request, artifact)
		if err != nil {
			return nil, time.Time{}, "", fmt.Errorf(
				"compact cost artifact after %d-byte transaction: %w",
				oversized.ActualBytes,
				err,
			)
		}
	}
}

func estimateObservedNTT(
	ctx context.Context,
	config ObservedFlowCostRefreshConfig,
	state *observedFlowCostState,
	direction arbitrage.Direction,
) ([]FlowCostComponent, error) {
	buy := config.Markets[direction.BuyMarket]
	kind := config.Chains[buy.Chain].Kind
	calibrationDirection := "solana-to-evm"
	if kind == "evm" {
		calibrationDirection = "evm-to-solana"
	}
	calibration, err := loadNTTCostCalibration(
		ctx, config, state, calibrationDirection,
	)
	if err != nil {
		return nil, fmt.Errorf("read NTT cost calibration: %w", err)
	}
	var evmChain, solanaChain market.ChainID
	for id, chain := range config.Chains {
		switch chain.Kind {
		case "evm":
			evmChain = market.ChainID(id)
		case "solana":
			solanaChain = market.ChainID(id)
		}
	}
	fees, ok := config.EVMFees.FeeSnapshot()
	if !ok {
		return nil, fmt.Errorf("EVM fee cache unavailable for NTT")
	}
	expectedGasPrice := new(big.Int).Add(fees.BaseFee, fees.TipCap)
	evmUnits := new(big.Int).Mul(
		new(big.Int).SetUint64(calibration.EVMGasUsed), expectedGasPrice,
	)
	evmCost, err := valueNativeFlowCost(
		config, evmChain, evmUnits, "base_bridge_evm",
		"ntt_real_high_water_gas+evm_fee_cache", fees.CapturedAt,
	)
	if err != nil {
		return nil, err
	}
	solanaUnits, ok := new(big.Int).SetString(
		calibration.SolanaDebitLamports, 10,
	)
	if !ok {
		return nil, fmt.Errorf("NTT Solana calibration is invalid")
	}
	solanaCost, err := valueNativeFlowCost(
		config, solanaChain, solanaUnits, "base_bridge_solana",
		"ntt_real_high_water_lamports", config.Clock().UTC(),
	)
	if err != nil {
		return nil, err
	}
	return []FlowCostComponent{evmCost, solanaCost}, nil
}

func loadNTTCostCalibration(
	ctx context.Context,
	config ObservedFlowCostRefreshConfig,
	state *observedFlowCostState,
	direction string,
) (sqlitestore.NTTCostCalibration, error) {
	state.mu.Lock()
	cached, ok := state.calibration[direction]
	state.mu.Unlock()
	if ok {
		return cached, nil
	}
	calibration, err := config.NTTCalibration.LatestCostCalibration(
		ctx, direction, 10,
	)
	if err == nil {
		state.mu.Lock()
		state.calibration[direction] = calibration
		state.mu.Unlock()
		return calibration, nil
	}
	transactions, completedAt, identityErr :=
		config.NTTCalibration.LatestCompletedTransactions(ctx, direction)
	if identityErr != nil {
		return sqlitestore.NTTCostCalibration{}, err
	}
	calibration = sqlitestore.NTTCostCalibration{
		Direction: direction, Samples: 1, LatestCompletedAt: completedAt,
	}
	solanaDebit := new(big.Int)
	for _, transaction := range transactions {
		if transaction.Status != "confirmed" {
			continue
		}
		switch transaction.Chain {
		case "evm":
			gas, gasErr := config.EVMFees.ConfirmedGasUsed(
				ctx, transaction.Identity,
			)
			if gasErr != nil {
				return sqlitestore.NTTCostCalibration{}, gasErr
			}
			calibration.EVMGasUsed += gas
		case "solana":
			debit, debitErr := config.SolanaFees.ConfirmedPayerDebit(
				ctx, transaction.Identity,
			)
			if debitErr != nil {
				return sqlitestore.NTTCostCalibration{}, debitErr
			}
			solanaDebit.Add(solanaDebit, new(big.Int).SetUint64(debit))
		}
	}
	if calibration.EVMGasUsed == 0 && solanaDebit.Sign() == 0 {
		return sqlitestore.NTTCostCalibration{}, fmt.Errorf(
			"completed NTT calibration has no resource usage",
		)
	}
	calibration.SolanaDebitLamports = solanaDebit.String()
	state.mu.Lock()
	state.calibration[direction] = calibration
	state.mu.Unlock()
	return calibration, nil
}

func estimateObservedAcross(
	ctx context.Context,
	config ObservedFlowCostRefreshConfig,
	direction arbitrage.Direction,
	input market.TokenAmount,
) ([]FlowCostComponent, error) {
	sell := config.Markets[direction.SellMarket]
	sourceChain := market.ChainID(sell.Chain)
	approvalResult, err := config.Across.CostApproval(
		ctx, sourceChain, input.Units(),
	)
	if err != nil {
		return nil, fmt.Errorf("estimate Across return cost: %w", err)
	}
	approval := approvalResult.Approval
	expected, ok := new(big.Int).SetString(approval.ExpectedOutputAmount, 10)
	if !ok || expected.Sign() <= 0 || expected.Cmp(input.Units()) > 0 {
		return nil, fmt.Errorf("across expected output is invalid")
	}
	spreadUnits := new(big.Int).Sub(input.Units(), expected)
	spread, err := tokenUnitsQuantity(
		sell.Quote.Token.Asset, spreadUnits, sell.Quote.Token.Decimals,
	)
	if err != nil {
		return nil, err
	}
	components := []FlowCostComponent{{
		Kind: "quote_bridge_spread", Amount: spread,
		Evidence:   "across_approval_expected_output",
		CapturedAt: approval.ObservedAt,
	}}
	transaction := approval.SwapTransaction
	switch config.Chains[sell.Chain].Kind {
	case "evm":
		gas, err := strconv.ParseUint(strings.TrimSpace(transaction.Gas), 10, 64)
		if err != nil || gas == 0 {
			identity, _, calibrationErr :=
				config.AcrossCalibration.LatestCompletedSource(
					ctx, "evm-to-solana",
				)
			if calibrationErr != nil {
				return nil, fmt.Errorf(
					"across EVM approval has no gas estimate and calibration is unavailable: %w",
					calibrationErr,
				)
			}
			gas, err = config.EVMFees.ConfirmedGasUsed(ctx, identity)
			if err != nil {
				return nil, fmt.Errorf("read Across EVM gas calibration: %w", err)
			}
		}
		fees, ok := config.EVMFees.FeeSnapshot()
		if !ok {
			return nil, fmt.Errorf("EVM fee cache unavailable for across")
		}
		expectedGasPrice := new(big.Int).Add(fees.BaseFee, fees.TipCap)
		native := new(big.Int).Mul(new(big.Int).SetUint64(gas), expectedGasPrice)
		component, err := valueNativeFlowCost(
			config, sourceChain, native, "quote_bridge_source",
			"across_approval_gas+evm_fee_cache", fees.CapturedAt,
		)
		if err != nil {
			return nil, err
		}
		components = append(components, component)
	case "solana":
		encoded := transaction.Serialized
		if encoded == "" {
			encoded = transaction.Data
		}
		raw, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("decode Across Solana cost artifact: %w", err)
		}
		parsed, err := solanago.TransactionFromBytes(raw)
		if err != nil {
			return nil, fmt.Errorf("parse Across Solana cost artifact: %w", err)
		}
		fee, err := config.SolanaFees.FeeForMessage(
			ctx, parsed.Message.ToBase64(),
		)
		if err != nil {
			return nil, fmt.Errorf("estimate Across Solana message fee: %w", err)
		}
		component, err := valueNativeFlowCost(
			config, sourceChain, new(big.Int).SetUint64(fee),
			"quote_bridge_source", "across_message_fee", config.Clock().UTC(),
		)
		if err != nil {
			return nil, err
		}
		components = append(components, component)
	default:
		return nil, fmt.Errorf("across source chain kind is unsupported")
	}
	return components, nil
}

func valueNativeFlowCost(
	config ObservedFlowCostRefreshConfig,
	chain market.ChainID,
	units *big.Int,
	kind, evidence string,
	capturedAt time.Time,
) (FlowCostComponent, error) {
	asset := config.NativeAssets[chain]
	decimals := config.NativeDecimals[chain]
	amount, err := tokenUnitsQuantity(asset, units, decimals)
	if err != nil {
		return FlowCostComponent{}, err
	}
	valued, err := config.Valuator.Value(execution.CostComponent{
		Kind: kind, Chain: chain, Amount: amount, Evidence: evidence,
	})
	if err != nil {
		return FlowCostComponent{}, err
	}
	price, ok := config.Valuator.Price(asset)
	if !ok {
		return FlowCostComponent{}, fmt.Errorf(
			"native price cache for %s is unavailable", asset,
		)
	}
	if price.CapturedAt.Before(capturedAt) {
		capturedAt = price.CapturedAt
	}
	return FlowCostComponent{
		Kind: kind, Amount: valued.QuoteValue,
		Evidence: valued.Evidence, CapturedAt: capturedAt.UTC(),
	}, nil
}

func tokenUnitsQuantity(
	asset market.AssetID,
	units *big.Int,
	decimals uint8,
) (market.AssetQuantity, error) {
	if asset == "" || units == nil || units.Sign() < 0 {
		return market.AssetQuantity{}, fmt.Errorf("token-unit cost is invalid")
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	return market.NewAssetQuantity(asset, new(big.Rat).SetFrac(units, scale))
}

var _ artifactNetworkCostEstimator = (*evmadapter.TxManager)(nil)
var _ artifactNetworkCostEstimator = (*solanaadapter.TxManager)(nil)
