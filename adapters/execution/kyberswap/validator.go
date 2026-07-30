// Package kyberswap converts a fresh KyberSwap route/build response into the
// generic in-memory Live artifact consumed by an EVM TxManager.
package kyberswap

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"

	quoteadapter "github.com/VarozXYZ/vernier/adapters/quote/kyberswap"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

type Simulator interface {
	CallContract(context.Context, geth.CallMsg, *big.Int) ([]byte, error)
	EstimateGas(context.Context, geth.CallMsg) (uint64, error)
}

type RouteBuilder interface {
	Route(context.Context, quoteadapter.RouteRequest) (quoteadapter.RouteResult, error)
	Build(context.Context, quoteadapter.BuildRequest) (quoteadapter.BuildResult, error)
}

type Config struct {
	ID                         market.SourceID
	ChainSlug                  string
	Sender                     common.Address
	TokenAddresses             map[market.TokenID]string
	SlippageBPS                uint16
	GasExecutionMode           string
	FixedExecutionGasLimit     uint64
	GasEstimationMultiplierBPS uint64
	GasCostMode                string
	FixedCostGasLimit          uint64
	Source                     RouteBuilder
	Simulator                  Simulator
	Clock                      func() time.Time
}

const (
	DefaultSwapGasLimit        uint64 = 1_500_000
	DefaultSwapExpectedGasUsed uint64 = 1_000_000
	defaultGasMultiplierBPS    uint64 = 12_000
)

type Validator struct {
	config Config
}

func New(config Config) (*Validator, error) {
	if config.ID == "" || strings.TrimSpace(config.ChainSlug) == "" ||
		config.Sender == (common.Address{}) || len(config.TokenAddresses) < 2 ||
		config.Source == nil || config.Simulator == nil {
		return nil, fmt.Errorf("KyberSwap validator configuration is incomplete")
	}
	for token, address := range config.TokenAddresses {
		if token == "" || !common.IsHexAddress(address) ||
			common.HexToAddress(address) == (common.Address{}) {
			return nil, fmt.Errorf("KyberSwap validator token mapping is invalid")
		}
	}
	if config.SlippageBPS == 0 {
		config.SlippageBPS = 10
	}
	if config.SlippageBPS > 2_000 {
		return nil, fmt.Errorf("KyberSwap validator slippage is invalid")
	}
	if config.GasExecutionMode == "" {
		config.GasExecutionMode = "estimate"
	}
	if config.GasEstimationMultiplierBPS == 0 {
		config.GasEstimationMultiplierBPS = defaultGasMultiplierBPS
	}
	if config.GasExecutionMode != "estimate" &&
		config.GasExecutionMode != "fixed" {
		return nil, fmt.Errorf("KyberSwap gas execution mode is invalid")
	}
	if config.GasEstimationMultiplierBPS < 10_000 {
		return nil, fmt.Errorf(
			"KyberSwap gas estimation multiplier cannot reduce gas",
		)
	}
	if config.GasExecutionMode == "fixed" &&
		config.FixedExecutionGasLimit == 0 {
		return nil, fmt.Errorf(
			"KyberSwap fixed execution gas limit is required",
		)
	}
	if config.GasCostMode == "" {
		config.GasCostMode = "estimated"
	}
	switch config.GasCostMode {
	case "estimated":
		if config.GasExecutionMode != "estimate" {
			return nil, fmt.Errorf(
				"KyberSwap estimated cost gas requires estimated execution gas",
			)
		}
	case "transaction_limit":
	case "fixed":
		if config.FixedCostGasLimit == 0 {
			return nil, fmt.Errorf(
				"KyberSwap fixed cost gas limit is required",
			)
		}
	default:
		return nil, fmt.Errorf("KyberSwap gas cost mode is invalid")
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	return &Validator{config: config}, nil
}

func (v *Validator) Validate(
	ctx context.Context,
	request executionport.ValidationRequest,
) (executionport.Artifact, error) {
	if err := request.Leg.Validate(); err != nil {
		return executionport.Artifact{}, err
	}
	if request.Discovery.Mode != market.QuoteModeExactInput ||
		request.Discovery.AmountIn.Token() != request.Leg.Input.Token() ||
		request.Discovery.AmountIn.Units().Cmp(request.Leg.Input.Units()) != 0 {
		return executionport.Artifact{}, fmt.Errorf("KyberSwap build requires matching ExactIn discovery input")
	}
	inputAddress := v.config.TokenAddresses[request.Leg.Input.Token()]
	outputAddress := v.config.TokenAddresses[request.Leg.ExpectedOutput.Token()]
	if inputAddress == "" || outputAddress == "" {
		return executionport.Artifact{}, fmt.Errorf("KyberSwap build token mapping is missing")
	}
	var (
		built        quoteadapter.BuildResult
		router       common.Address
		calldata     []byte
		value        *big.Int
		estimatedGas uint64
	)
	slippageBPS := v.config.SlippageBPS
	if request.Slippage != nil {
		slippageBPS = request.Slippage.BPS
	}
	buildAttempts := 0
	for {
		buildAttempts++
		route, err := v.config.Source.Route(ctx, quoteadapter.RouteRequest{
			Chain: v.config.ChainSlug, TokenIn: inputAddress,
			TokenOut: outputAddress, AmountIn: request.Leg.Input.String(),
			Origin: v.config.Sender.Hex(),
		})
		if err != nil {
			return executionport.Artifact{}, fmt.Errorf("KyberSwap route: %w", err)
		}
		built, err = v.config.Source.Build(ctx, quoteadapter.BuildRequest{
			Route: route, Sender: v.config.Sender.Hex(),
			Recipient: v.config.Sender.Hex(), Origin: v.config.Sender.Hex(),
			SlippageBPS:         slippageBPS,
			EnableGasEstimation: false,
		})
		if err == nil {
			router = common.HexToAddress(built.RouterAddress)
			calldata, err = hex.DecodeString(strings.TrimPrefix(built.Data, "0x"))
			if err != nil || len(calldata) < 4 {
				return executionport.Artifact{}, fmt.Errorf("decode KyberSwap calldata")
			}
			var ok bool
			value, ok = new(big.Int).SetString(built.TransactionValue, 10)
			if !ok || value.Sign() < 0 {
				return executionport.Artifact{}, fmt.Errorf("KyberSwap transaction value is invalid")
			}
			call := geth.CallMsg{
				From: v.config.Sender, To: &router, Value: value, Data: calldata,
			}
			validationPhase := "simulate"
			if _, err = v.config.Simulator.CallContract(ctx, call, nil); err == nil && v.config.GasExecutionMode == "estimate" {
				validationPhase = "estimate gas for"
				estimatedGas, err = v.config.Simulator.EstimateGas(ctx, call)
			}
			if err == nil {
				break
			}
			if buildAttempts == 1 && staleRouteBuildError(err) {
				continue
			}
			if buildAttempts > 1 {
				err = classifyAllowanceFailure(err, router)
				return executionport.Artifact{}, fmt.Errorf(
					"%s KyberSwap transaction after one fresh-route retry: %w",
					validationPhase,
					err,
				)
			}
			err = classifyAllowanceFailure(err, router)
			return executionport.Artifact{}, fmt.Errorf(
				"%s KyberSwap transaction: %w",
				validationPhase,
				err,
			)
		}
		if buildAttempts == 1 && staleRouteBuildError(err) {
			continue
		}
		if buildAttempts > 1 {
			return executionport.Artifact{}, fmt.Errorf(
				"KyberSwap build after one fresh-route retry: %w",
				err,
			)
		}
		return executionport.Artifact{}, fmt.Errorf("KyberSwap build: %w", err)
	}
	gasLimit, expectedGas, err := v.resolveGas(estimatedGas)
	if err != nil {
		return executionport.Artifact{}, err
	}
	outputUnits, ok := new(big.Int).SetString(built.AmountOut, 10)
	if !ok || outputUnits.Sign() <= 0 {
		return executionport.Artifact{}, fmt.Errorf("KyberSwap build output is invalid")
	}
	thresholdUnits := slippageFloor(outputUnits, slippageBPS)
	if request.Slippage != nil {
		minimum := request.Slippage.MinimumOutput
		if minimum.Token() != request.Leg.ExpectedOutput.Token() ||
			thresholdUnits.Cmp(minimum.Units()) < 0 {
			actual, amountErr := market.NewTokenAmount(
				request.Leg.ExpectedOutput.Token(),
				thresholdUnits,
			)
			if amountErr != nil {
				return executionport.Artifact{}, amountErr
			}
			return executionport.Artifact{},
				&executionport.SlippageThresholdError{
					Provider: "kyberswap",
					Actual:   actual,
					Required: minimum,
				}
		}
	}
	output, err := market.NewTokenAmount(request.Leg.ExpectedOutput.Token(), outputUnits)
	if err != nil {
		return executionport.Artifact{}, err
	}
	now := v.config.Clock().UTC()
	validated, err := market.NewQuote(market.Quote{
		Source: v.config.ID, Market: request.Leg.Market,
		SnapshotVersion: request.Discovery.SnapshotVersion,
		SnapshotHash:    request.Discovery.SnapshotHash,
		SourcePosition:  request.Discovery.SourcePosition,
		Purpose:         market.QuotePurposeLiveValidation,
		Mode:            market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
		AmountIn: request.Leg.Input, AmountOut: output, QuotedAt: now,
	})
	if err != nil {
		return executionport.Artifact{}, err
	}
	metadata := map[string]string{
		"kind": "kyberswap_route_build",
		"to":   router.Hex(), "value": value.String(),
		"gas_limit":            strconv.FormatUint(gasLimit, 10),
		"expected_gas_used":    strconv.FormatUint(expectedGas, 10),
		"build_attempts":       strconv.Itoa(buildAttempts),
		"slippage_bps":         strconv.FormatUint(uint64(slippageBPS), 10),
		"minimum_output_units": thresholdUnits.String(),
	}
	if request.Slippage != nil {
		metadata["slippage_reason"] = request.Slippage.Reason
		metadata["required_minimum_output_units"] =
			request.Slippage.MinimumOutput.String()
		for key, value := range request.Slippage.Evidence {
			metadata["slippage_"+key] = value
		}
	}
	return executionport.Artifact{
		Leg: request.Leg, ValidatedQuote: validated,
		Payload:  append([]byte(nil), calldata...),
		Metadata: metadata,
		BuiltAt:  now,
	}, nil
}

func slippageFloor(amount *big.Int, bps uint16) *big.Int {
	if amount == nil || amount.Sign() <= 0 {
		return new(big.Int)
	}
	numerator := new(big.Int).Mul(
		new(big.Int).Set(amount),
		big.NewInt(int64(10_000-uint64(bps))),
	)
	return numerator.Quo(numerator, big.NewInt(10_000))
}

func (v *Validator) resolveGas(estimated uint64) (uint64, uint64, error) {
	var gasLimit uint64
	switch v.config.GasExecutionMode {
	case "fixed":
		gasLimit = v.config.FixedExecutionGasLimit
	case "estimate":
		if estimated == 0 {
			return 0, 0, fmt.Errorf(
				"KyberSwap gas estimation returned zero",
			)
		}
		var err error
		gasLimit, err = scaleGasLimit(
			estimated,
			v.config.GasEstimationMultiplierBPS,
		)
		if err != nil {
			return 0, 0, err
		}
	default:
		return 0, 0, fmt.Errorf("KyberSwap gas execution mode is invalid")
	}
	var expected uint64
	switch v.config.GasCostMode {
	case "estimated":
		expected = estimated
	case "transaction_limit":
		expected = gasLimit
	case "fixed":
		expected = v.config.FixedCostGasLimit
	default:
		return 0, 0, fmt.Errorf("KyberSwap gas cost mode is invalid")
	}
	if expected == 0 {
		return 0, 0, fmt.Errorf("KyberSwap expected gas usage is zero")
	}
	return gasLimit, expected, nil
}

// EstimateNetworkGas supplies cost probes with policy-consistent gas evidence.
// Fixed policies perform no provider or RPC request.
func (v *Validator) EstimateNetworkGas(
	ctx context.Context,
	leg market.TokenAmount,
	outputToken market.TokenID,
) (uint64, error) {
	switch v.config.GasCostMode {
	case "fixed":
		return v.config.FixedCostGasLimit, nil
	case "transaction_limit":
		if v.config.GasExecutionMode == "fixed" {
			return v.config.FixedExecutionGasLimit, nil
		}
	}
	inputAddress := v.config.TokenAddresses[leg.Token()]
	outputAddress := v.config.TokenAddresses[outputToken]
	if inputAddress == "" || outputAddress == "" || leg.IsZero() {
		return 0, fmt.Errorf("KyberSwap route gas probe tokens are invalid")
	}
	route, err := v.config.Source.Route(ctx, quoteadapter.RouteRequest{
		Chain: v.config.ChainSlug, TokenIn: inputAddress,
		TokenOut: outputAddress, AmountIn: leg.String(),
		Origin: v.config.Sender.Hex(),
	})
	if err != nil {
		return 0, err
	}
	estimated, err := strconv.ParseUint(strings.TrimSpace(route.Gas), 10, 64)
	if err != nil || estimated == 0 {
		return 0, fmt.Errorf("KyberSwap route has no gas estimate")
	}
	if v.config.GasCostMode == "transaction_limit" {
		return scaleGasLimit(
			estimated,
			v.config.GasEstimationMultiplierBPS,
		)
	}
	return estimated, nil
}

func scaleGasLimit(estimated, multiplierBPS uint64) (uint64, error) {
	if estimated == 0 || multiplierBPS < 10_000 ||
		estimated > ^uint64(0)/multiplierBPS {
		return 0, fmt.Errorf("KyberSwap gas limit overflow")
	}
	limit := estimated * multiplierBPS / 10_000
	if limit < estimated || limit == 0 {
		return 0, fmt.Errorf("KyberSwap gas limit overflow")
	}
	return limit, nil
}

func classifyAllowanceFailure(err error, router common.Address) error {
	if err == nil || router == (common.Address{}) ||
		!strings.Contains(
			strings.ToUpper(err.Error()),
			"TRANSFER_FROM_FAILED",
		) {
		return err
	}
	return &executionport.AllowanceRequiredError{
		Spender: router.Hex(),
		Err:     err,
	}
}

func staleRouteBuildError(err error) bool {
	var apiErr *quoteadapter.APIError
	if errors.As(err, &apiErr) && apiErr.Code == "4227" {
		return true
	}
	return strings.Contains(
		strings.ToLower(err.Error()),
		"return amount is not enough",
	)
}

var _ executionport.Validator = (*Validator)(nil)
