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
	ID                  market.SourceID
	ChainSlug           string
	Sender              common.Address
	TokenAddresses      map[market.TokenID]string
	SlippageBPS         uint16
	EnableGasEstimation bool
	GasMultiplierBPS    uint64
	Source              RouteBuilder
	Simulator           Simulator
	Clock               func() time.Time
}

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
	if config.GasMultiplierBPS == 0 {
		config.GasMultiplierBPS = 12_000
	}
	if config.GasMultiplierBPS < 10_000 {
		return nil, fmt.Errorf("KyberSwap gas multiplier cannot reduce simulated gas")
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
			SlippageBPS:         v.config.SlippageBPS,
			EnableGasEstimation: v.config.EnableGasEstimation,
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
			if _, err = v.config.Simulator.CallContract(ctx, call, nil); err == nil {
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
	gasLimit := estimatedGas * v.config.GasMultiplierBPS / 10_000
	if gasLimit < estimatedGas || gasLimit == 0 {
		return executionport.Artifact{}, fmt.Errorf("KyberSwap gas limit overflow")
	}
	outputUnits, ok := new(big.Int).SetString(built.AmountOut, 10)
	if !ok || outputUnits.Sign() <= 0 {
		return executionport.Artifact{}, fmt.Errorf("KyberSwap build output is invalid")
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
	return executionport.Artifact{
		Leg: request.Leg, ValidatedQuote: validated,
		Payload: append([]byte(nil), calldata...),
		Metadata: map[string]string{
			"kind": "kyberswap_route_build",
			"to":   router.Hex(), "value": value.String(),
			"gas_limit":      new(big.Int).SetUint64(gasLimit).String(),
			"estimated_gas":  new(big.Int).SetUint64(estimatedGas).String(),
			"build_attempts": strconv.Itoa(buildAttempts),
		},
		BuiltAt: now,
	}, nil
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

// EstimateNetworkGas uses KyberSwap's route-level gas estimate without
// constructing or simulating calldata. The complete-flow cost oracle uses it
// only when a notional probe cannot pass transferFrom because the calibration
// wallet is temporarily below the configured trading inventory.
func (v *Validator) EstimateNetworkGas(
	ctx context.Context,
	leg market.TokenAmount,
	outputToken market.TokenID,
) (uint64, error) {
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
	gas, err := strconv.ParseUint(strings.TrimSpace(route.Gas), 10, 64)
	if err != nil || gas == 0 {
		return 0, fmt.Errorf("KyberSwap route has no gas estimate")
	}
	return gas, nil
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
