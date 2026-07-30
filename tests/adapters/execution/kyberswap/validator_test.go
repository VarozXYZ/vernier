package kyberswap_test

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	geth "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"

	kyberswapadapter "github.com/VarozXYZ/vernier/adapters/execution/kyberswap"
	quoteadapter "github.com/VarozXYZ/vernier/adapters/quote/kyberswap"
	domainexecution "github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

const (
	validatorTokenIn  = "0x1000000000000000000000000000000000000001"
	validatorTokenOut = "0x2000000000000000000000000000000000000002"
	validatorSender   = "0x3000000000000000000000000000000000000003"
	validatorRouter   = "0x4000000000000000000000000000000000000004"
)

type routeBuilderStub struct {
	routes    int
	builds    int
	buildErrs []error
	lastBuild quoteadapter.BuildRequest
}

func (s *routeBuilderStub) Route(
	_ context.Context,
	request quoteadapter.RouteRequest,
) (quoteadapter.RouteResult, error) {
	s.routes++
	return quoteadapter.RouteResult{
		Request:       request,
		RouterAddress: validatorRouter,
		TokenIn:       request.TokenIn,
		TokenOut:      request.TokenOut,
		AmountIn:      request.AmountIn,
		AmountOut:     "2500000",
	}, nil
}

func (s *routeBuilderStub) Build(
	_ context.Context,
	request quoteadapter.BuildRequest,
) (quoteadapter.BuildResult, error) {
	s.builds++
	s.lastBuild = request
	if len(s.buildErrs) >= s.builds && s.buildErrs[s.builds-1] != nil {
		return quoteadapter.BuildResult{}, s.buildErrs[s.builds-1]
	}
	return quoteadapter.BuildResult{
		AmountIn:         "1000000",
		AmountOut:        "2490000",
		RouterAddress:    validatorRouter,
		Data:             "0xabcdef01",
		TransactionValue: "0",
	}, nil
}

type simulatorStub struct {
	callErrors    []error
	calls         int
	estimateGas   uint64
	estimateCalls int
}

func (s *simulatorStub) CallContract(
	context.Context,
	geth.CallMsg,
	*big.Int,
) ([]byte, error) {
	s.calls++
	if len(s.callErrors) >= s.calls && s.callErrors[s.calls-1] != nil {
		return nil, s.callErrors[s.calls-1]
	}
	return nil, nil
}

func (s *simulatorStub) EstimateGas(
	context.Context,
	geth.CallMsg,
) (uint64, error) {
	s.estimateCalls++
	if s.estimateGas == 0 {
		return 250_000, nil
	}
	return s.estimateGas, nil
}

func TestValidatorRefreshesRouteOnceAfterStaleBuild(t *testing.T) {
	source := &routeBuilderStub{
		buildErrs: []error{
			&quoteadapter.APIError{
				Operation: "build", HTTPStatus: 422,
				Code: "4227", Message: "estimate gas failed: return amount is not enough",
			},
			nil,
		},
	}
	validator := newValidatorForTest(t, source)

	artifact, err := validator.Validate(context.Background(), validationRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	if source.routes != 2 || source.builds != 2 {
		t.Fatalf("route/build calls = %d/%d; want 2/2", source.routes, source.builds)
	}
	if artifact.Metadata["build_attempts"] != "2" {
		t.Fatalf("build_attempts = %q", artifact.Metadata["build_attempts"])
	}
}

func TestValidatorDoesNotRetryPermanentBuildError(t *testing.T) {
	source := &routeBuilderStub{
		buildErrs: []error{
			&quoteadapter.APIError{
				Operation: "build", HTTPStatus: 400,
				Code: "4001", Message: "invalid recipient",
			},
		},
	}
	validator := newValidatorForTest(t, source)

	_, err := validator.Validate(context.Background(), validationRequest(t))
	if err == nil {
		t.Fatal("Validate() error = nil")
	}
	if source.routes != 1 || source.builds != 1 {
		t.Fatalf("route/build calls = %d/%d; want 1/1", source.routes, source.builds)
	}
	var apiErr *quoteadapter.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "4001" {
		t.Fatalf("Validate() error = %T %v", err, err)
	}
}

func TestValidatorRefreshesRouteAfterLocalMinReturnRevert(t *testing.T) {
	source := &routeBuilderStub{}
	simulator := &simulatorStub{
		callErrors: []error{
			errors.New("execution reverted: return amount is not enough"),
			nil,
		},
	}
	validator := newValidatorWithSimulatorForTest(t, source, simulator)

	artifact, err := validator.Validate(context.Background(), validationRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	if source.routes != 2 || source.builds != 2 || simulator.calls != 2 {
		t.Fatalf(
			"route/build/simulate calls = %d/%d/%d; want 2/2/2",
			source.routes,
			source.builds,
			simulator.calls,
		)
	}
	if artifact.Metadata["build_attempts"] != "2" {
		t.Fatalf("build_attempts = %q", artifact.Metadata["build_attempts"])
	}
}

func TestValidatorSeparatesTransactionGasLimitFromExpectedGasUsage(t *testing.T) {
	source := &routeBuilderStub{}
	simulator := &simulatorStub{}
	validator := newValidatorWithGasPolicyForTest(
		t,
		source,
		simulator,
		"fixed",
		1_500_000,
		"fixed",
		1_000_000,
	)

	artifact, err := validator.Validate(context.Background(), validationRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	if got := artifact.Metadata["gas_limit"]; got != "1500000" {
		t.Fatalf("gas_limit = %q; want 1500000", got)
	}
	if got := artifact.Metadata["expected_gas_used"]; got != "1000000" {
		t.Fatalf("expected_gas_used = %q; want 1000000", got)
	}
	if source.lastBuild.EnableGasEstimation {
		t.Fatal("KyberSwap build requested provider gas estimation")
	}
	if simulator.estimateCalls != 0 {
		t.Fatalf(
			"fixed policy made %d eth_estimateGas calls",
			simulator.estimateCalls,
		)
	}
}

func TestValidatorEstimatesExecutionAndCostGasByDefault(t *testing.T) {
	source := &routeBuilderStub{}
	simulator := &simulatorStub{estimateGas: 250_000}
	validator := newValidatorWithSimulatorForTest(t, source, simulator)

	artifact, err := validator.Validate(
		context.Background(),
		validationRequest(t),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := artifact.Metadata["gas_limit"]; got != "300000" {
		t.Fatalf("gas_limit = %q; want 300000", got)
	}
	if got := artifact.Metadata["expected_gas_used"]; got != "250000" {
		t.Fatalf("expected_gas_used = %q; want 250000", got)
	}
	if simulator.estimateCalls != 1 {
		t.Fatalf(
			"estimated policy made %d eth_estimateGas calls; want 1",
			simulator.estimateCalls,
		)
	}
}

func TestValidatorAcceptsExplicitZeroDynamicSlippage(t *testing.T) {
	source := &routeBuilderStub{}
	validator := newValidatorForTest(t, source)
	request := validationRequest(t)
	minimum, _ := market.NewTokenAmount(
		"quote",
		big.NewInt(2_490_000),
	)
	request.Slippage = &executionport.SlippageConstraint{
		BPS:           0,
		MinimumOutput: minimum,
		Reason:        "dynamic_buy_budget",
	}
	artifact, err := validator.Validate(
		context.Background(),
		request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if source.lastBuild.SlippageBPS != 0 ||
		artifact.Metadata["slippage_bps"] != "0" ||
		artifact.Metadata["required_minimum_output_units"] != "2490000" {
		t.Fatalf(
			"dynamic slippage was not preserved: request=%+v metadata=%+v",
			source.lastBuild,
			artifact.Metadata,
		)
	}
}

func TestValidatorRejectsBuildBelowDynamicMinimumWithoutRetry(t *testing.T) {
	source := &routeBuilderStub{}
	validator := newValidatorForTest(t, source)
	request := validationRequest(t)
	minimum, _ := market.NewTokenAmount(
		"quote",
		big.NewInt(2_490_001),
	)
	request.Slippage = &executionport.SlippageConstraint{
		BPS:           0,
		MinimumOutput: minimum,
		Reason:        "dynamic_buy_budget",
	}
	_, err := validator.Validate(context.Background(), request)
	var thresholdErr *executionport.SlippageThresholdError
	if !errors.As(err, &thresholdErr) {
		t.Fatalf("error = %v; want slippage threshold error", err)
	}
	if source.builds != 1 {
		t.Fatalf("threshold rejection made %d builds; want 1", source.builds)
	}
}

func newValidatorForTest(
	t *testing.T,
	source kyberswapadapter.RouteBuilder,
) *kyberswapadapter.Validator {
	return newValidatorWithSimulatorForTest(t, source, &simulatorStub{})
}

func newValidatorWithSimulatorForTest(
	t *testing.T,
	source kyberswapadapter.RouteBuilder,
	simulator kyberswapadapter.Simulator,
) *kyberswapadapter.Validator {
	t.Helper()
	validator, err := kyberswapadapter.New(kyberswapadapter.Config{
		ID:        "kyberswap/live",
		ChainSlug: "polygon",
		Sender:    common.HexToAddress(validatorSender),
		TokenAddresses: map[market.TokenID]string{
			"base":  validatorTokenIn,
			"quote": validatorTokenOut,
		},
		SlippageBPS: 10,
		Source:      source,
		Simulator:   simulator,
		Clock:       time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return validator
}

func newValidatorWithGasPolicyForTest(
	t *testing.T,
	source kyberswapadapter.RouteBuilder,
	simulator kyberswapadapter.Simulator,
	executionMode string,
	executionLimit uint64,
	costMode string,
	costLimit uint64,
) *kyberswapadapter.Validator {
	t.Helper()
	validator, err := kyberswapadapter.New(kyberswapadapter.Config{
		ID:        "kyberswap/live",
		ChainSlug: "polygon",
		Sender:    common.HexToAddress(validatorSender),
		TokenAddresses: map[market.TokenID]string{
			"base":  validatorTokenIn,
			"quote": validatorTokenOut,
		},
		SlippageBPS:            10,
		GasExecutionMode:       executionMode,
		FixedExecutionGasLimit: executionLimit,
		GasCostMode:            costMode,
		FixedCostGasLimit:      costLimit,
		Source:                 source,
		Simulator:              simulator,
		Clock:                  time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return validator
}

func validationRequest(t *testing.T) executionport.ValidationRequest {
	t.Helper()
	input, err := market.NewTokenAmount("base", big.NewInt(1_000_000))
	if err != nil {
		t.Fatal(err)
	}
	output, err := market.NewTokenAmount("quote", big.NewInt(2_500_000))
	if err != nil {
		t.Fatal(err)
	}
	discovery, err := market.NewQuote(market.Quote{
		Source: "kyberswap", Market: "market",
		SnapshotVersion: 1,
		Purpose:         market.QuotePurposeLiveDiscovery,
		Mode:            market.QuoteModeExactInput,
		Quality:         market.QuoteQualityExact,
		AmountIn:        input,
		AmountOut:       output,
		QuotedAt:        time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return executionport.ValidationRequest{
		Operation: "operation",
		Leg: domainexecution.Leg{
			ID: "sell", Side: domainexecution.LegSell,
			Chain: "polygon", Account: "account", Market: "market",
			Input: input, ExpectedOutput: output,
		},
		Discovery: discovery,
	}
}
