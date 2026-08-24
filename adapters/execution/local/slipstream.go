package local

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

const slipstreamRouterABI = `[
 {"inputs":[{"components":[{"name":"tokenIn","type":"address"},{"name":"tokenOut","type":"address"},{"name":"tickSpacing","type":"int24"},{"name":"recipient","type":"address"},{"name":"deadline","type":"uint256"},{"name":"amountIn","type":"uint256"},{"name":"amountOutMinimum","type":"uint256"},{"name":"sqrtPriceLimitX96","type":"uint160"}],"name":"params","type":"tuple"}],"name":"exactInputSingle","outputs":[{"name":"amountOut","type":"uint256"}],"stateMutability":"payable","type":"function"}
]`

type SlipstreamConfig struct {
	Router         common.Address
	Recipient      common.Address
	TokenAddresses map[market.TokenID]common.Address
	Markets        map[market.MarketID]int32
	SlippageBPS    uint16
	Deadline       time.Duration
	GasLimit       uint64
	Clock          func() time.Time
}

// SlipstreamBuilder turns a single Aerodrome Slipstream pool allocation into
// the initial-deployment SwapRouter exactInputSingle calldata.
type SlipstreamBuilder struct {
	config SlipstreamConfig
	router abi.ABI
}

func NewSlipstreamBuilder(config SlipstreamConfig) (*SlipstreamBuilder, error) {
	if config.Router == (common.Address{}) || config.Recipient == (common.Address{}) ||
		len(config.TokenAddresses) == 0 || len(config.Markets) == 0 ||
		config.SlippageBPS == 0 || config.SlippageBPS > 10_000 ||
		config.Deadline <= 0 || config.GasLimit == 0 {
		return nil, fmt.Errorf(
			"slipstream builder configuration is incomplete: router=%t recipient=%t tokens=%d markets=%d slippage_bps=%d deadline=%s gas_limit=%d",
			config.Router != (common.Address{}), config.Recipient != (common.Address{}), len(config.TokenAddresses),
			len(config.Markets), config.SlippageBPS, config.Deadline, config.GasLimit,
		)
	}
	for _, spacing := range config.Markets {
		if spacing <= 0 || spacing > 8_388_607 {
			return nil, fmt.Errorf("slipstream tick spacing is invalid")
		}
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	parsed, err := abi.JSON(strings.NewReader(slipstreamRouterABI))
	if err != nil {
		return nil, err
	}
	return &SlipstreamBuilder{config: config, router: parsed}, nil
}

func (b *SlipstreamBuilder) BuildIntent(_ context.Context, operation execution.OperationID,
	leg execution.Leg, quote market.Quote, allocation execution.RouteAllocation) (Intent, error) {
	return b.buildIntent(operation, leg, quote, allocation, nil)
}

func (b *SlipstreamBuilder) BuildIntentWithSlippage(_ context.Context, operation execution.OperationID,
	leg execution.Leg, quote market.Quote, allocation execution.RouteAllocation,
	constraint *executionport.SlippageConstraint) (Intent, error) {
	return b.buildIntent(operation, leg, quote, allocation, constraint)
}

func (b *SlipstreamBuilder) buildIntent(operation execution.OperationID,
	leg execution.Leg, quote market.Quote, allocation execution.RouteAllocation,
	constraint *executionport.SlippageConstraint) (Intent, error) {
	if operation == "" || leg.Market == "" || quote.Market != leg.Market {
		return Intent{}, fmt.Errorf("slipstream intent identity is incomplete")
	}
	if err := allocation.Validate(); err != nil {
		return Intent{}, err
	}
	spacing, ok := b.config.Markets[leg.Market]
	tokenIn, inOK := b.config.TokenAddresses[leg.Input.Token()]
	tokenOut, outOK := b.config.TokenAddresses[leg.ExpectedOutput.Token()]
	if !ok || !inOK || !outOK || tokenIn == (common.Address{}) || tokenOut == (common.Address{}) || tokenIn == tokenOut {
		return Intent{}, fmt.Errorf("slipstream market or token is not allowlisted")
	}
	minimum := new(big.Int)
	if constraint != nil {
		if constraint.MinimumOutput.Token() != quote.AmountOut.Token() ||
			constraint.MinimumOutput.IsZero() ||
			constraint.MinimumOutput.Units().Cmp(quote.AmountOut.Units()) > 0 {
			return Intent{}, fmt.Errorf("slipstream slippage floor is incompatible with quote")
		}
		minimum.Set(constraint.MinimumOutput.Units())
	} else {
		minimum.Mul(quote.AmountOut.Units(), big.NewInt(int64(10_000-b.config.SlippageBPS)))
		minimum.Quo(minimum, big.NewInt(10_000))
	}
	if minimum.Sign() <= 0 {
		return Intent{}, fmt.Errorf("slipstream minimum output rounds to zero")
	}
	deadline := big.NewInt(b.config.Clock().Add(b.config.Deadline).Unix())
	params := struct {
		TokenIn, TokenOut                    common.Address
		TickSpacing                          *big.Int
		Recipient                            common.Address
		Deadline, AmountIn, AmountOutMinimum *big.Int
		SqrtPriceLimitX96                    *big.Int
	}{tokenIn, tokenOut, big.NewInt(int64(spacing)), b.config.Recipient, deadline,
		leg.Input.Units(), minimum, new(big.Int)}
	payload, err := b.router.Pack("exactInputSingle", params)
	if err != nil {
		return Intent{}, fmt.Errorf("encode Slipstream exactInputSingle: %w", err)
	}
	metadata := map[string]string{
		"kind": "aerodrome_slipstream_exact_input_single", "to": b.config.Router.Hex(),
		"value": "0", "gas_limit": strconv.FormatUint(b.config.GasLimit, 10),
		"minimum_output_units": minimum.String(), "simulation": "skipped_local_quote_gate",
		"deadline": deadline.String(), "tick_spacing": strconv.FormatInt(int64(spacing), 10),
	}
	if constraint != nil {
		for key, value := range constraint.Evidence {
			metadata["decision_"+key] = value
		}
	}
	return Intent{Payload: payload, Metadata: metadata}, nil
}

var _ IntentBuilder = (*SlipstreamBuilder)(nil)
var _ SlippageIntentBuilder = (*SlipstreamBuilder)(nil)
