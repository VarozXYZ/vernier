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
)

const pancakeV3RouterABI = `[
 {"inputs":[{"components":[{"name":"tokenIn","type":"address"},{"name":"tokenOut","type":"address"},{"name":"fee","type":"uint24"},{"name":"recipient","type":"address"},{"name":"amountIn","type":"uint256"},{"name":"amountOutMinimum","type":"uint256"},{"name":"sqrtPriceLimitX96","type":"uint160"}],"name":"params","type":"tuple"}],"name":"exactInputSingle","outputs":[{"name":"amountOut","type":"uint256"}],"stateMutability":"payable","type":"function"},
 {"inputs":[{"name":"deadline","type":"uint256"},{"name":"data","type":"bytes[]"}],"name":"multicall","outputs":[{"name":"results","type":"bytes[]"}],"stateMutability":"payable","type":"function"}
]`

type PancakeV3Config struct {
	Router         common.Address
	Recipient      common.Address
	TokenAddresses map[market.TokenID]common.Address
	Markets        map[market.MarketID]uint32
	SlippageBPS    uint16
	Deadline       time.Duration
	GasLimit       uint64
	Clock          func() time.Time
}

// PancakeV3Builder turns a single-pool local allocation into router calldata.
// It deliberately performs no eth_call: the immutable local quote is the Live
// admission evidence and receipt reconciliation proves the eventual effect.
type PancakeV3Builder struct {
	config PancakeV3Config
	router abi.ABI
}

func NewPancakeV3Builder(config PancakeV3Config) (*PancakeV3Builder, error) {
	if config.Router == (common.Address{}) || config.Recipient == (common.Address{}) ||
		len(config.TokenAddresses) == 0 || len(config.Markets) == 0 ||
		config.SlippageBPS == 0 || config.SlippageBPS > 10_000 {
		return nil, fmt.Errorf("PancakeSwap V3 builder configuration is incomplete")
	}
	if config.Deadline <= 0 {
		return nil, fmt.Errorf("PancakeSwap V3 deadline must be positive")
	}
	if config.GasLimit == 0 {
		return nil, fmt.Errorf("PancakeSwap V3 gas limit must be positive")
	}
	for id, address := range config.TokenAddresses {
		if id == "" || address == (common.Address{}) {
			return nil, fmt.Errorf("PancakeSwap V3 token allowlist is invalid")
		}
	}
	for id, fee := range config.Markets {
		if id == "" || fee == 0 || fee > 1_000_000 {
			return nil, fmt.Errorf("PancakeSwap V3 market fee allowlist is invalid")
		}
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	parsed, err := abi.JSON(strings.NewReader(pancakeV3RouterABI))
	if err != nil {
		return nil, err
	}
	return &PancakeV3Builder{config: config, router: parsed}, nil
}

func (b *PancakeV3Builder) BuildIntent(_ context.Context, operation execution.OperationID,
	leg execution.Leg, quote market.Quote, allocation execution.RouteAllocation) (Intent, error) {
	if operation == "" || leg.Market == "" || quote.Market != leg.Market {
		return Intent{}, fmt.Errorf("PancakeSwap V3 intent identity is incomplete")
	}
	if err := allocation.Validate(); err != nil {
		return Intent{}, err
	}
	if len(allocation.Groups) != 1 || len(allocation.Groups[0].Branches) != 1 ||
		allocation.Groups[0].Parent != "" || allocation.Groups[0].Branches[0].Market != leg.Market {
		return Intent{}, fmt.Errorf("PancakeSwap V3 intent requires one allowlisted pool allocation")
	}
	fee, ok := b.config.Markets[leg.Market]
	if !ok {
		return Intent{}, fmt.Errorf("PancakeSwap V3 market is not allowlisted")
	}
	tokenIn, inputOK := b.config.TokenAddresses[leg.Input.Token()]
	tokenOut, outputOK := b.config.TokenAddresses[leg.ExpectedOutput.Token()]
	if !inputOK || !outputOK || tokenIn == tokenOut {
		return Intent{}, fmt.Errorf("PancakeSwap V3 token direction is not allowlisted")
	}
	if quote.AmountIn.Token() != leg.Input.Token() ||
		quote.AmountIn.Units().Cmp(leg.Input.Units()) != 0 ||
		quote.AmountOut.Token() != leg.ExpectedOutput.Token() {
		return Intent{}, fmt.Errorf("PancakeSwap V3 quote does not match the fixed leg")
	}
	minimum := new(big.Int).Mul(quote.AmountOut.Units(), big.NewInt(int64(10_000-b.config.SlippageBPS)))
	minimum.Quo(minimum, big.NewInt(10_000))
	params := struct {
		TokenIn, TokenOut                             common.Address
		Fee                                           *big.Int
		Recipient                                     common.Address
		AmountIn, AmountOutMinimum, SqrtPriceLimitX96 *big.Int
	}{tokenIn, tokenOut, new(big.Int).SetUint64(uint64(fee)), b.config.Recipient,
		leg.Input.Units(), minimum, new(big.Int)}
	inner, err := b.router.Pack("exactInputSingle", params)
	if err != nil {
		return Intent{}, fmt.Errorf("encode PancakeSwap V3 exactInputSingle: %w", err)
	}
	deadline := big.NewInt(b.config.Clock().Add(b.config.Deadline).Unix())
	payload, err := b.router.Pack("multicall", deadline, [][]byte{inner})
	if err != nil {
		return Intent{}, fmt.Errorf("encode PancakeSwap V3 deadline multicall: %w", err)
	}
	return Intent{Payload: payload, Metadata: map[string]string{
		"kind": "pancakeswap_v3_exact_input_single", "to": b.config.Router.Hex(),
		"value": "0", "gas_limit": strconv.FormatUint(b.config.GasLimit, 10),
		"minimum_output_units": minimum.String(), "simulation": "skipped_local_quote_gate",
		"deadline": deadline.String(),
	}}, nil
}

var _ IntentBuilder = (*PancakeV3Builder)(nil)
