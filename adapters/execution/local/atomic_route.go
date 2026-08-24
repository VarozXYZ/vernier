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
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

const atomicRouteExecutorABI = `[{"inputs":[{"name":"operationId","type":"bytes32"},{"components":[{"name":"tokenIn","type":"address"},{"name":"tokenOut","type":"address"},{"name":"parent","type":"uint32"},{"name":"firstBranch","type":"uint32"},{"name":"branchCount","type":"uint32"},{"name":"plannedInput","type":"uint256"}],"name":"groups","type":"tuple[]"},{"components":[{"name":"adapter","type":"uint16"},{"name":"weight","type":"uint256"},{"name":"data","type":"bytes"}],"name":"branches","type":"tuple[]"},{"name":"amountIn","type":"uint256"},{"name":"minOut","type":"uint256"},{"name":"deadline","type":"uint256"}],"name":"execute","outputs":[{"name":"amountOut","type":"uint256"}],"stateMutability":"nonpayable","type":"function"}]`

const noParent = ^uint32(0)

type AtomicRouteConfig struct {
	Executor       common.Address
	TokenAddresses map[market.TokenID]common.Address
	Adapters       map[market.MarketID]uint16
	SlippageBPS    uint16
	Deadline       time.Duration
	GasLimit       uint64
	Clock          func() time.Time
}

type AtomicRouteBuilder struct {
	config AtomicRouteConfig
	abi    abi.ABI
}

func NewAtomicRouteBuilder(config AtomicRouteConfig) (*AtomicRouteBuilder, error) {
	if config.Executor == (common.Address{}) || len(config.TokenAddresses) == 0 ||
		len(config.Adapters) == 0 || config.SlippageBPS > 10_000 ||
		config.Deadline <= 0 || config.GasLimit == 0 {
		return nil, fmt.Errorf("atomic route builder configuration is incomplete")
	}
	for token, address := range config.TokenAddresses {
		if token == "" || address == (common.Address{}) {
			return nil, fmt.Errorf("atomic route token allowlist is invalid")
		}
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	parsed, err := abi.JSON(strings.NewReader(atomicRouteExecutorABI))
	if err != nil {
		return nil, err
	}
	return &AtomicRouteBuilder{config: config, abi: parsed}, nil
}

func (b *AtomicRouteBuilder) BuildIntent(ctx context.Context, operation execution.OperationID,
	leg execution.Leg, quote market.Quote, allocation execution.RouteAllocation) (Intent, error) {
	return b.BuildIntentWithSlippage(ctx, operation, leg, quote, allocation, nil)
}

func (b *AtomicRouteBuilder) BuildIntentWithSlippage(_ context.Context, operation execution.OperationID,
	leg execution.Leg, quote market.Quote, allocation execution.RouteAllocation,
	constraint *executionport.SlippageConstraint) (Intent, error) {
	if operation == "" || quote.Market != leg.Market || quote.AmountIn.Token() != leg.Input.Token() ||
		quote.AmountIn.Units().Cmp(leg.Input.Units()) != 0 {
		return Intent{}, fmt.Errorf("atomic route intent does not match its fixed leg")
	}
	if err := allocation.Validate(); err != nil {
		return Intent{}, err
	}
	if allocation.Input.Token() != quote.AmountIn.Token() ||
		allocation.Input.Units().Cmp(quote.AmountIn.Units()) != 0 ||
		allocation.ExpectedOutput.Token() != quote.AmountOut.Token() ||
		allocation.ExpectedOutput.Units().Cmp(quote.AmountOut.Units()) != 0 {
		return Intent{}, fmt.Errorf("atomic route allocation does not match quote")
	}

	type abiGroup struct {
		TokenIn      common.Address
		TokenOut     common.Address
		Parent       uint32
		FirstBranch  uint32
		BranchCount  uint32
		PlannedInput *big.Int
	}
	type abiBranch struct {
		Adapter uint16
		Weight  *big.Int
		Data    []byte
	}
	groups := make([]abiGroup, 0, len(allocation.Groups))
	branches := make([]abiBranch, 0)
	indices := make(map[execution.AllocationGroupID]uint32, len(allocation.Groups))
	for index, group := range allocation.Groups {
		in, inOK := b.config.TokenAddresses[group.InputToken]
		out, outOK := b.config.TokenAddresses[group.OutputToken]
		if !inOK || !outOK || in == out {
			return Intent{}, fmt.Errorf("atomic route token is not allowlisted")
		}
		parent := uint32(noParent)
		if group.Parent != "" {
			var ok bool
			parent, ok = indices[group.Parent]
			if !ok {
				return Intent{}, fmt.Errorf("atomic route parent does not precede child")
			}
		}
		planned := new(big.Int)
		first := uint32(len(branches))
		for _, branch := range group.Branches {
			adapter, ok := b.config.Adapters[branch.Market]
			if !ok {
				return Intent{}, fmt.Errorf("atomic route market %q is not allowlisted", branch.Market)
			}
			planned.Add(planned, branch.PlannedInput)
			branches = append(branches, abiBranch{
				Adapter: adapter, Weight: new(big.Int).Set(branch.PlannedInput), Data: []byte{},
			})
		}
		groups = append(groups, abiGroup{
			TokenIn: in, TokenOut: out, Parent: parent, FirstBranch: first,
			BranchCount: uint32(len(group.Branches)), PlannedInput: planned,
		})
		indices[group.ID] = uint32(index)
	}
	minimum, reason, err := b.minimumOutput(quote, constraint)
	if err != nil {
		return Intent{}, err
	}
	deadline := big.NewInt(b.config.Clock().Add(b.config.Deadline).Unix())
	// The on-chain replay key identifies one economic leg, not the whole saga:
	// a later circuit-breaker leg on the same executor must remain possible.
	operationHash := crypto.Keccak256Hash([]byte(string(operation) + "\x00" + string(leg.ID)))
	slippageBPS := b.config.SlippageBPS
	if constraint != nil {
		slippageBPS = constraint.BPS
	}
	payload, err := b.abi.Pack("execute", operationHash, groups, branches,
		allocation.Input.Units(), minimum, deadline)
	if err != nil {
		return Intent{}, fmt.Errorf("encode atomic route executor: %w", err)
	}
	metadata := map[string]string{
		"kind": "atomic_local_route", "to": b.config.Executor.Hex(), "value": "0",
		"gas_limit": strconv.FormatUint(b.config.GasLimit, 10), "deadline": deadline.String(),
		"minimum_output_units": minimum.String(), "slippage_reason": reason,
		"operation_hash": operationHash.Hex(), "simulation": "skipped_local_quote_gate",
		"route_groups": strconv.Itoa(len(groups)), "route_branches": strconv.Itoa(len(branches)),
		"slippage_bps": strconv.FormatUint(uint64(slippageBPS), 10),
	}
	if constraint != nil {
		for key, value := range constraint.Evidence {
			metadata["decision_"+key] = value
		}
	}
	return Intent{Payload: payload, Metadata: metadata}, nil
}

func (b *AtomicRouteBuilder) minimumOutput(quote market.Quote,
	constraint *executionport.SlippageConstraint) (*big.Int, string, error) {
	if constraint != nil {
		minimum := constraint.MinimumOutput
		if minimum.Token() != quote.AmountOut.Token() || minimum.IsZero() ||
			minimum.Units().Cmp(quote.AmountOut.Units()) > 0 {
			return nil, "", fmt.Errorf("atomic route slippage floor is incompatible with quote")
		}
		return minimum.Units(), constraint.Reason, nil
	}
	minimum := new(big.Int).Mul(quote.AmountOut.Units(), big.NewInt(int64(10_000-b.config.SlippageBPS)))
	minimum.Quo(minimum, big.NewInt(10_000))
	if minimum.Sign() <= 0 {
		return nil, "", fmt.Errorf("atomic route minimum output rounds to zero")
	}
	return minimum, "fixed_bps", nil
}

var _ SlippageIntentBuilder = (*AtomicRouteBuilder)(nil)
