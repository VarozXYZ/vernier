package local_test

import (
	"bytes"
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	gethcrypto "github.com/ethereum/go-ethereum/crypto"

	localexecution "github.com/VarozXYZ/vernier/adapters/execution/local"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

func TestAtomicRouteBuilderPreservesAllocationAndExplicitMinimum(t *testing.T) {
	builder, err := localexecution.NewAtomicRouteBuilder(localexecution.AtomicRouteConfig{
		Executor: common.HexToAddress("0x0000000000000000000000000000000000000010"),
		TokenAddresses: map[market.TokenID]common.Address{
			"quote":  common.HexToAddress("0x0000000000000000000000000000000000000030"),
			"base":   common.HexToAddress("0x0000000000000000000000000000000000000040"),
			"middle": common.HexToAddress("0x0000000000000000000000000000000000000050"),
		},
		Adapters:    map[market.MarketID]uint16{"direct": 2, "first": 3, "second": 4},
		SlippageBPS: 100, Deadline: time.Minute, GasLimit: 700_000,
		Clock: func() time.Time { return time.Unix(1_000, 0) },
	})
	if err != nil {
		t.Fatal(err)
	}
	in, _ := market.NewTokenAmount("quote", big.NewInt(1_000))
	out, _ := market.NewTokenAmount("base", big.NewInt(2_000))
	minimum, _ := market.NewTokenAmount("base", big.NewInt(1_925))
	quote, _ := market.NewQuote(market.Quote{Source: "local", Market: "composite", SnapshotVersion: 7,
		Purpose: market.QuotePurposeLiveValidation, Mode: market.QuoteModeExactInput,
		Quality: market.QuoteQualityExact, AmountIn: in, AmountOut: out, QuotedAt: time.Unix(1_000, 0)})
	allocation := execution.RouteAllocation{Input: in, ExpectedOutput: out, Groups: []execution.RouteGroup{
		{ID: "direct-root", InputToken: "quote", OutputToken: "base", Branches: []execution.RouteBranch{
			{Market: "direct", PlannedInput: big.NewInt(400), ExpectedOutput: big.NewInt(800)},
		}},
		{ID: "indirect-root", InputToken: "quote", OutputToken: "middle", Branches: []execution.RouteBranch{
			{Market: "first", PlannedInput: big.NewInt(600), ExpectedOutput: big.NewInt(1_200)},
		}},
		{ID: "continuation", Parent: "indirect-root", InputToken: "middle", OutputToken: "base", Branches: []execution.RouteBranch{
			{Market: "second", PlannedInput: big.NewInt(1_200), ExpectedOutput: big.NewInt(1_200)},
		}},
	}}
	intent, err := builder.BuildIntentWithSlippage(context.Background(), "operation-1", execution.Leg{
		ID: "leg", Side: execution.LegBuy, Chain: "chain", Account: "account", Market: "composite",
		Input: in, ExpectedOutput: out,
	}, quote, allocation, &executionport.SlippageConstraint{BPS: 375, MinimumOutput: minimum})
	if err != nil {
		t.Fatal(err)
	}
	wantSelector := gethcrypto.Keccak256([]byte("execute(bytes32,(address,address,uint32,uint32,uint32,uint256)[],(uint16,uint256,bytes)[],uint256,uint256,uint256)"))[:4]
	if len(intent.Payload) <= 4 || !bytes.Equal(intent.Payload[:4], wantSelector) {
		t.Fatalf("unexpected atomic executor calldata selector %x", intent.Payload[:4])
	}
	if intent.Metadata["minimum_output_units"] != "1925" || intent.Metadata["route_groups"] != "3" || intent.Metadata["slippage_bps"] != "375" {
		t.Fatalf("allocation/slippage metadata was not preserved: %v", intent.Metadata)
	}
}
