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
)

func TestSlipstreamBuilderBuildsExactInputSingleForConfiguredTickSpacing(t *testing.T) {
	builder, err := localexecution.NewSlipstreamBuilder(localexecution.SlipstreamConfig{
		Router:    common.HexToAddress("0x0000000000000000000000000000000000000010"),
		Recipient: common.HexToAddress("0x0000000000000000000000000000000000000020"),
		TokenAddresses: map[market.TokenID]common.Address{
			"quote": common.HexToAddress("0x0000000000000000000000000000000000000030"),
			"base":  common.HexToAddress("0x0000000000000000000000000000000000000040"),
		},
		Markets: map[market.MarketID]int32{"local": 1}, SlippageBPS: 50,
		Deadline: time.Minute, GasLimit: 400_000,
		Clock: func() time.Time { return time.Unix(1_000, 0) },
	})
	if err != nil {
		t.Fatal(err)
	}
	in, _ := market.NewTokenAmount("quote", big.NewInt(1_000_000))
	out, _ := market.NewTokenAmount("base", big.NewInt(2_000_000_000_000_000_000))
	quote, _ := market.NewQuote(market.Quote{Source: "local", Market: "local", SnapshotVersion: 1,
		Purpose: market.QuotePurposeLiveValidation, Mode: market.QuoteModeExactInput,
		Quality: market.QuoteQualityExact, AmountIn: in, AmountOut: out, QuotedAt: time.Unix(1_000, 0)})
	allocation := execution.RouteAllocation{Input: in, ExpectedOutput: out, Groups: []execution.RouteGroup{{
		ID: "root", InputToken: "quote", OutputToken: "base", Branches: []execution.RouteBranch{{
			Market: "local", PlannedInput: in.Units(), ExpectedOutput: out.Units(),
		}},
	}}}
	intent, err := builder.BuildIntent(context.Background(), "operation", execution.Leg{
		ID: "leg", Side: execution.LegBuy, Chain: "chain", Account: "account", Market: "local",
		Input: in, ExpectedOutput: out,
	}, quote, allocation)
	if err != nil {
		t.Fatal(err)
	}
	wantSelector := gethcrypto.Keccak256([]byte("exactInputSingle((address,address,int24,address,uint256,uint256,uint256,uint160))"))[:4]
	if len(intent.Payload) != 4+8*32 || !bytes.Equal(intent.Payload[:4], wantSelector) {
		t.Fatalf("unexpected exactInputSingle payload: bytes=%d selector=%x", len(intent.Payload), intent.Payload[:4])
	}
	if intent.Metadata["tick_spacing"] != "1" || intent.Metadata["deadline"] != "1060" ||
		intent.Metadata["minimum_output_units"] != "1990000000000000000" ||
		intent.Metadata["simulation"] != "skipped_local_quote_gate" {
		t.Fatalf("unexpected metadata: %v", intent.Metadata)
	}
}

func TestSlipstreamBuilderRejectsIncompleteConfiguration(t *testing.T) {
	if _, err := localexecution.NewSlipstreamBuilder(localexecution.SlipstreamConfig{}); err == nil {
		t.Fatal("incomplete builder was accepted")
	}
}
