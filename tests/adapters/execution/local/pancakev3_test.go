package local_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	localexecution "github.com/VarozXYZ/vernier/adapters/execution/local"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	"github.com/ethereum/go-ethereum/common"
)

func TestPancakeV3BuilderBuildsAllowlistedExactInputWithoutSimulation(t *testing.T) {
	builder, err := localexecution.NewPancakeV3Builder(localexecution.PancakeV3Config{
		Router:    common.HexToAddress("0x0000000000000000000000000000000000000010"),
		Recipient: common.HexToAddress("0x0000000000000000000000000000000000000020"),
		TokenAddresses: map[market.TokenID]common.Address{
			"quote": common.HexToAddress("0x0000000000000000000000000000000000000030"),
			"base":  common.HexToAddress("0x0000000000000000000000000000000000000040"),
		},
		Markets: map[market.MarketID]uint32{"local": 100}, SlippageBPS: 50,
		Deadline: time.Minute, GasLimit: 400_000,
		Clock: func() time.Time { return time.Unix(1_000, 0) },
	})
	if err != nil {
		t.Fatal(err)
	}
	in, _ := market.NewTokenAmount("quote", big.NewInt(500_000_000))
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
	if len(intent.Payload) < 4 || intent.Metadata["to"] == "" ||
		intent.Metadata["simulation"] != "skipped_local_quote_gate" ||
		intent.Metadata["minimum_output_units"] != "1990000000000000000" {
		t.Fatalf("unexpected intent: payload=%d metadata=%v", len(intent.Payload), intent.Metadata)
	}
	if got := common.Bytes2Hex(intent.Payload[:4]); got != "5ae401dc" {
		t.Fatalf("router selector=%s, want deadline multicall", got)
	}
}

func TestPancakeV3BuilderRejectsUnlistedMarket(t *testing.T) {
	_, err := localexecution.NewPancakeV3Builder(localexecution.PancakeV3Config{})
	if err == nil {
		t.Fatal("incomplete builder was accepted")
	}
}
