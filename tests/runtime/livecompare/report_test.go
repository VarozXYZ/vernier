package livecompare_test

import (
	"bytes"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	"github.com/VarozXYZ/vernier/runtime/livecompare"
	runtimeresearch "github.com/VarozXYZ/vernier/runtime/research"
)

func TestSummaryOutputOmitsRunMetadataAndCalculationCurve(t *testing.T) {
	probeSize, err := market.ParseAssetQuantity("quote", "10")
	if err != nil {
		t.Fatal(err)
	}
	report := livecompare.Report{Research: runtimeresearch.Report{
		RunID: "private-run", ConfigHash: "private-config", Status: runtimeresearch.StatusHealthy, Evaluations: 4,
		LocalTiming: strategy.EvaluationTiming{Discovery: &strategy.DirectionDiscoveryTiming{
			Samples: 3, Duration: 2 * time.Millisecond, Decision: "majority",
			Selected: marketDirection("market-a", "market-b"), Probes: []strategy.DirectionProbeTiming{{Size: probeSize, Winner: "market-a", Duration: time.Millisecond}},
		}},
	}}
	var text bytes.Buffer
	if err := livecompare.WriteTextWithOptions(&text, report, livecompare.OutputOptions{Calculations: livecompare.CalculationSummary}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(text.String(), "private-run") || strings.Contains(text.String(), "private-config") || strings.Contains(text.String(), "curve ") {
		t.Fatalf("summary output leaked repeated/full fields: %s", text.String())
	}
	if !strings.Contains(text.String(), "evaluation: 4") {
		t.Fatalf("summary output omitted evaluation number: %s", text.String())
	}
	if !strings.Contains(text.String(), "direction_discovery samples=3") {
		t.Fatalf("summary output omitted direction discovery: %s", text.String())
	}

	var jsonl bytes.Buffer
	if err := livecompare.WriteJSONLineWithOptions(&jsonl, report, livecompare.OutputOptions{Calculations: livecompare.CalculationSummary}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(jsonl.String(), "private-run") || strings.Contains(jsonl.String(), "private-config") || strings.Contains(jsonl.String(), "candidates\": [") {
		t.Fatalf("summary JSONL contains full report fields: %s", jsonl.String())
	}
	if !strings.Contains(jsonl.String(), "\"kind\":\"evaluation\"") {
		t.Fatalf("summary JSONL has no evaluation kind: %s", jsonl.String())
	}
	if !strings.Contains(jsonl.String(), "\"direction_discovery\"") {
		t.Fatalf("summary JSONL omitted direction discovery: %s", jsonl.String())
	}
}

func TestFullOutputRemainsAvailableExplicitly(t *testing.T) {
	report := fullReport(t)
	var jsonl bytes.Buffer
	if err := livecompare.WriteJSONLineWithOptions(&jsonl, report, livecompare.OutputOptions{Calculations: livecompare.CalculationFull}); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"\"run_id\":\"run\"", "\"config_hash\":\"config\"", "\"schema_version\":1"} {
		if !strings.Contains(jsonl.String(), expected) {
			t.Fatalf("full JSONL omitted %s: %s", expected, jsonl.String())
		}
	}
}

func TestFullOutputIncludesQuoteErrors(t *testing.T) {
	probeSize, err := market.ParseAssetQuantity("quote", "10")
	if err != nil {
		t.Fatal(err)
	}
	zero, err := market.ParseAssetQuantity("base", "0")
	if err != nil {
		t.Fatal(err)
	}
	reason := "route quote hop 0: incompatible snapshot"
	report := fullReport(t)
	report.Research = runtimeresearch.Report{
		LocalTiming: strategy.EvaluationTiming{
			Discovery: &strategy.DirectionDiscoveryTiming{
				Samples: 1, Probes: []strategy.DirectionProbeTiming{{Size: probeSize, Reason: "probe_quote_failed", Outputs: []strategy.DirectionProbeOutput{{Market: "market-a", Output: zero, Error: reason}}}},
			},
			Directions: []strategy.DirectionTiming{{Direction: marketDirection("market-a", "market-b"), Quotes: []strategy.QuoteTiming{{Market: "market-a", Leg: "buy", Mode: market.QuoteModeExactInput, Error: reason}}}},
		},
	}
	var text bytes.Buffer
	if err := livecompare.WriteTextWithOptions(&text, report, livecompare.OutputOptions{Calculations: livecompare.CalculationFull}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text.String(), `error="route quote hop 0: incompatible snapshot"`) {
		t.Fatalf("full text omitted quote error: %s", text.String())
	}
	var jsonl bytes.Buffer
	if err := livecompare.WriteJSONLineWithOptions(&jsonl, report, livecompare.OutputOptions{Calculations: livecompare.CalculationFull}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(jsonl.String(), `"error":"route quote hop 0: incompatible snapshot"`) {
		t.Fatalf("full JSON omitted quote error: %s", jsonl.String())
	}
}

func TestReferenceOutputPreservesLocalExternalDeltaAndTimings(t *testing.T) {
	local := mustTokenAmount(t, "token-out", "90")
	remote := mustTokenAmount(t, "token-out", "95")
	input := mustTokenAmount(t, "token-in", "100")
	report := livecompare.ReferenceReport{Evaluation: 2, Comparisons: []livecompare.ReferenceComparison{{
		Direction: marketDirection("buy", "sell"), Market: "market-b", Leg: "sell", Provider: "external",
		SnapshotVersion: 7, Input: input, LocalOutput: local, ReferenceOutput: remote, OutputDeltaRaw: "5",
		Status: "available", LocalQuoteDuration: 2 * time.Microsecond, ReferenceLatency: 3 * time.Millisecond, TotalDuration: 4 * time.Millisecond,
	}}}
	var output bytes.Buffer
	if err := livecompare.WriteReferenceJSONLine(&output, report); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"\"kind\":\"external_reference\"", "\"local_output\":\"90\"", "\"reference_output\":\"95\"", "\"output_delta_raw\":\"5\"", "\"reference_latency\":\"3ms\""} {
		if !strings.Contains(output.String(), expected) {
			t.Fatalf("reference output omitted %s: %s", expected, output.String())
		}
	}
}

func marketDirection(buy, sell string) arbitrage.Direction {
	return arbitrage.Direction{BuyMarket: market.MarketID(buy), SellMarket: market.MarketID(sell)}
}

func mustTokenAmount(t *testing.T, token market.TokenID, value string) market.TokenAmount {
	t.Helper()
	units, ok := new(big.Int).SetString(value, 10)
	if !ok {
		t.Fatal("invalid test amount")
	}
	amount, err := market.NewTokenAmount(token, units)
	if err != nil {
		t.Fatal(err)
	}
	return amount
}

func fullReport(t *testing.T) livecompare.Report {
	t.Helper()
	cost, err := market.NewAssetQuantity("usd", big.NewRat(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	price, err := market.NewPriceObservation("test", "weth", "usd", big.NewRat(2000, 1), "ref", time.Unix(1, 0), time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	return livecompare.Report{
		Research: runtimeresearch.Report{RunID: "run", ConfigHash: "config", Status: runtimeresearch.StatusHealthy, Evaluations: 1},
		Cost:     livecompare.CostEvidence{FixedAmount: big.NewRat(1, 1), FixedAsset: "usd", Cost: cost, Price: price},
	}
}
