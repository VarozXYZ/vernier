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
			Selected: marketDirection("market-a", "market-b"), Probes: []strategy.DirectionProbeTiming{{
				Size: probeSize, Winner: "market-a", Duration: time.Millisecond,
				Outputs: []strategy.DirectionProbeOutput{
					{Market: "market-a", Output: mustAssetQuantity(t, "base", "12"), Duration: 700 * time.Microsecond},
					{Market: "market-b", Output: mustAssetQuantity(t, "base", "11"), Duration: 300 * time.Microsecond, Cached: true},
				},
			}},
		}},
	}}
	var text bytes.Buffer
	if err := livecompare.WriteTextWithOptions(&text, report, livecompare.OutputOptions{Calculations: livecompare.CalculationSummary}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(text.String(), "private-run") || strings.Contains(text.String(), "private-config") || strings.Contains(text.String(), "curve ") {
		t.Fatalf("summary output leaked repeated/full fields: %s", text.String())
	}
	for _, removed := range []string{"evaluation:", "status:", "external_reference_checks:", "parity_checks:"} {
		if strings.Contains(text.String(), removed) {
			t.Fatalf("summary output retained noisy field %q: %s", removed, text.String())
		}
	}
	if !strings.Contains(text.String(), "Discovery    2.00 ms (3 samples, majority)") {
		t.Fatalf("summary output omitted direction discovery: %s", text.String())
	}
	for _, expected := range []string{
		"Probe 1      10 QUOTE · total 1.00 ms",
		"market-a  12 BASE · 700.00 us · fresh",
		"market-b  11 BASE · 300.00 us · cached",
	} {
		if !strings.Contains(text.String(), expected) {
			t.Fatalf("summary output omitted probe timing %q: %s", expected, text.String())
		}
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

func TestSummaryOutputUsesReadableQuoteAmountsAndKeepsRawValuesInJSON(t *testing.T) {
	input, err := market.ParseAssetQuantity("quote", "750")
	if err != nil {
		t.Fatal(err)
	}
	output, err := market.ParseAssetQuantity("base", "14550")
	if err != nil {
		t.Fatal(err)
	}
	inputRaw := mustTokenAmount(t, "quote-token", "750000000")
	outputRaw := mustTokenAmount(t, "base-token", "14550000000000")
	report := livecompare.Report{Research: runtimeresearch.Report{
		Status: runtimeresearch.StatusHealthy, Evaluations: 1,
		LocalTiming: strategy.EvaluationTiming{
			Duration: 20 * time.Millisecond,
			Directions: []strategy.DirectionTiming{{
				Direction: marketDirection("market-a", "market-b"),
				Duration:  12 * time.Millisecond,
				Quotes: []strategy.QuoteTiming{{
					Market: "market-a", Source: "provider-a", Leg: "buy", Mode: market.QuoteModeExactInput,
					Duration: 12 * time.Millisecond, AmountIn: inputRaw, AmountOut: outputRaw,
					Input: input, Output: output,
				}},
			}},
		},
	}}
	var text bytes.Buffer
	if err := livecompare.WriteTextWithOptions(&text, report, livecompare.OutputOptions{Calculations: livecompare.CalculationSummary}); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"ROUND TRIPS (parallel; buy -> sell within each route)",
		"ROUTE 1: market-a -> market-b",
		"BUY   provider-a  market-a  750 QUOTE  ->  14,550 BASE  [12.00 ms]",
		"Route time  12.00 ms",
		"TOTAL          20.00 ms",
	} {
		if !strings.Contains(text.String(), expected) {
			t.Fatalf("summary output omitted %q: %s", expected, text.String())
		}
	}
	for _, raw := range []string{"750000000", "14550000000000"} {
		if strings.Contains(text.String(), raw) {
			t.Fatalf("summary output exposed raw amount %q: %s", raw, text.String())
		}
	}
	var jsonl bytes.Buffer
	if err := livecompare.WriteJSONLineWithOptions(&jsonl, report, livecompare.OutputOptions{Calculations: livecompare.CalculationSummary}); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		`"provider":"provider-a"`, `"input":"750"`, `"input_raw":"750000000"`,
		`"output":"14550"`, `"output_raw":"14550000000000"`, `"latency":"12ms"`,
	} {
		if !strings.Contains(jsonl.String(), expected) {
			t.Fatalf("summary JSON omitted %s: %s", expected, jsonl.String())
		}
	}
}

func TestSummaryOutputLinksTransactionTriggerAndOmitsUncalculatedDirection(t *testing.T) {
	report := livecompare.Report{Research: runtimeresearch.Report{
		LocalTiming: strategy.EvaluationTiming{Duration: 25 * time.Millisecond},
		Opportunities: []arbitrage.Opportunity{{
			Direction:     marketDirection("not-calculated", "other"),
			SelectedIndex: -1,
			HasTrigger:    true,
			Trigger: arbitrage.TriggerMetadata{
				Market: "market-polygon", Source: "polygon/pool-activity/0",
				Reference: market.SourceReference{Kind: "evm_transaction_hash", Value: "0xabc123"},
				At:        time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC),
			},
		}},
	}}
	var text bytes.Buffer
	if err := livecompare.WriteTextWithOptions(&text, report, livecompare.OutputOptions{
		Calculations: livecompare.CalculationSummary, OmitCost: true,
	}); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"TRIGGER: EVM TRANSACTION | 2026-07-27 12:00:00.000 UTC",
		"Market       Polygon",
		"Watcher      polygon/pool-activity/0",
		"Transaction  0xabc123",
		"Explorer     https://polygonscan.com/tx/0xabc123",
		"TOTAL          25.00 ms",
	} {
		if !strings.Contains(text.String(), expected) {
			t.Fatalf("summary output omitted %q: %s", expected, text.String())
		}
	}
	if strings.Contains(text.String(), "not-calculated->other") || strings.Contains(text.String(), "opportunity:") {
		t.Fatalf("summary output retained the uncalculated leg: %s", text.String())
	}
}

func TestSummaryCostCanBeEmittedOncePerStream(t *testing.T) {
	report := fullReport(t)
	var first bytes.Buffer
	if err := livecompare.WriteTextWithOptions(&first, report, livecompare.OutputOptions{
		Calculations: livecompare.CalculationSummary,
	}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(first.String(), "Fixed cost   1 USD") || strings.Contains(first.String(), "not_required") {
		t.Fatalf("first report has unexpected cost output: %s", first.String())
	}
	var repeated bytes.Buffer
	if err := livecompare.WriteTextWithOptions(&repeated, report, livecompare.OutputOptions{
		Calculations: livecompare.CalculationSummary, OmitCost: true,
	}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(repeated.String(), "Fixed cost") {
		t.Fatalf("repeated report included invariant cost: %s", repeated.String())
	}
}

func TestSummaryOutputDoesNotPresentBootstrapAsTransaction(t *testing.T) {
	report := livecompare.Report{Research: runtimeresearch.Report{
		LocalTiming: strategy.EvaluationTiming{Duration: 10 * time.Millisecond},
		Opportunities: []arbitrage.Opportunity{{
			SelectedIndex: -1,
			HasTrigger:    true,
			Trigger: arbitrage.TriggerMetadata{
				Market: "synthetic_solana", Source: "jupiter_solana/events",
				Reference: market.SourceReference{Kind: "solana_signature", Value: "bootstrap"},
				At:        time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC),
			},
		}},
	}}
	var text bytes.Buffer
	if err := livecompare.WriteTextWithOptions(&text, report, livecompare.OutputOptions{
		Calculations: livecompare.CalculationSummary, OmitCost: true,
	}); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"TRIGGER: BOOTSTRAP | 2026-07-27 12:00:00.000 UTC",
		"Market       Solana",
		"Watcher      jupiter_solana/events",
	} {
		if !strings.Contains(text.String(), expected) {
			t.Fatalf("bootstrap summary omitted %q: %s", expected, text.String())
		}
	}
	if strings.Contains(text.String(), "Transaction") || strings.Contains(text.String(), "Explorer") {
		t.Fatalf("bootstrap summary was presented as a transaction: %s", text.String())
	}
}

func TestSummaryOutputMarksTheSelectedRoundTripRatherThanTheLargestBuy(t *testing.T) {
	gross, err := market.ParseAssetQuantity("quote", "3")
	if err != nil {
		t.Fatal(err)
	}
	net, err := market.ParseAssetQuantity("quote", "2")
	if err != nil {
		t.Fatal(err)
	}
	report := livecompare.Report{Research: runtimeresearch.Report{
		LocalTiming: strategy.EvaluationTiming{
			Duration: 30 * time.Millisecond,
			Directions: []strategy.DirectionTiming{
				{Direction: marketDirection("market-a", "market-b"), Duration: 20 * time.Millisecond},
				{Direction: marketDirection("market-b", "market-a"), Duration: 25 * time.Millisecond},
			},
		},
		Opportunities: []arbitrage.Opportunity{
			{Direction: marketDirection("market-a", "market-b"), SelectedIndex: -1},
			{
				Direction: marketDirection("market-b", "market-a"), SelectedIndex: 0,
				Classification: arbitrage.ClassificationPolicyQualified,
				Candidates:     []arbitrage.Candidate{{GrossPnL: gross, NetPnL: net}},
			},
		},
	}}
	var text bytes.Buffer
	if err := livecompare.WriteTextWithOptions(&text, report, livecompare.OutputOptions{
		Calculations: livecompare.CalculationSummary, OmitCost: true,
	}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text.String(), "ROUTE 2: market-b -> market-a  [SELECTED]") ||
		strings.Count(text.String(), "[SELECTED]") != 1 ||
		!strings.Contains(text.String(), "RESULT (market-b -> market-a)") {
		t.Fatalf("summary did not identify the selected complete route: %s", text.String())
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

func TestZeroCostOutputDoesNotRequirePriceEvidence(t *testing.T) {
	zero, err := market.NewAssetQuantity("quote", new(big.Rat))
	if err != nil {
		t.Fatal(err)
	}
	report := livecompare.Report{
		Research: runtimeresearch.Report{Status: runtimeresearch.StatusHealthy, Evaluations: 1},
		Cost: livecompare.CostEvidence{
			FixedAmount: new(big.Rat), FixedAsset: "usd", Cost: zero,
		},
	}
	var text bytes.Buffer
	if err := livecompare.WriteTextWithOptions(&text, report, livecompare.OutputOptions{Calculations: livecompare.CalculationFull}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text.String(), "price: not_required") || strings.Contains(text.String(), "price_source:") {
		t.Fatalf("zero-cost report implied a price lookup: %s", text.String())
	}
	var jsonl bytes.Buffer
	if err := livecompare.WriteJSONLineWithOptions(&jsonl, report, livecompare.OutputOptions{Calculations: livecompare.CalculationFull}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(jsonl.String(), `"price_required":false`) || !strings.Contains(jsonl.String(), `"price_source":"not_required"`) {
		t.Fatalf("zero-cost JSON did not describe skipped conversion: %s", jsonl.String())
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

func mustAssetQuantity(t *testing.T, asset market.AssetID, value string) market.AssetQuantity {
	t.Helper()
	amount, err := market.ParseAssetQuantity(asset, value)
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
