package research

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
)

func TestFixtureProducesDeterministicReport(t *testing.T) {
	fixture, hash := loadExample(t)

	first := runFixture(t, fixture, hash)
	second := runFixture(t, fixture, hash)
	if first.Status != StatusHealthy || first.Evaluations != 2 || len(first.Opportunities) != 8 {
		t.Fatalf("unexpected report summary: %+v", first)
	}
	if first.Opportunities[0].Classification != arbitrage.ClassificationPolicyQualified {
		t.Fatalf("first opportunity classified as %q", first.Opportunities[0].Classification)
	}
	if first.Opportunities[2].Classification != arbitrage.ClassificationEconomic {
		t.Fatalf("conservative opportunity classified as %q", first.Opportunities[2].Classification)
	}
	assertFixedSnapshots(t, first)
	assertSharedSnapshots(t, first.Opportunities[0], first.Opportunities[2])

	var firstJSON, secondJSON bytes.Buffer
	if err := WriteJSON(&firstJSON, first); err != nil {
		t.Fatal(err)
	}
	if err := WriteJSON(&secondJSON, second); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstJSON.Bytes(), secondJSON.Bytes()) {
		t.Fatal("the same fixture produced different JSON")
	}
	for _, required := range []string{hash, `"run_id": "synthetic-two-market"`, `"triggered_at"`, `"snapshot_version"`} {
		if !strings.Contains(firstJSON.String(), required) {
			t.Fatalf("JSON does not contain %q", required)
		}
	}
}

func assertSharedSnapshots(t *testing.T, left, right arbitrage.Opportunity) {
	t.Helper()
	if len(left.Snapshots) != 2 || len(right.Snapshots) != 2 {
		t.Fatal("strategies did not retain both snapshot references")
	}
	for index := range left.Snapshots {
		if left.Snapshots[index].Version != right.Snapshots[index].Version ||
			left.Snapshots[index].StateHash != right.Snapshots[index].StateHash {
			t.Fatal("strategies did not share the same immutable market state")
		}
	}
}

func TestGapDegradesReportAndOpportunities(t *testing.T) {
	fixture, hash := loadExample(t)
	fixture.Feeds[1].Events[1].Sequence = 3

	report := runFixture(t, fixture, hash)
	if report.Status != StatusDegraded || len(report.Gaps) != 1 {
		t.Fatalf("expected one explicit gap, got %+v", report)
	}
	if report.Gaps[0].Expected != 2 || report.Gaps[0].Actual != 3 || report.Gaps[0].Kind != "gap" {
		t.Fatalf("unexpected gap evidence: %+v", report.Gaps[0])
	}
	for _, opportunity := range report.Opportunities[len(report.Opportunities)-4:] {
		if opportunity.Classification != arbitrage.ClassificationUnclassifiable {
			t.Fatalf("gap result classified as %q", opportunity.Classification)
		}
		if len(opportunity.Reasons) != 1 || opportunity.Reasons[0] != "degraded_market_snapshot" {
			t.Fatalf("unexpected degradation reason: %v", opportunity.Reasons)
		}
	}
}

func TestFixtureDecoderIsStrict(t *testing.T) {
	for name, data := range map[string]string{
		"unknown field": `{"schema_version":1,"unknown":true}`,
		"wrong version": `{"schema_version":2}`,
		"second value":  `{"schema_version":1} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := ParseFixture([]byte(data)); err == nil {
				t.Fatal("expected fixture rejection")
			}
		})
	}
}

func TestRunnerIsSingleUse(t *testing.T) {
	fixture, hash := loadExample(t)
	runner, err := NewRunner(fixture, hash)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(context.Background()); err == nil {
		t.Fatal("second run unexpectedly succeeded")
	}
}

func loadExample(t *testing.T) (Fixture, string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "examples", "synthetic", "two-market.json"))
	if err != nil {
		t.Fatal(err)
	}
	fixture, hash, err := ParseFixture(data)
	if err != nil {
		t.Fatal(err)
	}
	return fixture, hash
}

func runFixture(t *testing.T, fixture Fixture, hash string) Report {
	t.Helper()
	runner, err := NewRunner(fixture, hash)
	if err != nil {
		t.Fatal(err)
	}
	report, err := runner.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return report
}

func assertFixedSnapshots(t *testing.T, report Report) {
	t.Helper()
	for _, opportunity := range report.Opportunities {
		if len(opportunity.Candidates) < 2 {
			continue
		}
		buyVersion := opportunity.Candidates[0].BuyQuote.SnapshotVersion
		sellVersion := opportunity.Candidates[0].SellQuote.SnapshotVersion
		for _, candidate := range opportunity.Candidates[1:] {
			if candidate.BuyQuote.SnapshotVersion != buyVersion || candidate.SellQuote.SnapshotVersion != sellVersion {
				t.Fatalf("opportunity %q mixes snapshot versions", opportunity.Evaluation)
			}
		}
	}
}
