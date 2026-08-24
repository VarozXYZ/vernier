package livecanary_test

import (
	"context"
	"math"
	"math/big"
	"strconv"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	"github.com/VarozXYZ/vernier/runtime/livecanary"
)

type oversizedCostEstimator struct {
	calls int
	now   time.Time
}

func (e *oversizedCostEstimator) EstimateArtifactNetworkCost(
	_ context.Context,
	artifact executionport.Artifact,
) (*big.Int, time.Time, error) {
	e.calls++
	maxAccounts, _ := strconv.Atoi(artifact.Metadata["max_accounts"])
	if maxAccounts > 48 {
		return nil, time.Time{}, &executionport.ArtifactTooLargeError{
			ActualBytes: 1247, MaximumBytes: 1232,
		}
	}
	return big.NewInt(1_005_000), e.now, nil
}

type compactCostValidator struct {
	compactions int
}

func (v *compactCostValidator) Validate(
	context.Context,
	executionport.ValidationRequest,
) (executionport.Artifact, error) {
	return executionport.Artifact{}, nil
}

func (v *compactCostValidator) ValidateCompact(
	_ context.Context,
	_ executionport.ValidationRequest,
	previous executionport.Artifact,
) (executionport.Artifact, error) {
	v.compactions++
	previous.Metadata["max_accounts"] = "48"
	return previous, nil
}

func TestCostEstimationCompactsOversizedSolanaArtifact(t *testing.T) {
	now := time.Now().UTC()
	estimator := &oversizedCostEstimator{now: now}
	validator := &compactCostValidator{}
	input, err := market.NewTokenAmount("quote", big.NewInt(750_000_000))
	if err != nil {
		t.Fatal(err)
	}
	output, err := market.NewTokenAmount("base", big.NewInt(3_000_000_000))
	if err != nil {
		t.Fatal(err)
	}
	request := executionport.ValidationRequest{
		Operation: "cost-probe",
		Leg: execution.Leg{
			ID: "swap", Side: execution.LegBuy, Chain: "solana",
			Account: "payer", Market: "market",
			Input: input, ExpectedOutput: output,
		},
	}
	units, capturedAt, err := livecanary.EstimateArtifactNetworkCostWithCompaction(
		context.Background(),
		estimator,
		validator,
		request,
		executionport.Artifact{
			Metadata: map[string]string{"max_accounts": "64"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if units.Cmp(big.NewInt(1_005_000)) != 0 ||
		!capturedAt.Equal(now) {
		t.Fatalf("cost=%s captured_at=%s", units, capturedAt)
	}
	if estimator.calls != 2 || validator.compactions != 1 {
		t.Fatalf(
			"estimate_calls=%d compactions=%d",
			estimator.calls,
			validator.compactions,
		)
	}
}

func TestAcrossSolanaPayerDebitIncludesCalibratedAdditionalDebit(t *testing.T) {
	total, err := livecanary.EstimateAcrossSolanaPayerDebit(10_000, 3_869_760)
	if err != nil {
		t.Fatal(err)
	}
	if total != 3_879_760 {
		t.Fatalf("payer debit=%d want=3879760", total)
	}
}

func TestAcrossSolanaPayerDebitRejectsOverflow(t *testing.T) {
	if _, err := livecanary.EstimateAcrossSolanaPayerDebit(
		2, math.MaxUint64,
	); err == nil {
		t.Fatal("expected overflow error")
	}
}
