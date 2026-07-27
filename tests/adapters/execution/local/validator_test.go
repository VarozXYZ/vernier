package local_test

import (
	"context"
	"crypto/sha256"
	"math/big"
	"testing"
	"time"

	localexecution "github.com/VarozXYZ/vernier/adapters/execution/local"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type validatorSnapshot struct{}

func (validatorSnapshot) SnapshotKind() string { return "synthetic" }

type executableSource struct {
	result localexecution.ExecutableQuote
}

func (s executableSource) QuoteExecutable(context.Context, quoteport.Input) (localexecution.ExecutableQuote, error) {
	return s.result, nil
}

type allocationBuilder struct {
	operation  execution.OperationID
	allocation execution.RouteAllocation
}

func (b *allocationBuilder) BuildIntent(
	_ context.Context,
	operation execution.OperationID,
	_ execution.Leg,
	_ market.Quote,
	allocation execution.RouteAllocation,
) (localexecution.Intent, error) {
	b.operation = operation
	b.allocation = allocation.Clone()
	return localexecution.Intent{
		Payload: []byte{1}, Metadata: map[string]string{"to": "synthetic"},
	}, nil
}

func TestLocalValidationCarriesOptimizerAllocationIntoArtifact(t *testing.T) {
	now := time.Date(2026, 7, 27, 21, 0, 0, 0, time.UTC)
	input, _ := market.NewTokenAmount("quote", big.NewInt(100))
	output, _ := market.NewTokenAmount("base", big.NewInt(145))
	quote, err := market.NewQuote(market.Quote{
		Source: "local", Market: "local-market", SnapshotVersion: 1,
		Purpose: market.QuotePurposeLiveValidation, Mode: market.QuoteModeExactInput,
		AmountIn: input, AmountOut: output, QuotedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	allocation := execution.RouteAllocation{
		Input: input, ExpectedOutput: output,
		Groups: []execution.RouteGroup{{
			ID: "direct", InputToken: "quote", OutputToken: "base",
			Branches: []execution.RouteBranch{{
				Market: "pool", PlannedInput: big.NewInt(100), ExpectedOutput: big.NewInt(145),
			}},
		}},
	}
	builder := &allocationBuilder{}
	validator, err := localexecution.New(localexecution.Config{
		Source: executableSource{result: localexecution.ExecutableQuote{
			Quote: quote, Allocation: allocation,
		}},
		Builder: builder, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := market.NewMarketSnapshot(market.SnapshotMetadata{
		Market: "local-market", Source: "feed", Version: 1,
		ReceivedAt: now, AppliedAt: now, Health: market.HealthHealthy,
		HealthChangedAt: now, StateHash: sha256.Sum256([]byte("state")),
	}, validatorSnapshot{})
	if err != nil {
		t.Fatal(err)
	}
	leg := execution.Leg{
		ID: "buy", Side: execution.LegBuy, Chain: "chain", Account: "account",
		Market: "local-market", Input: input, ExpectedOutput: output,
	}
	artifact, err := validator.Validate(context.Background(), executionport.ValidationRequest{
		Operation: "operation", Leg: leg, Snapshot: snapshot, RequestedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if builder.operation != "operation" || artifact.Allocation == nil ||
		artifact.Allocation.Groups[0].Branches[0].Market != "pool" {
		t.Fatalf("optimizer allocation was lost: builder=%q artifact=%+v", builder.operation, artifact)
	}
}

var (
	_ localexecution.ExecutableSource = executableSource{}
	_ localexecution.IntentBuilder    = (*allocationBuilder)(nil)
)
