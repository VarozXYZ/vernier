package local

import (
	"context"
	"fmt"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

// ValidatedQuoteBuilder builds a one-pool intent from the post-build local
// quote handed off by Research. It performs no quote, RPC call, or simulation.
type ValidatedQuoteBuilder struct {
	Builder IntentBuilder
	Clock   func() time.Time
}

func (v ValidatedQuoteBuilder) Validate(ctx context.Context, request executionport.ValidationRequest) (executionport.Artifact, error) {
	if v.Builder == nil || request.Operation == "" || request.Snapshot.Metadata().Health != market.HealthHealthy ||
		request.Snapshot.Metadata().Market != request.Leg.Market {
		return executionport.Artifact{}, fmt.Errorf("validated local quote builder configuration is incomplete")
	}
	quote := request.Discovery
	if quote.Market != request.Leg.Market || quote.AmountIn.Token() != request.Leg.Input.Token() ||
		quote.AmountIn.Units().Cmp(request.Leg.Input.Units()) != 0 ||
		quote.AmountOut.Token() != request.Leg.ExpectedOutput.Token() {
		return executionport.Artifact{}, fmt.Errorf("post-build local quote does not match the fixed Live leg")
	}
	allocation := execution.RouteAllocation{Input: quote.AmountIn, ExpectedOutput: quote.AmountOut,
		Groups: []execution.RouteGroup{{ID: "local", InputToken: quote.AmountIn.Token(), OutputToken: quote.AmountOut.Token(),
			Branches: []execution.RouteBranch{{Market: quote.Market, PlannedInput: quote.AmountIn.Units(), ExpectedOutput: quote.AmountOut.Units()}}}}}
	var intent Intent
	var err error
	if builder, ok := v.Builder.(SlippageIntentBuilder); ok {
		intent, err = builder.BuildIntentWithSlippage(
			ctx, request.Operation, request.Leg, quote, allocation, request.Slippage,
		)
	} else {
		intent, err = v.Builder.BuildIntent(ctx, request.Operation, request.Leg, quote, allocation)
	}
	if err != nil {
		return executionport.Artifact{}, err
	}
	clock := v.Clock
	if clock == nil {
		clock = time.Now
	}
	copy := allocation.Clone()
	return executionport.Artifact{Leg: request.Leg, ValidatedQuote: quote, Allocation: &copy,
		Payload: append([]byte(nil), intent.Payload...), Metadata: copyMetadata(intent.Metadata), BuiltAt: clock().UTC()}, nil
}

var _ executionport.Validator = ValidatedQuoteBuilder{}
