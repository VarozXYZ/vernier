package local

import (
	"context"
	"fmt"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

// DiscoveryQuoteBuilder builds a local intent from a fresh external/on-chain
// recovery quote. Unlike ValidatedQuoteBuilder it is not tied to a Research
// snapshot, so it is suitable only for recovery after a partial fill.
type DiscoveryQuoteBuilder struct {
	Builder IntentBuilder
	Clock   func() time.Time
}

func (v DiscoveryQuoteBuilder) Validate(ctx context.Context, request executionport.ValidationRequest) (executionport.Artifact, error) {
	if v.Builder == nil || request.Operation == "" {
		return executionport.Artifact{}, fmt.Errorf("local recovery quote builder configuration is incomplete")
	}
	quote := request.Discovery
	if quote.Market != request.Leg.Market || quote.Mode != market.QuoteModeExactInput ||
		quote.AmountIn.Token() != request.Leg.Input.Token() ||
		quote.AmountIn.Units().Cmp(request.Leg.Input.Units()) != 0 ||
		quote.AmountOut.Token() != request.Leg.ExpectedOutput.Token() {
		return executionport.Artifact{}, fmt.Errorf("fresh local recovery quote does not match the fixed leg")
	}
	allocation := execution.RouteAllocation{Input: quote.AmountIn, ExpectedOutput: quote.AmountOut,
		Groups: []execution.RouteGroup{{ID: "local_recovery", InputToken: quote.AmountIn.Token(), OutputToken: quote.AmountOut.Token(),
			Branches: []execution.RouteBranch{{Market: quote.Market, PlannedInput: quote.AmountIn.Units(), ExpectedOutput: quote.AmountOut.Units()}}}}}
	intent, err := v.Builder.BuildIntent(ctx, request.Operation, request.Leg, quote, allocation)
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

var _ executionport.Validator = DiscoveryQuoteBuilder{}
