// Package local turns one deterministic local route quote into an executable
// adapter-owned intent.
package local

import (
	"context"
	"fmt"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type Intent struct {
	Payload  []byte
	Metadata map[string]string
}

type ExecutableQuote struct {
	Quote      market.Quote
	Allocation execution.RouteAllocation
}

// ExecutableSource performs one definitive local optimization and returns
// both its economic quote and the exact route allocation that produced it.
type ExecutableSource interface {
	QuoteExecutable(context.Context, quoteport.Input) (ExecutableQuote, error)
}

type IntentBuilder interface {
	BuildIntent(
		context.Context,
		execution.OperationID,
		execution.Leg,
		market.Quote,
		execution.RouteAllocation,
	) (Intent, error)
}

type Config struct {
	Source  ExecutableSource
	Builder IntentBuilder
	Clock   func() time.Time
}

type Validator struct {
	source  ExecutableSource
	builder IntentBuilder
	clock   func() time.Time
}

func New(config Config) (*Validator, error) {
	if config.Source == nil || config.Builder == nil || config.Clock == nil {
		return nil, fmt.Errorf("local validator requires quote source, intent builder, and clock")
	}
	return &Validator{source: config.Source, builder: config.Builder, clock: config.Clock}, nil
}

func (v *Validator) Validate(ctx context.Context, request executionport.ValidationRequest) (executionport.Artifact, error) {
	if err := request.Leg.Validate(); err != nil {
		return executionport.Artifact{}, err
	}
	if request.Operation == "" {
		return executionport.Artifact{}, fmt.Errorf("local validation requires operation identity")
	}
	metadata := request.Snapshot.Metadata()
	if metadata.Market != request.Leg.Market || metadata.Health != market.HealthHealthy {
		return executionport.Artifact{}, fmt.Errorf("local validation requires a healthy matching snapshot")
	}
	executable, err := v.source.QuoteExecutable(ctx, quoteport.Input{
		Snapshot: request.Snapshot, TokenIn: request.Leg.Input.Token(),
		TokenOut: request.Leg.ExpectedOutput.Token(), AmountIn: request.Leg.Input,
		Purpose: market.QuotePurposeLiveValidation, QuotedAt: v.clock().UTC(),
	})
	if err != nil {
		return executionport.Artifact{}, err
	}
	quote := executable.Quote
	if quote.AmountIn.Token() != request.Leg.Input.Token() ||
		quote.AmountIn.Units().Cmp(request.Leg.Input.Units()) != 0 {
		return executionport.Artifact{}, fmt.Errorf("local validation changed fixed input")
	}
	if err := executable.Allocation.Validate(); err != nil {
		return executionport.Artifact{}, fmt.Errorf("local executable allocation: %w", err)
	}
	if executable.Allocation.Input.Token() != quote.AmountIn.Token() ||
		executable.Allocation.Input.Units().Cmp(quote.AmountIn.Units()) != 0 ||
		executable.Allocation.ExpectedOutput.Token() != quote.AmountOut.Token() ||
		executable.Allocation.ExpectedOutput.Units().Cmp(quote.AmountOut.Units()) != 0 {
		return executionport.Artifact{}, fmt.Errorf("local executable allocation does not match quote")
	}
	intent, err := v.builder.BuildIntent(
		ctx, request.Operation, request.Leg, quote, executable.Allocation,
	)
	if err != nil {
		return executionport.Artifact{}, err
	}
	if len(intent.Payload) == 0 {
		return executionport.Artifact{}, fmt.Errorf("local intent builder returned an empty payload")
	}
	allocation := executable.Allocation.Clone()
	return executionport.Artifact{
		Leg: request.Leg, ValidatedQuote: quote, Payload: append([]byte(nil), intent.Payload...),
		Allocation: &allocation, Metadata: copyMetadata(intent.Metadata), BuiltAt: v.clock().UTC(),
	}, nil
}

func copyMetadata(input map[string]string) map[string]string {
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

var _ executionport.Validator = (*Validator)(nil)
