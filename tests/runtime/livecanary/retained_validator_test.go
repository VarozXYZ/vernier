package livecanary_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	"github.com/VarozXYZ/vernier/runtime/livecanary"
	"github.com/ethereum/go-ethereum/common"
)

type retainedSource struct {
	artifact executionport.Artifact
	expected market.Quote
	used     bool
}

func (s *retainedSource) TakeExecutableArtifact(quote market.Quote) (executionport.Artifact, bool) {
	if s.used || (s.expected.Market != "" &&
		(quote.QuotedAt != s.expected.QuotedAt ||
			quote.AmountOut.Units().Cmp(s.expected.AmountOut.Units()) != 0)) {
		return executionport.Artifact{}, false
	}
	s.used = true
	return s.artifact, true
}

func TestRetainedValidatorConsumesExactAllowlistedBuildOnce(t *testing.T) {
	now := time.Now().UTC()
	in, _ := market.NewTokenAmount("quote", big.NewInt(1_000_000))
	discoveryOut, _ := market.NewTokenAmount("base", big.NewInt(2_000_000))
	buildOut, _ := market.NewTokenAmount("base", big.NewInt(1_999_000))
	discovery, _ := market.NewQuote(market.Quote{Source: "remote", Market: "remote-market", SnapshotVersion: 1,
		Purpose: market.QuotePurposeLiveValidation, Mode: market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
		AmountIn: in, AmountOut: discoveryOut, QuotedAt: now})
	validated, _ := market.NewQuote(market.Quote{Source: "remote", Market: "remote-market", SnapshotVersion: 1,
		Purpose: market.QuotePurposeLiveValidation, Mode: market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
		AmountIn: in, AmountOut: buildOut, QuotedAt: now.Add(time.Millisecond)})
	router := common.HexToAddress("0x0000000000000000000000000000000000000011")
	source := &retainedSource{expected: discovery, artifact: executionport.Artifact{ValidatedQuote: validated, Payload: []byte{1},
		Metadata: map[string]string{"to": router.Hex()}, BuiltAt: now}}
	validator := livecanary.RetainedArtifactValidator{Source: source, AllowedDestinations: []common.Address{router}}
	leg := execution.Leg{ID: "buy", Side: execution.LegBuy, Chain: "chain", Account: "account",
		Market: discovery.Market, Input: in, ExpectedOutput: discoveryOut}
	request := executionport.ValidationRequest{Operation: "operation", Leg: leg, Discovery: discovery, RequestedAt: now}
	artifact, err := validator.Validate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Leg.Account != leg.Account || artifact.ValidatedQuote.AmountOut.Units().Cmp(buildOut.Units()) != 0 {
		t.Fatal("retained artifact was not retargeted without economic mutation")
	}
	if _, err := validator.Validate(context.Background(), request); err == nil {
		t.Fatal("expected one-shot retained build")
	}
}

func TestRetainedValidatorRejectsUnallowlistedDestination(t *testing.T) {
	now := time.Now().UTC()
	in, _ := market.NewTokenAmount("quote", big.NewInt(1))
	out, _ := market.NewTokenAmount("base", big.NewInt(2))
	quote, _ := market.NewQuote(market.Quote{Source: "remote", Market: "remote-market", SnapshotVersion: 1,
		Purpose: market.QuotePurposeLiveValidation, Mode: market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
		AmountIn: in, AmountOut: out, QuotedAt: now})
	source := &retainedSource{artifact: executionport.Artifact{ValidatedQuote: quote, Payload: []byte{1},
		Metadata: map[string]string{"to": "0x0000000000000000000000000000000000000022"}, BuiltAt: now}}
	validator := livecanary.RetainedArtifactValidator{Source: source,
		AllowedDestinations: []common.Address{common.HexToAddress("0x0000000000000000000000000000000000000011")}}
	leg := execution.Leg{ID: "buy", Side: execution.LegBuy, Chain: "chain", Account: "account",
		Market: quote.Market, Input: in, ExpectedOutput: out}
	if _, err := validator.Validate(context.Background(), executionport.ValidationRequest{
		Operation: "operation", Leg: leg, Discovery: quote, RequestedAt: now}); err == nil {
		t.Fatal("expected destination allowlist rejection")
	}
}
