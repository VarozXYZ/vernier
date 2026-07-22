package jupiter_test

import (
	"context"
	"io"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/quote/jupiter"
	"github.com/VarozXYZ/vernier/domain/market"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type localSource struct{}

func (localSource) ID() market.SourceID { return "local" }
func (localSource) LastTiming() quoteport.Timing {
	return quoteport.Timing{Cached: true, Hops: []quoteport.HopTiming{{Market: "hop", AmountIn: "100", AmountOut: "90"}}}
}
func (localSource) Quote(_ context.Context, input quoteport.Input) (market.Quote, error) {
	out, _ := market.NewTokenAmount(input.TokenOut, big.NewInt(90))
	return market.NewQuote(market.Quote{Source: "local", Market: "pool", SnapshotVersion: 1, Purpose: input.Purpose, Mode: market.QuoteModeExactInput, AmountIn: input.AmountIn, AmountOut: out, QuotedAt: input.QuotedAt})
}

type clientFunc func(*http.Request) (*http.Response, error)

func (f clientFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

func TestJupiterReturnsLocalQuoteAndRouteEvidence(t *testing.T) {
	client := clientFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/swap/v2/build" || request.URL.Query().Get("amount") != "100" || request.URL.Query().Get("taker") != "public-taker" {
			t.Fatalf("unexpected request %s", request.URL.String())
		}
		body := `{"outAmount":"95","contextSlot":123,"routePlan":[{"swapInfo":{"ammKey":"pool","label":"Meteora","inputMint":"in","outputMint":"out","inAmount":"100","outAmount":"95"}}]}`
		return &http.Response{StatusCode: 200, Status: "200 OK", Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	source, err := jupiter.New(jupiter.Config{ID: "jupiter", BaseURL: "https://jupiter.test", Taker: "public-taker", TokenMints: map[market.TokenID]string{"in": "mint-in", "out": "mint-out"}, Local: localSource{}, Client: client, Clock: func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }})
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewTokenAmount("in", big.NewInt(100))
	result, err := source.QuoteWithReference(context.Background(), quoteport.Input{TokenIn: "in", TokenOut: "out", AmountIn: amount, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if result.Local.AmountOut.Units().Cmp(big.NewInt(90)) != 0 || result.Evidence.Status != quoteport.ReferenceAvailable || result.Evidence.AmountOut.Units().Cmp(big.NewInt(95)) != 0 || result.Evidence.ContextSlot != 123 || len(result.Evidence.Route) != 1 {
		t.Fatalf("unexpected result %+v", result)
	}
	trace := source.LastTiming()
	if !trace.Cached || len(trace.Hops) != 1 || trace.Hops[0].Market != "hop" {
		t.Fatalf("jupiter wrapper hid local timing: %+v", trace)
	}
}

func TestJupiterFailureDoesNotHideLocalQuote(t *testing.T) {
	source, err := jupiter.New(jupiter.Config{ID: "jupiter", BaseURL: "https://jupiter.test", Taker: "public-taker", TokenMints: map[market.TokenID]string{"in": "mint-in", "out": "mint-out"}, Local: localSource{}, Client: clientFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 503, Status: "503 Service Unavailable", Body: io.NopCloser(strings.NewReader("down"))}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewTokenAmount("in", big.NewInt(100))
	result, err := source.QuoteWithReference(context.Background(), quoteport.Input{TokenIn: "in", TokenOut: "out", AmountIn: amount, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()})
	if err != nil || result.Local.AmountOut.IsZero() || result.Evidence.Status != quoteport.ReferenceUnavailable || result.Evidence.Error == "" {
		t.Fatalf("unexpected failure result %+v err=%v", result, err)
	}
}

func TestJupiterReferenceUsesProvidedLocalQuote(t *testing.T) {
	localCalls := 0
	local := localSourceFunc(func(_ context.Context, input quoteport.Input) (market.Quote, error) {
		localCalls++
		out, _ := market.NewTokenAmount(input.TokenOut, big.NewInt(90))
		return market.NewQuote(market.Quote{Source: "local", Market: "pool", SnapshotVersion: 1, Purpose: input.Purpose, Mode: market.QuoteModeExactInput, AmountIn: input.AmountIn, AmountOut: out, QuotedAt: input.QuotedAt})
	})
	source, err := jupiter.New(jupiter.Config{ID: "jupiter", BaseURL: "https://jupiter.test", Taker: "public-taker", TokenMints: map[market.TokenID]string{"in": "mint-in", "out": "mint-out"}, Local: local, Client: clientFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"outAmount":"95"}`))}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := market.NewTokenAmount("in", big.NewInt(100))
	localQuote, _ := market.NewQuote(market.Quote{Source: "local", Market: "pool", SnapshotVersion: 1, Purpose: market.QuotePurposeResearchDiscovery, Mode: market.QuoteModeExactInput, AmountIn: amount, AmountOut: mustAmount(t, "out", "90"), QuotedAt: time.Now()})
	evidence, err := source.Reference(context.Background(), quoteport.Input{TokenIn: "in", TokenOut: "out", AmountIn: amount, Purpose: market.QuotePurposeResearchDiscovery, QuotedAt: time.Now()}, localQuote)
	if err != nil || evidence.Status != quoteport.ReferenceAvailable || evidence.AmountOut.Units().Cmp(big.NewInt(95)) != 0 {
		t.Fatalf("unexpected reference: %+v err=%v", evidence, err)
	}
	if localCalls != 0 {
		t.Fatalf("external validation recalculated local quote %d times", localCalls)
	}
}

type localSourceFunc func(context.Context, quoteport.Input) (market.Quote, error)

func (f localSourceFunc) ID() market.SourceID { return "local" }
func (f localSourceFunc) Quote(ctx context.Context, input quoteport.Input) (market.Quote, error) {
	return f(ctx, input)
}

func mustAmount(t *testing.T, token market.TokenID, value string) market.TokenAmount {
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
