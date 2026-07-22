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
