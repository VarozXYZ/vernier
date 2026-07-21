package coingecko_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/price/coingecko"
	priceport "github.com/VarozXYZ/vernier/ports/price"
)

func TestSourceReturnsExactPriceEvidence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/simple/price" || request.URL.Query().Get("ids") != "weth" ||
			request.URL.Query().Get("vs_currencies") != "usd" || request.Header.Get("x-cg-demo-api-key") != "secret" {
			t.Fatalf("unexpected request: %s", request.URL.String())
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"weth":{"usd":2.34567890123456789e3,"last_updated_at":1700000000}}`))
	}))
	defer server.Close()
	source, err := coingecko.New(coingecko.Config{
		ID: "coingecko/weth-usd", Base: "weth", Quote: "usd", CoinID: "weth", Currency: "usd",
		BaseURL: server.URL, APIKey: "secret", APIKeyHeader: "x-cg-demo-api-key", Client: server.Client(),
		Clock: func() time.Time { return time.Unix(1700000001, 0) },
	})
	if err != nil {
		t.Fatal(err)
	}
	observation, err := source.Observe(context.Background(), priceport.Request{Base: "weth", Quote: "usd"})
	if err != nil {
		t.Fatal(err)
	}
	if observation.Value().RatString() != "234567890123456789/100000000000000" ||
		observation.Reference() != "coingecko:coin/weth" || observation.SourceUpdatedAt() != time.Unix(1700000000, 0).UTC() {
		t.Fatalf("unexpected observation: %s %s %s", observation.Value(), observation.Reference(), observation.SourceUpdatedAt())
	}
}

func TestSourceRejectsMissingOrInvalidEvidence(t *testing.T) {
	for _, body := range []string{`{}`, `{"weth":{"usd":0,"last_updated_at":1700000000}}`, `{"weth":{"usd":1,"last_updated_at":0}}`} {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { _, _ = writer.Write([]byte(body)) }))
		source, err := coingecko.New(coingecko.Config{ID: "source", Base: "weth", Quote: "usd", CoinID: "weth", Currency: "usd", BaseURL: server.URL, Client: server.Client()})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := source.Observe(context.Background(), priceport.Request{Base: "weth", Quote: "usd"}); err == nil {
			t.Fatalf("invalid response was accepted: %s", body)
		}
		server.Close()
	}
}
