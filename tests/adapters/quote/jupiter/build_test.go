package jupiter_test

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/quote/jupiter"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

func TestBuildSourceValidatesFixedExactInputWithoutFastMode(t *testing.T) {
	var receivedKey string
	var receivedMaxAccounts []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedKey = request.Header.Get("x-api-key")
		query := request.URL.Query()
		receivedMaxAccounts = append(receivedMaxAccounts, query.Get("maxAccounts"))
		if query.Get("amount") != "1000000" || query.Get("mode") != "" {
			t.Errorf("unexpected build query: %s", request.URL.RawQuery)
		}
		blockhash := make([]byte, 32)
		for index := range blockhash {
			blockhash[index] = byte(index + 1)
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"inputMint": "quote-mint", "outputMint": "base-mint",
			"inAmount": "1000000", "outAmount": "14550", "contextSlot": 42,
			"swapInstruction": map[string]any{
				"programId": "11111111111111111111111111111111", "accounts": []any{}, "data": "",
			},
			"tipInstruction": map[string]any{
				"programId": "11111111111111111111111111111111", "accounts": []any{}, "data": "",
			},
			"addressesByLookupTableAddress": map[string]any{},
			"blockhashWithMetadata": map[string]any{
				"blockhash": blockhash, "lastValidBlockHeight": 500,
			},
		})
	}))
	defer server.Close()
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	source, err := jupiter.NewBuildSource(jupiter.BuildConfig{
		ID: "jupiter-build", BaseURL: server.URL, Taker: "public-wallet",
		APIKeys:     []string{"key-a", "key-b"},
		TokenMints:  map[market.TokenID]string{"quote": "quote-mint", "base": "base-mint"},
		SlippageBPS: 25, MaxAccounts: 64, TipAmount: "200000",
		ComputePricePercentile: "75", BlockhashSlotsToExpiry: 20,
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	input, _ := market.NewTokenAmount("quote", big.NewInt(1_000_000))
	output, _ := market.NewTokenAmount("base", big.NewInt(14_500))
	discovery, _ := market.NewQuote(market.Quote{
		Source: "jupiter-quote", Market: "remote-market", SnapshotVersion: 7,
		Purpose: market.QuotePurposeLiveDiscovery, Mode: market.QuoteModeExactInput,
		Quality: market.QuoteQualityExact, AmountIn: input, AmountOut: output, QuotedAt: now,
	})
	leg := execution.Leg{
		ID: "buy", Side: execution.LegBuy, Chain: "solana", Account: "wallet",
		Market: "remote-market", Input: input, ExpectedOutput: output,
	}
	artifact, err := source.Validate(context.Background(), executionport.ValidationRequest{
		Leg: leg, Discovery: discovery, RequestedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if receivedKey != "key-a" {
		t.Fatalf("API key = %q", receivedKey)
	}
	if artifact.Metadata["max_accounts"] != "64" {
		t.Fatalf("initial max_accounts = %q", artifact.Metadata["max_accounts"])
	}
	if artifact.ValidatedQuote.AmountIn.String() != "1000000" ||
		artifact.ValidatedQuote.AmountOut.String() != "14550" {
		t.Fatalf("validated amounts = %s -> %s", artifact.ValidatedQuote.AmountIn, artifact.ValidatedQuote.AmountOut)
	}
	if artifact.LastValidBlockHeight != 500 || artifact.Blockhash == "" || len(artifact.Payload) == 0 {
		t.Fatalf("incomplete artifact: %+v", artifact)
	}
	compact, err := source.ValidateCompact(
		context.Background(),
		executionport.ValidationRequest{
			Leg: leg, Discovery: discovery, RequestedAt: now,
		},
		artifact,
	)
	if err != nil {
		t.Fatal(err)
	}
	if compact.Metadata["max_accounts"] != "48" ||
		len(receivedMaxAccounts) != 2 ||
		receivedMaxAccounts[0] != "64" ||
		receivedMaxAccounts[1] != "48" {
		t.Fatalf(
			"compact limits = %v metadata=%q",
			receivedMaxAccounts,
			compact.Metadata["max_accounts"],
		)
	}
}

func TestBuildSourceExposesRateLimitWithoutFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusTooManyRequests)
		_, _ = writer.Write([]byte(`{"error":"too many requests"}`))
	}))
	defer server.Close()
	source, err := jupiter.NewBuildSource(jupiter.BuildConfig{
		ID: "jupiter-build", BaseURL: server.URL, Taker: "public-wallet", APIKeys: []string{"key"},
		TokenMints: map[market.TokenID]string{"quote": "quote-mint", "base": "base-mint"},
		TipAmount:  "1", ComputePricePercentile: "50",
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	input, _ := market.NewTokenAmount("quote", big.NewInt(1))
	output, _ := market.NewTokenAmount("base", big.NewInt(1))
	discovery, _ := market.NewQuote(market.Quote{
		Source: "quote", Market: "market", SnapshotVersion: 1,
		Purpose: market.QuotePurposeLiveDiscovery, Mode: market.QuoteModeExactInput,
		Quality: market.QuoteQualityExact, AmountIn: input, AmountOut: output, QuotedAt: now,
	})
	_, err = source.Validate(context.Background(), executionport.ValidationRequest{
		Leg: execution.Leg{
			ID: "buy", Side: execution.LegBuy, Chain: "solana", Account: "wallet",
			Market: "market", Input: input, ExpectedOutput: output,
		},
		Discovery: discovery, RequestedAt: now,
	})
	var apiErr *jupiter.APIError
	if !errors.As(err, &apiErr) || !apiErr.RateLimited() || apiErr.Operation != "build" {
		t.Fatalf("error = %#v", err)
	}
}
