package velora_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/VarozXYZ/vernier/adapters/quote/velora"
)

const (
	sourceToken = "0x1000000000000000000000000000000000000001"
	destToken   = "0x2000000000000000000000000000000000000002"
	userAddress = "0x3000000000000000000000000000000000000003"
	router      = "0x4000000000000000000000000000000000000004"
)

type clientFunc func(*http.Request) (*http.Response, error)

func (f clientFunc) Do(request *http.Request) (*http.Response, error) { return f(request) }

func TestPriceRequestsV62ExactInputRoute(t *testing.T) {
	source := newSource(t, func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet || request.URL.Path != "/prices" {
			t.Fatalf("request = %s %s", request.Method, request.URL.Path)
		}
		query := request.URL.Query()
		if query.Get("network") != "56" || query.Get("side") != "SELL" || query.Get("version") != velora.Version ||
			query.Get("srcToken") != sourceToken || query.Get("destToken") != destToken ||
			query.Get("srcDecimals") != "6" || query.Get("destDecimals") != "18" ||
			query.Get("amount") != "125000000" || query.Get("userAddress") != userAddress ||
			query.Get("partner") != "synthetic-client" {
			t.Fatalf("unexpected query: %s", request.URL.RawQuery)
		}
		return response(http.StatusOK, priceEnvelope()), nil
	})

	result, err := source.Price(context.Background(), priceRequest())
	if err != nil {
		t.Fatal(err)
	}
	if result.SourceAmount != "125000000" || result.DestAmount != "250000000000000000000" || result.Contract != router || len(result.PriceRoute) == 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestTransactionPassesPriceRouteVerbatimAndReturnsUnsignedCalldata(t *testing.T) {
	requests := 0
	source := newSource(t, func(request *http.Request) (*http.Response, error) {
		requests++
		if request.URL.Path == "/prices" {
			return response(http.StatusOK, priceEnvelope()), nil
		}
		if request.Method != http.MethodPost || request.URL.Path != "/transactions/56" || request.URL.Query().Get("ignoreChecks") != "true" {
			t.Fatalf("request = %s %s?%s", request.Method, request.URL.Path, request.URL.RawQuery)
		}
		var payload struct {
			PriceRoute   json.RawMessage `json:"priceRoute"`
			SourceToken  string          `json:"srcToken"`
			DestToken    string          `json:"destToken"`
			UserAddress  string          `json:"userAddress"`
			SourceAmount string          `json:"srcAmount"`
			DestAmount   *string         `json:"destAmount"`
			Slippage     uint16          `json:"slippage"`
		}
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if len(payload.PriceRoute) == 0 || payload.SourceToken != sourceToken || payload.DestToken != destToken || payload.UserAddress != userAddress || payload.SourceAmount != "125000000" || payload.DestAmount != nil || payload.Slippage != 50 {
			t.Fatalf("unexpected payload: %+v", payload)
		}
		return response(http.StatusOK, transactionJSON()), nil
	})
	price, err := source.Price(context.Background(), priceRequest())
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := source.Transaction(context.Background(), velora.TransactionRequest{Price: price, UserAddress: userAddress, SlippageBPS: 50, Partner: "synthetic-client", IgnoreChecks: true})
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 || transaction.To != router || transaction.Data != "0xabcdef" || transaction.ChainID != 56 {
		t.Fatalf("requests=%d transaction=%+v", requests, transaction)
	}
}

func TestSwapReturnsRouteAndTransactionFromOneRequest(t *testing.T) {
	source := newSource(t, func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/swap" || request.URL.Query().Get("slippage") != "50" || request.URL.Query().Get("version") != velora.Version {
			t.Fatalf("unexpected request: %s?%s", request.URL.Path, request.URL.RawQuery)
		}
		return response(http.StatusOK, `{"priceRoute":`+priceRoute()+`,"txParams":`+transactionJSON()+`}`), nil
	})
	result, err := source.Swap(context.Background(), velora.SwapRequest{PriceRequest: priceRequest(), SlippageBPS: 50})
	if err != nil {
		t.Fatal(err)
	}
	if result.Price.DestAmount != "250000000000000000000" || result.Transaction.Data != "0xabcdef" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestPriceRejectsMismatchedResponse(t *testing.T) {
	source := newSource(t, func(*http.Request) (*http.Response, error) {
		return response(http.StatusOK, strings.Replace(priceEnvelope(), `"srcAmount":"125000000"`, `"srcAmount":"1"`, 1)), nil
	})
	if _, err := source.Price(context.Background(), priceRequest()); err == nil {
		t.Fatal("expected mismatched response error")
	}
}

func newSource(t *testing.T, client clientFunc) *velora.Source {
	t.Helper()
	source, err := velora.New(velora.Config{BaseURL: "https://velora.test", Client: client})
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func priceRequest() velora.PriceRequest {
	return velora.PriceRequest{Network: 56, SourceToken: sourceToken, SourceUnits: 6, DestToken: destToken, DestUnits: 18, Amount: "125000000", UserAddress: userAddress, Partner: "synthetic-client"}
}

func priceEnvelope() string { return `{"priceRoute":` + priceRoute() + `}` }
func priceRoute() string {
	return `{"blockNumber":123,"network":56,"srcToken":"` + sourceToken + `","srcDecimals":6,"srcAmount":"125000000","destToken":"` + destToken + `","destDecimals":18,"destAmount":"250000000000000000000","bestRoute":[{}],"gasCost":"180000","gasCostUSD":"0.01","version":"6.2","contractAddress":"` + router + `","contractMethod":"swapExactAmountIn","hmac":"synthetic"}`
}
func transactionJSON() string {
	return `{"from":"` + userAddress + `","to":"` + router + `","value":"0","data":"0xabcdef","gasPrice":"1","gas":"180000","chainId":56}`
}
func response(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}
}
