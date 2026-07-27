package zerox_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/VarozXYZ/vernier/adapters/quote/zerox"
)

const (
	sellToken = "0x1000000000000000000000000000000000000001"
	buyToken  = "0x2000000000000000000000000000000000000002"
	taker     = "0x3000000000000000000000000000000000000003"
)

type clientFunc func(*http.Request) (*http.Response, error)

func (f clientFunc) Do(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestPriceUsesRobinhoodV2AndParsesRoute(t *testing.T) {
	source, err := zerox.New(zerox.Config{
		BaseURL: "https://zerox.test",
		APIKey:  "test-key",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			if request.URL.Path != zerox.DefaultPricePath {
				t.Fatalf("path = %q", request.URL.Path)
			}
			if request.Header.Get("0x-api-key") != "test-key" || request.Header.Get("0x-version") != "v2" {
				t.Fatalf("unexpected 0x headers: %+v", request.Header)
			}
			query := request.URL.Query()
			if query.Get("chainId") != "4663" || query.Get("sellToken") != sellToken || query.Get("buyToken") != buyToken || query.Get("sellAmount") != "1000000" || query.Get("taker") != taker || query.Get("slippageBps") != "50" {
				t.Fatalf("unexpected query: %s", request.URL.RawQuery)
			}
			body := `{"blockNumber":"123","sellAmount":"1000000","buyAmount":"2500000000000000000","minBuyAmount":"2480000000000000000","sellToken":"` + sellToken + `","buyToken":"` + buyToken + `","liquidityAvailable":true,"route":{"fills":[{"from":"` + sellToken + `","to":"` + buyToken + `","source":"SyntheticV3","proportionBps":"10000"}]},"issues":{"simulationIncomplete":false}}`
			return response(http.StatusOK, body), nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := source.Price(context.Background(), request())
	if err != nil {
		t.Fatal(err)
	}
	if result.BuyAmount != "2500000000000000000" || result.SellAmount != "1000000" || len(result.Route) != 1 || result.Route[0].Source != "SyntheticV3" {
		t.Fatalf("unexpected price result: %+v", result)
	}
}

func TestFirmQuoteReturnsUnsignedTransactionAndIssues(t *testing.T) {
	source, err := zerox.New(zerox.Config{
		BaseURL: "https://zerox.test",
		APIKey:  "test-key",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			if request.URL.Path != zerox.DefaultQuotePath {
				t.Fatalf("path = %q", request.URL.Path)
			}
			body := `{"sellAmount":"1000000","buyAmount":"2490000000000000000","minBuyAmount":"2470000000000000000","sellToken":"` + sellToken + `","buyToken":"` + buyToken + `","liquidityAvailable":true,"issues":{"allowance":{"actual":"0","spender":"0x4000000000000000000000000000000000000004"},"balance":{"token":"` + sellToken + `","actual":"0","expected":"1000000"},"simulationIncomplete":false},"transaction":{"to":"0x5000000000000000000000000000000000000005","data":"0xabcdef","value":"0","gas":"250000","gasPrice":"1"}}`
			return response(http.StatusOK, body), nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := source.Quote(context.Background(), request())
	if err != nil {
		t.Fatal(err)
	}
	if result.Transaction.To != "0x5000000000000000000000000000000000000005" || result.Transaction.Data != "0xabcdef" || result.Issues.AllowanceSpender == "" || result.Issues.BalanceExpected != "1000000" {
		t.Fatalf("unexpected firm quote: %+v", result)
	}
}

func TestQuoteReportsRateLimit(t *testing.T) {
	source, err := zerox.New(zerox.Config{
		BaseURL: "https://zerox.test",
		APIKey:  "test-key",
		Client: clientFunc(func(*http.Request) (*http.Response, error) {
			return response(http.StatusTooManyRequests, `{"name":"RATE_LIMIT_EXCEEDED","message":"too many requests"}`), nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.Quote(context.Background(), request())
	apiErr, ok := err.(*zerox.APIError)
	if !ok || !apiErr.RateLimited() {
		t.Fatalf("error = %T %v, want rate-limited APIError", err, err)
	}
}

func request() zerox.Request {
	return zerox.Request{
		ChainID:     zerox.RobinhoodChainID,
		SellToken:   sellToken,
		BuyToken:    buyToken,
		SellAmount:  "1000000",
		Taker:       taker,
		SlippageBPS: 50,
	}
}

func response(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
