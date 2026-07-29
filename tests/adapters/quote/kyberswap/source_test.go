package kyberswap_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/quote/kyberswap"
)

const (
	tokenIn  = "0x1000000000000000000000000000000000000001"
	tokenOut = "0x2000000000000000000000000000000000000002"
	taker    = "0x3000000000000000000000000000000000000003"
	router   = "0x4000000000000000000000000000000000000004"
	pool     = "0x5000000000000000000000000000000000000005"
)

type clientFunc func(*http.Request) (*http.Response, error)

func (f clientFunc) Do(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestRouteUsesRobinhoodV1AndParsesBestRoute(t *testing.T) {
	source, err := kyberswap.New(kyberswap.Config{
		BaseURL:  "https://kyberswap.test",
		ClientID: "vernier-test",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method != http.MethodGet || request.URL.Path != "/robinhood"+kyberswap.DefaultRoutePath {
				t.Fatalf("request = %s %s", request.Method, request.URL.Path)
			}
			if request.Header.Get("x-client-id") != "vernier-test" {
				t.Fatalf("x-client-id = %q", request.Header.Get("x-client-id"))
			}
			query := request.URL.Query()
			if query.Get("tokenIn") != tokenIn || query.Get("tokenOut") != tokenOut || query.Get("amountIn") != "1000000" || query.Get("origin") != taker {
				t.Fatalf("unexpected query: %s", request.URL.RawQuery)
			}
			return response(http.StatusOK, routeResponse()), nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := source.Route(context.Background(), routeRequest())
	if err != nil {
		t.Fatal(err)
	}
	if result.AmountIn != "1000000" || result.AmountOut != "2500000000000000000" || result.RouterAddress != router {
		t.Fatalf("unexpected route result: %+v", result)
	}
	if len(result.Paths) != 1 || len(result.Paths[0]) != 1 || result.Paths[0][0].Exchange != "synthetic-v3" {
		t.Fatalf("unexpected paths: %+v", result.Paths)
	}
}

func TestBuildReturnsUnsignedTransactionData(t *testing.T) {
	requests := 0
	source, err := kyberswap.New(kyberswap.Config{
		BaseURL:  "https://kyberswap.test",
		ClientID: "vernier-test",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			switch request.URL.Path {
			case "/robinhood" + kyberswap.DefaultRoutePath:
				return response(http.StatusOK, routeResponse()), nil
			case "/robinhood" + kyberswap.DefaultBuildPath:
				if request.Method != http.MethodPost || request.Header.Get("Content-Type") != "application/json" {
					t.Fatalf("unexpected build request: %s headers=%v", request.Method, request.Header)
				}
				body, readErr := io.ReadAll(request.Body)
				if readErr != nil {
					t.Fatal(readErr)
				}
				var payload struct {
					RouteSummary json.RawMessage `json:"routeSummary"`
					Sender       string          `json:"sender"`
					Recipient    string          `json:"recipient"`
					Slippage     uint16          `json:"slippageTolerance"`
					GasEstimate  bool            `json:"enableGasEstimation"`
					Source       string          `json:"source"`
				}
				if err := json.Unmarshal(body, &payload); err != nil {
					t.Fatal(err)
				}
				if len(payload.RouteSummary) == 0 || payload.Sender != taker ||
					payload.Recipient != taker || payload.Slippage != 50 ||
					!payload.GasEstimate || payload.Source != "vernier-test" {
					t.Fatalf("unexpected build payload: %s", body)
				}
				build := `{"code":0,"message":"successfully","data":{"amountIn":"1000000","amountOut":"2490000000000000000","gas":"250000","outputChange":{"amount":"42","percent":0.1},"routerAddress":"` + router + `","data":"0xabcdef","transactionValue":"0"}}`
				return response(http.StatusOK, build), nil
			default:
				t.Fatalf("unexpected path: %s", request.URL.Path)
				return nil, nil
			}
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	route, err := source.Route(context.Background(), routeRequest())
	if err != nil {
		t.Fatal(err)
	}
	result, err := source.Build(context.Background(), kyberswap.BuildRequest{
		Route:               route,
		Sender:              taker,
		Recipient:           taker,
		Origin:              taker,
		SlippageBPS:         50,
		EnableGasEstimation: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 || result.RouterAddress != router || result.Data != "0xabcdef" ||
		result.AmountOut != "2490000000000000000" ||
		result.OutputChange != `{"amount":"42","percent":0.1}` {
		t.Fatalf("unexpected build result: requests=%d result=%+v", requests, result)
	}
}

func TestRouteReportsRateLimit(t *testing.T) {
	source, err := kyberswap.New(kyberswap.Config{
		BaseURL:  "https://kyberswap.test",
		ClientID: "vernier-test",
		Client: clientFunc(func(*http.Request) (*http.Response, error) {
			result := response(http.StatusTooManyRequests, `{"code":4290,"message":"too many requests"}`)
			result.Header = http.Header{"Retry-After": []string{"2"}}
			return result, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.Route(context.Background(), routeRequest())
	apiErr, ok := err.(*kyberswap.APIError)
	if !ok || !apiErr.RateLimited() || apiErr.Code != "4290" || apiErr.RetryAfter() != 2*time.Second {
		t.Fatalf("error = %T %v, want rate-limited APIError", err, err)
	}
}

func routeRequest() kyberswap.RouteRequest {
	return kyberswap.RouteRequest{
		Chain:    kyberswap.DefaultChain,
		TokenIn:  tokenIn,
		TokenOut: tokenOut,
		AmountIn: "1000000",
		Origin:   taker,
	}
}

func routeResponse() string {
	return `{"code":0,"message":"successfully","data":{"routerAddress":"` + router + `","routeSummary":{"tokenIn":"` + tokenIn + `","amountIn":"1000000","amountInUsd":"1","tokenOut":"` + tokenOut + `","amountOut":"2500000000000000000","amountOutUsd":"1","gas":"200000","gasPrice":"1","gasUsd":"0.01","routeID":"synthetic-route","route":[[{"pool":"` + pool + `","tokenIn":"` + tokenIn + `","tokenOut":"` + tokenOut + `","swapAmount":"1000000","amountOut":"2500000000000000000","exchange":"synthetic-v3","poolType":"v3"}]]}}}`
}

func response(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
