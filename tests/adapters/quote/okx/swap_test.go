package okx_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/VarozXYZ/vernier/adapters/quote/okx"
)

type instructionClientFunc func(*http.Request) (*http.Response, error)

func (f instructionClientFunc) Do(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestSwapInstructionBuildsSolanaInstructionRequest(t *testing.T) {
	limiter, err := okx.NewSpacingLimiter(0)
	if err != nil {
		t.Fatal(err)
	}
	source, err := okx.New(okx.Config{
		ID:         "okx-instruction-test",
		APIKey:     "key",
		SecretKey:  "secret",
		Passphrase: "pass",
		ChainIndex: okx.SolanaChainIndex,
		Limiter:    limiter,
		Client: instructionClientFunc(func(request *http.Request) (*http.Response, error) {
			if request.URL.Path != okx.DefaultSwapInstructionPath {
				t.Fatalf("path = %q", request.URL.Path)
			}
			query := request.URL.Query()
			if query.Get("chainIndex") != "501" || query.Get("amount") != "1000000" || query.Get("slippagePercent") != "0.5" || query.Get("userWalletAddress") != "wallet" {
				t.Fatalf("unexpected query: %s", request.URL.RawQuery)
			}
			body := `{"code":"0","data":{"instructionLists":[{"programId":"one"},{"programId":"two"}],"addressLookupTableAccount":["table"]}}`
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(body))}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := source.SwapInstruction(context.Background(), okx.SwapInstructionRequest{
		Amount:            "1000000",
		FromTokenAddress:  "from",
		ToTokenAddress:    "to",
		Slippage:          "0.5",
		UserWalletAddress: "wallet",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.InstructionCount != 2 || result.AddressLookupTableCount != 1 || result.HTTPStatus != http.StatusOK {
		t.Fatalf("unexpected instruction result: %+v", result)
	}
}
