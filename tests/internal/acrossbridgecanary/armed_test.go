package acrossbridgecanary_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	solanago "github.com/gagliardetto/solana-go"

	domainexecution "github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/internal/acrossbridgecanary"
)

func TestAcrossSolanaBridgeTransactionOptsAllowRPCRebroadcasts(t *testing.T) {
	t.Parallel()

	initial := acrossbridgecanary.AcrossSolanaBridgeTransactionOpts(false)
	retry := acrossbridgecanary.AcrossSolanaBridgeTransactionOpts(true)
	if initial.MaxRetries == nil ||
		*initial.MaxRetries != acrossbridgecanary.AcrossSolanaBridgeMaxRetries {
		t.Fatalf("initial max retries = %v", initial.MaxRetries)
	}
	if retry.MaxRetries == nil ||
		*retry.MaxRetries != acrossbridgecanary.AcrossSolanaBridgeMaxRetries {
		t.Fatalf("retry max retries = %v", retry.MaxRetries)
	}
	if initial.SkipPreflight || !retry.SkipPreflight {
		t.Fatalf(
			"unexpected preflight policy: initial=%t retry=%t",
			initial.SkipPreflight,
			retry.SkipPreflight,
		)
	}
}

func TestAbsentAcrossSolanaSourceIsRejectedOnlyAfterBlockhashExpiry(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		valid bool
		want  domainexecution.TechnicalState
	}{
		{name: "still-valid", valid: true, want: domainexecution.StateOutcomeUnknown},
		{name: "expired", valid: false, want: domainexecution.StateBroadcastRejected},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				var envelope struct {
					Method string `json:"method"`
				}
				if err := json.NewDecoder(request.Body).Decode(&envelope); err != nil {
					t.Error(err)
					return
				}
				writer.Header().Set("Content-Type", "application/json")
				switch envelope.Method {
				case "getSignatureStatuses":
					_, _ = writer.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":1},"value":[null]}}`))
				case "isBlockhashValid":
					_ = json.NewEncoder(writer).Encode(map[string]any{
						"jsonrpc": "2.0", "id": 1,
						"result": map[string]any{"context": map[string]any{"slot": 1}, "value": test.valid},
					})
				default:
					t.Errorf("unexpected RPC method %q", envelope.Method)
				}
			}))
			defer server.Close()

			state, err := acrossbridgecanary.ReconcileSolanaSource(
				context.Background(), server.URL,
				(solanago.Signature{}).String(), (solanago.Hash{}).String(),
			)
			if err != nil {
				t.Fatal(err)
			}
			if state != test.want {
				t.Fatalf("state=%s want=%s", state, test.want)
			}
		})
	}
}
