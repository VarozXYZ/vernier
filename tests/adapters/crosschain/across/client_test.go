package across_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/crosschain/across"
)

func TestApprovalRequestsFreshExactInputArtifact(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls++
		if request.URL.Path != "/api/swap/approval" {
			t.Fatalf("unexpected path %q", request.URL.Path)
		}
		if request.Header.Get("Authorization") != "Bearer synthetic-key" {
			t.Fatalf("missing bearer authentication")
		}
		query := request.URL.Query()
		expected := map[string]string{
			"tradeType": "exactInput", "strictTradeType": "true",
			"amount": "1000000", "originChainId": "137",
			"destinationChainId": "34268394551451",
			"depositor":          "0x1111111111111111111111111111111111111111",
			"recipient":          "SyntheticSolanaRecipient", "refundOnOrigin": "true",
			"integratorId": "0xbeef",
		}
		for key, value := range expected {
			if query.Get(key) != value {
				t.Fatalf("unexpected %s: %q", key, query.Get(key))
			}
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"id": "synthetic-approval", "crossSwapType": "bridgeableToBridgeable",
			"amountType": "exactInput", "inputAmount": "1000000", "maxInputAmount": "1000000",
			"expectedOutputAmount": "999500", "minOutputAmount": "999000",
			"expectedFillTime": 2, "quoteExpiryTimestamp": now.Add(time.Minute).Unix(),
			"steps":        map[string]any{"bridge": map[string]any{"provider": "across"}},
			"approvalTxns": []any{},
			"swapTx": map[string]any{
				"simulationSuccess": true, "chainId": 137,
				"to":   "0x2222222222222222222222222222222222222222",
				"data": "0x1234", "value": "0", "gas": "250000",
			},
		})
	}))
	defer server.Close()

	client, err := across.New(across.Config{
		BaseURL: server.URL + "/api", APIKey: "synthetic-key", IntegratorID: "0xBEEF",
		Client: server.Client(), Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	request := across.ApprovalRequest{
		OriginChainID: 137, DestinationChainID: across.SolanaChainID,
		InputToken:  "0x3333333333333333333333333333333333333333",
		OutputToken: "SyntheticUSDCMint", Amount: "1000000",
		Depositor: "0x1111111111111111111111111111111111111111",
		Recipient: "SyntheticSolanaRecipient", Slippage: "auto",
	}
	first, err := client.Approval(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.Approval(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("approval artifacts must never be cached, calls=%d", calls)
	}
	if first.InputAmount != "1000000" || first.ExpectedOutputAmount != "999500" ||
		first.SwapTransaction.ChainID != 137 || first.ResponseHash == "" ||
		second.ResponseHash != first.ResponseHash {
		t.Fatalf("unexpected decoded approval: %+v", first)
	}
}

func TestApprovalRejectsExpiredOrMismatchedArtifact(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"id": "expired", "crossSwapType": "bridgeableToBridgeable", "amountType": "exactInput",
			"inputAmount": "999999", "expectedOutputAmount": "999500", "minOutputAmount": "999000",
			"expectedFillTime": 2, "quoteExpiryTimestamp": now.Add(-time.Second).Unix(),
			"swapTx": map[string]any{
				"simulationSuccess": true, "chainId": 137,
				"to": "0x2222222222222222222222222222222222222222", "data": "0x1234",
			},
		})
	}))
	defer server.Close()
	client, err := across.New(across.Config{
		BaseURL: server.URL, APIKey: "synthetic", IntegratorID: "0x0001",
		Client: server.Client(), Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Approval(context.Background(), across.ApprovalRequest{
		OriginChainID: 137, DestinationChainID: across.SolanaChainID,
		InputToken: "input", OutputToken: "output", Amount: "1000000",
		Depositor: "depositor", Recipient: "recipient",
	})
	if err == nil {
		t.Fatal("expected an expired/mismatched artifact rejection")
	}
}

func TestApprovalPreservesSolanaArtifact(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"id": "solana", "crossSwapType": "bridgeableToBridgeable", "amountType": "exactInput",
			"inputAmount": "1000000", "expectedOutputAmount": "999500", "minOutputAmount": "999000",
			"expectedFillTime": 2, "quoteExpiryTimestamp": now.Add(time.Minute).Unix(),
			"swapTx": map[string]any{
				"simulationSuccess": true, "chainId": across.SolanaChainID,
				"serializedTransaction": "synthetic-base64-transaction",
			},
		})
	}))
	defer server.Close()
	client, err := across.New(across.Config{
		BaseURL: server.URL, APIKey: "synthetic", IntegratorID: "0x0001",
		Client: server.Client(), Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	approval, err := client.Approval(context.Background(), across.ApprovalRequest{
		OriginChainID: across.SolanaChainID, DestinationChainID: 137,
		InputToken: "SyntheticUSDCMint", OutputToken: "0x3333333333333333333333333333333333333333",
		Amount: "1000000", Depositor: "SyntheticSolanaDepositor",
		Recipient: "0x1111111111111111111111111111111111111111",
	})
	if err != nil {
		t.Fatal(err)
	}
	if approval.SwapTransaction.Serialized != "synthetic-base64-transaction" ||
		len(approval.SwapTransaction.Raw) == 0 {
		t.Fatalf("Solana artifact was not preserved: %+v", approval.SwapTransaction)
	}
}

func TestApprovalAcceptsExplicitlyNonExpiringDirectArtifact(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"id": "direct-cctp", "crossSwapType": "bridgeableToBridgeable", "amountType": "exactInput",
			"inputAmount": "1000000", "expectedOutputAmount": "1000000", "minOutputAmount": "1000000",
			"expectedFillTime": 15, "quoteExpiryTimestamp": 0,
			"checks": map[string]any{"allowance": map[string]any{
				"token":   "0x3333333333333333333333333333333333333333",
				"spender": "0x2222222222222222222222222222222222222222",
				"actual":  "0", "expected": "1000000",
			}},
			"swapTx": map[string]any{
				"simulationSuccess": true, "chainId": 137,
				"to": "0x2222222222222222222222222222222222222222", "data": "0x1234",
			},
		})
	}))
	defer server.Close()
	client, err := across.New(across.Config{
		BaseURL: server.URL, APIKey: "synthetic", IntegratorID: "0x0001",
		Client: server.Client(), Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	approval, err := client.Approval(context.Background(), across.ApprovalRequest{
		OriginChainID: 137, DestinationChainID: across.SolanaChainID,
		InputToken: "input", OutputToken: "output", Amount: "1000000",
		Depositor: "depositor", Recipient: "recipient",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !approval.ExpiresAt.IsZero() ||
		approval.Allowance.Spender != "0x2222222222222222222222222222222222222222" {
		t.Fatalf("unexpected direct approval: %+v", approval)
	}
}

func TestApprovalAcceptsUnverifiedSolanaSimulationWhenBalanceIsSufficient(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"id": "solana-unverified", "crossSwapType": "bridgeableToBridgeable", "amountType": "exactInput",
			"inputAmount": "100000", "expectedOutputAmount": "100000", "minOutputAmount": "100000",
			"expectedFillTime": 8, "quoteExpiryTimestamp": 0,
			"checks": map[string]any{"balance": map[string]any{
				"actual": "500000", "expected": "100000",
			}},
			"swapTx": map[string]any{
				"simulationSuccess": false, "chainId": across.SolanaChainID,
				"serializedTransaction": "synthetic-base64-transaction",
			},
		})
	}))
	defer server.Close()
	client, err := across.New(across.Config{
		BaseURL: server.URL, APIKey: "synthetic", IntegratorID: "0x0001",
		Client: server.Client(), Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Approval(context.Background(), across.ApprovalRequest{
		OriginChainID: across.SolanaChainID, DestinationChainID: 137,
		InputToken: "input", OutputToken: "output", Amount: "100000",
		Depositor: "depositor", Recipient: "recipient",
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestApprovalRejectsInsufficientSourceBalance(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"id": "insufficient", "crossSwapType": "bridgeableToBridgeable", "amountType": "exactInput",
			"inputAmount": "1000000", "expectedOutputAmount": "1000000", "minOutputAmount": "1000000",
			"expectedFillTime": 8, "quoteExpiryTimestamp": 0,
			"checks": map[string]any{"balance": map[string]any{
				"actual": "500000", "expected": "1000000",
			}},
			"swapTx": map[string]any{
				"simulationSuccess": false, "chainId": across.SolanaChainID,
				"serializedTransaction": "synthetic-base64-transaction",
			},
		})
	}))
	defer server.Close()
	client, err := across.New(across.Config{
		BaseURL: server.URL, APIKey: "synthetic", IntegratorID: "0x0001",
		Client: server.Client(), Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Approval(context.Background(), across.ApprovalRequest{
		OriginChainID: across.SolanaChainID, DestinationChainID: 137,
		InputToken: "input", OutputToken: "output", Amount: "1000000",
		Depositor: "depositor", Recipient: "recipient",
	})
	if err == nil || !strings.Contains(err.Error(), "balance is insufficient") {
		t.Fatalf("expected insufficient balance error, got %v", err)
	}
}

func TestDepositStatusUsesTransactionReference(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/deposit/status" ||
			request.URL.Query().Get("depositTxnRef") != "synthetic-source-transaction" {
			t.Fatalf("unexpected status request %s", request.URL.String())
		}
		if request.URL.Query().Has("integratorId") {
			t.Fatalf("deposit status must not send unsupported integratorId: %s", request.URL.String())
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"status": "filled", "depositTxnRef": "synthetic-source-transaction",
			"fillTxnRef": "synthetic-destination-transaction",
		})
	}))
	defer server.Close()
	client, err := across.New(across.Config{
		BaseURL: server.URL, APIKey: "synthetic", IntegratorID: "0x0001",
		Client: server.Client(), Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	status, err := client.DepositStatus(context.Background(), "synthetic-source-transaction")
	if err != nil {
		t.Fatal(err)
	}
	if status.State != across.DepositFilled ||
		status.FillTransaction != "synthetic-destination-transaction" ||
		!status.ObservedAt.Equal(now) {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestAPIErrorIsTyped(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"code": "RATE_LIMITED", "message": "slow down", "id": "synthetic-request",
		})
	}))
	defer server.Close()
	client, err := across.New(across.Config{
		BaseURL: server.URL, APIKey: "synthetic", IntegratorID: "0x0001",
		Client: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Approval(context.Background(), across.ApprovalRequest{
		OriginChainID: 137, DestinationChainID: across.SolanaChainID,
		InputToken: "input", OutputToken: "output", Amount: "1000000",
		Depositor: "depositor", Recipient: "recipient",
	})
	var apiError *across.APIError
	if !errors.As(err, &apiError) || apiError.HTTPStatus != http.StatusTooManyRequests ||
		apiError.Code != "RATE_LIMITED" {
		t.Fatalf("expected typed API error, got %v", err)
	}
}
