package solana_test

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	solanago "github.com/gagliardetto/solana-go"

	solanaadapter "github.com/VarozXYZ/vernier/adapters/chain/solana"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

type reconciliationSpy struct {
	calls atomic.Int32
}

func (s *reconciliationSpy) ReadSignatureStatus(context.Context, string) (solanaadapter.SignatureStatus, error) {
	s.calls.Add(1)
	return solanaadapter.SignatureStatus{}, nil
}
func (s *reconciliationSpy) ReadTransaction(context.Context, string) (solanaadapter.Transaction, error) {
	s.calls.Add(1)
	return solanaadapter.Transaction{}, nil
}
func (s *reconciliationSpy) CurrentBlockHeight(context.Context) (uint64, error) {
	s.calls.Add(1)
	return 0, nil
}
func (s *reconciliationSpy) IsBlockhashValid(context.Context, string) (bool, error) {
	s.calls.Add(1)
	return true, nil
}

func TestSolanaTxManagerBuildsSignsAndSendsWithoutPreBroadcastRPC(t *testing.T) {
	privateKey, err := solanago.NewRandomPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	var expectedSignature atomic.Value
	var sends atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/ping":
			_, _ = writer.Write([]byte("ok"))
		case "/fast":
			sends.Add(1)
			var envelope struct {
				Params []json.RawMessage `json:"params"`
			}
			if err := json.NewDecoder(request.Body).Decode(&envelope); err != nil {
				t.Error(err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			var encoded string
			if len(envelope.Params) != 2 || json.Unmarshal(envelope.Params[0], &encoded) != nil {
				t.Error("invalid Sender request")
			}
			var options struct {
				Encoding      string `json:"encoding"`
				SkipPreflight bool   `json:"skipPreflight"`
				MaxRetries    int    `json:"maxRetries"`
			}
			if json.Unmarshal(envelope.Params[1], &options) != nil ||
				options.Encoding != "base64" ||
				!options.SkipPreflight ||
				options.MaxRetries != 0 {
				t.Errorf("invalid Sender options: %+v", options)
			}
			raw, decodeErr := base64.StdEncoding.DecodeString(encoded)
			if decodeErr != nil || len(raw) == 0 {
				t.Errorf("invalid transaction payload: %v", decodeErr)
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"jsonrpc": "2.0", "id": 1, "result": expectedSignature.Load().(string),
			})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	reconciliation := &reconciliationSpy{}
	manager, err := solanaadapter.NewTxManager(solanaadapter.TxManagerConfig{
		Chain: "solana", Account: "wallet", PrivateKey: privateKey,
		SenderEndpoint: server.URL + "/fast", PingEndpoint: server.URL + "/ping",
		Client: server.Client(), Reconciliation: reconciliation, Clock: time.Now,
		WarmInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := manager.Warm(ctx); err != nil {
		t.Fatal(err)
	}
	input, _ := market.NewTokenAmount("quote", big.NewInt(100))
	output, _ := market.NewTokenAmount("base", big.NewInt(145))
	quote, _ := market.NewQuote(market.Quote{
		Source: "jupiter-build", Market: "remote-market", SnapshotVersion: 1,
		Purpose: market.QuotePurposeLiveValidation, Mode: market.QuoteModeExactInput,
		Quality: market.QuoteQualityExact, AmountIn: input, AmountOut: output, QuotedAt: time.Now(),
	})
	blockhash := make([]byte, 32)
	for index := range blockhash {
		blockhash[index] = byte(index + 1)
	}
	instruction := map[string]any{
		"programId": "11111111111111111111111111111111",
		"accounts": []map[string]any{{
			"pubkey": privateKey.PublicKey().String(), "isWritable": true, "isSigner": true,
		}},
		"data": "",
	}
	providerLimit := make([]byte, 5)
	providerLimit[0] = 2
	binary.LittleEndian.PutUint32(providerLimit[1:], 900_000)
	providerComputeLimit := map[string]any{
		"programId": "ComputeBudget111111111111111111111111111111",
		"accounts":  []any{},
		"data":      base64.StdEncoding.EncodeToString(providerLimit),
	}
	payload, _ := json.Marshal(map[string]any{
		"computeBudgetInstructions": []any{providerComputeLimit}, "setupInstructions": []any{},
		"swapInstruction": instruction, "cleanupInstruction": nil, "otherInstructions": []any{},
		"tipInstruction": instruction, "addressesByLookupTableAddress": map[string]any{},
		"blockhashWithMetadata": map[string]any{"blockhash": blockhash, "lastValidBlockHeight": 500},
	})
	leg := execution.Leg{
		ID: "buy", Side: execution.LegBuy, Chain: "solana", Account: "wallet",
		Market: "remote-market", Input: input, ExpectedOutput: output,
	}
	prepared, err := manager.Prepare(ctx, executionport.Artifact{
		Leg: leg, ValidatedQuote: quote, Payload: payload,
		Metadata: map[string]string{"kind": "jupiter_build_v2"},
		BuiltAt:  time.Now(), LastValidBlockHeight: 500,
	})
	if err != nil {
		t.Fatal(err)
	}
	expectedSignature.Store(prepared.Identity.Hash)
	if len(prepared.SignedPayload) == 0 || prepared.Identity.Blockhash == "" ||
		prepared.Identity.LastValidBlockHeight != 500 {
		t.Fatalf("prepared transaction is incomplete: %+v", prepared.Identity)
	}
	transaction, err := solanago.TransactionFromBytes(prepared.SignedPayload)
	if err != nil {
		t.Fatal(err)
	}
	computeLimits := 0
	senderTips := 0
	for _, compiled := range transaction.Message.Instructions {
		program, programErr := transaction.Message.ResolveProgramIDIndex(compiled.ProgramIDIndex)
		if programErr != nil {
			t.Fatal(programErr)
		}
		if program.String() == "ComputeBudget111111111111111111111111111111" &&
			len(compiled.Data) == 5 && compiled.Data[0] == 2 {
			computeLimits++
			if binary.LittleEndian.Uint32(compiled.Data[1:]) != 900_000 {
				t.Fatalf("compute unit limit = %d", binary.LittleEndian.Uint32(compiled.Data[1:]))
			}
		}
		if program.Equals(solanago.SystemProgramID) &&
			len(compiled.Data) == 12 &&
			binary.LittleEndian.Uint32(compiled.Data[:4]) == 2 {
			accounts, accountErr := compiled.ResolveInstructionAccounts(
				&transaction.Message,
			)
			if accountErr != nil {
				t.Fatal(accountErr)
			}
			if len(accounts) == 2 &&
				solanaadapter.IsHeliusSenderTipAccount(accounts[1].PublicKey) {
				senderTips++
				if amount := binary.LittleEndian.Uint64(compiled.Data[4:]); amount != 1_000_000 {
					t.Fatalf("Sender tip = %d lamports", amount)
				}
			}
		}
	}
	if computeLimits != 1 {
		t.Fatalf("compiled transaction contains %d compute-unit-limit instructions", computeLimits)
	}
	if senderTips != 1 {
		t.Fatalf("compiled transaction contains %d Helius Sender tips", senderTips)
	}
	result, err := manager.Broadcast(ctx, prepared)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Accepted || sends.Load() != 1 {
		t.Fatalf("broadcast result = %+v sends=%d", result, sends.Load())
	}
	if reconciliation.calls.Load() != 0 {
		t.Fatalf("pre-broadcast RPC calls = %d", reconciliation.calls.Load())
	}
}

func TestAssembleJupiterBuildForSimulationLeavesReferenceSignerUnverified(t *testing.T) {
	payer, err := solanago.NewRandomPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	reference, err := solanago.NewRandomPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	blockhash := make([]byte, 32)
	for index := range blockhash {
		blockhash[index] = byte(index + 1)
	}
	instruction := map[string]any{
		"programId": "11111111111111111111111111111111",
		"accounts": []map[string]any{{
			"pubkey":     reference.PublicKey().String(),
			"isWritable": true,
			"isSigner":   true,
		}},
		"data": "",
	}
	payload, err := json.Marshal(map[string]any{
		"computeBudgetInstructions":     []any{},
		"setupInstructions":             []any{},
		"swapInstruction":               instruction,
		"cleanupInstruction":            nil,
		"otherInstructions":             []any{},
		"tipInstruction":                instruction,
		"addressesByLookupTableAddress": map[string]any{},
		"blockhashWithMetadata": map[string]any{
			"blockhash":            blockhash,
			"lastValidBlockHeight": 500,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, resolvedBlockhash, err :=
		solanaadapter.AssembleJupiterBuildForSimulation(
			payload,
			payer,
			1_400_000,
			solanaadapter.NextHeliusSenderTipAccount(),
			1_000_000,
		)
	if err != nil {
		t.Fatal(err)
	}
	if resolvedBlockhash == "" {
		t.Fatal("simulation transaction has no blockhash")
	}
	transaction, err := solanago.TransactionFromBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	signers := transaction.Message.Signers()
	if len(signers) != 2 || len(transaction.Signatures) != 2 {
		t.Fatalf(
			"signers=%v signatures=%d",
			signers,
			len(transaction.Signatures),
		)
	}
	if err := transaction.VerifySignatures(); err == nil {
		t.Fatal("reference preflight transaction unexpectedly became broadcastable")
	}
	var payerFound, referenceFound bool
	for index, signer := range signers {
		switch {
		case signer.Equals(payer.PublicKey()):
			payerFound = transaction.Signatures[index] !=
				(solanago.Signature{})
		case signer.Equals(reference.PublicKey()):
			referenceFound = transaction.Signatures[index] ==
				(solanago.Signature{})
		}
	}
	if !payerFound || !referenceFound {
		t.Fatalf(
			"payer signed=%t reference unsigned=%t",
			payerFound,
			referenceFound,
		)
	}
}

func TestSolanaTxManagerRejectsOversizedTransactionBeforePersistence(t *testing.T) {
	privateKey, err := solanago.NewRandomPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	manager, err := solanaadapter.NewTxManager(solanaadapter.TxManagerConfig{
		Chain: "solana", Account: "wallet", PrivateKey: privateKey,
		SenderEndpoint: "https://sender.invalid/fast",
		Reconciliation: &reconciliationSpy{}, Clock: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	input, _ := market.NewTokenAmount("quote", big.NewInt(100))
	output, _ := market.NewTokenAmount("base", big.NewInt(145))
	quote, _ := market.NewQuote(market.Quote{
		Source: "jupiter-build", Market: "remote-market", SnapshotVersion: 1,
		Purpose: market.QuotePurposeLiveValidation, Mode: market.QuoteModeExactInput,
		Quality: market.QuoteQualityExact, AmountIn: input, AmountOut: output,
		QuotedAt: time.Now(),
	})
	blockhash := make([]byte, 32)
	for index := range blockhash {
		blockhash[index] = byte(index + 1)
	}
	instruction := map[string]any{
		"programId": "11111111111111111111111111111111",
		"accounts": []map[string]any{{
			"pubkey":     privateKey.PublicKey().String(),
			"isWritable": true,
			"isSigner":   true,
		}},
		"data": base64.StdEncoding.EncodeToString(make([]byte, 1_300)),
	}
	payload, _ := json.Marshal(map[string]any{
		"computeBudgetInstructions":     []any{},
		"setupInstructions":             []any{},
		"swapInstruction":               instruction,
		"cleanupInstruction":            nil,
		"otherInstructions":             []any{},
		"tipInstruction":                instruction,
		"addressesByLookupTableAddress": map[string]any{},
		"blockhashWithMetadata": map[string]any{
			"blockhash": blockhash, "lastValidBlockHeight": 500,
		},
	})
	leg := execution.Leg{
		ID: "buy", Side: execution.LegBuy, Chain: "solana", Account: "wallet",
		Market: "remote-market", Input: input, ExpectedOutput: output,
	}
	_, err = manager.Prepare(context.Background(), executionport.Artifact{
		Leg: leg, ValidatedQuote: quote, Payload: payload,
		Metadata: map[string]string{"kind": "jupiter_build_v2"},
		BuiltAt:  time.Now(), LastValidBlockHeight: 500,
	})
	var oversized *executionport.ArtifactTooLargeError
	if !errors.As(err, &oversized) {
		t.Fatalf("Prepare() error = %T %v", err, err)
	}
	if oversized.ActualBytes <= oversized.MaximumBytes {
		t.Fatalf("oversized error = %+v", oversized)
	}
}

func TestSolanaTxManagerClassifiesSenderHTTPFailures(t *testing.T) {
	for _, test := range []struct {
		name        string
		status      int
		body        string
		errorText   string
		disposition chainport.BroadcastDisposition
	}{
		{
			name:        "rate limit is rejected before acceptance",
			status:      http.StatusTooManyRequests,
			body:        `{"jsonrpc":"2.0","error":{"code":-32005,"message":"Too many requests"}}`,
			errorText:   "RPC -32005: Too many requests",
			disposition: chainport.BroadcastRejected,
		},
		{
			name:        "service failure has uncertain outcome",
			status:      http.StatusServiceUnavailable,
			body:        `{"jsonrpc":"2.0","error":{"code":-32002,"message":"Service unavailable"}}`,
			errorText:   "RPC -32002: Service unavailable",
			disposition: chainport.BroadcastPossible,
		},
		{
			name:        "sender invalid params is rejected even behind HTTP 500",
			status:      http.StatusInternalServerError,
			body:        `{"jsonrpc":"2.0","error":{"code":-32602,"message":"Invalid Request: base64 encoded too large"}}`,
			errorText:   "RPC -32602: Invalid Request: base64 encoded too large",
			disposition: chainport.BroadcastRejected,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(
				func(writer http.ResponseWriter, _ *http.Request) {
					writer.WriteHeader(test.status)
					_, _ = writer.Write([]byte(test.body))
				},
			))
			defer server.Close()
			privateKey, err := solanago.NewRandomPrivateKey()
			if err != nil {
				t.Fatal(err)
			}
			manager, err := solanaadapter.NewTxManager(
				solanaadapter.TxManagerConfig{
					Chain: "solana", Account: "wallet", PrivateKey: privateKey,
					SenderEndpoint: server.URL + "/fast",
					PingEndpoint:   server.URL + "/ping",
					Client:         server.Client(),
					Reconciliation: &reconciliationSpy{},
					Clock:          time.Now,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			result, err := manager.Broadcast(
				context.Background(),
				chainport.PreparedTransaction{
					Leg: execution.Leg{Account: "wallet"},
					Identity: execution.TransactionIdentity{
						Chain: "solana", Account: "wallet", Hash: "synthetic",
					},
					SignedPayload: []byte{1, 2, 3},
				},
			)
			if err == nil || result.Disposition != test.disposition ||
				result.Accepted {
				t.Fatalf("result=%+v error=%v", result, err)
			}
			if !strings.Contains(err.Error(), test.errorText) {
				t.Fatalf("error %q does not contain %q", err, test.errorText)
			}
		})
	}
}
