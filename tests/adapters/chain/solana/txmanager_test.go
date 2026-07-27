package solana_test

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	solanago "github.com/gagliardetto/solana-go"

	solanaadapter "github.com/VarozXYZ/vernier/adapters/chain/solana"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
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
	for _, compiled := range transaction.Message.Instructions {
		program, programErr := transaction.Message.ResolveProgramIDIndex(compiled.ProgramIDIndex)
		if programErr != nil {
			t.Fatal(programErr)
		}
		if program.String() == "ComputeBudget111111111111111111111111111111" &&
			len(compiled.Data) == 5 && compiled.Data[0] == 2 {
			computeLimits++
			if binary.LittleEndian.Uint32(compiled.Data[1:]) != 1_400_000 {
				t.Fatalf("compute unit limit = %d", binary.LittleEndian.Uint32(compiled.Data[1:]))
			}
		}
	}
	if computeLimits != 1 {
		t.Fatalf("compiled transaction contains %d compute-unit-limit instructions", computeLimits)
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
