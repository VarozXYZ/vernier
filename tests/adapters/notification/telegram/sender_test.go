package telegram_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/notification/telegram"
	notificationport "github.com/VarozXYZ/vernier/ports/notification"
)

type clientFunc func(*http.Request) (*http.Response, error)

func (f clientFunc) Do(request *http.Request) (*http.Response, error) { return f(request) }

func TestSenderPostsOneCompleteOpeningMessage(t *testing.T) {
	requests := 0
	sender, err := telegram.New(telegram.Config{
		BotToken: "synthetic-token", ChatID: "synthetic-chat", BaseURL: "https://telegram.test",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			if request.Method != http.MethodPost || request.URL.Path != "/botsynthetic-token/sendMessage" {
				t.Fatalf("unexpected request: %s %s", request.Method, request.URL.Path)
			}
			var payload struct {
				ChatID                string `json:"chat_id"`
				Text                  string `json:"text"`
				ParseMode             string `json:"parse_mode"`
				DisableWebPagePreview bool   `json:"disable_web_page_preview"`
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			if payload.ChatID != "synthetic-chat" {
				t.Fatalf("chat id=%q", payload.ChatID)
			}
			if payload.ParseMode != "HTML" || !payload.DisableWebPagePreview {
				t.Fatalf("unexpected Telegram rendering options: %+v", payload)
			}
			for _, expected := range []string{
				"🎯 <b>ARB · +1 QUOTE net</b>",
				"📍 market-a → market-b",
				`💱 <b>750 QUOTE</b> → <a href="https://explorer.test/tx/tx-synthetic"><b>14,550 BASE</b></a> → <b>752 QUOTE</b>`,
				"🔌 provider-a → provider-b",
				"⚡ 50 ms · compra 20 ms · venta 30 ms",
			} {
				if !strings.Contains(payload.Text, expected) {
					t.Fatalf("message does not contain %q: %s", expected, payload.Text)
				}
			}
			for _, verbose := range []string{
				"Arbitrage window opened", "Direction:", "Gross PnL:", "Threshold:", "UTC:",
				"bruto", "coste", "mínimo", "Trigger tx",
			} {
				if strings.Contains(payload.Text, verbose) {
					t.Fatalf("message still contains verbose label %q: %s", verbose, payload.Text)
				}
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"ok":true,"result":{"message_id":1}}`)),
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = sender.SendOpening(context.Background(), notificationport.OpportunityOpening{
		Direction: "market-a -> market-b", BuyProvider: "provider-a", SellProvider: "provider-b",
		Input: "750.000000 QUOTE", BaseBought: "14550.000000000 BASE", SellOutput: "752.000000 QUOTE",
		GrossPnL: "2.000000 QUOTE", Cost: "1.000000 QUOTE", NetPnL: "1.000000 QUOTE",
		Threshold: "1.000000 QUOTE", BuyLatency: 20 * time.Millisecond, SellLatency: 30 * time.Millisecond,
		Trigger: "chain/tx-synthetic", TriggerURL: "https://explorer.test/tx/tx-synthetic",
		OpenedAt: time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("requests=%d, want 1", requests)
	}
}

func TestSenderPostsConfigurationWarningForAcceptedModeMismatch(t *testing.T) {
	requests := 0
	sender, err := telegram.New(telegram.Config{
		BotToken: "synthetic-token", ChatID: "synthetic-chat", BaseURL: "https://telegram.test",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			var payload struct {
				Text string `json:"text"`
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			for _, expected := range []string{
				"⚠️ <b>CONFIG · JUPITER</b>",
				"📍 Solana · Jupiter",
				"⚙️ manual → ultra · metis",
				"✅ Quote aceptado",
			} {
				if !strings.Contains(payload.Text, expected) {
					t.Fatalf("warning does not contain %q: %s", expected, payload.Text)
				}
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"ok":true,"result":{"message_id":2}}`)),
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = sender.SendConfigurationWarning(context.Background(), notificationport.ConfigurationWarning{
		Code: "jupiter_order_mode_mismatch", Provider: "Jupiter", Market: "Solana",
		Expected: "manual", Observed: "ultra", Details: map[string]string{"router": "metis"},
		ObservedAt: time.Date(2026, 7, 27, 10, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("requests=%d, want 1", requests)
	}
}

func TestSenderEditsOneMessageThroughoutLiveLifecycle(t *testing.T) {
	var paths []string
	var texts []string
	sender, err := telegram.New(telegram.Config{
		BotToken: "synthetic-token",
		ChatID:   "synthetic-chat",
		BaseURL:  "https://telegram.test",
		Client: clientFunc(func(
			request *http.Request,
		) (*http.Response, error) {
			var payload struct {
				Text string `json:"text"`
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			paths = append(paths, request.URL.Path)
			texts = append(texts, payload.Text)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(
					`{"ok":true,"result":{"message_id":42}}`,
				)),
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	events := []notificationport.LiveExecutionEvent{
		{
			Kind:           notificationport.LiveExecutionStarted,
			Operation:      "synthetic-operation",
			State:          "live",
			Direction:      "market-a -> market-b",
			Input:          "100 QUOTE",
			ExpectedBase:   "400 BASE",
			ExpectedOutput: "102 QUOTE",
			ExpectedNetPnL: "1 QUOTE",
			BuyProvider:    "adapter-a",
			SellProvider:   "adapter-b",
			Trigger:        "chain-a/transaction",
			TriggerURL:     "https://explorer.test/tx/trigger",
		},
		{
			Kind:              notificationport.LiveExecutionStageCompleted,
			Operation:         "synthetic-operation",
			Stage:             "buy",
			Ordinal:           1,
			TotalStages:       4,
			SourceChain:       "chain-a",
			Input:             "100 QUOTE",
			Output:            "401.123456 BASE",
			SourceTransaction: "source-transaction",
			SourceURL:         "https://explorer.test/tx/source",
			Duration:          730 * time.Millisecond,
		},
		{
			Kind:          notificationport.LiveExecutionCompleted,
			Operation:     "synthetic-operation",
			Output:        "101.500000 QUOTE",
			ExecutionCost: "0.500000 QUOTE",
			NetPnL:        "1.000000 QUOTE",
			Duration:      2500 * time.Millisecond,
		},
	}
	for _, event := range events {
		if err := sender.SendLiveExecution(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	if len(paths) != 3 ||
		paths[0] != "/botsynthetic-token/sendMessage" ||
		paths[1] != "/botsynthetic-token/editMessageText" ||
		paths[2] != "/botsynthetic-token/editMessageText" {
		t.Fatalf("unexpected Telegram operations: %v", paths)
	}
	final := texts[len(texts)-1]
	for _, expected := range []string{
		"🏁 <b>LIVE · COMPLETE</b>",
		"market-a → market-b",
		"✅ <b>1/4 · BUY</b> · chain-a",
		`<a href="https://explorer.test/tx/source">Swap on chain-a</a>`,
		"Return   <b>101.5 QUOTE</b>",
		"Costs    0.5 QUOTE",
		"Net PnL  <b>1 QUOTE</b>",
		"Total · 2.500 s",
	} {
		if !strings.Contains(final, expected) {
			t.Fatalf("final message missing %q:\n%s", expected, final)
		}
	}
}
