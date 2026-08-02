package telegram_test

import (
	"context"
	"encoding/json"
	"errors"
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

func TestSenderPostsReadableInsufficientBalanceRuntimeAlert(t *testing.T) {
	var message string
	sender, err := telegram.New(telegram.Config{
		BotToken: "synthetic-token", ChatID: "synthetic-chat", BaseURL: "https://telegram.test",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			var payload struct {
				Text string `json:"text"`
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			message = payload.Text
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(
				`{"ok":true,"result":{"message_id":1}}`,
			))}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.SendLiveRuntime(context.Background(), notificationport.LiveRuntimeEvent{
		Kind:  notificationport.LiveRuntimeBalanceInsufficient,
		Chain: "chain-b", Token: "BASE", AvailableUnits: "3980", RequiredUnits: "4000",
	}); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"INSUFFICIENT BALANCE", "chain-b", "Available <code>3980 BASE</code>",
		"Required <code>4000 BASE</code>",
	} {
		if !strings.Contains(message, expected) {
			t.Fatalf("balance alert does not contain %q: %s", expected, message)
		}
	}
}

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

func TestSenderRedactsBotCredentialFromTransportErrors(t *testing.T) {
	const token = "synthetic-secret-token"
	sender, err := telegram.New(telegram.Config{
		BotToken: token, ChatID: "synthetic-chat", BaseURL: "https://telegram.test",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			return nil, errors.New("POST " + request.URL.String() + ": connection reset")
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = sender.SendOpening(context.Background(), notificationport.OpportunityOpening{})
	if err == nil || strings.Contains(err.Error(), token) ||
		!strings.Contains(err.Error(), "/bot[REDACTED]/sendMessage") {
		t.Fatalf("credential was not safely redacted: %v", err)
	}
}

func TestSenderCreatesAndEditsOneTrackingWindowMessage(t *testing.T) {
	var paths, texts []string
	sender, err := telegram.New(telegram.Config{
		BotToken: "synthetic-token", ChatID: "synthetic-chat", BaseURL: "https://telegram.test",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			paths = append(paths, request.URL.Path)
			var payload struct {
				Text string `json:"text"`
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			texts = append(texts, payload.Text)
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"ok":true,"result":{"message_id":72}}`))}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	update := notificationport.TrackingWindowUpdate{
		WindowID: "window", State: "open", Direction: "market-a -> market-b", Input: "1000 USDT",
		BuyOutput: "5000 BASE", SellOutput: "1002 USDT", NetPnL: "1.5 USDT", DeltaOpening: "0 USDT",
		DeltaPrevious: "0 USDT", Threshold: "0.5 USDT", BestPnL: "1.5 USDT", WorstPnL: "1.5 USDT",
		Points: 1, DiscoveryDuration: 12 * time.Millisecond, TriggerToOpen: 15 * time.Millisecond,
		History: []notificationport.TrackingHistoryPoint{{SellOutput: "1002 USDT", NetPnL: "1.5 USDT", Delta: "0 USDT", Calculation: 12 * time.Millisecond, Total: 15 * time.Millisecond}},
	}
	messageID, err := sender.SendTrackingWindow(context.Background(), update)
	if err != nil || messageID != 72 {
		t.Fatalf("send tracking message: id=%d err=%v", messageID, err)
	}
	update.State, update.Reason, update.Points = "closed", "below_profit_threshold", 2
	update.NetPnL, update.DeltaPrevious = "0.2 USDT", "-1.3 USDT"
	if err := sender.EditTrackingWindow(context.Background(), messageID, update); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 || !strings.HasSuffix(paths[0], "/sendMessage") || !strings.HasSuffix(paths[1], "/editMessageText") {
		t.Fatalf("unexpected Telegram operations: %v", paths)
	}
	for _, expected := range []string{"CLOSED", "below_profit_threshold", "point 2", "trigger→result"} {
		if !strings.Contains(texts[1], expected) {
			t.Fatalf("tracking message missing %q: %s", expected, texts[1])
		}
	}
}

func TestSenderExposesTelegramRetryAfter(t *testing.T) {
	sender, err := telegram.New(telegram.Config{
		BotToken: "synthetic-token", ChatID: "synthetic-chat", BaseURL: "https://telegram.test",
		Client: clientFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusTooManyRequests, Body: io.NopCloser(strings.NewReader(`{"ok":false,"parameters":{"retry_after":2}}`))}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = sender.SendTrackingWindow(context.Background(), notificationport.TrackingWindowUpdate{State: "open"})
	var limited notificationport.RetryAfterError
	if !errors.As(err, &limited) || limited.RetryAfter() != 2*time.Second {
		t.Fatalf("retry-after was not exposed: %v", err)
	}
}

func TestSenderTreatsUnchangedTelegramEditAsSuccess(t *testing.T) {
	requests := 0
	sender, err := telegram.New(telegram.Config{
		BotToken: "synthetic-token", ChatID: "synthetic-chat", BaseURL: "https://telegram.test",
		Client: clientFunc(func(_ *http.Request) (*http.Response, error) {
			requests++
			if requests == 1 {
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(
					`{"ok":true,"result":{"message_id":41}}`,
				))}, nil
			}
			return &http.Response{StatusCode: http.StatusBadRequest, Body: io.NopCloser(strings.NewReader(
				`{"ok":false,"error_code":400,"description":"Bad Request: message is not modified"}`,
			))}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	operation := "synthetic-operation"
	for _, event := range []notificationport.LiveExecutionEvent{
		{Kind: notificationport.LiveExecutionStarted, Operation: operation},
		{Kind: notificationport.LiveExecutionStageStarted, Operation: operation, Stage: "sell", Ordinal: 2},
	} {
		if err := sender.SendLiveExecution(context.Background(), event); err != nil {
			t.Fatal(err)
		}
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

func TestSenderPostsConciseLiveRuntimeLifecycle(t *testing.T) {
	var messages []string
	sender, err := telegram.New(telegram.Config{
		BotToken: "synthetic-token", ChatID: "synthetic-chat",
		BaseURL: "https://telegram.test",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			var payload struct {
				Text string `json:"text"`
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			messages = append(messages, payload.Text)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(
					`{"ok":true,"result":{"message_id":7}}`,
				)),
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Date(2026, 7, 30, 8, 0, 0, 0, time.UTC)
	if err := sender.SendLiveRuntime(
		context.Background(),
		notificationport.LiveRuntimeEvent{
			Kind: notificationport.LiveRuntimeStarted,
			Mode: "live", StartedAt: startedAt, OccurredAt: startedAt,
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := sender.SendLiveRuntime(
		context.Background(),
		notificationport.LiveRuntimeEvent{
			Kind: notificationport.LiveRuntimeStopped,
			Mode: "live", Reason: "operator/system stop",
			StartedAt: startedAt, OccurredAt: startedAt.Add(90 * time.Second),
			Uptime: 90 * time.Second,
		},
	); err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 {
		t.Fatalf("messages=%d, want 2", len(messages))
	}
	for _, expected := range []string{
		"\U0001f7e2 <b>LIVE \u00b7 STARTED</b>",
		"\U0001f4cc Mode: <b>live</b>",
		"2026-07-30 08:00:00 UTC",
	} {
		if !strings.Contains(messages[0], expected) {
			t.Fatalf("startup does not contain %q:\n%s", expected, messages[0])
		}
	}
	for _, expected := range []string{
		"\U0001f6d1 <b>LIVE \u00b7 STOPPED</b>",
		"\U0001f9ed Reason: operator/system stop",
		"\u23f3 Uptime: 90.000 s",
	} {
		if !strings.Contains(messages[1], expected) {
			t.Fatalf("shutdown does not contain %q:\n%s", expected, messages[1])
		}
	}
}

func TestSenderPostsRecoveryAndRefuelStates(t *testing.T) {
	var messages []string
	sender, err := telegram.New(telegram.Config{
		BotToken: "synthetic-token", ChatID: "synthetic-chat",
		BaseURL: "https://telegram.test",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			var payload struct {
				Text string `json:"text"`
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			messages = append(messages, payload.Text)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(
					`{"ok":true,"result":{"message_id":8}}`,
				)),
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sender.SendLiveExecution(
		context.Background(),
		notificationport.LiveExecutionEvent{
			Kind:      notificationport.LiveExecutionRecoveryProgress,
			Operation: "recovery-operation",
			Detail:    "requote_rebuild_simulate_best_exit · attempt 4",
		},
	); err != nil {
		t.Fatal(err)
	}
	if err := sender.SendLiveExecution(
		context.Background(),
		notificationport.LiveExecutionEvent{
			Kind:          notificationport.LiveExecutionRefuelCompleted,
			Operation:     "refuel-operation",
			SourceChain:   "Polygon",
			Input:         "10 USDC",
			Output:        "20 POL",
			ExecutionCost: "0.01 POL",
		},
	); err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 ||
		!strings.Contains(messages[0], "LIVE · RECOVERING") ||
		!strings.Contains(messages[0], "attempt 4") ||
		!strings.Contains(messages[1], "GAS · REFUELED") ||
		!strings.Contains(messages[1], "Polygon") {
		t.Fatalf("messages=%q", messages)
	}
}

func TestSenderPostsConciseLiveProgressAndFailureMessages(t *testing.T) {
	var messages []string
	sender, err := telegram.New(telegram.Config{
		BotToken: "synthetic-token", ChatID: "synthetic-chat",
		BaseURL: "https://telegram.test",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			var payload struct {
				Text string `json:"text"`
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			messages = append(messages, payload.Text)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(
					`{"ok":true,"result":{"message_id":3}}`,
				)),
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = sender.SendLiveExecution(
		context.Background(),
		notificationport.LiveExecutionEvent{
			Kind:      notificationport.LiveExecutionStageCompleted,
			Operation: "operation-synthetic-1234567890",
			Stage:     "bridge_base", Ordinal: 2, TotalStages: 4,
			SourceChain: "Solana", DestinationChain: "Polygon",
			Input: "4.052168781 BASE", Output: "4.052168781 BASE",
			SourceTransaction: "source-transaction-1234567890",
			SourceURL:         "https://explorer.test/tx/source",
			DestinationTx:     "destination-transaction-1234567890",
			DestinationURL:    "https://explorer.test/tx/destination",
			Evidence:          "destination_balance", Duration: 8 * time.Second,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = sender.SendLiveExecution(
		context.Background(),
		notificationport.LiveExecutionEvent{
			Kind:      notificationport.LiveExecutionFailed,
			Operation: "operation-synthetic-1234567890",
			State:     "manual_intervention_required",
			Stage:     "sell", Ordinal: 3, TotalStages: 4,
			SourceChain: "Polygon",
			Detail:      "receipt outcome is unknown",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 {
		t.Fatalf("messages=%d, want 2", len(messages))
	}
	for _, expected := range []string{
		"✅ <b>2/4 · BRIDGE BASE</b> · Solana → Polygon · 8.000 s",
		`<a href="https://explorer.test/tx/source">Departure from Solana</a>`,
		`<a href="https://explorer.test/tx/destination">Receipt on Polygon</a>`,
	} {
		if !strings.Contains(messages[0], expected) {
			t.Fatalf("progress message does not contain %q: %s", expected, messages[0])
		}
	}
	for _, forbidden := range []string{
		"4.052168781 BASE",
		"destination_balance",
		"Costs",
	} {
		if strings.Contains(messages[0], forbidden) {
			t.Fatalf("progress message contains %q: %s", forbidden, messages[0])
		}
	}
	for _, expected := range []string{
		"🚨 <b>LIVE · MANUAL ACTION</b>",
		"3/4 · SELL · Polygon",
		"⚠️ receipt outcome is unknown",
	} {
		if !strings.Contains(messages[1], expected) {
			t.Fatalf("failure message does not contain %q: %s", expected, messages[1])
		}
	}
}

func TestSenderCreatesOneLiveMessageAndEditsItForEveryUpdate(t *testing.T) {
	var paths []string
	var messageIDs []int64
	var messages []string
	sender, err := telegram.New(telegram.Config{
		BotToken: "synthetic-token", ChatID: "synthetic-chat",
		BaseURL: "https://telegram.test",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			var payload struct {
				MessageID int64  `json:"message_id"`
				Text      string `json:"text"`
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			paths = append(paths, request.URL.Path)
			messageIDs = append(messageIDs, payload.MessageID)
			messages = append(messages, payload.Text)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(
					`{"ok":true,"result":{"message_id":37}}`,
				)),
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	operation := "operation-synthetic-1234567890"
	events := []notificationport.LiveExecutionEvent{
		{
			Kind: notificationport.LiveExecutionStarted, Operation: operation,
			Direction: "Solana -> Polygon", Input: "750 QUOTE",
			BuyProvider: "Jupiter", SellProvider: "KyberSwap",
			ExpectedBase: "3250 BASE", ExpectedOutput: "755 QUOTE",
			ExpectedNetPnL: "4 QUOTE",
			Trigger:        "solana/signature:synthetic",
			TriggerURL:     "https://explorer.test/tx/trigger",
		},
		{
			Kind: notificationport.LiveExecutionStageCompleted, Operation: operation,
			Stage: "buy", Ordinal: 1, TotalStages: 4,
			SourceChain: "Solana", Input: "750 QUOTE", Output: "3251 BASE",
			SourceTransaction: "signature-synthetic",
			SourceURL:         "https://explorer.test/tx/buy",
		},
		{
			Kind: notificationport.LiveExecutionCompleted, Operation: operation,
			Direction: "Solana -> Polygon", Input: "750 QUOTE",
			Output: "754 QUOTE", NetPnL: "3 QUOTE",
		},
	}
	for _, event := range events {
		if err := sender.SendLiveExecution(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	if len(paths) != 3 {
		t.Fatalf("requests=%d, want 3", len(paths))
	}
	if paths[0] != "/botsynthetic-token/sendMessage" || messageIDs[0] != 0 {
		t.Fatalf("initial request=%q message_id=%d", paths[0], messageIDs[0])
	}
	for index := 1; index < len(paths); index++ {
		if paths[index] != "/botsynthetic-token/editMessageText" ||
			messageIDs[index] != 37 {
			t.Fatalf(
				"update %d path=%q message_id=%d",
				index, paths[index], messageIDs[index],
			)
		}
	}
	final := messages[len(messages)-1]
	for _, expected := range []string{
		"LIVE · COMPLETE",
		"Solana → Polygon",
		"750 QUOTE → 3,250 BASE → 755 QUOTE",
		"Jupiter → KyberSwap · expected <b>+4 QUOTE</b>",
		`<a href="https://explorer.test/tx/trigger">Trigger transaction</a>`,
		"1/4 · BUY",
		"750 QUOTE → <b>3,251 BASE</b>",
		"💰 Return   <b>754 QUOTE</b>",
		"📈 Net PnL  <b>3 QUOTE</b>",
	} {
		if !strings.Contains(final, expected) {
			t.Fatalf("final Live message does not contain %q: %s", expected, final)
		}
	}
}

func TestRecoveredExecutionFinishesAsCompleteWithoutPendingStages(t *testing.T) {
	t.Parallel()

	var messages []string
	sender, err := telegram.New(telegram.Config{
		BotToken: "synthetic-token", ChatID: "synthetic-chat",
		BaseURL: "https://telegram.test",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			var payload struct {
				Text string `json:"text"`
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			messages = append(messages, payload.Text)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(
					`{"ok":true,"result":{"message_id":73}}`,
				)),
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	operation := "recovered-operation"
	events := []notificationport.LiveExecutionEvent{
		{
			Kind: notificationport.LiveExecutionStarted, Operation: operation,
			Direction: "Solana -> Polygon", Input: "500 USDC",
		},
		{
			Kind: notificationport.LiveExecutionStageStarted, Operation: operation,
			Stage: "bridge_base", Ordinal: 3, TotalStages: 4,
			SourceChain: "Solana", DestinationChain: "Polygon",
			Input: "2000 BASE",
		},
		{
			Kind: notificationport.LiveExecutionFailed, Operation: operation,
			Stage: "bridge_base", Ordinal: 3, Detail: "balance visibility timeout",
		},
		{
			Kind: notificationport.LiveExecutionRecoveryStarted, Operation: operation,
			Detail: "stage 3/4",
		},
		{
			Kind: notificationport.LiveExecutionStageCompleted, Operation: operation,
			Stage: "bridge_base", Ordinal: 3, TotalStages: 4,
			SourceChain: "Solana", DestinationChain: "Polygon",
			Input: "2000 BASE", Output: "2000 BASE",
		},
		{
			Kind: notificationport.LiveExecutionStageCompleted, Operation: operation,
			Stage: "bridge_quote_return", Ordinal: 4, TotalStages: 4,
			SourceChain: "Polygon", DestinationChain: "Solana",
			Input: "502 USDC", Output: "502 USDC",
		},
		{
			Kind: notificationport.LiveExecutionRecoveryCompleted, Operation: operation,
			Output: "502 USDC",
		},
		{
			Kind: notificationport.LiveExecutionCompleted, Operation: operation,
			Direction: "Solana -> Polygon", Input: "500 USDC",
			Output: "502 USDC", ExecutionCost: "0.5 USDC", NetPnL: "+1.5 USDC",
		},
	}
	for _, event := range events {
		if err := sender.SendLiveExecution(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	final := messages[len(messages)-1]
	if !strings.Contains(final, "LIVE") ||
		!strings.Contains(final, "COMPLETE") ||
		!strings.Contains(final, "Recovered automatically") {
		t.Fatalf("recovered final message is incomplete:\n%s", final)
	}
	if strings.Contains(final, "RECOVERED</b>") ||
		strings.Contains(final, "\u23f3 <b>3/4") {
		t.Fatalf("recovered final message retains transient state:\n%s", final)
	}
}

func TestSenderLabelsForcedCanaryWithoutDiscoverySizedOutputs(t *testing.T) {
	var messages []string
	sender, err := telegram.New(telegram.Config{
		BotToken: "synthetic-token", ChatID: "synthetic-chat",
		BaseURL: "https://telegram.test",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			var payload struct {
				Text string `json:"text"`
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			messages = append(messages, payload.Text)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(
					`{"ok":true,"result":{"message_id":41}}`,
				)),
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	operation := "forced-canary-operation"
	for _, event := range []notificationport.LiveExecutionEvent{
		{
			Kind: notificationport.LiveExecutionStarted, Operation: operation,
			State: "forced_canary", Direction: "Solana -> Polygon",
			Input: "1 QUOTE", BuyProvider: "Jupiter",
			SellProvider: "KyberSwap",
		},
		{
			Kind: notificationport.LiveExecutionCompleted, Operation: operation,
			Input: "1 QUOTE", Output: "0.99 QUOTE",
		},
	} {
		if err := sender.SendLiveExecution(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	if len(messages) != 2 ||
		!strings.Contains(messages[0], "CANARY · FORCED") ||
		!strings.Contains(messages[0], "1 QUOTE") ||
		!strings.Contains(messages[1], "CANARY · COMPLETE") {
		t.Fatalf("unexpected forced canary messages: %v", messages)
	}
	for _, forbidden := range []string{"750 QUOTE", "expected"} {
		if strings.Contains(messages[0], forbidden) {
			t.Fatalf("forced canary message contains %q: %s", forbidden, messages[0])
		}
	}
}

func TestSenderRendersCompactCompletedCanarySummary(t *testing.T) {
	var messages []string
	sender, err := telegram.New(telegram.Config{
		BotToken: "synthetic-token", ChatID: "synthetic-chat",
		BaseURL: "https://telegram.test",
		Client: clientFunc(func(request *http.Request) (*http.Response, error) {
			var payload struct {
				Text string `json:"text"`
			}
			if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			messages = append(messages, payload.Text)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(
					`{"ok":true,"result":{"message_id":51}}`,
				)),
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	operation := "sequential-operation-synthetic"
	events := []notificationport.LiveExecutionEvent{
		{
			Kind: notificationport.LiveExecutionStarted, Operation: operation,
			State: "forced_canary", Direction: "Solana -> Polygon",
			Input: "1 USDC", BuyProvider: "Jupiter", SellProvider: "KyberSwap",
			Trigger: "bootstrap", TriggerURL: "https://solscan.io/tx/bootstrap",
		},
		{
			Kind: notificationport.LiveExecutionStageCompleted, Operation: operation,
			Stage: "buy", Ordinal: 1, TotalStages: 4, SourceChain: "Solana",
			Input: "1 QUOTE", Output: "4.983035 BASE",
			ExecutionCost: "0.102409 QUOTE", Evidence: "balance_delta",
			Duration:          754 * time.Millisecond,
			SourceTransaction: "buy-signature",
			SourceURL:         "https://solscan.io/tx/buy-signature",
		},
		{
			Kind: notificationport.LiveExecutionStageCompleted, Operation: operation,
			Stage: "bridge_base", Ordinal: 2, TotalStages: 4,
			SourceChain: "Solana", DestinationChain: "Polygon",
			Input: "4.983035 BASE", Output: "4.983035 BASE",
			ExecutionCost: "0.37061 QUOTE", Evidence: "bridge_internal_tag",
			Duration:          17032 * time.Millisecond,
			SourceTransaction: "bridge-source",
			SourceURL:         "https://solscan.io/tx/bridge-source",
			DestinationTx:     "bridge-receipt",
			DestinationURL:    "https://polygonscan.com/tx/bridge-receipt",
		},
		{
			Kind: notificationport.LiveExecutionStageCompleted, Operation: operation,
			Stage: "sell", Ordinal: 3, TotalStages: 4, SourceChain: "Polygon",
			Input: "4.983035 BASE", Output: "1.00729 QUOTE",
			Duration:          1934 * time.Millisecond,
			SourceTransaction: "sell-transaction",
			SourceURL:         "https://polygonscan.com/tx/sell-transaction",
		},
		{
			Kind: notificationport.LiveExecutionStageCompleted, Operation: operation,
			Stage: "bridge_quote_return", Ordinal: 4, TotalStages: 4,
			SourceChain: "Polygon", DestinationChain: "Solana",
			Input: "1.00729 QUOTE", Output: "1.00729 QUOTE",
			Duration:          18187 * time.Millisecond,
			SourceTransaction: "return-source",
			SourceURL:         "https://polygonscan.com/tx/return-source",
			DestinationTx:     "return-receipt",
			DestinationURL:    "https://solscan.io/tx/return-receipt",
		},
		{
			Kind: notificationport.LiveExecutionExitSelected, Operation: operation,
			Stage: "sell_at_destination", DestinationValue: "1.007289 QUOTE",
		},
		{
			Kind: notificationport.LiveExecutionCompleted, Operation: operation,
			Input: "1 QUOTE", Output: "1.00729 QUOTE",
			ExecutionCost: "0.492405 QUOTE", NetPnL: "-0.485115 QUOTE",
			Duration: 38795 * time.Millisecond,
		},
	}
	for _, event := range events {
		if err := sender.SendLiveExecution(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	final := messages[len(messages)-1]
	for _, expected := range []string{
		"🏁 <b>CANARY · COMPLETE</b>",
		"✅ <b>1/4 · BUY</b> · Solana",
		"1 QUOTE → <b>4.983 BASE</b> · 754 ms",
		`>Swap on Solana</a>`,
		"✅ <b>2/4 · BRIDGE BASE</b> · Solana → Polygon · 17.032 s",
		`>Departure from Solana</a> → <a href="https://polygonscan.com/tx/bridge-receipt">Receipt on Polygon</a>`,
		"✅ <b>3/4 · SELL</b> · Polygon",
		"4.983 BASE → <b>1.007 QUOTE</b> · 1.934 s",
		"✅ <b>4/4 · RETURN QUOTE</b> · Polygon → Solana · 18.187 s",
		"<b>1.007 QUOTE</b> received",
		"💰 Return   <b>1.007 QUOTE</b>",
		"💸 Costs    0.4924 QUOTE",
		"📉 Net PnL  <b>-0.4851 QUOTE</b>",
		"⏱️ Total · 38.795 s",
	} {
		if !strings.Contains(final, expected) {
			t.Fatalf("completed message does not contain %q:\n%s", expected, final)
		}
	}
	for _, forbidden := range []string{
		"bootstrap",
		"bridge_internal_tag",
		"0.102409 QUOTE",
		"0.37061 QUOTE",
		"SELL DESTINATION",
		"sequenti",
	} {
		if strings.Contains(final, forbidden) {
			t.Fatalf("completed message contains %q:\n%s", forbidden, final)
		}
	}
}
