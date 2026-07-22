package solana_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/chain/solana"
	"github.com/gorilla/websocket"
)

func TestReadOnlyNetworkReadsSlotsAndAccounts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Method string `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		switch request.Method {
		case "getHealth":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"ok"}`))
		case "getSlot":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":2,"result":42}`))
		case "getMultipleAccounts":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":3,"result":{"context":{"slot":42},"value":[{"lamports":9,"owner":"owner","executable":false,"rentEpoch":4,"data":["AQID","base64"]}]}}`))
		default:
			t.Fatalf("unexpected method %s", request.Method)
		}
	}))
	defer server.Close()
	network, err := solana.NewReadOnlyNetwork("solana", "test", server.URL, websocketURL(server.URL), server.Client(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := network.Validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	slot, err := network.CurrentSlot(context.Background())
	if err != nil || slot != 42 {
		t.Fatalf("slot=%d err=%v", slot, err)
	}
	account, err := network.ReadAccount(context.Background(), "account")
	if err != nil {
		t.Fatal(err)
	}
	if account.Lamports != 9 || string(account.Data) != "\x01\x02\x03" {
		t.Fatalf("unexpected account %+v", account)
	}
}

func TestLogsSubscriptionUsesMentionFilterAndPublishesSlot(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var request map[string]any
		if err := conn.ReadJSON(&request); err != nil {
			return
		}
		params, ok := request["params"].([]any)
		if !ok || len(params) == 0 {
			t.Errorf("missing params: %#v", request)
			return
		}
		filter, ok := params[0].(map[string]any)
		if !ok || filter["mentions"].([]any)[0] != "pool-account" {
			t.Errorf("unexpected filter: %#v", params[0])
		}
		_ = conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": request["id"], "result": 7})
		_ = conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "method": "logsNotification", "params": map[string]any{"result": map[string]any{"context": map[string]any{"slot": 99}, "value": map[string]any{"signature": "sig", "err": nil, "logs": []string{"log"}}}}})
		for {
			if _, _, err := conn.NextReader(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	network, err := solana.NewReadOnlyNetwork("solana", "test", "http://127.0.0.1:1", websocketURL(server.URL), server.Client(), nil)
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := network.SubscribeLogs(context.Background(), "pool-account")
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Unsubscribe()
	select {
	case notification := <-subscription.Notifications():
		if notification.Slot != 99 || notification.Signature != "sig" || len(notification.Logs) != 1 {
			t.Fatalf("unexpected notification %+v", notification)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for log notification")
	}
}

func websocketURL(httpURL string) string {
	parsed, _ := url.Parse(httpURL)
	parsed.Scheme = "ws"
	return parsed.String()
}
