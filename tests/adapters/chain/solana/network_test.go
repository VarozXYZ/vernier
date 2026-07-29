package solana_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/chain/solana"
	"github.com/gorilla/websocket"
)

func TestReadOnlyNetworkSimulatesExactSignedTransaction(t *testing.T) {
	raw := []byte{1, 2, 3, 4}
	server := httptest.NewServer(http.HandlerFunc(
		func(writer http.ResponseWriter, request *http.Request) {
			var envelope struct {
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			if err := json.NewDecoder(request.Body).Decode(&envelope); err != nil {
				t.Fatal(err)
			}
			if envelope.Method != "simulateTransaction" ||
				len(envelope.Params) != 2 {
				t.Fatalf("request = %+v", envelope)
			}
			var encoded string
			var options map[string]any
			_ = json.Unmarshal(envelope.Params[0], &encoded)
			_ = json.Unmarshal(envelope.Params[1], &options)
			if encoded != base64.StdEncoding.EncodeToString(raw) ||
				options["encoding"] != "base64" ||
				options["sigVerify"] != true ||
				options["commitment"] != "confirmed" {
				t.Fatalf(
					"simulation payload=%q options=%v",
					encoded,
					options,
				)
			}
			_, _ = writer.Write([]byte(
				`{"jsonrpc":"2.0","id":1,"result":{"value":{"err":null,"logs":[],"unitsConsumed":123}}}`,
			))
		},
	))
	defer server.Close()
	network, err := solana.NewReadOnlyNetwork(
		"solana",
		"test",
		server.URL,
		websocketURL(server.URL),
		server.Client(),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := network.SimulateSignedTransaction(
		context.Background(),
		raw,
	); err != nil {
		t.Fatal(err)
	}
}

func TestReadOnlyNetworkCanSimulateNonBroadcastableReferenceTransaction(t *testing.T) {
	raw := []byte{4, 3, 2, 1}
	server := httptest.NewServer(http.HandlerFunc(
		func(writer http.ResponseWriter, request *http.Request) {
			var envelope struct {
				Method string            `json:"method"`
				Params []json.RawMessage `json:"params"`
			}
			if err := json.NewDecoder(request.Body).Decode(&envelope); err != nil {
				t.Fatal(err)
			}
			var options map[string]any
			if len(envelope.Params) != 2 ||
				json.Unmarshal(envelope.Params[1], &options) != nil ||
				options["sigVerify"] != false {
				t.Fatalf("simulation options=%v", options)
			}
			_, _ = writer.Write([]byte(
				`{"jsonrpc":"2.0","id":1,"result":{"value":{"err":null,"logs":[]}}}`,
			))
		},
	))
	defer server.Close()
	network, err := solana.NewReadOnlyNetwork(
		"solana",
		"test",
		server.URL,
		websocketURL(server.URL),
		server.Client(),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := network.SimulateTransactionWithoutSignatureVerification(
		context.Background(),
		raw,
	); err != nil {
		t.Fatal(err)
	}
}

func TestReadOnlyNetworkRejectsFailedSimulation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(writer http.ResponseWriter, _ *http.Request) {
			_, _ = writer.Write([]byte(
				`{"jsonrpc":"2.0","id":1,"result":{"value":{"err":{"InstructionError":[2,{"Custom":1}]},"logs":["failed"]}}}`,
			))
		},
	))
	defer server.Close()
	network, err := solana.NewReadOnlyNetwork(
		"solana",
		"test",
		server.URL,
		websocketURL(server.URL),
		server.Client(),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := network.SimulateSignedTransaction(
		context.Background(),
		[]byte{1},
	); err == nil {
		t.Fatal("failed simulation was accepted")
	}
}

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

func TestReadOnlyNetworkReadsProgramAccounts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Method string `json:"method"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if request.Method != "getProgramAccounts" {
			t.Fatalf("unexpected method %s", request.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":[{"pubkey":"account","account":{"lamports":9,"owner":"owner","executable":false,"rentEpoch":4,"data":["AQID","base64"]}}]}`))
	}))
	defer server.Close()
	network, err := solana.NewReadOnlyNetwork("solana", "test", server.URL, websocketURL(server.URL), server.Client(), nil)
	if err != nil {
		t.Fatal(err)
	}
	size := uint64(3)
	accounts, err := network.ReadProgramAccounts(context.Background(), "program", []solana.ProgramFilter{{DataSize: &size}})
	if err != nil {
		t.Fatal(err)
	}
	if len(accounts) != 1 || accounts[0].Account != "account" || string(accounts[0].Value.Data) != "\x01\x02\x03" {
		t.Fatalf("unexpected program accounts: %+v", accounts)
	}
}

func TestLogsSubscriptionKeepsIdleWebSocketAliveWithPings(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	serverErrors := make(chan error, 1)
	connectionClosed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer close(connectionClosed)
		defer conn.Close()
		var request map[string]any
		if err := conn.ReadJSON(&request); err != nil {
			serverErrors <- err
			return
		}
		if err := conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": request["id"], "result": 7}); err != nil {
			serverErrors <- err
			return
		}
		pings := 0
		conn.SetPingHandler(func(data string) error {
			pings++
			if err := conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(time.Second)); err != nil {
				return err
			}
			if pings == 2 {
				return conn.WriteJSON(map[string]any{
					"jsonrpc": "2.0",
					"method":  "logsNotification",
					"params": map[string]any{"result": map[string]any{
						"context": map[string]any{"slot": 99},
						"value":   map[string]any{"signature": "after-ping", "err": nil, "logs": []string{"log"}},
					}},
				})
			}
			return nil
		})
		for {
			if _, _, err := conn.NextReader(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	network, err := solana.NewReadOnlyNetworkWithKeepalive(
		"solana",
		"test",
		"http://127.0.0.1:1",
		websocketURL(server.URL),
		server.Client(),
		nil,
		solana.WebSocketKeepalive{
			PingInterval: 10 * time.Millisecond,
			PongTimeout:  80 * time.Millisecond,
			WriteTimeout: 20 * time.Millisecond,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := network.SubscribeLogs(context.Background(), "pool-account")
	if err != nil {
		t.Fatal(err)
	}

	select {
	case notification := <-subscription.Notifications():
		if notification.Slot != 99 || notification.Signature != "after-ping" {
			t.Fatalf("unexpected notification after keepalive pings: %+v", notification)
		}
	case err := <-serverErrors:
		t.Fatalf("server failed before keepalive notification: %v", err)
	case err := <-subscription.Err():
		t.Fatalf("subscription failed while server acknowledged pings: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for notification after keepalive pings")
	}
	subscription.Unsubscribe()
	select {
	case <-connectionClosed:
	case <-time.After(time.Second):
		t.Fatal("unsubscribe did not stop the keepalive connection")
	}
	deadline := time.After(time.Second)
	for {
		select {
		case _, open := <-subscription.Err():
			if !open {
				return
			}
		case <-deadline:
			t.Fatal("subscription reader did not stop after unsubscribe")
		}
	}
}

func TestLogsSubscriptionReportsMissedPong(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	pingSeen := make(chan struct{}, 1)
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
		if err := conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": request["id"], "result": 7}); err != nil {
			return
		}
		conn.SetPingHandler(func(string) error {
			select {
			case pingSeen <- struct{}{}:
			default:
			}
			return nil
		})
		for {
			if _, _, err := conn.NextReader(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	network, err := solana.NewReadOnlyNetworkWithKeepalive(
		"solana",
		"test",
		"http://127.0.0.1:1",
		websocketURL(server.URL),
		server.Client(),
		nil,
		solana.WebSocketKeepalive{
			PingInterval: 10 * time.Millisecond,
			PongTimeout:  50 * time.Millisecond,
			WriteTimeout: 20 * time.Millisecond,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := network.SubscribeLogs(context.Background(), "pool-account")
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Unsubscribe()

	select {
	case <-pingSeen:
	case <-time.After(time.Second):
		t.Fatal("client did not send a keepalive ping")
	}
	select {
	case err := <-subscription.Err():
		if err == nil {
			t.Fatal("missed pong closed the error channel without an error")
		}
	case <-time.After(time.Second):
		t.Fatal("missed pong did not fail the subscription")
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

func TestTransactionSubscriptionUsesConfirmedFullDetails(t *testing.T) {
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
		if request["method"] != "transactionSubscribe" {
			t.Errorf("method = %#v", request["method"])
			return
		}
		params, ok := request["params"].([]any)
		if !ok || len(params) != 2 {
			t.Errorf("params = %#v", request["params"])
			return
		}
		filter := params[0].(map[string]any)
		options := params[1].(map[string]any)
		if filter["accountInclude"].([]any)[0] != "wallet" ||
			options["commitment"] != "confirmed" ||
			options["transactionDetails"] != "full" {
			t.Errorf("unexpected transaction subscription: %#v", params)
			return
		}
		_ = conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": request["id"], "result": 9})
		_ = conn.WriteJSON(map[string]any{
			"jsonrpc": "2.0", "method": "transactionNotification",
			"params": map[string]any{"result": map[string]any{
				"signature": "signature", "slot": 55,
				"transaction": map[string]any{
					"transaction": map[string]any{"signatures": []string{"signature"}},
					"meta":        map[string]any{"err": nil},
				},
			}},
		})
		for {
			if _, _, err := conn.NextReader(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	network, err := solana.NewReadOnlyNetwork(
		"solana", "test", "http://127.0.0.1:1", websocketURL(server.URL), server.Client(), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := network.SubscribeTransactions(context.Background(), "wallet")
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Unsubscribe()
	select {
	case notification := <-subscription.Notifications():
		if notification.Signature != "signature" || notification.Slot != 55 ||
			len(notification.Meta) == 0 {
			t.Fatalf("notification = %+v", notification)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for transaction notification")
	}
}

func TestAccountSubscriptionPublishesAccountDataAndSlot(t *testing.T) {
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
		if request["method"] != "accountSubscribe" {
			t.Errorf("unexpected method: %#v", request["method"])
			return
		}
		_ = conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": request["id"], "result": 8})
		_ = conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "method": "accountNotification", "params": map[string]any{"result": map[string]any{"context": map[string]any{"slot": 123}, "value": map[string]any{"lamports": 7, "owner": "owner", "executable": false, "rentEpoch": 1, "data": []string{"AQID", "base64"}}}}})
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
	subscription, err := network.SubscribeAccount(context.Background(), "pool-account")
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Unsubscribe()
	select {
	case notification := <-subscription.Notifications():
		if notification.Slot != 123 || notification.Account != "pool-account" || notification.Value.Lamports != 7 || string(notification.Value.Data) != "\x01\x02\x03" {
			t.Fatalf("unexpected notification %+v", notification)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for account notification")
	}
}

func TestProgramSubscriptionPublishesAccountDataAndSlot(t *testing.T) {
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
		if request["method"] != "programSubscribe" {
			t.Errorf("unexpected method: %#v", request["method"])
			return
		}
		_ = conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": request["id"], "result": 8})
		_ = conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "method": "programNotification", "params": map[string]any{"result": map[string]any{"context": map[string]any{"slot": 321}, "value": map[string]any{"pubkey": "tick-account", "account": map[string]any{"lamports": 7, "owner": "owner", "executable": false, "rentEpoch": 1, "data": []string{"AQID", "base64"}}}}}})
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
	subscription, err := network.SubscribeProgram(context.Background(), solana.ProgramSubscriptionRequest{Program: "program"})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Unsubscribe()
	select {
	case notification := <-subscription.Notifications():
		if notification.Slot != 321 || notification.Account != "tick-account" || notification.Value.Lamports != 7 || string(notification.Value.Data) != "\x01\x02\x03" {
			t.Fatalf("unexpected notification %+v", notification)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for program notification")
	}
}

func websocketURL(httpURL string) string {
	parsed, _ := url.Parse(httpURL)
	parsed.Scheme = "ws"
	return parsed.String()
}
