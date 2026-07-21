package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	_ "modernc.org/sqlite"
)

func TestStorePersistsWindowLifecycleIdempotently(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "opportunities.sqlite")
	store, err := sqlite.Open(storePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	window := testWindow(t, "window-1", time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC))
	if err := store.OpenWindow(context.Background(), arbitrage.WindowOpening{Window: window}); err != nil {
		t.Fatal(err)
	}
	if err := store.OpenWindow(context.Background(), arbitrage.WindowOpening{Window: window}); err != nil {
		t.Fatalf("idempotent open: %v", err)
	}

	observation := arbitrage.WindowObservation{
		ID: "observation-1", WindowID: window.ID, Evaluation: "evaluation-2",
		ObservedAt: window.OpenedAt.Add(time.Second), Classification: arbitrage.ClassificationPolicyQualified,
		Candidate: arbitrage.WindowCandidate{
			Size:     quantity(t, "WETH", "2"),
			GrossPnL: quantity(t, "WETH", "0.4"),
			NetPnL:   quantity(t, "WETH", "0.3"),
			Cost:     quantity(t, "WETH", "0.1"),
		}, HasCandidate: true, Best: true,
	}
	if err := store.RecordImprovement(context.Background(), observation); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordImprovement(context.Background(), observation); err != nil {
		t.Fatalf("idempotent observation: %v", err)
	}
	conflictingObservation := observation
	conflictingObservation.Candidate.NetPnL = quantity(t, "WETH", "0.31")
	if err := store.RecordImprovement(context.Background(), conflictingObservation); err == nil {
		t.Fatal("conflicting observation was accepted")
	}

	closedAt := observation.ObservedAt.Add(time.Second)
	if err := store.CloseWindow(context.Background(), arbitrage.WindowClosing{
		WindowID: window.ID, ClosedAt: closedAt, LastProfitableAt: observation.ObservedAt,
		Classification: arbitrage.ClassificationObservedSpread, Reason: "profitability_lost",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.CloseWindow(context.Background(), arbitrage.WindowClosing{
		WindowID: window.ID, ClosedAt: closedAt, LastProfitableAt: observation.ObservedAt,
		Classification: arbitrage.ClassificationObservedSpread, Reason: "profitability_lost",
	}); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
	if err := store.CloseWindow(context.Background(), arbitrage.WindowClosing{
		WindowID: window.ID, ClosedAt: closedAt.Add(time.Second), LastProfitableAt: observation.ObservedAt,
		Classification: arbitrage.ClassificationObservedSpread, Reason: "profitability_lost",
	}); err == nil {
		t.Fatal("conflicting close was accepted")
	}

	records, err := store.ListWindows(context.Background(), arbitrage.WindowQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Window.Status != arbitrage.WindowStatusClosed {
		t.Fatalf("unexpected records: %+v", records)
	}
	if len(records[0].Observations) != 1 || records[0].Observations[0].ID != observation.ID {
		t.Fatalf("unexpected observations: %+v", records[0].Observations)
	}
	if records[0].Window.Best.NetPnL.String() != "3/10" {
		t.Fatalf("best net PnL: got %s", records[0].Window.Best.NetPnL.String())
	}
}

func TestStoreFinalizesDanglingWindowsWithoutRecoveryTables(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "opportunities.sqlite")
	store, err := sqlite.Open(storePath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	window := testWindow(t, "window-2", time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC))
	if err := store.OpenWindow(context.Background(), arbitrage.WindowOpening{Window: window}); err != nil {
		t.Fatal(err)
	}
	if err := store.FinalizeDangling(context.Background(), window.OpenedAt.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	records, err := store.ListWindows(context.Background(), arbitrage.WindowQuery{Status: arbitrage.WindowStatusFailed})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Window.CloseReason != "process_interrupted" || !records[0].Window.Degraded {
		t.Fatalf("unexpected dangling-window record: %+v", records)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", storePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(tables) != 2 || tables[0] != "opportunity_window_observations" || tables[1] != "opportunity_windows" {
		t.Fatalf("unexpected durable tables: %v", tables)
	}
}

func testWindow(t *testing.T, id string, openedAt time.Time) arbitrage.OpportunityWindow {
	t.Helper()
	trigger := arbitrage.TriggerMetadata{
		Market: "robinhood", Source: "robinhood/pool-logs",
		Position:  market.SourcePosition{Kind: "block", Value: 100},
		Reference: market.SourceReference{Kind: "evm_block_hash", Value: "0xabc"}, At: openedAt,
	}
	window := arbitrage.OpportunityWindow{
		ID: arbitrage.WindowID(id), Run: "run-1", Strategy: "strategy-1", ConfigHash: "hash-1",
		Direction: arbitrage.Direction{BuyMarket: "robinhood", SellMarket: "base"},
		Trigger:   trigger, HasTrigger: true, OpenedAt: openedAt,
		FirstProfitableAt: openedAt, LastProfitableAt: openedAt,
		Best: arbitrage.WindowCandidate{
			Size: quantity(t, "WETH", "1"), GrossPnL: quantity(t, "WETH", "0.2"),
			NetPnL: quantity(t, "WETH", "0.1"), Cost: quantity(t, "WETH", "0.1"),
		}, HasBest: true, Classification: arbitrage.ClassificationEconomic, Status: arbitrage.WindowStatusOpen,
	}
	if err := window.Validate(); err != nil {
		t.Fatal(err)
	}
	return window
}

func quantity(t *testing.T, asset, value string) market.AssetQuantity {
	t.Helper()
	quantity, err := market.ParseAssetQuantity(market.AssetID(asset), value)
	if err != nil {
		t.Fatal(err)
	}
	return quantity
}
