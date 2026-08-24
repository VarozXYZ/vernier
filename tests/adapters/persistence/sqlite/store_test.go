package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
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

func TestStoreMigratesV1WindowsToPersistAppliedThreshold(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE opportunity_windows (
			window_id TEXT PRIMARY KEY,
			status TEXT NOT NULL,
			opened_at TEXT NOT NULL,
			best_net_asset TEXT NOT NULL
		);
		INSERT INTO opportunity_windows(window_id, status, opened_at, best_net_asset)
		VALUES ('legacy', 'closed', '2026-01-01T00:00:00Z', 'QUOTE');
		PRAGMA user_version = 1;`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version int
	var asset, value string
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT threshold_asset, threshold_value FROM opportunity_windows").Scan(&asset, &value); err != nil {
		t.Fatal(err)
	}
	if version != 9 || asset != "QUOTE" || value != "0" {
		t.Fatalf("migration result: version=%d threshold=%s/%s", version, asset, value)
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
	expected := []string{"executable_validation_rounds", "opportunity_tracking_points", "opportunity_tracking_windows", "opportunity_window_observations", "opportunity_windows", "simulation_legs", "simulation_rounds"}
	if !reflect.DeepEqual(tables, expected) {
		t.Fatalf("unexpected durable tables: %v", tables)
	}
}

func TestStorePersistsNormalizedExecutableValidationWithoutPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "validation.sqlite")
	store, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	round := &arbitrage.ExecutableValidationRound{ID: "window-1/validation/1", WindowID: "window-1", PointSequence: 1,
		Direction: arbitrage.Direction{BuyMarket: "synthetic-local", SellMarket: "synthetic-remote"}, Status: arbitrage.ValidationConfirmed,
		RequestedAt: now, BuildFinishedAt: now.Add(time.Millisecond), SimulationFinishedAt: now.Add(2 * time.Millisecond), RecalculatedAt: now.Add(3 * time.Millisecond),
		DiscoveryOutput: quantity(t, "QUOTE", "501"), BuildOutput: quantity(t, "QUOTE", "500.9"), DiscoveryNet: quantity(t, "QUOTE", "1"),
		FinalNet: quantity(t, "QUOTE", "0.9"), Threshold: quantity(t, "QUOTE", "0.75"), RemoteMarket: "synthetic-remote", LocalMarket: "synthetic-local",
		RouteHash: "aa", BuildHash: "bb", RouteHTTPStatus: 200, BuildHTTPStatus: 200, RouteDuration: time.Millisecond, BuildDuration: 2 * time.Millisecond, BuildAttempts: 1}
	if err := store.RecordExecutableValidationRound(context.Background(), round); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var status, routeHash, buildHash, finalNet string
	if err := db.QueryRow("SELECT status, route_hash, build_hash, final_net_value FROM executable_validation_rounds WHERE round_id = ?", round.ID).Scan(&status, &routeHash, &buildHash, &finalNet); err != nil {
		t.Fatal(err)
	}
	if status != string(arbitrage.ValidationConfirmed) || routeHash != "aa" || buildHash != "bb" || finalNet != "9/10" {
		t.Fatalf("unexpected validation row: %s %s %s %s", status, routeHash, buildHash, finalNet)
	}
	rows, err := db.Query("PRAGMA table_info(executable_validation_rounds)")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primary int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primary); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(name, "payload") || strings.Contains(name, "calldata") || strings.Contains(name, "raw") {
			t.Fatalf("forbidden payload column %q", name)
		}
	}
}

func TestStorePersistsSimulationRoundAndLegs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "simulation.sqlite")
	store, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	buyInput, err := market.ParseTokenAmount("USDT_SOLANA", "1000000000")
	if err != nil {
		t.Fatal(err)
	}
	buyLocal, err := market.ParseTokenAmount("ANTFUN_SOLANA", "22000000000")
	if err != nil {
		t.Fatal(err)
	}
	buySimulated, err := market.ParseTokenAmount("ANTFUN_SOLANA", "21990000000")
	if err != nil {
		t.Fatal(err)
	}
	sellInput, err := market.ParseTokenAmount("ANTFUN_BSC", "21990000000000000000000")
	if err != nil {
		t.Fatal(err)
	}
	sellLocal, err := market.ParseTokenAmount("USDT_BSC", "1002000000000000000000")
	if err != nil {
		t.Fatal(err)
	}
	sellSimulated, err := market.ParseTokenAmount("USDT_BSC", "1001000000000000000000")
	if err != nil {
		t.Fatal(err)
	}
	round := &arbitrage.SimulationRound{
		ID: "simulation-window-1-3", WindowID: "window-1", PointSequence: 3,
		RequestedAt: now, StartedAt: now.Add(2 * time.Millisecond), FinishedAt: now.Add(42 * time.Millisecond),
		Status: arbitrage.SimulationConfirmed, LocalQualified: true,
		LocalNetPnL: quantity(t, "USDT", "0.7"), LocalThreshold: quantity(t, "USDT", "0.5"),
		SimulatedNetPnL: quantity(t, "USDT", "0.6"),
		Buy: arbitrage.SimulationLeg{
			Chain: "solana", Market: "antfun_usdt_solana", Input: buyInput,
			LocalOutput: buyLocal, SimulatedOutput: buySimulated, Status: arbitrage.SimulationConfirmed,
			SnapshotVersion: 12, Context: "buy", ContextPosition: 5, GasOrComputeUnits: 180000,
			StartedAt: now.Add(3 * time.Millisecond), FinishedAt: now.Add(18 * time.Millisecond),
		},
		Sell: arbitrage.SimulationLeg{
			Chain: "bsc", Market: "antfun_usdt_bsc", Input: sellInput,
			LocalOutput: sellLocal, SimulatedOutput: sellSimulated, Status: arbitrage.SimulationConfirmed,
			SnapshotVersion: 27, Context: "sell", ContextPosition: 9, GasOrComputeUnits: 210000,
			StartedAt: now.Add(4 * time.Millisecond), FinishedAt: now.Add(35 * time.Millisecond),
		},
	}
	round.Buy.SnapshotHash[0] = 0xab
	round.Sell.SnapshotHash[0] = 0xcd
	if err := store.RecordSimulationRound(context.Background(), round); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var rounds, legs int
	if err := db.QueryRow("SELECT COUNT(*) FROM simulation_rounds").Scan(&rounds); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM simulation_legs").Scan(&legs); err != nil {
		t.Fatal(err)
	}
	if rounds != 1 || legs != 2 {
		t.Fatalf("unexpected simulation rows: rounds=%d legs=%d", rounds, legs)
	}
	var status, localValue, simulatedValue string
	if err := db.QueryRow("SELECT status, local_net_value, simulated_net_value FROM simulation_rounds WHERE round_id = ?", round.ID).
		Scan(&status, &localValue, &simulatedValue); err != nil {
		t.Fatal(err)
	}
	if status != string(arbitrage.SimulationConfirmed) || localValue != "7/10" || simulatedValue != "3/5" {
		t.Fatalf("unexpected simulation round: status=%s local=%s simulated=%s", status, localValue, simulatedValue)
	}
	var buyStatus, buyInputUnits, buyHash string
	if err := db.QueryRow("SELECT status, input_units, snapshot_hash FROM simulation_legs WHERE round_id = ? AND leg = 'buy'", round.ID).
		Scan(&buyStatus, &buyInputUnits, &buyHash); err != nil {
		t.Fatal(err)
	}
	if buyStatus != string(arbitrage.SimulationConfirmed) || buyInputUnits != "1000000000" ||
		buyHash != "ab00000000000000000000000000000000000000000000000000000000000000" {
		t.Fatalf("unexpected persisted buy leg: status=%s input=%s hash=%s", buyStatus, buyInputUnits, buyHash)
	}
}

func TestStorePersistsFixedCandidatePointsAndTimers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tracking.sqlite")
	store, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	trigger := arbitrage.TriggerMetadata{
		Market: "market-a", Source: "chain/pool-events",
		Position:  market.SourcePosition{Kind: "block", Value: 10},
		Reference: market.SourceReference{Kind: "transaction", Value: "0xsynthetic"}, At: now,
	}
	window := arbitrage.TrackingWindow{
		ID: "tracking-window", Run: "run", Strategy: "strategy", ConfigHash: "hash",
		Direction: arbitrage.Direction{BuyMarket: "market-a", SellMarket: "market-b"},
		Input:     quantity(t, "QUOTE", "1000"), FixedThreshold: quantity(t, "QUOTE", "0.3"),
		PercentageThreshold: quantity(t, "QUOTE", "0.5"), EffectiveThreshold: quantity(t, "QUOTE", "0.5"),
		Cost: quantity(t, "QUOTE", "0.5"), OpeningTrigger: trigger, OpenedAt: now,
		DiscoveryStartedAt: now, DiscoveryFinishedAt: now.Add(10 * time.Millisecond), OpeningPersistedAt: now.Add(12 * time.Millisecond),
		Opening:          arbitrage.Candidate{Input: quantity(t, "QUOTE", "1000"), Output: quantity(t, "QUOTE", "1002"), NetPnL: quantity(t, "QUOTE", "1.5")},
		OpeningBuyOutput: quantity(t, "BASE", "5000"),
		OpeningSnapshots: []arbitrage.TrackingSnapshot{{Market: "market-a", Version: 1}, {Market: "market-b", Version: 2}},
		DiscoveryTrace:   []arbitrage.TrackingDiscoveryDirection{{Direction: arbitrage.Direction{BuyMarket: "market-a", SellMarket: "market-b"}, Duration: 10 * time.Millisecond}},
	}
	if err := store.OpenTrackingWindow(context.Background(), &window); err != nil {
		t.Fatal(err)
	}
	point := &arbitrage.TrackingPoint{
		ID: "tracking-window/2", WindowID: window.ID, Sequence: 2, Evaluation: "evaluation-2", Trigger: trigger,
		Timestamps: arbitrage.TrackingTimestamps{
			TriggerReceivedAt: now.Add(time.Second), SnapshotCapturedAt: now.Add(time.Second + time.Millisecond),
			QueuedAt: now.Add(time.Second + time.Millisecond), EvaluationStartedAt: now.Add(time.Second + 2*time.Millisecond),
			BuyStartedAt: now.Add(time.Second + 2*time.Millisecond), BuyFinishedAt: now.Add(time.Second + 3*time.Millisecond),
			ConversionStartedAt: now.Add(time.Second + 3*time.Millisecond), ConversionFinishedAt: now.Add(time.Second + 4*time.Millisecond),
			SellStartedAt: now.Add(time.Second + 4*time.Millisecond), SellFinishedAt: now.Add(time.Second + 5*time.Millisecond),
			PnLStartedAt: now.Add(time.Second + 5*time.Millisecond), PnLFinishedAt: now.Add(time.Second + 6*time.Millisecond),
			PersistenceStartedAt: time.Now().UTC(), EvaluationFinishedAt: now.Add(time.Second + 6*time.Millisecond),
		},
		Snapshots: []arbitrage.TrackingSnapshot{{Market: "market-a", Version: 2}, {Market: "market-b", Version: 3}},
		Input:     quantity(t, "QUOTE", "1000"), BuyOutput: quantity(t, "BASE", "5000"), SellOutput: quantity(t, "QUOTE", "1001.5"),
		GrossPnL: quantity(t, "QUOTE", "1.5"), NetPnL: quantity(t, "QUOTE", "1"),
		FixedThreshold: quantity(t, "QUOTE", "0.3"), PercentageThreshold: quantity(t, "QUOTE", "0.5"), EffectiveThreshold: quantity(t, "QUOTE", "0.5"),
		DeltaFromOpening: quantity(t, "QUOTE", "-0.5"), DeltaFromPrevious: quantity(t, "QUOTE", "-0.5"),
		Classification: arbitrage.ClassificationPolicyQualified, EconomicChange: true,
	}
	time.Sleep(time.Millisecond)
	if err := store.RecordTrackingPoint(context.Background(), point); err != nil {
		t.Fatal(err)
	}
	if point.Timestamps.PersistenceFinishedAt.IsZero() || point.Timestamps.Durations().Persistence <= 0 {
		t.Fatalf("persistence timer was not returned: %+v", point.Timestamps)
	}
	if err := store.MarkTrackingNotificationEnqueued(context.Background(), window.ID, 2, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := store.CloseTrackingWindow(context.Background(), arbitrage.TrackingWindowClosing{
		WindowID: window.ID, Status: arbitrage.WindowStatusClosed, Reason: "below_profit_threshold",
		ClosedAt: now.Add(2 * time.Second), ClosingTriggerAt: now.Add(2 * time.Second), EconomicDuration: 2 * time.Second,
		ObservedDuration: 2*time.Second + 6*time.Millisecond, CumulativeCalculation: 16 * time.Millisecond,
		CumulativeQueue: time.Millisecond, MaximumQueue: time.Millisecond, Events: 2, EconomicChanges: 1,
		InitialPnL: quantity(t, "QUOTE", "1.5"), FinalPnL: quantity(t, "QUOTE", "1"), BestPnL: quantity(t, "QUOTE", "1.5"), WorstPnL: quantity(t, "QUOTE", "1"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count, persistenceNanos int64
	if err := db.QueryRow(`SELECT COUNT(*), MAX(persistence_nanos) FROM opportunity_tracking_points`).Scan(&count, &persistenceNanos); err != nil {
		t.Fatal(err)
	}
	if count != 1 || persistenceNanos <= 0 {
		t.Fatalf("tracking points=%d persistence=%d", count, persistenceNanos)
	}
}

func TestStoreFinalizesDanglingTrackingWindowForTelegramRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dangling-tracking.sqlite")
	store, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	trigger := arbitrage.TriggerMetadata{
		Market: "market-a", Source: "chain/pool-events", Position: market.SourcePosition{Kind: "block", Value: 10},
		Reference: market.SourceReference{Kind: "transaction", Value: "synthetic"}, At: now,
	}
	window := arbitrage.TrackingWindow{
		ID: "dangling", Run: "run", Strategy: "strategy", ConfigHash: "hash",
		Direction: arbitrage.Direction{BuyMarket: "market-a", SellMarket: "market-b"},
		Input:     quantity(t, "QUOTE", "1000"), FixedThreshold: quantity(t, "QUOTE", "0.3"),
		PercentageThreshold: quantity(t, "QUOTE", "0.5"), EffectiveThreshold: quantity(t, "QUOTE", "0.5"),
		Cost: quantity(t, "QUOTE", "0.5"), OpeningTrigger: trigger, OpenedAt: now,
		DiscoveryStartedAt: now, DiscoveryFinishedAt: now.Add(time.Millisecond), OpeningPersistedAt: now.Add(2 * time.Millisecond),
		Opening:          arbitrage.Candidate{Input: quantity(t, "QUOTE", "1000"), Output: quantity(t, "QUOTE", "1002"), NetPnL: quantity(t, "QUOTE", "1.5")},
		OpeningBuyOutput: quantity(t, "BASE", "5000"),
		OpeningSnapshots: []arbitrage.TrackingSnapshot{{Market: "market-a", Version: 1}, {Market: "market-b", Version: 1}},
	}
	if err := store.OpenTrackingWindow(context.Background(), &window); err != nil {
		t.Fatal(err)
	}
	if err := store.SetTrackingMessage(context.Background(), window.ID, 99); err != nil {
		t.Fatal(err)
	}
	closed, err := store.FinalizeDanglingTracking(context.Background(), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(closed) != 1 || closed[0].WindowID != window.ID || closed[0].MessageID != 99 ||
		closed[0].BuyOutput.String() != "5000" || closed[0].NetPnL.String() != "3/2" ||
		closed[0].CumulativeCalculation != time.Millisecond {
		t.Fatalf("unexpected dangling closure: %+v", closed)
	}
	again, err := store.FinalizeDanglingTracking(context.Background(), now.Add(2*time.Second))
	if err != nil || len(again) != 0 {
		t.Fatalf("dangling closure was not idempotent: %+v %v", again, err)
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
		}, HasBest: true, Threshold: quantity(t, "WETH", "0.05"),
		Classification: arbitrage.ClassificationEconomic, Status: arbitrage.WindowStatusOpen,
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
