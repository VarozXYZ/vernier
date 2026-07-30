package sqlite_test

import (
	"context"
	"math/big"
	"path/filepath"
	"testing"
	"time"

	sqlitestore "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

func TestRefuelJournalKeepsUncertainIdentityAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "live.sqlite")
	store, err := sqlitestore.OpenSequentialLive(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Round(0)
	record := refuelRecord(t, now)
	if err := store.CreateRefuel(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRefuelBroadcast(
		context.Background(),
		record.ID,
		record.Identity,
	); err != nil {
		t.Fatal(err)
	}
	record.State = executionport.RefuelOutcomeUnknown
	record.LastError = "RPC confirmation timed out"
	if err := store.FinishRefuel(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := sqlitestore.OpenSequentialLive(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	active, found, err := reopened.ActiveRefuel(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !found || active.ID != record.ID ||
		active.Identity.Hash != record.Identity.Hash ||
		active.State != executionport.RefuelOutcomeUnknown {
		t.Fatalf("active refuel = %#v found=%t", active, found)
	}
}

func TestRefuelJournalCompletesReconciledUnknownOperation(t *testing.T) {
	store, err := sqlitestore.OpenSequentialLive(
		filepath.Join(t.TempDir(), "live.sqlite"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	record := refuelRecord(t, time.Now().UTC())
	if err := store.CreateRefuel(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	record.State = executionport.RefuelOutcomeUnknown
	if err := store.FinishRefuel(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	record.State = executionport.RefuelCompleted
	record.BalanceAfter, _ = market.NewAssetQuantity("pol", big.NewRat(11, 1))
	record.NativeReceived, _ = market.NewAssetQuantity("pol", big.NewRat(10, 1))
	record.Fee, _ = market.NewAssetQuantity("pol", big.NewRat(1, 100))
	if err := store.FinishRefuel(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.ActiveRefuel(
		context.Background(),
	); err != nil || found {
		t.Fatalf("active found=%t err=%v", found, err)
	}
	completed, found, err := store.LastCompletedRefuel(
		context.Background(),
		"polygon",
	)
	if err != nil || !found || completed.NativeReceived.String() != "10" {
		t.Fatalf("completed=%#v found=%t err=%v", completed, found, err)
	}
}

func refuelRecord(t *testing.T, now time.Time) executionport.RefuelRecord {
	t.Helper()
	input, _ := market.NewTokenAmount("usdc", big.NewInt(10_000_000))
	before, _ := market.NewAssetQuantity("pol", big.NewRat(1, 1))
	nonce := uint64(17)
	return executionport.RefuelRecord{
		ID: "refuel-test", Chain: "polygon",
		State: executionport.RefuelPrepared,
		Input: input, NativeAsset: "pol", BalanceBefore: before,
		Identity: execution.TransactionIdentity{
			Chain: "polygon", Account: "account",
			Hash:  "0x0000000000000000000000000000000000000000000000000000000000000011",
			Nonce: &nonce,
		},
		CreatedAt: now, UpdatedAt: now,
	}
}
