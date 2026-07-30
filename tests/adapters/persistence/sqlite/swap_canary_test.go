package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	sqlitestore "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
)

func TestSwapCanaryPersistsIdentityBeforeBroadcast(t *testing.T) {
	store, err := sqlitestore.OpenSwapCanary(
		filepath.Join(t.TempDir(), "swap-canary.sqlite"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	operation := sqlitestore.SwapCanaryOperation{
		ID: "swap-synthetic", Provider: "synthetic-provider",
		Market: "synthetic-market", Side: "buy", AmountUnits: "1000000",
		Status: "created", CreatedAt: time.Now().UTC(),
	}
	if err := store.Create(ctx, operation); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(ctx, operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Status != "created" || loaded.Identity != "" || loaded.Chain != "" {
		t.Fatalf("unexpected created operation: %#v", loaded)
	}
	if err := store.Prepared(
		ctx,
		operation.ID,
		"synthetic-chain",
		"synthetic-hash",
	); err != nil {
		t.Fatal(err)
	}
	loaded, err = store.Load(ctx, operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Status != "prepared" || loaded.Identity != "synthetic-hash" {
		t.Fatalf("prepared identity was not durable: %#v", loaded)
	}
	if err := store.Mark(ctx, operation.ID, "broadcast", nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Mark(ctx, operation.ID, "confirmed", nil); err != nil {
		t.Fatal(err)
	}
	loaded, err = store.Load(ctx, operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Status != "confirmed" {
		t.Fatalf("unexpected final state: %#v", loaded)
	}
}

func TestSwapCanaryRejectsInvalidTransitionAndDuplicateIdentity(t *testing.T) {
	store, err := sqlitestore.OpenSwapCanary(
		filepath.Join(t.TempDir(), "swap-canary.sqlite"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	for _, id := range []string{"swap-one", "swap-two"} {
		if err := store.Create(ctx, sqlitestore.SwapCanaryOperation{
			ID: id, Provider: "synthetic-provider", Market: "synthetic-market",
			Side: "sell", AmountUnits: "7", Status: "created", CreatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Mark(ctx, "swap-one", "confirmed", nil); err == nil {
		t.Fatal("created operation became confirmed without broadcast")
	}
	if err := store.Prepared(
		ctx,
		"swap-one",
		"synthetic-chain",
		"synthetic-hash",
	); err != nil {
		t.Fatal(err)
	}
	if err := store.Prepared(
		ctx,
		"swap-two",
		"synthetic-chain",
		"synthetic-hash",
	); err == nil {
		t.Fatal("duplicate transaction identity was accepted")
	}
}
