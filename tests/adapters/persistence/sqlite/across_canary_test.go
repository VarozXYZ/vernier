package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	sqlitepersistence "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
)

func TestAcrossCanaryPersistsIdentityBeforeBroadcast(t *testing.T) {
	store, err := sqlitepersistence.OpenAcrossCanary(filepath.Join(t.TempDir(), "across.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	operation := sqlitepersistence.AcrossCanaryOperation{
		ID: "synthetic-operation", Direction: "evm-to-solana",
		AmountUnits: "1000000", ExpectedOutput: "999900",
		Status: "created", CreatedAt: time.Now().UTC(),
	}
	if err := store.Create(ctx, operation); err != nil {
		t.Fatal(err)
	}
	if err := store.Prepared(
		ctx, operation.ID, "synthetic-evm", "0xsynthetic",
		"synthetic-solana", "1000000",
	); err != nil {
		t.Fatal(err)
	}
	if err := store.Mark(ctx, operation.ID, "broadcast", nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Mark(ctx, operation.ID, "source_confirmed", nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Destination(ctx, operation.ID, "synthetic-fill", "1999900"); err != nil {
		t.Fatal(err)
	}
	if err := store.Mark(ctx, operation.ID, "completed", nil); err != nil {
		t.Fatal(err)
	}
	stored, err := store.Get(ctx, operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != "completed" || stored.SourceIdentity != "0xsynthetic" ||
		stored.DestinationIdentity != "synthetic-fill" || stored.BalanceAfter != "1999900" {
		t.Fatalf("unexpected stored operation: %+v", stored)
	}
}
