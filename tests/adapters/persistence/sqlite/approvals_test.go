package sqlite_test

import (
	"context"
	"math/big"
	"path/filepath"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	"github.com/VarozXYZ/vernier/domain/execution"
	persistenceport "github.com/VarozXYZ/vernier/ports/persistence"
)

func TestApprovalJournalPersistsIdentityWithoutPayload(t *testing.T) {
	store, err := sqlite.OpenSequentialLive(filepath.Join(t.TempDir(), "live.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now().UTC()
	nonce := uint64(7)
	err = store.RecordApproval(context.Background(), persistenceport.ApprovalRecord{
		ID: "approval-1", Chain: "chain", Token: "token", Spender: "0xspender",
		Amount: big.NewInt(123), Identity: execution.TransactionIdentity{Chain: "chain", Account: "account", Hash: "0xhash", Nonce: &nonce},
		State: "prepared", CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetApprovalState(context.Background(), "approval-1", "confirmed", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
}
