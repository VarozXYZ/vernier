package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	sqlitestore "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
)

func TestNTTCanaryPersistsIdentityBeforeBroadcastState(t *testing.T) {
	store, err := sqlitestore.OpenNTTCanary(
		filepath.Join(t.TempDir(), "canary.sqlite"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	operation := sqlitestore.NTTCanaryOperation{
		ID: "synthetic-operation", Direction: "solana-to-evm",
		AmountUnits: "1000", Stage: "created", CreatedAt: now,
	}
	if err := store.Create(ctx, operation); err != nil {
		t.Fatal(err)
	}
	transaction := sqlitestore.NTTCanaryTransaction{
		OperationID: operation.ID, Ordinal: 1, Phase: "source_transfer",
		Chain: "solana", Identity: "synthetic-signature",
		Blockhash: "synthetic-blockhash", LastValidBlockHeight: 123,
		Status: "prepared", CreatedAt: now,
	}
	if err := store.RecordPrepared(ctx, transaction); err != nil {
		t.Fatal(err)
	}
	loaded, transactions, err := store.Load(ctx, operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Stage != "source_transfer_prepared" || len(transactions) != 1 {
		t.Fatalf("unexpected prepared journal: %#v %#v", loaded, transactions)
	}
	if transactions[0].Status != "prepared" ||
		transactions[0].Identity != transaction.Identity {
		t.Fatalf("prepared identity was not durable: %#v", transactions[0])
	}
	if err := store.MarkTransaction(ctx, operation.ID, 1, "broadcast"); err != nil {
		t.Fatal(err)
	}
	_, transactions, err = store.Load(ctx, operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if transactions[0].Status != "broadcast" {
		t.Fatalf("unexpected broadcast journal: %#v", transactions[0])
	}
	transactionMetrics := sqlitestore.NTTCanaryTransactionMetrics{
		OperationID: operation.ID, Ordinal: 1,
		PrepareDuration:      11 * time.Millisecond,
		BroadcastDuration:    12 * time.Millisecond,
		ConfirmationDuration: 13 * time.Millisecond,
		TotalDuration:        36 * time.Millisecond,
		NetworkFeeUnits:      "5000", FeeAsset: "lamports",
		AdditionalDebitUnits: "2039280", ComputeUnits: 4242,
	}
	if err := store.RecordTransactionMetrics(ctx, transactionMetrics); err != nil {
		t.Fatal(err)
	}
	operationMetrics := sqlitestore.NTTCanaryOperationMetrics{
		OperationID: operation.ID, Mode: "fresh",
		ReadinessDuration:   10 * time.Millisecond,
		SourceDuration:      36 * time.Millisecond,
		AttestationDuration: 2 * time.Second,
		DestinationDuration: 50 * time.Millisecond,
		BridgeDuration:      2086 * time.Millisecond,
		CommandDuration:     2096 * time.Millisecond,
		EVMNetworkFeeWei:    "123", EVMValueWei: "456",
		SolanaFeeLamports: "5000", SolanaDebitLamports: "2044280",
	}
	if err := store.RecordOperationMetrics(ctx, operationMetrics); err != nil {
		t.Fatal(err)
	}
	loadedMetrics, phaseMetrics, err := store.LoadMetrics(ctx, operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loadedMetrics.BridgeDuration != operationMetrics.BridgeDuration ||
		loadedMetrics.SolanaDebitLamports != operationMetrics.SolanaDebitLamports ||
		len(phaseMetrics) != 1 ||
		phaseMetrics[0].ComputeUnits != transactionMetrics.ComputeUnits ||
		phaseMetrics[0].ConfirmationDuration !=
			transactionMetrics.ConfirmationDuration {
		t.Fatalf(
			"unexpected durable metrics: %#v %#v",
			loadedMetrics,
			phaseMetrics,
		)
	}
}

func TestNTTCanaryRejectsDuplicateTransactionIdentity(t *testing.T) {
	store, err := sqlitestore.OpenNTTCanary(
		filepath.Join(t.TempDir(), "canary.sqlite"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	for _, id := range []string{"operation-one", "operation-two"} {
		if err := store.Create(ctx, sqlitestore.NTTCanaryOperation{
			ID: id, Direction: "evm-to-solana", AmountUnits: "1",
			Stage: "created", CreatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	first := sqlitestore.NTTCanaryTransaction{
		OperationID: "operation-one", Ordinal: 1, Phase: "source_transfer",
		Chain: "evm", Identity: "0xsynthetic", Nonce: "7",
		Status: "prepared", CreatedAt: now,
	}
	if err := store.RecordPrepared(ctx, first); err != nil {
		t.Fatal(err)
	}
	first.OperationID = "operation-two"
	if err := store.RecordPrepared(ctx, first); err == nil {
		t.Fatal("duplicate transaction identity was accepted")
	}
}

func TestNTTCanaryReusesOnlyUnbroadcastReadinessFailure(t *testing.T) {
	store, err := sqlitestore.OpenNTTCanary(
		filepath.Join(t.TempDir(), "canary.sqlite"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	operation := sqlitestore.NTTCanaryOperation{
		ID: "readiness-retry", Direction: "solana-to-evm",
		AmountUnits: "1000", Stage: "created", CreatedAt: now,
	}
	if err := store.Create(ctx, operation); err != nil {
		t.Fatal(err)
	}
	if err := store.Fail(
		ctx, operation.ID, "readiness_failed",
		context.DeadlineExceeded,
	); err != nil {
		t.Fatal(err)
	}
	operation.CreatedAt = now.Add(time.Second)
	if err := store.CreateOrReuseUnbroadcast(ctx, operation); err != nil {
		t.Fatal(err)
	}
	loaded, transactions, err := store.Load(ctx, operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Stage != "created" || loaded.LastError != "" ||
		len(transactions) != 0 {
		t.Fatalf("unexpected reused operation: %#v %#v", loaded, transactions)
	}
	if err := store.RecordPrepared(ctx, sqlitestore.NTTCanaryTransaction{
		OperationID: operation.ID, Ordinal: 1, Phase: "source_transfer",
		Chain: "solana", Identity: "prepared-signature",
		Status: "prepared", CreatedAt: now.Add(2 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateOrReuseUnbroadcast(
		ctx, operation,
	); err == nil {
		t.Fatal("operation with a durable identity was reused")
	}
}
