package sqlite_test

import (
	"context"
	"database/sql"
	"math/big"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sqliteadapter "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/inventory"
	"github.com/VarozXYZ/vernier/domain/market"
	_ "modernc.org/sqlite"
)

func TestOperationalStoreCommitsIdentityAndReservationWithoutSignedPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "operational.sqlite")
	store, err := sqliteadapter.OpenOperational(path, "FULL")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	buyInput := operationalAmount(t, "quote-a", 100)
	buyOutput := operationalAmount(t, "base-a", 145)
	sellInput := operationalAmount(t, "base-b", 140)
	sellOutput := operationalAmount(t, "quote-b", 101)
	nonce := uint64(7)
	operation := execution.Operation{
		ID: "operation", Plan: "plan", OpportunityID: "opportunity", ConfigHash: "config-hash",
		Technical: execution.StateCommitted, Economic: execution.EconomicReserved,
		CreatedAt: now, CommittedAt: now, Economics: operationalEconomics(t, now),
		Steps: []execution.OperationStep{
			{
				Leg: execution.Leg{
					ID: "buy", Side: execution.LegBuy, Chain: "solana", Account: "solana-account",
					Market: "remote", Input: buyInput, ExpectedOutput: buyOutput,
				},
				Identity: execution.TransactionIdentity{
					Chain: "solana", Account: "solana-account", Hash: "signature",
					Blockhash: "blockhash", LastValidBlockHeight: 123,
				},
				Technical: execution.StatePrepared, Economic: execution.EconomicReserved,
			},
			{
				Leg: execution.Leg{
					ID: "sell", Side: execution.LegSell, Chain: "evm", Account: "evm-account",
					Market: "local", Input: sellInput, ExpectedOutput: sellOutput,
				},
				Identity: execution.TransactionIdentity{
					Chain: "evm", Account: "evm-account", Hash: "0xhash", Nonce: &nonce,
				},
				Allocation: &execution.RouteAllocation{
					Input: sellInput, ExpectedOutput: sellOutput,
					Groups: []execution.RouteGroup{
						{
							ID: "direct", InputToken: "base-b", OutputToken: "quote-b",
							Branches: []execution.RouteBranch{
								{
									Market: "local", PlannedInput: big.NewInt(140),
									ExpectedOutput: big.NewInt(101),
								},
							},
						},
					},
				},
				Technical: execution.StatePrepared, Economic: execution.EconomicReserved,
			},
		},
	}
	owner, err := inventory.New(map[inventory.Key]market.TokenAmount{
		{Chain: "solana", Account: "solana-account", Token: "quote-a"}: operationalAmount(t, "quote-a", 1_000),
		{Chain: "evm", Account: "evm-account", Token: "base-b"}:        operationalAmount(t, "base-b", 1_000),
	})
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := owner.Reserve("operation", "operation", []inventory.Requirement{
		{Key: inventory.Key{Chain: "solana", Account: "solana-account", Token: "quote-a"}, Amount: buyInput},
		{Key: inventory.Key{Chain: "evm", Account: "evm-account", Token: "base-b"}, Amount: sellInput},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CommitPrepared(context.Background(), operation, reservation); err != nil {
		t.Fatal(err)
	}
	pending, err := store.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || len(pending[0].Steps) != 2 {
		t.Fatalf("pending = %+v", pending)
	}
	if pending[0].Steps[0].Identity.Hash == "" || pending[0].Steps[1].Identity.Nonce == nil {
		t.Fatalf("transaction identities were not recovered: %+v", pending[0].Steps)
	}
	if pending[0].Steps[1].Allocation == nil ||
		pending[0].Steps[1].Allocation.Groups[0].Branches[0].Market != "local" {
		t.Fatalf("route allocation was not recovered: %+v", pending[0].Steps[1].Allocation)
	}
	if pending[0].Economics.NetPnL.String() != "7/5" ||
		pending[0].Economics.BaseDelta.String() != "5" ||
		pending[0].Economics.Valuation.Price().RatString() != "1/10" {
		t.Fatalf("operation economics were not recovered: %+v", pending[0].Economics)
	}
	recoveredReservation, err := store.Reservation(context.Background(), operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recoveredReservation.ID != reservation.ID || len(recoveredReservation.Requirements()) != 2 {
		t.Fatalf("recovered reservation = %+v", recoveredReservation)
	}
	history, err := store.History(context.Background(), operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].Kind != execution.EventOperationCommitted {
		t.Fatalf("initial journal = %+v", history)
	}
	if err := store.RecordBroadcast(
		context.Background(), operation.ID, "sell",
		execution.StateBroadcastPossible, "synthetic-endpoint",
	); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordBroadcast(
		context.Background(), operation.ID, "sell",
		execution.StateBroadcastPossible, "synthetic-endpoint",
	); err != nil {
		t.Fatal(err)
	}
	for _, step := range operation.Steps {
		settlement := execution.Settlement{
			Operation: operation.ID, Step: step.Leg.ID, Identity: step.Identity,
			Technical: execution.StateConfirmedSuccess, Economic: execution.EconomicReserved,
			ObservedAt: now.Add(time.Second), Evidence: "receipt_without_amounts",
		}
		if err := store.RecordSettlement(context.Background(), settlement); err != nil {
			t.Fatal(err)
		}
		if err := store.RecordSettlement(context.Background(), settlement); err != nil {
			t.Fatal(err)
		}
	}
	history, err = store.History(context.Background(), operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 5 || history[4].Kind != execution.EventManualIntervention {
		t.Fatalf("idempotent journal has unexpected events: %+v", history)
	}
	pending, err = store.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Technical != execution.StateManualIntervention ||
		pending[0].Economic != execution.EconomicExposureOpen {
		t.Fatalf("technically successful but economically unverified operation was closed: %+v", pending)
	}
	settledOperation := operation
	settledOperation.ID = "operation-settled"
	settledOperation.Plan = "plan-settled"
	settledOperation.OpportunityID = "opportunity-settled"
	settledReservation, err := owner.Reserve(
		"operation-settled", "operation-settled", reservation.Requirements(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CommitPrepared(
		context.Background(), settledOperation, settledReservation,
	); err != nil {
		t.Fatal(err)
	}
	for _, step := range settledOperation.Steps {
		if err := store.RecordSettlement(context.Background(), execution.Settlement{
			Operation: settledOperation.ID, Step: step.Leg.ID, Identity: step.Identity,
			Technical: execution.StateConfirmedSuccess, Economic: execution.EconomicEffectVerified,
			ActualIn: step.Leg.Input, ActualOut: step.Leg.ExpectedOutput,
			ObservedAt: now.Add(2 * time.Second), Evidence: "verified_effect",
		}); err != nil {
			t.Fatal(err)
		}
	}
	pending, err = store.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("effect-verified operation became terminal before inventory settlement: %+v", pending)
	}
	if err := store.MarkSettled(context.Background(), settledOperation.ID); err != nil {
		t.Fatal(err)
	}
	pending, err = store.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].ID != operation.ID {
		t.Fatalf("settled operation remained recoverable: %+v", pending)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var reservationState, resolvedAt string
	if err := db.QueryRow(`SELECT state, resolved_at FROM inventory_reservations
		WHERE operation_id = ? LIMIT 1`, string(settledOperation.ID),
	).Scan(&reservationState, &resolvedAt); err != nil {
		t.Fatal(err)
	}
	if reservationState != "settled" || resolvedAt == "" {
		t.Fatalf("durable reservation state=%q resolved_at=%q", reservationState, resolvedAt)
	}
	var schema string
	if err := db.QueryRow(`SELECT group_concat(sql, ' ') FROM sqlite_master WHERE type = 'table'`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	lower := strings.ToLower(schema)
	for _, forbidden := range []string{"signed_payload", "raw_transaction", "private_key"} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("operational schema persists forbidden field %q", forbidden)
		}
	}
}

func operationalEconomics(t *testing.T, now time.Time) execution.OperationEconomics {
	t.Helper()
	valuation, err := arbitrage.NewValuationSnapshot(3, "base", "quote", big.NewRat(1, 10), 4, now)
	if err != nil {
		t.Fatal(err)
	}
	quantity := func(asset market.AssetID, value *big.Rat) market.AssetQuantity {
		result, quantityErr := market.NewAssetQuantity(asset, value)
		if quantityErr != nil {
			t.Fatal(quantityErr)
		}
		return result
	}
	return execution.OperationEconomics{
		Valuation:    valuation,
		QuoteDelta:   quantity("quote", big.NewRat(1, 1)),
		BaseDelta:    quantity("base", big.NewRat(5, 1)),
		MarkedBase:   quantity("quote", big.NewRat(1, 2)),
		GrossPnL:     quantity("quote", big.NewRat(3, 2)),
		Cost:         quantity("quote", big.NewRat(1, 10)),
		NetPnL:       quantity("quote", big.NewRat(7, 5)),
		Threshold:    quantity("quote", big.NewRat(1, 1)),
		DiscoveredAt: now.Add(-time.Millisecond),
		ValidatedAt:  now,
	}
}

func operationalAmount(t *testing.T, token market.TokenID, value int64) market.TokenAmount {
	t.Helper()
	amount, err := market.NewTokenAmount(token, big.NewInt(value))
	if err != nil {
		t.Fatal(err)
	}
	return amount
}
