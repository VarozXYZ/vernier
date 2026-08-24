package sqlite_test

import (
	"context"
	"math/big"
	"path/filepath"
	"testing"
	"time"

	sqlitestore "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	persistenceport "github.com/VarozXYZ/vernier/ports/persistence"
)

func TestRestorationJournalPersistsTwoQuoteJobsAndLatestTrigger(t *testing.T) {
	store, err := sqlitestore.OpenSequentialLive(filepath.Join(t.TempDir(), "live.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.SetBaseRestoration(ctx, "operation-1", true); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"quote-1", "quote-2"} {
		if err := store.StartQuoteRestoration(ctx, persistenceport.QuoteRestorationJob{
			ID: id, Operation: "operation-1", State: "pending", SourceChain: "a", DestinationChain: "b",
			InputToken: "quote-a", OutputToken: "quote-b", InputUnits: big.NewInt(1), CreatedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.StartQuoteRestoration(ctx, persistenceport.QuoteRestorationJob{
		ID: "quote-3", Operation: "operation-2", State: "pending", SourceChain: "a", DestinationChain: "b",
		InputToken: "quote-a", OutputToken: "quote-b", InputUnits: big.NewInt(1), CreatedAt: time.Now().UTC(),
	}); err == nil {
		t.Fatal("third quote restoration was persisted")
	}
	trigger := arbitrage.TriggerMetadata{Market: "market", Source: "source",
		Position:  market.SourcePosition{Kind: "block", Value: 42},
		Reference: market.SourceReference{Kind: "transaction", Value: "hash"}, At: time.Now().UTC()}
	if err := store.CoalesceReevaluation(ctx, trigger); err != nil {
		t.Fatal(err)
	}
	state, err := store.LoadRestoration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !state.BasePending || state.BaseOperation != "operation-1" || len(state.QuoteJobs) != 2 ||
		state.Reevaluation == nil || state.Reevaluation.Position.Value != 42 {
		t.Fatalf("unexpected restoration state: %+v", state)
	}
	if err := store.FinishQuoteRestoration(ctx, "quote-1", "delivered", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := store.StartQuoteRestoration(ctx, persistenceport.QuoteRestorationJob{
		ID: "quote-3", Operation: "operation-2", State: "pending", SourceChain: "a", DestinationChain: "b",
		InputToken: "quote-a", OutputToken: "quote-b", InputUnits: big.NewInt(1), CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
}
