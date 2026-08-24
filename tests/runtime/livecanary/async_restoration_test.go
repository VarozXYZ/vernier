package livecanary_test

import (
	"context"
	"errors"
	"math/big"
	"path/filepath"
	"testing"
	"time"

	sqlitestore "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	"github.com/VarozXYZ/vernier/runtime/livecanary"
)

type unusedRestorationDriver struct{}

func (unusedRestorationDriver) ExecuteStage(
	context.Context,
	execution.SequentialStageRequest,
	executionport.SequentialJournal,
) (execution.SequentialStageSettlement, error) {
	panic("unexpected restoration execution")
}

type flakyRestorationJournal struct {
	*sqlitestore.SequentialLiveStore
	finishFailures int
	finishCalls    int
}

func (j *flakyRestorationJournal) RecordStageSettlement(
	context.Context,
	execution.SequentialStageSettlement,
) error {
	return nil
}

func (j *flakyRestorationJournal) FinishQuoteRestoration(
	ctx context.Context,
	id string,
	state string,
	at time.Time,
) error {
	j.finishCalls++
	if j.finishFailures > 0 {
		j.finishFailures--
		return errors.New("transient sqlite contention")
	}
	return j.SequentialLiveStore.FinishQuoteRestoration(ctx, id, state, at)
}

type deliveredRestorationDriver struct{}

func (deliveredRestorationDriver) ExecuteStage(
	_ context.Context,
	request execution.SequentialStageRequest,
	_ executionport.SequentialJournal,
) (execution.SequentialStageSettlement, error) {
	output, _ := market.NewTokenAmount(request.Stage.OutputToken, big.NewInt(9))
	destination := execution.TransactionIdentity{Chain: request.Stage.DestinationChain, Account: "account", Hash: "destination"}
	return execution.SequentialStageSettlement{
		Request: request, ActualInput: request.Input, ActualOutput: output,
		SourceIdentity:      execution.TransactionIdentity{Chain: request.Stage.SourceChain, Account: "account", Hash: "source"},
		DestinationIdentity: &destination, ObservedAt: time.Now().UTC(), Evidence: "test-delivery",
	}, nil
}

func TestBoundedRunRejectsPersistedIncompleteRestoration(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitestore.OpenSequentialLive(filepath.Join(t.TempDir(), "operational.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.SetBaseRestoration(ctx, "operation-1", true); err != nil {
		t.Fatal(err)
	}
	restorer, err := livecanary.NewAsyncQuoteRestorer(livecanary.AsyncQuoteRestorerConfig{
		Context: ctx,
		Journal: store,
		Driver:  unusedRestorationDriver{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := restorer.WaitForPending(ctx); err == nil {
		t.Fatal("bounded run accepted an incomplete critical restoration")
	}
}

func TestQuoteRestorationRetriesDurableCompletionBeforeReleasingCapacity(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitestore.OpenSequentialLive(filepath.Join(t.TempDir(), "operational.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	journal := &flakyRestorationJournal{SequentialLiveStore: store, finishFailures: 2}
	restorer, err := livecanary.NewAsyncQuoteRestorer(livecanary.AsyncQuoteRestorerConfig{
		Context: ctx, Journal: journal, Driver: deliveredRestorationDriver{},
	})
	if err != nil {
		t.Fatal(err)
	}
	input, _ := market.NewTokenAmount("quote-b", big.NewInt(10))
	request := execution.SequentialStageRequest{
		Operation: "operation-1", Plan: "plan-1", Input: input,
		Stage: execution.SequentialStagePlan{
			Ordinal: 4, Stage: execution.StageBridgeQuoteReturn,
			SourceChain: "chain-b", DestinationChain: "chain-a",
			InputToken: "quote-b", OutputToken: "quote-a",
		},
	}
	if err := restorer.Start(ctx, request, deliveredRestorationDriver{}, journal); err != nil {
		t.Fatal(err)
	}
	if err := restorer.WaitForPending(ctx); err != nil {
		t.Fatal(err)
	}
	if journal.finishCalls != 3 {
		t.Fatalf("finish calls = %d, want 3", journal.finishCalls)
	}
	state, err := store.LoadRestoration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.QuoteJobs) != 0 {
		t.Fatalf("active quote restorations = %d, want 0", len(state.QuoteJobs))
	}
}
