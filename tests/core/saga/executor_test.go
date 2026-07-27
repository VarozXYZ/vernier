package saga_test

import (
	"context"
	"errors"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/core/saga"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/inventory"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

type blockingOperationalStore struct {
	commitStarted chan struct{}
	allowCommit   chan struct{}
	committed     atomic.Bool
	broadcasts    atomic.Int32
	manual        atomic.Int32
	noExecution   atomic.Int32
}

func (s *blockingOperationalStore) CommitPrepared(context.Context, execution.Operation, inventory.Reservation) error {
	close(s.commitStarted)
	<-s.allowCommit
	s.committed.Store(true)
	return nil
}
func (s *blockingOperationalStore) RecordBroadcast(context.Context, execution.OperationID, execution.StepID, execution.TechnicalState, string) error {
	s.broadcasts.Add(1)
	return nil
}
func (*blockingOperationalStore) RecordSettlement(context.Context, execution.Settlement) error {
	return nil
}
func (*blockingOperationalStore) MarkSettled(context.Context, execution.OperationID) error {
	return nil
}
func (s *blockingOperationalStore) MarkManualIntervention(context.Context, execution.OperationID, string) error {
	s.manual.Add(1)
	return nil
}
func (s *blockingOperationalStore) MarkNoExecution(context.Context, execution.OperationID, string) error {
	s.noExecution.Add(1)
	return nil
}
func (*blockingOperationalStore) History(context.Context, execution.OperationID) ([]execution.OperationalEvent, error) {
	return nil, nil
}
func (*blockingOperationalStore) Pending(context.Context) ([]execution.Operation, error) {
	return nil, nil
}
func (*blockingOperationalStore) Close() error { return nil }

type fakeManager struct {
	account   execution.AccountID
	store     *blockingOperationalStore
	started   chan execution.AccountID
	violation *atomic.Bool
	result    chainport.BroadcastResult
	err       error
}

func (m *fakeManager) Account() execution.AccountID { return m.account }
func (*fakeManager) Warm(context.Context) error     { return nil }
func (m *fakeManager) Prepare(_ context.Context, artifact executionport.Artifact) (chainport.PreparedTransaction, error) {
	return chainport.PreparedTransaction{
		Leg: artifact.Leg,
		Identity: execution.TransactionIdentity{
			Chain: artifact.Leg.Chain, Account: artifact.Leg.Account, Hash: string(artifact.Leg.ID) + "-hash",
		},
		SignedPayload: []byte("signed"), PreparedAt: time.Now(),
	}, nil
}
func (m *fakeManager) Broadcast(_ context.Context, prepared chainport.PreparedTransaction) (chainport.BroadcastResult, error) {
	if !m.store.committed.Load() {
		m.violation.Store(true)
	}
	m.started <- m.account
	if m.result.Disposition != "" || m.err != nil {
		m.result.Identity = prepared.Identity
		return m.result, m.err
	}
	return chainport.BroadcastResult{
		Identity: prepared.Identity, Disposition: chainport.BroadcastAccepted,
		Accepted: true, Endpoint: string(m.account),
		Attempts: 1, AcceptedAt: time.Now(),
	}, nil
}
func (*fakeManager) Reconcile(context.Context, execution.OperationStep) (execution.Settlement, error) {
	return execution.Settlement{}, nil
}

func TestExecutorBroadcastsBothLegsOnlyAfterDurableCommit(t *testing.T) {
	now := time.Now().UTC()
	plan := executablePlan(t, now)
	store := &blockingOperationalStore{commitStarted: make(chan struct{}), allowCommit: make(chan struct{})}
	started := make(chan execution.AccountID, 2)
	var violation atomic.Bool
	inventoryOwner, err := inventory.New(map[inventory.Key]market.TokenAmount{
		{Chain: "chain-a", Account: "account-a", Token: "quote-a"}: tokenAmount(t, "quote-a", 1_000),
		{Chain: "chain-b", Account: "account-b", Token: "base-b"}:  tokenAmount(t, "base-b", 1_000),
	})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := saga.NewExecutor(saga.ExecutorConfig{
		ConfigHash: "config-hash", Inventory: inventoryOwner, Store: store,
		Managers: map[execution.AccountID]chainport.TxManager{
			"account-a": &fakeManager{account: "account-a", store: store, started: started, violation: &violation},
			"account-b": &fakeManager{account: "account-b", store: store, started: started, violation: &violation},
		},
		ArtifactMaxAge: time.Second, Clock: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	artifacts := map[execution.StepID]executionport.Artifact{}
	for _, leg := range plan.Legs() {
		artifacts[leg.ID] = executionport.Artifact{
			Leg: leg, ValidatedQuote: quoteForLeg(t, leg, now), Payload: []byte("artifact"), BuiltAt: now,
		}
	}
	done := make(chan error, 1)
	go func() {
		_, executeErr := executor.Execute(context.Background(), "operation-1", plan, artifacts)
		done <- executeErr
	}()
	<-store.commitStarted
	select {
	case account := <-started:
		t.Fatalf("broadcast for %s started before commit returned", account)
	default:
	}
	close(store.allowCommit)
	first := <-started
	second := <-started
	if first == second {
		t.Fatalf("expected both managers, got %s twice", first)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if violation.Load() {
		t.Fatal("a broadcast observed the store before durable commit")
	}
	if store.broadcasts.Load() != 2 {
		t.Fatalf("broadcast records = %d", store.broadcasts.Load())
	}
	if _, err := executor.Execute(context.Background(), "operation-2", plan, artifacts); err != saga.ErrOperationInFlight {
		t.Fatalf("second operation before settlement returned %v", err)
	}
}

func TestExecutorRejectsOldArtifactBeforeCommit(t *testing.T) {
	now := time.Now().UTC()
	plan := executablePlan(t, now)
	store := &blockingOperationalStore{commitStarted: make(chan struct{}), allowCommit: make(chan struct{})}
	started := make(chan execution.AccountID, 2)
	var violation atomic.Bool
	inventoryOwner, _ := inventory.New(map[inventory.Key]market.TokenAmount{
		{Chain: "chain-a", Account: "account-a", Token: "quote-a"}: tokenAmount(t, "quote-a", 1_000),
		{Chain: "chain-b", Account: "account-b", Token: "base-b"}:  tokenAmount(t, "base-b", 1_000),
	})
	executor, err := saga.NewExecutor(saga.ExecutorConfig{
		ConfigHash: "config-hash", Inventory: inventoryOwner, Store: store,
		Managers: map[execution.AccountID]chainport.TxManager{
			"account-a": &fakeManager{account: "account-a", store: store, started: started, violation: &violation},
			"account-b": &fakeManager{account: "account-b", store: store, started: started, violation: &violation},
		},
		ArtifactMaxAge: time.Millisecond, Clock: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	artifacts := map[execution.StepID]executionport.Artifact{}
	for _, leg := range plan.Legs() {
		artifacts[leg.ID] = executionport.Artifact{
			Leg: leg, ValidatedQuote: quoteForLeg(t, leg, now), Payload: []byte("artifact"),
			BuiltAt: now.Add(-time.Second),
		}
	}
	if _, err := executor.Execute(context.Background(), "operation-old", plan, artifacts); err != saga.ErrArtifactTooOld {
		t.Fatalf("error = %v", err)
	}
	if _, active := executor.Gate().Active(); active {
		t.Fatal("pre-commit artifact rejection left the operation gate closed")
	}
	select {
	case <-store.commitStarted:
		t.Fatal("old artifact reached persistence")
	default:
	}
	select {
	case <-started:
		t.Fatal("old artifact reached broadcast")
	default:
	}
}

func TestExecutorReleasesOnlyWhenBothBroadcastsAreConclusivelyRejected(t *testing.T) {
	now := time.Now().UTC()
	plan := executablePlan(t, now)
	allowCommit := make(chan struct{})
	close(allowCommit)
	store := &blockingOperationalStore{
		commitStarted: make(chan struct{}), allowCommit: allowCommit,
	}
	started := make(chan execution.AccountID, 2)
	var violation atomic.Bool
	owner, _ := inventory.New(map[inventory.Key]market.TokenAmount{
		{Chain: "chain-a", Account: "account-a", Token: "quote-a"}: tokenAmount(t, "quote-a", 1_000),
		{Chain: "chain-b", Account: "account-b", Token: "base-b"}:  tokenAmount(t, "base-b", 1_000),
	})
	managers := map[execution.AccountID]chainport.TxManager{}
	for _, account := range []execution.AccountID{"account-a", "account-b"} {
		managers[account] = &fakeManager{
			account: account, store: store, started: started, violation: &violation,
			result: chainport.BroadcastResult{Disposition: chainport.BroadcastRejected},
			err:    errors.New("synthetic deterministic rejection"),
		}
	}
	executor, err := saga.NewExecutor(saga.ExecutorConfig{
		ConfigHash: "config-hash", Inventory: owner, Store: store, Managers: managers,
		ArtifactMaxAge: time.Second, Clock: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(context.Background(), "operation-rejected", plan, artifactsForPlan(t, plan, now))
	if !errors.Is(err, saga.ErrNoExecution) || result.ReconciliationRequired {
		t.Fatalf("conclusive rejection result=%+v error=%v", result, err)
	}
	if store.noExecution.Load() != 1 || store.manual.Load() != 0 {
		t.Fatalf("no_execution=%d manual=%d", store.noExecution.Load(), store.manual.Load())
	}
	if _, active := executor.Gate().Active(); active {
		t.Fatal("conclusive two-leg rejection retained operation gate")
	}
	assertExecutorAvailable(t, owner, inventory.Key{
		Chain: "chain-a", Account: "account-a", Token: "quote-a",
	}, 1_000)
}

func TestExecutorRetainsReservationForUnknownBroadcast(t *testing.T) {
	now := time.Now().UTC()
	plan := executablePlan(t, now)
	allowCommit := make(chan struct{})
	close(allowCommit)
	store := &blockingOperationalStore{
		commitStarted: make(chan struct{}), allowCommit: allowCommit,
	}
	started := make(chan execution.AccountID, 2)
	var violation atomic.Bool
	owner, _ := inventory.New(map[inventory.Key]market.TokenAmount{
		{Chain: "chain-a", Account: "account-a", Token: "quote-a"}: tokenAmount(t, "quote-a", 1_000),
		{Chain: "chain-b", Account: "account-b", Token: "base-b"}:  tokenAmount(t, "base-b", 1_000),
	})
	managers := map[execution.AccountID]chainport.TxManager{}
	for _, account := range []execution.AccountID{"account-a", "account-b"} {
		managers[account] = &fakeManager{
			account: account, store: store, started: started, violation: &violation,
			result: chainport.BroadcastResult{Disposition: chainport.BroadcastPossible},
			err:    context.DeadlineExceeded,
		}
	}
	executor, err := saga.NewExecutor(saga.ExecutorConfig{
		ConfigHash: "config-hash", Inventory: owner, Store: store, Managers: managers,
		ArtifactMaxAge: time.Second, Clock: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(context.Background(), "operation-unknown", plan, artifactsForPlan(t, plan, now))
	if !errors.Is(err, saga.ErrReconciliationRequired) || !result.ReconciliationRequired {
		t.Fatalf("unknown broadcast result=%+v error=%v", result, err)
	}
	if store.manual.Load() != 1 || store.noExecution.Load() != 0 {
		t.Fatalf("manual=%d no_execution=%d", store.manual.Load(), store.noExecution.Load())
	}
	if active, ok := executor.Gate().Active(); !ok || active != "operation-unknown" {
		t.Fatalf("unknown broadcast gate active=%q ok=%t", active, ok)
	}
	assertExecutorAvailable(t, owner, inventory.Key{
		Chain: "chain-a", Account: "account-a", Token: "quote-a",
	}, 900)
}

func artifactsForPlan(t *testing.T, plan execution.SagaPlan, now time.Time) map[execution.StepID]executionport.Artifact {
	t.Helper()
	result := make(map[execution.StepID]executionport.Artifact, len(plan.Legs()))
	for _, leg := range plan.Legs() {
		result[leg.ID] = executionport.Artifact{
			Leg: leg, ValidatedQuote: quoteForLeg(t, leg, now),
			Payload: []byte("artifact"), BuiltAt: now,
		}
	}
	return result
}

func assertExecutorAvailable(
	t *testing.T,
	owner *inventory.Inventory,
	key inventory.Key,
	want int64,
) {
	t.Helper()
	got, ok := owner.Available(key)
	if !ok || got.Cmp(big.NewInt(want)) != 0 {
		t.Fatalf("available inventory=%s ok=%t, want=%d", got, ok, want)
	}
}

func executablePlan(t *testing.T, now time.Time) execution.SagaPlan {
	t.Helper()
	buyInput := tokenAmount(t, "quote-a", 100)
	buyOutput := tokenAmount(t, "base-a", 150)
	sellInput := tokenAmount(t, "base-b", 145)
	sellOutput := tokenAmount(t, "quote-b", 101)
	buyQuote := newQuote(t, "market-a", buyInput, buyOutput, now)
	sellQuote := newQuote(t, "market-b", sellInput, sellOutput, now)
	quoteDelta, _ := market.NewAssetQuantity("quote", big.NewRat(1, 1))
	baseDelta, _ := market.NewAssetQuantity("base", big.NewRat(5, 1))
	marked, _ := market.NewAssetQuantity("quote", big.NewRat(1, 2))
	gross, _ := market.NewAssetQuantity("quote", big.NewRat(3, 2))
	cost, _ := market.NewAssetQuantity("quote", big.NewRat(1, 10))
	net, _ := market.NewAssetQuantity("quote", big.NewRat(7, 5))
	threshold, _ := market.NewAssetQuantity("quote", big.NewRat(1, 1))
	valuation, _ := arbitrage.NewValuationSnapshot(1, "base", "quote", big.NewRat(1, 10), 2, now)
	opportunity := arbitrage.LiveOpportunity{
		ID: "opportunity", Setup: "setup",
		Direction: arbitrage.Direction{BuyMarket: "market-a", SellMarket: "market-b"},
		Valuation: valuation, BuyQuote: buyQuote, SellQuote: sellQuote,
		QuoteDelta: quoteDelta, BaseDelta: baseDelta, MarkedBase: marked,
		GrossPnL: gross, Cost: cost, NetPnL: net, Threshold: threshold, DiscoveredAt: now,
		ValidatedAt: now,
	}
	plan, err := execution.NewSagaPlan("plan", opportunity, []execution.Leg{
		{ID: "buy", Side: execution.LegBuy, Chain: "chain-a", Account: "account-a", Market: "market-a", Input: buyInput, ExpectedOutput: buyOutput},
		{ID: "sell", Side: execution.LegSell, Chain: "chain-b", Account: "account-b", Market: "market-b", Input: sellInput, ExpectedOutput: sellOutput},
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func quoteForLeg(t *testing.T, leg execution.Leg, now time.Time) market.Quote {
	t.Helper()
	return newQuote(t, leg.Market, leg.Input, leg.ExpectedOutput, now)
}

func newQuote(t *testing.T, marketID market.MarketID, input, output market.TokenAmount, now time.Time) market.Quote {
	t.Helper()
	quote, err := market.NewQuote(market.Quote{
		Source: "source", Market: marketID, SnapshotVersion: 1,
		Purpose: market.QuotePurposeLiveValidation, Mode: market.QuoteModeExactInput,
		Quality: market.QuoteQualityExact, AmountIn: input, AmountOut: output, QuotedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return quote
}

func tokenAmount(t *testing.T, token market.TokenID, units int64) market.TokenAmount {
	t.Helper()
	amount, err := market.NewTokenAmount(token, big.NewInt(units))
	if err != nil {
		t.Fatal(err)
	}
	return amount
}
