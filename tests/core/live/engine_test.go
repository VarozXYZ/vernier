package live_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	corelive "github.com/VarozXYZ/vernier/core/live"
	"github.com/VarozXYZ/vernier/core/saga"
	"github.com/VarozXYZ/vernier/core/strategy"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/inventory"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	persistenceport "github.com/VarozXYZ/vernier/ports/persistence"
	quoteport "github.com/VarozXYZ/vernier/ports/quote"
)

type engineSnapshotData struct{}

func (engineSnapshotData) SnapshotKind() string { return "live-engine-test/v1" }

type fixedQuoteSource struct {
	id     market.SourceID
	output int64
}

func (s fixedQuoteSource) ID() market.SourceID { return s.id }

func (s fixedQuoteSource) Quote(_ context.Context, input quoteport.Input) (market.Quote, error) {
	output, _ := market.NewTokenAmount(input.TokenOut, big.NewInt(s.output))
	return market.NewQuote(market.Quote{
		Source: s.id, Market: input.Snapshot.Metadata().Market,
		SnapshotVersion: input.Snapshot.Metadata().Version,
		SnapshotHash:    input.Snapshot.Metadata().StateHash,
		Purpose:         input.Purpose,
		Mode:            market.QuoteModeExactInput,
		Quality:         market.QuoteQualityExact,
		AmountIn:        input.AmountIn,
		AmountOut:       output,
		QuotedAt:        input.QuotedAt,
	})
}

type controlledValidator struct {
	started chan market.MarketID
	release <-chan struct{}
	calls   atomic.Int32
	now     time.Time
	err     error
}

func (v *controlledValidator) Validate(ctx context.Context, request executionport.ValidationRequest) (executionport.Artifact, error) {
	v.calls.Add(1)
	if v.err != nil {
		return executionport.Artifact{}, v.err
	}
	if v.started != nil {
		select {
		case v.started <- request.Leg.Market:
		case <-ctx.Done():
			return executionport.Artifact{}, ctx.Err()
		}
	}
	if v.release != nil {
		select {
		case <-v.release:
		case <-ctx.Done():
			return executionport.Artifact{}, ctx.Err()
		}
	}
	validated, err := market.NewQuote(market.Quote{
		Source:          request.Discovery.Source,
		Market:          request.Discovery.Market,
		SnapshotVersion: request.Discovery.SnapshotVersion,
		SnapshotHash:    request.Discovery.SnapshotHash,
		Purpose:         market.QuotePurposeLiveValidation,
		Mode:            market.QuoteModeExactInput,
		Quality:         market.QuoteQualityExact,
		AmountIn:        request.Discovery.AmountIn,
		AmountOut:       request.Discovery.AmountOut,
		QuotedAt:        v.now,
	})
	if err != nil {
		return executionport.Artifact{}, err
	}
	return executionport.Artifact{
		Leg: request.Leg, ValidatedQuote: validated, Payload: []byte("executable"),
		BuiltAt: v.now,
	}, nil
}

type memoryOperationalStore struct {
	commits atomic.Int32
}

func (s *memoryOperationalStore) CommitPrepared(context.Context, execution.Operation, inventory.Reservation) error {
	s.commits.Add(1)
	return nil
}
func (*memoryOperationalStore) RecordBroadcast(context.Context, execution.OperationID, execution.StepID, execution.TechnicalState, string) error {
	return nil
}
func (*memoryOperationalStore) RecordSettlement(context.Context, execution.Settlement) error {
	return nil
}
func (*memoryOperationalStore) MarkSettled(context.Context, execution.OperationID) error {
	return nil
}
func (*memoryOperationalStore) MarkManualIntervention(context.Context, execution.OperationID, string) error {
	return nil
}
func (*memoryOperationalStore) MarkNoExecution(context.Context, execution.OperationID, string) error {
	return nil
}
func (*memoryOperationalStore) History(context.Context, execution.OperationID) ([]execution.OperationalEvent, error) {
	return nil, nil
}
func (*memoryOperationalStore) Pending(context.Context) ([]execution.Operation, error) {
	return nil, nil
}
func (*memoryOperationalStore) Close() error { return nil }

type immediateManager struct {
	account execution.AccountID
	now     time.Time
}

func (m immediateManager) Account() execution.AccountID { return m.account }
func (immediateManager) Warm(context.Context) error     { return nil }
func (m immediateManager) Prepare(_ context.Context, artifact executionport.Artifact) (chainport.PreparedTransaction, error) {
	return chainport.PreparedTransaction{
		Leg: artifact.Leg,
		Identity: execution.TransactionIdentity{
			Chain: artifact.Leg.Chain, Account: artifact.Leg.Account, Hash: string(artifact.Leg.ID) + "-hash",
		},
		SignedPayload: []byte("signed"), PreparedAt: m.now,
	}, nil
}
func (m immediateManager) Broadcast(_ context.Context, prepared chainport.PreparedTransaction) (chainport.BroadcastResult, error) {
	return chainport.BroadcastResult{
		Identity: prepared.Identity, Accepted: true, Endpoint: string(m.account),
		Attempts: 1, AcceptedAt: m.now,
	}, nil
}
func (immediateManager) Reconcile(context.Context, execution.OperationStep) (execution.Settlement, error) {
	return execution.Settlement{}, nil
}

type fixedCostEstimator struct {
	amount market.AssetQuantity
	calls  atomic.Int32
}

func (e *fixedCostEstimator) Estimate(context.Context, executionport.CostRequest) (market.AssetQuantity, error) {
	e.calls.Add(1)
	return e.amount, nil
}

func TestEngineDoesNotValidateWhenDiscoveryIsNotProfitable(t *testing.T) {
	fixture := newEngineFixture(t, 99, nil, nil, false)
	result, err := fixture.engine.Evaluate(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Discovery.Profitable() {
		t.Fatal("fixture unexpectedly produced a profitable discovery")
	}
	if result.Validated != nil || result.Execution != nil {
		t.Fatal("unprofitable discovery reached executable validation")
	}
	if fixture.buyValidator.calls.Load() != 0 || fixture.sellValidator.calls.Load() != 0 {
		t.Fatal("validator was called without an opportunity")
	}
	if fixture.store.commits.Load() != 0 {
		t.Fatal("unprofitable discovery reached persistence")
	}
	if fixture.costs.calls.Load() != 0 {
		t.Fatal("unprofitable discovery recalculated executable costs")
	}
}

func TestEngineValidatesBothLegsConcurrently(t *testing.T) {
	started := make(chan market.MarketID, 2)
	release := make(chan struct{})
	fixture := newEngineFixture(t, 102, started, release, false)
	type outcome struct {
		result corelive.Result
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := fixture.engine.Evaluate(context.Background(), fixture.request)
		done <- outcome{result: result, err: err}
	}()
	first := <-started
	second := <-started
	if first == second {
		t.Fatalf("expected both validators before release; got %q twice", first)
	}
	close(release)
	completed := <-done
	if completed.err != nil {
		t.Fatal(completed.err)
	}
	if completed.result.Validated == nil || completed.result.Execution == nil {
		t.Fatal("profitable discovery did not reach validated execution")
	}
	if fixture.buyValidator.calls.Load() != 1 || fixture.sellValidator.calls.Load() != 1 {
		t.Fatalf("validation calls buy=%d sell=%d", fixture.buyValidator.calls.Load(), fixture.sellValidator.calls.Load())
	}
	if fixture.store.commits.Load() != 1 {
		t.Fatalf("durable commits = %d", fixture.store.commits.Load())
	}
	if fixture.costs.calls.Load() != 1 {
		t.Fatalf("definitive cost estimates = %d", fixture.costs.calls.Load())
	}
}

func TestEngineAbortsOnlyOpportunityWhenExecutableValidationFails(t *testing.T) {
	fixture := newEngineFixture(t, 102, nil, nil, false)
	fixture.buyValidator.err = errors.New("synthetic provider failure")
	result, err := fixture.engine.Evaluate(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("validation failure stopped the Live engine: %v", err)
	}
	if !result.ValidationAbort || result.Execution != nil {
		t.Fatalf("validation failure result = %+v", result)
	}
	if fixture.store.commits.Load() != 0 {
		t.Fatal("failed executable validation reached persistence")
	}
	if fixture.costs.calls.Load() != 0 {
		t.Fatal("failed executable validation recalculated costs")
	}
}

func TestDryRunStopsAfterExecutableArtifacts(t *testing.T) {
	fixture := newEngineFixture(t, 102, nil, nil, true)
	result, err := fixture.engine.Evaluate(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Validated == nil || len(result.Artifacts) != 2 {
		t.Fatalf("dry-run did not expose validated artifacts: %+v", result)
	}
	if result.Execution != nil || fixture.store.commits.Load() != 0 {
		t.Fatal("dry-run reached persistence or execution")
	}
}

type engineFixture struct {
	engine        *corelive.Engine
	request       strategy.LiveEvaluationRequest
	buyValidator  *controlledValidator
	sellValidator *controlledValidator
	store         *memoryOperationalStore
	costs         *fixedCostEstimator
}

func newEngineFixture(
	t *testing.T,
	sellOutput int64,
	started chan market.MarketID,
	release <-chan struct{},
	dryRun bool,
) engineFixture {
	t.Helper()
	now := time.Date(2026, 7, 27, 15, 0, 0, 0, time.UTC)
	registry, setup, snapshots := engineRegistry(t, now)
	evaluator, err := strategy.NewLiveEvaluator(strategy.LiveConfig{
		Setup: setup, Registry: registry,
		Sources: map[market.MarketID]quoteport.Source{
			"market-a": fixedQuoteSource{id: "source-a", output: 150},
			"market-b": fixedQuoteSource{id: "source-b", output: sellOutput},
		},
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	planner, err := saga.NewPlanner(map[market.MarketID]saga.LegBinding{
		"market-a": {Chain: "chain-a", Account: "account-a"},
		"market-b": {Chain: "chain-b", Account: "account-b"},
	}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	owner, err := inventory.New(map[inventory.Key]market.TokenAmount{
		{Chain: "chain-a", Account: "account-a", Token: "quote-a"}: engineAmount(t, "quote-a", 1_000),
		{Chain: "chain-b", Account: "account-b", Token: "base-b"}:  engineAmount(t, "base-b", 1_000),
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &memoryOperationalStore{}
	executor, err := saga.NewExecutor(saga.ExecutorConfig{
		ConfigHash: "synthetic-config", Inventory: owner, Store: store,
		Managers: map[execution.AccountID]chainport.TxManager{
			"account-a": immediateManager{account: "account-a", now: now},
			"account-b": immediateManager{account: "account-b", now: now},
		},
		ArtifactMaxAge: time.Second, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	buyValidator := &controlledValidator{started: started, release: release, now: now}
	sellValidator := &controlledValidator{started: started, release: release, now: now}
	cachedCost, _ := market.NewAssetQuantity("quote", big.NewRat(1, 10))
	costs := &fixedCostEstimator{amount: cachedCost}
	var sequence atomic.Uint64
	configuredExecutor := executor
	if dryRun {
		configuredExecutor = nil
	}
	engine, err := corelive.New(corelive.EngineConfig{
		Evaluator: evaluator, Planner: planner, Executor: configuredExecutor, Costs: costs,
		Validators: map[market.MarketID]executionport.Validator{
			"market-a": buyValidator,
			"market-b": sellValidator,
		},
		Clock:  func() time.Time { return now },
		DryRun: dryRun,
		ID: func(prefix string) string {
			return prefix + "-" + new(big.Int).SetUint64(sequence.Add(1)).String()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	valuation, _ := arbitrage.NewValuationSnapshot(1, "base", "quote", big.NewRat(2, 3), 4, now)
	notional, _ := market.NewAssetQuantity("quote", big.NewRat(100, 1))
	cost, _ := market.NewAssetQuantity("quote", big.NewRat(1, 10))
	threshold, _ := market.NewAssetQuantity("quote", big.NewRat(1, 1))
	maximumCost, _ := market.NewAssetQuantity("quote", big.NewRat(1, 1))
	maximumExposure, _ := market.NewAssetQuantity("base", big.NewRat(100, 1))
	return engineFixture{
		engine: engine,
		request: strategy.LiveEvaluationRequest{
			ID: "synthetic-evaluation",
			Direction: arbitrage.Direction{
				BuyMarket: "market-a", SellMarket: "market-b",
			},
			Snapshots: snapshots, Notional: notional, Valuation: valuation,
			Cost: cost, Threshold: threshold, MaximumCost: maximumCost,
			MaximumBaseExposure: maximumExposure, TriggeredAt: now,
		},
		buyValidator: buyValidator, sellValidator: sellValidator, store: store, costs: costs,
	}
}

func engineRegistry(t *testing.T, now time.Time) (*market.Registry, arbitrage.ArbitrageSetup, map[market.MarketID]market.MarketSnapshot) {
	t.Helper()
	registry, err := market.NewRegistry(market.Catalog{
		Chains: []market.Chain{{ID: "chain-a"}, {ID: "chain-b"}},
		Assets: []market.Asset{{ID: "base", Symbol: "BASE"}, {ID: "quote", Symbol: "QUOTE"}},
		Tokens: []market.Token{
			{ID: "base-a", Asset: "base", Chain: "chain-a", Decimals: 0, Symbol: "BASE"},
			{ID: "quote-a", Asset: "quote", Chain: "chain-a", Decimals: 0, Symbol: "QUOTE"},
			{ID: "base-b", Asset: "base", Chain: "chain-b", Decimals: 0, Symbol: "BASE"},
			{ID: "quote-b", Asset: "quote", Chain: "chain-b", Decimals: 0, Symbol: "QUOTE"},
		},
		Venues: []market.Venue{{ID: "venue-a"}, {ID: "venue-b"}},
		Pairs:  []market.Pair{{ID: "pair", BaseAsset: "base", QuoteAsset: "quote"}},
		Pools: []market.Pool{
			{ID: "pool-a", Venue: "venue-a", Chain: "chain-a", Tokens: []market.TokenID{"base-a", "quote-a"}, Adapter: "synthetic"},
			{ID: "pool-b", Venue: "venue-b", Chain: "chain-b", Tokens: []market.TokenID{"base-b", "quote-b"}, Adapter: "synthetic"},
		},
		Paths: []market.Path{
			{ID: "path-a", Chain: "chain-a", Hops: []market.Hop{{Pool: "pool-a", TokenIn: "base-a", TokenOut: "quote-a"}}},
			{ID: "path-b", Chain: "chain-b", Hops: []market.Hop{{Pool: "pool-b", TokenIn: "base-b", TokenOut: "quote-b"}}},
		},
		Markets: []market.Market{
			{ID: "market-a", Pair: "pair", Chain: "chain-a", Path: "path-a", BaseToken: "base-a", QuoteToken: "quote-a"},
			{ID: "market-b", Pair: "pair", Chain: "chain-b", Path: "path-b", BaseToken: "base-b", QuoteToken: "quote-b"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	setup, err := arbitrage.NewArbitrageSetup("synthetic-setup", "pair", []market.MarketID{"market-a", "market-b"}, registry)
	if err != nil {
		t.Fatal(err)
	}
	snapshots := make(map[market.MarketID]market.MarketSnapshot, 2)
	for _, id := range []market.MarketID{"market-a", "market-b"} {
		hash := sha256.Sum256([]byte(id))
		snapshot, snapshotErr := market.NewMarketSnapshot(market.SnapshotMetadata{
			Market: id, Source: market.SourceID(id + "/source"), Version: 1,
			ReceivedAt: now, AppliedAt: now, Health: market.HealthHealthy,
			HealthChangedAt: now, StateHash: hash,
		}, engineSnapshotData{})
		if snapshotErr != nil {
			t.Fatal(snapshotErr)
		}
		snapshots[id] = snapshot
	}
	return registry, setup, snapshots
}

func engineAmount(t *testing.T, token market.TokenID, units int64) market.TokenAmount {
	t.Helper()
	amount, err := market.NewTokenAmount(token, big.NewInt(units))
	if err != nil {
		t.Fatal(err)
	}
	return amount
}

var (
	_ executionport.Validator          = (*controlledValidator)(nil)
	_ executionport.CostEstimator      = (*fixedCostEstimator)(nil)
	_ persistenceport.OperationalStore = (*memoryOperationalStore)(nil)
	_ chainport.TxManager              = immediateManager{}
	_ quoteport.Source                 = fixedQuoteSource{}
)
