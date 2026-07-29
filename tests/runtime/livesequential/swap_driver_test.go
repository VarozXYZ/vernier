package livesequential_test

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	"github.com/VarozXYZ/vernier/runtime/livesequential"
)

type rebuildingValidator struct {
	now   time.Time
	calls int
}

func (v *rebuildingValidator) Validate(
	_ context.Context,
	request executionport.ValidationRequest,
) (executionport.Artifact, error) {
	v.calls++
	output, _ := market.NewTokenAmount(
		request.Leg.ExpectedOutput.Token(), big.NewInt(4_000_000_000),
	)
	quote, _ := market.NewQuote(market.Quote{
		Source: "validator", Market: request.Leg.Market, SnapshotVersion: 1,
		Purpose: market.QuotePurposeLiveValidation,
		Mode:    market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
		AmountIn: request.Leg.Input, AmountOut: output, QuotedAt: v.now,
	})
	builtAt := v.now
	if v.calls == 1 {
		builtAt = v.now.Add(-2 * time.Second)
	}
	return executionport.Artifact{
		Leg: request.Leg, ValidatedQuote: quote,
		Metadata: map[string]string{"kind": "test"}, BuiltAt: builtAt,
	}, nil
}

type durableJournal struct {
	prepared bool
	marked   []string
}

func (*durableJournal) CreateSequentialOperation(context.Context, execution.SequentialOperation) error {
	return nil
}
func (j *durableJournal) RecordPreparedTransaction(context.Context, executionport.PreparedTransaction) error {
	j.prepared = true
	return nil
}
func (j *durableJournal) MarkTransaction(_ context.Context, _ execution.OperationID, _ int, _, status string) error {
	j.marked = append(j.marked, status)
	return nil
}
func (*durableJournal) RecordStageSettlement(context.Context, execution.SequentialStageSettlement) error {
	return nil
}
func (*durableJournal) FinishSequentialOperation(context.Context, execution.OperationID, execution.SequentialOperationState, error) error {
	return nil
}
func (*durableJournal) ActiveSequentialOperation(context.Context) (execution.SequentialOperation, bool, error) {
	return execution.SequentialOperation{}, false, nil
}

type settledTxManager struct {
	journal       *durableJournal
	broadcasts    int
	prepareCalls  int
	oversizedAt   string
	simulations   int
	simulationErr error
	actualOutput  market.TokenAmount
	preparedInput market.TokenAmount
}

func (*settledTxManager) Account() execution.AccountID { return "account" }
func (*settledTxManager) Warm(context.Context) error   { return nil }
func (m *settledTxManager) Prepare(
	_ context.Context,
	artifact executionport.Artifact,
) (chainport.PreparedTransaction, error) {
	m.prepareCalls++
	if m.oversizedAt != "" &&
		artifact.Metadata["max_accounts"] == m.oversizedAt {
		return chainport.PreparedTransaction{}, &executionport.ArtifactTooLargeError{
			ActualBytes: 1_300, MaximumBytes: 1_232,
		}
	}
	m.preparedInput = artifact.Leg.Input
	return chainport.PreparedTransaction{
		Leg: artifact.Leg,
		Identity: execution.TransactionIdentity{
			Chain: "chain-a", Account: "account", Hash: "transaction",
		},
		PreparedAt: time.Now(),
	}, nil
}
func (m *settledTxManager) SimulatePrepared(
	context.Context,
	chainport.PreparedTransaction,
) error {
	m.simulations++
	return m.simulationErr
}
func (m *settledTxManager) Broadcast(
	_ context.Context,
	prepared chainport.PreparedTransaction,
) (chainport.BroadcastResult, error) {
	if !m.journal.prepared {
		panic("broadcast happened before durable identity persistence")
	}
	m.broadcasts++
	return chainport.BroadcastResult{
		Identity: prepared.Identity, Disposition: chainport.BroadcastAccepted,
		Accepted: true, Endpoint: "test",
	}, nil
}
func (m *settledTxManager) Reconcile(
	_ context.Context,
	step execution.OperationStep,
) (execution.Settlement, error) {
	return execution.Settlement{
		Identity: step.Identity, Technical: execution.StateConfirmedSuccess,
		Economic: execution.EconomicEffectVerified,
		ActualIn: m.preparedInput, ActualOut: m.actualOutput,
		ObservedAt: time.Now(), Evidence: "test-receipt",
	}, nil
}

type compactingValidator struct {
	now          time.Time
	validate     int
	compact      int
	compactLimit string
}

type fixedOutputValidator struct {
	now    time.Time
	output *big.Int
	calls  int
	inputs []market.TokenAmount
}

type recordingSellPreflight struct {
	validator   *fixedOutputValidator
	simulations int
}

func (p *recordingSellPreflight) ValidateAndSimulate(
	ctx context.Context,
	request execution.SequentialStageRequest,
) (livesequential.SellPreflightResult, error) {
	p.simulations++
	placeholder, err := market.NewTokenAmount(
		request.Stage.OutputToken,
		big.NewInt(1),
	)
	if err != nil {
		return livesequential.SellPreflightResult{}, err
	}
	artifact, err := p.validator.Validate(
		ctx,
		executionport.ValidationRequest{
			Operation: request.Operation,
			Leg: execution.Leg{
				ID:             "sell-preflight",
				Side:           execution.LegSell,
				Chain:          request.Stage.SourceChain,
				Account:        "reference-account",
				Market:         request.Stage.Market,
				Input:          request.Input,
				ExpectedOutput: placeholder,
			},
		},
	)
	return livesequential.SellPreflightResult{
		Artifact: artifact,
		Identity: "reference-account",
	}, err
}

type failingValidator struct{ err error }

func (v failingValidator) Validate(
	context.Context,
	executionport.ValidationRequest,
) (executionport.Artifact, error) {
	return executionport.Artifact{}, v.err
}

type fixedQuoteEstimator struct {
	output *big.Int
	err    error
	calls  int
}

func (e *fixedQuoteEstimator) QuoteExactInput(
	_ context.Context,
	_ market.TokenAmount,
	output market.TokenID,
) (market.TokenAmount, error) {
	e.calls++
	if e.err != nil {
		return market.TokenAmount{}, e.err
	}
	return market.NewTokenAmount(output, e.output)
}

type fixedExitCostSource struct {
	costs map[execution.SequentialExitRoute]market.AssetQuantity
}

func (s fixedExitCostSource) ExitCost(
	_ arbitrage.Direction,
	route execution.SequentialExitRoute,
	_ time.Time,
) (market.AssetQuantity, bool) {
	value, ok := s.costs[route]
	return value, ok
}

type fixedPreflightCostSource struct {
	cost market.AssetQuantity
}

func (s fixedPreflightCostSource) Snapshot(
	_ arbitrage.Direction,
	at time.Time,
) (arbitrage.CostSnapshot, bool) {
	return arbitrage.CostSnapshot{
		ID:         "synthetic-flow-cost",
		Amount:     s.cost,
		CapturedAt: at,
	}, true
}

func preflightCostSource(
	t *testing.T,
	amount string,
) fixedPreflightCostSource {
	t.Helper()
	value, ok := new(big.Rat).SetString(amount)
	if !ok {
		t.Fatal("invalid synthetic preflight cost")
	}
	quantity, err := market.NewAssetQuantity("quote", value)
	if err != nil {
		t.Fatal(err)
	}
	return fixedPreflightCostSource{cost: quantity}
}

func (v *fixedOutputValidator) Validate(
	_ context.Context,
	request executionport.ValidationRequest,
) (executionport.Artifact, error) {
	v.calls++
	v.inputs = append(v.inputs, request.Leg.Input)
	output, _ := market.NewTokenAmount(
		request.Leg.ExpectedOutput.Token(),
		v.output,
	)
	quote, _ := market.NewQuote(market.Quote{
		Source: "validator", Market: request.Leg.Market,
		SnapshotVersion: 1,
		Purpose:         market.QuotePurposeLiveValidation,
		Mode:            market.QuoteModeExactInput,
		Quality:         market.QuoteQualityExact,
		AmountIn:        request.Leg.Input,
		AmountOut:       output,
		QuotedAt:        v.now,
	})
	return executionport.Artifact{
		Leg: request.Leg, ValidatedQuote: quote,
		Metadata: map[string]string{"kind": "test"},
		BuiltAt:  v.now,
	}, nil
}

func (v *compactingValidator) artifact(
	request executionport.ValidationRequest,
	maxAccounts string,
) executionport.Artifact {
	output, _ := market.NewTokenAmount(
		request.Leg.ExpectedOutput.Token(), big.NewInt(4_000_000_000),
	)
	quote, _ := market.NewQuote(market.Quote{
		Source: "validator", Market: request.Leg.Market, SnapshotVersion: 1,
		Purpose: market.QuotePurposeLiveValidation,
		Mode:    market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
		AmountIn: request.Leg.Input, AmountOut: output, QuotedAt: v.now,
	})
	return executionport.Artifact{
		Leg: request.Leg, ValidatedQuote: quote,
		Metadata: map[string]string{
			"kind":         "test",
			"max_accounts": maxAccounts,
		},
		BuiltAt: v.now,
	}
}

func (v *compactingValidator) Validate(
	_ context.Context,
	request executionport.ValidationRequest,
) (executionport.Artifact, error) {
	v.validate++
	return v.artifact(request, "64"), nil
}

func (v *compactingValidator) ValidateCompact(
	_ context.Context,
	request executionport.ValidationRequest,
	previous executionport.Artifact,
) (executionport.Artifact, error) {
	v.compact++
	if previous.Metadata["max_accounts"] != "64" {
		return executionport.Artifact{}, errors.New("unexpected previous account limit")
	}
	return v.artifact(request, v.compactLimit), nil
}

func TestSwapDriverRebuildsStaleArtifactBeforeDurableBroadcast(t *testing.T) {
	now := time.Now().UTC()
	input, _ := market.NewTokenAmount("quote-a", big.NewInt(1_000_000))
	output, _ := market.NewTokenAmount("base-a", big.NewInt(4_000_000_000))
	journal := &durableJournal{}
	manager := &settledTxManager{journal: journal, actualOutput: output}
	validator := &rebuildingValidator{now: now}
	driver := &livesequential.SwapDriver{
		Bindings: map[market.MarketID]livesequential.SwapBinding{
			"market-a": {
				Account: "account", Validator: validator, TxManager: manager,
			},
		},
		Clock:          func() time.Time { return now },
		ArtifactMaxAge: time.Second,
	}
	settlement, err := driver.ExecuteStage(
		context.Background(),
		execution.SequentialStageRequest{
			Operation: "operation", Plan: "plan",
			Stage: execution.SequentialStagePlan{
				Ordinal: 1, Stage: execution.StageBuy,
				SourceChain: "chain-a", InputToken: "quote-a",
				OutputToken: "base-a", Market: "market-a",
			},
			Input: input,
		},
		journal,
	)
	if err != nil {
		t.Fatal(err)
	}
	if validator.calls != 2 {
		t.Fatalf("expected one rebuild, got %d validations", validator.calls)
	}
	if manager.broadcasts != 1 || !journal.prepared {
		t.Fatalf("unexpected broadcast state: broadcasts=%d prepared=%t", manager.broadcasts, journal.prepared)
	}
	if settlement.ActualOutput.Units().Cmp(output.Units()) != 0 {
		t.Fatalf("unexpected actual output %s", settlement.ActualOutput)
	}
}

func TestSwapDriverCompactsOversizedArtifactBeforePersistence(t *testing.T) {
	now := time.Now().UTC()
	input, _ := market.NewTokenAmount("quote-a", big.NewInt(1_000_000))
	output, _ := market.NewTokenAmount("base-a", big.NewInt(4_000_000_000))
	journal := &durableJournal{}
	manager := &settledTxManager{
		journal: journal, actualOutput: output, oversizedAt: "64",
	}
	validator := &compactingValidator{now: now, compactLimit: "48"}
	driver := &livesequential.SwapDriver{
		Bindings: map[market.MarketID]livesequential.SwapBinding{
			"market-a": {
				Account: "account", Validator: validator, TxManager: manager,
			},
		},
		Clock: func() time.Time { return now },
	}
	_, err := driver.ExecuteStage(
		context.Background(),
		execution.SequentialStageRequest{
			Operation: "operation", Plan: "plan",
			Stage: execution.SequentialStagePlan{
				Ordinal: 1, Stage: execution.StageBuy,
				SourceChain: "chain-a", InputToken: "quote-a",
				OutputToken: "base-a", Market: "market-a",
			},
			Input: input,
		},
		journal,
	)
	if err != nil {
		t.Fatal(err)
	}
	if validator.validate != 1 || validator.compact != 1 {
		t.Fatalf(
			"validations: initial=%d compact=%d",
			validator.validate,
			validator.compact,
		)
	}
	if manager.prepareCalls != 2 {
		t.Fatalf("prepare calls = %d", manager.prepareCalls)
	}
	if manager.broadcasts != 1 || !journal.prepared {
		t.Fatalf(
			"unexpected durable broadcast state: broadcasts=%d prepared=%t",
			manager.broadcasts,
			journal.prepared,
		)
	}
}

func TestSwapDriverPreflightsBothTransactionsAndReusesSimulatedBuy(t *testing.T) {
	now := time.Now().UTC()
	plan := preflightPlan(t, now)
	journal := &durableJournal{}
	buyOutput, _ := market.NewTokenAmount(
		"base-a",
		big.NewInt(4_052_168_781),
	)
	buyManager := &settledTxManager{
		journal: journal, actualOutput: buyOutput,
	}
	sellManager := &settledTxManager{journal: journal}
	buyValidator := &fixedOutputValidator{
		now: now, output: big.NewInt(4_052_168_781),
	}
	sellValidator := &fixedOutputValidator{
		now: now, output: big.NewInt(1_020_000),
	}
	preflightSellValidator := &fixedOutputValidator{
		now: now, output: big.NewInt(1_010_000),
	}
	sellPreflight := &recordingSellPreflight{
		validator: preflightSellValidator,
	}
	driver := &livesequential.SwapDriver{
		Bindings: map[market.MarketID]livesequential.SwapBinding{
			"market-a": {
				Account: "account", Validator: buyValidator,
				TxManager: buyManager,
			},
			"market-b": {
				Account: "account", Validator: sellValidator,
				TxManager: sellManager,
			},
		},
		SellPreflights: map[market.MarketID]livesequential.SellPreflight{
			"market-b": sellPreflight,
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8,
		QuoteAsset:      "quote",
		PreflightCosts:  preflightCostSource(t, "0"),
		Clock:           func() time.Time { return now },
	}
	if err := driver.Preflight(
		context.Background(),
		"operation",
		plan,
	); err != nil {
		t.Fatal(err)
	}
	if buyValidator.calls != 1 || preflightSellValidator.calls != 1 ||
		buyManager.simulations != 1 ||
		sellPreflight.simulations != 1 ||
		sellValidator.calls != 0 || sellManager.simulations != 0 {
		t.Fatalf(
			"preflight calls buy=%d/%d preflight_sell=%d/%d real_sell=%d/%d",
			buyValidator.calls,
			buyManager.simulations,
			preflightSellValidator.calls,
			sellPreflight.simulations,
			sellValidator.calls,
			sellManager.simulations,
		)
	}
	expectedSellInput := "4052168780000000000"
	if len(preflightSellValidator.inputs) != 1 ||
		preflightSellValidator.inputs[0].String() != expectedSellInput {
		t.Fatalf(
			"sell input = %v; want %s",
			preflightSellValidator.inputs,
			expectedSellInput,
		)
	}
	_, err := driver.ExecuteStage(
		context.Background(),
		execution.SequentialStageRequest{
			Operation: "operation", Plan: plan.ID,
			Stage: plan.Stages[0], Input: plan.InitialInput,
		},
		journal,
	)
	if err != nil {
		t.Fatal(err)
	}
	if buyValidator.calls != 1 ||
		buyManager.prepareCalls != 1 ||
		buyManager.simulations != 1 ||
		buyManager.broadcasts != 1 {
		t.Fatalf(
			"simulated buy was rebuilt: validations=%d prepares=%d simulations=%d broadcasts=%d",
			buyValidator.calls,
			buyManager.prepareCalls,
			buyManager.simulations,
			buyManager.broadcasts,
		)
	}
}

func TestSwapDriverRejectsFreshRoundTripBelowNetThresholdBeforeBroadcast(
	t *testing.T,
) {
	now := time.Now().UTC()
	plan := preflightPlan(t, now)
	journal := &durableJournal{}
	buyManager := &settledTxManager{journal: journal}
	sellManager := &settledTxManager{journal: journal}
	driver := &livesequential.SwapDriver{
		Bindings: map[market.MarketID]livesequential.SwapBinding{
			"market-a": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(4_052_168_781),
				},
				TxManager: buyManager,
			},
			"market-b": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(1_010_000),
				},
				TxManager: sellManager,
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8,
		QuoteAsset:      "quote",
		MinimumNet:      big.NewRat(5, 1000),
		PreflightCosts:  preflightCostSource(t, "0.02"),
		Clock:           func() time.Time { return now },
	}
	err := driver.Preflight(context.Background(), "operation", plan)
	if err == nil ||
		executionport.ErrorDisposition(err) !=
			executionport.DispositionRejected {
		t.Fatalf("preflight error = %v", err)
	}
	if buyManager.broadcasts != 0 || sellManager.broadcasts != 0 ||
		journal.prepared {
		t.Fatalf(
			"deteriorated preflight reached persistence or broadcast",
		)
	}
}

func TestSwapDriverRejectsWhenSecondTransactionSimulationFails(t *testing.T) {
	now := time.Now().UTC()
	plan := preflightPlan(t, now)
	journal := &durableJournal{}
	buyManager := &settledTxManager{journal: journal}
	sellManager := &settledTxManager{
		journal:       journal,
		simulationErr: errors.New("sell transaction reverted"),
	}
	driver := &livesequential.SwapDriver{
		Bindings: map[market.MarketID]livesequential.SwapBinding{
			"market-a": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(4_052_168_781),
				},
				TxManager: buyManager,
			},
			"market-b": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(1_010_000),
				},
				TxManager: sellManager,
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8,
		QuoteAsset:      "quote",
		PreflightCosts:  preflightCostSource(t, "0"),
		Clock:           func() time.Time { return now },
	}
	err := driver.Preflight(context.Background(), "operation", plan)
	if err == nil ||
		executionport.ErrorDisposition(err) !=
			executionport.DispositionRejected {
		t.Fatalf("preflight error = %v", err)
	}
	if buyManager.simulations != 1 ||
		sellManager.simulations != 1 ||
		journal.prepared ||
		buyManager.broadcasts != 0 {
		t.Fatalf(
			"simulation failure state buy_sim=%d sell_sim=%d durable=%t broadcasts=%d",
			buyManager.simulations,
			sellManager.simulations,
			journal.prepared,
			buyManager.broadcasts,
		)
	}
}

func TestSwapDriverKeepsFreshProfitableDestinationExitAndReusesIt(t *testing.T) {
	now := time.Now().UTC()
	plan := preflightPlan(t, now)
	journal := &durableJournal{}
	manager := &settledTxManager{
		journal: journal,
		actualOutput: func() market.TokenAmount {
			value, _ := market.NewTokenAmount(
				"quote-b", big.NewInt(1_080_000),
			)
			return value
		}(),
	}
	cost, _ := market.NewAssetQuantity("quote", big.NewRat(1, 100))
	driver := &livesequential.SwapDriver{
		Bindings: map[market.MarketID]livesequential.SwapBinding{
			"market-b": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(1_080_000),
				},
				TxManager: manager,
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8, QuoteAsset: "quote",
		MinimumNet:   big.NewRat(5, 100),
		ReturnMargin: big.NewRat(1, 100),
		ExitCosts: fixedExitCostSource{
			costs: map[execution.SequentialExitRoute]market.AssetQuantity{
				execution.ExitSellAtDestination: cost,
			},
		},
		Clock: func() time.Time { return now },
	}
	bridged, _ := market.NewTokenAmount(
		"base-b", big.NewInt(4_000_000_000_000_000_000),
	)
	decision, err := driver.SelectExit(
		context.Background(), "operation", plan, bridged, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Route != execution.ExitSellAtDestination ||
		!decision.DestinationQualified ||
		decision.ReturnOutput.Token() != "" {
		t.Fatalf("unexpected decision: %+v", decision)
	}
	_, err = driver.ExecuteStage(
		context.Background(),
		execution.SequentialStageRequest{
			Operation: "operation", Plan: plan.ID,
			Stage: plan.Stages[2], Input: bridged,
		},
		journal,
	)
	if err != nil {
		t.Fatal(err)
	}
	if manager.prepareCalls != 1 || manager.simulations != 1 ||
		manager.broadcasts != 1 {
		t.Fatalf(
			"destination artifact was not reused: prepare=%d simulate=%d broadcast=%d",
			manager.prepareCalls, manager.simulations, manager.broadcasts,
		)
	}
}

func TestSwapDriverReturnsToOriginOnlyWhenNetRecoveryClearsMargin(t *testing.T) {
	now := time.Now().UTC()
	plan := preflightPlan(t, now)
	destinationManager := &settledTxManager{}
	returnManager := &settledTxManager{}
	destinationCost, _ := market.NewAssetQuantity(
		"quote", big.NewRat(1, 100),
	)
	returnCost, _ := market.NewAssetQuantity(
		"quote", big.NewRat(1, 100),
	)
	driver := &livesequential.SwapDriver{
		// The origin estimate is deliberately quote-only: the bridged token
		// is not present there yet and cannot be simulated.
		Bindings: map[market.MarketID]livesequential.SwapBinding{
			"market-a": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(980_000),
				},
				Estimator: &fixedQuoteEstimator{
					output: big.NewInt(980_000),
				},
				TxManager: returnManager,
			},
			"market-b": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(950_000),
				},
				TxManager: destinationManager,
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8, QuoteAsset: "quote",
		MinimumNet:   big.NewRat(5, 100),
		ReturnMargin: big.NewRat(1, 100),
		ExitCosts: fixedExitCostSource{
			costs: map[execution.SequentialExitRoute]market.AssetQuantity{
				execution.ExitSellAtDestination: destinationCost,
				execution.ExitReturnToOrigin:    returnCost,
			},
		},
		Clock: func() time.Time { return now },
	}
	bridged, _ := market.NewTokenAmount(
		"base-b", big.NewInt(4_000_000_000_000_000_000),
	)
	decision, err := driver.SelectExit(
		context.Background(), "operation", plan, bridged, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Route != execution.ExitReturnToOrigin ||
		decision.DestinationQualified ||
		decision.DestinationRecovery.Rat().Cmp(big.NewRat(94, 100)) != 0 ||
		decision.ReturnRecovery.Rat().Cmp(big.NewRat(97, 100)) != 0 {
		t.Fatalf("unexpected decision: %+v", decision)
	}
	if destinationManager.simulations != 1 ||
		returnManager.simulations != 0 ||
		returnManager.prepareCalls != 0 {
		t.Fatalf(
			"destination simulations=%d return simulations=%d prepares=%d",
			destinationManager.simulations,
			returnManager.simulations,
			returnManager.prepareCalls,
		)
	}
}

func TestSwapDriverReturnsToOriginWhenDestinationLiquidationIsUnavailable(t *testing.T) {
	now := time.Now().UTC()
	plan := preflightPlan(t, now)
	returnManager := &settledTxManager{}
	returnCost, _ := market.NewAssetQuantity(
		"quote", big.NewRat(1, 100),
	)
	driver := &livesequential.SwapDriver{
		Bindings: map[market.MarketID]livesequential.SwapBinding{
			"market-a": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(980_000),
				},
				Estimator: &fixedQuoteEstimator{
					output: big.NewInt(980_000),
				},
				TxManager: returnManager,
			},
			"market-b": {
				Account:   "account",
				Validator: failingValidator{err: errors.New("route unavailable")},
				TxManager: &settledTxManager{},
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8, QuoteAsset: "quote",
		ReturnMargin: big.NewRat(1, 100),
		ExitCosts: fixedExitCostSource{
			costs: map[execution.SequentialExitRoute]market.AssetQuantity{
				execution.ExitReturnToOrigin: returnCost,
			},
		},
		Clock: func() time.Time { return now },
	}
	bridged, _ := market.NewTokenAmount(
		"base-b", big.NewInt(4_000_000_000_000_000_000),
	)
	decision, err := driver.SelectExit(
		context.Background(), "operation", plan, bridged, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Route != execution.ExitReturnToOrigin ||
		!decision.DestinationOutput.IsZero() ||
		decision.ReturnOutput.IsZero() {
		t.Fatalf("unexpected forced-return decision: %+v", decision)
	}
}

func TestForcedExecutionPreflightAcceptsExecutableNegativeRoundTrip(t *testing.T) {
	now := time.Now().UTC()
	plan := forcedPreflightPlan(t, now)
	buyManager := &settledTxManager{journal: &durableJournal{}}
	sellManager := &settledTxManager{journal: &durableJournal{}}
	driver := &livesequential.SwapDriver{
		Bindings: map[market.MarketID]livesequential.SwapBinding{
			"market-a": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(4_052_168_781),
				},
				TxManager: buyManager,
			},
			"market-b": {
				Account: "account",
				Validator: &fixedOutputValidator{
					now: now, output: big.NewInt(999_999),
				},
				TxManager: sellManager,
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8,
		Clock:           func() time.Time { return now },
	}
	if err := driver.Preflight(
		context.Background(),
		"operation",
		plan,
	); err != nil {
		t.Fatal(err)
	}
	if buyManager.simulations != 1 || sellManager.simulations != 1 ||
		buyManager.broadcasts != 0 || sellManager.broadcasts != 0 {
		t.Fatalf(
			"forced preflight simulations=%d/%d broadcasts=%d/%d",
			buyManager.simulations,
			sellManager.simulations,
			buyManager.broadcasts,
			sellManager.broadcasts,
		)
	}
}

func TestForcedExecutionKeepsDestinationExitDespiteBetterReturn(t *testing.T) {
	now := time.Now().UTC()
	plan := forcedPreflightPlan(t, now)
	destinationManager := &settledTxManager{}
	returnEstimator := &fixedQuoteEstimator{output: big.NewInt(980_000)}
	cost, _ := market.NewAssetQuantity("quote", big.NewRat(1, 100))
	driver := &livesequential.SwapDriver{
		Bindings: map[market.MarketID]livesequential.SwapBinding{
			"market-a": {
				Account:   "account",
				Validator: &fixedOutputValidator{now: now, output: big.NewInt(980_000)},
				Estimator: returnEstimator,
				TxManager: &settledTxManager{},
			},
			"market-b": {
				Account:   "account",
				Validator: &fixedOutputValidator{now: now, output: big.NewInt(950_000)},
				TxManager: destinationManager,
			},
		},
		TokenDecimals: map[market.TokenID]uint8{
			"quote-a": 6, "base-a": 9,
			"base-b": 18, "quote-b": 6,
		},
		BridgePrecision: 8, QuoteAsset: "quote",
		MinimumNet: big.NewRat(1, 2), ReturnMargin: new(big.Rat),
		ExitCosts: fixedExitCostSource{
			costs: map[execution.SequentialExitRoute]market.AssetQuantity{
				execution.ExitSellAtDestination: cost,
				execution.ExitReturnToOrigin:    cost,
			},
		},
		Clock: func() time.Time { return now },
	}
	bridged, _ := market.NewTokenAmount(
		"base-b",
		big.NewInt(4_000_000_000_000_000_000),
	)
	decision, err := driver.SelectExit(
		context.Background(),
		"operation",
		plan,
		bridged,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Route != execution.ExitSellAtDestination ||
		decision.DestinationQualified ||
		decision.Evidence !=
			"fresh_destination_build+simulation+forced_execution_destination" {
		t.Fatalf("forced destination decision=%+v", decision)
	}
	if returnEstimator.calls != 0 {
		t.Fatalf("forced destination requested %d unnecessary return quotes", returnEstimator.calls)
	}
}

func forcedPreflightPlan(
	t *testing.T,
	now time.Time,
) execution.SequentialPlan {
	t.Helper()
	original := preflightPlan(t, now)
	forced, err := livesequential.ForceOpportunity(
		[]arbitrage.Opportunity{original.Opportunity},
		original.Opportunity.Direction,
	)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := execution.NewSequentialPlan(
		original.ID,
		forced,
		original.InitialInput,
		original.Stages[0].SourceChain,
		original.Stages[2].SourceChain,
		original.CreatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func preflightPlan(
	t *testing.T,
	now time.Time,
) execution.SequentialPlan {
	t.Helper()
	amount := func(token market.TokenID, units *big.Int) market.TokenAmount {
		value, err := market.NewTokenAmount(token, units)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	discoveryInput := amount("quote-a", big.NewInt(100_000_000))
	discoveryBuyOutput := amount("base-a", big.NewInt(3_000_000_000))
	discoverySellInput := amount(
		"base-b",
		big.NewInt(3_000_000_000_000_000_000),
	)
	discoverySellOutput := amount("quote-b", big.NewInt(752_000_000))
	opportunity := arbitrage.Opportunity{
		Evaluation: "evaluation", ConfigHash: "config",
		Classification: arbitrage.ClassificationPolicyQualified,
		Direction: arbitrage.Direction{
			BuyMarket: "market-a", SellMarket: "market-b",
		},
		Candidates: []arbitrage.Candidate{{
			BuyQuote: market.Quote{
				AmountIn:  discoveryInput,
				AmountOut: discoveryBuyOutput,
			},
			SellQuote: market.Quote{
				AmountIn:  discoverySellInput,
				AmountOut: discoverySellOutput,
			},
		}},
		SelectedIndex: 0,
	}
	initial := amount("quote-a", big.NewInt(1_000_000))
	plan, err := execution.NewSequentialPlan(
		"plan",
		opportunity,
		initial,
		"chain-a",
		"chain-b",
		now,
	)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
