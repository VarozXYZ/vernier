package sqlite_test

import (
	"context"
	"database/sql"
	"math/big"
	"path/filepath"
	"testing"
	"time"

	sqlitestore "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	"github.com/VarozXYZ/vernier/domain/arbitrage"
	domainexecution "github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"

	_ "modernc.org/sqlite"
)

func TestSequentialLiveStoreKeepsManualInterventionAsAnActiveBarrier(t *testing.T) {
	store, err := sqlitestore.OpenSequentialLive(
		filepath.Join(t.TempDir(), "live.sqlite"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	input, _ := market.NewTokenAmount("quote-a", big.NewInt(1_000_000))
	now := time.Now().UTC()
	operation := domainexecution.SequentialOperation{
		ID: "operation-1", Plan: "plan-1", OpportunityID: "opportunity-1",
		ConfigHash: "config", State: domainexecution.SequentialRunning,
		CurrentAmount: input, StartedAt: now, UpdatedAt: now,
	}
	if err := store.CreateSequentialOperation(context.Background(), operation); err != nil {
		t.Fatal(err)
	}
	if err := store.FinishSequentialOperation(
		context.Background(), operation.ID,
		domainexecution.SequentialManualIntervention,
		context.DeadlineExceeded,
	); err != nil {
		t.Fatal(err)
	}
	active, ok, err := store.ActiveSequentialOperation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ok || active.ID != operation.ID ||
		active.State != domainexecution.SequentialManualIntervention {
		t.Fatalf("manual intervention barrier was not recovered: %#v", active)
	}
	second := operation
	second.ID = "operation-2"
	if err := store.CreateSequentialOperation(context.Background(), second); err == nil {
		t.Fatal("expected unique active-operation barrier")
	}
}

func TestSequentialLiveStoreAcknowledgesManualReconciliation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "live.sqlite")
	store, err := sqlitestore.OpenSequentialLive(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	input, _ := market.NewTokenAmount("quote-a", big.NewInt(1_000_000))
	now := time.Now().UTC()
	operation := domainexecution.SequentialOperation{
		ID: "operation-reconciled", Plan: "plan-1",
		OpportunityID: "opportunity-1", ConfigHash: "config",
		State:         domainexecution.SequentialRunning,
		CurrentAmount: input, StartedAt: now, UpdatedAt: now,
	}
	ctx := context.Background()
	if err := store.CreateSequentialOperation(ctx, operation); err != nil {
		t.Fatal(err)
	}
	if err := store.FinishSequentialOperation(
		ctx,
		operation.ID,
		domainexecution.SequentialManualIntervention,
		context.DeadlineExceeded,
	); err != nil {
		t.Fatal(err)
	}
	if err := store.AcknowledgeManualReconciliation(ctx, operation.ID); err != nil {
		t.Fatal(err)
	}
	if active, ok, err := store.ActiveSequentialOperation(ctx); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatalf("reconciled operation remains active: %#v", active)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var state, audit string
	if err := db.QueryRow(
		`SELECT state, last_error FROM sequential_live_operations WHERE operation_id=?`,
		operation.ID,
	).Scan(&state, &audit); err != nil {
		t.Fatal(err)
	}
	if state != string(domainexecution.SequentialReconciledManually) {
		t.Fatalf("state = %q", state)
	}
	if audit == "" {
		t.Fatal("manual reconciliation audit note is empty")
	}
}

func TestSequentialLiveStoreAcknowledgesRecoveryBlocked(t *testing.T) {
	store, err := sqlitestore.OpenSequentialLive(
		filepath.Join(t.TempDir(), "live.sqlite"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	input, _ := market.NewTokenAmount("quote-a", big.NewInt(1_000_000))
	now := time.Now().UTC()
	operation := domainexecution.SequentialOperation{
		ID: "operation-blocked", Plan: "plan-1",
		OpportunityID: "opportunity-1", ConfigHash: "config",
		State:         domainexecution.SequentialRunning,
		CurrentAmount: input, StartedAt: now, UpdatedAt: now,
	}
	ctx := context.Background()
	if err := store.CreateSequentialOperation(ctx, operation); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSequentialRecoveryState(
		ctx,
		operation.ID,
		domainexecution.SequentialRecoveryBlocked,
		context.DeadlineExceeded,
	); err != nil {
		t.Fatal(err)
	}
	if err := store.AcknowledgeManualReconciliation(
		ctx,
		operation.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.ActiveSequentialOperation(
		ctx,
	); err != nil || found {
		t.Fatalf("active found=%t err=%v", found, err)
	}
}

func TestSequentialLiveStoreExplicitlyRetriesRecoveryBlocked(t *testing.T) {
	store, err := sqlitestore.OpenSequentialLive(
		filepath.Join(t.TempDir(), "live.sqlite"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	input := mustTokenAmount(t, "quote-a", 1_000_000)
	now := time.Now().UTC()
	operation := domainexecution.SequentialOperation{
		ID: "operation-retry-blocked", Plan: "plan-1",
		OpportunityID: "opportunity-1", ConfigHash: "config",
		State:         domainexecution.SequentialRunning,
		CurrentAmount: input, StartedAt: now, UpdatedAt: now,
	}
	ctx := context.Background()
	if err := store.CreateSequentialOperation(ctx, operation); err != nil {
		t.Fatal(err)
	}
	if err := store.SetSequentialRecoveryState(
		ctx,
		operation.ID,
		domainexecution.SequentialRecoveryBlocked,
		context.DeadlineExceeded,
	); err != nil {
		t.Fatal(err)
	}
	if err := store.RetryBlockedSequentialRecovery(ctx, operation.ID); err != nil {
		t.Fatal(err)
	}
	active, found, err := store.ActiveSequentialOperation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !found || active.State != domainexecution.SequentialRecovering ||
		active.LastError == "" {
		t.Fatalf("unexpected retried operation: %#v", active)
	}
	if err := store.RetryBlockedSequentialRecovery(
		ctx, operation.ID,
	); err == nil {
		t.Fatal("expected a second retry authorization to be rejected")
	}
}

func TestSequentialLiveStorePersistsRealizedCostsAndPnL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "live-costs.sqlite")
	store, err := sqlitestore.OpenSequentialLive(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Now().UTC()
	amounts := []market.TokenAmount{
		mustTokenAmount(t, "quote-a", 1_000_000),
		mustTokenAmount(t, "base-a", 4_000_000_000),
		mustTokenAmount(t, "base-b", 4_000_000_000_000_000_000),
		mustTokenAmount(t, "quote-b", 1_001_000),
		mustTokenAmount(t, "quote-a", 1_000_900),
	}
	operation := domainexecution.SequentialOperation{
		ID: "operation-cost", Plan: "plan-cost", OpportunityID: "opportunity-cost",
		ConfigHash: "config", State: domainexecution.SequentialRunning,
		CurrentAmount: amounts[0], StartedAt: now, UpdatedAt: now,
	}
	if err := store.CreateSequentialOperation(ctx, operation); err != nil {
		t.Fatal(err)
	}
	stages := []domainexecution.SequentialStagePlan{
		{Ordinal: 1, Stage: domainexecution.StageBuy, SourceChain: "chain-a", InputToken: "quote-a", OutputToken: "base-a", Market: "market-a"},
		{Ordinal: 2, Stage: domainexecution.StageBridgeBase, SourceChain: "chain-a", DestinationChain: "chain-b", InputToken: "base-a", OutputToken: "base-b"},
		{Ordinal: 3, Stage: domainexecution.StageSell, SourceChain: "chain-b", InputToken: "base-b", OutputToken: "quote-b", Market: "market-b"},
		{Ordinal: 4, Stage: domainexecution.StageBridgeQuoteReturn, SourceChain: "chain-b", DestinationChain: "chain-a", InputToken: "quote-b", OutputToken: "quote-a"},
	}
	costAmount, _ := market.NewAssetQuantity("sol", big.NewRat(1, 100_000))
	costValue, _ := market.NewAssetQuantity("quote", big.NewRat(1, 1_000))
	settlements := make([]domainexecution.SequentialStageSettlement, 0, 4)
	for index, stage := range stages {
		identity := domainexecution.TransactionIdentity{
			Chain: stage.SourceChain, Account: "account",
			Hash: "source-" + string(rune('1'+index)),
		}
		var destination *domainexecution.TransactionIdentity
		if stage.DestinationChain != "" {
			value := domainexecution.TransactionIdentity{
				Chain: stage.DestinationChain, Account: "account",
				Hash: "destination-" + string(rune('1'+index)),
			}
			destination = &value
		}
		settlement := domainexecution.SequentialStageSettlement{
			Request: domainexecution.SequentialStageRequest{
				Operation: operation.ID, Plan: operation.Plan,
				Stage: stage, Input: amounts[index],
			},
			ActualInput: amounts[index], ActualOutput: amounts[index+1],
			SourceIdentity: identity, DestinationIdentity: destination,
			ObservedAt: now.Add(time.Duration(index+1) * time.Second),
			Evidence:   "test",
		}
		if index == 0 {
			settlement.Costs = []domainexecution.CostComponent{{
				Kind: "network_fee", Chain: "chain-a", Amount: costAmount,
				QuoteValue: costValue, Evidence: "receipt",
			}}
		}
		if err := store.RecordStageSettlement(ctx, settlement); err != nil {
			t.Fatal(err)
		}
		settlements = append(settlements, settlement)
	}
	gross, _ := market.NewAssetQuantity("quote", big.NewRat(9, 10_000))
	net, _ := market.NewAssetQuantity("quote", big.NewRat(-1, 10_000))
	result := executionport.SequentialResult{
		Operation: operation.ID, FinalAmount: amounts[4],
		Settlements: settlements, Costs: settlements[0].Costs,
		ExecutionCost: costValue, ExternalCost: costValue,
		RealizedGross: gross, RealizedNetPnL: net,
	}
	if err := store.RecordSequentialResult(ctx, result); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	var netValue string
	if err := db.QueryRow(
		`SELECT COUNT(*), net_value FROM sequential_live_results`,
	).Scan(&count, &netValue); err != nil {
		t.Fatal(err)
	}
	if count != 1 || netValue != "-1/10000" {
		t.Fatalf("result count=%d net=%s", count, netValue)
	}
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM sequential_live_costs`,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("cost count=%d", count)
	}
}

func TestSequentialLiveStoreAcceptsPrefundedSettlementInputReference(t *testing.T) {
	store, err := sqlitestore.OpenSequentialLive(
		filepath.Join(t.TempDir(), "prefunded-live.sqlite"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	input := mustTokenAmount(t, "quote-a", 1_000_000)
	discoveryInput := mustTokenAmount(t, "quote-a", 750_000_000)
	discoveryBaseA := mustTokenAmount(t, "base-a", 3_614_009_167_212)
	baseA := mustTokenAmount(t, "base-a", 4_825_799_926)
	baseB := mustTokenAmount(t, "base-b", 4_825_799_926_000_000_000)
	quoteB := mustTokenAmount(t, "quote-b", 994_662)
	inputValue, _ := market.NewAssetQuantity("quote", big.NewRat(750, 1))
	outputValue, _ := market.NewAssetQuantity("quote", big.NewRat(994_662, 1_000_000))
	opportunity := arbitrage.Opportunity{
		Evaluation: "prefunded-evaluation", ConfigHash: "config",
		Classification: arbitrage.ClassificationPolicyQualified,
		Direction: arbitrage.Direction{
			BuyMarket: "market-a", SellMarket: "market-b",
		},
		Candidates: []arbitrage.Candidate{{
			Input: inputValue, Output: outputValue,
			BuyQuote: market.Quote{
				AmountIn: discoveryInput, AmountOut: discoveryBaseA,
			},
			SellQuote: market.Quote{
				AmountIn: baseB, AmountOut: quoteB,
			},
		}},
		SelectedIndex: 0,
	}
	plan, err := domainexecution.NewPrefundedSequentialPlan(
		"prefunded-plan", opportunity, input, "chain-a", "chain-b", now,
	)
	if err != nil {
		t.Fatal(err)
	}
	operation := domainexecution.SequentialOperation{
		ID: "prefunded-operation", Plan: plan.ID,
		OpportunityID: string(opportunity.Evaluation),
		ConfigHash:    opportunity.ConfigHash,
		State:         domainexecution.SequentialRunning,
		CurrentAmount: input, StartedAt: now, UpdatedAt: now,
	}
	if err := store.CreateRecoverableSequentialOperation(
		ctx, operation, plan,
	); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.LoadSequentialRecovery(ctx, operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	durableDiscovery := snapshot.Plan.Opportunity.Candidates[0].
		BuyQuote.AmountIn
	if durableDiscovery.Units().Cmp(discoveryInput.Units()) != 0 {
		t.Fatalf(
			"durable discovery input=%s want=%s",
			durableDiscovery, discoveryInput,
		)
	}
	settlements := []domainexecution.SequentialStageSettlement{
		{
			Request: domainexecution.SequentialStageRequest{
				Operation: operation.ID, Plan: plan.ID,
				Stage: plan.Stages[0], Input: input,
			},
			ActualInput: input, ActualOutput: baseA,
			SourceIdentity: domainexecution.TransactionIdentity{
				Chain: "chain-a", Account: "account-a", Hash: "buy",
			},
			ObservedAt: now.Add(time.Second), Evidence: "buy-receipt",
		},
		{
			Request: domainexecution.SequentialStageRequest{
				Operation: operation.ID, Plan: plan.ID,
				Stage: plan.Stages[1], Input: baseB,
			},
			ActualInput: baseB, ActualOutput: quoteB,
			SourceIdentity: domainexecution.TransactionIdentity{
				Chain: "chain-b", Account: "account-b", Hash: "sell",
			},
			ObservedAt: now.Add(2 * time.Second), Evidence: "sell-receipt",
		},
	}
	for _, settlement := range settlements {
		if err := store.RecordStageSettlement(ctx, settlement); err != nil {
			t.Fatal(err)
		}
	}
	active, found, err := store.ActiveSequentialOperation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !found || active.CurrentStage != 2 ||
		active.CurrentAmount.Token() != quoteB.Token() ||
		active.CurrentAmount.Units().Cmp(quoteB.Units()) != 0 {
		t.Fatalf("unexpected prefunded durable state: %#v", active)
	}
}

func TestSequentialLiveStorePersistsPostBridgeExitDecision(t *testing.T) {
	path := filepath.Join(t.TempDir(), "live-exit.sqlite")
	store, err := sqlitestore.OpenSequentialLive(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	input := mustTokenAmount(t, "quote-a", 1_000_000)
	baseA := mustTokenAmount(t, "base-a", 4_000_000_000)
	baseB := mustTokenAmount(t, "base-b", 4_000_000_000_000_000_000)
	operation := domainexecution.SequentialOperation{
		ID: "operation-exit", Plan: "plan-exit",
		OpportunityID: "opportunity-exit", ConfigHash: "config",
		State: domainexecution.SequentialRunning, CurrentAmount: input,
		StartedAt: now, UpdatedAt: now,
	}
	if err := store.CreateSequentialOperation(ctx, operation); err != nil {
		t.Fatal(err)
	}
	stages := []domainexecution.SequentialStagePlan{
		{
			Ordinal: 1, Stage: domainexecution.StageBuy,
			SourceChain: "chain-a", InputToken: "quote-a",
			OutputToken: "base-a", Market: "market-a",
		},
		{
			Ordinal: 2, Stage: domainexecution.StageBridgeBase,
			SourceChain: "chain-a", DestinationChain: "chain-b",
			InputToken: "base-a", OutputToken: "base-b",
		},
	}
	amounts := []market.TokenAmount{input, baseA, baseB}
	for index, stage := range stages {
		source := domainexecution.TransactionIdentity{
			Chain: stage.SourceChain, Account: "account",
			Hash: "source-exit-" + string(rune('1'+index)),
		}
		var destination *domainexecution.TransactionIdentity
		if stage.DestinationChain != "" {
			value := domainexecution.TransactionIdentity{
				Chain: stage.DestinationChain, Account: "account",
				Hash: "destination-exit-2",
			}
			destination = &value
		}
		if err := store.RecordStageSettlement(
			ctx,
			domainexecution.SequentialStageSettlement{
				Request: domainexecution.SequentialStageRequest{
					Operation: operation.ID, Plan: operation.Plan,
					Stage: stage, Input: amounts[index],
				},
				ActualInput: amounts[index], ActualOutput: amounts[index+1],
				SourceIdentity: source, DestinationIdentity: destination,
				ObservedAt: now.Add(time.Duration(index+1) * time.Second),
				Evidence:   "test",
			},
		); err != nil {
			t.Fatal(err)
		}
	}
	destinationRecovery, _ := market.NewAssetQuantity(
		"quote", big.NewRat(97, 100),
	)
	returnRecovery, _ := market.NewAssetQuantity(
		"quote", big.NewRat(99, 100),
	)
	margin, _ := market.NewAssetQuantity("quote", big.NewRat(1, 100))
	decision := domainexecution.SequentialExitDecision{
		Operation: operation.ID, Route: domainexecution.ExitReturnToOrigin,
		DestinationOutput:     mustTokenAmount(t, "quote-b", 980_000),
		ReturnOutput:          mustTokenAmount(t, "quote-a", 1_000_000),
		DestinationRecovery:   destinationRecovery,
		ReturnRecovery:        returnRecovery,
		SafetyMargin:          margin,
		DestinationQualified:  false,
		CostEvidenceAvailable: true,
		DecidedAt:             now.Add(3 * time.Second),
		Evidence:              "fresh-comparison",
	}
	if err := store.RecordSequentialExitDecision(ctx, decision); err != nil {
		t.Fatal(err)
	}
	failedGas, _ := market.NewAssetQuantity("pol", big.NewRat(1, 100))
	failedGasValue, _ := market.NewAssetQuantity("quote", big.NewRat(2, 100))
	if err := store.RecordStageFailureCosts(
		ctx,
		operation.ID,
		3,
		[]domainexecution.CostComponent{{
			Kind: "network_fee", Chain: "chain-b",
			Amount: failedGas, QuoteValue: failedGasValue,
			Evidence: "evm_receipt_gas",
		}},
	); err != nil {
		t.Fatal(err)
	}
	decision.Route = domainexecution.ExitSellAtDestination
	decision.ReturnOutput = market.TokenAmount{}
	decision.ReturnRecovery = market.AssetQuantity{}
	decision.DestinationQualified = true
	decision.DecidedAt = now.Add(4 * time.Second)
	decision.Evidence = "fresh-comparison+automatic-recovery"
	if err := store.RecordSequentialExitDecision(ctx, decision); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var route, destination, returned string
	if err := db.QueryRow(
		`SELECT route, destination_recovery, return_recovery
		 FROM sequential_live_exit_decisions WHERE operation_id=?`,
		operation.ID,
	).Scan(&route, &destination, &returned); err != nil {
		t.Fatal(err)
	}
	if route != string(domainexecution.ExitSellAtDestination) ||
		destination != "97/100" || returned != "" {
		t.Fatalf(
			"route=%s destination=%s return=%s",
			route, destination, returned,
		)
	}
	var costIndex int
	if err := db.QueryRow(
		`SELECT component_index FROM sequential_live_costs
			WHERE operation_id=? AND ordinal=3`,
		operation.ID,
	).Scan(&costIndex); err != nil {
		t.Fatal(err)
	}
	if costIndex >= 0 {
		t.Fatalf("failure cost index=%d, want negative", costIndex)
	}
}

func mustTokenAmount(
	t *testing.T,
	token market.TokenID,
	units int64,
) market.TokenAmount {
	t.Helper()
	amount, err := market.NewTokenAmount(token, big.NewInt(units))
	if err != nil {
		t.Fatal(err)
	}
	return amount
}
