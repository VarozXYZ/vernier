package livecanary_test

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	notificationport "github.com/VarozXYZ/vernier/ports/notification"
	"github.com/VarozXYZ/vernier/runtime/configuration"
	"github.com/VarozXYZ/vernier/runtime/livecanary"
)

type liveNotificationSender struct {
	mu        sync.Mutex
	events    []notificationport.LiveExecutionEvent
	lifecycle []notificationport.LiveRuntimeEvent
	order     []string
}

func (s *liveNotificationSender) SendLiveExecution(
	_ context.Context,
	event notificationport.LiveExecutionEvent,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	s.order = append(s.order, "execution:"+string(event.Kind))
	return nil
}

func (s *liveNotificationSender) SendLiveRuntime(
	_ context.Context,
	event notificationport.LiveRuntimeEvent,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lifecycle = append(s.lifecycle, event)
	s.order = append(s.order, "runtime:"+string(event.Kind))
	return nil
}

func TestLiveNotifierPreservesLifecycleAndExecutionOrder(t *testing.T) {
	sender := &liveNotificationSender{}
	notifier, err := livecanary.NewLiveNotifier(sender, nil)
	if err != nil {
		t.Fatal(err)
	}
	notifier.NotifyRuntime(notificationport.LiveRuntimeEvent{
		Kind: notificationport.LiveRuntimeStarted,
		Mode: "live",
	})
	notifier.Notify(notificationport.LiveExecutionEvent{
		Kind:      notificationport.LiveExecutionStarted,
		Operation: "operation",
	})
	notifier.NotifyRuntime(notificationport.LiveRuntimeEvent{
		Kind: notificationport.LiveRuntimeStopped,
		Mode: "live", Reason: "completed",
	})
	notifier.Close()

	sender.mu.Lock()
	defer sender.mu.Unlock()
	got := strings.Join(sender.order, ",")
	want := "runtime:started,execution:started,runtime:stopped"
	if got != want {
		t.Fatalf("notification order=%q, want %q", got, want)
	}
}

func TestProgressObserverEmitsNonBlockingLifecycleEvents(t *testing.T) {
	sender := &liveNotificationSender{}
	notifier, err := livecanary.NewLiveNotifier(sender, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	observer, err := livecanary.NewProgressObserver(
		notifier,
		map[market.TokenID]market.Token{
			"quote-a": {ID: "quote-a", Asset: "quote", Symbol: "QUOTE", Decimals: 6},
			"base-a":  {ID: "base-a", Asset: "base", Symbol: "BASE", Decimals: 9},
			"quote-b": {ID: "quote-b", Asset: "quote", Symbol: "QUOTE", Decimals: 6},
			"base-b":  {ID: "base-b", Asset: "base", Symbol: "BASE", Decimals: 18},
		},
		map[market.ChainID]configuration.ResolvedChain{
			"chain-a": {
				ID: "chain-a", Label: "Chain A", Kind: "solana",
			},
			"chain-b": {
				ID: "chain-b", Label: "Chain B", Kind: "evm",
			},
		},
		func() time.Time { return now },
	)
	if err != nil {
		t.Fatal(err)
	}
	opportunity := liveCanaryOpportunity(t)
	initial := opportunity.Candidates[0].BuyQuote.AmountIn
	plan, err := execution.NewSequentialPlan(
		"plan", opportunity, initial, "chain-a", "chain-b", now,
	)
	if err != nil {
		t.Fatal(err)
	}
	operation := execution.SequentialOperation{
		ID: "operation", Plan: plan.ID, State: execution.SequentialRunning,
		CurrentAmount: initial, StartedAt: now, UpdatedAt: now,
	}
	observer.OperationStarted(operation, plan)
	request := execution.SequentialStageRequest{
		Operation: operation.ID, Plan: plan.ID,
		Stage: plan.Stages[0], Input: initial,
	}
	observer.StageStarted(request)
	output := opportunity.Candidates[0].BuyQuote.AmountOut
	settlement := execution.SequentialStageSettlement{
		Request: request, ActualInput: initial, ActualOutput: output,
		SourceIdentity: execution.TransactionIdentity{
			Chain: "chain-a", Account: "account", Hash: "signature",
		},
		ObservedAt: now, Evidence: "websocket",
	}
	observer.StageSettled(settlement)
	recovery, _ := market.NewAssetQuantity("quote", big.NewRat(99, 100))
	margin, _ := market.NewAssetQuantity("quote", big.NewRat(1, 100))
	observer.ExitSelected(execution.SequentialExitDecision{
		Operation: operation.ID,
		Route:     execution.ExitSellAtDestination,
		DestinationOutput: func() market.TokenAmount {
			value, _ := market.NewTokenAmount(
				"quote-b", big.NewInt(990_000),
			)
			return value
		}(),
		DestinationRecovery: recovery,
		SafetyMargin:        margin,
		DecidedAt:           now,
		Evidence:            "test",
	})
	result := executionport.SequentialResult{
		Operation: operation.ID, FinalAmount: output,
		Settlements: []execution.SequentialStageSettlement{settlement},
	}
	result.ExecutionCost, _ = market.NewAssetQuantity("quote", big.NewRat(1, 10))
	result.ExternalCost, _ = market.NewAssetQuantity("quote", big.NewRat(3, 50))
	result.RealizedNetPnL, _ = market.NewAssetQuantity("quote", big.NewRat(-1, 20))
	observer.OperationFinished(
		operation, execution.SequentialCompleted, result, nil,
	)
	// A runtime fallback must not duplicate an already queued terminal event.
	notifier.Notify(notificationport.LiveExecutionEvent{
		Kind:      notificationport.LiveExecutionFailed,
		Operation: string(operation.ID), Detail: "duplicate",
	})
	notifier.Close()

	sender.mu.Lock()
	defer sender.mu.Unlock()
	if len(sender.events) != 5 {
		t.Fatalf("events=%d, want 5", len(sender.events))
	}
	expected := []notificationport.LiveExecutionEventKind{
		notificationport.LiveExecutionStarted,
		notificationport.LiveExecutionStageStarted,
		notificationport.LiveExecutionStageCompleted,
		notificationport.LiveExecutionExitSelected,
		notificationport.LiveExecutionCompleted,
	}
	for index, kind := range expected {
		if sender.events[index].Kind != kind {
			t.Fatalf(
				"event %d kind=%q, want %q",
				index, sender.events[index].Kind, kind,
			)
		}
	}
	if sender.events[2].Input != "750 QUOTE" ||
		sender.events[2].Output != "3 BASE" {
		t.Fatalf("unexpected settlement amounts: %+v", sender.events[2])
	}
	if sender.events[3].Stage != string(execution.ExitSellAtDestination) ||
		sender.events[3].DestinationValue != "0.99 QUOTE" {
		t.Fatalf("unexpected exit event: %+v", sender.events[3])
	}
	if sender.events[4].ExecutionCost != "0.06 QUOTE" ||
		sender.events[4].NetPnL != "-0.05 QUOTE" {
		t.Fatalf("unexpected realized economics event: %+v", sender.events[4])
	}
}

func TestPrefundedProgressDirectionUsesBuyAndSellChains(t *testing.T) {
	t.Parallel()

	sender := &liveNotificationSender{}
	notifier, err := livecanary.NewLiveNotifier(sender, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	observer := newSyntheticProgressObserver(t, notifier, now)
	opportunity := liveCanaryOpportunity(t)
	initial := opportunity.Candidates[0].BuyQuote.AmountIn
	plan, err := execution.NewPrefundedSequentialPlan(
		"prefunded-plan",
		opportunity,
		initial,
		"chain-a",
		"chain-b",
		now,
	)
	if err != nil {
		t.Fatal(err)
	}
	observer.OperationStarted(execution.SequentialOperation{
		ID: "prefunded-operation", Plan: plan.ID,
		State: execution.SequentialRunning, CurrentAmount: initial,
		StartedAt: now, UpdatedAt: now,
	}, plan)
	request := execution.SequentialStageRequest{
		Operation: "prefunded-operation", Plan: plan.ID,
		Stage: plan.Stages[0], Input: initial,
	}
	observer.StageStarted(request)
	observer.StageSettled(execution.SequentialStageSettlement{
		Request: request, ActualInput: initial,
		ActualOutput: opportunity.Candidates[0].BuyQuote.AmountOut,
		SourceIdentity: execution.TransactionIdentity{
			Chain: "chain-a", Account: "synthetic-account", Hash: "synthetic-tx",
		},
		ObservedAt: now, Evidence: "durable_receipt",
	})
	notifier.Close()

	sender.mu.Lock()
	defer sender.mu.Unlock()
	if len(sender.events) != 3 || sender.events[0].Direction != "Chain A -> Chain B" {
		t.Fatalf("unexpected prefunded direction events: %+v", sender.events)
	}
}

func TestProgressObserverLinksEVMTransactionsAndCompletesAsyncReturn(t *testing.T) {
	t.Parallel()

	sender := &liveNotificationSender{}
	notifier, err := livecanary.NewLiveNotifier(sender, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	observer, err := livecanary.NewProgressObserver(
		notifier,
		map[market.TokenID]market.Token{
			"quote-a": {ID: "quote-a", Asset: "quote", Symbol: "QUOTE", Decimals: 6},
			"base-a":  {ID: "base-a", Asset: "base", Symbol: "BASE", Decimals: 9},
			"quote-b": {ID: "quote-b", Asset: "quote", Symbol: "QUOTE", Decimals: 6},
			"base-b":  {ID: "base-b", Asset: "base", Symbol: "BASE", Decimals: 18},
		},
		map[market.ChainID]configuration.ResolvedChain{
			"chain-a": {ID: "chain-a", Label: "Chain A", Kind: "evm", ChainID: big.NewInt(8453)},
			"chain-b": {ID: "chain-b", Label: "Chain B", Kind: "evm", ChainID: big.NewInt(56)},
		},
		func() time.Time { return now },
	)
	if err != nil {
		t.Fatal(err)
	}
	opportunity := liveCanaryOpportunity(t)
	initial := opportunity.Candidates[0].BuyQuote.AmountIn
	plan, err := execution.NewPrefundedSequentialPlan(
		"async-plan", opportunity, initial, "chain-a", "chain-b", now,
	)
	if err != nil {
		t.Fatal(err)
	}
	operation := execution.SequentialOperation{
		ID: "async-operation", Plan: plan.ID, State: execution.SequentialRunning,
		CurrentAmount: initial, StartedAt: now, UpdatedAt: now,
	}
	observer.OperationStarted(operation, plan)
	buy := execution.SequentialStageRequest{
		Operation: operation.ID, Plan: plan.ID, Stage: plan.Stages[0], Input: initial,
	}
	observer.StageStarted(buy)
	observer.StageSettled(execution.SequentialStageSettlement{
		Request: buy, ActualInput: initial,
		ActualOutput:   opportunity.Candidates[0].BuyQuote.AmountOut,
		SourceIdentity: execution.TransactionIdentity{Chain: "chain-a", Hash: "0xbuy"},
		ObservedAt:     now, Evidence: "durable_receipt",
	})
	quoteOutput := opportunity.Candidates[0].SellQuote.AmountOut
	quoteReturn := execution.SequentialStageRequest{
		Operation: operation.ID, Plan: plan.ID, Stage: plan.Stages[3], Input: quoteOutput,
	}
	observer.StageStarted(quoteReturn)
	observer.OperationFinished(operation, execution.SequentialCompleted, executionport.SequentialResult{
		Operation: operation.ID, FinalAmount: quoteOutput,
	}, nil)
	observer.StageSettled(execution.SequentialStageSettlement{
		Request: quoteReturn, ActualInput: quoteOutput, ActualOutput: initial,
		SourceIdentity:      execution.TransactionIdentity{Chain: "chain-b", Hash: "0xreturn"},
		DestinationIdentity: &execution.TransactionIdentity{Chain: "chain-a", Hash: "0xreceipt"},
		ObservedAt:          now, Evidence: "bridge_delivery",
	})
	notifier.Close()

	sender.mu.Lock()
	defer sender.mu.Unlock()
	var buyEvent, returnEvent notificationport.LiveExecutionEvent
	terminalIndex, returnIndex := -1, -1
	for index, event := range sender.events {
		switch {
		case event.Kind == notificationport.LiveExecutionStageCompleted && event.Ordinal == 1:
			buyEvent = event
		case event.Kind == notificationport.LiveExecutionCompleted:
			terminalIndex = index
		case event.Kind == notificationport.LiveExecutionStageCompleted && event.Ordinal == 4:
			returnEvent = event
			returnIndex = index
		}
	}
	if buyEvent.SourceURL != "https://basescan.org/tx/0xbuy" {
		t.Fatalf("buy explorer URL=%q", buyEvent.SourceURL)
	}
	if returnEvent.SourceURL != "https://bscscan.com/tx/0xreturn" ||
		returnEvent.DestinationURL != "https://basescan.org/tx/0xreceipt" {
		t.Fatalf("return explorer URLs=%q -> %q", returnEvent.SourceURL, returnEvent.DestinationURL)
	}
	if terminalIndex < 0 || returnIndex <= terminalIndex {
		t.Fatalf("async return was not emitted after completion: %+v", sender.events)
	}
}

func TestProgressObserverSuppressesDefinitiveFailureBeforeBuySettlement(t *testing.T) {
	t.Parallel()

	sender := &liveNotificationSender{}
	notifier, err := livecanary.NewLiveNotifier(sender, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	observer := newSyntheticProgressObserver(t, notifier, now)
	opportunity := liveCanaryOpportunity(t)
	initial := opportunity.Candidates[0].BuyQuote.AmountIn
	plan, err := execution.NewPrefundedSequentialPlan(
		"silent-plan", opportunity, initial, "chain-a", "chain-b", now,
	)
	if err != nil {
		t.Fatal(err)
	}
	operation := execution.SequentialOperation{
		ID: "silent-operation", Plan: plan.ID,
		State: execution.SequentialRunning, CurrentAmount: initial,
		StartedAt: now, UpdatedAt: now,
	}
	observer.OperationStarted(operation, plan)
	observer.StageStarted(execution.SequentialStageRequest{
		Operation: operation.ID, Plan: plan.ID,
		Stage: plan.Stages[0], Input: initial,
	})
	observer.OperationFinished(
		operation,
		execution.SequentialAborted,
		executionport.SequentialResult{Operation: operation.ID},
		executionport.NewStageError(
			executionport.DispositionRejected,
			errors.New("artifact expired before broadcast"),
		),
	)
	notifier.Close()

	sender.mu.Lock()
	defer sender.mu.Unlock()
	if len(sender.events) != 0 {
		t.Fatalf("definitive pre-buy failure emitted events: %+v", sender.events)
	}
}

func TestLiveNotifierSuppressesRetryablePreflightFailure(t *testing.T) {
	t.Parallel()

	sender := &liveNotificationSender{}
	notifier, err := livecanary.NewLiveNotifier(sender, nil)
	if err != nil {
		t.Fatal(err)
	}
	notifier.Notify(notificationport.LiveExecutionEvent{
		Kind:      notificationport.LiveExecutionFailed,
		Operation: "preflight-operation", State: "aborted_retrying",
		Detail: "buy threshold is below required output",
	})
	notifier.Close()

	sender.mu.Lock()
	defer sender.mu.Unlock()
	if len(sender.events) != 0 {
		t.Fatalf("preflight failure emitted events: %+v", sender.events)
	}
}

func TestRecoveryReplaysDurableStagesAndEmitsFinalCompletion(t *testing.T) {
	t.Parallel()

	sender := &liveNotificationSender{}
	notifier, err := livecanary.NewLiveNotifier(sender, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	progress := newSyntheticProgressObserver(t, notifier, now)
	recovery := livecanary.NewRecoveryObserver(notifier, progress, func() time.Time {
		return now
	})
	opportunity := liveCanaryOpportunity(t)
	initial := opportunity.Candidates[0].BuyQuote.AmountIn
	plan, err := execution.NewPrefundedSequentialPlan(
		"recovery-plan", opportunity, initial,
		"chain-a", "chain-b", now,
	)
	if err != nil {
		t.Fatal(err)
	}
	operation := execution.SequentialOperation{
		ID: "recovery-operation", Plan: plan.ID,
		State:         execution.SequentialRecovering,
		CurrentAmount: initial, StartedAt: now.Add(-time.Minute), UpdatedAt: now,
	}
	request := execution.SequentialStageRequest{
		Operation: operation.ID, Plan: plan.ID,
		Stage: plan.Stages[0], Input: initial,
	}
	settlement := execution.SequentialStageSettlement{
		Request: request, ActualInput: initial,
		ActualOutput: opportunity.Candidates[0].BuyQuote.AmountOut,
		SourceIdentity: execution.TransactionIdentity{
			Chain: "chain-a", Account: "synthetic-account", Hash: "synthetic-tx",
		},
		ObservedAt: now, Evidence: "durable_receipt",
	}
	// Model the terminal failure already sent before recovery took ownership.
	notifier.Notify(notificationport.LiveExecutionEvent{
		Kind:      notificationport.LiveExecutionFailed,
		Operation: string(operation.ID), Detail: "temporary failure",
	})
	recovery.RecoveryStarted(executionport.SequentialRecoverySnapshot{
		Operation: operation, Plan: plan,
		Settlements: []execution.SequentialStageSettlement{settlement},
	})
	result := executionport.SequentialResult{
		Operation: operation.ID, FinalAmount: initial,
		Settlements: []execution.SequentialStageSettlement{settlement},
	}
	result.ExternalCost, _ = market.NewAssetQuantity("quote", new(big.Rat))
	result.RealizedNetPnL, _ = market.NewAssetQuantity("quote", new(big.Rat))
	recovery.RecoveryCompleted(result)
	notifier.Close()

	sender.mu.Lock()
	defer sender.mu.Unlock()
	var completed bool
	for _, event := range sender.events {
		if event.Kind == notificationport.LiveExecutionCompleted {
			completed = true
			if event.Direction != "Chain A -> Chain B" {
				t.Fatalf("completed direction=%q", event.Direction)
			}
		}
	}
	if !completed {
		t.Fatalf("recovery never emitted final completion: %+v", sender.events)
	}
}

func TestRecoveryWithoutEconomicEffectEmitsAbortedNotCompleted(t *testing.T) {
	t.Parallel()

	sender := &liveNotificationSender{}
	notifier, err := livecanary.NewLiveNotifier(sender, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	progress := newSyntheticProgressObserver(t, notifier, now)
	recovery := livecanary.NewRecoveryObserver(notifier, progress, func() time.Time { return now })
	opportunity := liveCanaryOpportunity(t)
	initial := opportunity.Candidates[0].BuyQuote.AmountIn
	plan, err := execution.NewPrefundedSequentialPlan(
		"aborted-recovery-plan", opportunity, initial, "chain-a", "chain-b", now,
	)
	if err != nil {
		t.Fatal(err)
	}
	operation := execution.SequentialOperation{
		ID: "aborted-recovery-operation", Plan: plan.ID,
		State: execution.SequentialRecovering, CurrentAmount: initial,
		StartedAt: now.Add(-time.Second), UpdatedAt: now,
	}
	recovery.RecoveryStarted(executionport.SequentialRecoverySnapshot{
		Operation: operation, Plan: plan,
	})
	cause := errors.New("initial purchase had no confirmed economic effect")
	recovery.RecoveryAborted(operation, executionport.SequentialResult{
		Operation: operation.ID, TerminalState: execution.SequentialAborted,
		TerminalError: cause.Error(),
	}, cause)
	notifier.Close()

	sender.mu.Lock()
	defer sender.mu.Unlock()
	var failed bool
	for _, event := range sender.events {
		if event.Kind == notificationport.LiveExecutionCompleted ||
			event.Kind == notificationport.LiveExecutionRecoveryCompleted {
			t.Fatalf("no-effect recovery emitted completion: %+v", sender.events)
		}
		if event.Kind == notificationport.LiveExecutionFailed &&
			event.State == string(execution.SequentialAborted) {
			failed = true
		}
	}
	if !failed {
		t.Fatalf("no-effect recovery did not emit terminal aborted event: %+v", sender.events)
	}
}

func newSyntheticProgressObserver(
	t *testing.T,
	notifier *livecanary.LiveNotifier,
	now time.Time,
) *livecanary.ProgressObserver {
	t.Helper()
	observer, err := livecanary.NewProgressObserver(
		notifier,
		map[market.TokenID]market.Token{
			"quote-a": {ID: "quote-a", Asset: "quote", Symbol: "QUOTE", Decimals: 6},
			"base-a":  {ID: "base-a", Asset: "base", Symbol: "BASE", Decimals: 9},
			"quote-b": {ID: "quote-b", Asset: "quote", Symbol: "QUOTE", Decimals: 6},
			"base-b":  {ID: "base-b", Asset: "base", Symbol: "BASE", Decimals: 18},
		},
		map[market.ChainID]configuration.ResolvedChain{
			"chain-a": {ID: "chain-a", Label: "Chain A", Kind: "solana"},
			"chain-b": {ID: "chain-b", Label: "Chain B", Kind: "evm"},
		},
		func() time.Time { return now },
	)
	if err != nil {
		t.Fatal(err)
	}
	return observer
}
