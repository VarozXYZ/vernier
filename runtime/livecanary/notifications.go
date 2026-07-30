package livecanary

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	notificationport "github.com/VarozXYZ/vernier/ports/notification"
	"github.com/VarozXYZ/vernier/runtime/configuration"
)

type LiveNotifier struct {
	sender        notificationport.LiveExecutionSender
	runtimeSender notificationport.LiveRuntimeSender
	logger        *slog.Logger
	queue         chan liveNotification

	mu        sync.Mutex
	closed    bool
	terminal  map[string]struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

type liveNotification struct {
	execution *notificationport.LiveExecutionEvent
	runtime   *notificationport.LiveRuntimeEvent
}

func NewLiveNotifier(
	sender notificationport.LiveExecutionSender,
	logger *slog.Logger,
) (*LiveNotifier, error) {
	if sender == nil {
		return nil, fmt.Errorf("live notification sender is required")
	}
	notifier := &LiveNotifier{
		sender: sender, logger: logger,
		queue:    make(chan liveNotification, 64),
		terminal: make(map[string]struct{}),
	}
	if runtimeSender, ok := sender.(notificationport.LiveRuntimeSender); ok {
		notifier.runtimeSender = runtimeSender
	}
	notifier.wg.Add(1)
	go notifier.run()
	return notifier, nil
}

// Notify only enqueues an in-memory event. Telegram transport latency can
// never delay transaction preparation, persistence, or broadcast.
func (n *LiveNotifier) Notify(event notificationport.LiveExecutionEvent) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return
	}
	if event.Kind == notificationport.LiveExecutionCompleted ||
		event.Kind == notificationport.LiveExecutionFailed ||
		event.Kind == notificationport.LiveExecutionRecoveryBlocked ||
		event.Kind == notificationport.LiveExecutionRefuelCompleted ||
		event.Kind == notificationport.LiveExecutionRefuelFailed ||
		event.Kind == notificationport.LiveExecutionRefuelUncertain {
		key := terminalNotificationKey(event)
		if _, exists := n.terminal[key]; exists {
			return
		}
		n.terminal[key] = struct{}{}
	}
	select {
	case n.queue <- liveNotification{execution: &event}:
	default:
		if n.logger != nil {
			n.logger.Error(
				"Live Telegram queue is full; notification dropped",
				"operation", event.Operation, "kind", event.Kind,
			)
		}
	}
}

func terminalNotificationKey(
	event notificationport.LiveExecutionEvent,
) string {
	class := "execution"
	switch event.Kind {
	case notificationport.LiveExecutionRecoveryBlocked:
		class = "recovery"
	case notificationport.LiveExecutionRefuelCompleted,
		notificationport.LiveExecutionRefuelFailed,
		notificationport.LiveExecutionRefuelUncertain:
		class = "refuel"
	}
	return event.Operation + "/" + class
}

// NotifyRuntime enqueues process lifecycle state without putting Telegram on
// the execution path. Senders that do not implement LiveRuntimeSender simply
// ignore lifecycle events.
func (n *LiveNotifier) NotifyRuntime(event notificationport.LiveRuntimeEvent) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed || n.runtimeSender == nil {
		return
	}
	select {
	case n.queue <- liveNotification{runtime: &event}:
	default:
		if n.logger != nil {
			n.logger.Error(
				"Live Telegram lifecycle queue is full; notification dropped",
				"kind", event.Kind,
			)
		}
	}
}

func (n *LiveNotifier) Close() {
	n.closeOnce.Do(func() {
		n.mu.Lock()
		n.closed = true
		close(n.queue)
		n.mu.Unlock()
		n.wg.Wait()
	})
}

func (n *LiveNotifier) run() {
	defer n.wg.Done()
	for event := range n.queue {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		var err error
		var kind any
		switch {
		case event.execution != nil:
			kind = event.execution.Kind
			err = n.sender.SendLiveExecution(ctx, *event.execution)
		case event.runtime != nil && n.runtimeSender != nil:
			kind = event.runtime.Kind
			err = n.runtimeSender.SendLiveRuntime(ctx, *event.runtime)
		}
		cancel()
		if err != nil && n.logger != nil {
			n.logger.Error(
				"Live Telegram notification failed",
				"kind", kind, "error", err,
			)
		}
	}
}

type progressOperation struct {
	direction string
	initial   market.TokenAmount
	started   time.Time
	stage     execution.SequentialStageRequest
	stageAt   time.Time
}

type ProgressObserver struct {
	notifier *LiveNotifier
	tokens   map[market.TokenID]market.Token
	chains   map[market.ChainID]configuration.ResolvedChain
	clock    func() time.Time

	mu         sync.Mutex
	operations map[execution.OperationID]progressOperation
}

func NewProgressObserver(
	notifier *LiveNotifier,
	tokens map[market.TokenID]market.Token,
	chains map[market.ChainID]configuration.ResolvedChain,
	clock func() time.Time,
) (*ProgressObserver, error) {
	if notifier == nil || len(tokens) < 4 || len(chains) != 2 {
		return nil, fmt.Errorf("live progress observer configuration is incomplete")
	}
	if clock == nil {
		clock = time.Now
	}
	return &ProgressObserver{
		notifier: notifier, tokens: tokens, chains: chains, clock: clock,
		operations: make(map[execution.OperationID]progressOperation),
	}, nil
}

func (o *ProgressObserver) OperationStarted(
	operation execution.SequentialOperation,
	plan execution.SequentialPlan,
) {
	direction := ""
	if len(plan.Stages) == 4 {
		direction = o.chainLabel(plan.Stages[0].SourceChain) + " -> " +
			o.chainLabel(plan.Stages[2].SourceChain)
	}
	value := progressOperation{
		direction: direction, initial: plan.InitialInput,
		started: operation.StartedAt,
	}
	o.mu.Lock()
	o.operations[operation.ID] = value
	o.mu.Unlock()
	event := notificationport.LiveExecutionEvent{
		Kind:      notificationport.LiveExecutionStarted,
		Operation: string(operation.ID), Direction: direction,
		Input: o.amount(plan.InitialInput), OccurredAt: o.clock().UTC(),
	}
	opportunity := plan.Opportunity
	forcedCanary := isForcedCanaryOpportunity(opportunity)
	if forcedCanary {
		event.State = "forced_canary"
	}
	if opportunity.SelectedIndex >= 0 &&
		opportunity.SelectedIndex < len(opportunity.Candidates) {
		candidate := opportunity.Candidates[opportunity.SelectedIndex]
		event.BuyProvider = readableExecutionProvider(candidate.BuyQuote.Source)
		event.SellProvider = readableExecutionProvider(candidate.SellQuote.Source)
		if !forcedCanary {
			event.ExpectedBase = o.amount(candidate.BuyQuote.AmountOut)
			event.ExpectedOutput = o.amount(candidate.SellQuote.AmountOut)
			event.ExpectedNetPnL = o.assetAmount(candidate.NetPnL)
		}
	}
	event.Trigger, event.TriggerURL = o.opportunityTrigger(opportunity, plan)
	o.notifier.Notify(event)
}

func (o *ProgressObserver) StageStarted(
	request execution.SequentialStageRequest,
) {
	now := o.clock().UTC()
	o.mu.Lock()
	value := o.operations[request.Operation]
	value.stage, value.stageAt = request, now
	o.operations[request.Operation] = value
	o.mu.Unlock()
	o.notifier.Notify(notificationport.LiveExecutionEvent{
		Kind:      notificationport.LiveExecutionStageStarted,
		Operation: string(request.Operation),
		Stage:     string(request.Stage.Stage), Ordinal: request.Stage.Ordinal,
		TotalStages: 4, SourceChain: o.chainLabel(request.Stage.SourceChain),
		DestinationChain: o.chainLabel(request.Stage.DestinationChain),
		Input:            o.amount(request.Input), OccurredAt: now,
	})
}

func (o *ProgressObserver) StageSettled(
	settlement execution.SequentialStageSettlement,
) {
	now := o.clock().UTC()
	o.mu.Lock()
	value := o.operations[settlement.Request.Operation]
	stageAt := value.stageAt
	o.mu.Unlock()
	duration := time.Duration(0)
	if !stageAt.IsZero() {
		duration = now.Sub(stageAt)
	}
	event := notificationport.LiveExecutionEvent{
		Kind:      notificationport.LiveExecutionStageCompleted,
		Operation: string(settlement.Request.Operation),
		Stage:     string(settlement.Request.Stage.Stage),
		Ordinal:   settlement.Request.Stage.Ordinal, TotalStages: 4,
		SourceChain:      o.chainLabel(settlement.Request.Stage.SourceChain),
		DestinationChain: o.chainLabel(settlement.Request.Stage.DestinationChain),
		Input:            o.amount(settlement.ActualInput), Output: o.amount(settlement.ActualOutput),
		ExecutionCost:     o.costAmount(settlement.Costs),
		SourceTransaction: settlement.SourceIdentity.Hash,
		SourceURL: o.explorer(
			settlement.SourceIdentity.Chain, settlement.SourceIdentity.Hash,
		),
		Evidence: settlement.Evidence, Duration: duration, OccurredAt: now,
	}
	if settlement.DestinationIdentity != nil {
		event.DestinationTx = settlement.DestinationIdentity.Hash
		event.DestinationURL = o.explorer(
			settlement.DestinationIdentity.Chain,
			settlement.DestinationIdentity.Hash,
		)
	}
	o.notifier.Notify(event)
}

func (o *ProgressObserver) ExitSelected(
	decision execution.SequentialExitDecision,
) {
	destination := ""
	if !decision.DestinationOutput.IsZero() {
		destination = o.amount(decision.DestinationOutput)
	}
	alternative := ""
	if !decision.ReturnOutput.IsZero() {
		alternative = o.amount(decision.ReturnOutput)
	}
	returnValue := ""
	if decision.ReturnRecovery.Asset() != "" {
		returnValue = o.assetAmount(decision.ReturnRecovery)
	}
	o.notifier.Notify(notificationport.LiveExecutionEvent{
		Kind:              notificationport.LiveExecutionExitSelected,
		Operation:         string(decision.Operation),
		Stage:             string(decision.Route),
		Output:            destination,
		AlternativeOutput: alternative,
		DestinationValue:  o.assetAmount(decision.DestinationRecovery),
		ReturnValue:       returnValue,
		SafetyMargin:      o.assetAmount(decision.SafetyMargin),
		Evidence:          decision.Evidence,
		OccurredAt:        decision.DecidedAt,
	})
}

func (o *ProgressObserver) costAmount(
	costs []execution.CostComponent,
) string {
	var total market.AssetQuantity
	for _, cost := range costs {
		if cost.QuoteValue.Asset() == "" {
			continue
		}
		if total.Asset() == "" {
			total, _ = market.NewAssetQuantity(
				cost.QuoteValue.Asset(), new(big.Rat),
			)
		}
		if total.Asset() != cost.QuoteValue.Asset() {
			return ""
		}
		total, _ = total.Add(cost.QuoteValue)
	}
	if total.Asset() == "" {
		return ""
	}
	symbol := string(total.Asset())
	for _, token := range o.tokens {
		if token.Asset == total.Asset() && token.Symbol != "" {
			symbol = token.Symbol
			break
		}
	}
	return compactRat(total.Rat(), 6) + " " + symbol
}

func (o *ProgressObserver) OperationFinished(
	operation execution.SequentialOperation,
	state execution.SequentialOperationState,
	result executionport.SequentialResult,
	cause error,
) {
	now := o.clock().UTC()
	o.mu.Lock()
	value := o.operations[operation.ID]
	delete(o.operations, operation.ID)
	o.mu.Unlock()
	duration := time.Duration(0)
	if !value.started.IsZero() {
		duration = now.Sub(value.started)
	}
	if state == execution.SequentialCompleted {
		// The terminal message must show the exact cost subtracted from gross
		// PnL. ExecutionCost also contains costs already reflected in the
		// realized output (for example an observed bridge spread), so exposing
		// it here would make the displayed cost and net PnL disagree.
		executionCost := o.assetAmount(result.ExternalCost)
		netPnL := o.signedAssetAmount(result.RealizedNetPnL)
		if executionCost == "" || netPnL == "" {
			executionCost, netPnL = o.realizedEconomics(
				value.initial, result.FinalAmount, result.Costs,
			)
		}
		o.notifier.Notify(notificationport.LiveExecutionEvent{
			Kind:      notificationport.LiveExecutionCompleted,
			Operation: string(operation.ID), State: string(state),
			Direction: value.direction, Input: o.amount(value.initial),
			Output:        o.amount(result.FinalAmount),
			ExecutionCost: executionCost, NetPnL: netPnL, Duration: duration,
			OccurredAt: now,
		})
		return
	}
	detail := ""
	if cause != nil {
		detail = cause.Error()
	}
	o.notifier.Notify(notificationport.LiveExecutionEvent{
		Kind:      notificationport.LiveExecutionFailed,
		Operation: string(operation.ID), State: string(state),
		Stage:   string(value.stage.Stage.Stage),
		Ordinal: value.stage.Stage.Ordinal, TotalStages: 4,
		SourceChain:      o.chainLabel(value.stage.Stage.SourceChain),
		DestinationChain: o.chainLabel(value.stage.Stage.DestinationChain),
		Detail:           detail, Duration: duration, OccurredAt: now,
	})
}

func (o *ProgressObserver) assetAmount(quantity market.AssetQuantity) string {
	if quantity.Asset() == "" {
		return ""
	}
	symbol := string(quantity.Asset())
	for _, token := range o.tokens {
		if token.Asset == quantity.Asset() && token.Symbol != "" {
			symbol = token.Symbol
			break
		}
	}
	return compactRat(quantity.Rat(), 6) + " " + symbol
}

func (o *ProgressObserver) signedAssetAmount(
	quantity market.AssetQuantity,
) string {
	if quantity.Asset() == "" {
		return ""
	}
	text := o.assetAmount(quantity)
	if quantity.Sign() > 0 {
		return "+" + text
	}
	return text
}

func (o *ProgressObserver) realizedEconomics(
	initial market.TokenAmount,
	final market.TokenAmount,
	costs []execution.CostComponent,
) (string, string) {
	initialToken, initialOK := o.tokens[initial.Token()]
	finalToken, finalOK := o.tokens[final.Token()]
	if !initialOK || !finalOK || initialToken.Asset == "" ||
		initialToken.Asset != finalToken.Asset {
		return "", ""
	}
	initialValue, err := initial.ToAssetQuantity(initialToken)
	if err != nil {
		return "", ""
	}
	finalValue, err := final.ToAssetQuantity(finalToken)
	if err != nil {
		return "", ""
	}
	external, _ := market.NewAssetQuantity(initialToken.Asset, new(big.Rat))
	for _, cost := range costs {
		if cost.QuoteValue.Asset() != initialToken.Asset {
			continue
		}
		if !cost.IncludedInOutput {
			external, _ = external.Add(cost.QuoteValue)
		}
	}
	net, err := finalValue.Sub(initialValue)
	if err != nil {
		return "", ""
	}
	net, err = net.Sub(external)
	if err != nil {
		return "", ""
	}
	symbol := initialToken.Symbol
	if symbol == "" {
		symbol = string(initialToken.Asset)
	}
	return compactRat(external.Rat(), 6) + " " + symbol,
		signedRat(net.Rat(), 6) + " " + symbol
}

func compactRat(value *big.Rat, decimals int) string {
	text := strings.TrimRight(
		strings.TrimRight(value.FloatString(decimals), "0"),
		".",
	)
	if text == "" || text == "-" {
		return "0"
	}
	return text
}

func signedRat(value *big.Rat, decimals int) string {
	text := compactRat(value, decimals)
	if value.Sign() > 0 {
		return "+" + text
	}
	return text
}

func (o *ProgressObserver) amount(amount market.TokenAmount) string {
	token, ok := o.tokens[amount.Token()]
	if !ok {
		return amount.String() + " " + string(amount.Token())
	}
	scale := new(big.Int).Exp(
		big.NewInt(10), big.NewInt(int64(token.Decimals)), nil,
	)
	value := new(big.Rat).SetFrac(amount.Units(), scale)
	text := strings.TrimRight(
		strings.TrimRight(value.FloatString(int(token.Decimals)), "0"), ".",
	)
	if text == "" {
		text = "0"
	}
	return text + " " + token.Symbol
}

func (o *ProgressObserver) chainLabel(chain market.ChainID) string {
	if chain == "" {
		return ""
	}
	if configured, ok := o.chains[chain]; ok {
		return configured.Label
	}
	return string(chain)
}

func (o *ProgressObserver) explorer(
	chain market.ChainID,
	identity string,
) string {
	if identity == "" {
		return ""
	}
	configured, ok := o.chains[chain]
	if !ok {
		return ""
	}
	switch {
	case configured.Kind == "solana":
		return "https://solscan.io/tx/" + identity
	case configured.Kind == "evm" &&
		configured.ChainID != nil &&
		configured.ChainID.Cmp(big.NewInt(137)) == 0:
		return "https://polygonscan.com/tx/" + identity
	default:
		return ""
	}
}

func (o *ProgressObserver) opportunityTrigger(
	opportunity arbitrage.Opportunity,
	plan execution.SequentialPlan,
) (string, string) {
	if !opportunity.HasTrigger {
		return "synthetic", ""
	}
	trigger := opportunity.Trigger
	text := string(trigger.Source)
	if trigger.Reference.Kind != "" || trigger.Reference.Value != "" {
		text += "/" + string(trigger.Reference.Kind) + ":" + trigger.Reference.Value
	}
	if trigger.Reference.Value == "" {
		return text, ""
	}
	for _, stage := range plan.Stages {
		if stage.Market != trigger.Market {
			continue
		}
		return text, o.explorer(stage.SourceChain, trigger.Reference.Value)
	}
	return text, ""
}

func readableExecutionProvider(source market.SourceID) string {
	value := strings.ToLower(string(source))
	switch {
	case strings.Contains(value, "jupiter"):
		return "Jupiter"
	case strings.Contains(value, "kyber"):
		return "KyberSwap"
	default:
		return string(source)
	}
}

var _ executionport.SequentialObserver = (*ProgressObserver)(nil)
var _ executionport.SequentialExitObserver = (*ProgressObserver)(nil)
