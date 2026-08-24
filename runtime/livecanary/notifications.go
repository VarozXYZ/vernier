package livecanary

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
	notificationport "github.com/VarozXYZ/vernier/ports/notification"
	persistenceport "github.com/VarozXYZ/vernier/ports/persistence"
	"github.com/VarozXYZ/vernier/runtime/configuration"
)

type LiveNotifier struct {
	sender        notificationport.LiveExecutionSender
	runtimeSender notificationport.LiveRuntimeSender
	logger        *slog.Logger
	queue         chan liveNotification
	outbox        persistenceport.LiveNotificationOutbox

	mu        sync.Mutex
	closed    bool
	terminal  map[string]struct{}
	inflight  map[string]struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

type liveNotification struct {
	id        string
	attempts  int
	execution *notificationport.LiveExecutionEvent
	runtime   *notificationport.LiveRuntimeEvent
}

type durableLiveNotification struct {
	Execution *notificationport.LiveExecutionEvent `json:"execution,omitempty"`
	Runtime   *notificationport.LiveRuntimeEvent   `json:"runtime,omitempty"`
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
		inflight: make(map[string]struct{}),
	}
	if runtimeSender, ok := sender.(notificationport.LiveRuntimeSender); ok {
		notifier.runtimeSender = runtimeSender
	}
	notifier.wg.Add(1)
	go notifier.run()
	return notifier, nil
}

func (n *LiveNotifier) AttachOutbox(ctx context.Context, outbox persistenceport.LiveNotificationOutbox) error {
	if outbox == nil {
		return fmt.Errorf("live notification outbox is required")
	}
	n.mu.Lock()
	n.outbox = outbox
	n.mu.Unlock()
	return n.enqueueDue(ctx)
}

// Notify only enqueues an in-memory event. Telegram transport latency can
// never delay transaction preparation, persistence, or broadcast.
func (n *LiveNotifier) Notify(event notificationport.LiveExecutionEvent) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return
	}
	// Preflight failures happen before any transaction can affect inventory.
	// They remain visible in operational logs and trigger a fresh evaluation,
	// but creating a Telegram execution message for them is only noise.
	if event.Kind == notificationport.LiveExecutionFailed &&
		event.State == "aborted_retrying" {
		return
	}
	if event.Kind == notificationport.LiveExecutionRecoveryStarted {
		// A previous failure notification is no longer terminal once durable
		// recovery owns the operation. Allow the eventual completed event to
		// replace it with the reconstructed final result.
		delete(n.terminal, event.Operation+"/execution")
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
	n.persistAndEnqueueLocked(liveNotification{execution: &event})
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
	n.persistAndEnqueueLocked(liveNotification{runtime: &event})
}

func (n *LiveNotifier) persistAndEnqueueLocked(item liveNotification) {
	if n.outbox != nil {
		payload, err := json.Marshal(durableLiveNotification{Execution: item.execution, Runtime: item.runtime})
		if err == nil {
			sum := sha256.Sum256(payload)
			item.id = "live-notification-" + hex.EncodeToString(sum[:16])
			now := time.Now().UTC()
			var inserted bool
			persistCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			inserted, err = n.outbox.PutLiveNotification(persistCtx, persistenceport.LiveNotificationRecord{
				ID: item.id, Payload: payload, State: "pending", CreatedAt: now, UpdatedAt: now})
			cancel()
			if err == nil && !inserted {
				return
			}
		}
		if err != nil && n.logger != nil {
			n.logger.Error("Live notification outbox persistence failed", "error", err)
		}
	}
	if item.id != "" {
		if _, exists := n.inflight[item.id]; exists {
			return
		}
		n.inflight[item.id] = struct{}{}
	}
	select {
	case n.queue <- item:
	default:
		delete(n.inflight, item.id)
		if n.logger != nil {
			n.logger.Error("Live Telegram queue is full; durable notification awaits retry")
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
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		var event liveNotification
		var ok bool
		select {
		case event, ok = <-n.queue:
			if !ok {
				return
			}
		case <-ticker.C:
			_ = n.enqueueDue(context.Background())
			continue
		}
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
		n.finishDelivery(event, err)
		if err != nil && n.logger != nil {
			n.logger.Error(
				"Live Telegram notification failed",
				"kind", kind, "error", err,
			)
		}
	}
}

func (n *LiveNotifier) finishDelivery(event liveNotification, deliveryErr error) {
	n.mu.Lock()
	delete(n.inflight, event.id)
	outbox := n.outbox
	n.mu.Unlock()
	if outbox == nil || event.id == "" {
		return
	}
	now := time.Now().UTC()
	state, next, detail := "delivered", time.Time{}, ""
	attempts := event.attempts + 1
	if deliveryErr != nil {
		state, next, detail = "retrying", now.Add(notificationRetryDelay(attempts)), deliveryErr.Error()
		if len(detail) > 500 {
			detail = detail[:500]
		}
	}
	if err := outbox.MarkLiveNotification(context.Background(), event.id, state, attempts, next, detail, now); err != nil && n.logger != nil {
		n.logger.Error("Live notification outbox update failed", "error", err)
	}
}

func (n *LiveNotifier) enqueueDue(ctx context.Context) error {
	n.mu.Lock()
	outbox := n.outbox
	closed := n.closed
	n.mu.Unlock()
	if outbox == nil || closed {
		return nil
	}
	records, err := outbox.LoadDueLiveNotifications(ctx, time.Now().UTC(), 32)
	if err != nil {
		return err
	}
	for _, record := range records {
		var payload durableLiveNotification
		if err := json.Unmarshal(record.Payload, &payload); err != nil {
			_ = outbox.MarkLiveNotification(ctx, record.ID, "rejected", record.Attempts+1, time.Time{}, "invalid normalized payload", time.Now().UTC())
			continue
		}
		n.mu.Lock()
		if _, exists := n.inflight[record.ID]; !exists && !n.closed {
			n.inflight[record.ID] = struct{}{}
			select {
			case n.queue <- liveNotification{id: record.ID, attempts: record.Attempts, execution: payload.Execution, runtime: payload.Runtime}:
			default:
				delete(n.inflight, record.ID)
			}
		}
		n.mu.Unlock()
	}
	return nil
}

func notificationRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := time.Second << min(attempt-1, 6)
	if delay > time.Minute {
		return time.Minute
	}
	return delay
}

type progressOperation struct {
	direction string
	initial   market.TokenAmount
	started   time.Time
	stage     execution.SequentialStageRequest
	stageAt   map[int]time.Time
	pending   []notificationport.LiveExecutionEvent
	visible   bool
	finished  bool
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
	direction := o.planDirection(plan)
	value := progressOperation{
		direction: direction, initial: plan.InitialInput,
		started: operation.StartedAt, stageAt: make(map[int]time.Time),
	}
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
	// Keep the operation silent until the initial buy has a durable economic
	// settlement. A rejected preflight or buy is not actionable and should not
	// create a Telegram message that is immediately changed to ABORTED.
	value.pending = append(value.pending, event)
	o.mu.Lock()
	o.operations[operation.ID] = value
	o.mu.Unlock()
}

func (o *ProgressObserver) planDirection(
	plan execution.SequentialPlan,
) string {
	var buyChain, sellChain market.ChainID
	for _, stage := range plan.Stages {
		switch stage.Stage {
		case execution.StageBuy:
			if buyChain == "" {
				buyChain = stage.SourceChain
			}
		case execution.StageSell:
			if sellChain == "" {
				sellChain = stage.SourceChain
			}
		}
	}
	if buyChain == "" || sellChain == "" {
		return ""
	}
	return o.chainLabel(buyChain) + " -> " + o.chainLabel(sellChain)
}

func (o *ProgressObserver) StageStarted(
	request execution.SequentialStageRequest,
) {
	now := o.clock().UTC()
	event := notificationport.LiveExecutionEvent{
		Kind:      notificationport.LiveExecutionStageStarted,
		Operation: string(request.Operation),
		Stage:     string(request.Stage.Stage), Ordinal: request.Stage.Ordinal,
		TotalStages: 4, SourceChain: o.chainLabel(request.Stage.SourceChain),
		DestinationChain: o.chainLabel(request.Stage.DestinationChain),
		Input:            o.amount(request.Input), OccurredAt: now,
	}
	o.mu.Lock()
	value := o.operations[request.Operation]
	value.stage = request
	if value.stageAt == nil {
		value.stageAt = make(map[int]time.Time)
	}
	value.stageAt[request.Stage.Ordinal] = now
	visible := value.visible
	if !visible {
		value.pending = append(value.pending, event)
	}
	o.operations[request.Operation] = value
	o.mu.Unlock()
	if visible {
		o.notifier.Notify(event)
	}
}

func (o *ProgressObserver) StageSettled(
	settlement execution.SequentialStageSettlement,
) {
	now := o.clock().UTC()
	o.mu.Lock()
	value := o.operations[settlement.Request.Operation]
	stageAt := value.stageAt[settlement.Request.Stage.Ordinal]
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
	o.mu.Lock()
	value = o.operations[settlement.Request.Operation]
	delete(value.stageAt, settlement.Request.Stage.Ordinal)
	if !value.visible && settlement.Request.Stage.Stage == execution.StageBuy {
		value.visible = true
		pending := append(
			[]notificationport.LiveExecutionEvent(nil),
			value.pending...,
		)
		value.pending = nil
		o.operations[settlement.Request.Operation] = value
		o.mu.Unlock()
		for _, buffered := range pending {
			o.notifier.Notify(buffered)
		}
		o.notifier.Notify(event)
		return
	}
	visible := value.visible
	if !visible {
		value.pending = append(value.pending, event)
	}
	if value.finished {
		delete(o.operations, settlement.Request.Operation)
	} else {
		o.operations[settlement.Request.Operation] = value
	}
	o.mu.Unlock()
	if visible {
		o.notifier.Notify(event)
	}
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
	_, quoteReturnPending := value.stageAt[4]
	if quoteReturnPending &&
		value.stage.Stage.Stage == execution.StageBridgeQuoteReturn {
		// Quote restoration may deliberately continue after the economic
		// operation completes. Retain just enough progress state for its
		// eventual settlement to update the existing notification.
		value.finished = true
		o.operations[operation.ID] = value
	} else {
		delete(o.operations, operation.ID)
	}
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
		event := notificationport.LiveExecutionEvent{
			Kind:      notificationport.LiveExecutionCompleted,
			Operation: string(operation.ID), State: string(state),
			Direction: value.direction, Input: o.amount(value.initial),
			Output:        o.amount(result.FinalAmount),
			ExecutionCost: executionCost, NetPnL: netPnL, Duration: duration,
			OccurredAt: now,
		}
		if result.QuoteDelta.Asset() != "" {
			event.QuoteDelta = o.signedAssetAmount(result.QuoteDelta)
		}
		if result.BaseDelta.Asset() != "" {
			event.BaseDelta = o.signedAssetAmount(result.BaseDelta)
		}
		if result.MarkedBase.Asset() != "" {
			event.BaseValue = o.signedAssetAmount(result.MarkedBase)
		}
		for _, pending := range value.pending {
			o.notifier.Notify(pending)
		}
		o.notifier.Notify(event)
		return
	}
	detail := ""
	if cause != nil {
		detail = cause.Error()
	}
	event := notificationport.LiveExecutionEvent{
		Kind:      notificationport.LiveExecutionFailed,
		Operation: string(operation.ID), State: string(state),
		Stage:   string(value.stage.Stage.Stage),
		Ordinal: value.stage.Stage.Ordinal, TotalStages: 4,
		SourceChain:      o.chainLabel(value.stage.Stage.SourceChain),
		DestinationChain: o.chainLabel(value.stage.Stage.DestinationChain),
		Detail:           detail, Duration: duration, OccurredAt: now,
	}
	if !value.visible &&
		(state == execution.SequentialAborted ||
			executionport.IsDefinitiveFailure(cause)) {
		return
	}
	for _, pending := range value.pending {
		o.notifier.Notify(pending)
	}
	o.notifier.Notify(event)
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
	escapedIdentity := url.PathEscape(strings.TrimSpace(identity))
	switch {
	case configured.Kind == "solana":
		return "https://solscan.io/tx/" + escapedIdentity
	case configured.Kind == "evm" &&
		configured.ChainID != nil &&
		configured.ChainID.IsInt64():
		var prefix string
		switch configured.ChainID.Int64() {
		case 56:
			prefix = "https://bscscan.com/tx/"
		case 137:
			prefix = "https://polygonscan.com/tx/"
		case 8453:
			prefix = "https://basescan.org/tx/"
		}
		if prefix == "" {
			return ""
		}
		return prefix + escapedIdentity
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
