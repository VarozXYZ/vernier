package livecanary

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

type parallelSimulationEconomics struct {
	BuyOutput   market.TokenAmount
	SellOutput  market.TokenAmount
	QuoteDelta  market.AssetQuantity
	BaseDelta   market.AssetQuantity
	MarkPrice   *big.Rat
	Gross       market.AssetQuantity
	Cost        market.AssetQuantity
	Net         market.AssetQuantity
	ValidatedAt time.Time
}

func (d *SwapDriver) preflightParallel(
	ctx context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
) error {
	if operation == "" || len(plan.Stages) != 4 ||
		plan.Stages[0].Stage != execution.StageBuy ||
		plan.Stages[1].Stage != execution.StageSell {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf("parallel swap preflight plan is incomplete"),
		)
	}
	started := time.Now()
	sellInput, err := d.parallelSellInput(plan)
	if err != nil {
		return executionport.NewStageError(executionport.DispositionRejected, err)
	}
	requests := []execution.SequentialStageRequest{
		{Operation: operation, Plan: plan.ID, Stage: plan.Stages[0], Input: plan.InitialInput},
		{Operation: operation, Plan: plan.ID, Stage: plan.Stages[1], Input: sellInput},
	}
	localTriggerIndex := d.localTriggeredSwapIndex(plan, requests)
	candidate, candidateErr := selectedCandidate(plan.Opportunity)
	if candidateErr != nil {
		return executionport.NewStageError(executionport.DispositionRejected, candidateErr)
	}
	discoveries := []market.Quote{candidate.BuyQuote, candidate.SellQuote}
	if d.StagedFor(plan) {
		return d.preflightTriggeredLocal(ctx, operation, plan, requests, discoveries)
	}
	type result struct {
		bundle preparedSwap
		err    error
	}
	results := make([]result, 2)
	var group sync.WaitGroup
	for index := range requests {
		index := index
		group.Add(1)
		go func() {
			defer group.Done()
			binding, bindErr := d.binding(requests[index])
			if bindErr != nil {
				results[index].err = bindErr
				return
			}
			if localTriggerIndex >= 0 && index != localTriggerIndex {
				if binding.RecoveryValidator == nil {
					results[index].err = fmt.Errorf("fresh remote validator is unavailable")
					return
				}
				binding.Validator = binding.RecoveryValidator
			}
			var slippage *executionport.SlippageConstraint
			if index == 0 {
				slippage, bindErr = d.dynamicBuySlippage(plan)
				if bindErr != nil {
					results[index].err = bindErr
					return
				}
			}
			var discovery []market.Quote
			// Hybrid Live bindings opt in to carrying the exact Research quote
			// into validation. Legacy bindings continue to build from the fixed
			// operation input because their opportunity may come from a sizing
			// point with a different amount.
			if binding.SnapshotForQuote != nil || binding.TrustValidatedQuote {
				discovery = append(discovery, discoveries[index])
			}
			bundle, prepareErr := d.prepareSwap(ctx, requests[index], binding, slippage, discovery...)
			if prepareErr != nil {
				results[index].err = prepareErr
				return
			}
			var simulation chainport.EconomicSimulationResult
			// A remote-triggered hybrid opportunity must validate both legs against
			// current chain state before any broadcast.  The local-quote shortcut
			// is reserved for the local-triggered fast path, where the local leg is
			// sent first and the remote leg is prepared while it confirms.
			localTriggered := index == localTriggerIndex && d.StagedFor(plan)
			if localTriggered {
				simulation = chainport.EconomicSimulationResult{
					Input:    bundle.artifact.ValidatedQuote.AmountIn,
					Output:   bundle.artifact.ValidatedQuote.AmountOut,
					Evidence: "local_quote_gate_no_simulation",
				}
			} else {
				var simulationErr error
				simulation, simulationErr = d.simulateEconomic(ctx, binding, bundle.prepared)
				if simulationErr != nil {
					results[index].err = simulationErr
					return
				}
			}
			bundle.simulation = simulation
			results[index].bundle = bundle
		}()
	}
	group.Wait()
	for index, result := range results {
		if result.err != nil {
			return executionport.NewStageError(
				executionport.DispositionRejected,
				fmt.Errorf("%s parallel preflight: %w", requests[index].Stage.Stage, result.err),
			)
		}
	}
	economics, err := d.parallelEconomics(plan, results[0].bundle.simulation, results[1].bundle.simulation)
	if err != nil {
		return executionport.NewStageError(executionport.DispositionRejected, err)
	}
	forced := isForcedCanaryOpportunity(plan.Opportunity)
	if d.MaximumCost != nil && economics.Cost.Rat().Cmp(d.MaximumCost) > 0 {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf("complete-flow cost exceeds maximum: cost=%s maximum=%s", economics.Cost, d.MaximumCost),
		)
	}
	minimumNet := d.minimumNetFor(plan.Opportunity.Direction)
	if !forced && economics.Net.Rat().Cmp(minimumNet) < 0 {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf("joint simulated PnL is below threshold: gross=%s cost=%s net=%s minimum=%s",
				economics.Gross, economics.Cost, economics.Net, minimumNet),
		)
	}
	d.preflightMu.Lock()
	if d.preflightBuys == nil {
		d.preflightBuys = make(map[execution.OperationID]preparedSwap)
	}
	if d.exitSells == nil {
		d.exitSells = make(map[execution.OperationID]preparedSwap)
	}
	d.preflightBuys[operation] = results[0].bundle
	d.exitSells[operation] = results[1].bundle
	d.preflightMu.Unlock()
	d.write("live_preflight operation=%s mode=prefunded_parallel status=accepted buy_input_units=%s buy_simulated_output_units=%s sell_input_units=%s sell_simulated_output_units=%s quote_delta=%s base_delta=%s gross=%s cost=%s net=%s latency=%s\n",
		operation, plan.InitialInput, economics.BuyOutput, sellInput,
		economics.SellOutput, economics.QuoteDelta, economics.BaseDelta,
		economics.Gross, economics.Cost, economics.Net,
		time.Since(started).Round(10*time.Microsecond),
	)
	return nil
}

func (d *SwapDriver) StagedFor(plan execution.SequentialPlan) bool {
	if !plan.Opportunity.HasTrigger || len(plan.Stages) < 2 {
		return false
	}
	// A local BUY can be risk-reduced by selling the acquired base remotely.
	// A local SELL would leave an unbounded remote BUY obligation; empirical
	// route/build drift makes that direction ineligible for staged emission.
	if plan.EffectivePolicy() == execution.PolicyPrefundedTriggerFirst {
		for _, stage := range plan.Stages[:2] {
			binding, ok := d.Bindings[stage.Market]
			if ok && binding.TrustValidatedQuote &&
				liveTriggerMatchesMarket(plan.Opportunity.Trigger.Market, stage.Market) {
				return true
			}
		}
		return false
	}
	stage := plan.Stages[0]
	binding, ok := d.Bindings[stage.Market]
	return ok && binding.TrustValidatedQuote &&
		liveTriggerMatchesMarket(plan.Opportunity.Trigger.Market, stage.Market)
}

func (d *SwapDriver) localTriggeredSwapIndex(
	plan execution.SequentialPlan,
	requests []execution.SequentialStageRequest,
) int {
	if !plan.Opportunity.HasTrigger {
		return -1
	}
	for index := range requests {
		binding, ok := d.Bindings[requests[index].Stage.Market]
		if ok && binding.TrustValidatedQuote &&
			liveTriggerMatchesMarket(plan.Opportunity.Trigger.Market, requests[index].Stage.Market) {
			return index
		}
	}
	return -1
}

func (d *SwapDriver) preflightTriggeredLocal(
	ctx context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
	requests []execution.SequentialStageRequest,
	discoveries []market.Quote,
) error {
	localIndex := -1
	for index := range requests {
		binding, ok := d.Bindings[requests[index].Stage.Market]
		if ok && binding.TrustValidatedQuote && liveTriggerMatchesMarket(plan.Opportunity.Trigger.Market, requests[index].Stage.Market) {
			localIndex = index
			break
		}
	}
	if localIndex < 0 {
		return executionport.NewStageError(executionport.DispositionRejected, fmt.Errorf("local trigger binding is unavailable"))
	}
	remoteIndex := 1 - localIndex
	remoteBinding, err := d.binding(requests[remoteIndex])
	if err != nil {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf("remote static preflight: %w", err),
		)
	}
	if remoteBinding.RecoveryValidator == nil {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf("remote static preflight: fresh remote validator is unavailable"),
		)
	}
	remoteDiscovery := discoveries[remoteIndex]
	if remoteDiscovery.AmountIn.Token() != requests[remoteIndex].Input.Token() ||
		remoteDiscovery.AmountIn.Units().Cmp(requests[remoteIndex].Input.Units()) != 0 {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf("remote static preflight: discovery changed the fixed input"),
		)
	}
	// Fail every deterministic remote-leg invariant before the irreversible
	// local broadcast. Preparation remains asynchronous, but an impossible
	// economic slippage floor must never be discovered only after settlement.
	if remoteIndex == 0 {
		if _, err := d.dynamicBuySlippage(plan); err != nil {
			return executionport.NewStageError(
				executionport.DispositionRejected,
				fmt.Errorf("remote buy preflight: %w", err),
			)
		}
	}
	binding, err := d.binding(requests[localIndex])
	if err != nil {
		return executionport.NewStageError(executionport.DispositionRejected, err)
	}
	var slippage *executionport.SlippageConstraint
	if plan.EffectivePolicy() == execution.PolicyPrefundedTriggerFirst {
		slippage, err = d.dynamicTriggerFirstSlippage(plan, discoveries[localIndex])
	} else if localIndex == 0 {
		slippage, err = d.dynamicBuySlippage(plan)
	}
	if err != nil {
		return executionport.NewStageError(executionport.DispositionRejected, err)
	}
	bundle, err := d.prepareSwap(ctx, requests[localIndex], binding, slippage, discoveries[localIndex])
	if err != nil {
		return executionport.NewStageError(executionport.DispositionRejected, fmt.Errorf("local triggered preflight: %w", err))
	}
	bundle.simulation = chainport.EconomicSimulationResult{
		Input: bundle.artifact.ValidatedQuote.AmountIn, Output: bundle.artifact.ValidatedQuote.AmountOut,
		Evidence: "local_quote_gate_no_simulation",
	}
	d.preflightMu.Lock()
	if localIndex == 0 {
		if d.preflightBuys == nil {
			d.preflightBuys = make(map[execution.OperationID]preparedSwap)
		}
		d.preflightBuys[operation] = bundle
	} else {
		if d.exitSells == nil {
			d.exitSells = make(map[execution.OperationID]preparedSwap)
		}
		d.exitSells[operation] = bundle
	}
	d.preflightMu.Unlock()
	d.write("live_preflight operation=%s mode=local_trigger_staged status=accepted local_market=%s local_stage=%s\n",
		operation, requests[localIndex].Stage.Market, requests[localIndex].Stage.Stage)
	return nil
}

func (d *SwapDriver) ExecuteParallelSwaps(
	ctx context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
	journal executionport.SequentialJournal,
) ([]execution.SequentialStageSettlement, error) {
	if !plan.IsPrefundedDualInventory() || len(plan.Stages) != 4 {
		return nil, fmt.Errorf("parallel swap execution plan is invalid")
	}
	sellInput, err := d.parallelSellInput(plan)
	if err != nil {
		return nil, err
	}
	requests := []execution.SequentialStageRequest{
		{Operation: operation, Plan: plan.ID, Stage: plan.Stages[0], Input: plan.InitialInput},
		{Operation: operation, Plan: plan.ID, Stage: plan.Stages[1], Input: sellInput},
	}
	d.preflightMu.Lock()
	bundles := []preparedSwap{d.preflightBuys[operation], d.exitSells[operation]}
	delete(d.preflightBuys, operation)
	delete(d.exitSells, operation)
	d.preflightMu.Unlock()
	preparedRecords := make([]executionport.PreparedTransaction, 2)
	phases := make([]string, 2)
	clock := d.Clock
	if clock == nil {
		clock = time.Now
	}
	for index := range bundles {
		if bundles[index].prepared.Identity.Hash == "" || bundles[index].simulation.Output.IsZero() {
			return nil, fmt.Errorf("parallel preflight artifacts are unavailable")
		}
		if d.artifactExpired(bundles[index].artifact, clock()) {
			return nil, executionport.NewStageError(
				executionport.DispositionRejected,
				fmt.Errorf("simulated %s artifact expired before atomic persistence", requests[index].Stage.Stage),
			)
		}
		phases[index] = d.nextSwapTransactionPhase(operation, requests[index].Stage.Ordinal)
		preparedRecords[index] = executionport.PreparedTransaction{
			Operation: operation, Ordinal: requests[index].Stage.Ordinal,
			Phase: phases[index], Identity: bundles[index].prepared.Identity,
			PreparedAt:               bundles[index].prepared.PreparedAt,
			SimulatedInput:           bundles[index].simulation.Input,
			SimulatedOutput:          bundles[index].simulation.Output,
			SimulationEvidence:       bundles[index].simulation.Evidence,
			SimulationContextVersion: bundles[index].simulation.ContextVersion,
			SimulationUnitsConsumed:  bundles[index].simulation.UnitsConsumed,
		}
	}
	batch, ok := journal.(executionport.SequentialPreparedBatchJournal)
	if !ok {
		return nil, fmt.Errorf("parallel execution requires atomic prepared-transaction persistence")
	}
	if err := batch.RecordPreparedTransactions(ctx, preparedRecords); err != nil {
		return nil, err
	}

	type legResult struct {
		settlement execution.SequentialStageSettlement
		err        error
	}
	results := make([]legResult, 2)
	var group sync.WaitGroup
	for index := range bundles {
		index := index
		group.Add(1)
		go func() {
			defer group.Done()
			results[index].settlement, results[index].err = d.broadcastPreparedSwap(
				ctx, requests[index], phases[index], bundles[index], journal,
			)
		}()
	}
	group.Wait()
	settlements := make([]execution.SequentialStageSettlement, 0, 2)
	failures := make([]error, 0, 2)
	for index := range results {
		if results[index].err != nil {
			failures = append(failures, fmt.Errorf("%s parallel leg: %w", requests[index].Stage.Stage, results[index].err))
			continue
		}
		settlements = append(settlements, results[index].settlement)
	}
	if len(failures) > 0 {
		return settlements, errors.Join(failures...)
	}
	return settlements, nil
}

func (d *SwapDriver) ExecuteTriggeredSwaps(
	ctx context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
	journal executionport.SequentialJournal,
) ([]execution.SequentialStageSettlement, error) {
	if !d.StagedFor(plan) || len(plan.Stages) != 4 {
		return nil, fmt.Errorf("local-triggered swap execution plan is invalid")
	}
	sellInput, err := d.parallelSellInput(plan)
	if err != nil {
		return nil, err
	}
	requests := []execution.SequentialStageRequest{
		{Operation: operation, Plan: plan.ID, Stage: plan.Stages[0], Input: plan.InitialInput},
		{Operation: operation, Plan: plan.ID, Stage: plan.Stages[1], Input: sellInput},
	}
	localIndex := -1
	for index := range requests {
		binding, ok := d.Bindings[requests[index].Stage.Market]
		if ok && binding.TrustValidatedQuote && liveTriggerMatchesMarket(plan.Opportunity.Trigger.Market, requests[index].Stage.Market) {
			localIndex = index
			break
		}
	}
	if localIndex < 0 {
		return nil, fmt.Errorf("local-triggered swap binding is unavailable")
	}
	remoteIndex := 1 - localIndex
	d.preflightMu.Lock()
	var local preparedSwap
	if localIndex == 0 {
		local = d.preflightBuys[operation]
		delete(d.preflightBuys, operation)
	} else {
		local = d.exitSells[operation]
		delete(d.exitSells, operation)
	}
	d.preflightMu.Unlock()
	if local.prepared.Identity.Hash == "" {
		return nil, fmt.Errorf("local-triggered preflight artifact is unavailable")
	}

	localPhase := d.nextSwapTransactionPhase(operation, requests[localIndex].Stage.Ordinal)
	if plan.EffectivePolicy() == execution.PolicyPrefundedTriggerFirst {
		decisionJournal, ok := journal.(executionport.SequentialTriggerFirstDecisionJournal)
		if !ok {
			return nil, fmt.Errorf("trigger-first execution requires durable decision persistence")
		}
		decision, decisionErr := triggerFirstDecision(
			operation,
			requests[localIndex].Stage.Ordinal,
			local,
			plan,
			d.now(),
		)
		if decisionErr != nil {
			return nil, decisionErr
		}
		if err := decisionJournal.RecordTriggerFirstDecision(ctx, decision); err != nil {
			return nil, err
		}
	}
	if err := d.recordPreparedSwap(ctx, journal, requests[localIndex], localPhase, local); err != nil {
		return nil, err
	}
	type remotePreparation struct {
		bundle preparedSwap
		err    error
	}
	prepareRemote := func(prepareCtx context.Context) remotePreparation {
		binding, bindErr := d.binding(requests[remoteIndex])
		if bindErr != nil {
			return remotePreparation{err: bindErr}
		}
		if binding.RecoveryValidator == nil {
			return remotePreparation{err: fmt.Errorf("fresh second-leg validator is unavailable")}
		}
		binding.Validator = binding.RecoveryValidator
		if plan.EffectivePolicy() == execution.PolicyPrefundedTriggerFirst && binding.LatestSnapshot != nil {
			binding.SnapshotForQuote = func(market.Quote) (market.MarketSnapshot, bool) {
				return binding.LatestSnapshot()
			}
		}
		candidate, candidateErr := selectedCandidate(plan.Opportunity)
		if candidateErr != nil {
			return remotePreparation{err: candidateErr}
		}
		discovery := []market.Quote{candidate.BuyQuote, candidate.SellQuote}[remoteIndex]
		var slippage *executionport.SlippageConstraint
		if remoteIndex == 0 && plan.EffectivePolicy() != execution.PolicyPrefundedTriggerFirst {
			slippage, bindErr = d.dynamicBuySlippage(plan)
			if bindErr != nil {
				return remotePreparation{err: bindErr}
			}
		}
		bundle, prepareErr := d.prepareSwap(prepareCtx, requests[remoteIndex], binding, slippage, discovery)
		if prepareErr == nil && plan.EffectivePolicy() == execution.PolicyPrefundedTriggerFirst && binding.TrustValidatedQuote {
			bundle.simulation = chainport.EconomicSimulationResult{Input: bundle.artifact.ValidatedQuote.AmountIn,
				Output: bundle.artifact.ValidatedQuote.AmountOut, Evidence: "fresh_local_quote_after_first_confirmation"}
		} else if prepareErr == nil {
			bundle.simulation, prepareErr = d.simulateEconomic(prepareCtx, binding, bundle.prepared)
		}
		return remotePreparation{bundle: bundle, err: prepareErr}
	}
	var remoteReady chan remotePreparation
	var cancelRemote context.CancelFunc = func() {}
	if plan.EffectivePolicy() != execution.PolicyPrefundedTriggerFirst {
		remoteCtx, cancel := context.WithCancel(ctx)
		cancelRemote = cancel
		remoteReady = make(chan remotePreparation, 1)
		go func() { remoteReady <- prepareRemote(remoteCtx) }()
	}
	defer cancelRemote()

	localSettlement, localErr := d.broadcastPreparedSwap(ctx, requests[localIndex], localPhase, local, journal)
	if localErr != nil {
		cancelRemote()
		return nil, fmt.Errorf("local-triggered first leg: %w", localErr)
	}
	remote := remotePreparation{}
	if plan.EffectivePolicy() == execution.PolicyPrefundedTriggerFirst {
		remote = prepareRemote(ctx)
	} else {
		remote = <-remoteReady
	}
	if remote.err != nil {
		return []execution.SequentialStageSettlement{localSettlement}, fmt.Errorf("prepare remote leg after local confirmation: %w", remote.err)
	}
	localSimulation := chainport.EconomicSimulationResult{Input: localSettlement.ActualInput, Output: localSettlement.ActualOutput, Evidence: "confirmed_local_settlement"}
	simulations := []chainport.EconomicSimulationResult{remote.bundle.simulation, remote.bundle.simulation}
	simulations[localIndex] = localSimulation
	// Once the trigger-first leg has settled, the second leg is exposure
	// reduction rather than a new economic admission. Its fresh local quote and
	// fixed on-chain minOut remain mandatory, but a missing valuation/cost cache
	// or a negative recalculated PnL must never strand the acquired inventory.
	if plan.EffectivePolicy() != execution.PolicyPrefundedTriggerFirst {
		economics, economicsErr := d.parallelEconomics(plan, simulations[0], simulations[1])
		if economicsErr != nil {
			return []execution.SequentialStageSettlement{localSettlement}, economicsErr
		}
		forced := isForcedCanaryOpportunity(plan.Opportunity)
		if d.MaximumCost != nil && economics.Cost.Rat().Cmp(d.MaximumCost) > 0 {
			return []execution.SequentialStageSettlement{localSettlement}, fmt.Errorf("complete-flow cost exceeds maximum after local confirmation")
		}
		minimumNet := d.minimumNetFor(plan.Opportunity.Direction)
		if !forced && economics.Net.Rat().Cmp(minimumNet) < 0 {
			return []execution.SequentialStageSettlement{localSettlement}, fmt.Errorf(
				"remote simulation no longer qualifies after local confirmation: net=%s minimum=%s", economics.Net, minimumNet)
		}
	}
	remotePhase := d.nextSwapTransactionPhase(operation, requests[remoteIndex].Stage.Ordinal)
	if err := d.recordPreparedSwap(ctx, journal, requests[remoteIndex], remotePhase, remote.bundle); err != nil {
		return []execution.SequentialStageSettlement{localSettlement}, err
	}
	remoteSettlement, err := d.broadcastPreparedSwap(ctx, requests[remoteIndex], remotePhase, remote.bundle, journal)
	if err != nil {
		return []execution.SequentialStageSettlement{localSettlement}, fmt.Errorf("remote leg after local confirmation: %w", err)
	}
	settlements := make([]execution.SequentialStageSettlement, 2)
	settlements[localIndex] = localSettlement
	settlements[remoteIndex] = remoteSettlement
	return settlements, nil
}

func triggerFirstDecision(operation execution.OperationID, ordinal int, bundle preparedSwap,
	plan execution.SequentialPlan, decidedAt time.Time) (executionport.TriggerFirstDecision, error) {
	metadata := bundle.artifact.Metadata
	decision := executionport.TriggerFirstDecision{Operation: operation, Ordinal: ordinal,
		Kind:        executionport.TriggerFirstDecisionEconomic75_25,
		ExpectedNet: metadata["decision_expected_net"], ReservedHeadroom: metadata["decision_reserved_headroom"],
		ConsumableBudget:   metadata["decision_consumable_budget"],
		MinimumOutputToken: bundle.artifact.ValidatedQuote.AmountOut.Token(),
		MinimumOutputUnits: metadata["minimum_output_units"], DecidedAt: decidedAt.UTC()}
	if isForcedCanaryOpportunity(plan.Opportunity) {
		decision.Kind = executionport.TriggerFirstDecisionForcedFixed
		decision.ReservedHeadroom = ""
		decision.ConsumableBudget = ""
		if candidate, err := selectedCandidate(plan.Opportunity); err == nil {
			decision.ExpectedNet = candidate.NetPnL.String()
		}
	}
	if _, err := fmt.Sscan(metadata["slippage_bps"], &decision.EquivalentBPS); err != nil {
		return executionport.TriggerFirstDecision{}, fmt.Errorf("trigger-first slippage evidence is invalid")
	}
	if bundle.artifact.Allocation == nil {
		return executionport.TriggerFirstDecision{}, fmt.Errorf("trigger-first allocation evidence is unavailable")
	}
	hasher := sha256.New()
	allocation := bundle.artifact.Allocation
	fmt.Fprintf(hasher, "%s|%s|%s|%s", allocation.Input.Token(), allocation.Input.Units(),
		allocation.ExpectedOutput.Token(), allocation.ExpectedOutput.Units())
	for _, group := range allocation.Groups {
		fmt.Fprintf(hasher, "|%s|%s|%s|%s", group.ID, group.Parent, group.InputToken, group.OutputToken)
		for _, branch := range group.Branches {
			fmt.Fprintf(hasher, "|%s|%s|%s", branch.Market, branch.PlannedInput, branch.ExpectedOutput)
		}
	}
	decision.AllocationHash = fmt.Sprintf("%x", hasher.Sum(nil))
	if err := decision.Validate(); err != nil {
		return executionport.TriggerFirstDecision{}, err
	}
	return decision, nil
}

func liveTriggerMatchesMarket(trigger, configured market.MarketID) bool {
	return trigger == configured || strings.HasPrefix(string(trigger), string(configured)+"/")
}

func (d *SwapDriver) recordPreparedSwap(
	ctx context.Context,
	journal executionport.SequentialJournal,
	request execution.SequentialStageRequest,
	phase string,
	bundle preparedSwap,
) error {
	if bundle.prepared.Identity.Hash == "" || bundle.simulation.Output.IsZero() {
		return fmt.Errorf("prepared swap evidence is incomplete")
	}
	if d.artifactExpired(bundle.artifact, d.now()) {
		return executionport.NewStageError(executionport.DispositionRejected, fmt.Errorf("prepared swap artifact expired before persistence"))
	}
	return journal.RecordPreparedTransaction(ctx, executionport.PreparedTransaction{
		Operation: request.Operation, Ordinal: request.Stage.Ordinal, Phase: phase,
		Identity: bundle.prepared.Identity, PreparedAt: bundle.prepared.PreparedAt,
		SimulatedInput: bundle.simulation.Input, SimulatedOutput: bundle.simulation.Output,
		SimulationEvidence: bundle.simulation.Evidence, SimulationContextVersion: bundle.simulation.ContextVersion,
		SimulationUnitsConsumed: bundle.simulation.UnitsConsumed,
	})
}

// RecoverParallelBuy spends exactly the original quote input. It compares
// fresh ExactIn artifacts on both chains and deliberately accepts a residual
// base delta; preserving quote inventory is the recovery objective.
func (d *SwapDriver) RecoverParallelBuy(
	ctx context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
	transactions []executionport.SequentialTransactionRecord,
	journal executionport.SequentialJournal,
) (execution.SequentialStageSettlement, error) {
	if !plan.IsPrefundedDualInventory() || len(plan.Stages) != 4 {
		return execution.SequentialStageSettlement{}, fmt.Errorf("parallel buy recovery plan is invalid")
	}
	d.primeSwapAttempts(operation, 1, len(transactions))
	original := execution.SequentialStageRequest{
		Operation: operation, Plan: plan.ID, Stage: plan.Stages[0], Input: plan.InitialInput,
	}
	alternateStage := plan.Stages[0]
	alternateStage.SourceChain = plan.Stages[1].SourceChain
	alternateStage.Market = plan.Stages[1].Market
	alternateStage.InputToken = plan.Stages[1].OutputToken
	alternateStage.OutputToken = plan.Stages[1].InputToken
	alternateInput, err := d.convertAmount(plan.InitialInput, alternateStage.InputToken)
	if err != nil {
		return execution.SequentialStageSettlement{}, err
	}
	alternate := execution.SequentialStageRequest{
		Operation: operation, Plan: plan.ID, Stage: alternateStage, Input: alternateInput,
	}
	requests := []execution.SequentialStageRequest{original, alternate}
	type candidate struct {
		bundle  preparedSwap
		binding SwapBinding
		err     error
	}
	candidates := make([]candidate, 2)
	var group sync.WaitGroup
	for index := range requests {
		index := index
		group.Add(1)
		go func() {
			defer group.Done()
			binding, bindErr := d.binding(requests[index])
			if bindErr != nil {
				candidates[index].err = bindErr
				return
			}
			if binding.RecoveryValidator == nil || binding.Estimator == nil {
				candidates[index].err = fmt.Errorf("fresh recovery validator or quote source is unavailable")
				return
			}
			binding.Validator = binding.RecoveryValidator
			output, quoteErr := binding.Estimator.QuoteExactInput(ctx, requests[index].Input, requests[index].Stage.OutputToken)
			if quoteErr != nil {
				candidates[index].err = quoteErr
				return
			}
			discovery, quoteErr := market.NewQuote(market.Quote{
				Source: "live/recovery", Market: requests[index].Stage.Market,
				SnapshotVersion: 1, Purpose: market.QuotePurposeLiveValidation,
				Mode: market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
				AmountIn: requests[index].Input, AmountOut: output, QuotedAt: time.Now().UTC(),
			})
			if quoteErr != nil {
				candidates[index].err = quoteErr
				return
			}
			bundle, prepareErr := d.prepareSwap(ctx, requests[index], binding, nil, discovery)
			if prepareErr != nil {
				candidates[index].err = prepareErr
				return
			}
			simulation, simulationErr := d.simulateEconomic(ctx, binding, bundle.prepared)
			if simulationErr != nil {
				candidates[index].err = simulationErr
				return
			}
			bundle.simulation = simulation
			candidates[index] = candidate{bundle: bundle, binding: binding}
		}()
	}
	group.Wait()
	best := -1
	for index := range candidates {
		if candidates[index].err != nil {
			continue
		}
		if best < 0 {
			best = index
			continue
		}
		left, convErr := d.convertAmount(candidates[index].bundle.simulation.Output, candidates[best].bundle.simulation.Output.Token())
		if convErr != nil {
			continue
		}
		if left.Units().Cmp(candidates[best].bundle.simulation.Output.Units()) > 0 {
			best = index
		}
	}
	if best < 0 {
		return execution.SequentialStageSettlement{}, errors.Join(
			fmt.Errorf("original buy recovery: %w", candidates[0].err),
			fmt.Errorf("alternate buy recovery: %w", candidates[1].err),
		)
	}
	d.preflightMu.Lock()
	if d.preflightBuys == nil {
		d.preflightBuys = make(map[execution.OperationID]preparedSwap)
	}
	d.preflightBuys[operation] = candidates[best].bundle
	d.preflightMu.Unlock()
	d.write("live_recovery operation=%s action=buy_same_quote_input selected_market=%s input_units=%s simulated_output_units=%s\n",
		operation, requests[best].Stage.Market, requests[best].Input,
		candidates[best].bundle.simulation.Output,
	)
	return d.ExecuteStage(ctx, requests[best], journal)
}

func (d *SwapDriver) broadcastPreparedSwap(
	ctx context.Context,
	request execution.SequentialStageRequest,
	phase string,
	bundle preparedSwap,
	journal executionport.SequentialJournal,
) (execution.SequentialStageSettlement, error) {
	binding, err := d.binding(request)
	if err != nil {
		return execution.SequentialStageSettlement{}, err
	}
	started := time.Now()
	broadcast, err := binding.TxManager.Broadcast(ctx, bundle.prepared)
	if err != nil && broadcast.Disposition == chainport.BroadcastRejected {
		var nonceLow *chainport.AllFanoutNonceTooLowError
		if errors.As(err, &nonceLow) {
			_ = journal.MarkTransaction(
				context.WithoutCancel(ctx), request.Operation,
				request.Stage.Ordinal, phase, "rejected",
			)
			bundle, phase, broadcast, err = d.rebuildAfterNonceTooLow(
				ctx, request, binding, journal, phase, nonceLow,
			)
			started = time.Now()
		}
	}
	if err != nil {
		disposition := executionport.DispositionPossible
		status := "outcome_unknown"
		if broadcast.Disposition == chainport.BroadcastRejected {
			disposition, status = executionport.DispositionRejected, "rejected"
		}
		_ = journal.MarkTransaction(context.WithoutCancel(ctx), request.Operation, request.Stage.Ordinal, phase, status)
		return execution.SequentialStageSettlement{}, executionport.NewStageError(disposition, err)
	}
	if !broadcast.Accepted {
		_ = journal.MarkTransaction(context.WithoutCancel(ctx), request.Operation, request.Stage.Ordinal, phase, "outcome_unknown")
		return execution.SequentialStageSettlement{}, executionport.NewStageError(executionport.DispositionPossible, fmt.Errorf("parallel swap broadcaster did not accept the prepared identity"))
	}
	if err := journal.MarkTransaction(ctx, request.Operation, request.Stage.Ordinal, phase, "broadcast"); err != nil {
		return execution.SequentialStageSettlement{}, executionport.NewStageError(executionport.DispositionPossible, err)
	}
	step := execution.OperationStep{
		Operation: request.Operation, Leg: bundle.artifact.Leg,
		Identity:  bundle.prepared.Identity,
		Technical: execution.StateBroadcastPossible, Economic: execution.EconomicReserved,
	}
	fallback := d.FallbackAfter
	if fallback <= 0 {
		fallback = 2 * time.Second
	}
	observed, err := d.confirm(ctx, binding, step, fallback)
	if err != nil {
		_ = journal.MarkTransaction(context.WithoutCancel(ctx), request.Operation, request.Stage.Ordinal, phase, "outcome_unknown")
		return execution.SequentialStageSettlement{}, executionport.NewStageError(executionport.DispositionPossible, err)
	}
	if observed.Technical != execution.StateConfirmedSuccess ||
		observed.Economic != execution.EconomicEffectVerified ||
		observed.ActualIn.IsZero() || observed.ActualOut.IsZero() {
		status := "outcome_unknown"
		disposition := executionport.DispositionPossible
		var valued []execution.CostComponent
		switch observed.Technical {
		case execution.StateConfirmedRevert:
			status = "confirmed_revert"
			disposition = executionport.DispositionConfirmedFailure
			valued, _ = valueCosts(d.Costs, observed.Costs)
		case execution.StateBroadcastRejected:
			status = "rejected"
			disposition = executionport.DispositionRejected
		}
		_ = journal.MarkTransaction(context.WithoutCancel(ctx), request.Operation, request.Stage.Ordinal, phase, status)
		return execution.SequentialStageSettlement{}, executionport.NewStageErrorWithCosts(
			disposition, valued,
			fmt.Errorf("parallel swap settlement failed: technical=%s economic=%s", observed.Technical, observed.Economic),
		)
	}
	valued, err := valueCosts(d.Costs, observed.Costs)
	if err != nil {
		return execution.SequentialStageSettlement{}, executionport.NewStageError(executionport.DispositionPossible, err)
	}
	if err := journal.MarkTransaction(ctx, request.Operation, request.Stage.Ordinal, phase, "confirmed"); err != nil {
		return execution.SequentialStageSettlement{}, executionport.NewStageError(executionport.DispositionPossible, err)
	}
	d.write("live_stage operation=%s stage=%d/%s phase=settled mode=parallel actual_input_units=%s actual_output_units=%s evidence=%s latency=%s\n",
		request.Operation, request.Stage.Ordinal, request.Stage.Stage,
		observed.ActualIn, observed.ActualOut, observed.Evidence,
		time.Since(started).Round(10*time.Microsecond),
	)
	return execution.SequentialStageSettlement{
		Request: request, ActualInput: observed.ActualIn, ActualOutput: observed.ActualOut,
		Costs: valued, SourceIdentity: bundle.prepared.Identity,
		ObservedAt: observed.ObservedAt, Evidence: observed.Evidence,
	}, nil
}

func (d *SwapDriver) parallelSellInput(plan execution.SequentialPlan) (market.TokenAmount, error) {
	return plan.ParallelSellInput()
}

func (d *SwapDriver) simulateEconomic(
	ctx context.Context,
	binding SwapBinding,
	prepared chainport.PreparedTransaction,
) (chainport.EconomicSimulationResult, error) {
	simulator, ok := binding.TxManager.(chainport.EconomicPreparedTransactionSimulator)
	if !ok || binding.BalanceSnapshot == nil {
		return chainport.EconomicSimulationResult{}, &executionport.SimulationInvariantError{
			Chain: prepared.Leg.Chain, Market: prepared.Leg.Market,
			Identity: prepared.Identity.Hash,
			Err:      fmt.Errorf("economic simulator or balance snapshot is unavailable"),
		}
	}
	before, version, err := binding.BalanceSnapshot(prepared.Leg.ExpectedOutput.Token())
	if err != nil {
		return chainport.EconomicSimulationResult{}, err
	}
	result, err := simulator.SimulatePreparedEconomic(ctx, chainport.EconomicSimulationRequest{
		Prepared: prepared, OutputBalanceBefore: before, BalanceVersion: version,
	})
	if err != nil {
		var outputErr *chainport.EconomicOutputError
		if errors.As(err, &outputErr) {
			return chainport.EconomicSimulationResult{}, &executionport.SimulationInvariantError{
				Chain: prepared.Leg.Chain, Market: prepared.Leg.Market,
				Identity: prepared.Identity.Hash, Err: outputErr,
			}
		}
		return chainport.EconomicSimulationResult{}, err
	}
	if result.Output.IsZero() || result.Output.Token() != prepared.Leg.ExpectedOutput.Token() {
		return chainport.EconomicSimulationResult{}, &executionport.SimulationInvariantError{
			Chain: prepared.Leg.Chain, Market: prepared.Leg.Market,
			Identity: prepared.Identity.Hash,
			Err:      fmt.Errorf("simulation result has no attributable output"),
		}
	}
	return result, nil
}

func (d *SwapDriver) parallelEconomics(
	plan execution.SequentialPlan,
	buy, sell chainport.EconomicSimulationResult,
) (parallelSimulationEconomics, error) {
	candidate, err := selectedCandidate(plan.Opportunity)
	if err != nil {
		return parallelSimulationEconomics{}, err
	}
	buyQuoteDecimals, ok := d.TokenDecimals[plan.InitialInput.Token()]
	if !ok {
		return parallelSimulationEconomics{}, fmt.Errorf("parallel quote decimals unavailable")
	}
	buyBaseDecimals, ok := d.TokenDecimals[buy.Output.Token()]
	if !ok {
		return parallelSimulationEconomics{}, fmt.Errorf("parallel buy base decimals unavailable")
	}
	sellBaseDecimals, ok := d.TokenDecimals[sell.Input.Token()]
	if !ok {
		return parallelSimulationEconomics{}, fmt.Errorf("parallel sell base decimals unavailable")
	}
	sellQuoteDecimals, ok := d.TokenDecimals[sell.Output.Token()]
	if !ok {
		return parallelSimulationEconomics{}, fmt.Errorf("parallel sell quote decimals unavailable")
	}
	qIn := new(big.Rat).SetFrac(plan.InitialInput.Units(), decimalScale(buyQuoteDecimals))
	bBuy := new(big.Rat).SetFrac(buy.Output.Units(), decimalScale(buyBaseDecimals))
	bSell := new(big.Rat).SetFrac(sell.Input.Units(), decimalScale(sellBaseDecimals))
	qOut := new(big.Rat).SetFrac(sell.Output.Units(), decimalScale(sellQuoteDecimals))
	if bBuy.Sign() <= 0 || bSell.Sign() <= 0 {
		return parallelSimulationEconomics{}, fmt.Errorf("parallel simulated base amount is zero")
	}
	quoteDeltaRat := new(big.Rat).Sub(qOut, qIn)
	baseDeltaRat := new(big.Rat).Sub(bBuy, bSell)
	mark := new(big.Rat).Quo(qIn, bBuy)
	mark.Add(mark, new(big.Rat).Quo(qOut, bSell)).Quo(mark, big.NewRat(2, 1))
	marked := new(big.Rat).Mul(baseDeltaRat, mark)
	grossRat := new(big.Rat).Add(quoteDeltaRat, marked)
	quoteDelta, _ := market.NewAssetQuantity(d.QuoteAsset, quoteDeltaRat)
	if d.BaseAsset == "" {
		return parallelSimulationEconomics{}, fmt.Errorf("parallel base asset unavailable")
	}
	baseDelta, _ := market.NewAssetQuantity(d.BaseAsset, baseDeltaRat)
	gross, _ := market.NewAssetQuantity(d.QuoteAsset, grossRat)
	cost := candidate.Cost.Amount
	if cost.Asset() != d.QuoteAsset {
		return parallelSimulationEconomics{}, fmt.Errorf("parallel cost is not valued in %s", d.QuoteAsset)
	}
	net, err := gross.Sub(cost)
	if err != nil {
		return parallelSimulationEconomics{}, err
	}
	return parallelSimulationEconomics{
		BuyOutput: buy.Output, SellOutput: sell.Output,
		QuoteDelta: quoteDelta, BaseDelta: baseDelta, MarkPrice: mark,
		Gross: gross, Cost: cost, Net: net, ValidatedAt: time.Now().UTC(),
	}, nil
}
