package livecanary

import (
	"context"
	"errors"
	"fmt"
	"math/big"
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
			var slippage *executionport.SlippageConstraint
			if index == 0 {
				slippage, bindErr = d.dynamicBuySlippage(plan)
				if bindErr != nil {
					results[index].err = bindErr
					return
				}
			}
			bundle, prepareErr := d.prepareSwap(ctx, requests[index], binding, slippage)
			if prepareErr != nil {
				results[index].err = prepareErr
				return
			}
			simulation, simulationErr := d.simulateEconomic(ctx, binding, bundle.prepared)
			if simulationErr != nil {
				results[index].err = simulationErr
				return
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
	if !forced && economics.Net.Rat().Cmp(cloneRatOrZero(d.MinimumNet)) < 0 {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf("joint simulated PnL is below threshold: gross=%s cost=%s net=%s minimum=%s",
				economics.Gross, economics.Cost, economics.Net, cloneRatOrZero(d.MinimumNet)),
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

func (d *SwapDriver) ExecuteParallelSwaps(
	ctx context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
	journal executionport.SequentialJournal,
) ([]execution.SequentialStageSettlement, error) {
	if plan.EffectivePolicy() != execution.PolicyPrefundedParallel || len(plan.Stages) != 4 {
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
	if plan.EffectivePolicy() != execution.PolicyPrefundedParallel || len(plan.Stages) != 4 {
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
			bundle, prepareErr := d.prepareSwap(ctx, requests[index], binding, nil)
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
		disposition := executionport.DispositionConfirmedFailure
		if observed.Technical == execution.StateConfirmedRevert {
			status = "confirmed_revert"
		}
		_ = journal.MarkTransaction(context.WithoutCancel(ctx), request.Operation, request.Stage.Ordinal, phase, status)
		valued, _ := valueCosts(d.Costs, observed.Costs)
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
	candidate, err := selectedCandidate(plan.Opportunity)
	if err != nil {
		return market.TokenAmount{}, err
	}
	if candidate.BuyQuote.AmountIn.IsZero() || candidate.SellQuote.AmountIn.IsZero() {
		return market.TokenAmount{}, fmt.Errorf("parallel discovery amounts are incomplete")
	}
	units := new(big.Int).Quo(
		new(big.Int).Mul(candidate.SellQuote.AmountIn.Units(), plan.InitialInput.Units()),
		candidate.BuyQuote.AmountIn.Units(),
	)
	if units.Sign() <= 0 {
		return market.TokenAmount{}, fmt.Errorf("parallel sell input rounds to zero")
	}
	return market.NewTokenAmount(candidate.SellQuote.AmountIn.Token(), units)
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
