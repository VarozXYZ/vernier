package livecanary

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	chainport "github.com/VarozXYZ/vernier/ports/chain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

type SwapBinding struct {
	Account          execution.AccountID
	Validator        executionport.Validator
	Estimator        SwapQuoteEstimator
	TxManager        chainport.TxManager
	Confirmation     chainport.ConfirmationSource
	NonceCoordinator chainport.EVMNonceCoordinator
}

type SwapQuoteEstimator interface {
	QuoteExactInput(
		context.Context,
		market.TokenAmount,
		market.TokenID,
	) (market.TokenAmount, error)
}

type SwapQuoteEstimatorFunc func(
	context.Context,
	market.TokenAmount,
	market.TokenID,
) (market.TokenAmount, error)

func (f SwapQuoteEstimatorFunc) QuoteExactInput(
	ctx context.Context,
	input market.TokenAmount,
	output market.TokenID,
) (market.TokenAmount, error) {
	return f(ctx, input, output)
}

type SellPreflightResult struct {
	Artifact executionport.Artifact
	Identity string
}

type SellPreflight interface {
	ValidateAndSimulate(
		context.Context,
		execution.SequentialStageRequest,
	) (SellPreflightResult, error)
}

type SellPreflightFunc struct {
	Identity string
	Run      func(
		context.Context,
		execution.SequentialStageRequest,
	) (executionport.Artifact, error)
}

func (f SellPreflightFunc) ValidateAndSimulate(
	ctx context.Context,
	request execution.SequentialStageRequest,
) (SellPreflightResult, error) {
	if f.Run == nil {
		return SellPreflightResult{}, fmt.Errorf(
			"sell preflight function is unavailable",
		)
	}
	artifact, err := f.Run(ctx, request)
	if err != nil {
		return SellPreflightResult{}, err
	}
	return SellPreflightResult{
		Artifact: artifact,
		Identity: f.Identity,
	}, nil
}

type ExitCostSource interface {
	ExitCost(
		arbitrage.Direction,
		execution.SequentialExitRoute,
		time.Time,
	) (market.AssetQuantity, bool)
}

type SwapDriver struct {
	Bindings        map[market.MarketID]SwapBinding
	SellPreflights  map[market.MarketID]SellPreflight
	TokenDecimals   map[market.TokenID]uint8
	BridgePrecision uint8
	QuoteAsset      market.AssetID
	MinimumNet      *big.Rat
	ReturnMargin    *big.Rat
	ExitCosts       ExitCostSource
	Clock           func() time.Time
	FallbackAfter   time.Duration
	ArtifactMaxAge  time.Duration
	Output          io.Writer
	Costs           executionport.CostValuator

	preflightMu   sync.Mutex
	preflightBuys map[execution.OperationID]preparedSwap
	exitSells     map[execution.OperationID]preparedSwap
	swapAttempts  map[execution.OperationID]map[int]int
}

type preparedSwap struct {
	artifact        executionport.Artifact
	prepared        chainport.PreparedTransaction
	validationTime  time.Duration
	compactRebuilds int
}

type exitReturnQuote struct {
	output market.TokenAmount
	err    error
}

func (d *SwapDriver) ExecuteStage(
	ctx context.Context,
	request execution.SequentialStageRequest,
	journal executionport.SequentialJournal,
) (execution.SequentialStageSettlement, error) {
	clock := d.Clock
	if clock == nil {
		clock = time.Now
	}
	fallback := d.FallbackAfter
	if fallback <= 0 {
		fallback = 2 * time.Second
	}
	binding, err := d.binding(request)
	if err != nil {
		return execution.SequentialStageSettlement{}, err
	}
	bundle, cached := d.takePreparedSwap(request)
	if cached && d.artifactExpired(bundle.artifact, clock()) {
		return execution.SequentialStageSettlement{},
			executionport.NewStageError(
				executionport.DispositionRejected,
				fmt.Errorf(
					"simulated buy artifact expired before durable persistence",
				),
			)
	}
	if !cached {
		bundle, err = d.prepareAndSimulate(ctx, request, binding)
		if err != nil {
			return execution.SequentialStageSettlement{}, executionport.NewStageError(
				executionport.DispositionRejected, err,
			)
		}
	}
	artifact, prepared := bundle.artifact, bundle.prepared
	d.logPrepared(request, bundle, cached)
	prepareStarted := clock()
	transactionPhase := d.nextSwapTransactionPhase(
		request.Operation,
		request.Stage.Ordinal,
	)
	if err := journal.RecordPreparedTransaction(ctx, executionport.PreparedTransaction{
		Operation: request.Operation, Ordinal: request.Stage.Ordinal,
		Phase:      transactionPhase,
		Identity:   prepared.Identity,
		PreparedAt: prepared.PreparedAt,
	}); err != nil {
		return execution.SequentialStageSettlement{}, executionport.NewStageError(
			executionport.DispositionRejected, err,
		)
	}
	if prepared.Identity.Nonce != nil {
		d.write(
			"live_stage operation=%s stage=%d/%s phase=durable tx=%s nonce=%d latency=%s\n",
			request.Operation, request.Stage.Ordinal, request.Stage.Stage,
			prepared.Identity.Hash, *prepared.Identity.Nonce,
			clock().Sub(prepareStarted).Round(10*time.Microsecond),
		)
	} else {
		d.write(
			"live_stage operation=%s stage=%d/%s phase=durable tx=%s latency=%s\n",
			request.Operation, request.Stage.Ordinal, request.Stage.Stage,
			prepared.Identity.Hash,
			clock().Sub(prepareStarted).Round(10*time.Microsecond),
		)
	}
	broadcastStarted := clock()
	broadcast, err := binding.TxManager.Broadcast(ctx, prepared)
	if err != nil {
		disposition := executionport.DispositionPossible
		if broadcast.Disposition == chainport.BroadcastRejected {
			disposition = executionport.DispositionRejected
			_ = journal.MarkTransaction(
				context.WithoutCancel(ctx), request.Operation,
				request.Stage.Ordinal, transactionPhase, "rejected",
			)
		} else {
			_ = journal.MarkTransaction(
				context.WithoutCancel(ctx), request.Operation,
				request.Stage.Ordinal, transactionPhase, "outcome_unknown",
			)
		}
		return execution.SequentialStageSettlement{},
			executionport.NewStageError(disposition, err)
	}
	if !broadcast.Accepted {
		err := fmt.Errorf("swap broadcaster did not accept the prepared identity")
		_ = journal.MarkTransaction(
			context.WithoutCancel(ctx), request.Operation,
			request.Stage.Ordinal, transactionPhase, "outcome_unknown",
		)
		return execution.SequentialStageSettlement{},
			executionport.NewStageError(executionport.DispositionPossible, err)
	}
	if err := journal.MarkTransaction(
		ctx, request.Operation, request.Stage.Ordinal, transactionPhase, "broadcast",
	); err != nil {
		return execution.SequentialStageSettlement{},
			executionport.NewStageError(executionport.DispositionPossible, err)
	}
	d.write(
		"live_stage operation=%s stage=%d/%s phase=broadcast tx=%s endpoint=%s latency=%s\n",
		request.Operation, request.Stage.Ordinal, request.Stage.Stage,
		prepared.Identity.Hash, broadcast.Endpoint,
		clock().Sub(broadcastStarted).Round(10*time.Microsecond),
	)
	step := execution.OperationStep{
		Operation: request.Operation, Leg: artifact.Leg,
		Identity: prepared.Identity, Technical: execution.StateBroadcastPossible,
		Economic: execution.EconomicReserved,
	}
	confirmationStarted := clock()
	settlement, err := d.confirm(ctx, binding, step, fallback)
	if err != nil {
		_ = journal.MarkTransaction(
			context.WithoutCancel(ctx), request.Operation,
			request.Stage.Ordinal, transactionPhase, "outcome_unknown",
		)
		return execution.SequentialStageSettlement{},
			executionport.NewStageError(executionport.DispositionPossible, err)
	}
	if settlement.Technical != execution.StateConfirmedSuccess ||
		settlement.Economic != execution.EconomicEffectVerified ||
		settlement.ActualIn.IsZero() || settlement.ActualOut.IsZero() {
		err := fmt.Errorf(
			"swap settlement is not a confirmed economic success: technical=%s economic=%s",
			settlement.Technical, settlement.Economic,
		)
		disposition := executionport.DispositionPossible
		status := "outcome_unknown"
		var failureCosts []execution.CostComponent
		if settlement.Technical == execution.StateConfirmedRevert {
			disposition = executionport.DispositionConfirmedFailure
			status = "confirmed_revert"
			failureCosts, err = valueCosts(d.Costs, settlement.Costs)
			if err != nil {
				return execution.SequentialStageSettlement{},
					executionport.NewStageError(
						executionport.DispositionPossible,
						fmt.Errorf("value confirmed revert costs: %w", err),
					)
			}
		}
		_ = journal.MarkTransaction(
			context.WithoutCancel(ctx), request.Operation,
			request.Stage.Ordinal, transactionPhase, status,
		)
		return execution.SequentialStageSettlement{},
			executionport.NewStageErrorWithCosts(
				disposition,
				failureCosts,
				err,
			)
	}
	valuedCosts, err := valueCosts(d.Costs, settlement.Costs)
	if err != nil {
		return execution.SequentialStageSettlement{},
			executionport.NewStageError(executionport.DispositionPossible, err)
	}
	if err := journal.MarkTransaction(
		ctx, request.Operation, request.Stage.Ordinal, transactionPhase, "confirmed",
	); err != nil {
		return execution.SequentialStageSettlement{},
			executionport.NewStageError(executionport.DispositionPossible, err)
	}
	d.write(
		"live_stage operation=%s stage=%d/%s phase=settled actual_input_units=%s actual_output_units=%s evidence=%s latency=%s\n",
		request.Operation, request.Stage.Ordinal, request.Stage.Stage,
		settlement.ActualIn, settlement.ActualOut, settlement.Evidence,
		clock().Sub(confirmationStarted).Round(10*time.Microsecond),
	)
	return execution.SequentialStageSettlement{
		Request: request, ActualInput: settlement.ActualIn,
		ActualOutput: settlement.ActualOut, Costs: valuedCosts,
		SourceIdentity: prepared.Identity,
		ObservedAt:     settlement.ObservedAt, Evidence: settlement.Evidence,
	}, nil
}

func (d *SwapDriver) Preflight(
	ctx context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
) error {
	if operation == "" || len(plan.Stages) != 4 ||
		plan.Stages[0].Stage != execution.StageBuy ||
		plan.Stages[2].Stage != execution.StageSell {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf("swap preflight plan is incomplete"),
		)
	}
	started := time.Now()
	buyRequest := execution.SequentialStageRequest{
		Operation: operation, Plan: plan.ID,
		Stage: plan.Stages[0], Input: plan.InitialInput,
	}
	buyBinding, err := d.binding(buyRequest)
	if err != nil {
		return executionport.NewStageError(
			executionport.DispositionRejected, err,
		)
	}
	buy, err := d.prepareSwap(ctx, buyRequest, buyBinding)
	if err != nil {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf("buy preflight: %w", err),
		)
	}
	sellInput, err := d.bridgeDestinationAmount(
		buy.artifact.ValidatedQuote.AmountOut,
		plan.Stages[2].InputToken,
	)
	if err != nil {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf("derive preflight sell input: %w", err),
		)
	}
	sellRequest := execution.SequentialStageRequest{
		Operation: operation, Plan: plan.ID,
		Stage: plan.Stages[2], Input: sellInput,
	}
	sellBinding, err := d.binding(sellRequest)
	if err != nil {
		return executionport.NewStageError(
			executionport.DispositionRejected, err,
		)
	}
	sellPreflight := d.SellPreflights[sellRequest.Stage.Market]
	type sellResult struct {
		artifact executionport.Artifact
		identity string
		err      error
	}
	buySimulation := make(chan error, 1)
	sellPrepared := make(chan sellResult, 1)
	go func() {
		buySimulation <- d.simulate(ctx, buyBinding, buy.prepared)
	}()
	go func() {
		if sellPreflight != nil {
			result, preflightErr := sellPreflight.ValidateAndSimulate(
				ctx,
				sellRequest,
			)
			sellPrepared <- sellResult{
				artifact: result.Artifact,
				identity: result.Identity,
				err:      preflightErr,
			}
			return
		}
		bundle, prepareErr := d.prepareAndSimulate(
			ctx, sellRequest, sellBinding,
		)
		sellPrepared <- sellResult{
			artifact: bundle.artifact,
			identity: string(sellBinding.Account),
			err:      prepareErr,
		}
	}()
	buySimulationErr := <-buySimulation
	sellResultValue := <-sellPrepared
	if buySimulationErr != nil {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf("buy preflight: %w", buySimulationErr),
		)
	}
	if sellResultValue.err != nil {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf("sell preflight: %w", sellResultValue.err),
		)
	}
	roundTrip, err := d.convertAmount(
		sellResultValue.artifact.ValidatedQuote.AmountOut,
		plan.InitialInput.Token(),
	)
	if err != nil {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf("value preflight round trip: %w", err),
		)
	}
	forcedCanary := isForcedCanaryOpportunity(plan.Opportunity)
	if roundTrip.Units().Cmp(plan.InitialInput.Units()) <= 0 &&
		!forcedCanary {
		return executionport.NewStageError(
			executionport.DispositionRejected,
			fmt.Errorf(
				"fresh simulated round trip is no longer gross profitable: input=%s output=%s",
				plan.InitialInput,
				roundTrip,
			),
		)
	}
	d.preflightMu.Lock()
	if d.preflightBuys == nil {
		d.preflightBuys = make(map[execution.OperationID]preparedSwap)
	}
	d.preflightBuys[operation] = buy
	d.preflightMu.Unlock()
	d.write(
		"live_preflight operation=%s status=accepted forced_canary=%t buy_input_units=%s buy_output_units=%s sell_input_units=%s sell_output_units=%s sell_preflight_reference=%s round_trip_units=%s latency=%s\n",
		operation,
		forcedCanary,
		plan.InitialInput,
		buy.artifact.ValidatedQuote.AmountOut,
		sellInput,
		sellResultValue.artifact.ValidatedQuote.AmountOut,
		sellResultValue.identity,
		roundTrip,
		time.Since(started).Round(10*time.Microsecond),
	)
	return nil
}

func (d *SwapDriver) DiscardPreflight(operation execution.OperationID) {
	d.preflightMu.Lock()
	delete(d.preflightBuys, operation)
	delete(d.exitSells, operation)
	delete(d.swapAttempts, operation)
	d.preflightMu.Unlock()
}

func (d *SwapDriver) nextSwapTransactionPhase(
	operation execution.OperationID,
	ordinal int,
) string {
	d.preflightMu.Lock()
	defer d.preflightMu.Unlock()
	if d.swapAttempts == nil {
		d.swapAttempts = make(map[execution.OperationID]map[int]int)
	}
	byOrdinal := d.swapAttempts[operation]
	if byOrdinal == nil {
		byOrdinal = make(map[int]int)
		d.swapAttempts[operation] = byOrdinal
	}
	attempt := byOrdinal[ordinal]
	byOrdinal[ordinal] = attempt + 1
	if attempt == 0 {
		return "swap"
	}
	return fmt.Sprintf("swap_recovery_%d", attempt)
}

func (d *SwapDriver) SelectExit(
	ctx context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
	bridged market.TokenAmount,
	incurred []execution.CostComponent,
) (execution.SequentialExitDecision, error) {
	return d.selectExit(ctx, operation, plan, bridged, incurred, false)
}

func (d *SwapDriver) SelectRecoveryExit(
	ctx context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
	bridged market.TokenAmount,
	incurred []execution.CostComponent,
) (execution.SequentialExitDecision, error) {
	return d.selectExit(ctx, operation, plan, bridged, incurred, true)
}

func (d *SwapDriver) selectExit(
	ctx context.Context,
	operation execution.OperationID,
	plan execution.SequentialPlan,
	bridged market.TokenAmount,
	incurred []execution.CostComponent,
	forceComparison bool,
) (execution.SequentialExitDecision, error) {
	clock := d.Clock
	if clock == nil {
		clock = time.Now
	}
	if operation == "" || len(plan.Stages) != 4 || bridged.IsZero() {
		return execution.SequentialExitDecision{},
			fmt.Errorf("post-bridge exit input is incomplete")
	}
	quoteAsset := d.QuoteAsset
	if quoteAsset == "" &&
		plan.Opportunity.SelectedIndex >= 0 &&
		plan.Opportunity.SelectedIndex < len(plan.Opportunity.Candidates) {
		quoteAsset = plan.Opportunity.
			Candidates[plan.Opportunity.SelectedIndex].
			Input.Asset()
	}
	if quoteAsset == "" {
		return execution.SequentialExitDecision{},
			fmt.Errorf("post-bridge exit quote asset is unavailable")
	}
	margin, err := market.NewAssetQuantity(
		quoteAsset,
		cloneRatOrZero(d.ReturnMargin),
	)
	if err != nil {
		return execution.SequentialExitDecision{}, err
	}
	forcedCanary := isForcedCanaryOpportunity(plan.Opportunity)
	compareReturn := !forcedCanary || forceComparison
	destinationRequest := execution.SequentialStageRequest{
		Operation: operation,
		Plan:      plan.ID,
		Stage:     plan.Stages[2],
		Input:     bridged,
	}
	destinationBinding, err := d.binding(destinationRequest)
	if err != nil {
		return execution.SequentialExitDecision{}, err
	}
	var returnResult chan exitReturnQuote
	if compareReturn {
		returnCtx, cancelReturn := context.WithCancel(ctx)
		defer cancelReturn()
		returnResult = make(chan exitReturnQuote, 1)
		go func() {
			returnResult <- d.quoteReturnExit(
				returnCtx,
				plan,
				bridged,
			)
		}()
	}
	started := clock()
	destination, err := d.prepareAndSimulate(
		ctx,
		destinationRequest,
		destinationBinding,
	)
	if err != nil {
		var quotedReturn exitReturnQuote
		if !compareReturn {
			quotedReturn = d.quoteReturnExit(ctx, plan, bridged)
		} else {
			quotedReturn = <-returnResult
		}
		return d.selectForcedReturn(
			operation,
			plan,
			quoteAsset,
			margin,
			started,
			quotedReturn,
			fmt.Errorf("destination liquidation unavailable: %w", err),
		)
	}
	destinationOutput := destination.artifact.ValidatedQuote.AmountOut
	destinationRecovery, err := d.recoveryValue(
		destinationOutput,
		plan.Stages[0].InputToken,
		quoteAsset,
	)
	if err != nil {
		return execution.SequentialExitDecision{}, err
	}
	now := clock().UTC()
	destinationCost, destinationCostsOK := market.AssetQuantity{}, false
	if d.ExitCosts != nil {
		destinationCost, destinationCostsOK = d.ExitCosts.ExitCost(
			plan.Opportunity.Direction,
			execution.ExitSellAtDestination,
			now,
		)
	}
	if destinationCostsOK {
		destinationRecovery, err = destinationRecovery.Sub(destinationCost)
		if err != nil {
			return execution.SequentialExitDecision{}, err
		}
	}
	destinationQualified := false
	if destinationCostsOK {
		destinationQualified, err = d.exitStillQualified(
			plan,
			destinationRecovery,
			incurred,
			quoteAsset,
		)
		if err != nil {
			return execution.SequentialExitDecision{}, err
		}
	}
	decision := execution.SequentialExitDecision{
		Operation:             operation,
		Route:                 execution.ExitSellAtDestination,
		DestinationOutput:     destinationOutput,
		DestinationRecovery:   destinationRecovery,
		SafetyMargin:          margin,
		DestinationQualified:  destinationQualified,
		CostEvidenceAvailable: destinationCostsOK,
		DecidedAt:             now,
		Evidence:              "fresh_destination_build+simulation",
	}
	if forceComparison {
		decision.Evidence += "+automatic_recovery_comparison"
	}
	if forcedCanary && !forceComparison {
		decision.Evidence += "+forced_canary_destination"
		d.storeExitSell(operation, destination)
		d.logExitDecision(decision, clock().Sub(started))
		return decision, nil
	}
	if destinationQualified && !forceComparison {
		d.storeExitSell(operation, destination)
		d.logExitDecision(decision, clock().Sub(started))
		return decision, nil
	}
	if !destinationCostsOK && !forceComparison {
		decision.Evidence += "+destination_cost_cache_unavailable"
		d.storeExitSell(operation, destination)
		d.logExitDecision(decision, clock().Sub(started))
		return decision, nil
	}

	quotedReturn := <-returnResult
	if quotedReturn.err != nil {
		decision.Evidence += "+return_quote_unavailable"
		d.storeExitSell(operation, destination)
		d.logExitDecision(decision, clock().Sub(started))
		return decision, nil
	}
	decision.ReturnOutput = quotedReturn.output
	returnRecovery, err := d.recoveryValue(
		decision.ReturnOutput,
		plan.Stages[0].InputToken,
		quoteAsset,
	)
	if err != nil {
		return execution.SequentialExitDecision{}, err
	}
	returnCost, returnCostsOK := d.ExitCosts.ExitCost(
		plan.Opportunity.Direction,
		execution.ExitReturnToOrigin,
		now,
	)
	if returnCostsOK {
		returnRecovery, err = returnRecovery.Sub(returnCost)
		if err != nil {
			return execution.SequentialExitDecision{}, err
		}
	}
	decision.ReturnRecovery = returnRecovery
	decision.CostEvidenceAvailable =
		decision.CostEvidenceAvailable && returnCostsOK
	decision.Evidence += "+fresh_origin_quote"
	if !returnCostsOK {
		decision.Evidence += "+return_cost_cache_unavailable"
		d.storeExitSell(operation, destination)
		d.logExitDecision(decision, clock().Sub(started))
		return decision, nil
	}
	returnThreshold, err := destinationRecovery.Add(margin)
	if err != nil {
		return execution.SequentialExitDecision{}, err
	}
	if returnRecovery.Rat().Cmp(returnThreshold.Rat()) > 0 {
		decision.Route = execution.ExitReturnToOrigin
		decision.Evidence += "+return_advantage"
	} else {
		decision.Evidence += "+destination_advantage"
		d.storeExitSell(operation, destination)
	}
	d.logExitDecision(decision, clock().Sub(started))
	return decision, nil
}

func (d *SwapDriver) selectForcedReturn(
	operation execution.OperationID,
	plan execution.SequentialPlan,
	quoteAsset market.AssetID,
	margin market.AssetQuantity,
	started time.Time,
	quotedReturn exitReturnQuote,
	destinationErr error,
) (execution.SequentialExitDecision, error) {
	clock := d.Clock
	if clock == nil {
		clock = time.Now
	}
	if quotedReturn.err != nil {
		return execution.SequentialExitDecision{},
			errors.Join(destinationErr, fmt.Errorf(
				"return liquidation quote unavailable: %w",
				quotedReturn.err,
			))
	}
	returnRecovery, err := d.recoveryValue(
		quotedReturn.output,
		plan.Stages[0].InputToken,
		quoteAsset,
	)
	if err != nil {
		return execution.SequentialExitDecision{}, err
	}
	costEvidence := false
	if d.ExitCosts != nil {
		if cost, ok := d.ExitCosts.ExitCost(
			plan.Opportunity.Direction,
			execution.ExitReturnToOrigin,
			clock().UTC(),
		); ok {
			returnRecovery, err = returnRecovery.Sub(cost)
			if err != nil {
				return execution.SequentialExitDecision{}, err
			}
			costEvidence = true
		}
	}
	zero, _ := market.NewAssetQuantity(quoteAsset, new(big.Rat))
	decision := execution.SequentialExitDecision{
		Operation:             operation,
		Route:                 execution.ExitReturnToOrigin,
		DestinationRecovery:   zero,
		ReturnOutput:          quotedReturn.output,
		ReturnRecovery:        returnRecovery,
		SafetyMargin:          margin,
		CostEvidenceAvailable: costEvidence,
		DecidedAt:             clock().UTC(),
		Evidence:              "destination_unavailable+fresh_origin_quote",
	}
	d.logExitDecision(decision, clock().Sub(started))
	return decision, nil
}

func (d *SwapDriver) quoteReturnExit(
	ctx context.Context,
	plan execution.SequentialPlan,
	bridged market.TokenAmount,
) exitReturnQuote {
	returnStages, err := plan.ReturnExitStages()
	if err != nil {
		return exitReturnQuote{err: err}
	}
	returnInput, err := d.bridgeDestinationAmount(
		bridged,
		returnStages[1].InputToken,
	)
	if err != nil {
		return exitReturnQuote{err: err}
	}
	binding, ok := d.Bindings[returnStages[1].Market]
	if !ok || binding.Estimator == nil {
		return exitReturnQuote{
			err: fmt.Errorf("origin liquidation quote estimator is unavailable"),
		}
	}
	output, err := binding.Estimator.QuoteExactInput(
		ctx,
		returnInput,
		returnStages[1].OutputToken,
	)
	if err != nil {
		return exitReturnQuote{err: err}
	}
	return exitReturnQuote{output: output}
}

func (d *SwapDriver) storeExitSell(
	operation execution.OperationID,
	bundle preparedSwap,
) {
	d.preflightMu.Lock()
	if d.exitSells == nil {
		d.exitSells = make(map[execution.OperationID]preparedSwap)
	}
	d.exitSells[operation] = bundle
	d.preflightMu.Unlock()
}

func (d *SwapDriver) recoveryValue(
	output market.TokenAmount,
	terminalToken market.TokenID,
	asset market.AssetID,
) (market.AssetQuantity, error) {
	terminal, err := d.convertAmount(output, terminalToken)
	if err != nil {
		return market.AssetQuantity{}, err
	}
	decimals, ok := d.TokenDecimals[terminalToken]
	if !ok {
		return market.AssetQuantity{},
			fmt.Errorf("terminal quote-token decimals are unavailable")
	}
	return market.NewAssetQuantity(
		asset,
		new(big.Rat).SetFrac(
			terminal.Units(),
			decimalScale(decimals),
		),
	)
}

func (d *SwapDriver) exitStillQualified(
	plan execution.SequentialPlan,
	recovery market.AssetQuantity,
	incurred []execution.CostComponent,
	asset market.AssetID,
) (bool, error) {
	initial, err := d.recoveryValue(
		plan.InitialInput,
		plan.Stages[0].InputToken,
		asset,
	)
	if err != nil {
		return false, err
	}
	net, err := recovery.Sub(initial)
	if err != nil {
		return false, err
	}
	for _, component := range incurred {
		if component.IncludedInOutput {
			continue
		}
		if component.QuoteValue.Asset() != asset {
			return false, fmt.Errorf(
				"incurred exit cost is not valued in %s",
				asset,
			)
		}
		net, err = net.Sub(component.QuoteValue)
		if err != nil {
			return false, err
		}
	}
	return net.Rat().Cmp(cloneRatOrZero(d.MinimumNet)) >= 0, nil
}

func cloneRatOrZero(value *big.Rat) *big.Rat {
	if value == nil {
		return new(big.Rat)
	}
	return new(big.Rat).Set(value)
}

func (d *SwapDriver) logExitDecision(
	decision execution.SequentialExitDecision,
	elapsed time.Duration,
) {
	returnOutput, returnRecovery := "", ""
	if !decision.ReturnOutput.IsZero() {
		returnOutput = decision.ReturnOutput.String()
	}
	if decision.ReturnRecovery.Asset() != "" {
		returnRecovery = decision.ReturnRecovery.Decimal(8)
	}
	d.write(
		"live_exit operation=%s route=%s destination_output_units=%s destination_recovery=%s return_output_units=%s return_recovery=%s safety_margin=%s destination_qualified=%t cost_evidence=%t evidence=%s latency=%s\n",
		decision.Operation,
		decision.Route,
		tokenUnitsOrEmpty(decision.DestinationOutput),
		decision.DestinationRecovery.Decimal(8),
		returnOutput,
		returnRecovery,
		decision.SafetyMargin.Decimal(8),
		decision.DestinationQualified,
		decision.CostEvidenceAvailable,
		decision.Evidence,
		elapsed.Round(10*time.Microsecond),
	)
}

func tokenUnitsOrEmpty(amount market.TokenAmount) string {
	if amount.IsZero() {
		return ""
	}
	return amount.String()
}

func (d *SwapDriver) binding(
	request execution.SequentialStageRequest,
) (SwapBinding, error) {
	if err := request.Validate(); err != nil {
		return SwapBinding{}, err
	}
	if request.Stage.Stage != execution.StageBuy &&
		request.Stage.Stage != execution.StageSell {
		return SwapBinding{}, fmt.Errorf("swap driver received bridge stage")
	}
	binding, ok := d.Bindings[request.Stage.Market]
	if !ok || binding.Account == "" || binding.Validator == nil ||
		binding.TxManager == nil {
		return SwapBinding{}, fmt.Errorf(
			"swap binding for market %q is unavailable", request.Stage.Market,
		)
	}
	if _, ok := binding.TxManager.(chainport.PreparedTransactionSimulator); !ok {
		return SwapBinding{}, fmt.Errorf(
			"swap binding for market %q has no transaction simulator",
			request.Stage.Market,
		)
	}
	return binding, nil
}

func (d *SwapDriver) prepareAndSimulate(
	ctx context.Context,
	request execution.SequentialStageRequest,
	binding SwapBinding,
) (preparedSwap, error) {
	bundle, err := d.prepareSwap(ctx, request, binding)
	if err != nil {
		return preparedSwap{}, err
	}
	if err := d.simulate(ctx, binding, bundle.prepared); err != nil {
		return preparedSwap{}, err
	}
	return bundle, nil
}

func (d *SwapDriver) prepareSwap(
	ctx context.Context,
	request execution.SequentialStageRequest,
	binding SwapBinding,
) (preparedSwap, error) {
	clock := d.Clock
	if clock == nil {
		clock = time.Now
	}
	now := clock().UTC()
	validationRequest, err := swapValidationRequest(
		request,
		binding.Account,
		now,
	)
	if err != nil {
		return preparedSwap{}, err
	}
	started := clock()
	artifact, err := binding.Validator.Validate(ctx, validationRequest)
	if err != nil {
		return preparedSwap{}, err
	}
	if d.artifactExpired(artifact, clock()) {
		validationRequest.RequestedAt = clock().UTC()
		artifact, err = binding.Validator.Validate(ctx, validationRequest)
		if err != nil {
			return preparedSwap{}, err
		}
		if d.artifactExpired(artifact, clock()) {
			return preparedSwap{}, fmt.Errorf(
				"rebuilt swap artifact exceeded the pre-commit age limit",
			)
		}
	}
	var prepared chainport.PreparedTransaction
	compactRebuilds := 0
	for {
		artifact.Leg.ExpectedOutput = artifact.ValidatedQuote.AmountOut
		prepared, err = binding.TxManager.Prepare(ctx, artifact)
		if err == nil {
			break
		}
		var oversized *executionport.ArtifactTooLargeError
		compact, supportsCompact := binding.Validator.(executionport.CompactValidator)
		if !errors.As(err, &oversized) ||
			!supportsCompact ||
			compactRebuilds >= 3 {
			return preparedSwap{}, err
		}
		validationRequest.RequestedAt = clock().UTC()
		artifact, err = compact.ValidateCompact(
			ctx,
			validationRequest,
			artifact,
		)
		if err != nil {
			return preparedSwap{}, fmt.Errorf(
				"compact swap artifact after %d-byte transaction: %w",
				oversized.ActualBytes,
				err,
			)
		}
		compactRebuilds++
		if d.artifactExpired(artifact, clock()) {
			return preparedSwap{}, fmt.Errorf(
				"compact swap artifact exceeded the pre-commit age limit",
			)
		}
	}
	return preparedSwap{
		artifact: artifact, prepared: prepared,
		validationTime:  clock().Sub(started),
		compactRebuilds: compactRebuilds,
	}, nil
}

func swapValidationRequest(
	request execution.SequentialStageRequest,
	account execution.AccountID,
	now time.Time,
) (executionport.ValidationRequest, error) {
	if account == "" {
		return executionport.ValidationRequest{},
			fmt.Errorf("swap validation account is unavailable")
	}
	placeholder, err := market.NewTokenAmount(
		request.Stage.OutputToken,
		big.NewInt(1),
	)
	if err != nil {
		return executionport.ValidationRequest{}, err
	}
	side := execution.LegBuy
	if request.Stage.Stage == execution.StageSell {
		side = execution.LegSell
	}
	leg := execution.Leg{
		ID: execution.StepID(fmt.Sprintf(
			"%02d-%s",
			request.Stage.Ordinal,
			request.Stage.Stage,
		)),
		Side: side, Chain: request.Stage.SourceChain,
		Account: account, Market: request.Stage.Market,
		Input: request.Input, ExpectedOutput: placeholder,
	}
	discovery, err := market.NewQuote(market.Quote{
		Source: "live", Market: request.Stage.Market,
		SnapshotVersion: 1, Purpose: market.QuotePurposeLiveDiscovery,
		Mode: market.QuoteModeExactInput, Quality: market.QuoteQualityExact,
		AmountIn: request.Input, AmountOut: placeholder, QuotedAt: now,
	})
	if err != nil {
		return executionport.ValidationRequest{}, err
	}
	return executionport.ValidationRequest{
		Operation: request.Operation, Leg: leg, Discovery: discovery,
		RequestedAt: now,
	}, nil
}

func (d *SwapDriver) simulate(
	ctx context.Context,
	binding SwapBinding,
	prepared chainport.PreparedTransaction,
) error {
	simulator := binding.TxManager.(chainport.PreparedTransactionSimulator)
	return simulator.SimulatePrepared(ctx, prepared)
}

func (d *SwapDriver) takePreparedSwap(
	request execution.SequentialStageRequest,
) (preparedSwap, bool) {
	if request.Stage.Ordinal != 1 && request.Stage.Ordinal != 3 {
		return preparedSwap{}, false
	}
	d.preflightMu.Lock()
	var bundle preparedSwap
	var ok bool
	if request.Stage.Ordinal == 1 &&
		request.Stage.Stage == execution.StageBuy {
		bundle, ok = d.preflightBuys[request.Operation]
		delete(d.preflightBuys, request.Operation)
	} else if request.Stage.Ordinal == 3 &&
		request.Stage.Stage == execution.StageSell {
		bundle, ok = d.exitSells[request.Operation]
		delete(d.exitSells, request.Operation)
	}
	d.preflightMu.Unlock()
	if !ok ||
		bundle.artifact.Leg.Market != request.Stage.Market ||
		bundle.artifact.Leg.Input.Token() != request.Input.Token() ||
		bundle.artifact.Leg.Input.Units().Cmp(request.Input.Units()) != 0 {
		return preparedSwap{}, false
	}
	return bundle, true
}

func (d *SwapDriver) artifactExpired(
	artifact executionport.Artifact,
	now time.Time,
) bool {
	return d.ArtifactMaxAge > 0 &&
		now.UTC().Sub(artifact.BuiltAt) > d.ArtifactMaxAge
}

func (d *SwapDriver) logPrepared(
	request execution.SequentialStageRequest,
	bundle preparedSwap,
	preflightReused bool,
) {
	if bundle.compactRebuilds > 0 {
		d.write(
			"live_stage operation=%s stage=%d/%s phase=artifact_compacted rebuilds=%d max_accounts=%s serialized_bytes=%d\n",
			request.Operation,
			request.Stage.Ordinal,
			request.Stage.Stage,
			bundle.compactRebuilds,
			bundle.artifact.Metadata["max_accounts"],
			len(bundle.prepared.SignedPayload),
		)
	}
	attempts := bundle.artifact.Metadata["build_attempts"]
	d.write(
		"live_stage operation=%s stage=%d/%s phase=artifact_ready input_units=%s output_units=%s build_attempts=%s preflight_reused=%t latency=%s\n",
		request.Operation,
		request.Stage.Ordinal,
		request.Stage.Stage,
		request.Input,
		bundle.artifact.ValidatedQuote.AmountOut,
		attempts,
		preflightReused,
		bundle.validationTime.Round(10*time.Microsecond),
	)
	d.write(
		"live_stage operation=%s stage=%d/%s phase=simulation_ready tx=%s\n",
		request.Operation,
		request.Stage.Ordinal,
		request.Stage.Stage,
		bundle.prepared.Identity.Hash,
	)
}

func (d *SwapDriver) bridgeDestinationAmount(
	source market.TokenAmount,
	destinationToken market.TokenID,
) (market.TokenAmount, error) {
	sourceDecimals, sourceOK := d.TokenDecimals[source.Token()]
	destinationDecimals, destinationOK := d.TokenDecimals[destinationToken]
	if !sourceOK || !destinationOK {
		return market.TokenAmount{}, fmt.Errorf(
			"bridge preflight token decimals are unavailable",
		)
	}
	precision := d.BridgePrecision
	if precision == 0 {
		precision = 8
	}
	if sourceDecimals < precision {
		precision = sourceDecimals
	}
	if destinationDecimals < precision {
		precision = destinationDecimals
	}
	sourceScale := decimalScale(sourceDecimals - precision)
	messageUnits := new(big.Int).Quo(source.Units(), sourceScale)
	if messageUnits.Sign() <= 0 {
		return market.TokenAmount{}, fmt.Errorf(
			"preflight buy output is below bridge precision",
		)
	}
	destinationUnits := new(big.Int).Mul(
		messageUnits,
		decimalScale(destinationDecimals-precision),
	)
	return market.NewTokenAmount(destinationToken, destinationUnits)
}

func (d *SwapDriver) convertAmount(
	source market.TokenAmount,
	destinationToken market.TokenID,
) (market.TokenAmount, error) {
	sourceDecimals, sourceOK := d.TokenDecimals[source.Token()]
	destinationDecimals, destinationOK := d.TokenDecimals[destinationToken]
	if !sourceOK || !destinationOK {
		return market.TokenAmount{}, fmt.Errorf(
			"preflight comparison token decimals are unavailable",
		)
	}
	units := source.Units()
	switch {
	case sourceDecimals > destinationDecimals:
		units.Quo(units, decimalScale(sourceDecimals-destinationDecimals))
	case sourceDecimals < destinationDecimals:
		units.Mul(units, decimalScale(destinationDecimals-sourceDecimals))
	}
	return market.NewTokenAmount(destinationToken, units)
}

func decimalScale(decimals uint8) *big.Int {
	return new(big.Int).Exp(
		big.NewInt(10),
		big.NewInt(int64(decimals)),
		nil,
	)
}

func (d *SwapDriver) write(format string, arguments ...any) {
	if d.Output != nil {
		_, _ = fmt.Fprintf(d.Output, format, arguments...)
	}
}

func (d *SwapDriver) confirm(
	ctx context.Context,
	binding SwapBinding,
	step execution.OperationStep,
	fallbackAfter time.Duration,
) (execution.Settlement, error) {
	if binding.Confirmation != nil {
		websocketCtx, cancel := context.WithTimeout(ctx, fallbackAfter)
		settlement, err := binding.Confirmation.Await(websocketCtx, step)
		cancel()
		if err == nil && settlement.Technical == execution.StateConfirmedSuccess &&
			settlement.Economic == execution.EconomicEffectVerified {
			return settlement, nil
		}
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		settlement, err := binding.TxManager.Reconcile(ctx, step)
		if err == nil {
			switch settlement.Technical {
			case execution.StateConfirmedSuccess, execution.StateConfirmedRevert:
				return settlement, nil
			}
		}
		select {
		case <-ctx.Done():
			if err != nil {
				return execution.Settlement{}, err
			}
			return execution.Settlement{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

var _ executionport.SequentialStageDriver = (*SwapDriver)(nil)
var _ executionport.SequentialPreflight = (*SwapDriver)(nil)
var _ executionport.SequentialExitSelector = (*SwapDriver)(nil)
var _ executionport.SequentialRecoveryExitSelector = (*SwapDriver)(nil)
