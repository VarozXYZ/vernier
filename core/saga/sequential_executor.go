package saga

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	domainexecution "github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

// SequentialExecutor runs a single inventory-carrying cross-chain operation.
// It intentionally has no retry loop: an uncertain broadcast is reconciled,
// never resent.
type SequentialExecutor struct {
	journal  executionport.SequentialJournal
	drivers  executionport.DriverSet
	clock    func() time.Time
	observer executionport.SequentialObserver
	mu       sync.Mutex
	running  bool
}

func NewSequentialExecutor(
	journal executionport.SequentialJournal,
	drivers executionport.DriverSet,
	clock func() time.Time,
) (*SequentialExecutor, error) {
	return NewSequentialExecutorWithObserver(journal, drivers, clock, nil)
}

func NewSequentialExecutorWithObserver(
	journal executionport.SequentialJournal,
	drivers executionport.DriverSet,
	clock func() time.Time,
	observer executionport.SequentialObserver,
) (*SequentialExecutor, error) {
	if journal == nil {
		return nil, fmt.Errorf("sequential executor journal is required")
	}
	if clock == nil {
		clock = time.Now
	}
	for _, stage := range []domainexecution.SequentialStage{
		domainexecution.StageBuy,
		domainexecution.StageBridgeBase,
		domainexecution.StageSell,
		domainexecution.StageBridgeQuoteReturn,
	} {
		if _, err := drivers.Driver(stage); err != nil {
			return nil, err
		}
	}
	return &SequentialExecutor{
		journal: journal, drivers: drivers, clock: clock, observer: observer,
	}, nil
}

func (e *SequentialExecutor) Execute(
	ctx context.Context,
	operationID domainexecution.OperationID,
	plan domainexecution.SequentialPlan,
) (executionport.SequentialResult, error) {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return executionport.SequentialResult{}, fmt.Errorf("a sequential operation is already active")
	}
	e.running = true
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		e.running = false
		e.mu.Unlock()
	}()

	if operationID == "" || plan.ID == "" || len(plan.Stages) != 4 ||
		plan.InitialInput.IsZero() {
		return executionport.SequentialResult{}, fmt.Errorf("sequential execution plan is incomplete")
	}
	if active, ok, err := e.journal.ActiveSequentialOperation(ctx); err != nil {
		return executionport.SequentialResult{}, err
	} else if ok {
		return executionport.SequentialResult{}, fmt.Errorf(
			"operation %s requires reconciliation before a new operation can start",
			active.ID,
		)
	}
	result := executionport.SequentialResult{
		Operation:   operationID,
		Settlements: make([]domainexecution.SequentialStageSettlement, 0, 4),
	}
	if preflight, ok := e.drivers.Buy.(executionport.SequentialPreflight); ok {
		if err := preflight.Preflight(ctx, operationID, plan); err != nil {
			return result, fmt.Errorf("swap preflight: %w", err)
		}
		defer preflight.DiscardPreflight(operationID)
	}
	now := e.clock().UTC()
	operation := domainexecution.SequentialOperation{
		ID: operationID, Plan: plan.ID,
		OpportunityID: string(plan.Opportunity.Evaluation),
		ConfigHash:    plan.Opportunity.ConfigHash,
		State:         domainexecution.SequentialRunning,
		CurrentStage:  0, CurrentAmount: plan.InitialInput,
		StartedAt: now, UpdatedAt: now,
	}
	if recoveryJournal, ok :=
		e.journal.(executionport.SequentialRecoveryJournal); ok {
		if err := recoveryJournal.CreateRecoverableSequentialOperation(
			ctx,
			operation,
			plan,
		); err != nil {
			return executionport.SequentialResult{}, err
		}
	} else {
		if err := e.journal.CreateSequentialOperation(ctx, operation); err != nil {
			return executionport.SequentialResult{}, err
		}
	}
	e.observe(func(observer executionport.SequentialObserver) {
		observer.OperationStarted(operation, plan)
	})

	current := plan.InitialInput
	stages := append([]domainexecution.SequentialStagePlan(nil), plan.Stages...)
	recoveryAttempts := make(map[int]int, 2)
	for index := 0; index < len(stages); index++ {
		stage := stages[index]
		request := domainexecution.SequentialStageRequest{
			Operation: operationID, Plan: plan.ID, Stage: stage, Input: current,
		}
		if err := request.Validate(); err != nil {
			finishErr := e.finish(
				ctx, operation, domainexecution.SequentialManualIntervention,
				result, err,
			)
			return result, errors.Join(err, finishErr)
		}
		driver, err := e.drivers.Driver(stage.Stage)
		if err != nil {
			finishErr := e.finish(
				ctx, operation, domainexecution.SequentialManualIntervention,
				result, err,
			)
			return result, errors.Join(err, finishErr)
		}
		e.observe(func(observer executionport.SequentialObserver) {
			observer.StageStarted(request)
		})
		settlement, err := driver.ExecuteStage(ctx, request, e.journal)
		if err != nil {
			failureCosts := executionport.ErrorCosts(err)
			if len(failureCosts) > 0 {
				failureJournal, ok :=
					e.journal.(executionport.SequentialStageFailureJournal)
				if !ok {
					stageErr := fmt.Errorf(
						"stage %d (%s) failure costs cannot be persisted: %w",
						stage.Ordinal,
						stage.Stage,
						err,
					)
					finishErr := e.finish(
						ctx,
						operation,
						domainexecution.SequentialManualIntervention,
						result,
						stageErr,
					)
					return result, errors.Join(stageErr, finishErr)
				}
				if persistErr := failureJournal.RecordStageFailureCosts(
					context.WithoutCancel(ctx),
					operationID,
					stage.Ordinal,
					failureCosts,
				); persistErr != nil {
					stageErr := fmt.Errorf(
						"persist stage %d (%s) failure costs: %w",
						stage.Ordinal,
						stage.Stage,
						persistErr,
					)
					finishErr := e.finish(
						ctx,
						operation,
						domainexecution.SequentialManualIntervention,
						result,
						stageErr,
					)
					return result, errors.Join(stageErr, finishErr)
				}
				result.Costs = append(result.Costs, failureCosts...)
			}
			if e.canAutomaticallyRecoverSwap(
				stage,
				err,
				recoveryAttempts[stage.Ordinal],
			) {
				recoveryAttempts[stage.Ordinal]++
				if stage.Ordinal == 3 {
					recoveredStages, decision, recoveryErr :=
						e.reselectPostBridgeExit(
							ctx,
							operationID,
							plan,
							current,
							result.Costs,
							err,
						)
					if recoveryErr != nil {
						stageErr := fmt.Errorf(
							"stage %d (%s) failed and automatic recovery could not be selected: %w",
							stage.Ordinal,
							stage.Stage,
							errors.Join(err, recoveryErr),
						)
						finishErr := e.finish(
							ctx,
							operation,
							domainexecution.SequentialManualIntervention,
							result,
							stageErr,
						)
						return result, errors.Join(stageErr, finishErr)
					}
					stages = append(stages[:2], recoveredStages...)
					result.ExitDecision = &decision
					e.observeExit(decision)
				}
				// Retry the same ordinal with a newly built and simulated
				// transaction. The failed identity is never reused.
				index--
				continue
			}
			state := domainexecution.SequentialManualIntervention
			if stage.Ordinal == 1 &&
				executionport.IsDefinitiveFailure(err) {
				state = domainexecution.SequentialAborted
			}
			stageErr := fmt.Errorf(
				"stage %d (%s): %w", stage.Ordinal, stage.Stage, err,
			)
			finishErr := e.finish(ctx, operation, state, result, stageErr)
			return result, errors.Join(stageErr, finishErr)
		}
		if err := settlement.Validate(); err != nil {
			finishErr := e.finish(
				ctx, operation, domainexecution.SequentialManualIntervention,
				result, err,
			)
			return result, errors.Join(err, finishErr)
		}
		if err := e.journal.RecordStageSettlement(ctx, settlement); err != nil {
			finishErr := e.finish(
				ctx, operation, domainexecution.SequentialManualIntervention,
				result, err,
			)
			return result, errors.Join(err, finishErr)
		}
		current = settlement.ActualOutput
		result.Settlements = append(result.Settlements, settlement)
		result.Costs = append(result.Costs, settlement.Costs...)
		operation.CurrentStage = stage.Ordinal
		operation.CurrentAmount = current
		operation.UpdatedAt = settlement.ObservedAt
		e.observe(func(observer executionport.SequentialObserver) {
			observer.StageSettled(settlement)
		})
		if stage.Ordinal == 2 && e.drivers.ExitSelector != nil {
			decision, selectErr := e.drivers.ExitSelector.SelectExit(
				ctx,
				operationID,
				plan,
				current,
				append([]domainexecution.CostComponent(nil), result.Costs...),
			)
			if selectErr != nil {
				finishErr := e.finish(
					ctx, operation,
					domainexecution.SequentialManualIntervention,
					result,
					fmt.Errorf("select post-bridge exit: %w", selectErr),
				)
				return result, errors.Join(selectErr, finishErr)
			}
			if err := decision.Validate(); err != nil {
				finishErr := e.finish(
					ctx, operation,
					domainexecution.SequentialManualIntervention,
					result,
					err,
				)
				return result, errors.Join(err, finishErr)
			}
			exitJournal, ok := e.journal.(executionport.SequentialExitDecisionJournal)
			if !ok {
				err := fmt.Errorf(
					"sequential journal cannot persist the exit decision",
				)
				finishErr := e.finish(
					ctx, operation,
					domainexecution.SequentialManualIntervention,
					result,
					err,
				)
				return result, errors.Join(err, finishErr)
			}
			if err := exitJournal.RecordSequentialExitDecision(
				ctx,
				decision,
			); err != nil {
				finishErr := e.finish(
					ctx, operation,
					domainexecution.SequentialManualIntervention,
					result,
					err,
				)
				return result, errors.Join(err, finishErr)
			}
			result.ExitDecision = &decision
			if decision.Route == domainexecution.ExitReturnToOrigin {
				returnStages, err := plan.ReturnExitStages()
				if err != nil {
					finishErr := e.finish(
						ctx, operation,
						domainexecution.SequentialManualIntervention,
						result,
						err,
					)
					return result, errors.Join(err, finishErr)
				}
				stages = append(stages[:2], returnStages...)
			}
			e.observeExit(decision)
		}
	}
	result.FinalAmount = current
	if err := calculateSequentialEconomics(plan, &result); err != nil {
		finishErr := e.finish(
			ctx, operation, domainexecution.SequentialManualIntervention,
			result, err,
		)
		return result, errors.Join(err, finishErr)
	}
	if journal, ok := e.journal.(executionport.SequentialResultJournal); ok {
		if err := journal.RecordSequentialResult(ctx, result); err != nil {
			finishErr := e.finish(
				ctx, operation, domainexecution.SequentialManualIntervention,
				result, err,
			)
			return result, errors.Join(err, finishErr)
		}
	}
	if err := e.finish(
		ctx, operation, domainexecution.SequentialCompleted, result, nil,
	); err != nil {
		return result, err
	}
	return result, nil
}

func (e *SequentialExecutor) canAutomaticallyRecoverSwap(
	stage domainexecution.SequentialStagePlan,
	err error,
	attempts int,
) bool {
	if attempts >= 1 || stage.Stage != domainexecution.StageSell {
		return false
	}
	switch executionport.ErrorDisposition(err) {
	case executionport.DispositionRejected,
		executionport.DispositionConfirmedFailure:
		return true
	default:
		return false
	}
}

func (e *SequentialExecutor) reselectPostBridgeExit(
	ctx context.Context,
	operationID domainexecution.OperationID,
	plan domainexecution.SequentialPlan,
	current market.TokenAmount,
	costs []domainexecution.CostComponent,
	cause error,
) (
	[]domainexecution.SequentialStagePlan,
	domainexecution.SequentialExitDecision,
	error,
) {
	recoverySelector, ok :=
		e.drivers.ExitSelector.(executionport.SequentialRecoveryExitSelector)
	if e.drivers.ExitSelector == nil || !ok {
		return nil, domainexecution.SequentialExitDecision{},
			fmt.Errorf(
				"automatic recovery comparison selector is unavailable",
			)
	}
	decision, err := recoverySelector.SelectRecoveryExit(
		ctx,
		operationID,
		plan,
		current,
		append([]domainexecution.CostComponent(nil), costs...),
	)
	if err != nil {
		return nil, domainexecution.SequentialExitDecision{}, err
	}
	decision.Evidence += "+automatic_recovery_after_" +
		string(executionport.ErrorDisposition(cause))
	if err := decision.Validate(); err != nil {
		return nil, domainexecution.SequentialExitDecision{}, err
	}
	exitJournal, ok :=
		e.journal.(executionport.SequentialExitDecisionJournal)
	if !ok {
		return nil, domainexecution.SequentialExitDecision{},
			fmt.Errorf("sequential journal cannot persist the recovery exit decision")
	}
	if err := exitJournal.RecordSequentialExitDecision(
		ctx,
		decision,
	); err != nil {
		return nil, domainexecution.SequentialExitDecision{}, err
	}
	if decision.Route == domainexecution.ExitReturnToOrigin {
		stages, err := plan.ReturnExitStages()
		return stages, decision, err
	}
	return append(
		[]domainexecution.SequentialStagePlan(nil),
		plan.Stages[2:]...,
	), decision, nil
}

func calculateSequentialEconomics(
	plan domainexecution.SequentialPlan,
	result *executionport.SequentialResult,
) error {
	if result == nil || result.FinalAmount.IsZero() ||
		plan.Opportunity.SelectedIndex < 0 ||
		plan.Opportunity.SelectedIndex >= len(plan.Opportunity.Candidates) {
		return fmt.Errorf("sequential realized economics input is incomplete")
	}
	candidate := plan.Opportunity.Candidates[plan.Opportunity.SelectedIndex]
	asset := candidate.Input.Asset()
	if asset == "" || candidate.Input.Sign() <= 0 ||
		candidate.BuyQuote.AmountIn.IsZero() {
		// Older synthetic fixtures predate economic quantities. The real Live
		// planner always carries them; retain compatibility for pure saga
		// transition tests that exercise only raw token propagation.
		return nil
	}
	unitValue := new(big.Rat).Quo(
		candidate.Input.Rat(),
		new(big.Rat).SetInt(candidate.BuyQuote.AmountIn.Units()),
	)
	initial, err := market.NewAssetQuantity(
		asset,
		new(big.Rat).Mul(
			new(big.Rat).SetInt(plan.InitialInput.Units()), unitValue,
		),
	)
	if err != nil {
		return err
	}
	final, err := market.NewAssetQuantity(
		asset,
		new(big.Rat).Mul(
			new(big.Rat).SetInt(result.FinalAmount.Units()), unitValue,
		),
	)
	if err != nil {
		return err
	}
	total, _ := market.NewAssetQuantity(asset, new(big.Rat))
	external, _ := market.NewAssetQuantity(asset, new(big.Rat))
	for index, cost := range result.Costs {
		if cost.QuoteValue.Asset() != asset {
			return fmt.Errorf(
				"sequential cost %d is not valued in %s", index, asset,
			)
		}
		total, err = total.Add(cost.QuoteValue)
		if err != nil {
			return err
		}
		if !cost.IncludedInOutput {
			external, err = external.Add(cost.QuoteValue)
			if err != nil {
				return err
			}
		}
	}
	gross, err := final.Sub(initial)
	if err != nil {
		return err
	}
	net, err := gross.Sub(external)
	if err != nil {
		return err
	}
	result.ExecutionCost = total
	result.ExternalCost = external
	result.RealizedGross = gross
	result.RealizedNetPnL = net
	return nil
}

func (e *SequentialExecutor) finish(
	ctx context.Context,
	operation domainexecution.SequentialOperation,
	state domainexecution.SequentialOperationState,
	result executionport.SequentialResult,
	cause error,
) error {
	if err := e.journal.FinishSequentialOperation(
		context.WithoutCancel(ctx), operation.ID, state, cause,
	); err != nil {
		return err
	}
	operation.State = state
	operation.LastError = ""
	if cause != nil {
		operation.LastError = cause.Error()
	}
	operation.UpdatedAt = e.clock().UTC()
	e.observe(func(observer executionport.SequentialObserver) {
		observer.OperationFinished(operation, state, result, cause)
	})
	return nil
}

func (e *SequentialExecutor) observe(
	callback func(executionport.SequentialObserver),
) {
	if e.observer == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	callback(e.observer)
}

func (e *SequentialExecutor) observeExit(
	decision domainexecution.SequentialExitDecision,
) {
	if e.observer == nil {
		return
	}
	observer, ok := e.observer.(executionport.SequentialExitObserver)
	if !ok {
		return
	}
	defer func() {
		_ = recover()
	}()
	observer.ExitSelected(decision)
}
