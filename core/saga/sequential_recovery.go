package saga

import (
	"context"
	"errors"
	"fmt"
	"time"

	domainexecution "github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

var ErrSequentialRecoveryBlocked = errors.New("sequential recovery is blocked")

type SequentialRecoveryObserver interface {
	RecoveryStarted(executionport.SequentialRecoverySnapshot)
	RecoveryAttempt(executionport.SequentialRecoveryAttempt)
	RecoveryCompleted(executionport.SequentialResult)
	RecoveryBlocked(domainexecution.SequentialOperation, error)
}

type SequentialRecoveryConfig struct {
	Journal          executionport.SequentialJournal
	RecoveryJournal  executionport.SequentialRecoveryJournal
	Drivers          executionport.DriverSet
	Observer         SequentialRecoveryObserver
	Clock            func() time.Time
	Sleep            func(context.Context, time.Duration) error
	UncertainTimeout time.Duration
	CostValuator     executionport.CostValuator
}

// SequentialRecoveryCoordinator is the only startup path allowed to resume a
// durable sequential operation. It never has access to raw or signed
// transactions; each stage driver reconciles the persisted identities before
// it may construct a replacement.
type SequentialRecoveryCoordinator struct {
	config          SequentialRecoveryConfig
	emergencyRefuel func(context.Context, market.ChainID) error
}

func NewSequentialRecoveryCoordinator(
	config SequentialRecoveryConfig,
) (*SequentialRecoveryCoordinator, error) {
	if config.Journal == nil || config.RecoveryJournal == nil {
		return nil, fmt.Errorf("sequential recovery journals are required")
	}
	for _, stage := range []domainexecution.SequentialStage{
		domainexecution.StageBuy,
		domainexecution.StageBridgeBase,
		domainexecution.StageSell,
		domainexecution.StageBridgeQuoteReturn,
	} {
		driver, err := config.Drivers.Driver(stage)
		if err != nil {
			return nil, err
		}
		if _, ok := driver.(executionport.SequentialRecoveryDriver); !ok {
			return nil, fmt.Errorf(
				"driver for stage %q does not support durable recovery",
				stage,
			)
		}
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.Sleep == nil {
		config.Sleep = sleepContext
	}
	if config.UncertainTimeout <= 0 {
		config.UncertainTimeout = 10 * time.Minute
	}
	return &SequentialRecoveryCoordinator{config: config}, nil
}

func (c *SequentialRecoveryCoordinator) SetEmergencyRefuel(
	refuel func(context.Context, market.ChainID) error,
) {
	c.emergencyRefuel = refuel
}

func (c *SequentialRecoveryCoordinator) RecoverActive(
	ctx context.Context,
) (executionport.SequentialResult, bool, error) {
	active, found, err := c.config.Journal.ActiveSequentialOperation(ctx)
	if err != nil || !found {
		return executionport.SequentialResult{}, found, err
	}
	if active.State == domainexecution.SequentialRecoveryBlocked {
		blocked := fmt.Errorf(
			"%w: operation %s",
			ErrSequentialRecoveryBlocked,
			active.ID,
		)
		if c.config.Observer != nil {
			c.config.Observer.RecoveryBlocked(active, blocked)
		}
		return executionport.SequentialResult{}, true, blocked
	}
	snapshot, err := c.config.RecoveryJournal.LoadSequentialRecovery(
		ctx,
		active.ID,
	)
	if err != nil {
		blockErr := fmt.Errorf(
			"load durable recovery snapshot for %s: %w",
			active.ID,
			err,
		)
		_ = c.block(context.WithoutCancel(ctx), active, blockErr)
		return executionport.SequentialResult{}, true, blockErr
	}
	for index := range snapshot.Costs {
		if snapshot.Costs[index].QuoteValue.Asset() != "" {
			continue
		}
		if c.config.CostValuator == nil {
			blockErr := fmt.Errorf(
				"value durable recovery cost %d: execution cost valuator is unavailable",
				index,
			)
			_ = c.block(context.WithoutCancel(ctx), active, blockErr)
			return executionport.SequentialResult{}, true, blockErr
		}
		valued, valueErr := c.config.CostValuator.Value(snapshot.Costs[index])
		if valueErr != nil {
			blockErr := fmt.Errorf(
				"value durable recovery cost %d: %w", index, valueErr,
			)
			_ = c.block(context.WithoutCancel(ctx), active, blockErr)
			return executionport.SequentialResult{}, true, blockErr
		}
		snapshot.Costs[index] = valued
	}
	if c.config.Observer != nil {
		c.config.Observer.RecoveryStarted(snapshot)
	}
	if err := c.config.RecoveryJournal.SetSequentialRecoveryState(
		ctx,
		active.ID,
		domainexecution.SequentialRecovering,
		activeError(active),
	); err != nil {
		return executionport.SequentialResult{}, true, err
	}
	result, err := c.resume(ctx, snapshot)
	if err != nil {
		return result, true, err
	}
	if c.config.Observer != nil {
		c.config.Observer.RecoveryCompleted(result)
	}
	return result, true, nil
}

func (c *SequentialRecoveryCoordinator) resume(
	ctx context.Context,
	snapshot executionport.SequentialRecoverySnapshot,
) (executionport.SequentialResult, error) {
	operation := snapshot.Operation
	operation.State = domainexecution.SequentialRecovering
	result := executionport.SequentialResult{
		Operation: operation.ID,
		Settlements: append(
			[]domainexecution.SequentialStageSettlement(nil),
			snapshot.Settlements...,
		),
		Costs: append(
			[]domainexecution.CostComponent(nil),
			snapshot.Costs...,
		),
		ExitDecision: snapshot.ExitDecision,
	}
	current := operation.CurrentAmount
	stages, err := recoveryStages(snapshot.Plan, snapshot.ExitDecision)
	if err != nil {
		_ = c.block(context.WithoutCancel(ctx), operation, err)
		return result, err
	}
	outputs := make(map[int]market.TokenAmount, len(snapshot.Settlements))
	settled := make(map[int]bool, len(snapshot.Settlements))
	for _, settlement := range snapshot.Settlements {
		ordinal := settlement.Request.Stage.Ordinal
		outputs[ordinal] = settlement.ActualOutput
		settled[ordinal] = true
	}
	if snapshot.Plan.EffectivePolicy() ==
		domainexecution.PolicyPrefundedSequential &&
		settled[1] && snapshot.ExitDecision == nil {
		selected, selectErr := c.selectPrefundedRecoveryExit(
			ctx, snapshot.Plan, operation.ID, outputs[1], result.Costs, nil,
		)
		if selectErr != nil {
			_ = c.block(context.WithoutCancel(ctx), operation, selectErr)
			return result, selectErr
		}
		stages = selected.stages
		result.ExitDecision = &selected.decision
	}
	attempts := 0
	exitRefreshed := operation.CurrentStage < 2
	for {
		stage, ok := nextUnsettledRecoveryStage(stages, settled)
		if !ok {
			break
		}
		if snapshot.Plan.EffectivePolicy() ==
			domainexecution.PolicyTransportedSequential &&
			operation.CurrentStage == 2 && !exitRefreshed {
			selected, selectErr := c.reselectExit(
				ctx,
				snapshot.Plan,
				operation.ID,
				current,
				result.Costs,
			)
			if selectErr != nil {
				attempts++
				delay := recoveryBackoff(attempts)
				attempt := executionport.SequentialRecoveryAttempt{
					Operation: operation.ID, Ordinal: 3,
					Action: "select_best_recovery_exit",
					Reason: string(
						executionport.RecoveryFailureTemporary,
					),
					Detail: selectErr.Error(), Attempt: attempts,
					CreatedAt: c.config.Clock().UTC(),
					RetryAt:   c.config.Clock().UTC().Add(delay),
				}
				if persistErr := c.config.RecoveryJournal.
					RecordSequentialRecoveryAttempt(
						context.WithoutCancel(ctx),
						attempt,
					); persistErr != nil {
					return result, persistErr
				}
				if c.config.Observer != nil {
					c.config.Observer.RecoveryAttempt(attempt)
				}
				if sleepErr := c.config.Sleep(ctx, delay); sleepErr != nil {
					return result, sleepErr
				}
				continue
			}
			stages = selected.stages
			result.ExitDecision = &selected.decision
			exitRefreshed = true
			attempts = 0
			continue
		}
		input, inputErr := c.resolveRecoveryInput(
			snapshot.Plan, stage, current, outputs,
		)
		if inputErr != nil {
			_ = c.block(context.WithoutCancel(ctx), operation, inputErr)
			return result, inputErr
		}
		request := domainexecution.SequentialStageRequest{
			Operation: operation.ID,
			Plan:      snapshot.Plan.ID,
			Stage:     stage,
			Input:     input,
		}
		transactions := transactionsForOrdinal(
			snapshot.Transactions,
			stage.Ordinal,
		)
		if persistedAttempts := transactionRecoveryAttempts(
			transactions,
			stage.Ordinal,
		); persistedAttempts > attempts {
			attempts = persistedAttempts
		}
		if retryAt := latestRecoveryAttempt(transactions); retryAt.After(
			c.config.Clock().UTC(),
		) {
			if sleepErr := c.config.Sleep(
				ctx,
				retryAt.Sub(c.config.Clock().UTC()),
			); sleepErr != nil {
				return result, sleepErr
			}
		}
		if stage.Ordinal == 1 &&
			allTransactionsDefinitiveOrAbsent(transactions) {
			if err := c.config.Journal.FinishSequentialOperation(
				context.WithoutCancel(ctx),
				operation.ID,
				domainexecution.SequentialAborted,
				fmt.Errorf("initial purchase had no confirmed economic effect"),
			); err != nil {
				return result, err
			}
			return result, nil
		}
		driver, _ := c.config.Drivers.Driver(stage.Stage)
		recoveryDriver := driver.(executionport.SequentialRecoveryDriver)
		settlement, stageErr := recoveryDriver.RecoverStage(
			ctx,
			request,
			transactions,
			c.config.Journal,
		)
		if stageErr != nil {
			failureCosts := executionport.ErrorCosts(stageErr)
			if len(failureCosts) > 0 {
				failureJournal, ok :=
					c.config.Journal.(executionport.SequentialStageFailureJournal)
				if !ok {
					blockErr := fmt.Errorf(
						"cannot persist recovery failure costs: %w",
						stageErr,
					)
					_ = c.block(context.WithoutCancel(ctx), operation, blockErr)
					return result, blockErr
				}
				if err := failureJournal.RecordStageFailureCosts(
					context.WithoutCancel(ctx),
					operation.ID,
					stage.Ordinal,
					failureCosts,
				); err != nil {
					_ = c.block(context.WithoutCancel(ctx), operation, err)
					return result, err
				}
				result.Costs = append(result.Costs, failureCosts...)
			}
			kind := executionport.RecoveryKind(stageErr)
			if kind == executionport.RecoveryFailureInsufficientNative &&
				c.emergencyRefuel != nil {
				if refuelErr := c.emergencyRefuel(
					ctx,
					stage.SourceChain,
				); refuelErr != nil {
					stageErr = fmt.Errorf(
						"%w; emergency refuel: %v",
						stageErr,
						refuelErr,
					)
				}
			}
			if kind == executionport.RecoveryFailureUncertain {
				first := firstUncertain(transactions)
				if first.IsZero() {
					first = operation.UpdatedAt
				}
				if first.IsZero() {
					first = operation.StartedAt
				}
				if c.config.Clock().UTC().Sub(first) >=
					c.config.UncertainTimeout {
					blockErr := fmt.Errorf(
						"%w: stage %d remained uncertain for %s: %v",
						ErrSequentialRecoveryBlocked,
						stage.Ordinal,
						c.config.UncertainTimeout,
						stageErr,
					)
					_ = c.block(
						context.WithoutCancel(ctx),
						operation,
						blockErr,
					)
					return result, blockErr
				}
			}
			if blocksRecovery(kind) {
				blockErr := fmt.Errorf(
					"%w: stage %d: %v",
					ErrSequentialRecoveryBlocked,
					stage.Ordinal,
					stageErr,
				)
				_ = c.block(
					context.WithoutCancel(ctx),
					operation,
					blockErr,
				)
				return result, blockErr
			}
			if snapshot.Plan.EffectivePolicy() ==
				domainexecution.PolicyPrefundedSequential &&
				stage.Ordinal == 2 &&
				executionport.IsDefinitiveFailure(stageErr) {
				selected, selectErr := c.selectPrefundedRecoveryExit(
					ctx, snapshot.Plan, operation.ID, outputs[1],
					result.Costs, stageErr,
				)
				if selectErr != nil {
					blockErr := fmt.Errorf(
						"%w: prefunded recovery selection: %v",
						ErrSequentialRecoveryBlocked, selectErr,
					)
					_ = c.block(context.WithoutCancel(ctx), operation, blockErr)
					return result, blockErr
				}
				stages = selected.stages
				result.ExitDecision = &selected.decision
				attempts = 0
				continue
			}
			attempts++
			delay := recoveryBackoff(attempts)
			attempt := executionport.SequentialRecoveryAttempt{
				Operation: operation.ID,
				Ordinal:   stage.Ordinal,
				Action:    recoveryAction(stage.Stage, kind),
				Reason:    string(kind),
				Detail:    stageErr.Error(),
				Attempt:   attempts,
				CreatedAt: c.config.Clock().UTC(),
				RetryAt:   c.config.Clock().UTC().Add(delay),
			}
			if persistErr := c.config.RecoveryJournal.
				RecordSequentialRecoveryAttempt(
					context.WithoutCancel(ctx),
					attempt,
				); persistErr != nil {
				return result, persistErr
			}
			if c.config.Observer != nil {
				c.config.Observer.RecoveryAttempt(attempt)
			}
			if snapshot.Plan.EffectivePolicy() ==
				domainexecution.PolicyTransportedSequential &&
				stage.Stage == domainexecution.StageSell {
				reselected, selectErr := c.reselectExit(
					ctx,
					snapshot.Plan,
					operation.ID,
					current,
					result.Costs,
				)
				if selectErr == nil {
					stages = reselected.stages
					result.ExitDecision = &reselected.decision
					exitRefreshed = true
				}
			}
			if sleepErr := c.config.Sleep(ctx, delay); sleepErr != nil {
				return result, sleepErr
			}
			refreshed, loadErr :=
				c.config.RecoveryJournal.LoadSequentialRecovery(
					ctx,
					operation.ID,
				)
			if loadErr != nil {
				return result, loadErr
			}
			snapshot.Transactions = refreshed.Transactions
			continue
		}
		if err := settlement.Validate(); err != nil {
			_ = c.block(context.WithoutCancel(ctx), operation, err)
			return result, err
		}
		if err := c.config.Journal.RecordStageSettlement(
			context.WithoutCancel(ctx),
			settlement,
		); err != nil {
			_ = c.block(context.WithoutCancel(ctx), operation, err)
			return result, err
		}
		attempts = 0
		current = settlement.ActualOutput
		outputs[stage.Ordinal] = settlement.ActualOutput
		settled[stage.Ordinal] = true
		operation.CurrentStage = stage.Ordinal
		operation.CurrentAmount = current
		operation.UpdatedAt = settlement.ObservedAt
		result.Settlements = append(result.Settlements, settlement)
		result.Costs = append(result.Costs, settlement.Costs...)
		if snapshot.Plan.EffectivePolicy() ==
			domainexecution.PolicyTransportedSequential &&
			stage.Ordinal == 2 {
			selected, selectErr := c.reselectExit(
				ctx,
				snapshot.Plan,
				operation.ID,
				current,
				result.Costs,
			)
			if selectErr != nil {
				attempts++
				delay := recoveryBackoff(attempts)
				if sleepErr := c.config.Sleep(ctx, delay); sleepErr != nil {
					return result, sleepErr
				}
				exitRefreshed = false
				continue
			}
			stages = selected.stages
			result.ExitDecision = &selected.decision
			exitRefreshed = true
		}
	}
	result.FinalAmount = current
	if err := calculateSequentialEconomics(snapshot.Plan, &result); err != nil {
		_ = c.block(context.WithoutCancel(ctx), operation, err)
		return result, err
	}
	if journal, ok := c.config.Journal.(executionport.SequentialResultJournal); ok {
		if err := journal.RecordSequentialResult(
			context.WithoutCancel(ctx),
			result,
		); err != nil {
			return result, err
		}
	}
	if err := c.config.Journal.FinishSequentialOperation(
		context.WithoutCancel(ctx),
		operation.ID,
		domainexecution.SequentialCompleted,
		nil,
	); err != nil {
		return result, err
	}
	return result, nil
}

type recoveryExit struct {
	stages   []domainexecution.SequentialStagePlan
	decision domainexecution.SequentialExitDecision
}

func (c *SequentialRecoveryCoordinator) reselectExit(
	ctx context.Context,
	plan domainexecution.SequentialPlan,
	operation domainexecution.OperationID,
	current market.TokenAmount,
	costs []domainexecution.CostComponent,
) (recoveryExit, error) {
	selector, ok :=
		c.config.Drivers.ExitSelector.(executionport.SequentialRecoveryExitSelector)
	if !ok {
		return recoveryExit{}, fmt.Errorf(
			"recovery exit selector is unavailable",
		)
	}
	decision, err := selector.SelectRecoveryExit(
		ctx,
		operation,
		plan,
		current,
		append([]domainexecution.CostComponent(nil), costs...),
	)
	if err != nil {
		return recoveryExit{}, err
	}
	if err := decision.Validate(); err != nil {
		return recoveryExit{}, err
	}
	journal, ok :=
		c.config.Journal.(executionport.SequentialExitDecisionJournal)
	if !ok {
		return recoveryExit{}, fmt.Errorf(
			"recovery journal cannot persist an exit decision",
		)
	}
	if err := journal.RecordSequentialExitDecision(ctx, decision); err != nil {
		return recoveryExit{}, err
	}
	stages := append(
		[]domainexecution.SequentialStagePlan(nil),
		plan.Stages...,
	)
	if decision.Route == domainexecution.ExitReturnToOrigin {
		returnStages, deriveErr := plan.ReturnExitStages()
		if deriveErr != nil {
			return recoveryExit{}, deriveErr
		}
		stages = append(stages[:2], returnStages...)
	}
	return recoveryExit{stages: stages, decision: decision}, nil
}

func (c *SequentialRecoveryCoordinator) selectPrefundedRecoveryExit(
	ctx context.Context,
	plan domainexecution.SequentialPlan,
	operation domainexecution.OperationID,
	bought market.TokenAmount,
	costs []domainexecution.CostComponent,
	cause error,
) (recoveryExit, error) {
	selector, ok :=
		c.config.Drivers.ExitSelector.(executionport.SequentialPrefundedExitSelector)
	if !ok {
		return recoveryExit{},
			fmt.Errorf("prefunded recovery selector is unavailable")
	}
	var (
		decision domainexecution.SequentialExitDecision
		err      error
	)
	if cause == nil {
		decision, err = selector.SelectPrefundedExit(
			ctx, operation, plan, bought,
			append([]domainexecution.CostComponent(nil), costs...),
		)
	} else {
		decision, err = selector.SelectPrefundedRecoveryExit(
			ctx, operation, plan, bought,
			append([]domainexecution.CostComponent(nil), costs...), cause,
		)
	}
	if err != nil {
		return recoveryExit{}, err
	}
	if err := decision.Validate(); err != nil {
		return recoveryExit{}, err
	}
	journal, ok :=
		c.config.Journal.(executionport.SequentialExitDecisionJournal)
	if !ok {
		return recoveryExit{},
			fmt.Errorf("recovery journal cannot persist an exit decision")
	}
	if err := journal.RecordSequentialExitDecision(ctx, decision); err != nil {
		return recoveryExit{}, err
	}
	stages := append(
		[]domainexecution.SequentialStagePlan(nil), plan.Stages...,
	)
	if decision.Route == domainexecution.ExitSellAtOrigin {
		stages = append(
			[]domainexecution.SequentialStagePlan{plan.Stages[0]},
			plan.CircuitBreaker...,
		)
	}
	return recoveryExit{stages: stages, decision: decision}, nil
}

func (c *SequentialRecoveryCoordinator) resolveRecoveryInput(
	plan domainexecution.SequentialPlan,
	stage domainexecution.SequentialStagePlan,
	legacyCurrent market.TokenAmount,
	outputs map[int]market.TokenAmount,
) (market.TokenAmount, error) {
	for _, dependency := range stage.DependsOn {
		if _, ok := outputs[dependency]; !ok {
			return market.TokenAmount{}, fmt.Errorf(
				"recovery stage %d awaits dependency %d",
				stage.Ordinal, dependency,
			)
		}
	}
	var input market.TokenAmount
	if stage.InputFromOrdinal == 0 && stage.Ordinal > 1 {
		input = legacyCurrent
	} else {
		var err error
		input, err = plan.InputFor(stage, outputs)
		if err != nil {
			return market.TokenAmount{}, err
		}
	}
	if input.Token() == stage.InputToken {
		return input, nil
	}
	driver, err := c.config.Drivers.Driver(stage.Stage)
	if err != nil {
		return market.TokenAmount{}, err
	}
	converter, ok := driver.(executionport.SequentialInputConverter)
	if !ok {
		return market.TokenAmount{}, fmt.Errorf(
			"recovery stage %d requires a chain-local input converter",
			stage.Ordinal,
		)
	}
	return converter.ConvertStageInput(stage, input)
}

func (c *SequentialRecoveryCoordinator) block(
	ctx context.Context,
	operation domainexecution.SequentialOperation,
	cause error,
) error {
	err := c.config.RecoveryJournal.SetSequentialRecoveryState(
		ctx,
		operation.ID,
		domainexecution.SequentialRecoveryBlocked,
		cause,
	)
	if c.config.Observer != nil {
		c.config.Observer.RecoveryBlocked(operation, cause)
	}
	return err
}

func recoveryStages(
	plan domainexecution.SequentialPlan,
	decision *domainexecution.SequentialExitDecision,
) ([]domainexecution.SequentialStagePlan, error) {
	stages := append([]domainexecution.SequentialStagePlan(nil), plan.Stages...)
	if decision != nil &&
		decision.Route == domainexecution.ExitSellAtOrigin {
		return append(
			[]domainexecution.SequentialStagePlan{plan.Stages[0]},
			plan.CircuitBreaker...,
		), nil
	}
	if decision != nil &&
		decision.Route == domainexecution.ExitReturnToOrigin {
		returnStages, err := plan.ReturnExitStages()
		if err != nil {
			return nil, err
		}
		stages = append(stages[:2], returnStages...)
	}
	return stages, nil
}

func nextUnsettledRecoveryStage(
	stages []domainexecution.SequentialStagePlan,
	settled map[int]bool,
) (domainexecution.SequentialStagePlan, bool) {
	for _, stage := range stages {
		if !settled[stage.Ordinal] {
			return stage, true
		}
	}
	return domainexecution.SequentialStagePlan{}, false
}

func transactionsForOrdinal(
	transactions []executionport.SequentialTransactionRecord,
	ordinal int,
) []executionport.SequentialTransactionRecord {
	result := make([]executionport.SequentialTransactionRecord, 0)
	for _, transaction := range transactions {
		if transaction.Ordinal == ordinal {
			result = append(result, transaction)
		}
	}
	return result
}

func allTransactionsDefinitiveOrAbsent(
	transactions []executionport.SequentialTransactionRecord,
) bool {
	if len(transactions) == 0 {
		return true
	}
	for _, transaction := range transactions {
		switch transaction.Status {
		case "rejected", "confirmed_revert":
		default:
			return false
		}
	}
	return true
}

func firstUncertain(
	transactions []executionport.SequentialTransactionRecord,
) time.Time {
	var first time.Time
	for _, transaction := range transactions {
		candidate := transaction.FirstUncertainAt
		if candidate.IsZero() &&
			(transaction.Status == "prepared" ||
				transaction.Status == "broadcast" ||
				transaction.Status == "outcome_unknown") {
			candidate = transaction.PreparedAt
		}
		if !candidate.IsZero() &&
			(first.IsZero() || candidate.Before(first)) {
			first = candidate
		}
	}
	return first
}

func transactionRecoveryAttempts(
	transactions []executionport.SequentialTransactionRecord,
	ordinal int,
) int {
	attempts := 0
	for _, transaction := range transactions {
		if transaction.Ordinal == ordinal &&
			transaction.RecoveryAttempts > attempts {
			attempts = transaction.RecoveryAttempts
		}
	}
	return attempts
}

func latestRecoveryAttempt(
	transactions []executionport.SequentialTransactionRecord,
) time.Time {
	var latest time.Time
	for _, transaction := range transactions {
		if transaction.NextRecoveryAttempt.After(latest) {
			latest = transaction.NextRecoveryAttempt
		}
	}
	return latest
}

func blocksRecovery(kind executionport.RecoveryFailureKind) bool {
	switch kind {
	case executionport.RecoveryFailureAllowance,
		executionport.RecoveryFailureSigner,
		executionport.RecoveryFailureConfiguration:
		return true
	default:
		return false
	}
}

func recoveryAction(
	stage domainexecution.SequentialStage,
	kind executionport.RecoveryFailureKind,
) string {
	if kind == executionport.RecoveryFailureUncertain {
		return "reconcile_without_resend"
	}
	if stage == domainexecution.StageSell {
		return "requote_rebuild_simulate_best_exit"
	}
	return "resume_stage"
}

func recoveryBackoff(attempt int) time.Duration {
	schedule := [...]time.Duration{
		250 * time.Millisecond,
		500 * time.Millisecond,
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		15 * time.Second,
		30 * time.Second,
	}
	if attempt <= 0 {
		return schedule[0]
	}
	if attempt > len(schedule) {
		return schedule[len(schedule)-1]
	}
	return schedule[attempt-1]
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func activeError(operation domainexecution.SequentialOperation) error {
	if operation.LastError == "" {
		return nil
	}
	return errors.New(operation.LastError)
}
