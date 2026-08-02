package saga

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"

	domainexecution "github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

func (e *SequentialExecutor) executePrefundedParallel(
	ctx context.Context,
	operation domainexecution.SequentialOperation,
	plan domainexecution.SequentialPlan,
	result executionport.SequentialResult,
) (executionport.SequentialResult, error) {
	parallel, ok := e.drivers.Buy.(executionport.SequentialParallelSwapDriver)
	if !ok {
		return result, fmt.Errorf("prefunded_parallel swap driver is unavailable")
	}
	settlements, swapErr := parallel.ExecuteParallelSwaps(ctx, operation.ID, plan, e.journal)
	for _, settlement := range settlements {
		if err := e.recordParallelSettlement(ctx, &operation, &result, settlement); err != nil {
			finishErr := e.finish(ctx, operation, domainexecution.SequentialManualIntervention, result, err)
			return result, errors.Join(err, finishErr)
		}
	}
	if swapErr != nil {
		state := domainexecution.SequentialManualIntervention
		if len(settlements) == 0 && executionport.IsDefinitiveFailure(swapErr) {
			state = domainexecution.SequentialAborted
		}
		finishErr := e.finish(ctx, operation, state, result, swapErr)
		return result, errors.Join(swapErr, finishErr)
	}
	if len(settlements) != 2 {
		return result, fmt.Errorf("parallel swap settlements are incomplete")
	}
	outputs := map[int]market.TokenAmount{}
	for _, settlement := range settlements {
		outputs[settlement.Request.Stage.Ordinal] = settlement.ActualOutput
	}

	bridgeStages := []domainexecution.SequentialStagePlan{plan.Stages[2], plan.Stages[3]}
	type bridgeResult struct {
		settlement domainexecution.SequentialStageSettlement
		err        error
	}
	bridges := make([]bridgeResult, 2)
	var group sync.WaitGroup
	for index, stage := range bridgeStages {
		index, stage := index, stage
		input, err := e.resolveInput(plan, stage, plan.InitialInput, outputs)
		if err != nil {
			return result, err
		}
		request := domainexecution.SequentialStageRequest{Operation: operation.ID, Plan: plan.ID, Stage: stage, Input: input}
		driver, err := e.drivers.Driver(stage.Stage)
		if err != nil {
			return result, err
		}
		group.Add(1)
		go func() {
			defer group.Done()
			e.observe(func(observer executionport.SequentialObserver) { observer.StageStarted(request) })
			bridges[index].settlement, bridges[index].err = driver.ExecuteStage(ctx, request, e.journal)
		}()
	}
	group.Wait()
	var bridgeErrors []error
	for index := range bridges {
		if bridges[index].err != nil {
			bridgeErrors = append(bridgeErrors, fmt.Errorf("parallel %s: %w", bridgeStages[index].Stage, bridges[index].err))
			continue
		}
		if err := e.recordParallelSettlement(ctx, &operation, &result, bridges[index].settlement); err != nil {
			bridgeErrors = append(bridgeErrors, err)
		}
	}
	if len(bridgeErrors) > 0 {
		err := errors.Join(bridgeErrors...)
		finishErr := e.finish(ctx, operation, domainexecution.SequentialManualIntervention, result, err)
		return result, errors.Join(err, finishErr)
	}
	if err := calculateParallelEconomics(plan, &result); err != nil {
		finishErr := e.finish(ctx, operation, domainexecution.SequentialManualIntervention, result, err)
		return result, errors.Join(err, finishErr)
	}
	if journal, ok := e.journal.(executionport.SequentialResultJournal); ok {
		if err := journal.RecordSequentialResult(ctx, result); err != nil {
			return result, err
		}
	}
	if err := e.finish(ctx, operation, domainexecution.SequentialCompleted, result, nil); err != nil {
		return result, err
	}
	return result, nil
}

func (e *SequentialExecutor) recordParallelSettlement(
	ctx context.Context,
	operation *domainexecution.SequentialOperation,
	result *executionport.SequentialResult,
	settlement domainexecution.SequentialStageSettlement,
) error {
	if err := settlement.Validate(); err != nil {
		return err
	}
	if err := e.journal.RecordStageSettlement(ctx, settlement); err != nil {
		return err
	}
	result.Settlements = append(result.Settlements, settlement)
	result.Costs = append(result.Costs, settlement.Costs...)
	operation.CurrentStage = settlement.Request.Stage.Ordinal
	operation.CurrentAmount = settlement.ActualOutput
	operation.UpdatedAt = settlement.ObservedAt
	e.observe(func(observer executionport.SequentialObserver) { observer.StageSettled(settlement) })
	return nil
}

func calculateParallelEconomics(plan domainexecution.SequentialPlan, result *executionport.SequentialResult) error {
	if result == nil || len(result.Settlements) < 2 || plan.BaseAsset == "" || plan.QuoteAsset == "" {
		return fmt.Errorf("parallel realized economics input is incomplete")
	}
	var buy, sell domainexecution.SequentialStageSettlement
	for _, settlement := range result.Settlements {
		switch settlement.Request.Stage.Stage {
		case domainexecution.StageBuy:
			buy = settlement
		case domainexecution.StageSell:
			sell = settlement
		}
	}
	if buy.ActualOutput.IsZero() || sell.ActualOutput.IsZero() {
		return fmt.Errorf("parallel swap settlements are incomplete")
	}
	qIn, err := parallelHumanAmount(plan, buy.ActualInput)
	if err != nil {
		return err
	}
	qOut, err := parallelHumanAmount(plan, sell.ActualOutput)
	if err != nil {
		return err
	}
	bBuy, err := parallelHumanAmount(plan, buy.ActualOutput)
	if err != nil {
		return err
	}
	bSell, err := parallelHumanAmount(plan, sell.ActualInput)
	if err != nil {
		return err
	}
	quoteDeltaRat := new(big.Rat).Sub(qOut, qIn)
	baseDeltaRat := new(big.Rat).Sub(bBuy, bSell)
	buyPrice := new(big.Rat).Quo(qIn, bBuy)
	sellPrice := new(big.Rat).Quo(qOut, bSell)
	mark := new(big.Rat).Quo(new(big.Rat).Add(buyPrice, sellPrice), big.NewRat(2, 1))
	markedRat := new(big.Rat).Mul(baseDeltaRat, mark)
	quoteDelta, _ := market.NewAssetQuantity(plan.QuoteAsset, quoteDeltaRat)
	baseDelta, _ := market.NewAssetQuantity(plan.BaseAsset, baseDeltaRat)
	marked, _ := market.NewAssetQuantity(plan.QuoteAsset, markedRat)
	gross, _ := quoteDelta.Add(marked)
	total, _ := market.NewAssetQuantity(plan.QuoteAsset, new(big.Rat))
	external, _ := market.NewAssetQuantity(plan.QuoteAsset, new(big.Rat))
	for _, cost := range result.Costs {
		if cost.QuoteValue.Asset() != plan.QuoteAsset {
			return fmt.Errorf("parallel cost is not valued in %s", plan.QuoteAsset)
		}
		total, _ = total.Add(cost.QuoteValue)
		if !cost.IncludedInOutput {
			external, _ = external.Add(cost.QuoteValue)
		}
	}
	net, _ := gross.Sub(external)
	result.FinalAmount = sell.ActualOutput
	result.QuoteDelta, result.BaseDelta, result.MarkedBase, result.MarkPrice = quoteDelta, baseDelta, marked, mark
	result.ExecutionCost, result.ExternalCost = total, external
	result.RealizedGross, result.RealizedNetPnL = gross, net
	return nil
}

func parallelHumanAmount(
	plan domainexecution.SequentialPlan,
	amount market.TokenAmount,
) (*big.Rat, error) {
	decimals, ok := plan.TokenDecimals[amount.Token()]
	if !ok {
		return nil, fmt.Errorf("parallel token decimals are unavailable for %s", amount.Token())
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	return new(big.Rat).SetFrac(amount.Units(), scale), nil
}
