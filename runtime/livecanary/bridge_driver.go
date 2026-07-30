package livecanary

import (
	"context"
	"fmt"
	"math/big"

	"github.com/VarozXYZ/vernier/domain/execution"
	crosschainport "github.com/VarozXYZ/vernier/ports/crosschain"
	executionport "github.com/VarozXYZ/vernier/ports/execution"
)

type BridgeDriver struct {
	Stage    execution.SequentialStage
	Provider crosschainport.LiveTransferService
	Costs    executionport.CostValuator
}

func (d *BridgeDriver) ExecuteStage(
	ctx context.Context,
	request execution.SequentialStageRequest,
	journal executionport.SequentialJournal,
) (execution.SequentialStageSettlement, error) {
	if err := request.Validate(); err != nil {
		return execution.SequentialStageSettlement{}, err
	}
	if request.Stage.Stage != d.Stage || d.Provider == nil {
		return execution.SequentialStageSettlement{}, fmt.Errorf(
			"bridge driver cannot execute stage %q", request.Stage.Stage,
		)
	}
	result, err := d.Provider.Transfer(ctx, request, journal)
	if err != nil {
		costs := executionport.ErrorCosts(err)
		if len(costs) > 0 {
			valued, valueErr := valueCosts(d.Costs, costs)
			if valueErr != nil {
				return execution.SequentialStageSettlement{},
					executionport.NewStageError(
						executionport.DispositionPossible,
						fmt.Errorf(
							"value interrupted bridge costs: %w",
							valueErr,
						),
					)
			}
			return execution.SequentialStageSettlement{},
				executionport.NewStageErrorWithCosts(
					executionport.ErrorDisposition(err),
					valued,
					err,
				)
		}
		return execution.SequentialStageSettlement{}, err
	}
	valuedCosts, err := valueCosts(d.Costs, result.Costs)
	if err != nil {
		return execution.SequentialStageSettlement{},
			executionport.NewStageError(executionport.DispositionPossible, err)
	}
	settlement := execution.SequentialStageSettlement{
		Request: request, ActualInput: result.ActualInput,
		ActualOutput: result.ActualOutput, Costs: valuedCosts,
		SourceIdentity:           result.SourceIdentity,
		DestinationIdentity:      &result.DestinationIdentity,
		DestinationBalanceBefore: cloneBigInt(result.DestinationBalanceBefore),
		DestinationBalanceAfter:  cloneBigInt(result.DestinationBalanceAfter),
		ObservedAt:               result.ObservedAt, Evidence: result.Evidence,
	}
	if err := settlement.Validate(); err != nil {
		return execution.SequentialStageSettlement{},
			executionport.NewStageError(executionport.DispositionPossible, err)
	}
	return settlement, nil
}

func (d *BridgeDriver) RecoverStage(
	ctx context.Context,
	request execution.SequentialStageRequest,
	transactions []executionport.SequentialTransactionRecord,
	journal executionport.SequentialJournal,
) (execution.SequentialStageSettlement, error) {
	provider, ok := d.Provider.(crosschainport.RecoverableLiveTransferService)
	if !ok {
		return execution.SequentialStageSettlement{},
			executionport.NewRecoveryError(
				executionport.RecoveryFailureConfiguration,
				fmt.Errorf("bridge provider does not support durable recovery"),
			)
	}
	result, err := provider.RecoverTransfer(
		ctx,
		request,
		transactions,
		journal,
	)
	if err != nil {
		return execution.SequentialStageSettlement{}, err
	}
	valuedCosts, err := valueCosts(d.Costs, result.Costs)
	if err != nil {
		return execution.SequentialStageSettlement{},
			executionport.NewRecoveryError(
				executionport.RecoveryFailureTemporary,
				fmt.Errorf("value recovered bridge costs: %w", err),
			)
	}
	settlement := execution.SequentialStageSettlement{
		Request: request, ActualInput: result.ActualInput,
		ActualOutput: result.ActualOutput, Costs: valuedCosts,
		SourceIdentity:           result.SourceIdentity,
		DestinationIdentity:      &result.DestinationIdentity,
		DestinationBalanceBefore: cloneBigInt(result.DestinationBalanceBefore),
		DestinationBalanceAfter:  cloneBigInt(result.DestinationBalanceAfter),
		ObservedAt:               result.ObservedAt, Evidence: result.Evidence,
	}
	if err := settlement.Validate(); err != nil {
		return execution.SequentialStageSettlement{},
			executionport.NewRecoveryError(
				executionport.RecoveryFailureUncertain,
				err,
			)
	}
	return settlement, nil
}

var _ executionport.SequentialStageDriver = (*BridgeDriver)(nil)
var _ executionport.SequentialRecoveryDriver = (*BridgeDriver)(nil)

func cloneBigInt(value *big.Int) *big.Int {
	if value == nil {
		return nil
	}
	return new(big.Int).Set(value)
}
