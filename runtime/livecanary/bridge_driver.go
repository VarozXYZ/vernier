package livecanary

import (
	"context"
	"fmt"

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
		SourceIdentity:      result.SourceIdentity,
		DestinationIdentity: &result.DestinationIdentity,
		ObservedAt:          result.ObservedAt, Evidence: result.Evidence,
	}
	if err := settlement.Validate(); err != nil {
		return execution.SequentialStageSettlement{},
			executionport.NewStageError(executionport.DispositionPossible, err)
	}
	return settlement, nil
}

var _ executionport.SequentialStageDriver = (*BridgeDriver)(nil)
