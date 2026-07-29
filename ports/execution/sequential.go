package execution

import (
	"context"
	"errors"
	"fmt"
	"time"

	domainexecution "github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
)

// BroadcastDisposition tells the saga whether a failed stage is known not to
// have changed balances or whether its result must be reconciled manually.
type BroadcastDisposition string

const (
	DispositionRejected         BroadcastDisposition = "rejected"
	DispositionPossible         BroadcastDisposition = "possible"
	DispositionConfirmedFailure BroadcastDisposition = "confirmed_failure"
)

type StageError struct {
	Disposition BroadcastDisposition
	Err         error
}

func (e *StageError) Error() string {
	if e == nil || e.Err == nil {
		return "sequential stage failed"
	}
	return e.Err.Error()
}

func (e *StageError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func NewStageError(disposition BroadcastDisposition, err error) error {
	if err == nil {
		err = errors.New("sequential stage failed")
	}
	return &StageError{Disposition: disposition, Err: err}
}

func ErrorDisposition(err error) BroadcastDisposition {
	var stageErr *StageError
	if errors.As(err, &stageErr) {
		return stageErr.Disposition
	}
	return DispositionPossible
}

type PreparedTransaction struct {
	Operation  domainexecution.OperationID
	Ordinal    int
	Phase      string
	Identity   domainexecution.TransactionIdentity
	PreparedAt time.Time
}

func (t PreparedTransaction) Validate() error {
	if t.Operation == "" || t.Ordinal < 1 || t.Phase == "" || t.PreparedAt.IsZero() {
		return fmt.Errorf("prepared sequential transaction is incomplete")
	}
	return t.Identity.Validate()
}

// SequentialJournal is deliberately passed into stage drivers. A driver must
// durably call RecordPrepared before broadcasting each source or destination
// transaction and then update its disposition.
type SequentialJournal interface {
	CreateSequentialOperation(context.Context, domainexecution.SequentialOperation) error
	RecordPreparedTransaction(context.Context, PreparedTransaction) error
	MarkTransaction(context.Context, domainexecution.OperationID, int, string, string) error
	RecordStageSettlement(context.Context, domainexecution.SequentialStageSettlement) error
	FinishSequentialOperation(context.Context, domainexecution.OperationID, domainexecution.SequentialOperationState, error) error
	ActiveSequentialOperation(context.Context) (domainexecution.SequentialOperation, bool, error)
}

type SequentialResultJournal interface {
	RecordSequentialResult(context.Context, SequentialResult) error
}

type SequentialExitDecisionJournal interface {
	RecordSequentialExitDecision(
		context.Context,
		domainexecution.SequentialExitDecision,
	) error
}

type SequentialStageDriver interface {
	ExecuteStage(
		context.Context,
		domainexecution.SequentialStageRequest,
		SequentialJournal,
	) (domainexecution.SequentialStageSettlement, error)
}

// SequentialPreflight validates all dependent swap legs before the first
// transaction is persisted or broadcast. DiscardPreflight releases any
// in-memory signed artifact retained for the first stage.
type SequentialPreflight interface {
	Preflight(
		context.Context,
		domainexecution.OperationID,
		domainexecution.SequentialPlan,
	) error
	DiscardPreflight(domainexecution.OperationID)
}

// SequentialExitSelector revalidates the liquidation available after the
// first base bridge. It may select the normal destination sale or one terminal
// base-token bridge back to the purchase chain.
type SequentialExitSelector interface {
	SelectExit(
		context.Context,
		domainexecution.OperationID,
		domainexecution.SequentialPlan,
		market.TokenAmount,
		[]domainexecution.CostComponent,
	) (domainexecution.SequentialExitDecision, error)
}

// CostValuator converts measured native-chain costs into the setup quote
// asset using a preloaded price snapshot. Implementations must not perform
// network I/O on the settlement path.
type CostValuator interface {
	Value(domainexecution.CostComponent) (domainexecution.CostComponent, error)
}

type DriverSet struct {
	Buy               SequentialStageDriver
	BridgeBase        SequentialStageDriver
	Sell              SequentialStageDriver
	BridgeQuoteReturn SequentialStageDriver
	ExitSelector      SequentialExitSelector
}

func (d DriverSet) Driver(stage domainexecution.SequentialStage) (SequentialStageDriver, error) {
	var driver SequentialStageDriver
	switch stage {
	case domainexecution.StageBuy:
		driver = d.Buy
	case domainexecution.StageBridgeBase:
		driver = d.BridgeBase
	case domainexecution.StageSell:
		driver = d.Sell
	case domainexecution.StageBridgeQuoteReturn:
		driver = d.BridgeQuoteReturn
	}
	if driver == nil {
		return nil, fmt.Errorf("driver for stage %q is unavailable", stage)
	}
	return driver, nil
}

type SequentialResult struct {
	Operation      domainexecution.OperationID
	FinalAmount    market.TokenAmount
	ExitDecision   *domainexecution.SequentialExitDecision
	Settlements    []domainexecution.SequentialStageSettlement
	Costs          []domainexecution.CostComponent
	ExecutionCost  market.AssetQuantity
	ExternalCost   market.AssetQuantity
	RealizedGross  market.AssetQuantity
	RealizedNetPnL market.AssetQuantity
}

// SequentialObserver receives lifecycle evidence after durable state changes.
// Implementations must return quickly and must never make network calls on the
// executor goroutine.
type SequentialObserver interface {
	OperationStarted(
		domainexecution.SequentialOperation,
		domainexecution.SequentialPlan,
	)
	StageStarted(domainexecution.SequentialStageRequest)
	StageSettled(domainexecution.SequentialStageSettlement)
	OperationFinished(
		domainexecution.SequentialOperation,
		domainexecution.SequentialOperationState,
		SequentialResult,
		error,
	)
}

type SequentialExitObserver interface {
	ExitSelected(domainexecution.SequentialExitDecision)
}
