package execution

import (
	"context"
	"errors"
	"fmt"
	"math/big"
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
	Costs       []domainexecution.CostComponent
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
	return NewStageErrorWithCosts(disposition, nil, err)
}

func NewStageErrorWithCosts(
	disposition BroadcastDisposition,
	costs []domainexecution.CostComponent,
	err error,
) error {
	if err == nil {
		err = errors.New("sequential stage failed")
	}
	return &StageError{
		Disposition: disposition,
		Costs:       append([]domainexecution.CostComponent(nil), costs...),
		Err:         err,
	}
}

func ErrorDisposition(err error) BroadcastDisposition {
	var stageErr *StageError
	if errors.As(err, &stageErr) {
		return stageErr.Disposition
	}
	return DispositionPossible
}

// IsDefinitiveFailure reports whether the stage is known not to have produced
// its intended economic effect. A confirmed on-chain revert may still incur
// network costs, but its token/account state changes were rolled back.
func IsDefinitiveFailure(err error) bool {
	switch ErrorDisposition(err) {
	case DispositionRejected, DispositionConfirmedFailure:
		return true
	default:
		return false
	}
}

func ErrorCosts(err error) []domainexecution.CostComponent {
	var stageErr *StageError
	if !errors.As(err, &stageErr) {
		return nil
	}
	return append([]domainexecution.CostComponent(nil), stageErr.Costs...)
}

type PreparedTransaction struct {
	Operation                domainexecution.OperationID
	Ordinal                  int
	Phase                    string
	Identity                 domainexecution.TransactionIdentity
	PreparedAt               time.Time
	SimulatedInput           market.TokenAmount
	SimulatedOutput          market.TokenAmount
	SimulationEvidence       string
	SimulationContextVersion uint64
	SimulationUnitsConsumed  uint64
}

func (t PreparedTransaction) Validate() error {
	if t.Operation == "" || t.Ordinal < 1 || t.Phase == "" || t.PreparedAt.IsZero() {
		return fmt.Errorf("prepared sequential transaction is incomplete")
	}
	if t.SimulatedInput.IsZero() != t.SimulatedOutput.IsZero() {
		return fmt.Errorf("prepared transaction simulation evidence is incomplete")
	}
	if !t.SimulatedInput.IsZero() && t.SimulationEvidence == "" {
		return fmt.Errorf("prepared transaction simulation evidence is incomplete")
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

type SequentialPreparedBatchJournal interface {
	RecordPreparedTransactions(context.Context, []PreparedTransaction) error
}

type TriggerFirstDecisionKind string

const (
	TriggerFirstDecisionEconomic75_25 TriggerFirstDecisionKind = "economic_75_25"
	TriggerFirstDecisionForcedFixed   TriggerFirstDecisionKind = "forced_canary_fixed"
)

// TriggerFirstDecision is normalized pre-broadcast evidence for the first-leg
// floor. Normal execution records the dynamic economic 75/25 budget; a forced
// canary records that it deliberately used the configured fixed floor. It
// intentionally contains neither calldata nor a signed transaction.
type TriggerFirstDecision struct {
	Operation          domainexecution.OperationID
	Ordinal            int
	Kind               TriggerFirstDecisionKind
	ExpectedNet        string
	ReservedHeadroom   string
	ConsumableBudget   string
	MinimumOutputToken market.TokenID
	MinimumOutputUnits string
	EquivalentBPS      uint16
	AllocationHash     string
	DecidedAt          time.Time
}

func (d TriggerFirstDecision) Validate() error {
	kind := d.Kind
	if kind == "" {
		kind = TriggerFirstDecisionEconomic75_25
	}
	if d.Operation == "" || d.Ordinal < 1 || d.MinimumOutputToken == "" || d.MinimumOutputUnits == "" ||
		d.EquivalentBPS > 10_000 || d.AllocationHash == "" || d.DecidedAt.IsZero() {
		return fmt.Errorf("trigger-first decision evidence is incomplete")
	}
	switch kind {
	case TriggerFirstDecisionEconomic75_25:
		if d.ExpectedNet == "" || d.ReservedHeadroom == "" || d.ConsumableBudget == "" {
			return fmt.Errorf("trigger-first decision evidence is incomplete")
		}
	case TriggerFirstDecisionForcedFixed:
		if d.ReservedHeadroom != "" || d.ConsumableBudget != "" {
			return fmt.Errorf("forced canary decision must not claim an economic slippage budget")
		}
	default:
		return fmt.Errorf("trigger-first decision kind %q is unsupported", kind)
	}
	return nil
}

type SequentialTriggerFirstDecisionJournal interface {
	RecordTriggerFirstDecision(context.Context, TriggerFirstDecision) error
}

type SequentialParallelSwapDriver interface {
	ExecuteParallelSwaps(
		context.Context,
		domainexecution.OperationID,
		domainexecution.SequentialPlan,
		SequentialJournal,
	) ([]domainexecution.SequentialStageSettlement, error)
}

// SequentialTriggeredSwapDriver specializes prefunded parallel execution for
// a locally triggered opportunity. The first (local) swap is confirmed before
// the freshly quoted and simulated remote swap may be broadcast.
type SequentialTriggeredSwapDriver interface {
	StagedFor(domainexecution.SequentialPlan) bool
	ExecuteTriggeredSwaps(
		context.Context,
		domainexecution.OperationID,
		domainexecution.SequentialPlan,
		SequentialJournal,
	) ([]domainexecution.SequentialStageSettlement, error)
}

type SequentialParallelBuyRecovery interface {
	RecoverParallelBuy(
		context.Context,
		domainexecution.OperationID,
		domainexecution.SequentialPlan,
		[]SequentialTransactionRecord,
		SequentialJournal,
	) (domainexecution.SequentialStageSettlement, error)
}

type SequentialTransactionRecord struct {
	Operation           domainexecution.OperationID
	Ordinal             int
	Phase               string
	Identity            domainexecution.TransactionIdentity
	Status              string
	LastError           string
	PreparedAt          time.Time
	UpdatedAt           time.Time
	FirstUncertainAt    time.Time
	RecoveryReason      string
	RecoveryAttempts    int
	NextRecoveryAttempt time.Time
	SimulatedInput      market.TokenAmount
	SimulatedOutput     market.TokenAmount
	SimulationEvidence  string
	SimulationVersion   uint64
	SimulationUnits     uint64
}

type SequentialRecoverySnapshot struct {
	Operation    domainexecution.SequentialOperation
	Plan         domainexecution.SequentialPlan
	Transactions []SequentialTransactionRecord
	Settlements  []domainexecution.SequentialStageSettlement
	Costs        []domainexecution.CostComponent
	ExitDecision *domainexecution.SequentialExitDecision
}

type SequentialRecoveryAttempt struct {
	Operation domainexecution.OperationID
	Ordinal   int
	Action    string
	Reason    string
	Detail    string
	Attempt   int
	CreatedAt time.Time
	RetryAt   time.Time
}

// SequentialRecoveryJournal is implemented by production journals. Creation
// of an operation and its neutral execution plan must be one durable commit.
type SequentialRecoveryJournal interface {
	CreateRecoverableSequentialOperation(
		context.Context,
		domainexecution.SequentialOperation,
		domainexecution.SequentialPlan,
	) error
	LoadSequentialRecovery(
		context.Context,
		domainexecution.OperationID,
	) (SequentialRecoverySnapshot, error)
	SetSequentialRecoveryState(
		context.Context,
		domainexecution.OperationID,
		domainexecution.SequentialOperationState,
		error,
	) error
	RecordSequentialRecoveryAttempt(
		context.Context,
		SequentialRecoveryAttempt,
	) error
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

type SequentialStageFailureJournal interface {
	RecordStageFailureCosts(
		context.Context,
		domainexecution.OperationID,
		int,
		[]domainexecution.CostComponent,
	) error
}

type SequentialStageDriver interface {
	ExecuteStage(
		context.Context,
		domainexecution.SequentialStageRequest,
		SequentialJournal,
	) (domainexecution.SequentialStageSettlement, error)
}

// SequentialRecoveryDriver resumes a stage from durable transaction
// identities. Implementations must reconcile every possibly-broadcast
// identity before constructing a replacement.
type SequentialRecoveryDriver interface {
	RecoverStage(
		context.Context,
		domainexecution.SequentialStageRequest,
		[]SequentialTransactionRecord,
		SequentialJournal,
	) (domainexecution.SequentialStageSettlement, error)
}

type RecoveryFailureKind string

const (
	RecoveryFailureUncertain          RecoveryFailureKind = "uncertain"
	RecoveryFailureTemporary          RecoveryFailureKind = "temporary"
	RecoveryFailureStaleArtifact      RecoveryFailureKind = "stale_artifact"
	RecoveryFailureBalanceMismatch    RecoveryFailureKind = "balance_mismatch"
	RecoveryFailureAllowance          RecoveryFailureKind = "allowance"
	RecoveryFailureSigner             RecoveryFailureKind = "signer"
	RecoveryFailureConfiguration      RecoveryFailureKind = "configuration"
	RecoveryFailureFeeCap             RecoveryFailureKind = "fee_cap"
	RecoveryFailureInsufficientNative RecoveryFailureKind = "insufficient_native"
)

type RecoveryError struct {
	Kind  RecoveryFailureKind
	Chain market.ChainID
	Err   error
}

func (e *RecoveryError) Error() string {
	if e == nil || e.Err == nil {
		return "sequential recovery failed"
	}
	return e.Err.Error()
}

func (e *RecoveryError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func NewRecoveryError(kind RecoveryFailureKind, err error) error {
	if err == nil {
		err = errors.New("sequential recovery failed")
	}
	return &RecoveryError{Kind: kind, Err: err}
}

func NewChainRecoveryError(
	kind RecoveryFailureKind,
	chain market.ChainID,
	err error,
) error {
	if err == nil {
		err = errors.New("sequential recovery failed")
	}
	return &RecoveryError{Kind: kind, Chain: chain, Err: err}
}

func RecoveryKind(err error) RecoveryFailureKind {
	if err == nil {
		return ""
	}
	var recoveryErr *RecoveryError
	if errors.As(err, &recoveryErr) {
		return recoveryErr.Kind
	}
	if ErrorDisposition(err) == DispositionPossible {
		return RecoveryFailureUncertain
	}
	return RecoveryFailureTemporary
}

func RecoveryChain(err error) (market.ChainID, bool) {
	if err == nil {
		return "", false
	}
	var recoveryErr *RecoveryError
	if !errors.As(err, &recoveryErr) || recoveryErr.Chain == "" {
		return "", false
	}
	return recoveryErr.Chain, true
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

// SequentialRecoveryExitSelector performs the same post-bridge comparison
// after a known-safe liquidation failure, but must evaluate both available
// exits even when the destination sale remains policy-qualified.
type SequentialRecoveryExitSelector interface {
	SelectRecoveryExit(
		context.Context,
		domainexecution.OperationID,
		domainexecution.SequentialPlan,
		market.TokenAmount,
		[]domainexecution.CostComponent,
	) (domainexecution.SequentialExitDecision, error)
}

// SequentialPrefundedExitSelector prepares the destination sale after the buy
// settlement is known. The normal destination sale uses fixed slippage. Only
// a safe preparation or execution failure starts a fresh comparison of both
// executable recovery sales.
type SequentialPrefundedExitSelector interface {
	SelectPrefundedExit(
		context.Context,
		domainexecution.OperationID,
		domainexecution.SequentialPlan,
		market.TokenAmount,
		[]domainexecution.CostComponent,
	) (domainexecution.SequentialExitDecision, error)
	SelectPrefundedRecoveryExit(
		context.Context,
		domainexecution.OperationID,
		domainexecution.SequentialPlan,
		market.TokenAmount,
		[]domainexecution.CostComponent,
		error,
	) (domainexecution.SequentialExitDecision, error)
}

// SequentialInputConverter converts an economic settlement into the
// chain-local token identity consumed by a dependent step. Implementations
// must round down and must not perform I/O.
type SequentialInputConverter interface {
	ConvertStageInput(
		domainexecution.SequentialStagePlan,
		market.TokenAmount,
	) (market.TokenAmount, error)
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
	QuoteDelta     market.AssetQuantity
	BaseDelta      market.AssetQuantity
	MarkedBase     market.AssetQuantity
	MarkPrice      *big.Rat
	TerminalState  domainexecution.SequentialOperationState
	TerminalError  string
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

// SequentialAsyncQuoteRestorer owns the independently durable quote-token
// return after prefunded parallel swaps. Start must persist capacity and job
// intent before launching work; it never retains an economic candidate.
type SequentialAsyncQuoteRestorer interface {
	Start(
		context.Context,
		domainexecution.SequentialStageRequest,
		SequentialStageDriver,
		SequentialJournal,
	) error
	BeginBase(context.Context, domainexecution.OperationID) error
	CompleteBase(context.Context, domainexecution.OperationID, error) error
}

type SequentialExitObserver interface {
	ExitSelected(domainexecution.SequentialExitDecision)
}
