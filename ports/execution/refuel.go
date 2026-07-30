package execution

import (
	"context"
	"time"

	domainexecution "github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
)

type RefuelState string

const (
	RefuelPrepared       RefuelState = "prepared"
	RefuelBroadcast      RefuelState = "broadcast"
	RefuelCompleted      RefuelState = "completed"
	RefuelFailed         RefuelState = "failed"
	RefuelOutcomeUnknown RefuelState = "outcome_unknown"
)

type RefuelRecord struct {
	ID             string
	Chain          market.ChainID
	State          RefuelState
	Input          market.TokenAmount
	NativeAsset    market.AssetID
	BalanceBefore  market.AssetQuantity
	BalanceAfter   market.AssetQuantity
	NativeReceived market.AssetQuantity
	Fee            market.AssetQuantity
	Identity       domainexecution.TransactionIdentity
	LastError      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type RefuelJournal interface {
	CreateRefuel(context.Context, RefuelRecord) error
	MarkRefuelBroadcast(
		context.Context,
		string,
		domainexecution.TransactionIdentity,
	) error
	FinishRefuel(context.Context, RefuelRecord) error
	ActiveRefuel(context.Context) (RefuelRecord, bool, error)
	LastCompletedRefuel(
		context.Context,
		market.ChainID,
	) (RefuelRecord, bool, error)
}

type RefuelBalance struct {
	Chain      market.ChainID
	Native     market.AssetQuantity
	QuoteValue market.AssetQuantity
	ObservedAt time.Time
}

type RefuelExecutor interface {
	Chain() market.ChainID
	Balance(context.Context) (RefuelBalance, error)
	Preview(
		context.Context,
		market.AssetQuantity,
	) (RefuelRecord, error)
	Execute(
		context.Context,
		market.AssetQuantity,
		RefuelJournal,
	) (RefuelRecord, error)
	Reconcile(
		context.Context,
		RefuelRecord,
		RefuelJournal,
	) (RefuelRecord, error)
}
