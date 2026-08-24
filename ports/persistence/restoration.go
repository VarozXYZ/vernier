package persistence

import (
	"context"
	"math/big"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
)

type QuoteRestorationJob struct {
	ID               string
	Operation        execution.OperationID
	State            string
	SourceChain      market.ChainID
	DestinationChain market.ChainID
	InputToken       market.TokenID
	OutputToken      market.TokenID
	InputUnits       *big.Int
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type RestorationState struct {
	BaseOperation execution.OperationID
	BasePending   bool
	QuoteJobs     []QuoteRestorationJob
	Reevaluation  *arbitrage.TriggerMetadata
}

// RestorationJournal persists only normalized identities and trigger
// metadata. Bridge payloads, calldata, provider responses, and signed bytes do
// not cross this boundary.
type RestorationJournal interface {
	LoadRestoration(context.Context) (RestorationState, error)
	SetBaseRestoration(context.Context, execution.OperationID, bool) error
	StartQuoteRestoration(context.Context, QuoteRestorationJob) error
	FinishQuoteRestoration(context.Context, string, string, time.Time) error
	CoalesceReevaluation(context.Context, arbitrage.TriggerMetadata) error
	ClearReevaluation(context.Context) error
}
