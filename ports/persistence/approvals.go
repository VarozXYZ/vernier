package persistence

import (
	"context"
	"math/big"
	"time"

	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
)

type ApprovalRecord struct {
	ID        string
	Chain     market.ChainID
	Token     market.TokenID
	Spender   string
	Amount    *big.Int
	Identity  execution.TransactionIdentity
	State     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type ApprovalJournal interface {
	RecordApproval(context.Context, ApprovalRecord) error
	SetApprovalState(context.Context, string, string, time.Time) error
	LoadApprovalRecovery(context.Context) ([]ApprovalRecord, error)
}
