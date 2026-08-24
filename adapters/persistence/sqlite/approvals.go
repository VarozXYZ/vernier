package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"time"

	persistenceport "github.com/VarozXYZ/vernier/ports/persistence"
)

func (s *SequentialLiveStore) LoadApprovalRecovery(ctx context.Context) ([]persistenceport.ApprovalRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, chain_id, token_id, spender, amount_units,
		tx_hash, tx_nonce, state, created_at, updated_at FROM live_approvals
		WHERE state IN ('prepared','broadcast','outcome_unknown','confirmed_revert') ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []persistenceport.ApprovalRecord
	for rows.Next() {
		var record persistenceport.ApprovalRecord
		var units, created, updated string
		var nonce sql.NullInt64
		if err := rows.Scan(&record.ID, &record.Chain, &record.Token, &record.Spender, &units,
			&record.Identity.Hash, &nonce, &record.State, &created, &updated); err != nil {
			return nil, err
		}
		record.Amount, _ = new(big.Int).SetString(units, 10)
		if nonce.Valid {
			value := uint64(nonce.Int64)
			record.Identity.Nonce = &value
		}
		record.Identity.Chain = record.Chain
		record.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
		if err != nil {
			return nil, err
		}
		record.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (s *SequentialLiveStore) RecordApproval(ctx context.Context, record persistenceport.ApprovalRecord) error {
	if record.ID == "" || record.Chain == "" || record.Token == "" || record.Spender == "" ||
		record.Amount == nil || record.Amount.Sign() < 0 || record.Identity.Hash == "" ||
		record.State == "" || record.CreatedAt.IsZero() || record.UpdatedAt.IsZero() {
		return fmt.Errorf("approval record is incomplete")
	}
	var nonce any
	if record.Identity.Nonce != nil {
		nonce = int64(*record.Identity.Nonce)
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO live_approvals(
 id, chain_id, token_id, spender, amount_units, tx_hash, tx_nonce, state, created_at, updated_at
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, record.ID, record.Chain, record.Token,
		record.Spender, record.Amount.String(), record.Identity.Hash, nonce, record.State,
		record.CreatedAt.UTC().Format(time.RFC3339Nano), record.UpdatedAt.UTC().Format(time.RFC3339Nano))
	return err
}

func (s *SequentialLiveStore) SetApprovalState(ctx context.Context, id, state string, at time.Time) error {
	if id == "" || state == "" || at.IsZero() {
		return fmt.Errorf("approval state update is incomplete")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE live_approvals SET state = ?, updated_at = ? WHERE id = ?`,
		state, at.UTC().Format(time.RFC3339Nano), id)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return fmt.Errorf("approval record %q is unavailable", id)
	}
	return nil
}
