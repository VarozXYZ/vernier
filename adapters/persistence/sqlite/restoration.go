package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	persistenceport "github.com/VarozXYZ/vernier/ports/persistence"
)

const maxActiveQuoteRestorations = 2

func (s *SequentialLiveStore) LoadRestoration(ctx context.Context) (persistenceport.RestorationState, error) {
	var result persistenceport.RestorationState
	var pending int
	var operation string
	err := s.db.QueryRowContext(ctx, `SELECT operation_id, pending FROM parallel_base_restoration WHERE singleton = 1`).Scan(&operation, &pending)
	if err != nil && err != sql.ErrNoRows {
		return result, err
	}
	if err == nil {
		result.BaseOperation, result.BasePending = execution.OperationID(operation), pending != 0
	}
	rows, err := s.db.QueryContext(ctx, `SELECT job_id, operation_id, state, source_chain, destination_chain,
		input_token, output_token, input_units, created_at, updated_at
		FROM parallel_quote_restorations WHERE state IN ('pending', 'broadcast', 'outcome_unknown') ORDER BY created_at`)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var job persistenceport.QuoteRestorationJob
		var created, updated string
		var units string
		if err := rows.Scan(&job.ID, &job.Operation, &job.State, &job.SourceChain, &job.DestinationChain,
			&job.InputToken, &job.OutputToken, &units, &created, &updated); err != nil {
			return result, err
		}
		if units != "" {
			job.InputUnits, _ = new(big.Int).SetString(units, 10)
		}
		job.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
		if err != nil {
			return result, err
		}
		job.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated)
		if err != nil {
			return result, err
		}
		result.QuoteJobs = append(result.QuoteJobs, job)
	}
	if err := rows.Err(); err != nil {
		return result, err
	}
	var trigger arbitrage.TriggerMetadata
	var at string
	err = s.db.QueryRowContext(ctx, `SELECT market_id, source_id, position_kind, position_value,
		reference_kind, reference_value, triggered_at FROM parallel_reevaluation_coalescer WHERE singleton = 1`).Scan(
		&trigger.Market, &trigger.Source, &trigger.Position.Kind, &trigger.Position.Value,
		&trigger.Reference.Kind, &trigger.Reference.Value, &at)
	if err == nil {
		trigger.At, err = time.Parse(time.RFC3339Nano, at)
		if err != nil {
			return result, err
		}
		result.Reevaluation = &trigger
	} else if err != sql.ErrNoRows {
		return result, err
	}
	return result, nil
}

func (s *SequentialLiveStore) SetBaseRestoration(ctx context.Context, operation execution.OperationID, pending bool) error {
	if pending && operation == "" {
		return fmt.Errorf("pending base restoration requires an operation")
	}
	value := 0
	if pending {
		value = 1
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO parallel_base_restoration(singleton, operation_id, pending, updated_at)
		VALUES(1, ?, ?, ?) ON CONFLICT(singleton) DO UPDATE SET operation_id=excluded.operation_id,
		pending=excluded.pending, updated_at=excluded.updated_at`, operation, value, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (s *SequentialLiveStore) StartQuoteRestoration(ctx context.Context, job persistenceport.QuoteRestorationJob) error {
	if job.ID == "" || job.Operation == "" || job.State == "" || job.SourceChain == "" || job.DestinationChain == "" ||
		job.InputToken == "" || job.OutputToken == "" || job.InputUnits == nil || job.InputUnits.Sign() <= 0 || job.CreatedAt.IsZero() {
		return fmt.Errorf("quote restoration job is incomplete")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var active int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM parallel_quote_restorations
		WHERE state IN ('pending', 'broadcast', 'outcome_unknown') AND job_id <> ?`, job.ID).Scan(&active); err != nil {
		return err
	}
	if active >= maxActiveQuoteRestorations {
		return fmt.Errorf("quote restoration capacity is exhausted")
	}
	updated := job.UpdatedAt
	if updated.IsZero() {
		updated = job.CreatedAt
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO parallel_quote_restorations(job_id, operation_id, state, source_chain,
		destination_chain, input_token, output_token, input_units, created_at, updated_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(job_id) DO UPDATE SET state=excluded.state, updated_at=excluded.updated_at`,
		job.ID, job.Operation, job.State, job.SourceChain, job.DestinationChain, job.InputToken, job.OutputToken,
		job.InputUnits.String(), job.CreatedAt.UTC().Format(time.RFC3339Nano), updated.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SequentialLiveStore) FinishQuoteRestoration(ctx context.Context, id, state string, at time.Time) error {
	if id == "" || state == "" || at.IsZero() {
		return fmt.Errorf("quote restoration completion is incomplete")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE parallel_quote_restorations SET state=?, updated_at=? WHERE job_id=?`,
		state, at.UTC().Format(time.RFC3339Nano), id)
	if err != nil {
		return err
	}
	changed, _ := result.RowsAffected()
	if changed != 1 {
		return fmt.Errorf("quote restoration job %q does not exist", id)
	}
	return nil
}

func (s *SequentialLiveStore) CoalesceReevaluation(ctx context.Context, trigger arbitrage.TriggerMetadata) error {
	if err := trigger.Validate(); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO parallel_reevaluation_coalescer(singleton, market_id, source_id,
		position_kind, position_value, reference_kind, reference_value, triggered_at, updated_at)
		VALUES(1, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(singleton) DO UPDATE SET market_id=excluded.market_id,
		source_id=excluded.source_id, position_kind=excluded.position_kind, position_value=excluded.position_value,
		reference_kind=excluded.reference_kind, reference_value=excluded.reference_value,
		triggered_at=excluded.triggered_at, updated_at=excluded.updated_at`, string(trigger.Market), string(trigger.Source),
		trigger.Position.Kind, trigger.Position.Value, trigger.Reference.Kind, trigger.Reference.Value,
		trigger.At.UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (s *SequentialLiveStore) ClearReevaluation(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM parallel_reevaluation_coalescer WHERE singleton=1`)
	return err
}

var _ persistenceport.RestorationJournal = (*SequentialLiveStore)(nil)
