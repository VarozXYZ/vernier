package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	domainexecution "github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/market"
	executionport "github.com/VarozXYZ/vernier/ports/execution"

	_ "modernc.org/sqlite"
)

// SequentialLiveStore is an operational journal. It stores economic intent,
// transaction identities, and confirmed settlements, but never signed
// payloads or provider artifacts.
type SequentialLiveStore struct {
	db   *sql.DB
	once sync.Once
	err  error
}

func OpenSequentialLive(path string) (*SequentialLiveStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("sequential Live SQLite path is required")
	}
	if directory := filepath.Dir(path); directory != "." && directory != "" {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return nil, fmt.Errorf("create sequential Live directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sequential Live SQLite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
		`CREATE TABLE IF NOT EXISTS sequential_live_operations (
			operation_id TEXT PRIMARY KEY,
			plan_id TEXT NOT NULL,
			opportunity_id TEXT NOT NULL,
			config_hash TEXT NOT NULL,
			state TEXT NOT NULL,
			current_stage INTEGER NOT NULL,
			current_token TEXT NOT NULL,
			current_units TEXT NOT NULL,
			last_error TEXT NOT NULL DEFAULT '',
			started_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS one_active_sequential_live_operation
			ON sequential_live_operations((1))
			WHERE state IN ('running', 'manual_intervention_required')`,
		`CREATE TABLE IF NOT EXISTS sequential_live_transactions (
			operation_id TEXT NOT NULL REFERENCES sequential_live_operations(operation_id),
			ordinal INTEGER NOT NULL,
			phase TEXT NOT NULL,
			chain_name TEXT NOT NULL,
			account_id TEXT NOT NULL,
			identity TEXT NOT NULL,
			nonce TEXT NOT NULL DEFAULT '',
			blockhash TEXT NOT NULL DEFAULT '',
			last_valid_block_height INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL,
			last_error TEXT NOT NULL DEFAULT '',
			prepared_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY(operation_id, ordinal, phase),
			UNIQUE(chain_name, identity)
		)`,
		`CREATE TABLE IF NOT EXISTS sequential_live_settlements (
			operation_id TEXT NOT NULL REFERENCES sequential_live_operations(operation_id),
			ordinal INTEGER NOT NULL,
			stage TEXT NOT NULL,
			input_token TEXT NOT NULL,
			input_units TEXT NOT NULL,
			output_token TEXT NOT NULL,
			output_units TEXT NOT NULL,
			source_identity TEXT NOT NULL,
			destination_identity TEXT NOT NULL DEFAULT '',
			evidence TEXT NOT NULL,
			observed_at TEXT NOT NULL,
			PRIMARY KEY(operation_id, ordinal)
		)`,
		`CREATE TABLE IF NOT EXISTS sequential_live_costs (
			operation_id TEXT NOT NULL REFERENCES sequential_live_operations(operation_id),
			ordinal INTEGER NOT NULL,
			component_index INTEGER NOT NULL,
			kind TEXT NOT NULL,
			chain_name TEXT NOT NULL,
			asset TEXT NOT NULL,
			amount_value TEXT NOT NULL,
			quote_asset TEXT NOT NULL,
			quote_value TEXT NOT NULL,
			included_in_output INTEGER NOT NULL,
			evidence TEXT NOT NULL,
			PRIMARY KEY(operation_id, ordinal, component_index)
		)`,
		`CREATE TABLE IF NOT EXISTS sequential_live_results (
			operation_id TEXT PRIMARY KEY REFERENCES sequential_live_operations(operation_id),
			final_token TEXT NOT NULL,
			final_units TEXT NOT NULL,
			cost_asset TEXT NOT NULL,
			cost_value TEXT NOT NULL,
			external_cost_value TEXT NOT NULL,
			gross_value TEXT NOT NULL,
			net_value TEXT NOT NULL,
			recorded_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS sequential_live_exit_decisions (
			operation_id TEXT PRIMARY KEY REFERENCES sequential_live_operations(operation_id),
			route TEXT NOT NULL,
			destination_output_token TEXT NOT NULL,
			destination_output_units TEXT NOT NULL,
			return_output_token TEXT NOT NULL DEFAULT '',
			return_output_units TEXT NOT NULL DEFAULT '',
			recovery_asset TEXT NOT NULL,
			destination_recovery TEXT NOT NULL,
			return_recovery TEXT NOT NULL DEFAULT '',
			safety_margin TEXT NOT NULL,
			destination_qualified INTEGER NOT NULL,
			cost_evidence_available INTEGER NOT NULL,
			evidence TEXT NOT NULL,
			decided_at TEXT NOT NULL
		)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("configure sequential Live SQLite: %w", err)
		}
	}
	return &SequentialLiveStore{db: db}, nil
}

func (s *SequentialLiveStore) RecordSequentialExitDecision(
	ctx context.Context,
	decision domainexecution.SequentialExitDecision,
) error {
	if err := decision.Validate(); err != nil {
		return err
	}
	destinationToken, destinationUnits := "", ""
	if !decision.DestinationOutput.IsZero() {
		destinationToken = string(decision.DestinationOutput.Token())
		destinationUnits = decision.DestinationOutput.Units().String()
	}
	returnToken, returnUnits, returnRecovery := "", "", ""
	if !decision.ReturnOutput.IsZero() {
		returnToken = string(decision.ReturnOutput.Token())
		returnUnits = decision.ReturnOutput.Units().String()
	}
	if decision.ReturnRecovery.Asset() != "" {
		returnRecovery = decision.ReturnRecovery.String()
	}
	qualified, costsAvailable := 0, 0
	if decision.DestinationQualified {
		qualified = 1
	}
	if decision.CostEvidenceAvailable {
		costsAvailable = 1
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO sequential_live_exit_decisions (
		operation_id, route, destination_output_token,
		destination_output_units, return_output_token, return_output_units,
		recovery_asset, destination_recovery, return_recovery,
		safety_margin, destination_qualified, cost_evidence_available,
		evidence, decided_at
	) SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		WHERE EXISTS (
			SELECT 1 FROM sequential_live_operations
			WHERE operation_id=? AND state='running' AND current_stage=2
		)`,
		decision.Operation, decision.Route,
		destinationToken,
		destinationUnits,
		returnToken, returnUnits,
		decision.DestinationRecovery.Asset(),
		decision.DestinationRecovery.String(),
		returnRecovery, decision.SafetyMargin.String(),
		qualified, costsAvailable, decision.Evidence,
		decision.DecidedAt.UTC().Format(time.RFC3339Nano),
		decision.Operation,
	)
	if err != nil {
		return fmt.Errorf("persist sequential exit decision: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf(
			"bridged sequential operation was not found for exit decision",
		)
	}
	return nil
}

func (s *SequentialLiveStore) RecordSequentialResult(
	ctx context.Context,
	result executionport.SequentialResult,
) error {
	if result.Operation == "" || result.FinalAmount.IsZero() ||
		len(result.Settlements) != 4 ||
		result.ExecutionCost.Asset() == "" ||
		result.ExternalCost.Asset() != result.ExecutionCost.Asset() ||
		result.RealizedGross.Asset() != result.ExecutionCost.Asset() ||
		result.RealizedNetPnL.Asset() != result.ExecutionCost.Asset() {
		return fmt.Errorf("sequential Live result is incomplete")
	}
	inserted, err := s.db.ExecContext(ctx, `INSERT INTO sequential_live_results (
		operation_id, final_token, final_units, cost_asset, cost_value,
		external_cost_value, gross_value, net_value, recorded_at
	) SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
		WHERE EXISTS (
			SELECT 1 FROM sequential_live_operations
			WHERE operation_id=? AND state='running' AND current_stage=4
		)`,
		result.Operation, result.FinalAmount.Token(),
		result.FinalAmount.Units().String(), result.ExecutionCost.Asset(),
		result.ExecutionCost.String(), result.ExternalCost.String(),
		result.RealizedGross.String(), result.RealizedNetPnL.String(),
		time.Now().UTC().Format(time.RFC3339Nano), result.Operation,
	)
	if err != nil {
		return fmt.Errorf("persist sequential Live result: %w", err)
	}
	affected, _ := inserted.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("completed sequential stage chain was not found")
	}
	return nil
}

func (s *SequentialLiveStore) CreateSequentialOperation(
	ctx context.Context,
	operation domainexecution.SequentialOperation,
) error {
	if operation.ID == "" || operation.Plan == "" ||
		operation.OpportunityID == "" || operation.ConfigHash == "" ||
		operation.State != domainexecution.SequentialRunning ||
		operation.CurrentStage != 0 || operation.CurrentAmount.IsZero() ||
		operation.StartedAt.IsZero() {
		return fmt.Errorf("sequential Live operation is incomplete")
	}
	started := operation.StartedAt.UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `INSERT INTO sequential_live_operations (
		operation_id, plan_id, opportunity_id, config_hash, state,
		current_stage, current_token, current_units, started_at, updated_at
	) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?)`,
		operation.ID, operation.Plan, operation.OpportunityID, operation.ConfigHash,
		operation.State, operation.CurrentAmount.Token(),
		operation.CurrentAmount.Units().String(), started, started,
	)
	if err != nil {
		return fmt.Errorf("create sequential Live operation: %w", err)
	}
	return nil
}

func (s *SequentialLiveStore) RecordPreparedTransaction(
	ctx context.Context,
	prepared executionport.PreparedTransaction,
) error {
	if err := prepared.Validate(); err != nil {
		return err
	}
	nonce := ""
	if prepared.Identity.Nonce != nil {
		nonce = strconv.FormatUint(*prepared.Identity.Nonce, 10)
	}
	at := prepared.PreparedAt.UTC().Format(time.RFC3339Nano)
	result, err := s.db.ExecContext(ctx, `INSERT INTO sequential_live_transactions (
		operation_id, ordinal, phase, chain_name, account_id, identity, nonce,
		blockhash, last_valid_block_height, status, prepared_at, updated_at
	) SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, 'prepared', ?, ?
		WHERE EXISTS (
			SELECT 1 FROM sequential_live_operations
			WHERE operation_id=? AND state='running'
		)`,
		prepared.Operation, prepared.Ordinal, prepared.Phase,
		prepared.Identity.Chain, prepared.Identity.Account,
		prepared.Identity.Hash, nonce, prepared.Identity.Blockhash,
		prepared.Identity.LastValidBlockHeight, at, at, prepared.Operation,
	)
	if err != nil {
		return fmt.Errorf("persist prepared sequential transaction: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("running sequential operation was not found")
	}
	return nil
}

func (s *SequentialLiveStore) MarkTransaction(
	ctx context.Context,
	operationID domainexecution.OperationID,
	ordinal int,
	phase, status string,
) error {
	switch status {
	case "broadcast", "confirmed", "rejected", "outcome_unknown":
	default:
		return fmt.Errorf("invalid sequential transaction status %q", status)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE sequential_live_transactions
		SET status=?, updated_at=?
		WHERE operation_id=? AND ordinal=? AND phase=?`,
		status, time.Now().UTC().Format(time.RFC3339Nano),
		operationID, ordinal, phase,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("sequential transaction was not found")
	}
	return nil
}

func (s *SequentialLiveStore) RecordStageSettlement(
	ctx context.Context,
	settlement domainexecution.SequentialStageSettlement,
) error {
	if err := settlement.Validate(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var state, currentToken, currentUnits string
	var currentStage int
	err = tx.QueryRowContext(ctx, `SELECT state, current_stage, current_token, current_units
		FROM sequential_live_operations WHERE operation_id=?`,
		settlement.Request.Operation,
	).Scan(&state, &currentStage, &currentToken, &currentUnits)
	if err != nil {
		return err
	}
	if state != string(domainexecution.SequentialRunning) ||
		currentStage+1 != settlement.Request.Stage.Ordinal ||
		currentToken != string(settlement.Request.Input.Token()) ||
		currentUnits != settlement.Request.Input.Units().String() {
		return fmt.Errorf("sequential settlement does not extend the durable operation state")
	}
	destination := ""
	if settlement.DestinationIdentity != nil {
		destination = settlement.DestinationIdentity.Hash
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO sequential_live_settlements (
		operation_id, ordinal, stage, input_token, input_units, output_token,
		output_units, source_identity, destination_identity, evidence, observed_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		settlement.Request.Operation, settlement.Request.Stage.Ordinal,
		settlement.Request.Stage.Stage, settlement.ActualInput.Token(),
		settlement.ActualInput.Units().String(), settlement.ActualOutput.Token(),
		settlement.ActualOutput.Units().String(), settlement.SourceIdentity.Hash,
		destination, settlement.Evidence,
		settlement.ObservedAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return err
	}
	for index, cost := range settlement.Costs {
		quoteAsset := ""
		quoteValue := ""
		if cost.QuoteValue.Asset() != "" {
			quoteAsset = string(cost.QuoteValue.Asset())
			quoteValue = cost.QuoteValue.String()
		}
		included := 0
		if cost.IncludedInOutput {
			included = 1
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO sequential_live_costs (
			operation_id, ordinal, component_index, kind, chain_name,
			asset, amount_value, quote_asset, quote_value,
			included_in_output, evidence
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			settlement.Request.Operation, settlement.Request.Stage.Ordinal,
			index, cost.Kind, cost.Chain, cost.Amount.Asset(),
			cost.Amount.String(), quoteAsset, quoteValue, included,
			cost.Evidence,
		); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE sequential_live_operations
		SET current_stage=?, current_token=?, current_units=?, updated_at=?
		WHERE operation_id=? AND state='running'`,
		settlement.Request.Stage.Ordinal, settlement.ActualOutput.Token(),
		settlement.ActualOutput.Units().String(),
		settlement.ObservedAt.UTC().Format(time.RFC3339Nano),
		settlement.Request.Operation,
	)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SequentialLiveStore) FinishSequentialOperation(
	ctx context.Context,
	operationID domainexecution.OperationID,
	state domainexecution.SequentialOperationState,
	cause error,
) error {
	switch state {
	case domainexecution.SequentialCompleted,
		domainexecution.SequentialAborted,
		domainexecution.SequentialManualIntervention:
	default:
		return fmt.Errorf("invalid terminal sequential operation state")
	}
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	result, err := s.db.ExecContext(ctx, `UPDATE sequential_live_operations
		SET state=?, last_error=?, updated_at=?
		WHERE operation_id=? AND state='running'`,
		state, message, time.Now().UTC().Format(time.RFC3339Nano), operationID,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("running sequential operation was not found")
	}
	return nil
}

func (s *SequentialLiveStore) ActiveSequentialOperation(
	ctx context.Context,
) (domainexecution.SequentialOperation, bool, error) {
	var operation domainexecution.SequentialOperation
	var token, units, started, updated string
	err := s.db.QueryRowContext(ctx, `SELECT operation_id, plan_id,
		opportunity_id, config_hash, state, current_stage, current_token,
		current_units, last_error, started_at, updated_at
		FROM sequential_live_operations
		WHERE state IN ('running', 'manual_intervention_required')
		ORDER BY started_at LIMIT 1`,
	).Scan(
		&operation.ID, &operation.Plan, &operation.OpportunityID,
		&operation.ConfigHash, &operation.State, &operation.CurrentStage,
		&token, &units, &operation.LastError, &started, &updated,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return domainexecution.SequentialOperation{}, false, nil
	}
	if err != nil {
		return domainexecution.SequentialOperation{}, false, err
	}
	operation.CurrentAmount, err = market.ParseTokenAmount(market.TokenID(token), units)
	if err != nil {
		return domainexecution.SequentialOperation{}, false, err
	}
	operation.StartedAt, err = time.Parse(time.RFC3339Nano, started)
	if err != nil {
		return domainexecution.SequentialOperation{}, false, err
	}
	operation.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated)
	if err != nil {
		return domainexecution.SequentialOperation{}, false, err
	}
	return operation, true, nil
}

// AcknowledgeManualReconciliation closes an operational barrier only after an
// operator has reconciled the real balances and transactions outside the
// automatic saga. It preserves the original failure and appends the audit note.
func (s *SequentialLiveStore) AcknowledgeManualReconciliation(
	ctx context.Context,
	operationID domainexecution.OperationID,
) error {
	if strings.TrimSpace(string(operationID)) == "" {
		return fmt.Errorf("reconciled operation ID is required")
	}
	const note = "manual reconciliation acknowledged by operator"
	result, err := s.db.ExecContext(ctx, `UPDATE sequential_live_operations
		SET state=?,
			last_error=CASE
				WHEN last_error='' THEN ?
				ELSE last_error || ' | ' || ?
			END,
			updated_at=?
		WHERE operation_id=? AND state=?`,
		domainexecution.SequentialReconciledManually,
		note,
		note,
		time.Now().UTC().Format(time.RFC3339Nano),
		operationID,
		domainexecution.SequentialManualIntervention,
	)
	if err != nil {
		return fmt.Errorf("acknowledge manual reconciliation: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf(
			"operation %s is not awaiting manual reconciliation",
			operationID,
		)
	}
	return nil
}

func (s *SequentialLiveStore) Close() error {
	s.once.Do(func() { s.err = s.db.Close() })
	return s.err
}
