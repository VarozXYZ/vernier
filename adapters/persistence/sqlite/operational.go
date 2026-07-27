package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/execution"
	"github.com/VarozXYZ/vernier/domain/inventory"
	"github.com/VarozXYZ/vernier/domain/market"
	persistenceport "github.com/VarozXYZ/vernier/ports/persistence"
	_ "modernc.org/sqlite"
)

const operationalSchemaVersion = 3

type OperationalStore struct {
	db   *sql.DB
	once sync.Once
	err  error
}

// OpenOperational opens a database dedicated to Live state. synchronous must
// be FULL or NORMAL; Live configuration defaults to FULL.
func OpenOperational(path, synchronous string) (*OperationalStore, error) {
	path = strings.TrimSpace(path)
	synchronous = strings.ToUpper(strings.TrimSpace(synchronous))
	if path == "" {
		return nil, fmt.Errorf("operational SQLite path is required")
	}
	if synchronous == "" {
		synchronous = "FULL"
	}
	if synchronous != "FULL" && synchronous != "NORMAL" {
		return nil, fmt.Errorf("operational SQLite synchronous mode must be FULL or NORMAL")
	}
	directory := filepath.Dir(path)
	if directory != "." && directory != "" {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return nil, fmt.Errorf("create operational store directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open operational store: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &OperationalStore{db: db}
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=" + synchronous,
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("configure operational SQLite (%s): %w", statement, err)
		}
	}
	if err := store.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *OperationalStore) migrate() error {
	var version int
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version > operationalSchemaVersion {
		return fmt.Errorf("unsupported operational SQLite schema version %d", version)
	}
	if version == operationalSchemaVersion {
		return nil
	}
	if version == 1 {
		if err := s.migrateV1ToV2(); err != nil {
			return err
		}
		version = 2
	}
	if version == 2 {
		return s.migrateV2ToV3()
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, statement := range []string{
		`CREATE TABLE operations (
			operation_id TEXT PRIMARY KEY,
			plan_id TEXT NOT NULL,
			opportunity_id TEXT NOT NULL,
			config_hash TEXT NOT NULL,
			technical_state TEXT NOT NULL,
			economic_state TEXT NOT NULL,
			created_at TEXT NOT NULL,
			committed_at TEXT NOT NULL,
			manual_reason TEXT NOT NULL DEFAULT '',
			valuation_version INTEGER NOT NULL,
			valuation_base_asset TEXT NOT NULL,
			valuation_quote_asset TEXT NOT NULL,
			valuation_price TEXT NOT NULL,
			valuation_observations INTEGER NOT NULL,
			valuation_captured_at TEXT NOT NULL,
			quote_delta_asset TEXT NOT NULL,
			quote_delta_value TEXT NOT NULL,
			base_delta_asset TEXT NOT NULL,
			base_delta_value TEXT NOT NULL,
			marked_base_asset TEXT NOT NULL,
			marked_base_value TEXT NOT NULL,
			gross_pnl_asset TEXT NOT NULL,
			gross_pnl_value TEXT NOT NULL,
			execution_cost_asset TEXT NOT NULL,
			execution_cost_value TEXT NOT NULL,
			net_pnl_asset TEXT NOT NULL,
			net_pnl_value TEXT NOT NULL,
			threshold_asset TEXT NOT NULL,
			threshold_value TEXT NOT NULL,
			discovered_at TEXT NOT NULL,
			validated_at TEXT NOT NULL
		)`,
		`CREATE TABLE operation_steps (
			operation_id TEXT NOT NULL REFERENCES operations(operation_id),
			step_id TEXT NOT NULL,
			side TEXT NOT NULL,
			chain_id TEXT NOT NULL,
			account_id TEXT NOT NULL,
			market_id TEXT NOT NULL,
			input_token TEXT NOT NULL,
			input_units TEXT NOT NULL,
			expected_output_token TEXT NOT NULL,
			expected_output_units TEXT NOT NULL,
			tx_hash TEXT NOT NULL,
			nonce TEXT NOT NULL DEFAULT '',
			blockhash TEXT NOT NULL DEFAULT '',
			last_valid_block_height INTEGER NOT NULL DEFAULT 0,
			route_allocation_json TEXT NOT NULL DEFAULT '',
			technical_state TEXT NOT NULL,
			economic_state TEXT NOT NULL,
			broadcast_detail TEXT NOT NULL DEFAULT '',
			actual_input_token TEXT NOT NULL DEFAULT '',
			actual_input_units TEXT NOT NULL DEFAULT '',
			actual_output_token TEXT NOT NULL DEFAULT '',
			actual_output_units TEXT NOT NULL DEFAULT '',
			settlement_evidence TEXT NOT NULL DEFAULT '',
			settled_at TEXT NOT NULL DEFAULT '',
			PRIMARY KEY(operation_id, step_id)
		)`,
		`CREATE TABLE inventory_reservations (
			operation_id TEXT NOT NULL REFERENCES operations(operation_id),
			reservation_id TEXT NOT NULL,
			chain_id TEXT NOT NULL,
			account_id TEXT NOT NULL,
			token_id TEXT NOT NULL,
			units TEXT NOT NULL,
			state TEXT NOT NULL,
			created_at TEXT NOT NULL,
			resolved_at TEXT NOT NULL DEFAULT '',
			resolution_reason TEXT NOT NULL DEFAULT '',
			PRIMARY KEY(operation_id, reservation_id, chain_id, account_id, token_id)
		)`,
		`CREATE TABLE operation_events (
			sequence INTEGER PRIMARY KEY AUTOINCREMENT,
			operation_id TEXT NOT NULL REFERENCES operations(operation_id),
			step_id TEXT NOT NULL DEFAULT '',
			event_kind TEXT NOT NULL,
			technical_state TEXT NOT NULL DEFAULT '',
			economic_state TEXT NOT NULL DEFAULT '',
			detail TEXT NOT NULL DEFAULT '',
			evidence TEXT NOT NULL DEFAULT '',
			actual_input_token TEXT NOT NULL DEFAULT '',
			actual_input_units TEXT NOT NULL DEFAULT '',
			actual_output_token TEXT NOT NULL DEFAULT '',
			actual_output_units TEXT NOT NULL DEFAULT '',
			occurred_at TEXT NOT NULL,
			dedupe_key TEXT NOT NULL UNIQUE
		)`,
		`CREATE INDEX operation_pending_idx ON operations(technical_state, committed_at)`,
		`CREATE INDEX operation_events_operation_idx ON operation_events(operation_id, sequence)`,
	} {
		if _, err := tx.Exec(statement); err != nil {
			return fmt.Errorf("apply operational migration: %w", err)
		}
	}
	if _, err := tx.Exec("PRAGMA user_version = 3"); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *OperationalStore) migrateV2ToV3() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, statement := range []string{
		`ALTER TABLE inventory_reservations ADD COLUMN state TEXT NOT NULL DEFAULT 'reserved'`,
		`ALTER TABLE inventory_reservations ADD COLUMN created_at TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE inventory_reservations ADD COLUMN resolved_at TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE inventory_reservations ADD COLUMN resolution_reason TEXT NOT NULL DEFAULT ''`,
		`UPDATE inventory_reservations
		 SET created_at = (SELECT committed_at FROM operations
		                   WHERE operations.operation_id = inventory_reservations.operation_id)`,
		`UPDATE inventory_reservations
		 SET state = CASE
		   WHEN (SELECT economic_state FROM operations
		         WHERE operations.operation_id = inventory_reservations.operation_id) = 'settled'
		     THEN 'settled'
		   WHEN (SELECT economic_state FROM operations
		         WHERE operations.operation_id = inventory_reservations.operation_id) = 'released'
		     THEN 'released'
		   ELSE 'reserved'
		 END`,
		`PRAGMA user_version = 3`,
	} {
		if _, err := tx.Exec(statement); err != nil {
			return fmt.Errorf("migrate operational SQLite v2 to v3: %w", err)
		}
	}
	return tx.Commit()
}

func (s *OperationalStore) migrateV1ToV2() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, statement := range []string{
		`ALTER TABLE operation_steps ADD COLUMN route_allocation_json TEXT NOT NULL DEFAULT ''`,
		`CREATE TABLE operation_events (
			sequence INTEGER PRIMARY KEY AUTOINCREMENT,
			operation_id TEXT NOT NULL REFERENCES operations(operation_id),
			step_id TEXT NOT NULL DEFAULT '',
			event_kind TEXT NOT NULL,
			technical_state TEXT NOT NULL DEFAULT '',
			economic_state TEXT NOT NULL DEFAULT '',
			detail TEXT NOT NULL DEFAULT '',
			evidence TEXT NOT NULL DEFAULT '',
			actual_input_token TEXT NOT NULL DEFAULT '',
			actual_input_units TEXT NOT NULL DEFAULT '',
			actual_output_token TEXT NOT NULL DEFAULT '',
			actual_output_units TEXT NOT NULL DEFAULT '',
			occurred_at TEXT NOT NULL,
			dedupe_key TEXT NOT NULL UNIQUE
		)`,
		`CREATE INDEX operation_events_operation_idx ON operation_events(operation_id, sequence)`,
		`INSERT INTO operation_events (
			operation_id, event_kind, technical_state, economic_state, detail, occurred_at, dedupe_key
		)
		SELECT operation_id, 'operation_committed', technical_state, economic_state,
			'imported from schema v1', committed_at, 'schema-v1:' || operation_id
		FROM operations`,
		`PRAGMA user_version = 2`,
	} {
		if _, err := tx.Exec(statement); err != nil {
			return fmt.Errorf("migrate operational SQLite v1 to v2: %w", err)
		}
	}
	return tx.Commit()
}

func (s *OperationalStore) CommitPrepared(ctx context.Context, operation execution.Operation, reservation inventory.Reservation) error {
	if err := operation.ValidatePrepared(); err != nil {
		return err
	}
	if reservation.Operation != operation.ID || reservation.ID == "" {
		return fmt.Errorf("operation reservation does not match prepared operation")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin prepared operation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var existing string
	err = tx.QueryRowContext(ctx, "SELECT config_hash FROM operations WHERE operation_id = ?", string(operation.ID)).Scan(&existing)
	if err == nil {
		if existing != operation.ConfigHash {
			return fmt.Errorf("operation %q already exists with different data", operation.ID)
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	economics := operation.Economics
	_, err = tx.ExecContext(ctx, `INSERT INTO operations (
		operation_id, plan_id, opportunity_id, config_hash, technical_state, economic_state, created_at, committed_at,
		valuation_version, valuation_base_asset, valuation_quote_asset, valuation_price,
		valuation_observations, valuation_captured_at,
		quote_delta_asset, quote_delta_value, base_delta_asset, base_delta_value,
		marked_base_asset, marked_base_value, gross_pnl_asset, gross_pnl_value,
		execution_cost_asset, execution_cost_value, net_pnl_asset, net_pnl_value,
		threshold_asset, threshold_value, discovered_at, validated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(operation.ID), string(operation.Plan), operation.OpportunityID, operation.ConfigHash,
		string(operation.Technical), string(operation.Economic), formatOperationalTime(operation.CreatedAt),
		formatOperationalTime(operation.CommittedAt),
		economics.Valuation.Version(), string(economics.Valuation.Base()), string(economics.Valuation.Quote()),
		economics.Valuation.Price().RatString(), economics.Valuation.Observations(),
		formatOperationalTime(economics.Valuation.CapturedAt()),
		string(economics.QuoteDelta.Asset()), economics.QuoteDelta.String(),
		string(economics.BaseDelta.Asset()), economics.BaseDelta.String(),
		string(economics.MarkedBase.Asset()), economics.MarkedBase.String(),
		string(economics.GrossPnL.Asset()), economics.GrossPnL.String(),
		string(economics.Cost.Asset()), economics.Cost.String(),
		string(economics.NetPnL.Asset()), economics.NetPnL.String(),
		string(economics.Threshold.Asset()), economics.Threshold.String(),
		formatOperationalTime(economics.DiscoveredAt), formatOperationalTime(economics.ValidatedAt),
	)
	if err != nil {
		return fmt.Errorf("insert operation: %w", err)
	}
	for _, step := range operation.Steps {
		nonce := ""
		if step.Identity.Nonce != nil {
			nonce = strconv.FormatUint(*step.Identity.Nonce, 10)
		}
		allocation, err := encodeRouteAllocation(step.Allocation)
		if err != nil {
			return fmt.Errorf("encode operation step allocation: %w", err)
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO operation_steps (
			operation_id, step_id, side, chain_id, account_id, market_id,
			input_token, input_units, expected_output_token, expected_output_units,
			tx_hash, nonce, blockhash, last_valid_block_height, route_allocation_json,
			technical_state, economic_state
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			string(operation.ID), string(step.Leg.ID), string(step.Leg.Side), string(step.Leg.Chain),
			string(step.Leg.Account), string(step.Leg.Market), string(step.Leg.Input.Token()), step.Leg.Input.String(),
			string(step.Leg.ExpectedOutput.Token()), step.Leg.ExpectedOutput.String(), step.Identity.Hash, nonce,
			step.Identity.Blockhash, step.Identity.LastValidBlockHeight, allocation,
			string(step.Technical), string(step.Economic),
		)
		if err != nil {
			return fmt.Errorf("insert operation step: %w", err)
		}
	}
	for _, requirement := range reservation.Requirements() {
		_, err = tx.ExecContext(ctx, `INSERT INTO inventory_reservations (
			operation_id, reservation_id, chain_id, account_id, token_id, units, state, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			string(operation.ID), string(reservation.ID), string(requirement.Key.Chain),
			string(requirement.Key.Account), string(requirement.Key.Token), requirement.Amount.String(),
			"reserved", formatOperationalTime(operation.CommittedAt),
		)
		if err != nil {
			return fmt.Errorf("insert inventory reservation: %w", err)
		}
	}
	if _, err := appendOperationalEvent(ctx, tx, operationalEventRecord{
		operation: operation.ID, kind: execution.EventOperationCommitted,
		technical: operation.Technical, economic: operation.Economic,
		occurredAt: operation.CommittedAt,
	}); err != nil {
		return fmt.Errorf("append committed operation event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit prepared operation: %w", err)
	}
	return nil
}

func (s *OperationalStore) RecordBroadcast(ctx context.Context, operationID execution.OperationID, stepID execution.StepID, state execution.TechnicalState, detail string) error {
	if operationID == "" || stepID == "" ||
		state != execution.StateBroadcastPossible &&
			state != execution.StateBroadcastRejected &&
			state != execution.StateOutcomeUnknown {
		return fmt.Errorf("broadcast record is invalid")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var currentTechnical, currentEconomic string
	if err := tx.QueryRowContext(ctx, `SELECT technical_state, economic_state
		FROM operation_steps WHERE operation_id = ? AND step_id = ?`,
		string(operationID), string(stepID),
	).Scan(&currentTechnical, &currentEconomic); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("operation step was not found")
		}
		return err
	}
	if err := execution.ValidateTechnicalTransition(execution.TechnicalState(currentTechnical), state); err != nil {
		return err
	}
	inserted, err := appendOperationalEvent(ctx, tx, operationalEventRecord{
		operation: operationID, step: stepID, kind: execution.EventBroadcastObserved,
		technical: state, economic: execution.EconomicState(currentEconomic),
		detail: detail, occurredAt: time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	if !inserted {
		return nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE operation_steps SET technical_state = ?, broadcast_detail = ?
		WHERE operation_id = ? AND step_id = ?`, string(state), detail, string(operationID), string(stepID))
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("operation step was not found")
	}
	return tx.Commit()
}

func (s *OperationalStore) RecordSettlement(ctx context.Context, settlement execution.Settlement) error {
	if settlement.Operation == "" || settlement.Step == "" || settlement.ObservedAt.IsZero() ||
		settlement.Technical == "" || settlement.Economic == "" {
		return fmt.Errorf("settlement is incomplete")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var currentTechnical, currentEconomic string
	if err := tx.QueryRowContext(ctx, `SELECT technical_state, economic_state
		FROM operation_steps WHERE operation_id = ? AND step_id = ?`,
		string(settlement.Operation), string(settlement.Step),
	).Scan(&currentTechnical, &currentEconomic); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("operation step was not found")
		}
		return err
	}
	if err := execution.ValidateTechnicalTransition(
		execution.TechnicalState(currentTechnical), settlement.Technical,
	); err != nil {
		return err
	}
	if err := execution.ValidateEconomicTransition(
		execution.EconomicState(currentEconomic), settlement.Economic,
	); err != nil {
		return err
	}
	inserted, err := appendOperationalEvent(ctx, tx, operationalEventRecord{
		operation: settlement.Operation, step: settlement.Step, kind: execution.EventSettlementObserved,
		technical: settlement.Technical, economic: settlement.Economic,
		evidence: settlement.Evidence, actualIn: settlement.ActualIn, actualOut: settlement.ActualOut,
		occurredAt: settlement.ObservedAt,
	})
	if err != nil {
		return err
	}
	if !inserted {
		return nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE operation_steps SET
		technical_state = ?, economic_state = ?, actual_input_token = ?, actual_input_units = ?,
		actual_output_token = ?, actual_output_units = ?, settlement_evidence = ?, settled_at = ?
		WHERE operation_id = ? AND step_id = ?`,
		string(settlement.Technical), string(settlement.Economic), string(settlement.ActualIn.Token()),
		settlement.ActualIn.String(), string(settlement.ActualOut.Token()), settlement.ActualOut.String(),
		settlement.Evidence, formatOperationalTime(settlement.ObservedAt), string(settlement.Operation), string(settlement.Step),
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("operation step was not found")
	}
	var pending, failed, unverified int
	if err := tx.QueryRowContext(ctx, `SELECT
		SUM(CASE WHEN technical_state NOT IN (?, ?) THEN 1 ELSE 0 END),
		SUM(CASE WHEN technical_state = ? OR economic_state = ? THEN 1 ELSE 0 END),
		SUM(CASE WHEN economic_state != ? THEN 1 ELSE 0 END)
		FROM operation_steps WHERE operation_id = ?`,
		string(execution.StateConfirmedSuccess), string(execution.StateConfirmedRevert),
		string(execution.StateConfirmedRevert), string(execution.EconomicEffectMismatch),
		string(execution.EconomicEffectVerified),
		string(settlement.Operation),
	).Scan(&pending, &failed, &unverified); err != nil {
		return err
	}
	if pending == 0 {
		technical := execution.StateConfirmedSuccess
		economic := execution.EconomicEffectVerified
		if failed > 0 || unverified > 0 {
			technical = execution.StateManualIntervention
			economic = execution.EconomicExposureOpen
			if _, err := appendOperationalEvent(ctx, tx, operationalEventRecord{
				operation: settlement.Operation, kind: execution.EventManualIntervention,
				technical: technical, economic: economic,
				detail: "derived from completed step settlements", occurredAt: settlement.ObservedAt,
			}); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE operations SET technical_state = ?, economic_state = ?
			WHERE operation_id = ?`, string(technical), string(economic), string(settlement.Operation)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *OperationalStore) MarkSettled(ctx context.Context, operationID execution.OperationID) error {
	if operationID == "" {
		return fmt.Errorf("inventory settlement requires operation identity")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var currentTechnical, currentEconomic string
	if err := tx.QueryRowContext(ctx, `SELECT technical_state, economic_state
		FROM operations WHERE operation_id = ?`, string(operationID),
	).Scan(&currentTechnical, &currentEconomic); err != nil {
		return err
	}
	if execution.TechnicalState(currentTechnical) != execution.StateConfirmedSuccess {
		return fmt.Errorf("inventory settlement requires confirmed successful operation")
	}
	if err := execution.ValidateEconomicTransition(
		execution.EconomicState(currentEconomic), execution.EconomicSettled,
	); err != nil {
		return err
	}
	now := time.Now().UTC()
	inserted, err := appendOperationalEvent(ctx, tx, operationalEventRecord{
		operation: operationID, kind: execution.EventOperationCompleted,
		technical: execution.StateConfirmedSuccess, economic: execution.EconomicSettled,
		detail: "observed effects applied to inventory", occurredAt: now,
	})
	if err != nil {
		return err
	}
	if !inserted {
		return nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE operations SET economic_state = ?
		WHERE operation_id = ?`, string(execution.EconomicSettled), string(operationID))
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("operation was not found")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE inventory_reservations
		SET state = 'settled', resolved_at = ?, resolution_reason = ?
		WHERE operation_id = ?`,
		formatOperationalTime(now), "observed effects applied to inventory", string(operationID),
	); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *OperationalStore) MarkManualIntervention(ctx context.Context, operationID execution.OperationID, reason string) error {
	if operationID == "" || strings.TrimSpace(reason) == "" {
		return fmt.Errorf("manual intervention requires operation and reason")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var currentTechnical, currentEconomic string
	if err := tx.QueryRowContext(ctx, `SELECT technical_state, economic_state
		FROM operations WHERE operation_id = ?`, string(operationID),
	).Scan(&currentTechnical, &currentEconomic); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("operation was not found")
		}
		return err
	}
	if err := execution.ValidateTechnicalTransition(
		execution.TechnicalState(currentTechnical), execution.StateManualIntervention,
	); err != nil {
		return err
	}
	if err := execution.ValidateEconomicTransition(
		execution.EconomicState(currentEconomic), execution.EconomicExposureOpen,
	); err != nil {
		return err
	}
	inserted, err := appendOperationalEvent(ctx, tx, operationalEventRecord{
		operation: operationID, kind: execution.EventManualIntervention,
		technical: execution.StateManualIntervention, economic: execution.EconomicExposureOpen,
		detail: reason, occurredAt: time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	if !inserted {
		return nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE operations SET technical_state = ?, economic_state = ?, manual_reason = ?
		WHERE operation_id = ?`, string(execution.StateManualIntervention), string(execution.EconomicExposureOpen), reason, string(operationID))
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("operation was not found")
	}
	return tx.Commit()
}

func (s *OperationalStore) MarkNoExecution(ctx context.Context, operationID execution.OperationID, reason string) error {
	if operationID == "" || strings.TrimSpace(reason) == "" {
		return fmt.Errorf("no-execution proof requires operation and reason")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var currentTechnical, currentEconomic string
	if err := tx.QueryRowContext(ctx, `SELECT technical_state, economic_state
		FROM operations WHERE operation_id = ?`, string(operationID),
	).Scan(&currentTechnical, &currentEconomic); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("operation was not found")
		}
		return err
	}
	if err := execution.ValidateTechnicalTransition(
		execution.TechnicalState(currentTechnical), execution.StateNoExecution,
	); err != nil {
		return err
	}
	if err := execution.ValidateEconomicTransition(
		execution.EconomicState(currentEconomic), execution.EconomicReleased,
	); err != nil {
		return err
	}
	inserted, err := appendOperationalEvent(ctx, tx, operationalEventRecord{
		operation: operationID, kind: execution.EventNoExecution,
		technical: execution.StateNoExecution, economic: execution.EconomicReleased,
		detail: reason, occurredAt: time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	if !inserted {
		return nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE operations SET technical_state = ?, economic_state = ?, manual_reason = ?
		WHERE operation_id = ?`, string(execution.StateNoExecution), string(execution.EconomicReleased), reason, string(operationID))
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("operation was not found")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE inventory_reservations
		SET state = 'released', resolved_at = ?, resolution_reason = ?
		WHERE operation_id = ?`,
		formatOperationalTime(time.Now().UTC()), reason, string(operationID),
	); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *OperationalStore) Pending(ctx context.Context) ([]execution.Operation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT operation_id, plan_id, opportunity_id, config_hash,
		technical_state, economic_state, created_at, committed_at,
		valuation_version, valuation_base_asset, valuation_quote_asset, valuation_price,
		valuation_observations, valuation_captured_at,
		quote_delta_asset, quote_delta_value, base_delta_asset, base_delta_value,
		marked_base_asset, marked_base_value, gross_pnl_asset, gross_pnl_value,
		execution_cost_asset, execution_cost_value, net_pnl_asset, net_pnl_value,
		threshold_asset, threshold_value, discovered_at, validated_at
		FROM operations
		WHERE technical_state != ?
		  AND NOT (technical_state = ? AND economic_state = ?)
		ORDER BY committed_at`,
		string(execution.StateNoExecution), string(execution.StateConfirmedSuccess),
		string(execution.EconomicSettled))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []execution.Operation
	for rows.Next() {
		var operation execution.Operation
		var id, plan, technical, economic, createdAt, committedAt string
		var valuationVersion uint64
		var valuationBase, valuationQuote, valuationPrice, valuationCapturedAt string
		var valuationObservations int
		var quoteDeltaAsset, quoteDeltaValue, baseDeltaAsset, baseDeltaValue string
		var markedBaseAsset, markedBaseValue, grossAsset, grossValue string
		var costAsset, costValue, netAsset, netValue, thresholdAsset, thresholdValue string
		var discoveredAt, validatedAt string
		if err := rows.Scan(
			&id, &plan, &operation.OpportunityID, &operation.ConfigHash, &technical, &economic,
			&createdAt, &committedAt, &valuationVersion, &valuationBase, &valuationQuote,
			&valuationPrice, &valuationObservations, &valuationCapturedAt,
			&quoteDeltaAsset, &quoteDeltaValue, &baseDeltaAsset, &baseDeltaValue,
			&markedBaseAsset, &markedBaseValue, &grossAsset, &grossValue,
			&costAsset, &costValue, &netAsset, &netValue, &thresholdAsset, &thresholdValue,
			&discoveredAt, &validatedAt,
		); err != nil {
			return nil, err
		}
		operation.ID = execution.OperationID(id)
		operation.Plan = execution.PlanID(plan)
		operation.Technical = execution.TechnicalState(technical)
		operation.Economic = execution.EconomicState(economic)
		operation.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
		if err != nil {
			return nil, err
		}
		operation.CommittedAt, err = time.Parse(time.RFC3339Nano, committedAt)
		if err != nil {
			return nil, err
		}
		operation.Economics, err = parseOperationEconomics(
			valuationVersion, valuationBase, valuationQuote, valuationPrice,
			valuationObservations, valuationCapturedAt,
			quoteDeltaAsset, quoteDeltaValue, baseDeltaAsset, baseDeltaValue,
			markedBaseAsset, markedBaseValue, grossAsset, grossValue,
			costAsset, costValue, netAsset, netValue, thresholdAsset, thresholdValue,
			discoveredAt, validatedAt,
		)
		if err != nil {
			return nil, err
		}
		result = append(result, operation)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for index := range result {
		result[index].Steps, err = s.steps(ctx, result[index].ID)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (s *OperationalStore) Reservation(ctx context.Context, operationID execution.OperationID) (inventory.Reservation, error) {
	if operationID == "" {
		return inventory.Reservation{}, fmt.Errorf("operation ID is required")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT reservation_id, chain_id, account_id, token_id, units
		FROM inventory_reservations WHERE operation_id = ?
		ORDER BY reservation_id, chain_id, account_id, token_id`, string(operationID))
	if err != nil {
		return inventory.Reservation{}, err
	}
	defer rows.Close()
	var (
		reservationID inventory.ReservationID
		requirements  []inventory.Requirement
	)
	for rows.Next() {
		var id, chain, account, token, units string
		if err := rows.Scan(&id, &chain, &account, &token, &units); err != nil {
			return inventory.Reservation{}, err
		}
		if reservationID == "" {
			reservationID = inventory.ReservationID(id)
		} else if reservationID != inventory.ReservationID(id) {
			return inventory.Reservation{}, fmt.Errorf("operation has multiple reservation identities")
		}
		amount, err := market.ParseTokenAmount(market.TokenID(token), units)
		if err != nil {
			return inventory.Reservation{}, err
		}
		requirements = append(requirements, inventory.Requirement{
			Key: inventory.Key{
				Chain: market.ChainID(chain), Account: execution.AccountID(account),
				Token: market.TokenID(token),
			},
			Amount: amount,
		})
	}
	if err := rows.Err(); err != nil {
		return inventory.Reservation{}, err
	}
	if reservationID == "" {
		return inventory.Reservation{}, fmt.Errorf("operation %q has no persisted reservation", operationID)
	}
	return inventory.NewReservation(reservationID, operationID, requirements)
}

func (s *OperationalStore) History(ctx context.Context, operationID execution.OperationID) ([]execution.OperationalEvent, error) {
	if operationID == "" {
		return nil, fmt.Errorf("operation ID is required")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT sequence, step_id, event_kind,
		technical_state, economic_state, detail, evidence, occurred_at
		FROM operation_events WHERE operation_id = ? ORDER BY sequence`, string(operationID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []execution.OperationalEvent
	for rows.Next() {
		var event execution.OperationalEvent
		var step, kind, technical, economic, occurredAt string
		if err := rows.Scan(
			&event.Sequence, &step, &kind, &technical, &economic,
			&event.Detail, &event.Evidence, &occurredAt,
		); err != nil {
			return nil, err
		}
		event.Operation = operationID
		event.Step = execution.StepID(step)
		event.Kind = execution.EventKind(kind)
		event.Technical = execution.TechnicalState(technical)
		event.Economic = execution.EconomicState(economic)
		event.OccurredAt, err = time.Parse(time.RFC3339Nano, occurredAt)
		if err != nil {
			return nil, err
		}
		result = append(result, event)
	}
	return result, rows.Err()
}

func parseOperationEconomics(
	valuationVersion uint64,
	valuationBase, valuationQuote, valuationPrice string,
	valuationObservations int,
	valuationCapturedAt string,
	quoteDeltaAsset, quoteDeltaValue, baseDeltaAsset, baseDeltaValue string,
	markedBaseAsset, markedBaseValue, grossAsset, grossValue string,
	costAsset, costValue, netAsset, netValue, thresholdAsset, thresholdValue string,
	discoveredAt, validatedAt string,
) (execution.OperationEconomics, error) {
	price, ok := new(big.Rat).SetString(valuationPrice)
	if !ok {
		return execution.OperationEconomics{}, fmt.Errorf("invalid persisted valuation price")
	}
	captured, err := time.Parse(time.RFC3339Nano, valuationCapturedAt)
	if err != nil {
		return execution.OperationEconomics{}, err
	}
	valuation, err := arbitrage.NewValuationSnapshot(
		valuationVersion, market.AssetID(valuationBase), market.AssetID(valuationQuote),
		price, valuationObservations, captured,
	)
	if err != nil {
		return execution.OperationEconomics{}, err
	}
	parse := func(asset, value string) (market.AssetQuantity, error) {
		return market.ParseAssetQuantity(market.AssetID(asset), value)
	}
	economics := execution.OperationEconomics{Valuation: valuation}
	if economics.QuoteDelta, err = parse(quoteDeltaAsset, quoteDeltaValue); err != nil {
		return execution.OperationEconomics{}, err
	}
	if economics.BaseDelta, err = parse(baseDeltaAsset, baseDeltaValue); err != nil {
		return execution.OperationEconomics{}, err
	}
	if economics.MarkedBase, err = parse(markedBaseAsset, markedBaseValue); err != nil {
		return execution.OperationEconomics{}, err
	}
	if economics.GrossPnL, err = parse(grossAsset, grossValue); err != nil {
		return execution.OperationEconomics{}, err
	}
	if economics.Cost, err = parse(costAsset, costValue); err != nil {
		return execution.OperationEconomics{}, err
	}
	if economics.NetPnL, err = parse(netAsset, netValue); err != nil {
		return execution.OperationEconomics{}, err
	}
	if economics.Threshold, err = parse(thresholdAsset, thresholdValue); err != nil {
		return execution.OperationEconomics{}, err
	}
	if economics.DiscoveredAt, err = time.Parse(time.RFC3339Nano, discoveredAt); err != nil {
		return execution.OperationEconomics{}, err
	}
	if economics.ValidatedAt, err = time.Parse(time.RFC3339Nano, validatedAt); err != nil {
		return execution.OperationEconomics{}, err
	}
	if err := economics.Validate(); err != nil {
		return execution.OperationEconomics{}, err
	}
	return economics, nil
}

func (s *OperationalStore) steps(ctx context.Context, operationID execution.OperationID) ([]execution.OperationStep, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT step_id, side, chain_id, account_id, market_id,
		input_token, input_units, expected_output_token, expected_output_units, tx_hash, nonce,
		blockhash, last_valid_block_height, route_allocation_json, technical_state, economic_state
		FROM operation_steps WHERE operation_id = ? ORDER BY step_id`, string(operationID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []execution.OperationStep
	for rows.Next() {
		var step execution.OperationStep
		var id, side, chain, account, marketID, inputToken, inputUnits, outputToken, outputUnits string
		var hash, nonce, blockhash, allocationJSON, technical, economic string
		if err := rows.Scan(&id, &side, &chain, &account, &marketID, &inputToken, &inputUnits,
			&outputToken, &outputUnits, &hash, &nonce, &blockhash, &step.Identity.LastValidBlockHeight,
			&allocationJSON, &technical, &economic); err != nil {
			return nil, err
		}
		step.Leg.ID, step.Leg.Side = execution.StepID(id), execution.LegSide(side)
		step.Operation = operationID
		step.Leg.Chain, step.Leg.Account = market.ChainID(chain), execution.AccountID(account)
		step.Leg.Market = market.MarketID(marketID)
		step.Leg.Input, err = market.ParseTokenAmount(market.TokenID(inputToken), inputUnits)
		if err != nil {
			return nil, err
		}
		step.Leg.ExpectedOutput, err = market.ParseTokenAmount(market.TokenID(outputToken), outputUnits)
		if err != nil {
			return nil, err
		}
		step.Identity = execution.TransactionIdentity{
			Chain: step.Leg.Chain, Account: step.Leg.Account, Hash: hash,
			Blockhash: blockhash, LastValidBlockHeight: step.Identity.LastValidBlockHeight,
		}
		if nonce != "" {
			value, parseErr := strconv.ParseUint(nonce, 10, 64)
			if parseErr != nil {
				return nil, parseErr
			}
			step.Identity.Nonce = &value
		}
		step.Allocation, err = decodeRouteAllocation(allocationJSON)
		if err != nil {
			return nil, err
		}
		step.Technical, step.Economic = execution.TechnicalState(technical), execution.EconomicState(economic)
		result = append(result, step)
	}
	return result, rows.Err()
}

type operationalEventRecord struct {
	operation  execution.OperationID
	step       execution.StepID
	kind       execution.EventKind
	technical  execution.TechnicalState
	economic   execution.EconomicState
	detail     string
	evidence   string
	actualIn   market.TokenAmount
	actualOut  market.TokenAmount
	occurredAt time.Time
}

func appendOperationalEvent(
	ctx context.Context,
	tx *sql.Tx,
	event operationalEventRecord,
) (bool, error) {
	if event.operation == "" || event.kind == "" || event.occurredAt.IsZero() {
		return false, fmt.Errorf("operational event is incomplete")
	}
	inputToken, inputUnits := string(event.actualIn.Token()), ""
	if event.actualIn.Token() != "" {
		inputUnits = event.actualIn.String()
	}
	outputToken, outputUnits := string(event.actualOut.Token()), ""
	if event.actualOut.Token() != "" {
		outputUnits = event.actualOut.String()
	}
	canonical := strings.Join([]string{
		string(event.operation), string(event.step), string(event.kind),
		string(event.technical), string(event.economic), event.detail, event.evidence,
		inputToken, inputUnits, outputToken, outputUnits,
	}, "\x00")
	sum := sha256.Sum256([]byte(canonical))
	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO operation_events (
		operation_id, step_id, event_kind, technical_state, economic_state,
		detail, evidence, actual_input_token, actual_input_units,
		actual_output_token, actual_output_units, occurred_at, dedupe_key
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(event.operation), string(event.step), string(event.kind),
		string(event.technical), string(event.economic), event.detail, event.evidence,
		inputToken, inputUnits, outputToken, outputUnits,
		formatOperationalTime(event.occurredAt), hex.EncodeToString(sum[:]),
	)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected == 1, err
}

type persistedRouteAllocation struct {
	InputToken          string                `json:"input_token"`
	InputUnits          string                `json:"input_units"`
	ExpectedOutputToken string                `json:"expected_output_token"`
	ExpectedOutputUnits string                `json:"expected_output_units"`
	Groups              []persistedRouteGroup `json:"groups"`
}

type persistedRouteGroup struct {
	ID          string                 `json:"id"`
	Parent      string                 `json:"parent,omitempty"`
	InputToken  string                 `json:"input_token"`
	OutputToken string                 `json:"output_token"`
	Branches    []persistedRouteBranch `json:"branches"`
}

type persistedRouteBranch struct {
	Market         string `json:"market"`
	PlannedInput   string `json:"planned_input"`
	ExpectedOutput string `json:"expected_output"`
}

func encodeRouteAllocation(allocation *execution.RouteAllocation) (string, error) {
	if allocation == nil {
		return "", nil
	}
	if err := allocation.Validate(); err != nil {
		return "", err
	}
	wire := persistedRouteAllocation{
		InputToken: string(allocation.Input.Token()), InputUnits: allocation.Input.String(),
		ExpectedOutputToken: string(allocation.ExpectedOutput.Token()),
		ExpectedOutputUnits: allocation.ExpectedOutput.String(),
		Groups:              make([]persistedRouteGroup, len(allocation.Groups)),
	}
	for index, group := range allocation.Groups {
		wire.Groups[index] = persistedRouteGroup{
			ID: string(group.ID), Parent: string(group.Parent),
			InputToken: string(group.InputToken), OutputToken: string(group.OutputToken),
			Branches: make([]persistedRouteBranch, len(group.Branches)),
		}
		for branchIndex, branch := range group.Branches {
			wire.Groups[index].Branches[branchIndex] = persistedRouteBranch{
				Market: string(branch.Market), PlannedInput: branch.PlannedInput.String(),
				ExpectedOutput: branch.ExpectedOutput.String(),
			}
		}
	}
	encoded, err := json.Marshal(wire)
	return string(encoded), err
}

func decodeRouteAllocation(encoded string) (*execution.RouteAllocation, error) {
	if strings.TrimSpace(encoded) == "" {
		return nil, nil
	}
	var wire persistedRouteAllocation
	if err := json.Unmarshal([]byte(encoded), &wire); err != nil {
		return nil, fmt.Errorf("decode persisted route allocation: %w", err)
	}
	input, err := market.ParseTokenAmount(market.TokenID(wire.InputToken), wire.InputUnits)
	if err != nil {
		return nil, err
	}
	output, err := market.ParseTokenAmount(
		market.TokenID(wire.ExpectedOutputToken), wire.ExpectedOutputUnits,
	)
	if err != nil {
		return nil, err
	}
	allocation := execution.RouteAllocation{
		Input: input, ExpectedOutput: output, Groups: make([]execution.RouteGroup, len(wire.Groups)),
	}
	for index, group := range wire.Groups {
		allocation.Groups[index] = execution.RouteGroup{
			ID: execution.AllocationGroupID(group.ID), Parent: execution.AllocationGroupID(group.Parent),
			InputToken: market.TokenID(group.InputToken), OutputToken: market.TokenID(group.OutputToken),
			Branches: make([]execution.RouteBranch, len(group.Branches)),
		}
		for branchIndex, branch := range group.Branches {
			planned, ok := new(big.Int).SetString(branch.PlannedInput, 10)
			if !ok {
				return nil, fmt.Errorf("invalid persisted route planned input")
			}
			expected, ok := new(big.Int).SetString(branch.ExpectedOutput, 10)
			if !ok {
				return nil, fmt.Errorf("invalid persisted route expected output")
			}
			allocation.Groups[index].Branches[branchIndex] = execution.RouteBranch{
				Market: market.MarketID(branch.Market), PlannedInput: planned, ExpectedOutput: expected,
			}
		}
	}
	if err := allocation.Validate(); err != nil {
		return nil, err
	}
	return &allocation, nil
}

func (s *OperationalStore) Close() error {
	s.once.Do(func() { s.err = s.db.Close() })
	return s.err
}

func formatOperationalTime(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }

var _ persistenceport.OperationalStore = (*OperationalStore)(nil)
var _ persistenceport.RecoveryStore = (*OperationalStore)(nil)
