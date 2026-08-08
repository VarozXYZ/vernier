package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
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
			WHERE state IN (
				'running', 'recovering', 'recovery_blocked',
				'manual_intervention_required'
			)`,
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
			simulation_input_token TEXT NOT NULL DEFAULT '',
			simulation_input_units TEXT NOT NULL DEFAULT '',
			simulation_output_token TEXT NOT NULL DEFAULT '',
			simulation_output_units TEXT NOT NULL DEFAULT '',
			simulation_evidence TEXT NOT NULL DEFAULT '',
			simulation_context_version INTEGER NOT NULL DEFAULT 0,
			simulation_units_consumed INTEGER NOT NULL DEFAULT 0,
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
			destination_balance_before TEXT NOT NULL DEFAULT '',
			destination_balance_after TEXT NOT NULL DEFAULT '',
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
			quote_delta_asset TEXT NOT NULL DEFAULT '',
			quote_delta_value TEXT NOT NULL DEFAULT '',
			base_delta_asset TEXT NOT NULL DEFAULT '',
			base_delta_value TEXT NOT NULL DEFAULT '',
			marked_base_asset TEXT NOT NULL DEFAULT '',
			marked_base_value TEXT NOT NULL DEFAULT '',
			mark_price TEXT NOT NULL DEFAULT '',
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
		`CREATE TABLE IF NOT EXISTS sequential_live_plan_snapshots (
			operation_id TEXT PRIMARY KEY REFERENCES sequential_live_operations(operation_id),
			plan_id TEXT NOT NULL,
			evaluation_id TEXT NOT NULL,
			config_hash TEXT NOT NULL,
			buy_market TEXT NOT NULL,
			sell_market TEXT NOT NULL,
			initial_token TEXT NOT NULL,
			initial_units TEXT NOT NULL,
			discovery_token TEXT NOT NULL,
			discovery_units TEXT NOT NULL,
			input_asset TEXT NOT NULL,
			input_value TEXT NOT NULL,
			buy_output_token TEXT NOT NULL,
			buy_output_units TEXT NOT NULL,
			sell_input_token TEXT NOT NULL,
			sell_input_units TEXT NOT NULL,
			sell_output_token TEXT NOT NULL,
			sell_output_units TEXT NOT NULL,
			forced_canary INTEGER NOT NULL,
			execution_policy_kind TEXT NOT NULL DEFAULT 'transported_sequential',
			admission_cost_id TEXT NOT NULL DEFAULT '',
			admission_cost_asset TEXT NOT NULL DEFAULT '',
			admission_cost_value TEXT NOT NULL DEFAULT '',
			admission_cost_captured_at TEXT NOT NULL DEFAULT '',
			base_asset TEXT NOT NULL DEFAULT '',
			quote_asset TEXT NOT NULL DEFAULT '',
			token_decimals_json TEXT NOT NULL DEFAULT '{}',
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS sequential_live_plan_stages (
			operation_id TEXT NOT NULL REFERENCES sequential_live_operations(operation_id),
			ordinal INTEGER NOT NULL,
			stage TEXT NOT NULL,
			source_chain TEXT NOT NULL,
			destination_chain TEXT NOT NULL DEFAULT '',
			input_token TEXT NOT NULL,
			output_token TEXT NOT NULL,
			market_id TEXT NOT NULL DEFAULT '',
			branch TEXT NOT NULL DEFAULT 'main',
			depends_on TEXT NOT NULL DEFAULT '',
			input_from_ordinal INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY(operation_id, ordinal)
		)`,
		`CREATE TABLE IF NOT EXISTS sequential_live_recovery_attempts (
			operation_id TEXT NOT NULL REFERENCES sequential_live_operations(operation_id),
			attempt_index INTEGER NOT NULL,
			ordinal INTEGER NOT NULL,
			action TEXT NOT NULL,
			reason TEXT NOT NULL,
			detail TEXT NOT NULL DEFAULT '',
			attempt INTEGER NOT NULL,
			created_at TEXT NOT NULL,
			retry_at TEXT NOT NULL DEFAULT '',
			PRIMARY KEY(operation_id, attempt_index)
		)`,
		`CREATE TABLE IF NOT EXISTS live_refuel_operations (
			refuel_id TEXT PRIMARY KEY,
			chain_name TEXT NOT NULL,
			state TEXT NOT NULL,
			input_token TEXT NOT NULL,
			input_units TEXT NOT NULL,
			native_asset TEXT NOT NULL,
			balance_before TEXT NOT NULL,
			balance_after TEXT NOT NULL DEFAULT '',
			native_received TEXT NOT NULL DEFAULT '',
			fee_value TEXT NOT NULL DEFAULT '',
			tx_chain TEXT NOT NULL DEFAULT '',
			tx_account TEXT NOT NULL DEFAULT '',
			tx_identity TEXT NOT NULL DEFAULT '',
			tx_nonce TEXT NOT NULL DEFAULT '',
			tx_blockhash TEXT NOT NULL DEFAULT '',
			tx_last_valid_height INTEGER NOT NULL DEFAULT 0,
			last_error TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS one_active_live_refuel
			ON live_refuel_operations((1))
			WHERE state IN ('prepared', 'broadcast', 'outcome_unknown')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("configure sequential Live SQLite: %w", err)
		}
	}
	migrationRequired, err := sequentialMigrationRequired(db)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("inspect sequential Live schema: %w", err)
	}
	if migrationRequired {
		var active int
		if err := db.QueryRow(`SELECT COUNT(*) FROM sequential_live_operations
			WHERE state IN ('running', 'recovering', 'recovery_blocked',
				'manual_intervention_required')`).Scan(&active); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("inspect active sequential operations: %w", err)
		}
		if active != 0 {
			_ = db.Close()
			return nil, fmt.Errorf(
				"cannot migrate sequential Live schema while an operation is active",
			)
		}
	}
	for _, migration := range []string{
		`ALTER TABLE sequential_live_transactions
			ADD COLUMN first_uncertain_at TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_transactions
			ADD COLUMN recovery_reason TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_transactions
			ADD COLUMN recovery_attempts INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE sequential_live_transactions
			ADD COLUMN next_recovery_attempt TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_transactions
			ADD COLUMN simulation_input_token TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_transactions
			ADD COLUMN simulation_input_units TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_transactions
			ADD COLUMN simulation_output_token TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_transactions
			ADD COLUMN simulation_output_units TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_transactions
			ADD COLUMN simulation_evidence TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_transactions
			ADD COLUMN simulation_context_version INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE sequential_live_transactions
			ADD COLUMN simulation_units_consumed INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE sequential_live_settlements
			ADD COLUMN destination_balance_before TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_settlements
			ADD COLUMN destination_balance_after TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_plan_snapshots
			ADD COLUMN execution_policy_kind TEXT NOT NULL
			DEFAULT 'transported_sequential'`,
		`ALTER TABLE sequential_live_plan_snapshots
			ADD COLUMN admission_cost_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_plan_snapshots
			ADD COLUMN admission_cost_asset TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_plan_snapshots
			ADD COLUMN admission_cost_value TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_plan_snapshots
			ADD COLUMN admission_cost_captured_at TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_plan_snapshots
			ADD COLUMN base_asset TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_plan_snapshots
			ADD COLUMN quote_asset TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_plan_snapshots
			ADD COLUMN token_decimals_json TEXT NOT NULL DEFAULT '{}'`,
		`ALTER TABLE sequential_live_results
			ADD COLUMN quote_delta_asset TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_results
			ADD COLUMN quote_delta_value TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_results
			ADD COLUMN base_delta_asset TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_results
			ADD COLUMN base_delta_value TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_results
			ADD COLUMN marked_base_asset TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_results
			ADD COLUMN marked_base_value TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_results
			ADD COLUMN mark_price TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_plan_stages
			ADD COLUMN branch TEXT NOT NULL DEFAULT 'main'`,
		`ALTER TABLE sequential_live_plan_stages
			ADD COLUMN depends_on TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE sequential_live_plan_stages
			ADD COLUMN input_from_ordinal INTEGER NOT NULL DEFAULT 0`,
	} {
		if _, err := db.Exec(migration); err != nil &&
			!strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			_ = db.Close()
			return nil, fmt.Errorf("migrate sequential Live SQLite: %w", err)
		}
	}
	if _, err := db.Exec(
		`DROP INDEX IF EXISTS one_active_sequential_live_operation`,
	); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec(`CREATE UNIQUE INDEX
		one_active_sequential_live_operation
		ON sequential_live_operations((1))
		WHERE state IN (
			'running', 'recovering', 'recovery_blocked',
			'manual_intervention_required'
		)`); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &SequentialLiveStore{db: db}, nil
}

func sequentialMigrationRequired(db *sql.DB) (bool, error) {
	for table, columns := range map[string][]string{
		"sequential_live_transactions": {
			"simulation_input_token", "simulation_input_units",
			"simulation_output_token", "simulation_output_units",
			"simulation_evidence", "simulation_context_version",
			"simulation_units_consumed",
		},
		"sequential_live_plan_snapshots": {
			"execution_policy_kind", "admission_cost_id",
			"admission_cost_asset", "admission_cost_value",
			"admission_cost_captured_at", "base_asset", "quote_asset",
			"token_decimals_json",
		},
		"sequential_live_plan_stages": {
			"branch", "depends_on", "input_from_ordinal",
		},
		"sequential_live_results": {
			"quote_delta_asset", "quote_delta_value", "base_delta_asset",
			"base_delta_value", "marked_base_asset", "marked_base_value",
			"mark_price",
		},
	} {
		rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
		if err != nil {
			return false, err
		}
		found := make(map[string]bool)
		for rows.Next() {
			var cid int
			var name, kind string
			var notNull, primaryKey int
			var defaultValue any
			if err := rows.Scan(
				&cid, &name, &kind, &notNull, &defaultValue, &primaryKey,
			); err != nil {
				_ = rows.Close()
				return false, err
			}
			found[name] = true
		}
		if err := rows.Close(); err != nil {
			return false, err
		}
		for _, column := range columns {
			if !found[column] {
				return true, nil
			}
		}
	}
	return false, nil
}

func ratString(value *big.Rat) string {
	if value == nil {
		return ""
	}
	return value.RatString()
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
	result, err := s.db.ExecContext(ctx, `INSERT OR REPLACE INTO sequential_live_exit_decisions (
		operation_id, route, destination_output_token,
		destination_output_units, return_output_token, return_output_units,
		recovery_asset, destination_recovery, return_recovery,
		safety_margin, destination_qualified, cost_evidence_available,
		evidence, decided_at
	) SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		WHERE EXISTS (
			SELECT 1 FROM sequential_live_operations
			WHERE operation_id=? AND state IN ('running', 'recovering')
				AND current_stage IN (1, 2)
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
		(len(result.Settlements) != 5 && len(result.Settlements) != 4 &&
			len(result.Settlements) != 2) ||
		result.ExecutionCost.Asset() == "" ||
		result.ExternalCost.Asset() != result.ExecutionCost.Asset() ||
		result.RealizedGross.Asset() != result.ExecutionCost.Asset() ||
		result.RealizedNetPnL.Asset() != result.ExecutionCost.Asset() {
		return fmt.Errorf("sequential Live result is incomplete")
	}
	inserted, err := s.db.ExecContext(ctx, `INSERT INTO sequential_live_results (
		operation_id, final_token, final_units, cost_asset, cost_value,
		external_cost_value, gross_value, net_value, quote_delta_asset,
		quote_delta_value, base_delta_asset, base_delta_value,
		marked_base_asset, marked_base_value, mark_price, recorded_at
	) SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		WHERE EXISTS (
			SELECT 1 FROM sequential_live_operations
			WHERE operation_id=? AND state IN ('running', 'recovering')
				AND current_stage IN (2, 4, 5)
			)`,
		result.Operation, result.FinalAmount.Token(),
		result.FinalAmount.Units().String(), result.ExecutionCost.Asset(),
		result.ExecutionCost.String(), result.ExternalCost.String(),
		result.RealizedGross.String(), result.RealizedNetPnL.String(),
		result.QuoteDelta.Asset(), result.QuoteDelta.String(),
		result.BaseDelta.Asset(), result.BaseDelta.String(),
		result.MarkedBase.Asset(), result.MarkedBase.String(),
		ratString(result.MarkPrice),
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
	simInToken, simInUnits, simOutToken, simOutUnits := simulationFields(prepared)
	result, err := s.db.ExecContext(ctx, `INSERT INTO sequential_live_transactions (
		operation_id, ordinal, phase, chain_name, account_id, identity, nonce,
		blockhash, last_valid_block_height, simulation_input_token,
		simulation_input_units, simulation_output_token, simulation_output_units,
		simulation_evidence, simulation_context_version,
		simulation_units_consumed, status, prepared_at, updated_at
	) SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'prepared', ?, ?
		WHERE EXISTS (
			SELECT 1 FROM sequential_live_operations
			WHERE operation_id=? AND state IN ('running', 'recovering')
			)`,
		prepared.Operation, prepared.Ordinal, prepared.Phase,
		prepared.Identity.Chain, prepared.Identity.Account,
		prepared.Identity.Hash, nonce, prepared.Identity.Blockhash,
		prepared.Identity.LastValidBlockHeight, simInToken, simInUnits,
		simOutToken, simOutUnits, prepared.SimulationEvidence,
		prepared.SimulationContextVersion, prepared.SimulationUnitsConsumed,
		at, at, prepared.Operation,
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

func (s *SequentialLiveStore) RecordPreparedTransactions(
	ctx context.Context,
	prepared []executionport.PreparedTransaction,
) error {
	if len(prepared) == 0 {
		return fmt.Errorf("prepared transaction batch is empty")
	}
	for _, item := range prepared {
		if err := item.Validate(); err != nil {
			return err
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, item := range prepared {
		nonce := ""
		if item.Identity.Nonce != nil {
			nonce = strconv.FormatUint(*item.Identity.Nonce, 10)
		}
		at := item.PreparedAt.UTC().Format(time.RFC3339Nano)
		simInToken, simInUnits, simOutToken, simOutUnits := simulationFields(item)
		result, insertErr := tx.ExecContext(ctx, `INSERT INTO sequential_live_transactions (
			operation_id, ordinal, phase, chain_name, account_id, identity, nonce,
			blockhash, last_valid_block_height, simulation_input_token,
			simulation_input_units, simulation_output_token, simulation_output_units,
			simulation_evidence, simulation_context_version,
			simulation_units_consumed, status, prepared_at, updated_at
		) SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'prepared', ?, ?
			WHERE EXISTS (SELECT 1 FROM sequential_live_operations
			WHERE operation_id=? AND state IN ('running', 'recovering'))`,
			item.Operation, item.Ordinal, item.Phase, item.Identity.Chain,
			item.Identity.Account, item.Identity.Hash, nonce,
			item.Identity.Blockhash, item.Identity.LastValidBlockHeight,
			simInToken, simInUnits, simOutToken, simOutUnits,
			item.SimulationEvidence, item.SimulationContextVersion,
			item.SimulationUnitsConsumed,
			at, at, item.Operation,
		)
		if insertErr != nil {
			return fmt.Errorf("persist prepared transaction batch: %w", insertErr)
		}
		affected, _ := result.RowsAffected()
		if affected != 1 {
			return fmt.Errorf("running sequential operation was not found")
		}
	}
	return tx.Commit()
}

func simulationFields(
	prepared executionport.PreparedTransaction,
) (string, string, string, string) {
	if prepared.SimulatedInput.IsZero() {
		return "", "", "", ""
	}
	return string(prepared.SimulatedInput.Token()), prepared.SimulatedInput.Units().String(),
		string(prepared.SimulatedOutput.Token()), prepared.SimulatedOutput.Units().String()
}

func (s *SequentialLiveStore) MarkTransaction(
	ctx context.Context,
	operationID domainexecution.OperationID,
	ordinal int,
	phase, status string,
) error {
	switch status {
	case "broadcast", "confirmed", "confirmed_revert", "rejected", "outcome_unknown":
	default:
		return fmt.Errorf("invalid sequential transaction status %q", status)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE sequential_live_transactions
		SET status=?,
			first_uncertain_at=CASE
				WHEN ?='outcome_unknown' AND first_uncertain_at=''
				THEN ?
				ELSE first_uncertain_at
			END,
			updated_at=?
		WHERE operation_id=? AND ordinal=? AND phase=?`,
		status, status, time.Now().UTC().Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano),
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

func (s *SequentialLiveStore) RecordStageFailureCosts(
	ctx context.Context,
	operationID domainexecution.OperationID,
	ordinal int,
	costs []domainexecution.CostComponent,
) error {
	if operationID == "" || ordinal < 1 || len(costs) == 0 {
		return fmt.Errorf("sequential stage failure costs are incomplete")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var state string
	var currentStage int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT state, current_stage FROM sequential_live_operations
			WHERE operation_id=?`,
		operationID,
	).Scan(&state, &currentStage); err != nil {
		return err
	}
	if state != string(domainexecution.SequentialRunning) &&
		state != string(domainexecution.SequentialRecovering) ||
		currentStage+1 != ordinal {
		return fmt.Errorf(
			"sequential stage failure does not extend the durable operation state",
		)
	}
	var lowestIndex int
	if err := tx.QueryRowContext(
		ctx,
		`SELECT COALESCE(MIN(component_index), 0)
			FROM sequential_live_costs
			WHERE operation_id=? AND ordinal=? AND component_index<0`,
		operationID,
		ordinal,
	).Scan(&lowestIndex); err != nil {
		return err
	}
	for index, cost := range costs {
		if err := cost.Validate(); err != nil {
			return fmt.Errorf("stage failure cost %d: %w", index, err)
		}
		quoteAsset, quoteValue := "", ""
		if cost.QuoteValue.Asset() != "" {
			quoteAsset = string(cost.QuoteValue.Asset())
			quoteValue = cost.QuoteValue.String()
		}
		included := 0
		if cost.IncludedInOutput {
			included = 1
		}
		// Negative component indices reserve a separate durable namespace
		// from a later successful settlement of the same ordinal.
		componentIndex := lowestIndex - index - 1
		if _, err := tx.ExecContext(ctx, `INSERT INTO sequential_live_costs (
			operation_id, ordinal, component_index, kind, chain_name,
			asset, amount_value, quote_asset, quote_value,
			included_in_output, evidence
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			operationID, ordinal, componentIndex, cost.Kind, cost.Chain,
			cost.Amount.Asset(), cost.Amount.String(), quoteAsset, quoteValue,
			included, cost.Evidence,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
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
	if err := validateSettlementExtension(
		ctx, tx, settlement, state, currentStage, currentToken, currentUnits,
	); err != nil {
		return fmt.Errorf("sequential settlement does not extend the durable operation state")
	}
	destination := ""
	if settlement.DestinationIdentity != nil {
		destination = settlement.DestinationIdentity.Hash
	}
	balanceBefore, balanceAfter := "", ""
	if settlement.DestinationBalanceBefore != nil {
		balanceBefore = settlement.DestinationBalanceBefore.String()
		balanceAfter = settlement.DestinationBalanceAfter.String()
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO sequential_live_settlements (
		operation_id, ordinal, stage, input_token, input_units, output_token,
		output_units, source_identity, destination_identity,
		destination_balance_before, destination_balance_after,
		evidence, observed_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		settlement.Request.Operation, settlement.Request.Stage.Ordinal,
		settlement.Request.Stage.Stage, settlement.ActualInput.Token(),
		settlement.ActualInput.Units().String(), settlement.ActualOutput.Token(),
		settlement.ActualOutput.Units().String(), settlement.SourceIdentity.Hash,
		destination, balanceBefore, balanceAfter, settlement.Evidence,
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
		SET current_token=CASE WHEN ? >= current_stage THEN ? ELSE current_token END,
			current_units=CASE WHEN ? >= current_stage THEN ? ELSE current_units END,
			current_stage=MAX(current_stage, ?), updated_at=?
		WHERE operation_id=? AND state IN ('running', 'recovering')`,
		settlement.Request.Stage.Ordinal, settlement.ActualOutput.Token(),
		settlement.Request.Stage.Ordinal, settlement.ActualOutput.Units().String(),
		settlement.Request.Stage.Ordinal,
		settlement.ObservedAt.UTC().Format(time.RFC3339Nano),
		settlement.Request.Operation,
	)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func validateSettlementExtension(
	ctx context.Context,
	tx *sql.Tx,
	settlement domainexecution.SequentialStageSettlement,
	state string,
	currentStage int,
	currentToken, currentUnits string,
) error {
	if state != string(domainexecution.SequentialRunning) &&
		state != string(domainexecution.SequentialRecovering) {
		return fmt.Errorf("operation is not executable")
	}
	var policy domainexecution.ExecutionPolicyKind
	err := tx.QueryRowContext(ctx, `SELECT execution_policy_kind
		FROM sequential_live_plan_snapshots WHERE operation_id=?`,
		settlement.Request.Operation,
	).Scan(&policy)
	if errors.Is(err, sql.ErrNoRows) ||
		policy == "" ||
		policy == domainexecution.PolicyTransportedSequential {
		if currentStage+1 != settlement.Request.Stage.Ordinal ||
			currentToken != string(settlement.Request.Input.Token()) ||
			currentUnits != settlement.Request.Input.Units().String() {
			return fmt.Errorf("settlement is not predecessor-linked")
		}
		return nil
	}
	if err != nil {
		return err
	}
	if policy != domainexecution.PolicyPrefundedSequential &&
		policy != domainexecution.PolicyPrefundedParallel {
		return fmt.Errorf("unsupported dependent execution policy")
	}

	var (
		stage, branch, inputToken, outputToken, sourceChain, marketID, dependencies string
		inputFrom                                                                   int
	)
	err = tx.QueryRowContext(ctx, `SELECT stage, branch, input_token,
		output_token, source_chain, market_id, depends_on, input_from_ordinal
		FROM sequential_live_plan_stages
		WHERE operation_id=? AND ordinal=?`,
		settlement.Request.Operation,
		settlement.Request.Stage.Ordinal,
	).Scan(&stage, &branch, &inputToken, &outputToken, &sourceChain,
		&marketID, &dependencies, &inputFrom)
	if err != nil {
		return err
	}
	if stage != string(settlement.Request.Stage.Stage) {
		return fmt.Errorf("settlement does not match its durable stage")
	}
	if policy == domainexecution.PolicyPrefundedParallel {
		normal := inputToken == string(settlement.Request.Input.Token()) &&
			outputToken == string(settlement.ActualOutput.Token()) &&
			sourceChain == string(settlement.Request.Stage.SourceChain) &&
			marketID == string(settlement.Request.Stage.Market)
		alternateBuy := false
		if settlement.Request.Stage.Ordinal == 1 && !normal {
			var sellSource, sellInput, sellOutput, sellMarket string
			queryErr := tx.QueryRowContext(ctx, `SELECT source_chain,
				input_token, output_token, market_id
				FROM sequential_live_plan_stages
				WHERE operation_id=? AND ordinal=2`,
				settlement.Request.Operation,
			).Scan(&sellSource, &sellInput, &sellOutput, &sellMarket)
			alternateBuy = queryErr == nil &&
				string(settlement.Request.Stage.SourceChain) == sellSource &&
				string(settlement.Request.Input.Token()) == sellOutput &&
				string(settlement.ActualOutput.Token()) == sellInput &&
				string(settlement.Request.Stage.Market) == sellMarket
		}
		if !normal && !alternateBuy {
			return fmt.Errorf("parallel settlement does not match a durable execution leg")
		}
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*)
			FROM sequential_live_settlements
			WHERE operation_id=? AND ordinal=?`,
			settlement.Request.Operation, settlement.Request.Stage.Ordinal,
		).Scan(&exists); err != nil || exists != 0 {
			return fmt.Errorf("parallel settlement already exists")
		}
		for _, dependency := range strings.Split(dependencies, ",") {
			dependency = strings.TrimSpace(dependency)
			if dependency == "" {
				continue
			}
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*)
				FROM sequential_live_settlements
				WHERE operation_id=? AND ordinal=?`,
				settlement.Request.Operation, dependency,
			).Scan(&exists); err != nil || exists != 1 {
				return fmt.Errorf("durable dependency is unsettled")
			}
		}
		return nil
	}
	if inputToken != string(settlement.Request.Input.Token()) {
		return fmt.Errorf("settlement does not match its durable stage")
	}
	if settlement.Request.Stage.Ordinal == 1 {
		if currentStage != 0 ||
			currentToken != string(settlement.Request.Input.Token()) ||
			currentUnits != settlement.Request.Input.Units().String() {
			return fmt.Errorf("buy settlement does not extend initial input")
		}
		return nil
	}
	for _, dependency := range strings.Split(dependencies, ",") {
		dependency = strings.TrimSpace(dependency)
		if dependency == "" {
			continue
		}
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*)
			FROM sequential_live_settlements
			WHERE operation_id=? AND ordinal=?`,
			settlement.Request.Operation, dependency,
		).Scan(&exists); err != nil || exists != 1 {
			return fmt.Errorf("durable dependency is unsettled")
		}
	}
	if inputFrom < 1 {
		return fmt.Errorf("dependent settlement has no input reference")
	}
	var sourceExists int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM sequential_live_settlements
		WHERE operation_id=? AND ordinal=?`,
		settlement.Request.Operation, inputFrom,
	).Scan(&sourceExists); err != nil || sourceExists != 1 {
		return fmt.Errorf("durable input settlement is missing")
	}
	switch domainexecution.SequentialBranch(branch) {
	case domainexecution.BranchMain:
		if currentStage+1 != settlement.Request.Stage.Ordinal {
			return fmt.Errorf("main settlement is out of order")
		}
	case domainexecution.BranchCircuitBreaker:
		var route string
		if currentStage != 1 ||
			tx.QueryRowContext(ctx, `SELECT route
				FROM sequential_live_exit_decisions WHERE operation_id=?`,
				settlement.Request.Operation,
			).Scan(&route) != nil ||
			route != string(domainexecution.ExitSellAtOrigin) {
			return fmt.Errorf("circuit-breaker settlement is not authorized")
		}
	default:
		return fmt.Errorf("settlement branch is invalid")
	}
	return nil
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
		domainexecution.SequentialManualIntervention,
		domainexecution.SequentialRecoveryBlocked:
	default:
		return fmt.Errorf("invalid terminal sequential operation state")
	}
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	result, err := s.db.ExecContext(ctx, `UPDATE sequential_live_operations
		SET state=?, last_error=?, updated_at=?
		WHERE operation_id=? AND state IN (
			'running', 'recovering', 'manual_intervention_required'
		)`,
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
		WHERE state IN (
			'running', 'recovering', 'recovery_blocked',
			'manual_intervention_required'
		)
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
		WHERE operation_id=? AND state IN (?, ?)`,
		domainexecution.SequentialReconciledManually,
		note,
		note,
		time.Now().UTC().Format(time.RFC3339Nano),
		operationID,
		domainexecution.SequentialManualIntervention,
		domainexecution.SequentialRecoveryBlocked,
	)
	if err != nil {
		return fmt.Errorf("acknowledge manual reconciliation: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf(
			"operation %s is not awaiting manual reconciliation or recovery unblock",
			operationID,
		)
	}
	return nil
}

// MarkSequentialOperationReconciled closes an active operation after the
// operator has completed its recovery outside the runtime. It never broadcasts
// and retains the operator's note as audit evidence.
func (s *SequentialLiveStore) MarkSequentialOperationReconciled(
	ctx context.Context,
	operationID domainexecution.OperationID,
	note string,
) error {
	if strings.TrimSpace(string(operationID)) == "" {
		return fmt.Errorf("reconciled operation ID is required")
	}
	note = strings.TrimSpace(note)
	if note == "" {
		return fmt.Errorf("manual reconciliation note is required")
	}
	const auditPrefix = "manual reconciliation completed: "
	result, err := s.db.ExecContext(ctx, `UPDATE sequential_live_operations
		SET state=?,
			last_error=CASE
				WHEN last_error='' THEN ?
				ELSE last_error || ' | ' || ?
			END,
			updated_at=?
		WHERE operation_id=? AND state IN (?, ?, ?, ?)`,
		domainexecution.SequentialReconciledManually,
		auditPrefix+note,
		auditPrefix+note,
		time.Now().UTC().Format(time.RFC3339Nano),
		operationID,
		domainexecution.SequentialRunning,
		domainexecution.SequentialRecovering,
		domainexecution.SequentialRecoveryBlocked,
		domainexecution.SequentialManualIntervention,
	)
	if err != nil {
		return fmt.Errorf("mark operation reconciled: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("operation %s is not an active operation requiring reconciliation", operationID)
	}
	return nil
}

// RetryBlockedSequentialRecovery reopens one explicitly selected recovery
// barrier without discarding its transaction identities or audit history.
// The next recovery pass must reconcile those identities before doing work.
func (s *SequentialLiveStore) RetryBlockedSequentialRecovery(
	ctx context.Context,
	operationID domainexecution.OperationID,
) error {
	if strings.TrimSpace(string(operationID)) == "" {
		return fmt.Errorf("blocked recovery operation ID is required")
	}
	const note = "blocked recovery retry authorized by operator"
	result, err := s.db.ExecContext(ctx, `UPDATE sequential_live_operations
		SET state=?,
			last_error=CASE
				WHEN last_error='' THEN ?
				ELSE last_error || ' | ' || ?
			END,
			updated_at=?
		WHERE operation_id=? AND state=?`,
		domainexecution.SequentialRecovering,
		note,
		note,
		time.Now().UTC().Format(time.RFC3339Nano),
		operationID,
		domainexecution.SequentialRecoveryBlocked,
	)
	if err != nil {
		return fmt.Errorf("authorize blocked recovery retry: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf(
			"operation %s is not blocked awaiting an explicit recovery retry",
			operationID,
		)
	}
	return nil
}

func (s *SequentialLiveStore) Close() error {
	s.once.Do(func() { s.err = s.db.Close() })
	return s.err
}
