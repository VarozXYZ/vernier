package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/internal/safeerr"

	_ "modernc.org/sqlite"
)

// NTTCanaryOperation is the durable state of one manually armed bridge.
// It intentionally stores transaction identities, never signed payloads.
type NTTCanaryOperation struct {
	ID             string
	Direction      string
	AmountUnits    string
	Stage          string
	SourceTx       string
	EmitterChain   uint16
	EmitterAddress string
	Sequence       uint64
	VAAFingerprint string
	LastError      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// NTTCanaryMessage is the durable Wormhole identity emitted by a confirmed
// source transfer. It lets recovery continue without querying an old source
// receipt again.
type NTTCanaryMessage struct {
	SourceTx       string
	EmitterChain   uint16
	EmitterAddress string
	Sequence       uint64
}

type NTTCanaryTransaction struct {
	OperationID          string
	Ordinal              int
	Phase                string
	Chain                string
	Identity             string
	Nonce                string
	Blockhash            string
	LastValidBlockHeight uint64
	Status               string
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

type NTTCanaryTransactionMetrics struct {
	OperationID          string
	Ordinal              int
	PrepareDuration      time.Duration
	BroadcastDuration    time.Duration
	ConfirmationDuration time.Duration
	TotalDuration        time.Duration
	NetworkFeeUnits      string
	FeeAsset             string
	AdditionalDebitUnits string
	GasUsed              uint64
	EffectiveGasPrice    string
	ComputeUnits         uint64
}

type NTTCanaryOperationMetrics struct {
	OperationID         string
	Mode                string
	ReadinessDuration   time.Duration
	SourceDuration      time.Duration
	AttestationDuration time.Duration
	DestinationDuration time.Duration
	BridgeDuration      time.Duration
	CommandDuration     time.Duration
	EVMNetworkFeeWei    string
	EVMValueWei         string
	SolanaFeeLamports   string
	SolanaDebitLamports string
}

type NTTCostCalibration struct {
	Direction           string
	EVMGasUsed          uint64
	SolanaDebitLamports string
	Samples             int
	LatestCompletedAt   time.Time
}

type NTTCanaryStore struct {
	db   *sql.DB
	once sync.Once
	err  error
}

func OpenNTTCanary(path string) (*NTTCanaryStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("NTT canary SQLite path is required")
	}
	if directory := filepath.Dir(path); directory != "." && directory != "" {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return nil, fmt.Errorf("create NTT canary store directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open NTT canary store: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &NTTCanaryStore{db: db}
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
		`CREATE TABLE IF NOT EXISTS ntt_canary_operations (
			operation_id TEXT PRIMARY KEY,
			direction TEXT NOT NULL,
			amount_units TEXT NOT NULL,
			stage TEXT NOT NULL,
			source_tx TEXT NOT NULL DEFAULT '',
			emitter_chain INTEGER NOT NULL DEFAULT 0,
			emitter_address TEXT NOT NULL DEFAULT '',
			sequence INTEGER NOT NULL DEFAULT 0,
			vaa_fingerprint TEXT NOT NULL DEFAULT '',
			last_error TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS ntt_canary_transactions (
			operation_id TEXT NOT NULL REFERENCES ntt_canary_operations(operation_id),
			ordinal INTEGER NOT NULL,
			phase TEXT NOT NULL,
			chain_name TEXT NOT NULL,
			identity TEXT NOT NULL,
			nonce TEXT NOT NULL DEFAULT '',
			blockhash TEXT NOT NULL DEFAULT '',
			last_valid_block_height INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY(operation_id, ordinal),
			UNIQUE(chain_name, identity)
		)`,
		`CREATE TABLE IF NOT EXISTS ntt_canary_transaction_metrics (
			operation_id TEXT NOT NULL,
			ordinal INTEGER NOT NULL,
			prepare_ns INTEGER NOT NULL,
			broadcast_ns INTEGER NOT NULL,
			confirmation_ns INTEGER NOT NULL,
			total_ns INTEGER NOT NULL,
			network_fee_units TEXT NOT NULL DEFAULT '',
			fee_asset TEXT NOT NULL DEFAULT '',
			additional_debit_units TEXT NOT NULL DEFAULT '',
			gas_used INTEGER NOT NULL DEFAULT 0,
			effective_gas_price TEXT NOT NULL DEFAULT '',
			compute_units INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY(operation_id, ordinal),
			FOREIGN KEY(operation_id, ordinal)
				REFERENCES ntt_canary_transactions(operation_id, ordinal)
		)`,
		`CREATE TABLE IF NOT EXISTS ntt_canary_operation_metrics (
			operation_id TEXT PRIMARY KEY
				REFERENCES ntt_canary_operations(operation_id),
			mode TEXT NOT NULL,
			readiness_ns INTEGER NOT NULL,
			source_ns INTEGER NOT NULL,
			attestation_ns INTEGER NOT NULL,
			destination_ns INTEGER NOT NULL,
			bridge_ns INTEGER NOT NULL,
			command_ns INTEGER NOT NULL,
			evm_network_fee_wei TEXT NOT NULL DEFAULT '0',
			evm_value_wei TEXT NOT NULL DEFAULT '0',
			solana_fee_lamports TEXT NOT NULL DEFAULT '0',
			solana_debit_lamports TEXT NOT NULL DEFAULT '0'
		)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("configure NTT canary SQLite: %w", err)
		}
	}
	if err := sanitizeDurableDiagnostics(
		db,
		"ntt_canary_operations",
		"last_error",
	); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sanitize NTT canary diagnostics: %w", err)
	}
	return store, nil
}

// LatestCostCalibration returns conservative high-water resource usage from
// completed real transfers. It deliberately returns gas/lamports rather than
// historical fiat values so the Live oracle can reprice them with current
// background fee and native-price caches.
func (s *NTTCanaryStore) LatestCostCalibration(
	ctx context.Context,
	direction string,
	limit int,
) (NTTCostCalibration, error) {
	if strings.TrimSpace(direction) == "" {
		return NTTCostCalibration{}, fmt.Errorf("NTT calibration direction is required")
	}
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx, `SELECT
			o.operation_id,
			COALESCE(SUM(tm.gas_used), 0),
			om.solana_debit_lamports,
			o.updated_at
		FROM ntt_canary_operations o
		JOIN ntt_canary_operation_metrics om
			ON om.operation_id=o.operation_id
		LEFT JOIN ntt_canary_transaction_metrics tm
			ON tm.operation_id=o.operation_id
		WHERE o.direction=? AND o.stage='completed'
		GROUP BY o.operation_id, om.solana_debit_lamports, o.updated_at
		ORDER BY o.updated_at DESC
		LIMIT ?`, direction, limit,
	)
	if err != nil {
		return NTTCostCalibration{}, err
	}
	defer rows.Close()
	result := NTTCostCalibration{Direction: direction}
	maxLamports := new(big.Int)
	for rows.Next() {
		var operationID, lamports, updatedText string
		var gas uint64
		if err := rows.Scan(&operationID, &gas, &lamports, &updatedText); err != nil {
			return NTTCostCalibration{}, err
		}
		parsedLamports, ok := new(big.Int).SetString(lamports, 10)
		if !ok || parsedLamports.Sign() < 0 {
			return NTTCostCalibration{}, fmt.Errorf(
				"NTT calibration %s has invalid Solana debit", operationID,
			)
		}
		updated, err := time.Parse(time.RFC3339Nano, updatedText)
		if err != nil {
			return NTTCostCalibration{}, err
		}
		if gas > result.EVMGasUsed {
			result.EVMGasUsed = gas
		}
		if parsedLamports.Cmp(maxLamports) > 0 {
			maxLamports.Set(parsedLamports)
		}
		if updated.After(result.LatestCompletedAt) {
			result.LatestCompletedAt = updated.UTC()
		}
		result.Samples++
	}
	if err := rows.Err(); err != nil {
		return NTTCostCalibration{}, err
	}
	if result.Samples == 0 {
		return NTTCostCalibration{}, fmt.Errorf(
			"no completed NTT calibration for %s", direction,
		)
	}
	result.SolanaDebitLamports = maxLamports.String()
	return result, nil
}

func (s *NTTCanaryStore) LatestCompletedTransactions(
	ctx context.Context,
	direction string,
) ([]NTTCanaryTransaction, time.Time, error) {
	var operationID, updatedText string
	err := s.db.QueryRowContext(ctx, `SELECT operation_id, updated_at
		FROM ntt_canary_operations
		WHERE direction=? AND stage='completed'
		ORDER BY updated_at DESC LIMIT 1`, direction,
	).Scan(&operationID, &updatedText)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, time.Time{}, fmt.Errorf(
			"no completed NTT calibration for %s", direction,
		)
	}
	if err != nil {
		return nil, time.Time{}, err
	}
	_, transactions, err := s.Load(ctx, operationID)
	if err != nil {
		return nil, time.Time{}, err
	}
	updatedAt, err := time.Parse(time.RFC3339Nano, updatedText)
	if err != nil {
		return nil, time.Time{}, err
	}
	return transactions, updatedAt.UTC(), nil
}

func (s *NTTCanaryStore) Create(
	ctx context.Context,
	operation NTTCanaryOperation,
) error {
	if operation.ID == "" || operation.Direction == "" || operation.AmountUnits == "" ||
		operation.Stage == "" || operation.CreatedAt.IsZero() {
		return fmt.Errorf("NTT canary operation is incomplete")
	}
	operation.UpdatedAt = operation.CreatedAt
	_, err := s.db.ExecContext(ctx, `INSERT INTO ntt_canary_operations (
		operation_id, direction, amount_units, stage, source_tx, emitter_chain,
		emitter_address, sequence, vaa_fingerprint, last_error, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		operation.ID, operation.Direction, operation.AmountUnits, operation.Stage,
		operation.SourceTx, operation.EmitterChain, operation.EmitterAddress,
		operation.Sequence, operation.VAAFingerprint, operation.LastError,
		formatCanaryTime(operation.CreatedAt), formatCanaryTime(operation.UpdatedAt),
	)
	if err != nil {
		return fmt.Errorf("create NTT canary operation: %w", err)
	}
	return nil
}

// CreateOrReuseUnbroadcast creates an operation or reuses the same
// deterministic identity only when no transaction has ever been prepared.
// Pre-broadcast source reconstruction and attestation failures are reusable:
// they cannot have emitted a destination transaction. This keeps recovery
// idempotent without weakening no-resend rules.
func (s *NTTCanaryStore) CreateOrReuseUnbroadcast(
	ctx context.Context,
	operation NTTCanaryOperation,
) error {
	if operation.ID == "" || operation.Direction == "" ||
		operation.AmountUnits == "" || operation.Stage == "" ||
		operation.CreatedAt.IsZero() {
		return fmt.Errorf("NTT canary operation is incomplete")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var direction, amount, stage, source string
	err = tx.QueryRowContext(ctx, `SELECT direction, amount_units, stage, source_tx
		FROM ntt_canary_operations WHERE operation_id=?`,
		operation.ID,
	).Scan(&direction, &amount, &stage, &source)
	if errors.Is(err, sql.ErrNoRows) {
		operation.UpdatedAt = operation.CreatedAt
		if _, err := tx.ExecContext(ctx, `INSERT INTO ntt_canary_operations (
			operation_id, direction, amount_units, stage, source_tx,
			emitter_chain, emitter_address, sequence, vaa_fingerprint,
			last_error, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			operation.ID, operation.Direction, operation.AmountUnits,
			operation.Stage, operation.SourceTx, operation.EmitterChain,
			operation.EmitterAddress, operation.Sequence,
			operation.VAAFingerprint, operation.LastError,
			formatCanaryTime(operation.CreatedAt),
			formatCanaryTime(operation.UpdatedAt),
		); err != nil {
			return fmt.Errorf("create NTT canary operation: %w", err)
		}
		return tx.Commit()
	}
	if err != nil {
		return err
	}
	reusableStage := stage == "created" || stage == "readiness_failed" ||
		stage == "source_failed" || stage == "source_recovery_failed" ||
		stage == "attestation_failed"
	if direction != operation.Direction ||
		amount != operation.AmountUnits ||
		source != operation.SourceTx || !reusableStage ||
		(stage == "source_failed" && source != "") {
		return fmt.Errorf(
			"NTT canary operation %s cannot be safely reused",
			operation.ID,
		)
	}
	var transactions int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM ntt_canary_transactions WHERE operation_id=?`,
		operation.ID,
	).Scan(&transactions); err != nil {
		return err
	}
	if transactions != 0 {
		return fmt.Errorf(
			"NTT canary operation %s has durable transaction identities",
			operation.ID,
		)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE ntt_canary_operations
		SET stage='created', last_error='', updated_at=?
		WHERE operation_id=?`,
		formatCanaryTime(operation.CreatedAt), operation.ID,
	); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *NTTCanaryStore) RecordPrepared(
	ctx context.Context,
	transaction NTTCanaryTransaction,
) error {
	if transaction.OperationID == "" || transaction.Ordinal <= 0 ||
		transaction.Phase == "" || transaction.Chain == "" ||
		transaction.Identity == "" || transaction.Status != "prepared" ||
		transaction.CreatedAt.IsZero() {
		return fmt.Errorf("prepared NTT canary transaction is incomplete")
	}
	transaction.UpdatedAt = transaction.CreatedAt
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `INSERT INTO ntt_canary_transactions (
		operation_id, ordinal, phase, chain_name, identity, nonce, blockhash,
		last_valid_block_height, status, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		transaction.OperationID, transaction.Ordinal, transaction.Phase,
		transaction.Chain, transaction.Identity, transaction.Nonce,
		transaction.Blockhash, transaction.LastValidBlockHeight,
		transaction.Status, formatCanaryTime(transaction.CreatedAt),
		formatCanaryTime(transaction.UpdatedAt),
	); err != nil {
		return fmt.Errorf("record prepared NTT canary transaction: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE ntt_canary_operations
		SET stage = ?, updated_at = ? WHERE operation_id = ?`,
		transaction.Phase+"_prepared", formatCanaryTime(transaction.UpdatedAt),
		transaction.OperationID,
	); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *NTTCanaryStore) MarkTransaction(
	ctx context.Context,
	operationID string,
	ordinal int,
	status string,
) error {
	if status != "broadcast" && status != "confirmed" && status != "failed" &&
		status != "outcome_unknown" {
		return fmt.Errorf("invalid NTT canary transaction status")
	}
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `UPDATE ntt_canary_transactions
		SET status = ?, updated_at = ? WHERE operation_id = ? AND ordinal = ?`,
		status, formatCanaryTime(now), operationID, ordinal,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("NTT canary transaction was not found")
	}
	_, err = s.db.ExecContext(ctx, `UPDATE ntt_canary_operations
		SET stage = ?, updated_at = ? WHERE operation_id = ?`,
		status, formatCanaryTime(now), operationID,
	)
	return err
}

func (s *NTTCanaryStore) UpdateMessage(
	ctx context.Context,
	operationID, sourceTx string,
	emitterChain uint16,
	emitterAddress string,
	sequence uint64,
	vaaFingerprint, stage string,
) error {
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `UPDATE ntt_canary_operations SET
		source_tx = ?, emitter_chain = ?, emitter_address = ?, sequence = ?,
		vaa_fingerprint = ?, stage = ?, updated_at = ?
		WHERE operation_id = ?`,
		sourceTx, emitterChain, emitterAddress, sequence, vaaFingerprint,
		stage, formatCanaryTime(now), operationID,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("NTT canary operation was not found")
	}
	return nil
}

func (s *NTTCanaryStore) FindMessageBySourceTransaction(
	ctx context.Context,
	sourceTx string,
) (NTTCanaryMessage, bool, error) {
	var message NTTCanaryMessage
	err := s.db.QueryRowContext(ctx, `SELECT source_tx, emitter_chain,
		emitter_address, sequence FROM ntt_canary_operations
		WHERE source_tx = ? AND emitter_chain > 0 AND emitter_address <> ''
		ORDER BY updated_at DESC LIMIT 1`, strings.TrimSpace(sourceTx),
	).Scan(
		&message.SourceTx,
		&message.EmitterChain,
		&message.EmitterAddress,
		&message.Sequence,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return NTTCanaryMessage{}, false, nil
	}
	if err != nil {
		return NTTCanaryMessage{}, false, err
	}
	return message, true, nil
}

func (s *NTTCanaryStore) Fail(
	ctx context.Context,
	operationID, stage string,
	cause error,
) error {
	now := time.Now().UTC()
	message := ""
	if cause != nil {
		message = safeerr.Message(cause)
	}
	_, err := s.db.ExecContext(ctx, `UPDATE ntt_canary_operations
		SET stage = ?, last_error = ?, updated_at = ? WHERE operation_id = ?`,
		stage, message, formatCanaryTime(now), operationID,
	)
	return err
}

func (s *NTTCanaryStore) Load(
	ctx context.Context,
	operationID string,
) (NTTCanaryOperation, []NTTCanaryTransaction, error) {
	var operation NTTCanaryOperation
	var createdAt, updatedAt string
	err := s.db.QueryRowContext(ctx, `SELECT operation_id, direction, amount_units,
		stage, source_tx, emitter_chain, emitter_address, sequence, vaa_fingerprint,
		last_error, created_at, updated_at
		FROM ntt_canary_operations WHERE operation_id = ?`, operationID,
	).Scan(
		&operation.ID, &operation.Direction, &operation.AmountUnits,
		&operation.Stage, &operation.SourceTx, &operation.EmitterChain,
		&operation.EmitterAddress, &operation.Sequence, &operation.VAAFingerprint,
		&operation.LastError, &createdAt, &updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return NTTCanaryOperation{}, nil, fmt.Errorf("NTT canary operation was not found")
	}
	if err != nil {
		return NTTCanaryOperation{}, nil, err
	}
	operation.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return NTTCanaryOperation{}, nil, err
	}
	operation.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return NTTCanaryOperation{}, nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT operation_id, ordinal, phase,
		chain_name, identity, nonce, blockhash, last_valid_block_height, status,
		created_at, updated_at FROM ntt_canary_transactions
		WHERE operation_id = ? ORDER BY ordinal`, operationID,
	)
	if err != nil {
		return NTTCanaryOperation{}, nil, err
	}
	defer rows.Close()
	var transactions []NTTCanaryTransaction
	for rows.Next() {
		var item NTTCanaryTransaction
		if err := rows.Scan(
			&item.OperationID, &item.Ordinal, &item.Phase, &item.Chain,
			&item.Identity, &item.Nonce, &item.Blockhash,
			&item.LastValidBlockHeight, &item.Status, &createdAt, &updatedAt,
		); err != nil {
			return NTTCanaryOperation{}, nil, err
		}
		item.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
		if err != nil {
			return NTTCanaryOperation{}, nil, err
		}
		item.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt)
		if err != nil {
			return NTTCanaryOperation{}, nil, err
		}
		transactions = append(transactions, item)
	}
	return operation, transactions, rows.Err()
}

func (s *NTTCanaryStore) RecordTransactionMetrics(
	ctx context.Context,
	metrics NTTCanaryTransactionMetrics,
) error {
	if metrics.OperationID == "" || metrics.Ordinal <= 0 ||
		metrics.PrepareDuration < 0 || metrics.BroadcastDuration < 0 ||
		metrics.ConfirmationDuration < 0 || metrics.TotalDuration < 0 {
		return fmt.Errorf("NTT canary transaction metrics are incomplete")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO ntt_canary_transaction_metrics (
		operation_id, ordinal, prepare_ns, broadcast_ns, confirmation_ns,
		total_ns, network_fee_units, fee_asset, additional_debit_units,
		gas_used, effective_gas_price, compute_units
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(operation_id, ordinal) DO UPDATE SET
		prepare_ns = excluded.prepare_ns,
		broadcast_ns = excluded.broadcast_ns,
		confirmation_ns = excluded.confirmation_ns,
		total_ns = excluded.total_ns,
		network_fee_units = excluded.network_fee_units,
		fee_asset = excluded.fee_asset,
		additional_debit_units = excluded.additional_debit_units,
		gas_used = excluded.gas_used,
		effective_gas_price = excluded.effective_gas_price,
		compute_units = excluded.compute_units`,
		metrics.OperationID, metrics.Ordinal,
		metrics.PrepareDuration.Nanoseconds(),
		metrics.BroadcastDuration.Nanoseconds(),
		metrics.ConfirmationDuration.Nanoseconds(),
		metrics.TotalDuration.Nanoseconds(),
		metrics.NetworkFeeUnits, metrics.FeeAsset,
		metrics.AdditionalDebitUnits, metrics.GasUsed,
		metrics.EffectiveGasPrice, metrics.ComputeUnits,
	)
	if err != nil {
		return fmt.Errorf("record NTT canary transaction metrics: %w", err)
	}
	return nil
}

func (s *NTTCanaryStore) RecordOperationMetrics(
	ctx context.Context,
	metrics NTTCanaryOperationMetrics,
) error {
	if metrics.OperationID == "" ||
		(metrics.Mode != "fresh" && metrics.Mode != "recovery") ||
		metrics.ReadinessDuration < 0 || metrics.SourceDuration < 0 ||
		metrics.AttestationDuration < 0 || metrics.DestinationDuration < 0 ||
		metrics.BridgeDuration < 0 || metrics.CommandDuration < 0 {
		return fmt.Errorf("NTT canary operation metrics are incomplete")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO ntt_canary_operation_metrics (
		operation_id, mode, readiness_ns, source_ns, attestation_ns,
		destination_ns, bridge_ns, command_ns, evm_network_fee_wei,
		evm_value_wei, solana_fee_lamports, solana_debit_lamports
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(operation_id) DO UPDATE SET
		mode = excluded.mode,
		readiness_ns = excluded.readiness_ns,
		source_ns = excluded.source_ns,
		attestation_ns = excluded.attestation_ns,
		destination_ns = excluded.destination_ns,
		bridge_ns = excluded.bridge_ns,
		command_ns = excluded.command_ns,
		evm_network_fee_wei = excluded.evm_network_fee_wei,
		evm_value_wei = excluded.evm_value_wei,
		solana_fee_lamports = excluded.solana_fee_lamports,
		solana_debit_lamports = excluded.solana_debit_lamports`,
		metrics.OperationID, metrics.Mode,
		metrics.ReadinessDuration.Nanoseconds(),
		metrics.SourceDuration.Nanoseconds(),
		metrics.AttestationDuration.Nanoseconds(),
		metrics.DestinationDuration.Nanoseconds(),
		metrics.BridgeDuration.Nanoseconds(),
		metrics.CommandDuration.Nanoseconds(),
		metrics.EVMNetworkFeeWei, metrics.EVMValueWei,
		metrics.SolanaFeeLamports, metrics.SolanaDebitLamports,
	)
	if err != nil {
		return fmt.Errorf("record NTT canary operation metrics: %w", err)
	}
	return nil
}

func (s *NTTCanaryStore) LoadMetrics(
	ctx context.Context,
	operationID string,
) (NTTCanaryOperationMetrics, []NTTCanaryTransactionMetrics, error) {
	var operation NTTCanaryOperationMetrics
	var readinessNS, sourceNS, attestationNS, destinationNS, bridgeNS, commandNS int64
	err := s.db.QueryRowContext(ctx, `SELECT operation_id, mode, readiness_ns,
		source_ns, attestation_ns, destination_ns, bridge_ns, command_ns,
		evm_network_fee_wei, evm_value_wei, solana_fee_lamports,
		solana_debit_lamports
		FROM ntt_canary_operation_metrics WHERE operation_id = ?`,
		operationID,
	).Scan(
		&operation.OperationID, &operation.Mode, &readinessNS, &sourceNS,
		&attestationNS, &destinationNS, &bridgeNS, &commandNS,
		&operation.EVMNetworkFeeWei, &operation.EVMValueWei,
		&operation.SolanaFeeLamports, &operation.SolanaDebitLamports,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return NTTCanaryOperationMetrics{}, nil,
			fmt.Errorf("NTT canary operation metrics were not found")
	}
	if err != nil {
		return NTTCanaryOperationMetrics{}, nil, err
	}
	operation.ReadinessDuration = time.Duration(readinessNS)
	operation.SourceDuration = time.Duration(sourceNS)
	operation.AttestationDuration = time.Duration(attestationNS)
	operation.DestinationDuration = time.Duration(destinationNS)
	operation.BridgeDuration = time.Duration(bridgeNS)
	operation.CommandDuration = time.Duration(commandNS)

	rows, err := s.db.QueryContext(ctx, `SELECT operation_id, ordinal,
		prepare_ns, broadcast_ns, confirmation_ns, total_ns,
		network_fee_units, fee_asset, additional_debit_units, gas_used,
		effective_gas_price, compute_units
		FROM ntt_canary_transaction_metrics
		WHERE operation_id = ? ORDER BY ordinal`, operationID,
	)
	if err != nil {
		return NTTCanaryOperationMetrics{}, nil, err
	}
	defer rows.Close()
	var transactions []NTTCanaryTransactionMetrics
	for rows.Next() {
		var item NTTCanaryTransactionMetrics
		var prepareNS, broadcastNS, confirmationNS, totalNS int64
		if err := rows.Scan(
			&item.OperationID, &item.Ordinal, &prepareNS, &broadcastNS,
			&confirmationNS, &totalNS, &item.NetworkFeeUnits,
			&item.FeeAsset, &item.AdditionalDebitUnits, &item.GasUsed,
			&item.EffectiveGasPrice, &item.ComputeUnits,
		); err != nil {
			return NTTCanaryOperationMetrics{}, nil, err
		}
		item.PrepareDuration = time.Duration(prepareNS)
		item.BroadcastDuration = time.Duration(broadcastNS)
		item.ConfirmationDuration = time.Duration(confirmationNS)
		item.TotalDuration = time.Duration(totalNS)
		transactions = append(transactions, item)
	}
	return operation, transactions, rows.Err()
}

func (s *NTTCanaryStore) Close() error {
	s.once.Do(func() { s.err = s.db.Close() })
	return s.err
}

func formatCanaryTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}
