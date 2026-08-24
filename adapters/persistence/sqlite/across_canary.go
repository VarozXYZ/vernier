package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/VarozXYZ/vernier/internal/safeerr"

	_ "modernc.org/sqlite"
)

type AcrossCanaryOperation struct {
	ID                  string
	Direction           string
	AmountUnits         string
	ExpectedOutput      string
	Status              string
	SourceChain         string
	SourceIdentity      string
	SourceBlockhash     string
	DestinationChain    string
	DestinationIdentity string
	BalanceBefore       string
	BalanceAfter        string
	LastError           string
	CreatedAt           time.Time
}

type AcrossCanaryStore struct {
	db   *sql.DB
	once sync.Once
	err  error
}

func OpenAcrossCanary(path string) (*AcrossCanaryStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("across canary SQLite path is required")
	}
	if directory := filepath.Dir(path); directory != "." && directory != "" {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
		`CREATE TABLE IF NOT EXISTS across_canary_operations (
			operation_id TEXT PRIMARY KEY,
			direction TEXT NOT NULL,
			amount_units TEXT NOT NULL,
			expected_output_units TEXT NOT NULL,
			status TEXT NOT NULL,
			 source_chain TEXT NOT NULL DEFAULT '',
			 source_identity TEXT NOT NULL DEFAULT '',
			 source_blockhash TEXT NOT NULL DEFAULT '',
			destination_chain TEXT NOT NULL DEFAULT '',
			destination_identity TEXT NOT NULL DEFAULT '',
			balance_before TEXT NOT NULL DEFAULT '',
			balance_after TEXT NOT NULL DEFAULT '',
			last_error TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			UNIQUE(source_chain, source_identity)
		)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	if _, err := db.Exec(`ALTER TABLE across_canary_operations
		ADD COLUMN source_blockhash TEXT NOT NULL DEFAULT ''`); err != nil &&
		!strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
		_ = db.Close()
		return nil, fmt.Errorf("migrate Across canary SQLite: %w", err)
	}
	if err := sanitizeDurableDiagnostics(
		db,
		"across_canary_operations",
		"last_error",
	); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sanitize Across canary diagnostics: %w", err)
	}
	return &AcrossCanaryStore{db: db}, nil
}

func (s *AcrossCanaryStore) Create(ctx context.Context, operation AcrossCanaryOperation) error {
	if operation.ID == "" || operation.Direction == "" || operation.AmountUnits == "" ||
		operation.ExpectedOutput == "" || operation.Status != "created" || operation.CreatedAt.IsZero() {
		return fmt.Errorf("across canary operation is incomplete")
	}
	now := operation.CreatedAt.UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `INSERT INTO across_canary_operations (
		operation_id, direction, amount_units, expected_output_units, status,
		source_chain, source_identity, created_at, updated_at
	) VALUES (?, ?, ?, ?, 'created', 'pending', ?, ?, ?)`,
		operation.ID, operation.Direction, operation.AmountUnits, operation.ExpectedOutput,
		operation.ID, now, now,
	)
	return err
}

func (s *AcrossCanaryStore) LatestCompletedSource(
	ctx context.Context,
	direction string,
) (string, time.Time, error) {
	var identity, updatedText string
	err := s.db.QueryRowContext(ctx, `SELECT source_identity, updated_at
		FROM across_canary_operations
		WHERE direction=? AND status='completed' AND source_identity<>''
		ORDER BY updated_at DESC LIMIT 1`, direction,
	).Scan(&identity, &updatedText)
	if err != nil {
		return "", time.Time{}, err
	}
	updatedAt, err := time.Parse(time.RFC3339Nano, updatedText)
	if err != nil {
		return "", time.Time{}, err
	}
	return identity, updatedAt.UTC(), nil
}

func (s *AcrossCanaryStore) Prepared(
	ctx context.Context,
	operationID, sourceChain, sourceIdentity, destinationChain, balanceBefore string,
) error {
	result, err := s.db.ExecContext(ctx, `UPDATE across_canary_operations
		SET source_chain=?, source_identity=?, destination_chain=?, balance_before=?,
		    status='prepared', updated_at=?
		WHERE operation_id=? AND status='created'`,
		sourceChain, sourceIdentity, destinationChain, balanceBefore,
		time.Now().UTC().Format(time.RFC3339Nano), operationID,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("across canary operation cannot become prepared")
	}
	return nil
}

func (s *AcrossCanaryStore) PreparedSolana(
	ctx context.Context,
	operationID, sourceIdentity, sourceBlockhash, destinationChain,
	balanceBefore string,
) error {
	if strings.TrimSpace(sourceBlockhash) == "" {
		return fmt.Errorf("across Solana source blockhash is required")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE across_canary_operations
		SET source_chain='solana', source_identity=?, source_blockhash=?,
		    destination_chain=?, balance_before=?, status='prepared', updated_at=?
		WHERE operation_id=? AND status='created'`,
		sourceIdentity, sourceBlockhash, destinationChain, balanceBefore,
		time.Now().UTC().Format(time.RFC3339Nano), operationID,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("across canary operation cannot become prepared")
	}
	return nil
}

func (s *AcrossCanaryStore) Mark(ctx context.Context, operationID, status string, cause error) error {
	switch status {
	case "broadcast", "source_confirmed", "destination_confirmed", "completed", "failed", "rejected", "outcome_unknown":
	default:
		return fmt.Errorf("invalid Across canary status")
	}
	message := ""
	if cause != nil {
		message = safeerr.Message(cause)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE across_canary_operations
		SET status=?, last_error=?, updated_at=? WHERE operation_id=?`,
		status, message, time.Now().UTC().Format(time.RFC3339Nano), operationID,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("across canary operation was not found")
	}
	return nil
}

func (s *AcrossCanaryStore) Destination(
	ctx context.Context,
	operationID, identity, balanceAfter string,
) error {
	result, err := s.db.ExecContext(ctx, `UPDATE across_canary_operations
		SET destination_identity=?, balance_after=?, status='destination_confirmed', updated_at=?
		WHERE operation_id=?`,
		identity, balanceAfter, time.Now().UTC().Format(time.RFC3339Nano), operationID,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("across canary operation was not found")
	}
	return nil
}

func (s *AcrossCanaryStore) Get(ctx context.Context, operationID string) (AcrossCanaryOperation, error) {
	return s.get(ctx, "operation_id=?", operationID)
}

func (s *AcrossCanaryStore) GetBySourceIdentity(
	ctx context.Context,
	sourceIdentity string,
) (AcrossCanaryOperation, error) {
	if strings.TrimSpace(sourceIdentity) == "" {
		return AcrossCanaryOperation{},
			fmt.Errorf("across source identity is required")
	}
	return s.get(ctx, "source_identity=?", sourceIdentity)
}

func (s *AcrossCanaryStore) get(
	ctx context.Context,
	clause string,
	argument string,
) (AcrossCanaryOperation, error) {
	var operation AcrossCanaryOperation
	var createdAt string
	err := s.db.QueryRowContext(ctx, `SELECT
		operation_id, direction, amount_units, expected_output_units, status,
		source_chain, source_identity, source_blockhash, destination_chain, destination_identity,
		balance_before, balance_after, last_error, created_at
		FROM across_canary_operations WHERE `+clause, argument,
	).Scan(
		&operation.ID, &operation.Direction, &operation.AmountUnits, &operation.ExpectedOutput,
		&operation.Status, &operation.SourceChain, &operation.SourceIdentity,
		&operation.SourceBlockhash,
		&operation.DestinationChain, &operation.DestinationIdentity,
		&operation.BalanceBefore, &operation.BalanceAfter, &operation.LastError, &createdAt,
	)
	if err != nil {
		return AcrossCanaryOperation{}, err
	}
	operation.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return AcrossCanaryOperation{}, err
	}
	return operation, nil
}

func (s *AcrossCanaryStore) Close() error {
	s.once.Do(func() { s.err = s.db.Close() })
	return s.err
}
