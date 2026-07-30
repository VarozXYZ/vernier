package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type SwapCanaryOperation struct {
	ID          string
	Provider    string
	Market      string
	Side        string
	AmountUnits string
	Chain       string
	Identity    string
	Status      string
	LastError   string
	CreatedAt   time.Time
}

type SwapCanaryStore struct {
	db   *sql.DB
	once sync.Once
	err  error
}

func OpenSwapCanary(path string) (*SwapCanaryStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("swap canary SQLite path is required")
	}
	if directory := filepath.Dir(path); directory != "." && directory != "" {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return nil, fmt.Errorf("create swap canary directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open swap canary SQLite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
		`CREATE TABLE IF NOT EXISTS swap_canary_operations (
			operation_id TEXT PRIMARY KEY,
			provider TEXT NOT NULL,
			market_id TEXT NOT NULL,
			side TEXT NOT NULL,
			amount_units TEXT NOT NULL,
			chain_name TEXT NOT NULL DEFAULT '',
			identity TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL,
			last_error TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			UNIQUE(chain_name, identity)
		)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("configure swap canary SQLite: %w", err)
		}
	}
	return &SwapCanaryStore{db: db}, nil
}

func (s *SwapCanaryStore) Create(
	ctx context.Context,
	operation SwapCanaryOperation,
) error {
	if operation.ID == "" || operation.Provider == "" || operation.Market == "" ||
		operation.Side == "" || operation.AmountUnits == "" ||
		operation.Status != "created" || operation.CreatedAt.IsZero() {
		return fmt.Errorf("swap canary operation is incomplete")
	}
	now := operation.CreatedAt.UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `INSERT INTO swap_canary_operations (
		operation_id, provider, market_id, side, amount_units, chain_name,
		identity, status, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)`,
		operation.ID, operation.Provider, operation.Market, operation.Side,
		operation.AmountUnits, operation.ID, operation.Status, now, now,
	)
	return err
}

func (s *SwapCanaryStore) Prepared(
	ctx context.Context,
	operationID, chain, identity string,
) error {
	if operationID == "" || chain == "" || identity == "" {
		return fmt.Errorf("prepared swap canary identity is incomplete")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE swap_canary_operations
		SET chain_name = ?, identity = ?, status = 'prepared', updated_at = ?
		WHERE operation_id = ? AND status = 'created'`,
		chain, identity, time.Now().UTC().Format(time.RFC3339Nano), operationID,
	)
	if err != nil {
		return fmt.Errorf("persist swap canary identity: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("swap canary operation cannot become prepared")
	}
	return nil
}

func (s *SwapCanaryStore) Mark(
	ctx context.Context,
	operationID, status string,
	cause error,
) error {
	switch status {
	case "broadcast", "confirmed", "failed", "outcome_unknown":
	default:
		return fmt.Errorf("invalid swap canary status")
	}
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	allowedFrom := ""
	switch status {
	case "broadcast":
		allowedFrom = "prepared"
	case "confirmed":
		allowedFrom = "broadcast"
	case "failed":
		allowedFrom = "created,prepared,broadcast"
	case "outcome_unknown":
		allowedFrom = "prepared,broadcast"
	}
	result, err := s.db.ExecContext(ctx, `UPDATE swap_canary_operations
		SET status = ?, last_error = ?, updated_at = ?
		WHERE operation_id = ? AND instr(',' || ? || ',', ',' || status || ',') > 0`,
		status, message, time.Now().UTC().Format(time.RFC3339Nano), operationID,
		allowedFrom,
	)
	if err != nil {
		return err
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		return fmt.Errorf("swap canary operation was not found")
	}
	return nil
}

func (s *SwapCanaryStore) Load(
	ctx context.Context,
	operationID string,
) (SwapCanaryOperation, error) {
	var operation SwapCanaryOperation
	var createdAt string
	err := s.db.QueryRowContext(ctx, `SELECT operation_id, provider, market_id,
		side, amount_units, chain_name, identity, status, last_error, created_at
		FROM swap_canary_operations WHERE operation_id = ?`, operationID,
	).Scan(
		&operation.ID, &operation.Provider, &operation.Market, &operation.Side,
		&operation.AmountUnits, &operation.Chain, &operation.Identity,
		&operation.Status, &operation.LastError, &createdAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return SwapCanaryOperation{}, fmt.Errorf("swap canary operation was not found")
	}
	if err != nil {
		return SwapCanaryOperation{}, err
	}
	operation.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return SwapCanaryOperation{}, err
	}
	if operation.Status == "created" && operation.Chain == "pending" &&
		operation.Identity == operation.ID {
		operation.Chain = ""
		operation.Identity = ""
	}
	return operation, nil
}

func (s *SwapCanaryStore) Close() error {
	s.once.Do(func() { s.err = s.db.Close() })
	return s.err
}
