// Package sqlite persists only Research opportunity-window lifecycles.
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

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	"github.com/VarozXYZ/vernier/domain/market"
	persistence "github.com/VarozXYZ/vernier/ports/persistence"
	_ "modernc.org/sqlite"
)

const schemaVersion = 1

type Store struct {
	db   *sql.DB
	path string
	once sync.Once
	err  error
}

func Open(path string) (*Store, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("SQLite opportunity store path is required")
	}
	directory := filepath.Dir(path)
	if directory != "." && directory != "" {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return nil, fmt.Errorf("create opportunity store directory: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open opportunity store: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &Store{db: db, path: path}
	if err := store.configure(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := store.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) configure() error {
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := s.db.Exec(statement); err != nil {
			return fmt.Errorf("configure SQLite (%s): %w", statement, err)
		}
	}
	return nil
}

func (s *Store) migrate() error {
	var version int
	if err := s.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("read SQLite schema version: %w", err)
	}
	if version > schemaVersion {
		return fmt.Errorf("unsupported SQLite schema version %d", version)
	}
	if version == schemaVersion {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin SQLite migration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	statements := []string{
		`CREATE TABLE IF NOT EXISTS opportunity_windows (
			window_id TEXT PRIMARY KEY,
			run_id TEXT NOT NULL,
			strategy_id TEXT NOT NULL,
			config_hash TEXT NOT NULL,
			buy_market TEXT NOT NULL,
			sell_market TEXT NOT NULL,
			trigger_market TEXT NOT NULL DEFAULT '',
			trigger_source TEXT NOT NULL DEFAULT '',
			trigger_position_kind TEXT NOT NULL DEFAULT '',
			trigger_position_value TEXT NOT NULL DEFAULT '0',
			trigger_reference_kind TEXT NOT NULL DEFAULT '',
			trigger_reference_value TEXT NOT NULL DEFAULT '',
			trigger_at TEXT NOT NULL DEFAULT '',
			has_trigger INTEGER NOT NULL CHECK (has_trigger IN (0, 1)),
			opened_at TEXT NOT NULL,
			first_profitable_at TEXT NOT NULL,
			last_profitable_at TEXT NOT NULL,
			closed_at TEXT NOT NULL DEFAULT '',
			best_size_asset TEXT NOT NULL,
			best_size_value TEXT NOT NULL,
			best_gross_asset TEXT NOT NULL,
			best_gross_value TEXT NOT NULL,
			best_net_asset TEXT NOT NULL,
			best_net_value TEXT NOT NULL,
			best_cost_asset TEXT NOT NULL,
			best_cost_value TEXT NOT NULL,
			classification TEXT NOT NULL,
			status TEXT NOT NULL,
			close_reason TEXT NOT NULL DEFAULT '',
			degraded INTEGER NOT NULL CHECK (degraded IN (0, 1)),
			duration_nanos INTEGER NOT NULL DEFAULT 0,
			identity_hash TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS opportunity_windows_open_idx ON opportunity_windows(status, opened_at)`,
		`CREATE TABLE IF NOT EXISTS opportunity_window_observations (
			observation_id TEXT PRIMARY KEY,
			window_id TEXT NOT NULL REFERENCES opportunity_windows(window_id) ON DELETE CASCADE,
			evaluation_id TEXT NOT NULL,
			observed_at TEXT NOT NULL,
			classification TEXT NOT NULL,
			has_candidate INTEGER NOT NULL CHECK (has_candidate IN (0, 1)),
			best INTEGER NOT NULL CHECK (best IN (0, 1)),
			size_asset TEXT NOT NULL DEFAULT '',
			size_value TEXT NOT NULL DEFAULT '',
			gross_asset TEXT NOT NULL DEFAULT '',
			gross_value TEXT NOT NULL DEFAULT '',
			net_asset TEXT NOT NULL DEFAULT '',
			net_value TEXT NOT NULL DEFAULT '',
			cost_asset TEXT NOT NULL DEFAULT '',
			cost_value TEXT NOT NULL DEFAULT '',
			observation_fingerprint TEXT NOT NULL,
			UNIQUE(window_id, evaluation_id)
		)`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return fmt.Errorf("apply SQLite migration: %w", err)
		}
	}
	if _, err := tx.Exec("PRAGMA user_version = 1"); err != nil {
		return fmt.Errorf("set SQLite schema version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit SQLite migration: %w", err)
	}
	return nil
}

func (s *Store) OpenWindow(ctx context.Context, opening arbitrage.WindowOpening) error {
	window := opening.Window
	if err := window.Validate(); err != nil {
		return fmt.Errorf("validate opportunity window: %w", err)
	}
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin open window: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var existingHash string
	err = tx.QueryRowContext(ctx, "SELECT identity_hash FROM opportunity_windows WHERE window_id = ?", string(window.ID)).Scan(&existingHash)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		_, err = tx.ExecContext(ctx, `INSERT INTO opportunity_windows (
			window_id, run_id, strategy_id, config_hash, buy_market, sell_market,
			trigger_market, trigger_source, trigger_position_kind, trigger_position_value,
			trigger_reference_kind, trigger_reference_value, trigger_at, has_trigger,
			opened_at, first_profitable_at, last_profitable_at, closed_at,
			best_size_asset, best_size_value, best_gross_asset, best_gross_value,
			best_net_asset, best_net_value, best_cost_asset, best_cost_value,
			classification, status, close_reason, degraded, duration_nanos, identity_hash,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			string(window.ID), string(window.Run), string(window.Strategy), window.ConfigHash,
			string(window.Direction.BuyMarket), string(window.Direction.SellMarket),
			triggerMarket(window), triggerSource(window), triggerPositionKind(window), triggerPositionValue(window),
			triggerReferenceKind(window), triggerReferenceValue(window), triggerAt(window), boolInt(window.HasTrigger),
			formatTime(window.OpenedAt), formatTime(window.FirstProfitableAt), formatTime(window.LastProfitableAt), "",
			string(window.Best.Size.Asset()), window.Best.Size.String(), string(window.Best.GrossPnL.Asset()), window.Best.GrossPnL.String(),
			string(window.Best.NetPnL.Asset()), window.Best.NetPnL.String(), string(window.Best.Cost.Asset()), window.Best.Cost.String(),
			string(window.Classification), string(window.Status), "", 0, 0, arbitrage.WindowFingerprint(window), formatTime(now), formatTime(now),
		)
	case err == nil:
		if existingHash != arbitrage.WindowFingerprint(window) {
			return fmt.Errorf("window %q already exists with different data", window.ID)
		}
		return nil
	default:
		return fmt.Errorf("check existing window: %w", err)
	}
	if err != nil {
		return fmt.Errorf("insert opportunity window: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit opportunity window: %w", err)
	}
	return nil
}

func (s *Store) RecordImprovement(ctx context.Context, observation arbitrage.WindowObservation) error {
	if err := observation.Validate(); err != nil {
		return fmt.Errorf("validate window observation: %w", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin window observation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var existingFingerprint string
	err = tx.QueryRowContext(ctx, `SELECT observation_fingerprint FROM opportunity_window_observations WHERE observation_id = ?`, observation.ID).Scan(&existingFingerprint)
	if err == nil {
		if existingFingerprint != observationFingerprint(observation) {
			return fmt.Errorf("observation %q already exists with different data", observation.ID)
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check existing observation: %w", err)
	}
	var status string
	var lastText string
	err = tx.QueryRowContext(ctx, "SELECT status, last_profitable_at FROM opportunity_windows WHERE window_id = ?", string(observation.WindowID)).Scan(&status, &lastText)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("window %q does not exist", observation.WindowID)
	}
	if err != nil {
		return fmt.Errorf("read window for observation: %w", err)
	}
	if status != string(arbitrage.WindowStatusOpen) {
		return fmt.Errorf("window %q is not open", observation.WindowID)
	}
	last, err := parseTime(lastText)
	if err != nil {
		return err
	}
	if observation.ObservedAt.Before(last) {
		return fmt.Errorf("window observation precedes last profitable timestamp")
	}
	update := `UPDATE opportunity_windows SET last_profitable_at = ?, classification = ?, updated_at = ? WHERE window_id = ?`
	args := []any{formatTime(observation.ObservedAt), string(observation.Classification), formatTime(time.Now().UTC()), string(observation.WindowID)}
	if observation.Best {
		update = `UPDATE opportunity_windows SET last_profitable_at = ?, classification = ?,
			best_size_asset = ?, best_size_value = ?, best_gross_asset = ?, best_gross_value = ?,
			best_net_asset = ?, best_net_value = ?, best_cost_asset = ?, best_cost_value = ?, updated_at = ?
			WHERE window_id = ?`
		args = []any{formatTime(observation.ObservedAt), string(observation.Classification),
			string(observation.Candidate.Size.Asset()), observation.Candidate.Size.String(),
			string(observation.Candidate.GrossPnL.Asset()), observation.Candidate.GrossPnL.String(),
			string(observation.Candidate.NetPnL.Asset()), observation.Candidate.NetPnL.String(),
			string(observation.Candidate.Cost.Asset()), observation.Candidate.Cost.String(), formatTime(time.Now().UTC()), string(observation.WindowID)}
	}
	if _, err := tx.ExecContext(ctx, update, args...); err != nil {
		return fmt.Errorf("update opportunity window: %w", err)
	}
	if observation.Best {
		if _, err := tx.ExecContext(ctx, `INSERT INTO opportunity_window_observations (
			observation_id, window_id, evaluation_id, observed_at, classification, has_candidate, best,
			size_asset, size_value, gross_asset, gross_value, net_asset, net_value, cost_asset, cost_value,
			observation_fingerprint
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			observation.ID, string(observation.WindowID), string(observation.Evaluation), formatTime(observation.ObservedAt), string(observation.Classification),
			boolInt(observation.HasCandidate), boolInt(observation.Best), string(observation.Candidate.Size.Asset()), observation.Candidate.Size.String(),
			string(observation.Candidate.GrossPnL.Asset()), observation.Candidate.GrossPnL.String(), string(observation.Candidate.NetPnL.Asset()), observation.Candidate.NetPnL.String(),
			string(observation.Candidate.Cost.Asset()), observation.Candidate.Cost.String(), observationFingerprint(observation),
		); err != nil {
			return fmt.Errorf("insert window observation: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit window observation: %w", err)
	}
	return nil
}

func (s *Store) CloseWindow(ctx context.Context, closing arbitrage.WindowClosing) error {
	return s.finishWindow(ctx, closing.WindowID, closing.ClosedAt, closing.LastProfitableAt, string(arbitrage.WindowStatusClosed), closing.Reason, closing.Degraded)
}

func (s *Store) FailWindow(ctx context.Context, failure arbitrage.WindowFailure) error {
	return s.finishWindow(ctx, failure.WindowID, failure.ClosedAt, failure.LastProfitableAt, string(arbitrage.WindowStatusFailed), failure.Reason, true)
}

func (s *Store) finishWindow(ctx context.Context, id arbitrage.WindowID, closedAt, lastProfitableAt time.Time, status, reason string, degraded bool) error {
	if id == "" || closedAt.IsZero() || lastProfitableAt.IsZero() || reason == "" {
		return fmt.Errorf("window close identity, timestamps, and reason are required")
	}
	closedAt, lastProfitableAt = closedAt.UTC(), lastProfitableAt.UTC()
	if closedAt.Before(lastProfitableAt) {
		return fmt.Errorf("window closes before last profitable timestamp")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin close window: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var currentStatus, openedText, currentReason, currentClosedText, currentLastText string
	var currentDegraded int64
	err = tx.QueryRowContext(ctx, `SELECT status, opened_at, close_reason, degraded, closed_at, last_profitable_at FROM opportunity_windows WHERE window_id = ?`, string(id)).Scan(&currentStatus, &openedText, &currentReason, &currentDegraded, &currentClosedText, &currentLastText)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("window %q does not exist", id)
	}
	if err != nil {
		return fmt.Errorf("read window for close: %w", err)
	}
	if currentStatus != string(arbitrage.WindowStatusOpen) {
		if currentStatus == status && currentReason == reason && currentDegraded == int64(boolInt(degraded)) && currentClosedText == formatTime(closedAt) && currentLastText == formatTime(lastProfitableAt) {
			return nil
		}
		return fmt.Errorf("window %q is already finalized", id)
	}
	openedAt, err := parseTime(openedText)
	if err != nil {
		return err
	}
	if closedAt.Before(openedAt) {
		return fmt.Errorf("window closes before opening")
	}
	duration := closedAt.Sub(openedAt)
	if _, err := tx.ExecContext(ctx, `UPDATE opportunity_windows SET last_profitable_at = ?, closed_at = ?, status = ?, close_reason = ?, degraded = ?, duration_nanos = ?, updated_at = ? WHERE window_id = ?`,
		formatTime(lastProfitableAt), formatTime(closedAt), status, reason, boolInt(degraded), duration.Nanoseconds(), formatTime(time.Now().UTC()), string(id)); err != nil {
		return fmt.Errorf("finalize opportunity window: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit finalized opportunity window: %w", err)
	}
	return nil
}

func (s *Store) FinalizeDangling(ctx context.Context, observedAt time.Time) error {
	if observedAt.IsZero() {
		return fmt.Errorf("dangling-window timestamp is required")
	}
	observedAt = observedAt.UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin dangling-window cleanup: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `UPDATE opportunity_windows SET closed_at = ?, status = ?, close_reason = ?, degraded = 1,
		duration_nanos = CASE WHEN julianday(?) >= julianday(opened_at) THEN CAST((julianday(?) - julianday(opened_at)) * 86400000000000 AS INTEGER) ELSE 0 END,
		updated_at = ? WHERE status = ?`,
		formatTime(observedAt), string(arbitrage.WindowStatusFailed), "process_interrupted", formatTime(observedAt), formatTime(observedAt), formatTime(observedAt), string(arbitrage.WindowStatusOpen)); err != nil {
		return fmt.Errorf("finalize dangling windows: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit dangling-window cleanup: %w", err)
	}
	return nil
}

func (s *Store) ListWindows(ctx context.Context, query arbitrage.WindowQuery) ([]arbitrage.WindowRecord, error) {
	limit := query.Limit
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	statement := windowSelect + " FROM opportunity_windows"
	args := make([]any, 0, 3)
	conditions := make([]string, 0, 3)
	if query.Run != "" {
		conditions = append(conditions, "run_id = ?")
		args = append(args, string(query.Run))
	}
	if query.Strategy != "" {
		conditions = append(conditions, "strategy_id = ?")
		args = append(args, string(query.Strategy))
	}
	if query.Status != "" {
		conditions = append(conditions, "status = ?")
		args = append(args, string(query.Status))
	}
	if len(conditions) != 0 {
		statement += " WHERE " + strings.Join(conditions, " AND ")
	}
	statement += " ORDER BY opened_at DESC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("list opportunity windows: %w", err)
	}
	defer rows.Close()
	windows := make([]arbitrage.OpportunityWindow, 0)
	for rows.Next() {
		window, err := scanWindow(rows)
		if err != nil {
			return nil, err
		}
		windows = append(windows, window)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate opportunity windows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close opportunity window rows: %w", err)
	}
	result := make([]arbitrage.WindowRecord, 0, len(windows))
	for _, window := range windows {
		observations, err := s.listObservations(ctx, window.ID)
		if err != nil {
			return nil, err
		}
		result = append(result, arbitrage.WindowRecord{Window: window, Observations: observations})
	}
	return result, nil
}

func (s *Store) listObservations(ctx context.Context, id arbitrage.WindowID) ([]arbitrage.WindowObservation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT observation_id, window_id, evaluation_id, observed_at, classification,
		has_candidate, best, size_asset, size_value, gross_asset, gross_value, net_asset, net_value, cost_asset, cost_value
		FROM opportunity_window_observations WHERE window_id = ? ORDER BY observed_at ASC`, string(id))
	if err != nil {
		return nil, fmt.Errorf("list window observations: %w", err)
	}
	defer rows.Close()
	result := make([]arbitrage.WindowObservation, 0)
	for rows.Next() {
		var observation arbitrage.WindowObservation
		var windowID, evaluation, observed, classification string
		var hasCandidate, best int64
		var sizeAsset, sizeValue, grossAsset, grossValue, netAsset, netValue, costAsset, costValue string
		if err := rows.Scan(&observation.ID, &windowID, &evaluation, &observed, &classification, &hasCandidate, &best, &sizeAsset, &sizeValue, &grossAsset, &grossValue, &netAsset, &netValue, &costAsset, &costValue); err != nil {
			return nil, fmt.Errorf("scan window observation: %w", err)
		}
		observation.WindowID = arbitrage.WindowID(windowID)
		observation.Evaluation = arbitrage.EvaluationID(evaluation)
		observation.ObservedAt, err = parseTime(observed)
		if err != nil {
			return nil, err
		}
		observation.Classification = arbitrage.Classification(classification)
		observation.HasCandidate, observation.Best = hasCandidate == 1, best == 1
		if observation.HasCandidate {
			observation.Candidate, err = parseCandidate(sizeAsset, sizeValue, grossAsset, grossValue, netAsset, netValue, costAsset, costValue)
			if err != nil {
				return nil, err
			}
		}
		if err := observation.Validate(); err != nil {
			return nil, fmt.Errorf("validate stored observation: %w", err)
		}
		result = append(result, observation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate window observations: %w", err)
	}
	return result, nil
}

func (s *Store) Close() error {
	s.once.Do(func() { s.err = s.db.Close() })
	return s.err
}

var _ persistence.OpportunityStore = (*Store)(nil)

const windowSelect = `SELECT window_id, run_id, strategy_id, config_hash, buy_market, sell_market,
	trigger_market, trigger_source, trigger_position_kind, trigger_position_value,
	trigger_reference_kind, trigger_reference_value, trigger_at, has_trigger,
	opened_at, first_profitable_at, last_profitable_at, closed_at,
	best_size_asset, best_size_value, best_gross_asset, best_gross_value,
	best_net_asset, best_net_value, best_cost_asset, best_cost_value,
	classification, status, close_reason, degraded`

type scanner interface{ Scan(...any) error }

func scanWindow(row scanner) (arbitrage.OpportunityWindow, error) {
	var window arbitrage.OpportunityWindow
	var id, run, strategy, configHash, buyMarket, sellMarket string
	var triggerMarket, triggerSource, positionKind, positionValue, referenceKind, referenceValue, triggerAt string
	var hasTrigger int64
	var openedAt, firstAt, lastAt, closedAt string
	var sizeAsset, sizeValue, grossAsset, grossValue, netAsset, netValue, costAsset, costValue string
	var classification, status, closeReason string
	var degraded int64
	if err := row.Scan(&id, &run, &strategy, &configHash, &buyMarket, &sellMarket,
		&triggerMarket, &triggerSource, &positionKind, &positionValue, &referenceKind, &referenceValue, &triggerAt, &hasTrigger,
		&openedAt, &firstAt, &lastAt, &closedAt, &sizeAsset, &sizeValue, &grossAsset, &grossValue, &netAsset, &netValue, &costAsset, &costValue,
		&classification, &status, &closeReason, &degraded); err != nil {
		return arbitrage.OpportunityWindow{}, fmt.Errorf("scan opportunity window: %w", err)
	}
	window.ID, window.Run, window.Strategy, window.ConfigHash = arbitrage.WindowID(id), arbitrage.ResearchRunID(run), arbitrage.StrategyID(strategy), configHash
	window.Direction = arbitrage.Direction{BuyMarket: market.MarketID(buyMarket), SellMarket: market.MarketID(sellMarket)}
	var err error
	window.OpenedAt, err = parseTime(openedAt)
	if err != nil {
		return arbitrage.OpportunityWindow{}, err
	}
	window.FirstProfitableAt, err = parseTime(firstAt)
	if err != nil {
		return arbitrage.OpportunityWindow{}, err
	}
	window.LastProfitableAt, err = parseTime(lastAt)
	if err != nil {
		return arbitrage.OpportunityWindow{}, err
	}
	if closedAt != "" {
		window.ClosedAt, err = parseTime(closedAt)
		if err != nil {
			return arbitrage.OpportunityWindow{}, err
		}
	}
	window.Best, err = parseCandidate(sizeAsset, sizeValue, grossAsset, grossValue, netAsset, netValue, costAsset, costValue)
	if err != nil {
		return arbitrage.OpportunityWindow{}, err
	}
	window.HasBest = true
	window.Classification, window.Status, window.CloseReason, window.Degraded = arbitrage.Classification(classification), arbitrage.WindowStatus(status), closeReason, degraded == 1
	window.HasTrigger = hasTrigger == 1
	if window.HasTrigger {
		window.Trigger.Market, window.Trigger.Source = market.MarketID(triggerMarket), market.SourceID(triggerSource)
		window.Trigger.Position = market.SourcePosition{Kind: market.SourcePositionKind(positionKind), Value: parseUint(positionValue)}
		window.Trigger.Reference = market.SourceReference{Kind: market.SourceReferenceKind(referenceKind), Value: referenceValue}
		window.Trigger.At, err = parseTime(triggerAt)
		if err != nil {
			return arbitrage.OpportunityWindow{}, err
		}
	}
	if err := window.Validate(); err != nil {
		return arbitrage.OpportunityWindow{}, fmt.Errorf("validate stored window: %w", err)
	}
	return window, nil
}

func parseCandidate(sizeAsset, sizeValue, grossAsset, grossValue, netAsset, netValue, costAsset, costValue string) (arbitrage.WindowCandidate, error) {
	size, err := market.ParseAssetQuantity(market.AssetID(sizeAsset), sizeValue)
	if err != nil {
		return arbitrage.WindowCandidate{}, err
	}
	gross, err := market.ParseAssetQuantity(market.AssetID(grossAsset), grossValue)
	if err != nil {
		return arbitrage.WindowCandidate{}, err
	}
	net, err := market.ParseAssetQuantity(market.AssetID(netAsset), netValue)
	if err != nil {
		return arbitrage.WindowCandidate{}, err
	}
	cost, err := market.ParseAssetQuantity(market.AssetID(costAsset), costValue)
	if err != nil {
		return arbitrage.WindowCandidate{}, err
	}
	return arbitrage.WindowCandidate{Size: size, GrossPnL: gross, NetPnL: net, Cost: cost}, nil
}

func parseTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse stored timestamp %q: %w", value, err)
	}
	return parsed.UTC(), nil
}

func formatTime(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func parseUint(value string) uint64 { parsed, _ := strconv.ParseUint(value, 10, 64); return parsed }

func triggerMarket(window arbitrage.OpportunityWindow) string {
	if window.HasTrigger {
		return string(window.Trigger.Market)
	}
	return ""
}
func triggerSource(window arbitrage.OpportunityWindow) string {
	if window.HasTrigger {
		return string(window.Trigger.Source)
	}
	return ""
}
func triggerPositionKind(window arbitrage.OpportunityWindow) string {
	if window.HasTrigger {
		return string(window.Trigger.Position.Kind)
	}
	return ""
}
func triggerPositionValue(window arbitrage.OpportunityWindow) string {
	if window.HasTrigger {
		return strconv.FormatUint(window.Trigger.Position.Value, 10)
	}
	return "0"
}
func triggerReferenceKind(window arbitrage.OpportunityWindow) string {
	if window.HasTrigger {
		return string(window.Trigger.Reference.Kind)
	}
	return ""
}
func triggerReferenceValue(window arbitrage.OpportunityWindow) string {
	if window.HasTrigger {
		return window.Trigger.Reference.Value
	}
	return ""
}
func triggerAt(window arbitrage.OpportunityWindow) string {
	if window.HasTrigger {
		return formatTime(window.Trigger.At)
	}
	return ""
}

func observationFingerprint(observation arbitrage.WindowObservation) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%t|%t|%s|%s|%s|%s|%s|%s|%s|%s",
		observation.ID, observation.WindowID, observation.Evaluation, formatTime(observation.ObservedAt), observation.Classification,
		observation.HasCandidate, observation.Best, observation.Candidate.Size.String(), observation.Candidate.GrossPnL.String(), observation.Candidate.NetPnL.String(), observation.Candidate.Cost.String(),
		observation.Candidate.Size.Asset(), observation.Candidate.GrossPnL.Asset(), observation.Candidate.NetPnL.Asset(), observation.Candidate.Cost.Asset())
}
