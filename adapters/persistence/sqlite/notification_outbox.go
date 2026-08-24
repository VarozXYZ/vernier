package sqlite

import (
	"context"
	"fmt"
	"time"

	persistenceport "github.com/VarozXYZ/vernier/ports/persistence"
)

func (s *SequentialLiveStore) PutLiveNotification(ctx context.Context, record persistenceport.LiveNotificationRecord) (bool, error) {
	if record.ID == "" || len(record.Payload) == 0 || record.State == "" || record.CreatedAt.IsZero() || record.UpdatedAt.IsZero() {
		return false, fmt.Errorf("live notification outbox record is incomplete")
	}
	next := ""
	if !record.NextAttempt.IsZero() {
		next = record.NextAttempt.UTC().Format(time.RFC3339Nano)
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO live_notification_outbox(
		id, payload_json, state, attempts, next_attempt_at, last_error, created_at, updated_at
	) VALUES(?, ?, ?, ?, ?, '', ?, ?) ON CONFLICT(id) DO NOTHING`, record.ID, record.Payload, record.State,
		record.Attempts, next, record.CreatedAt.UTC().Format(time.RFC3339Nano), record.UpdatedAt.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return false, err
	}
	changed, _ := result.RowsAffected()
	return changed == 1, nil
}

func (s *SequentialLiveStore) LoadDueLiveNotifications(ctx context.Context, now time.Time, limit int) ([]persistenceport.LiveNotificationRecord, error) {
	if now.IsZero() || limit <= 0 {
		return nil, fmt.Errorf("live notification outbox query is invalid")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, payload_json, state, attempts, next_attempt_at, created_at, updated_at
		FROM live_notification_outbox WHERE state IN ('pending','retrying') AND (next_attempt_at='' OR next_attempt_at<=?)
		ORDER BY created_at LIMIT ?`, now.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []persistenceport.LiveNotificationRecord
	for rows.Next() {
		var record persistenceport.LiveNotificationRecord
		var next, created, updated string
		if err := rows.Scan(&record.ID, &record.Payload, &record.State, &record.Attempts, &next, &created, &updated); err != nil {
			return nil, err
		}
		if next != "" {
			record.NextAttempt, err = time.Parse(time.RFC3339Nano, next)
			if err != nil {
				return nil, err
			}
		}
		record.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
		if err != nil {
			return nil, err
		}
		record.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated)
		if err != nil {
			return nil, err
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

func (s *SequentialLiveStore) MarkLiveNotification(ctx context.Context, id, state string, attempts int,
	next time.Time, lastError string, at time.Time) error {
	if id == "" || state == "" || attempts < 0 || at.IsZero() {
		return fmt.Errorf("live notification outbox update is incomplete")
	}
	nextText := ""
	if !next.IsZero() {
		nextText = next.UTC().Format(time.RFC3339Nano)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE live_notification_outbox SET state=?, attempts=?, next_attempt_at=?,
		last_error=?, updated_at=? WHERE id=?`, state, attempts, nextText, lastError, at.UTC().Format(time.RFC3339Nano), id)
	if err != nil {
		return err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return fmt.Errorf("live notification outbox record %q is unavailable", id)
	}
	return nil
}

var _ persistenceport.LiveNotificationOutbox = (*SequentialLiveStore)(nil)
