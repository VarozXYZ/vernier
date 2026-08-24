package persistence

import (
	"context"
	"time"
)

type LiveNotificationRecord struct {
	ID          string
	Payload     []byte
	State       string
	Attempts    int
	NextAttempt time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type LiveNotificationOutbox interface {
	PutLiveNotification(context.Context, LiveNotificationRecord) (bool, error)
	LoadDueLiveNotifications(context.Context, time.Time, int) ([]LiveNotificationRecord, error)
	MarkLiveNotification(context.Context, string, string, int, time.Time, string, time.Time) error
}
