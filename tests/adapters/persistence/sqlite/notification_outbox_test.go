package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	sqlitestore "github.com/VarozXYZ/vernier/adapters/persistence/sqlite"
	persistenceport "github.com/VarozXYZ/vernier/ports/persistence"
)

func TestLiveNotificationOutboxSurvivesReopenAndStopsAfterDelivery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "live.sqlite")
	store, err := sqlitestore.OpenSequentialLive(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	record := persistenceport.LiveNotificationRecord{ID: "notification-1", Payload: []byte(`{"runtime":{"kind":"started"}}`),
		State: "pending", CreatedAt: now, UpdatedAt: now}
	if inserted, err := store.PutLiveNotification(context.Background(), record); err != nil || !inserted {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = sqlitestore.OpenSequentialLive(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	due, err := store.LoadDueLiveNotifications(context.Background(), now.Add(time.Second), 10)
	if err != nil || len(due) != 1 || due[0].ID != record.ID {
		t.Fatalf("due=%+v err=%v", due, err)
	}
	if err := store.MarkLiveNotification(context.Background(), record.ID, "delivered", 1, time.Time{}, "", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	due, err = store.LoadDueLiveNotifications(context.Background(), now.Add(2*time.Second), 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("delivered notification remained due: %+v err=%v", due, err)
	}
}
