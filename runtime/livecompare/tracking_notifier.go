package livecompare

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/VarozXYZ/vernier/domain/arbitrage"
	notificationport "github.com/VarozXYZ/vernier/ports/notification"
	persistenceport "github.com/VarozXYZ/vernier/ports/persistence"
)

type trackingNotification struct {
	window arbitrage.WindowID
	update notificationport.TrackingWindowUpdate
}

type trackingNotifier struct {
	sender notificationport.TrackingWindowSender
	store  persistenceport.TrackingStore
	logger *slog.Logger
	queue  chan trackingNotification
	done   chan struct{}
}

func newTrackingNotifier(sender notificationport.OpeningSender, store persistenceport.TrackingStore, logger *slog.Logger) *trackingNotifier {
	tracking, _ := sender.(notificationport.TrackingWindowSender)
	if tracking == nil {
		return nil
	}
	// Updates contain a bounded recent history. Keep the queue bounded too so
	// Telegram backpressure cannot turn a long-lived stream into an in-memory
	// event log.
	n := &trackingNotifier{sender: tracking, store: store, logger: logger, queue: make(chan trackingNotification, 256), done: make(chan struct{})}
	return n
}

func (n *trackingNotifier) start(ctx context.Context) {
	if n == nil {
		return
	}
	go n.run(ctx)
}

func (n *trackingNotifier) enqueue(ctx context.Context, job trackingNotification) error {
	if n == nil {
		return nil
	}
	select {
	case n.queue <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *trackingNotifier) stop() {
	if n == nil {
		return
	}
	close(n.queue)
	<-n.done
}

func (n *trackingNotifier) run(ctx context.Context) {
	defer close(n.done)
	messages := make(map[arbitrage.WindowID]int64)
	pending := make([]trackingNotification, 0)
	queueClosed := false
	for len(pending) > 0 || !queueClosed {
		var job trackingNotification
		if len(pending) > 0 {
			job = pending[0]
			pending = pending[1:]
		} else {
			var ok bool
			job, ok = <-n.queue
			if !ok {
				return
			}
		}
		deferred, closed := n.deliver(ctx, job, messages)
		pending = append(pending, deferred...)
		queueClosed = queueClosed || closed
	}
}

func (n *trackingNotifier) deliver(ctx context.Context, job trackingNotification, messages map[arbitrage.WindowID]int64) ([]trackingNotification, bool) {
	var deferred []trackingNotification
	queueClosed := false
	messageID := messages[job.window]
	if messageID == 0 {
		if stored, ok, err := n.store.TrackingMessage(ctx, job.window); err == nil && ok {
			messageID = stored
		}
	}
	for {
		var err error
		if messageID == 0 {
			messageID, err = n.sender.SendTrackingWindow(ctx, job.update)
			if err == nil {
				messages[job.window] = messageID
				err = n.store.SetTrackingMessage(ctx, job.window, messageID)
			}
		} else {
			err = n.sender.EditTrackingWindow(ctx, messageID, job.update)
			messages[job.window] = messageID
		}
		if err == nil {
			return deferred, queueClosed
		}
		var limited notificationport.RetryAfterError
		if !errors.As(err, &limited) || limited.RetryAfter() <= 0 {
			n.logger.Error("Telegram tracking update failed", "window", job.window, "error", err)
			return deferred, queueClosed
		}
		timer := time.NewTimer(limited.RetryAfter())
		select {
		case <-ctx.Done():
			timer.Stop()
			return deferred, queueClosed
		case <-timer.C:
		}
		// Only after Telegram actually rate-limits do we collapse queued complete
		// projections to the newest state for this single active window.
		for {
			select {
			case newer, ok := <-n.queue:
				if !ok {
					queueClosed = true
					goto retry
				}
				if newer.window == job.window {
					job = newer
				} else {
					deferred = append(deferred, newer)
				}
			default:
				goto retry
			}
		}
	retry:
	}
}
