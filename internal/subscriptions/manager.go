package subscriptions

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/remade/ledger/internal/storage"
	redisclient "github.com/remade/ledger/internal/storage/redis"
)

const (
	// subscriptionBufferSize is the per-subscription channel buffer.
	subscriptionBufferSize = 100
	// maxReplayEvents bounds historical replay so the dedup set cannot grow
	// without limit; larger windows must use the Export RPC.
	maxReplayEvents = 100_000
	// dropLogInterval throttles the slow-consumer drop warnings.
	dropLogInterval = 100
)

// EventNotification is published to Redis when a new event is committed.
type EventNotification struct {
	LedgerID string `json:"ledger_id"`
	EventID  string `json:"event_id"`
	Type     int16  `json:"type"`
}

// Manager manages streaming subscriptions via Redis pub/sub.
type Manager struct {
	redis  *redisclient.Client
	logger *zap.Logger
}

// NewManager creates a new subscription Manager.
func NewManager(rc *redisclient.Client, logger *zap.Logger) *Manager {
	return &Manager{
		redis:  rc,
		logger: logger.Named("subscriptions"),
	}
}

// Publish notifies subscribers about a new event.
func (m *Manager) Publish(ctx context.Context, notification EventNotification) error {
	data, err := json.Marshal(notification)
	if err != nil {
		return err
	}
	return m.redis.Publish(ctx, notification.LedgerID, data)
}

// Subscribe creates a subscription to events for a ledger.
// It returns a channel of events and a cancel function.
func (m *Manager) Subscribe(ctx context.Context, ledgerID string, eventTypes []int16, fromEventID string, store storage.Store) (<-chan storage.LogEventRecord, func(), error) {
	ch := make(chan storage.LogEventRecord, subscriptionBufferSize)

	// Subscribe to Redis pub/sub FIRST so we don't miss events during replay.
	rdb := m.redis.Underlying()
	channel := redisclient.SubscriptionChannel(ledgerID)
	pubsub := rdb.Subscribe(ctx, channel)

	// Track replayed event IDs to deduplicate overlap between
	// historical replay and live events. Nil when no replay is requested.
	// highWaterMark is the highest event ID seen during replay (ULIDs are
	// lexicographically ordered). Once a live event exceeds it, all replay
	// events must have arrived and the dedup set can be cleared.
	var replayedIDs map[string]struct{}
	var highWaterMark string

	// If fromEventID is set, replay historical events first.
	// ListLogEvents uses event_id > pageToken, so this resumes after the given event.
	if fromEventID != "" {
		replayedIDs = make(map[string]struct{})
		pageToken := fromEventID
		replayed := 0
		for {
			events, nextToken, err := store.ListLogEvents(ctx, ledgerID, storage.ListParams{PageSize: 100, PageToken: pageToken})
			if err != nil {
				_ = pubsub.Close()
				close(ch)
				return nil, nil, err
			}
			for _, e := range events {
				replayed++
				if replayed > maxReplayEvents {
					// Bound the dedup set and replay duration: very large windows
					// must be read via the Export RPC, not a live Subscribe.
					_ = pubsub.Close()
					close(ch)
					return nil, nil, fmt.Errorf("replay window exceeds %d events from %q; use the Export RPC for full history instead of Subscribe", maxReplayEvents, fromEventID)
				}
				replayedIDs[e.EventID] = struct{}{}
				if e.EventID > highWaterMark {
					highWaterMark = e.EventID
				}
				if shouldInclude(e.Type, eventTypes) {
					select {
					case ch <- e:
					case <-ctx.Done():
						_ = pubsub.Close()
						close(ch)
						return nil, nil, ctx.Err()
					}
				}
			}
			if nextToken == "" {
				break
			}
			pageToken = nextToken
		}
	}

	cancelCtx, cancel := context.WithCancel(ctx)

	go func() {
		defer close(ch)
		defer func() { _ = pubsub.Close() }()

		// Per-subscription drop counter for slow-consumer backpressure.
		var dropped uint64
		msgCh := pubsub.Channel()
		for {
			select {
			case <-cancelCtx.Done():
				return
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				var notification EventNotification
				if err := json.Unmarshal([]byte(msg.Payload), &notification); err != nil {
					m.logger.Warn("invalid subscription message", zap.Error(err))
					continue
				}

				// Deduplicate: skip events already sent during replay.
				if replayedIDs != nil {
					if _, seen := replayedIDs[notification.EventID]; seen {
						delete(replayedIDs, notification.EventID)
						continue
					}
					// Once we see a live event past the high-water mark,
					// all replayed events have been accounted for.
					if notification.EventID > highWaterMark {
						replayedIDs = nil
					}
				}
				m.handleNotification(cancelCtx, notification, eventTypes, store, ch, &dropped)
			}
		}
	}()

	return ch, cancel, nil
}

func (m *Manager) handleNotification(ctx context.Context, notification EventNotification, eventTypes []int16, store storage.Store, ch chan<- storage.LogEventRecord, dropped *uint64) {
	if !shouldInclude(notification.Type, eventTypes) {
		return
	}

	event, err := store.GetLogEvent(ctx, notification.LedgerID, notification.EventID)
	if err != nil {
		m.logger.Warn("failed to fetch event for subscription", zap.Error(err))
		return
	}
	if event == nil {
		m.logger.Warn("event not found for subscription",
			zap.String("event_id", notification.EventID))
		return
	}

	// Non-blocking send: a slow consumer drops the live event (and is warned)
	// rather than blocking the Redis pub/sub processing loop. Dropped events can
	// be recovered by re-subscribing with fromEventID or via the Export RPC.
	select {
	case ch <- *event:
	case <-ctx.Done():
	default:
		n := atomic.AddUint64(dropped, 1)
		if n == 1 || n%dropLogInterval == 0 {
			m.logger.Warn("subscription channel full; dropping live event (slow consumer)",
				zap.String("ledger", notification.LedgerID),
				zap.String("event_id", notification.EventID),
				zap.Uint64("dropped_total", n),
			)
		}
	}
}

func shouldInclude(eventType int16, filter []int16) bool {
	if len(filter) == 0 {
		return true
	}
	for _, t := range filter {
		if eventType == t {
			return true
		}
	}
	return false
}
