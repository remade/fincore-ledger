package subscriptions

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/remade/ledger/internal/storage"
)

type subFakeStore struct {
	storage.Store
	event *storage.LogEventRecord
}

func (s *subFakeStore) GetLogEvent(context.Context, string, string) (*storage.LogEventRecord, error) {
	return s.event, nil
}

func notif() EventNotification {
	return EventNotification{LedgerID: "L1", EventID: "e1", Type: 1}
}

func TestHandleNotification_SendsWhenRoom(t *testing.T) {
	m := &Manager{logger: zap.NewNop()}
	store := &subFakeStore{event: &storage.LogEventRecord{EventID: "e1", Type: 1}}
	ch := make(chan storage.LogEventRecord, 1)
	var dropped uint64

	m.handleNotification(context.Background(), notif(), nil, store, ch, &dropped)
	assert.Equal(t, uint64(0), dropped)
	require.Len(t, ch, 1)
}

func TestHandleNotification_DropsWhenFull(t *testing.T) {
	m := &Manager{logger: zap.NewNop()}
	store := &subFakeStore{event: &storage.LogEventRecord{EventID: "e1", Type: 1}}
	ch := make(chan storage.LogEventRecord) // unbuffered, no reader -> always full
	var dropped uint64

	m.handleNotification(context.Background(), notif(), nil, store, ch, &dropped)
	assert.Equal(t, uint64(1), dropped, "slow consumer must drop, not block the pub/sub loop")
}

func TestHandleNotification_FilteredEventSkipped(t *testing.T) {
	m := &Manager{logger: zap.NewNop()}
	store := &subFakeStore{event: &storage.LogEventRecord{EventID: "e1", Type: 1}}
	ch := make(chan storage.LogEventRecord, 1)
	var dropped uint64

	// Filter wants only type 2; the type-1 notification is skipped entirely.
	m.handleNotification(context.Background(), notif(), []int16{2}, store, ch, &dropped)
	assert.Len(t, ch, 0)
	assert.Equal(t, uint64(0), dropped)
}

func TestShouldInclude(t *testing.T) {
	assert.True(t, shouldInclude(1, nil), "empty filter includes all")
	assert.True(t, shouldInclude(2, []int16{1, 2, 3}))
	assert.False(t, shouldInclude(9, []int16{1, 2, 3}))
}
