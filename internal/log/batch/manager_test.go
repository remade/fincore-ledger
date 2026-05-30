package batch

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/remade/ledger/internal/storage"
)

// batchFakeStore implements storage.Store with only the methods closeBatch uses.
type batchFakeStore struct {
	storage.Store
	events        []storage.LogEventRecord
	closeFailLeft int // fail CloseBatch this many times, then succeed
	closeAttempts int
	closed        bool
}

func (s *batchFakeStore) ListBatchEvents(context.Context, string) ([]storage.LogEventRecord, error) {
	return s.events, nil
}

func (s *batchFakeStore) CloseBatch(context.Context, string, []byte, int) error {
	s.closeAttempts++
	if s.closeFailLeft > 0 {
		s.closeFailLeft--
		return errors.New("transient close failure")
	}
	s.closed = true
	return nil
}

func newTestManager(store storage.Store) *Manager {
	return &Manager{
		store:        store,
		logger:       zap.NewNop(),
		maxEvents:    DefaultMaxEvents,
		maxAge:       DefaultMaxAge,
		ledgerLocks:  map[string]*sync.Mutex{},
		openBatch:    map[string]*openBatchState{},
		pendingClose: map[string]bool{},
		shutdownCtx:  context.Background(),
	}
}

func oneEvent() []storage.LogEventRecord {
	return []storage.LogEventRecord{{EventID: "e1", Type: 1, Payload: []byte("x")}}
}

func TestCloseBatchWithRetry_SucceedsAfterTransientFailures(t *testing.T) {
	store := &batchFakeStore{events: oneEvent(), closeFailLeft: 2}
	m := newTestManager(store)

	m.closeBatchWithRetry(context.Background(), "L1", "batch-1")
	assert.True(t, store.closed, "batch eventually closed")
	assert.Equal(t, 3, store.closeAttempts, "2 failures + 1 success")
	assert.False(t, m.pendingClose["batch-1"], "unmarked after completion")
}

func TestCloseBatchWithRetry_ExhaustsAndUnmarks(t *testing.T) {
	store := &batchFakeStore{events: oneEvent(), closeFailLeft: 1000} // always fails
	m := newTestManager(store)

	m.closeBatchWithRetry(context.Background(), "L1", "batch-1")
	assert.False(t, store.closed)
	assert.Equal(t, maxCloseRetries+1, store.closeAttempts, "first attempt + maxCloseRetries")
	assert.False(t, m.pendingClose["batch-1"], "unmarked so a later sweep can retry")
}

func TestCloseBatchWithRetry_SkipsWhenAlreadyPending(t *testing.T) {
	store := &batchFakeStore{events: oneEvent()}
	m := newTestManager(store)
	require.True(t, m.markPendingClose("batch-1")) // simulate a concurrent close in flight

	m.closeBatchWithRetry(context.Background(), "L1", "batch-1")
	assert.Equal(t, 0, store.closeAttempts, "must not race a close already in flight")
}

func TestMarkPendingClose_Dedup(t *testing.T) {
	m := newTestManager(&batchFakeStore{})
	assert.True(t, m.markPendingClose("b1"))
	assert.False(t, m.markPendingClose("b1"), "second claim blocked")
	m.unmarkPendingClose("b1")
	assert.True(t, m.markPendingClose("b1"), "available again after unmark")
}

func TestCloseBatch_EmptyIsNoop(t *testing.T) {
	store := &batchFakeStore{events: nil}
	m := newTestManager(store)

	require.NoError(t, m.closeBatch(context.Background(), "L1", "b1"))
	assert.False(t, store.closed)
	assert.Equal(t, 0, store.closeAttempts, "CloseBatch not called for an empty batch")
}

func TestCloseBatch_HappyPath(t *testing.T) {
	store := &batchFakeStore{events: oneEvent()}
	m := newTestManager(store)

	require.NoError(t, m.closeBatch(context.Background(), "L1", "b1"))
	assert.True(t, store.closed)
	assert.Equal(t, 1, store.closeAttempts)
}
