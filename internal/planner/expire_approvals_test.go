package planner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/remade/ledger/internal/storage"
)

// approvalStubStore implements storage.Store but defines only the methods
// ExpireStaleApprovals exercises. Any other call panics via the embedded nil
// interface, keeping the test honest about the recovery path's dependencies.
type approvalStubStore struct {
	storage.Store
	expired     []storage.PendingApprovalRecord
	stuck       []storage.PendingApprovalRecord
	eventsByKey map[string][]storage.LogEventRecord
	lookupErr   error

	// stateIfBlocked simulates a concurrent writer having already moved the row,
	// so the guarded UpdateApprovalStateIf reports no update.
	stateIfBlocked bool

	stateUpdates   map[string]string // unconditional (expired-pending path)
	stateIfUpdates map[string]string // guarded (stuck-recovery path)
}

func (s *approvalStubStore) ListExpiredApprovals(context.Context) ([]storage.PendingApprovalRecord, error) {
	return s.expired, nil
}

func (s *approvalStubStore) ListStuckApprovals(context.Context, time.Duration) ([]storage.PendingApprovalRecord, error) {
	return s.stuck, nil
}

func (s *approvalStubStore) ListLogEventsByIdempotencyKey(_ context.Context, _, key string) ([]storage.LogEventRecord, error) {
	if s.lookupErr != nil {
		return nil, s.lookupErr
	}
	return s.eventsByKey[key], nil
}

func (s *approvalStubStore) UpdateApprovalState(_ context.Context, _, intentID, state string) error {
	if s.stateUpdates == nil {
		s.stateUpdates = map[string]string{}
	}
	s.stateUpdates[intentID] = state
	return nil
}

func (s *approvalStubStore) UpdateApprovalStateIf(_ context.Context, _, intentID, _, toState string) (bool, error) {
	if s.stateIfBlocked {
		return false, nil
	}
	if s.stateIfUpdates == nil {
		s.stateIfUpdates = map[string]string{}
	}
	s.stateIfUpdates[intentID] = toState
	return true, nil
}

func newApprovalTestPlanner(store storage.Store) *Planner {
	return &Planner{store: store, logger: zap.NewNop()}
}

func stuckRecord(intentID string) storage.PendingApprovalRecord {
	return storage.PendingApprovalRecord{LedgerID: "L1", IntentID: intentID, State: "executing"}
}

func TestExpireStaleApprovals_RecoversExecutedIntent(t *testing.T) {
	// A stuck approval whose intent produced an event (under its server-derived
	// key) must resolve to "executed", never "expired".
	const intentID = "01INTENT_EXECUTED"
	store := &approvalStubStore{
		stuck: []storage.PendingApprovalRecord{stuckRecord(intentID)},
		eventsByKey: map[string][]storage.LogEventRecord{
			"approval:" + intentID: {{EventID: "e1"}},
		},
	}
	p := newApprovalTestPlanner(store)
	require.NoError(t, p.ExpireStaleApprovals(context.Background(), time.Minute))
	assert.Equal(t, "executed", store.stateIfUpdates[intentID])
}

func TestExpireStaleApprovals_ExpiresUnexecutedIntent(t *testing.T) {
	// No committed events means it never ran, so it is safe to expire.
	const intentID = "01INTENT_NOEVENTS"
	store := &approvalStubStore{
		stuck:       []storage.PendingApprovalRecord{stuckRecord(intentID)},
		eventsByKey: map[string][]storage.LogEventRecord{},
	}
	p := newApprovalTestPlanner(store)
	require.NoError(t, p.ExpireStaleApprovals(context.Background(), time.Minute))
	assert.Equal(t, "expired", store.stateIfUpdates[intentID])
}

func TestExpireStaleApprovals_GuardedTransitionSkipsConcurrentResolution(t *testing.T) {
	// If a concurrent Approve already moved the row out of "executing", the
	// guarded update reports no change and recovery must not clobber it.
	const intentID = "01INTENT_RACED"
	store := &approvalStubStore{
		stuck:          []storage.PendingApprovalRecord{stuckRecord(intentID)},
		eventsByKey:    map[string][]storage.LogEventRecord{}, // recovery would choose "expired"
		stateIfBlocked: true,
	}
	p := newApprovalTestPlanner(store)
	require.NoError(t, p.ExpireStaleApprovals(context.Background(), time.Minute))
	_, recorded := store.stateIfUpdates[intentID]
	assert.False(t, recorded, "must not clobber a concurrently-resolved approval")
}

func TestExpireStaleApprovals_LeavesExecutingWhenStatusUnknown(t *testing.T) {
	// A lookup error must leave the approval "executing" for the next sweep.
	const intentID = "01INTENT_LOOKUPERR"
	store := &approvalStubStore{
		stuck:     []storage.PendingApprovalRecord{stuckRecord(intentID)},
		lookupErr: errors.New("db unavailable"),
	}
	p := newApprovalTestPlanner(store)
	require.NoError(t, p.ExpireStaleApprovals(context.Background(), time.Minute))
	_, recorded := store.stateIfUpdates[intentID]
	assert.False(t, recorded, "state must not change when execution status is unknown")
}

func TestExpireStaleApprovals_ExpiresPastDeadlinePending(t *testing.T) {
	store := &approvalStubStore{
		expired: []storage.PendingApprovalRecord{
			{LedgerID: "L1", IntentID: "01EXPIRED", State: "pending"},
		},
	}
	p := newApprovalTestPlanner(store)
	require.NoError(t, p.ExpireStaleApprovals(context.Background(), time.Minute))
	assert.Equal(t, "expired", store.stateUpdates["01EXPIRED"])
}

func TestApprovalExecutionKey(t *testing.T) {
	// Always server-derived from the intent ID; never the client payload key.
	assert.Equal(t, "approval:01ABC", approvalExecutionKey("01ABC"))
	assert.NotEqual(t, approvalExecutionKey("id-a"), approvalExecutionKey("id-b"))
}
