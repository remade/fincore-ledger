package planner

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/remade/ledger/internal/storage"
)

// flakyAuditStore makes the audit-event AppendLogEvent fail a configurable number
// of times so the bounded-retry path in recordApprovalAuditEvent can be tested.
// appendCalls counts every AppendLogEvent attempt (across the per-retry BeginTx
// calls) so a test can prove the retry loop actually ran the expected number of
// times rather than passing vacuously.
type flakyAuditStore struct {
	*fakeStore
	failsLeft   int
	appendCalls int
}

func (s *flakyAuditStore) BeginTx(context.Context) (storage.TxStore, error) {
	return &flakyAuditTx{fakeTx: &fakeTx{parent: s.fakeStore}, owner: s}, nil
}

type flakyAuditTx struct {
	*fakeTx
	owner *flakyAuditStore
}

func (t *flakyAuditTx) AppendLogEvent(ctx context.Context, e storage.LogEventRecord) error {
	t.owner.appendCalls++
	if t.owner.failsLeft > 0 {
		t.owner.failsLeft--
		return errors.New("transient append failure")
	}
	return t.fakeTx.AppendLogEvent(ctx, e)
}

func newAuditTestPlanner(store storage.Store, logger *zap.Logger) *Planner {
	return &Planner{store: store, batch: &fakeBatch{}, redis: newFakeCache(), logger: logger}
}

func TestRecordApprovalAuditEvent_Success(t *testing.T) {
	fs := newFakeStore()
	store := &flakyAuditStore{fakeStore: fs}
	p := newAuditTestPlanner(store, zap.NewNop())

	p.recordApprovalAuditEvent(context.Background(), "L1", "intent-1", []byte(`{}`))

	require.Len(t, fs.events, 1)
	assert.Equal(t, storage.EventTypeApprovalRecorded, fs.events[0].Type)
}

func TestRecordApprovalAuditEvent_RetriesTransientFailure(t *testing.T) {
	fs := newFakeStore()
	store := &flakyAuditStore{fakeStore: fs, failsLeft: 2} // fail twice, succeed on the third attempt
	p := newAuditTestPlanner(store, zap.NewNop())

	p.recordApprovalAuditEvent(context.Background(), "L1", "intent-1", []byte(`{}`))

	require.Len(t, fs.events, 1, "audit event must eventually be written after transient failures")
	assert.Equal(t, 0, store.failsLeft)
	assert.Equal(t, 3, store.appendCalls, "should have attempted exactly 3 times (2 failures + 1 success)")
}

func TestRecordApprovalAuditEvent_ExhaustionLogsAuditGap(t *testing.T) {
	fs := newFakeStore()
	// Never succeeds: more failures than the retry budget so exhaustion is hit.
	store := &flakyAuditStore{fakeStore: fs, failsLeft: maxAuditRetries + 5}
	core, logs := observer.New(zapcore.ErrorLevel)
	p := newAuditTestPlanner(store, zap.New(core))

	p.recordApprovalAuditEvent(context.Background(), "L1", "intent-1", []byte(`{}`))

	assert.Empty(t, fs.events, "no event committed when all attempts fail")
	// Prove the bounded retry actually ran the full budget (initial + maxAuditRetries),
	// so a regression that short-circuited or removed the loop would fail here.
	assert.Equal(t, maxAuditRetries+1, store.appendCalls, "must attempt exactly maxAuditRetries+1 times before giving up")

	// The AUDIT_GAP must be the specific exhaustion log, carrying structured
	// recovery context -- not merely some Error line elsewhere.
	gapLogs := logs.FilterMessageSnippet("AUDIT_GAP").All()
	require.Len(t, gapLogs, 1, "exhausted retries must log exactly one AUDIT_GAP")
	fields := gapLogs[0].ContextMap()
	assert.Equal(t, "intent-1", fields["intent_id"])
	assert.Equal(t, int64(maxAuditRetries+1), fields["attempts"])
}
