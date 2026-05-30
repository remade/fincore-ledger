package planner

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// postIntentPayload serializes a single-post BatchIntent as an approval payload.
func postIntentPayload(t *testing.T, src, dst string, amount int64) []byte {
	t.Helper()
	b, err := json.Marshal(BatchIntent{Type: "post", Postings: []PostingInput{post(src, dst, amount)}})
	require.NoError(t, err)
	return b
}

func TestSubmitForApproval_ParksIntent(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	id, err := p.SubmitForApproval(context.Background(), "L1",
		postIntentPayload(t, "_world", "alice", 100), []string{"approver-1"}, "submitter", time.Hour)
	require.NoError(t, err)
	require.NotEmpty(t, id)
	rec := fs.approvals["L1|"+id]
	require.NotNil(t, rec)
	assert.Equal(t, "pending", rec.State)
	assert.Equal(t, "submitter", rec.SubmittedBy)
}

func TestApprove_SelfApprovalRejected(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	id, err := p.SubmitForApproval(context.Background(), "L1",
		postIntentPayload(t, "_world", "alice", 100), []string{"submitter"}, "submitter", time.Hour)
	require.NoError(t, err)

	_, err = p.Approve(context.Background(), "L1", id, "submitter", "sig")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot approve their own intent")
	assert.Equal(t, "pending", fs.approvals["L1|"+id].State, "rejected self-approval leaves it pending")
}

func TestApprove_NonRequiredApproverRejected(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	id, err := p.SubmitForApproval(context.Background(), "L1",
		postIntentPayload(t, "_world", "alice", 100), []string{"approver-1"}, "submitter", time.Hour)
	require.NoError(t, err)

	_, err = p.Approve(context.Background(), "L1", id, "stranger", "sig")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a required approver")
}

func TestApprove_ExpiredRejected(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	id, err := p.SubmitForApproval(context.Background(), "L1",
		postIntentPayload(t, "_world", "alice", 100), []string{"approver-1"}, "submitter", time.Hour)
	require.NoError(t, err)
	// Force expiry.
	fs.approvals["L1|"+id].ExpiresAt = time.Now().Add(-time.Minute)

	_, err = p.Approve(context.Background(), "L1", id, "approver-1", "sig")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expired")
	assert.Equal(t, "expired", fs.approvals["L1|"+id].State)
}

// Quorum: with two required approvers, the first approval leaves it pending and
// the second triggers execution of the underlying intent.
func TestApprove_QuorumExecutesIntent(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	id, err := p.SubmitForApproval(context.Background(), "L1",
		postIntentPayload(t, "_world", "alice", 100), []string{"approver-1", "approver-2"}, "submitter", time.Hour)
	require.NoError(t, err)

	// First approval: still pending, nothing executed.
	_, err = p.Approve(context.Background(), "L1", id, "approver-1", "sig1")
	require.NoError(t, err)
	assert.Equal(t, "pending", fs.approvals["L1|"+id].State)
	assert.Empty(t, fs.events, "no execution until quorum is met")

	// Second approval: quorum reached -> intent executes.
	res, err := p.Approve(context.Background(), "L1", id, "approver-2", "sig2")
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, "executed", fs.approvals["L1|"+id].State)
	assert.Equal(t, int64(100), fs.getBalance("L1", "alice", "USD").input.Int64(), "approved post applied")
	// The executed financial event carries the deterministic approval key.
	evs, _ := fs.ListLogEventsByIdempotencyKey(context.Background(), "L1", approvalExecutionKey(id))
	assert.NotEmpty(t, evs, "executed event recorded under the approval idempotency key")
}

func TestApprove_DuplicateApprovalRejected(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	id, err := p.SubmitForApproval(context.Background(), "L1",
		postIntentPayload(t, "_world", "alice", 100), []string{"approver-1", "approver-2"}, "submitter", time.Hour)
	require.NoError(t, err)

	_, err = p.Approve(context.Background(), "L1", id, "approver-1", "sig")
	require.NoError(t, err)
	_, err = p.Approve(context.Background(), "L1", id, "approver-1", "sig-again")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already approved")
}
