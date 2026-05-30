package planner

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/remade/ledger/internal/storage"
)

func TestEvaluatePolicy_NoPolicyAllows(t *testing.T) {
	fs := newFakeStore()
	p := newPostTestPlanner(fs)
	// No active policy => unrestricted.
	require.NoError(t, p.EvaluatePolicy(context.Background(), "L1", "alice", "post", []string{"users:1"}))
}

func TestEvaluatePolicy_DenyRuleBlocks(t *testing.T) {
	fs := newFakeStore()
	fs.setActivePolicy("deny alice post *")
	p := newPostTestPlanner(fs)

	err := p.EvaluatePolicy(context.Background(), "L1", "alice", "post", []string{"users:1"})
	require.Error(t, err)
	require.ErrorIs(t, err, storage.ErrPolicyDenied)

	// A non-matching principal is allowed by the same policy.
	require.NoError(t, p.EvaluatePolicy(context.Background(), "L1", "bob", "post", []string{"users:1"}))
}

// The headline task-022 behavior: a malformed active policy must FAIL CLOSED
// (block the operation) rather than silently evaluating to allow.
func TestEvaluatePolicy_MalformedPolicyFailsClosed(t *testing.T) {
	fs := newFakeStore()
	fs.setActivePolicy("permit everyone") // not a recognized deny-list rule
	p := newPostTestPlanner(fs)

	err := p.EvaluatePolicy(context.Background(), "L1", "alice", "post", []string{"users:1"})
	require.Error(t, err, "a malformed policy must block, not allow")
	assert.Contains(t, err.Error(), "malformed")
}

func TestWritePolicyDenialEvent_WritesAuditEvent(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	require.NoError(t, p.WritePolicyDenialEvent(context.Background(), "L1", "alice", "denied by rule"))
	require.Len(t, fs.events, 1)
	assert.Equal(t, storage.EventTypePolicyDenied, fs.events[0].Type)
}
