package planner

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/remade/ledger/internal/policy"
	"github.com/remade/ledger/internal/storage"
)

// These are characterization tests: they pin the OBSERVABLE behavior of
// SubmitPost so the task-017 extraction (moving core logic into a shared
// doPostInTx) can be verified behavior-preserving.

func openLedger(id string, issuers ...string) *storage.LedgerRecord {
	return &storage.LedgerRecord{ID: id, State: "open", Features: map[string]string{}, IssuerAccounts: issuers}
}

func newPostTestPlanner(store storage.Store) *Planner {
	return &Planner{
		store:     store,
		batch:     &fakeBatch{},
		redis:     newFakeCache(),
		evaluator: &policy.SimpleEvaluator{},
		logger:    zap.NewNop(),
	}
}

func post(src, dst string, amount int64) PostingInput {
	return PostingInput{Source: src, Destination: dst, Amount: big.NewInt(amount), Asset: "USD"}
}

func TestSubmitPost_HappyPath(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	res, err := p.SubmitPost(context.Background(), "L1",
		[]PostingInput{post("_world", "alice", 100)}, "ref-1", nil, "", nil, false)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.NotEmpty(t, res.EventID)
	require.NotNil(t, res.Transaction)
	assert.Len(t, fs.events, 1, "one event committed")
	assert.Equal(t, storage.EventTypeTransactionPosted, fs.events[0].Type)
	assert.Equal(t, int64(100), fs.getBalance("L1", "alice", "USD").input.Int64())
	assert.Equal(t, int64(100), fs.getBalance("L1", "_world", "USD").output.Int64())
}

func TestSubmitPost_ValidationRejectsBadInput(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	tests := []struct {
		name    string
		posting PostingInput
	}{
		{"bad source", PostingInput{Source: "bad acct!", Destination: "alice", Amount: big.NewInt(1), Asset: "USD"}},
		{"bad destination", PostingInput{Source: "_world", Destination: "no spaces!", Amount: big.NewInt(1), Asset: "USD"}},
		{"bad asset", PostingInput{Source: "_world", Destination: "alice", Amount: big.NewInt(1), Asset: "lower!"}},
		{"zero amount", PostingInput{Source: "_world", Destination: "alice", Amount: big.NewInt(0), Asset: "USD"}},
		{"negative amount", PostingInput{Source: "_world", Destination: "alice", Amount: big.NewInt(-5), Asset: "USD"}},
		{"nil amount", PostingInput{Source: "_world", Destination: "alice", Amount: nil, Asset: "USD"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := p.SubmitPost(context.Background(), "L1", []PostingInput{tt.posting}, "", nil, "", nil, false)
			require.Error(t, err)
			assert.Empty(t, fs.events, "no event on validation failure")
		})
	}
}

func TestSubmitPost_EmptyPostingsRejected(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)
	_, err := p.SubmitPost(context.Background(), "L1", nil, "", nil, "", nil, false)
	require.Error(t, err)
}

func TestSubmitPost_InsufficientFunds(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	// alice has no funds and is not an issuer.
	_, err := p.SubmitPost(context.Background(), "L1",
		[]PostingInput{post("alice", "bob", 50)}, "", nil, "", nil, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, storage.ErrInsufficientFunds)
	assert.Empty(t, fs.events)
}

func TestSubmitPost_IssuerSourceBypassesBalanceCheck(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	// _world has no positive balance but is an issuer -> allowed to go negative.
	res, err := p.SubmitPost(context.Background(), "L1",
		[]PostingInput{post("_world", "alice", 1000)}, "", nil, "", nil, false)
	require.NoError(t, err)
	assert.NotEmpty(t, res.EventID)
}

func TestSubmitPost_SufficientAfterFunding(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.setBalance("L1", "alice", "USD", 100, 0) // alice has 100 net
	p := newPostTestPlanner(fs)

	res, err := p.SubmitPost(context.Background(), "L1",
		[]PostingInput{post("alice", "bob", 60)}, "", nil, "", nil, false)
	require.NoError(t, err)
	assert.NotEmpty(t, res.EventID)
	// alice spent 60 -> output 60; bob received 60 -> input 60.
	assert.Equal(t, int64(60), fs.getBalance("L1", "alice", "USD").output.Int64())
	assert.Equal(t, int64(60), fs.getBalance("L1", "bob", "USD").input.Int64())
}

func TestSubmitPost_DryRunNoWrites(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	res, err := p.SubmitPost(context.Background(), "L1",
		[]PostingInput{post("_world", "alice", 100)}, "", nil, "", nil, true)
	require.NoError(t, err)
	assert.Equal(t, "dry-run", res.EventID)
	assert.Empty(t, fs.events, "dry-run writes nothing")
	assert.Equal(t, int64(0), fs.getBalance("L1", "alice", "USD").input.Int64())
}

func TestSubmitPost_IdempotentReplay_PGHit(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	postings := []PostingInput{post("_world", "alice", 100)}
	first, err := p.SubmitPost(context.Background(), "L1", postings, "", nil, "key-1", nil, false)
	require.NoError(t, err)
	require.Len(t, fs.events, 1)

	// Replay with the same key + same inputs -> idempotent hit, no new event.
	second, err := p.SubmitPost(context.Background(), "L1", postings, "", nil, "key-1", nil, false)
	require.NoError(t, err)
	assert.True(t, second.IdempotentHit)
	assert.Equal(t, first.EventID, second.EventID)
	assert.Len(t, fs.events, 1, "no duplicate event on replay")
}

func TestSubmitPost_IdempotencyHashMismatch(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	_, err := p.SubmitPost(context.Background(), "L1",
		[]PostingInput{post("_world", "alice", 100)}, "", nil, "key-1", nil, false)
	require.NoError(t, err)

	// Same key, DIFFERENT inputs -> invalid idempotency input.
	_, err = p.SubmitPost(context.Background(), "L1",
		[]PostingInput{post("_world", "alice", 200)}, "", nil, "key-1", nil, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, storage.ErrInvalidIdempotencyInput)
}

func TestSubmitPost_SealedLedgerRejected(t *testing.T) {
	fs := newFakeStore()
	sealed := openLedger("L1", "_world")
	sealed.State = "sealed"
	fs.addLedger(sealed)
	p := newPostTestPlanner(fs)

	_, err := p.SubmitPost(context.Background(), "L1",
		[]PostingInput{post("_world", "alice", 100)}, "", nil, "", nil, false)
	require.Error(t, err)
}
