package planner

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/remade/ledger/internal/storage"
)

// These exercise the ALL_OR_NOTHING batch path for every operation type, which
// routes through executeIntentInTx -> execute*InTx against a single shared tx.

func TestSubmitBatch_Capture(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.addHold(heldRecord("L1", "h1", "alice", "merchant", "USD", 100, 0))
	p := newPostTestPlanner(fs)

	res, err := p.SubmitBatch(context.Background(), "L1", []BatchIntent{
		{Type: "capture", HoldID: "h1", Amount: big.NewInt(60), Destination: "merchant"},
	}, "ALL_OR_NOTHING")
	require.NoError(t, err)
	assert.Equal(t, 1, res.Successes)
	assert.Equal(t, int64(60), fs.holdRecords["L1|h1"].CapturedAmount.Int64())
}

func TestSubmitBatch_Void(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.addHold(heldRecord("L1", "h1", "alice", "merchant", "USD", 100, 0))
	p := newPostTestPlanner(fs)

	res, err := p.SubmitBatch(context.Background(), "L1", []BatchIntent{
		{Type: "void", HoldID: "h1"},
	}, "ALL_OR_NOTHING")
	require.NoError(t, err)
	assert.Equal(t, 1, res.Successes)
	assert.True(t, fs.holdRecords["L1|h1"].Voided)
}

func TestSubmitBatch_Revert(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.addTransaction(originalTx("L1", "tx-orig", "alice", "bob", "100", "USD"))
	p := newPostTestPlanner(fs)

	// Force=true skips the balance check so the reversal applies unconditionally.
	res, err := p.SubmitBatch(context.Background(), "L1", []BatchIntent{
		{Type: "revert", OriginalTxID: "tx-orig", Force: true, Reason: "mistake"},
	}, "ALL_OR_NOTHING")
	require.NoError(t, err)
	assert.Equal(t, 1, res.Successes)
	// Assert the actual reversal state change, not merely success: the reversing
	// transaction books the swapped direction (bob -> alice 100).
	assert.Equal(t, int64(100), fs.getBalance("L1", "bob", "USD").output.Int64())
	assert.Equal(t, int64(100), fs.getBalance("L1", "alice", "USD").input.Int64())
}

func TestSubmitBatch_Amend(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.addTransaction(originalTx("L1", "tx-orig", "alice", "bob", "100", "USD"))
	p := newPostTestPlanner(fs)

	res, err := p.SubmitBatch(context.Background(), "L1", []BatchIntent{
		{Type: "amend", OriginalTxID: "tx-orig", Metadata: map[string]any{"note": "reviewed"}},
	}, "ALL_OR_NOTHING")
	require.NoError(t, err)
	assert.Equal(t, 1, res.Successes)
	assert.Equal(t, "reviewed", fs.txByID["L1|tx-orig"].Metadata["note"])
}

func TestSubmitBatch_DeleteMetadata(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.addTransaction(&storage.TransactionRecord{LedgerID: "L1", TransactionID: "tx1", Metadata: map[string]any{"a": "1", "b": "2"}})
	p := newPostTestPlanner(fs)

	res, err := p.SubmitBatch(context.Background(), "L1", []BatchIntent{
		{Type: "delete_metadata", TargetType: targetTransaction, TargetID: "tx1", MetadataKey: "a"},
	}, "ALL_OR_NOTHING")
	require.NoError(t, err)
	assert.Equal(t, 1, res.Successes)
	_, hasA := fs.txByID["L1|tx1"].Metadata["a"]
	assert.False(t, hasA)
}

// ALL_OR_NOTHING rolls the whole batch back when a non-post intent fails midway:
// the earlier successful post must NOT survive the failing capture. (The
// post-then-post rollback is already covered by operations_batch_test.go; this
// exercises rollback after an execute*InTx other than post.)
func TestSubmitBatch_AllOrNothing_RollsBackAfterCaptureFailure(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	res, err := p.SubmitBatch(context.Background(), "L1", []BatchIntent{
		{Type: "post", Postings: []PostingInput{post("_world", "alice", 100)}},
		{Type: "capture", HoldID: "does-not-exist", Amount: big.NewInt(10), Destination: "m"},
	}, "ALL_OR_NOTHING")
	require.Error(t, err)
	assert.True(t, res.Failed)
	assert.Equal(t, 1, res.FailedAt)
	assert.Empty(t, fs.events, "no events committed when the batch rolls back")
	assert.Equal(t, int64(0), fs.getBalance("L1", "alice", "USD").input.Int64())
}
