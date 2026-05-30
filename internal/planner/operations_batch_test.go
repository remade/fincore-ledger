package planner

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/remade/ledger/internal/storage"
)

func postIntent(src, dst string, amount int64) BatchIntent {
	return BatchIntent{Type: "post", Postings: []PostingInput{post(src, dst, amount)}}
}

// invalidPostIntent passes balance (issuer source) but fails validation (bad asset)
// — so it distinguishes "batch now validates" from the prior validation-skipping path.
func invalidPostIntent() BatchIntent {
	return BatchIntent{Type: "post", Postings: []PostingInput{
		{Source: "_world", Destination: "bob", Amount: big.NewInt(50), Asset: "lower!"},
	}}
}

func TestSubmitBatch_AllOrNothing_HappyPath(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	res, err := p.SubmitBatch(context.Background(), "L1",
		[]BatchIntent{postIntent("_world", "alice", 100), postIntent("_world", "bob", 50)}, "ALL_OR_NOTHING")
	require.NoError(t, err)
	assert.Equal(t, 2, res.Successes)
	assert.Len(t, fs.events, 2)
}

// TestSubmitBatch_AllOrNothing_ValidatesAndRollsBack is the key fix: the batch
// path previously skipped input validation, so a malformed posting could be
// written. Now doPostInTx validates, so the whole batch rolls back.
func TestSubmitBatch_AllOrNothing_ValidatesAndRollsBack(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	res, err := p.SubmitBatch(context.Background(), "L1",
		[]BatchIntent{postIntent("_world", "alice", 100), invalidPostIntent()}, "ALL_OR_NOTHING")
	require.Error(t, err)
	require.NotNil(t, res)
	assert.True(t, res.Failed)
	assert.Equal(t, 1, res.FailedAt)
	assert.Empty(t, fs.events, "ALL_OR_NOTHING must roll back entirely on the invalid intent")
}

func TestSubmitBatch_AllOrNothing_RollsBackOnInsufficientFunds(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	// Second intent: alice (non-issuer, no funds) tries to send -> insufficient.
	res, err := p.SubmitBatch(context.Background(), "L1",
		[]BatchIntent{postIntent("_world", "alice", 100), postIntent("alice", "bob", 999)}, "ALL_OR_NOTHING")
	require.Error(t, err)
	assert.True(t, res.Failed)
	assert.Empty(t, fs.events, "no partial commit")
}

func TestSubmitBatch_BestEffort_ContinuesPastFailure(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	res, err := p.SubmitBatch(context.Background(), "L1", []BatchIntent{
		postIntent("_world", "alice", 100),
		invalidPostIntent(),
		postIntent("_world", "carol", 25),
	}, "BEST_EFFORT")
	require.NoError(t, err)
	assert.Equal(t, 2, res.Successes)
	assert.Equal(t, 1, res.Failures)
	assert.Len(t, fs.events, 2, "the two valid intents commit; the invalid one is skipped")
	assert.False(t, res.Results[0].Success == false)
	assert.False(t, res.Results[1].Success)
	assert.True(t, res.Results[2].Success)
}

func TestSubmitBatch_SealedLedgerRejected(t *testing.T) {
	fs := newFakeStore()
	sealed := openLedger("L1", "_world")
	sealed.State = "sealed"
	fs.addLedger(sealed)
	p := newPostTestPlanner(fs)

	_, err := p.SubmitBatch(context.Background(), "L1",
		[]BatchIntent{postIntent("_world", "alice", 100)}, "ALL_OR_NOTHING")
	require.ErrorIs(t, err, storage.ErrLedgerSealed)
}
