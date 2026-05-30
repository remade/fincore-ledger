package planner

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/remade/ledger/internal/storage"
)

func originalTx(ledgerID, txID, src, dst, amount, asset string) *storage.TransactionRecord {
	return &storage.TransactionRecord{
		LedgerID: ledgerID, TransactionID: txID, ValidTime: time.Unix(1000, 0).UTC(),
		Postings: []map[string]any{
			{"source": src, "destination": dst, "amount": amount, "asset": asset},
		},
		Metadata: map[string]any{},
	}
}

func TestSubmitRevert_HappyPath(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.setBalance("L1", "bob", "USD", 100, 0) // bob must be able to give it back
	fs.addTransaction(originalTx("L1", "tx-orig", "alice", "bob", "100", "USD"))
	p := newPostTestPlanner(fs)

	res, err := p.SubmitRevert(context.Background(), "L1", "tx-orig", false, false, "mistake", "")
	require.NoError(t, err)
	assert.NotEmpty(t, res.EventID)
	require.NotNil(t, res.Transaction)
	// Reversed direction: bob -> alice 100.
	assert.Equal(t, int64(100), fs.getBalance("L1", "bob", "USD").output.Int64())
	assert.Equal(t, int64(100), fs.getBalance("L1", "alice", "USD").input.Int64())
}

func TestSubmitRevert_DoubleRevertRejected(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.setBalance("L1", "bob", "USD", 100, 0)
	fs.addTransaction(originalTx("L1", "tx-orig", "alice", "bob", "100", "USD"))
	p := newPostTestPlanner(fs)

	_, err := p.SubmitRevert(context.Background(), "L1", "tx-orig", false, false, "", "")
	require.NoError(t, err)
	_, err = p.SubmitRevert(context.Background(), "L1", "tx-orig", true, false, "", "")
	require.ErrorIs(t, err, storage.ErrAlreadyReverted)
}

func TestSubmitRevert_ForceSkipsBalanceCheck(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	// bob has NO balance, but force=true bypasses the insufficient-funds guard.
	fs.addTransaction(originalTx("L1", "tx-orig", "alice", "bob", "100", "USD"))
	p := newPostTestPlanner(fs)

	res, err := p.SubmitRevert(context.Background(), "L1", "tx-orig", true, false, "", "")
	require.NoError(t, err)
	assert.NotEmpty(t, res.EventID)
}

func TestSubmitRevert_InsufficientWithoutForce(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.addTransaction(originalTx("L1", "tx-orig", "alice", "bob", "100", "USD"))
	p := newPostTestPlanner(fs)

	_, err := p.SubmitRevert(context.Background(), "L1", "tx-orig", false, false, "", "")
	require.ErrorIs(t, err, storage.ErrInsufficientFunds)
}

func TestSubmitRevert_MissingOriginalRejected(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)
	_, err := p.SubmitRevert(context.Background(), "L1", "nope", false, false, "", "")
	require.Error(t, err)
}

func TestSubmitAmend_HappyPath(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.addTransaction(originalTx("L1", "tx-orig", "alice", "bob", "100", "USD"))
	p := newPostTestPlanner(fs)

	res, err := p.SubmitAmend(context.Background(), "L1", "tx-orig", map[string]any{"note": "reviewed"}, "")
	require.NoError(t, err)
	assert.NotEmpty(t, res.EventID)
	assert.Equal(t, "reviewed", fs.txByID["L1|tx-orig"].Metadata["note"])
}

func TestSubmitAmend_MissingTxRejected(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)
	_, err := p.SubmitAmend(context.Background(), "L1", "nope", map[string]any{"x": "y"}, "")
	require.Error(t, err)
}
