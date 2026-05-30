package planner

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/remade/ledger/internal/storage"
)

const (
	targetAccount     int16 = 0
	targetTransaction int16 = 1
)

func TestSubmitSetMetadata_Transaction(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.addTransaction(&storage.TransactionRecord{LedgerID: "L1", TransactionID: "tx1", Metadata: map[string]any{}})
	p := newPostTestPlanner(fs)

	res, err := p.SubmitSetMetadata(context.Background(), "L1", targetTransaction, "tx1", map[string]any{"note": "hello"}, "")
	require.NoError(t, err)
	assert.NotEmpty(t, res.EventID)
	assert.Len(t, fs.events, 1)
	assert.Equal(t, storage.EventTypeMetadataSet, fs.events[0].Type)
	assert.Equal(t, "hello", fs.txByID["L1|tx1"].Metadata["note"])
}

func TestSubmitSetMetadata_Account(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	res, err := p.SubmitSetMetadata(context.Background(), "L1", targetAccount, "alice", map[string]any{"tier": "gold"}, "")
	require.NoError(t, err)
	assert.NotEmpty(t, res.EventID)
	require.NotNil(t, fs.accountRecords["L1|alice"])
	assert.Equal(t, "gold", fs.accountRecords["L1|alice"].Metadata["tier"])
}

func TestSubmitDeleteMetadata_Transaction(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.addTransaction(&storage.TransactionRecord{LedgerID: "L1", TransactionID: "tx1", Metadata: map[string]any{"a": "1", "b": "2"}})
	p := newPostTestPlanner(fs)

	res, err := p.SubmitDeleteMetadata(context.Background(), "L1", targetTransaction, "tx1", "a", "")
	require.NoError(t, err)
	assert.NotEmpty(t, res.EventID)
	assert.Equal(t, storage.EventTypeMetadataDeleted, fs.events[0].Type)
	_, hasA := fs.txByID["L1|tx1"].Metadata["a"]
	assert.False(t, hasA, "key a removed")
	assert.Equal(t, "2", fs.txByID["L1|tx1"].Metadata["b"], "other keys retained")
}

func TestSubmitSetMetadata_SealedLedgerRejected(t *testing.T) {
	fs := newFakeStore()
	sealed := openLedger("L1", "_world")
	sealed.State = "sealed"
	fs.addLedger(sealed)
	p := newPostTestPlanner(fs)
	_, err := p.SubmitSetMetadata(context.Background(), "L1", targetTransaction, "tx1", map[string]any{"x": "y"}, "")
	require.Error(t, err)
}

// Batch set-metadata now records metadata history (previously skipped) via the
// shared core; verify the batch path succeeds end to end.
func TestSubmitBatch_SetMetadata(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.addTransaction(&storage.TransactionRecord{LedgerID: "L1", TransactionID: "tx1", Metadata: map[string]any{}})
	p := newPostTestPlanner(fs)

	res, err := p.SubmitBatch(context.Background(), "L1", []BatchIntent{
		{Type: "set_metadata", TargetType: targetTransaction, TargetID: "tx1", Metadata: map[string]any{"k": "v"}},
	}, "ALL_OR_NOTHING")
	require.NoError(t, err)
	assert.Equal(t, 1, res.Successes)
	assert.Equal(t, "v", fs.txByID["L1|tx1"].Metadata["k"])
}
