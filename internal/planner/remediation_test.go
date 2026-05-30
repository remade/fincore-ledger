package planner

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/remade/ledger/internal/storage"
)

// These tests cover the remediation fixes from the codebase review:
//   - idempotency-key-conflict recovery on the non-Post write paths, and
//   - the ExpireHolds no-progress guard.

// seedConcurrentWinner marks an idempotency key as already inserted by a
// concurrent writer, so the next AppendLogEvent for that key conflicts while the
// initial pre-check (which reads the IK record, not yet present) still misses.
func seedConcurrentWinner(fs *fakeStore, ledgerID, key string) {
	fs.ikSeen[ledgerID+"|"+key] = true
}

func TestSubmitConvert_IdempotencyConflictResolvesToHit(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.setBalance("L1", "alice", "USD", 1000, 0)
	seedConcurrentWinner(fs, "L1", "key-1")
	p := newPostTestPlanner(fs)

	res, err := p.SubmitConvert(context.Background(), "L1",
		convertParams("alice", "market", 100, "USD", 90, "EUR"), "key-1")

	require.NoError(t, err, "a concurrent same-key commit must resolve to an idempotent hit, not an error")
	require.NotNil(t, res)
	assert.True(t, res.IdempotentHit)
	assert.Equal(t, "concurrent-winner", res.EventID)
	assert.Empty(t, fs.events, "no new event is committed on the conflict path")
}

func TestSubmitPost_IdempotencyConflictResolvesToHit(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	seedConcurrentWinner(fs, "L1", "key-1")
	p := newPostTestPlanner(fs)

	res, err := p.SubmitPost(context.Background(), "L1",
		[]PostingInput{post("_world", "alice", 100)}, "ref-1", nil, "key-1", nil, false)

	require.NoError(t, err)
	require.NotNil(t, res)
	assert.True(t, res.IdempotentHit)
	assert.Equal(t, "concurrent-winner", res.EventID)
	assert.Empty(t, fs.events)
}

func TestSubmitSetMetadata_IdempotencyConflictResolvesToHit(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	seedConcurrentWinner(fs, "L1", "key-1")
	p := newPostTestPlanner(fs)

	res, err := p.SubmitSetMetadata(context.Background(), "L1",
		storage.TargetTypeTransaction, "tx-1", map[string]any{"k": "v"}, "key-1")

	require.NoError(t, err)
	require.NotNil(t, res)
	assert.True(t, res.IdempotentHit)
	assert.Equal(t, "concurrent-winner", res.EventID)
	assert.Empty(t, fs.events)
}

func TestMicroFakeExpireHoldCommits(t *testing.T) {
	fs := newFakeStore()
	fs.holdRecords["L1|h"] = &storage.HoldRecord{LedgerID: "L1", HoldID: "h"}
	tx, _ := fs.BeginTx(context.Background())
	require.NoError(t, tx.ExpireHold(context.Background(), "L1", "h"))
	require.NoError(t, tx.Commit())
	require.True(t, fs.holdRecords["L1|h"].Expired, "MICRO: Commit must apply the ExpireHold write")
}

func TestExpireHolds_ExpiresReleasableHold(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1"))
	fs.holdRecords["L1|good"] = &storage.HoldRecord{
		LedgerID: "L1", HoldID: "good", Source: "alice", Asset: "USD",
		AuthorizedAmount: big.NewInt(10), CapturedAmount: big.NewInt(0),
	}
	p := newPostTestPlanner(fs)

	exp, lerr := fs.ListExpiredHolds(context.Background())
	require.NoError(t, lerr)
	require.Len(t, exp, 1, "DEBUG: ListExpiredHolds should return the seeded hold")
	require.NoError(t, p.expireSingleHold(context.Background(), exp[0]), "DEBUG: expireSingleHold should succeed")
	assert.True(t, fs.holdRecords["L1|good"].Expired, "a releasable hold is expired")
	require.Len(t, fs.events, 1, "exactly one expiry event is committed")
	assert.Equal(t, storage.EventTypeHoldExpired, fs.events[0].Type)
}

// A full page of holds that all fail to expire must NOT cause the sweep to
// re-fetch the same page forever: the no-progress guard stops after one pass.
func TestExpireHolds_NoProgressDoesNotSpin(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1"))
	for i := 0; i < expiredHoldsBatchSize; i++ {
		id := fmt.Sprintf("hold-%d", i)
		fs.holdRecords["L1|"+id] = &storage.HoldRecord{
			LedgerID: "L1", HoldID: id, Source: "alice", Asset: "USD",
			AuthorizedAmount: big.NewInt(10), CapturedAmount: big.NewInt(0),
		}
		fs.expireHoldFails["L1|"+id] = true
	}
	p := newPostTestPlanner(fs)

	require.NoError(t, p.ExpireHolds(context.Background()))
	assert.Equal(t, 1, fs.listExpiredCalls,
		"a full page that makes no progress must stop after a single fetch")
	assert.Empty(t, fs.events, "no expiry events committed for poison holds")
}
