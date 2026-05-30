package planner

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/remade/ledger/internal/storage"
)

func futureTime() time.Time { return time.Unix(1<<40, 0).UTC() }

func heldRecord(ledgerID, holdID, source, dest, asset string, authorized, captured int64) *storage.HoldRecord {
	return &storage.HoldRecord{
		LedgerID: ledgerID, HoldID: holdID, Source: source, DestinationHint: dest, Asset: asset,
		AuthorizedAmount: big.NewInt(authorized), CapturedAmount: big.NewInt(captured),
		ExpiresAt: futureTime(),
	}
}

func TestSubmitAuthorize_HappyPath(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.setBalance("L1", "alice", "USD", 100, 0)
	p := newPostTestPlanner(fs)

	res, err := p.SubmitAuthorize(context.Background(), "L1", "alice", "merchant", "USD", big.NewInt(40), futureTime(), "")
	require.NoError(t, err)
	assert.NotEmpty(t, res.EventID)
	assert.NotEmpty(t, res.HoldID)
	assert.Len(t, fs.events, 1)
	// Active holds total now reflects the reservation.
	held := fs.holds[balKey("L1", "alice", "USD")]
	require.NotNil(t, held)
	assert.Equal(t, int64(40), held.Int64())
}

func TestSubmitAuthorize_AmountValidation(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)
	for _, amt := range []*big.Int{nil, big.NewInt(0), big.NewInt(-5)} {
		_, err := p.SubmitAuthorize(context.Background(), "L1", "alice", "", "USD", amt, futureTime(), "")
		require.Error(t, err)
	}
	assert.Empty(t, fs.events)
}

func TestSubmitAuthorize_InsufficientAvailable(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.setBalance("L1", "alice", "USD", 100, 0)
	// Pre-existing hold of 80 leaves only 20 available.
	fs.addHold(heldRecord("L1", "h-existing", "alice", "m", "USD", 80, 0))
	p := newPostTestPlanner(fs)

	_, err := p.SubmitAuthorize(context.Background(), "L1", "alice", "", "USD", big.NewInt(40), futureTime(), "")
	require.ErrorIs(t, err, storage.ErrInsufficientFunds)
}

func TestSubmitCapture_PartialCapture(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.addHold(heldRecord("L1", "h1", "alice", "merchant", "USD", 100, 0))
	p := newPostTestPlanner(fs)

	res, err := p.SubmitCapture(context.Background(), "L1", "h1", big.NewInt(60), "merchant", "")
	require.NoError(t, err)
	assert.NotEmpty(t, res.EventID)
	assert.Equal(t, int64(60), fs.holdRecords["L1|h1"].CapturedAmount.Int64())
	assert.Equal(t, int64(60), fs.getBalance("L1", "alice", "USD").output.Int64())
	assert.Equal(t, int64(60), fs.getBalance("L1", "merchant", "USD").input.Int64())
}

func TestSubmitCapture_OverRemainingRejected(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.addHold(heldRecord("L1", "h1", "alice", "merchant", "USD", 100, 70)) // 30 remaining
	p := newPostTestPlanner(fs)

	_, err := p.SubmitCapture(context.Background(), "L1", "h1", big.NewInt(50), "merchant", "")
	require.ErrorIs(t, err, storage.ErrInsufficientFunds)
	assert.Empty(t, fs.events)
}

func TestSubmitCapture_VoidedAndExpiredRejected(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	voided := heldRecord("L1", "hv", "alice", "m", "USD", 100, 0)
	voided.Voided = true
	expired := heldRecord("L1", "he", "alice", "m", "USD", 100, 0)
	expired.Expired = true
	fs.addHold(voided)
	fs.addHold(expired)
	p := newPostTestPlanner(fs)

	_, err := p.SubmitCapture(context.Background(), "L1", "hv", big.NewInt(10), "m", "")
	require.ErrorIs(t, err, storage.ErrHoldVoided)
	_, err = p.SubmitCapture(context.Background(), "L1", "he", big.NewInt(10), "m", "")
	require.ErrorIs(t, err, storage.ErrHoldExpired)
}

func TestSubmitCapture_RejectsNonPositiveAmount(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.addHold(heldRecord("L1", "h1", "alice", "merchant", "USD", 100, 0))
	p := newPostTestPlanner(fs)

	// Zero, negative, and nil amounts must all be rejected before any state change.
	for _, amt := range []*big.Int{big.NewInt(0), big.NewInt(-5), nil} {
		_, err := p.SubmitCapture(context.Background(), "L1", "h1", amt, "merchant", "")
		require.Error(t, err, "capture must reject non-positive/nil amount %v", amt)
	}
	// The hold is untouched after the rejected captures.
	assert.Equal(t, int64(0), fs.holdRecords["L1|h1"].CapturedAmount.Int64())
	assert.Empty(t, fs.events)
}

func TestSubmitVoid_HappyAndAlreadyVoided(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	fs.addHold(heldRecord("L1", "h1", "alice", "m", "USD", 100, 0))
	p := newPostTestPlanner(fs)

	res, err := p.SubmitVoid(context.Background(), "L1", "h1", "")
	require.NoError(t, err)
	assert.NotEmpty(t, res.EventID)
	assert.True(t, fs.holdRecords["L1|h1"].Voided)

	// Second void is rejected.
	_, err = p.SubmitVoid(context.Background(), "L1", "h1", "")
	require.ErrorIs(t, err, storage.ErrHoldVoided)
}

// Batch ALL_OR_NOTHING authorize now validates the amount (previously skipped).
func TestSubmitBatch_Authorize_NowValidatesAmount(t *testing.T) {
	fs := newFakeStore()
	fs.addLedger(openLedger("L1", "_world"))
	p := newPostTestPlanner(fs)

	intents := []BatchIntent{
		{Type: "authorize", Source: "_world", Asset: "USD", Amount: big.NewInt(0), ExpiresAt: futureTime()}, // zero -> invalid
	}
	res, err := p.SubmitBatch(context.Background(), "L1", intents, "ALL_OR_NOTHING")
	require.Error(t, err)
	assert.True(t, res.Failed)
	assert.Empty(t, fs.events)
}
