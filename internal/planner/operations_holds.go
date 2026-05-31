package planner

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/remade/ledger/internal/storage"
)

// SubmitAuthorize creates a hold (authorization) on a source account. The
// in-transaction core is shared with the batch path via doAuthorizeInTx.
func (p *Planner) SubmitAuthorize(ctx context.Context, ledgerID, source, destHint, asset string, amount *big.Int, expiresAt time.Time, idempotencyKey string) (*SubmitResult, error) {
	// Validate up front so computeGenericIdempotencyHash never sees a nil amount.
	if amount == nil || amount.Sign() <= 0 {
		return nil, fmt.Errorf("authorize amount must be positive")
	}

	ledger, err := p.checkLedgerNotSealed(ctx, ledgerID)
	if err != nil {
		return nil, err
	}

	var ikHash []byte
	if idempotencyKey != "" {
		ikHash = computeGenericIdempotencyHash(ledgerID, "authorize", source, destHint, asset, amount.String(), expiresAt.Format(time.RFC3339Nano))
	}

	now := time.Now().UTC()
	return p.submitWithIdempotency(ctx, ledgerID, idempotencyKey, ikHash, storage.EventTypeHoldCreated, false,
		func(txStore storage.TxStore) (*SubmitResult, error) {
			return p.doAuthorizeInTx(ctx, txStore, ledger, source, destHint, asset, amount, expiresAt, idempotencyKey, ikHash, now)
		})
}

// SubmitCapture captures (partially or fully) an authorized hold.
func (p *Planner) SubmitCapture(ctx context.Context, ledgerID, holdID string, amount *big.Int, destination string, idempotencyKey string) (*SubmitResult, error) {
	if amount == nil || amount.Sign() <= 0 {
		return nil, fmt.Errorf("capture amount must be positive")
	}
	if _, err := p.checkLedgerNotSealed(ctx, ledgerID); err != nil {
		return nil, err
	}

	var ikHash []byte
	if idempotencyKey != "" {
		ikHash = computeGenericIdempotencyHash(ledgerID, "capture", holdID, amount.String(), destination)
	}

	now := time.Now().UTC()
	return p.submitWithIdempotency(ctx, ledgerID, idempotencyKey, ikHash, storage.EventTypeHoldConfirmed, false,
		func(txStore storage.TxStore) (*SubmitResult, error) {
			return p.doCaptureInTx(ctx, txStore, ledgerID, holdID, amount, destination, idempotencyKey, ikHash, now)
		})
}

// SubmitVoid voids an active hold, releasing the reserved funds.
func (p *Planner) SubmitVoid(ctx context.Context, ledgerID, holdID string, idempotencyKey string) (*SubmitResult, error) {
	if _, err := p.checkLedgerNotSealed(ctx, ledgerID); err != nil {
		return nil, err
	}

	var ikHash []byte
	if idempotencyKey != "" {
		ikHash = computeGenericIdempotencyHash(ledgerID, "void", holdID)
	}

	now := time.Now().UTC()
	return p.submitWithIdempotency(ctx, ledgerID, idempotencyKey, ikHash, storage.EventTypeHoldVoided, false,
		func(txStore storage.TxStore) (*SubmitResult, error) {
			return p.doVoidInTx(ctx, txStore, ledgerID, holdID, idempotencyKey, ikHash, now)
		})
}
