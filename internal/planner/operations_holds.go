package planner

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"go.uber.org/zap"
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
		if result, err := p.checkIdempotency(ctx, ledgerID, idempotencyKey, ikHash); err != nil {
			return nil, err
		} else if result != nil {
			return result, nil
		}
	}

	now := time.Now().UTC()
	var result *SubmitResult
	err = withDeadlockRetry(ctx, 5, func() error {
		txStore, err := p.store.BeginTx(ctx)
		if err != nil {
			return err
		}
		defer txStore.Rollback()

		r, err := p.doAuthorizeInTx(ctx, txStore, ledger, source, destHint, asset, amount, expiresAt, idempotencyKey, ikHash, now)
		if err != nil {
			return err
		}
		if err := txStore.Commit(); err != nil {
			return err
		}
		if idempotencyKey != "" {
			p.postCommitIdempotency(ctx, ledgerID, idempotencyKey, r.EventID, ikHash)
		}
		p.publishEvent(ctx, ledgerID, r.EventID, 2)
		p.logger.Debug("hold authorized", zap.String("hold_id", r.HoldID), zap.String("source", source))
		result = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// SubmitCapture captures (partially or fully) an authorized hold.
func (p *Planner) SubmitCapture(ctx context.Context, ledgerID, holdID string, amount *big.Int, destination string, idempotencyKey string) (*SubmitResult, error) {
	if _, err := p.checkLedgerNotSealed(ctx, ledgerID); err != nil {
		return nil, err
	}

	var ikHash []byte
	if idempotencyKey != "" {
		ikHash = computeGenericIdempotencyHash(ledgerID, "capture", holdID, amount.String(), destination)
		if result, err := p.checkIdempotency(ctx, ledgerID, idempotencyKey, ikHash); err != nil {
			return nil, err
		} else if result != nil {
			return result, nil
		}
	}

	now := time.Now().UTC()
	var result *SubmitResult
	err := withDeadlockRetry(ctx, 5, func() error {
		txStore, err := p.store.BeginTx(ctx)
		if err != nil {
			return err
		}
		defer txStore.Rollback()

		r, err := p.doCaptureInTx(ctx, txStore, ledgerID, holdID, amount, destination, idempotencyKey, ikHash, now)
		if err != nil {
			return err
		}
		if err := txStore.Commit(); err != nil {
			return err
		}
		if idempotencyKey != "" {
			p.postCommitIdempotency(ctx, ledgerID, idempotencyKey, r.EventID, ikHash)
		}
		p.publishEvent(ctx, ledgerID, r.EventID, 3)
		result = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// SubmitVoid voids an active hold, releasing the reserved funds.
func (p *Planner) SubmitVoid(ctx context.Context, ledgerID, holdID string, idempotencyKey string) (*SubmitResult, error) {
	if _, err := p.checkLedgerNotSealed(ctx, ledgerID); err != nil {
		return nil, err
	}

	var ikHash []byte
	if idempotencyKey != "" {
		ikHash = computeGenericIdempotencyHash(ledgerID, "void", holdID)
		if result, err := p.checkIdempotency(ctx, ledgerID, idempotencyKey, ikHash); err != nil {
			return nil, err
		} else if result != nil {
			return result, nil
		}
	}

	now := time.Now().UTC()
	var result *SubmitResult
	err := withDeadlockRetry(ctx, 5, func() error {
		txStore, err := p.store.BeginTx(ctx)
		if err != nil {
			return err
		}
		defer txStore.Rollback()

		r, err := p.doVoidInTx(ctx, txStore, ledgerID, holdID, idempotencyKey, ikHash, now)
		if err != nil {
			return err
		}
		if err := txStore.Commit(); err != nil {
			return err
		}
		if idempotencyKey != "" {
			p.postCommitIdempotency(ctx, ledgerID, idempotencyKey, r.EventID, ikHash)
		}
		p.publishEvent(ctx, ledgerID, r.EventID, 4)
		result = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
