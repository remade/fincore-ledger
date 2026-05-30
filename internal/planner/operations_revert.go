package planner

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// SubmitRevert creates a reverting transaction for an existing transaction. The
// in-transaction core is shared with the batch path via doRevertInTx.
func (p *Planner) SubmitRevert(ctx context.Context, ledgerID, originalTxID string, force, atEffectiveDate bool, reason, idempotencyKey string) (*SubmitResult, error) {
	if _, err := p.checkLedgerNotSealed(ctx, ledgerID); err != nil {
		return nil, err
	}

	var ikHash []byte
	if idempotencyKey != "" {
		ikHash = computeGenericIdempotencyHash(ledgerID, "revert", originalTxID, fmt.Sprintf("%t", force), fmt.Sprintf("%t", atEffectiveDate), reason)
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
			return fmt.Errorf("beginning tx: %w", err)
		}
		defer txStore.Rollback()

		r, err := p.doRevertInTx(ctx, txStore, ledgerID, originalTxID, force, atEffectiveDate, reason, idempotencyKey, ikHash, now)
		if err != nil {
			return err
		}
		if err := txStore.Commit(); err != nil {
			return fmt.Errorf("committing revert transaction: %w", err)
		}
		if idempotencyKey != "" {
			p.postCommitIdempotency(ctx, ledgerID, idempotencyKey, r.EventID, ikHash)
		}
		p.publishEvent(ctx, ledgerID, r.EventID, 7)
		p.logger.Debug("transaction reverted",
			zap.String("event_id", r.EventID),
			zap.String("original_tx_id", originalTxID),
			zap.String("ledger", ledgerID),
		)
		result = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// SubmitAmend overlays metadata changes on an existing transaction.
func (p *Planner) SubmitAmend(ctx context.Context, ledgerID, originalTxID string, metadataChanges map[string]any, idempotencyKey string) (*SubmitResult, error) {
	if _, err := p.checkLedgerNotSealed(ctx, ledgerID); err != nil {
		return nil, err
	}

	var ikHash []byte
	if idempotencyKey != "" {
		metaJSON, err := json.Marshal(metadataChanges)
		if err != nil {
			return nil, fmt.Errorf("marshaling metadata for idempotency hash: %w", err)
		}
		ikHash = computeGenericIdempotencyHash(ledgerID, "amend", originalTxID, string(metaJSON))
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

		r, err := p.doAmendInTx(ctx, txStore, ledgerID, originalTxID, metadataChanges, idempotencyKey, ikHash, now)
		if err != nil {
			return err
		}
		if err := txStore.Commit(); err != nil {
			return err
		}
		if idempotencyKey != "" {
			p.postCommitIdempotency(ctx, ledgerID, idempotencyKey, r.EventID, ikHash)
		}
		p.publishEvent(ctx, ledgerID, r.EventID, 8)
		result = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
