package planner

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/remade/ledger/internal/storage"
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
	}

	now := time.Now().UTC()
	return p.submitWithIdempotency(ctx, ledgerID, idempotencyKey, ikHash, storage.EventTypeTransactionReverted, false,
		func(txStore storage.TxStore) (*SubmitResult, error) {
			return p.doRevertInTx(ctx, txStore, ledgerID, originalTxID, force, atEffectiveDate, reason, idempotencyKey, ikHash, now)
		})
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
	}

	now := time.Now().UTC()
	return p.submitWithIdempotency(ctx, ledgerID, idempotencyKey, ikHash, storage.EventTypeTransactionAmended, false,
		func(txStore storage.TxStore) (*SubmitResult, error) {
			return p.doAmendInTx(ctx, txStore, ledgerID, originalTxID, metadataChanges, idempotencyKey, ikHash, now)
		})
}
