package planner

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/remade/ledger/internal/storage"
)

// SubmitSetMetadata handles a SetMetadataOperation. The in-transaction core is
// shared with the batch path via doSetMetadataInTx.
func (p *Planner) SubmitSetMetadata(ctx context.Context, ledgerID string, targetType int16, targetID string, metadata map[string]any, idempotencyKey string) (*SubmitResult, error) {
	if _, err := p.checkLedgerNotSealed(ctx, ledgerID); err != nil {
		return nil, err
	}

	var ikHash []byte
	if idempotencyKey != "" {
		metaJSON, err := json.Marshal(metadata)
		if err != nil {
			return nil, fmt.Errorf("marshaling metadata for idempotency hash: %w", err)
		}
		ikHash = computeGenericIdempotencyHash(ledgerID, "set_metadata", fmt.Sprint(targetType), targetID, string(metaJSON))
	}

	now := time.Now().UTC()
	return p.submitWithIdempotency(ctx, ledgerID, idempotencyKey, ikHash, storage.EventTypeMetadataSet, false,
		func(txStore storage.TxStore) (*SubmitResult, error) {
			return p.doSetMetadataInTx(ctx, txStore, ledgerID, targetType, targetID, metadata, idempotencyKey, ikHash, now)
		})
}

// SubmitDeleteMetadata handles a DeleteMetadataOperation.
func (p *Planner) SubmitDeleteMetadata(ctx context.Context, ledgerID string, targetType int16, targetID string, key string, idempotencyKey string) (*SubmitResult, error) {
	if _, err := p.checkLedgerNotSealed(ctx, ledgerID); err != nil {
		return nil, err
	}

	var ikHash []byte
	if idempotencyKey != "" {
		ikHash = computeGenericIdempotencyHash(ledgerID, "delete_metadata", fmt.Sprint(targetType), targetID, key)
	}

	now := time.Now().UTC()
	return p.submitWithIdempotency(ctx, ledgerID, idempotencyKey, ikHash, storage.EventTypeMetadataDeleted, false,
		func(txStore storage.TxStore) (*SubmitResult, error) {
			return p.doDeleteMetadataInTx(ctx, txStore, ledgerID, targetType, targetID, key, idempotencyKey, ikHash, now)
		})
}
