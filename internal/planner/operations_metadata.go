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

		r, err := p.doSetMetadataInTx(ctx, txStore, ledgerID, targetType, targetID, metadata, idempotencyKey, ikHash, now)
		if err != nil {
			return err
		}
		if err := txStore.Commit(); err != nil {
			return err
		}
		if idempotencyKey != "" {
			p.postCommitIdempotency(ctx, ledgerID, idempotencyKey, r.EventID, ikHash)
		}
		p.publishEvent(ctx, ledgerID, r.EventID, storage.EventTypeMetadataSet)
		result = r
		return nil
	})
	if err != nil {
		if idempotencyKey != "" && isIdempotencyConflict(err) {
			return p.resolveIdempotencyConflict(ctx, ledgerID, idempotencyKey)
		}
		return nil, err
	}
	return result, nil
}

// SubmitDeleteMetadata handles a DeleteMetadataOperation.
func (p *Planner) SubmitDeleteMetadata(ctx context.Context, ledgerID string, targetType int16, targetID string, key string, idempotencyKey string) (*SubmitResult, error) {
	if _, err := p.checkLedgerNotSealed(ctx, ledgerID); err != nil {
		return nil, err
	}

	var ikHash []byte
	if idempotencyKey != "" {
		ikHash = computeGenericIdempotencyHash(ledgerID, "delete_metadata", fmt.Sprint(targetType), targetID, key)
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

		r, err := p.doDeleteMetadataInTx(ctx, txStore, ledgerID, targetType, targetID, key, idempotencyKey, ikHash, now)
		if err != nil {
			return err
		}
		if err := txStore.Commit(); err != nil {
			return err
		}
		if idempotencyKey != "" {
			p.postCommitIdempotency(ctx, ledgerID, idempotencyKey, r.EventID, ikHash)
		}
		p.publishEvent(ctx, ledgerID, r.EventID, storage.EventTypeMetadataDeleted)
		result = r
		return nil
	})
	if err != nil {
		if idempotencyKey != "" && isIdempotencyConflict(err) {
			return p.resolveIdempotencyConflict(ctx, ledgerID, idempotencyKey)
		}
		return nil, err
	}
	return result, nil
}
