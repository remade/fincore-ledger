package planner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/remade/ledger/internal/storage"
)

// SubmitSetMetadata handles a SetMetadataOperation.
func (p *Planner) SubmitSetMetadata(ctx context.Context, ledgerID string, targetType int16, targetID string, metadata map[string]any, idempotencyKey string) (*SubmitResult, error) {
	if _, err := p.checkLedgerNotSealed(ctx, ledgerID); err != nil {
		return nil, err
	}

	// Check idempotency (Redis first, then PG).
	var ikHash []byte
	if idempotencyKey != "" {
		metaJSON, _ := json.Marshal(metadata)
		ikHash = computeGenericIdempotencyHash(ledgerID, "set_metadata", fmt.Sprint(targetType), targetID, string(metaJSON))
		if result, err := p.checkIdempotency(ctx, ledgerID, idempotencyKey, ikHash); err != nil {
			return nil, err
		} else if result != nil {
			return result, nil
		}
	}

	var result *SubmitResult
	err := withDeadlockRetry(ctx, 5, func() error {
		now := time.Now().UTC()
		eventID := ulid.Make().String()

		batchID, err := p.batch.CurrentBatchID(ctx, ledgerID)
		if err != nil {
			return fmt.Errorf("getting batch ID: %w", err)
		}

		payload, err := json.Marshal(map[string]any{
			"target_type": targetType,
			"target_id":   targetID,
			"metadata":    metadata,
		})
		if err != nil {
			return fmt.Errorf("marshaling payload: %w", err)
		}

		txStore, err := p.store.BeginTx(ctx)
		if err != nil {
			return fmt.Errorf("beginning tx: %w", err)
		}
		defer txStore.Rollback()

		seq, err := txStore.NextLedgerSeq(ctx, ledgerID)
		if err != nil {
			return err
		}

		logEvent := storage.LogEventRecord{
			EventID:        eventID,
			LedgerID:       ledgerID,
			LedgerSeq:      seq,
			SystemTime:     now,
			ValidTime:      now,
			Type:           9, // METADATA_SET
			Payload:        payload,
			IdempotencyKey: idempotencyKey,
			BatchID:        batchID,
			SchemaVersion:  1,
		}
		if idempotencyKey != "" {
			logEvent.IdempotencyHash = ikHash
		}

		if err := txStore.AppendLogEvent(ctx, logEvent); err != nil {
			return err
		}

		// If target is an account, upsert with the new metadata.
		if targetType == 0 { // ACCOUNT
			if err := txStore.UpsertAccount(ctx, storage.AccountRecord{
				LedgerID:   ledgerID,
				Address:    targetID,
				FirstUsage: now,
				UpdatedAt:  now,
				Metadata:   metadata,
			}); err != nil {
				return err
			}
		}

		if targetType == 1 { // TRANSACTION
			// Update the transaction's metadata in the projection table.
			if err := txStore.UpdateTransactionMetadata(ctx, ledgerID, targetID, metadata); err != nil {
				return fmt.Errorf("updating transaction metadata: %w", err)
			}
		}

		// Write metadata history.
		if err := txStore.InsertMetadataHistory(ctx, storage.MetadataHistoryRecord{
			LedgerID:   ledgerID,
			TargetType: targetType,
			TargetID:   targetID,
			Revision:   seq,
			Metadata:   metadata,
			EventID:    eventID,
			SystemTime: now,
		}); err != nil {
			return err
		}

		if idempotencyKey != "" {
			if err := p.recordIdempotency(ctx, txStore, ledgerID, idempotencyKey, eventID, ikHash); err != nil {
				return err
			}
		}

		if err := txStore.Commit(); err != nil {
			return err
		}

		if idempotencyKey != "" {
			p.postCommitIdempotency(ctx, ledgerID, idempotencyKey, eventID, ikHash)
		}

		p.publishEvent(ctx, ledgerID, eventID, 9)

		result = &SubmitResult{EventID: eventID}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// SubmitDeleteMetadata handles a DeleteMetadataOperation.
func (p *Planner) SubmitDeleteMetadata(ctx context.Context, ledgerID string, targetType int16, targetID string, key string, idempotencyKey string) (*SubmitResult, error) {
	if _, err := p.checkLedgerNotSealed(ctx, ledgerID); err != nil {
		return nil, err
	}

	// Check idempotency (Redis first, then PG).
	var ikHash []byte
	if idempotencyKey != "" {
		ikHash = computeGenericIdempotencyHash(ledgerID, "delete_metadata", fmt.Sprint(targetType), targetID, key)
		if result, err := p.checkIdempotency(ctx, ledgerID, idempotencyKey, ikHash); err != nil {
			return nil, err
		} else if result != nil {
			return result, nil
		}
	}

	var result *SubmitResult
	err := withDeadlockRetry(ctx, 5, func() error {
		now := time.Now().UTC()
		eventID := ulid.Make().String()

		batchID, err := p.batch.CurrentBatchID(ctx, ledgerID)
		if err != nil {
			return fmt.Errorf("getting batch ID: %w", err)
		}

		payload, err := json.Marshal(map[string]any{
			"target_type": targetType,
			"target_id":   targetID,
			"key":         key,
		})
		if err != nil {
			return fmt.Errorf("marshaling payload: %w", err)
		}

		txStore, err := p.store.BeginTx(ctx)
		if err != nil {
			return fmt.Errorf("beginning tx: %w", err)
		}
		defer txStore.Rollback()

		seq, err := txStore.NextLedgerSeq(ctx, ledgerID)
		if err != nil {
			return err
		}

		logEvent := storage.LogEventRecord{
			EventID:        eventID,
			LedgerID:       ledgerID,
			LedgerSeq:      seq,
			SystemTime:     now,
			ValidTime:      now,
			Type:           10, // METADATA_DELETED
			Payload:        payload,
			IdempotencyKey: idempotencyKey,
			BatchID:        batchID,
			SchemaVersion:  1,
		}
		if idempotencyKey != "" {
			logEvent.IdempotencyHash = ikHash
		}

		if err := txStore.AppendLogEvent(ctx, logEvent); err != nil {
			return err
		}

		// If target is an account, remove the key from metadata.
		if targetType == 0 { // ACCOUNT
			acct, err := txStore.GetAccount(ctx, ledgerID, targetID)
			if err != nil && !errors.Is(err, storage.ErrNotFound) {
				return err
			}
			if acct != nil {
				delete(acct.Metadata, key)
				acct.UpdatedAt = now
				if err := txStore.UpsertAccount(ctx, *acct); err != nil {
					return err
				}
			}
		}

		if targetType == 1 { // TRANSACTION
			// For transactions, we need to remove the key from metadata.
			if err := txStore.DeleteTransactionMetadataKey(ctx, ledgerID, targetID, key); err != nil && !errors.Is(err, storage.ErrNotFound) {
				return fmt.Errorf("deleting transaction metadata key: %w", err)
			}
		}

		// Write metadata history with full snapshot after deletion.
		var currentMetadata map[string]any
		if targetType == 0 { // ACCOUNT
			acctAfter, err := txStore.GetAccount(ctx, ledgerID, targetID)
			if err != nil && !errors.Is(err, storage.ErrNotFound) {
				return err
			}
			if acctAfter != nil {
				currentMetadata = acctAfter.Metadata
			}
		} else if targetType == 1 { // TRANSACTION
			txRec, err := txStore.GetTransaction(ctx, ledgerID, targetID)
			if err != nil && !errors.Is(err, storage.ErrNotFound) {
				return err
			}
			if txRec != nil {
				currentMetadata = txRec.Metadata
			}
		}
		if currentMetadata == nil {
			currentMetadata = map[string]any{}
		}
		if err := txStore.InsertMetadataHistory(ctx, storage.MetadataHistoryRecord{
			LedgerID:   ledgerID,
			TargetType: targetType,
			TargetID:   targetID,
			Revision:   seq,
			Metadata:   currentMetadata,
			EventID:    eventID,
			SystemTime: now,
		}); err != nil {
			return err
		}

		if idempotencyKey != "" {
			if err := p.recordIdempotency(ctx, txStore, ledgerID, idempotencyKey, eventID, ikHash); err != nil {
				return err
			}
		}

		if err := txStore.Commit(); err != nil {
			return err
		}

		if idempotencyKey != "" {
			p.postCommitIdempotency(ctx, ledgerID, idempotencyKey, eventID, ikHash)
		}

		p.publishEvent(ctx, ledgerID, eventID, 10)

		result = &SubmitResult{EventID: eventID}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
