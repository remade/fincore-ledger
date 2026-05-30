package planner

import (
	"context"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/remade/ledger/internal/storage"
)

// SubmitInsertSchema handles an InsertSchemaOperation.
func (p *Planner) SubmitInsertSchema(ctx context.Context, ledgerID string, schemaBytes []byte, version string, idempotencyKey string) (*SubmitResult, error) {
	if _, err := p.checkLedgerNotSealed(ctx, ledgerID); err != nil {
		return nil, err
	}

	// Check idempotency (Redis first, then PG).
	var ikHash []byte
	if idempotencyKey != "" {
		ikHash = computeGenericIdempotencyHash(ledgerID, "insert_schema", version)
		if result, err := p.checkIdempotency(ctx, ledgerID, idempotencyKey, ikHash); err != nil {
			return nil, err
		} else if result != nil {
			return result, nil
		}
	}

	now := time.Now().UTC()
	var result *SubmitResult
	err := withDeadlockRetry(ctx, 5, func() error {
		eventID := ulid.Make().String()

		batchID, err := p.batch.CurrentBatchID(ctx, ledgerID)
		if err != nil {
			return fmt.Errorf("getting batch ID: %w", err)
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
			Type:           storage.EventTypeSchemaInserted,
			Payload:        schemaBytes,
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

		if err := txStore.InsertSchema(ctx, storage.SchemaRecord{
			LedgerID: ledgerID,
			Version:  version,
			Document: schemaBytes,
			EventID:  eventID,
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

		p.publishEvent(ctx, ledgerID, eventID, 11)

		result = &SubmitResult{EventID: eventID}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
