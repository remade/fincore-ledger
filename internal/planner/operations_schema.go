package planner

import (
	"context"
	"time"

	"github.com/remade/ledger/internal/storage"
)

// SubmitInsertSchema handles an InsertSchemaOperation.
func (p *Planner) SubmitInsertSchema(ctx context.Context, ledgerID string, schemaBytes []byte, version string, idempotencyKey string) (*SubmitResult, error) {
	if _, err := p.checkLedgerNotSealed(ctx, ledgerID); err != nil {
		return nil, err
	}

	var ikHash []byte
	if idempotencyKey != "" {
		ikHash = computeGenericIdempotencyHash(ledgerID, "insert_schema", version)
	}

	now := time.Now().UTC()
	return p.submitWithIdempotency(ctx, ledgerID, idempotencyKey, ikHash, storage.EventTypeSchemaInserted, false,
		func(txStore storage.TxStore) (*SubmitResult, error) {
			eventID, _, err := p.appendEvent(ctx, txStore, ledgerID, storage.EventTypeSchemaInserted, schemaBytes, idempotencyKey, ikHash, now, now)
			if err != nil {
				return nil, err
			}
			if err := txStore.InsertSchema(ctx, storage.SchemaRecord{
				LedgerID: ledgerID,
				Version:  version,
				Document: schemaBytes,
				EventID:  eventID,
			}); err != nil {
				return nil, err
			}
			if idempotencyKey != "" && ikHash != nil {
				if err := p.recordIdempotency(ctx, txStore, ledgerID, idempotencyKey, eventID, ikHash); err != nil {
					return nil, err
				}
			}
			return &SubmitResult{EventID: eventID}, nil
		})
}
