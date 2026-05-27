package planner

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"

	"github.com/remade/ledger/internal/storage"
)

// ExpireHolds finds and expires all holds past their deadline.
// Loops until all expired holds are processed (ListExpiredHolds caps at 1000).
func (p *Planner) ExpireHolds(ctx context.Context) error {
	totalExpired := 0
	for {
		expired, err := p.store.ListExpiredHolds(ctx)
		if err != nil {
			return err
		}
		if len(expired) == 0 {
			break
		}

		for _, hold := range expired {
			if err := p.expireSingleHold(ctx, hold); err != nil {
				p.logger.Error("failed to expire hold",
					zap.String("hold_id", hold.HoldID),
					zap.Error(err),
				)
			}
		}

		totalExpired += len(expired)

		// If fewer than the batch limit, we've processed all of them.
		if len(expired) < 1000 {
			break
		}
		p.logger.Warn("more than 1000 expired holds found, processing next batch")
	}

	if totalExpired > 0 {
		p.logger.Info("expired holds", zap.Int("count", totalExpired))
	}
	return nil
}

func (p *Planner) expireSingleHold(ctx context.Context, hold storage.HoldRecord) error {
	now := time.Now().UTC()
	eventID := ulid.Make().String()

	batchID, err := p.batch.CurrentBatchID(ctx, hold.LedgerID)
	if err != nil {
		return fmt.Errorf("getting batch ID: %w", err)
	}

	payload, err := json.Marshal(map[string]string{
		"hold_id": hold.HoldID,
		"source":  hold.Source,
		"asset":   hold.Asset,
	})
	if err != nil {
		return fmt.Errorf("marshaling payload: %w", err)
	}

	txStore, err := p.store.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer txStore.Rollback()

	// Re-read hold inside TX to guard against concurrent expiry/void.
	currentHold, err := txStore.GetHold(ctx, hold.LedgerID, hold.HoldID)
	if err != nil {
		return err
	}
	if currentHold.Expired || currentHold.Voided {
		return nil // already handled
	}

	seq, err := txStore.NextLedgerSeq(ctx, hold.LedgerID)
	if err != nil {
		return err
	}

	if err := txStore.AppendLogEvent(ctx, storage.LogEventRecord{
		EventID: eventID, LedgerID: hold.LedgerID, LedgerSeq: seq,
		SystemTime: now, ValidTime: now, Type: 5, // HOLD_EXPIRED
		Payload: payload, BatchID: batchID, SchemaVersion: 1,
	}); err != nil {
		return err
	}

	if err := txStore.ExpireHold(ctx, hold.LedgerID, hold.HoldID); err != nil {
		return err
	}

	if err := txStore.Commit(); err != nil {
		return err
	}

	p.publishEvent(ctx, hold.LedgerID, eventID, 5)

	return nil
}
