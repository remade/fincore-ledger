package planner

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/remade/ledger/internal/storage"
)

// expiredHoldsBatchSize is the page size ListExpiredHolds returns; the sweep
// keeps looping while a full page comes back. Kept in sync with the store cap.
const expiredHoldsBatchSize = 1000

// ExpireHolds finds and expires all holds past their deadline.
// Loops until all expired holds are processed (ListExpiredHolds caps the page).
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

		progressed := 0
		for _, hold := range expired {
			if err := p.expireSingleHold(ctx, hold); err != nil {
				p.logger.Error("failed to expire hold",
					zap.String("hold_id", hold.HoldID),
					zap.Error(err),
				)
				continue
			}
			progressed++
		}

		totalExpired += progressed

		// If no hold in this page could be expired, the remaining rows are
		// failing (e.g. a poison hold) and ListExpiredHolds would return the same
		// set every iteration, hot-spinning until the job timeout. Stop and let
		// the next scheduled sweep retry them instead.
		if progressed == 0 {
			p.logger.Warn("hold expiry made no progress this pass; deferring to next sweep",
				zap.Int("pending", len(expired)),
			)
			break
		}

		// If fewer than the batch limit, we've processed all of them.
		if len(expired) < expiredHoldsBatchSize {
			break
		}
		p.logger.Warn("full page of expired holds found, processing next batch",
			zap.Int("batch_size", expiredHoldsBatchSize))
	}

	if totalExpired > 0 {
		p.logger.Info("expired holds", zap.Int("count", totalExpired))
	}
	return nil
}

func (p *Planner) expireSingleHold(ctx context.Context, hold storage.HoldRecord) error {
	now := time.Now().UTC()

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

	eventID, _, err := p.appendEvent(ctx, txStore, hold.LedgerID, storage.EventTypeHoldExpired, payload, "", nil, now, now)
	if err != nil {
		return err
	}

	if err := txStore.ExpireHold(ctx, hold.LedgerID, hold.HoldID); err != nil {
		return err
	}

	if err := txStore.Commit(); err != nil {
		return err
	}

	p.publishEvent(ctx, hold.LedgerID, eventID, storage.EventTypeHoldExpired)

	return nil
}
