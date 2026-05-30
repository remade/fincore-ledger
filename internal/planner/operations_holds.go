package planner

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"

	"github.com/remade/ledger/internal/storage"
	"github.com/remade/ledger/pkg/accounts"
)

// SubmitAuthorize creates a hold (authorization) on a source account.
func (p *Planner) SubmitAuthorize(ctx context.Context, ledgerID, source, destHint, asset string, amount *big.Int, expiresAt time.Time, idempotencyKey string) (*SubmitResult, error) {
	if amount == nil || amount.Sign() <= 0 {
		return nil, fmt.Errorf("authorize amount must be positive")
	}

	ledger, err := p.checkLedgerNotSealed(ctx, ledgerID)
	if err != nil {
		return nil, err
	}

	// Check idempotency.
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
		eventID := ulid.Make().String()
		holdID := ulid.Make().String()

		batchID, err := p.batch.CurrentBatchID(ctx, ledgerID)
		if err != nil {
			return fmt.Errorf("getting batch ID: %w", err)
		}

		txStore, err := p.store.BeginTx(ctx)
		if err != nil {
			return err
		}
		defer txStore.Rollback()

		seq, err := txStore.NextLedgerSeq(ctx, ledgerID)
		if err != nil {
			return err
		}

		bal, err := txStore.GetBalance(ctx, ledgerID, source, asset, nil, nil)
		if err != nil {
			return err
		}
		postedBalance := new(big.Int).Sub(bal.Input, bal.Output)

		activeHolds, err := txStore.GetActiveHoldsTotal(ctx, ledgerID, source, asset)
		if err != nil {
			return err
		}

		availableBalance := new(big.Int).Sub(postedBalance, activeHolds)
		if !accounts.IsIssuer(source, ledger.IssuerAccounts) && availableBalance.Cmp(amount) < 0 {
			return fmt.Errorf("%w: account %s, asset %s (available: %s, requested: %s)",
				storage.ErrInsufficientFunds, source, asset, availableBalance, amount)
		}

		payload, err := json.Marshal(map[string]string{
			"hold_id":          holdID,
			"source":           source,
			"destination_hint": destHint,
			"amount":           amount.String(),
			"asset":            asset,
			"expires_at":       expiresAt.Format(time.RFC3339Nano),
		})
		if err != nil {
			return fmt.Errorf("marshaling payload: %w", err)
		}

		if err := txStore.AppendLogEvent(ctx, storage.LogEventRecord{
			EventID: eventID, LedgerID: ledgerID, LedgerSeq: seq,
			SystemTime: now, ValidTime: now, Type: storage.EventTypeHoldCreated,
			Payload: payload, IdempotencyKey: idempotencyKey,
			IdempotencyHash: ikHash,
			BatchID:         batchID, SchemaVersion: 1,
		}); err != nil {
			return err
		}

		if err := txStore.InsertHold(ctx, storage.HoldRecord{
			LedgerID: ledgerID, HoldID: holdID, Source: source,
			DestinationHint: destHint, Asset: asset,
			AuthorizedAmount: amount, CapturedAmount: new(big.Int),
			ExpiresAt: expiresAt, AuthorizedEventID: eventID,
			ValidTime: now, SystemTime: now,
		}); err != nil {
			return err
		}

		if err := txStore.UpsertAccount(ctx, storage.AccountRecord{
			LedgerID: ledgerID, Address: source, FirstUsage: now, UpdatedAt: now, Metadata: map[string]any{},
		}); err != nil {
			return fmt.Errorf("upserting source account: %w", err)
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

		p.publishEvent(ctx, ledgerID, eventID, 2)

		p.logger.Debug("hold authorized", zap.String("hold_id", holdID), zap.String("source", source))
		result = &SubmitResult{EventID: eventID, HoldID: holdID}
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
		eventID := ulid.Make().String()

		batchID, err := p.batch.CurrentBatchID(ctx, ledgerID)
		if err != nil {
			return fmt.Errorf("getting batch ID: %w", err)
		}

		txStore, err := p.store.BeginTx(ctx)
		if err != nil {
			return err
		}
		defer txStore.Rollback()

		seq, err := txStore.NextLedgerSeq(ctx, ledgerID)
		if err != nil {
			return err
		}

		hold, err := txStore.GetHold(ctx, ledgerID, holdID)
		if err != nil {
			return err
		}
		if hold.Voided {
			return fmt.Errorf("%w: %s", storage.ErrHoldVoided, holdID)
		}
		if hold.Expired {
			return fmt.Errorf("%w: %s", storage.ErrHoldExpired, holdID)
		}

		remaining := new(big.Int).Sub(hold.AuthorizedAmount, hold.CapturedAmount)
		if amount.Cmp(remaining) > 0 {
			return fmt.Errorf("%w: capture %s exceeds remaining %s on hold %s",
				storage.ErrInsufficientFunds, amount, remaining, holdID)
		}

		dest := destination
		if dest == "" {
			dest = hold.DestinationHint
		}
		if dest == "" {
			return fmt.Errorf("destination required for capture (no destination_hint on hold)")
		}

		payload, err := json.Marshal(map[string]string{
			"hold_id":     holdID,
			"amount":      amount.String(),
			"destination": dest,
		})
		if err != nil {
			return fmt.Errorf("marshaling payload: %w", err)
		}

		if err := txStore.AppendLogEvent(ctx, storage.LogEventRecord{
			EventID: eventID, LedgerID: ledgerID, LedgerSeq: seq,
			SystemTime: now, ValidTime: now, Type: storage.EventTypeHoldConfirmed,
			Payload: payload, IdempotencyKey: idempotencyKey,
			IdempotencyHash: ikHash,
			BatchID:         batchID, SchemaVersion: 1,
		}); err != nil {
			return err
		}

		if err := txStore.UpdateHoldCaptured(ctx, ledgerID, holdID, amount); err != nil {
			return err
		}

		if err := txStore.InsertVolumeDelta(ctx, storage.VolumeDeltaRecord{
			LedgerID: ledgerID, Account: hold.Source, Asset: hold.Asset,
			EventID: eventID, ValidTime: now, SystemTime: now,
			InputDelta: big.NewInt(0), OutputDelta: amount,
		}); err != nil {
			return err
		}
		if err := txStore.InsertVolumeDelta(ctx, storage.VolumeDeltaRecord{
			LedgerID: ledgerID, Account: dest, Asset: hold.Asset,
			EventID: eventID, ValidTime: now, SystemTime: now,
			InputDelta: amount, OutputDelta: big.NewInt(0),
		}); err != nil {
			return err
		}

		if err := txStore.UpsertAccount(ctx, storage.AccountRecord{
			LedgerID: ledgerID, Address: dest, FirstUsage: now, UpdatedAt: now, Metadata: map[string]any{},
		}); err != nil {
			return fmt.Errorf("upserting destination account: %w", err)
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

		p.publishEvent(ctx, ledgerID, eventID, 3)

		result = &SubmitResult{EventID: eventID}
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
		eventID := ulid.Make().String()

		batchID, err := p.batch.CurrentBatchID(ctx, ledgerID)
		if err != nil {
			return fmt.Errorf("getting batch ID: %w", err)
		}

		txStore, err := p.store.BeginTx(ctx)
		if err != nil {
			return err
		}
		defer txStore.Rollback()

		seq, err := txStore.NextLedgerSeq(ctx, ledgerID)
		if err != nil {
			return err
		}

		// Read hold state inside TX to prevent TOCTOU.
		hold, err := txStore.GetHold(ctx, ledgerID, holdID)
		if err != nil {
			return err
		}
		if hold.Voided {
			return fmt.Errorf("%w: %s", storage.ErrHoldVoided, holdID)
		}
		if hold.Expired {
			return fmt.Errorf("%w: %s", storage.ErrHoldExpired, holdID)
		}

		payload, err := json.Marshal(map[string]string{
			"hold_id": holdID,
		})
		if err != nil {
			return fmt.Errorf("marshaling payload: %w", err)
		}

		if err := txStore.AppendLogEvent(ctx, storage.LogEventRecord{
			EventID: eventID, LedgerID: ledgerID, LedgerSeq: seq,
			SystemTime: now, ValidTime: now, Type: storage.EventTypeHoldVoided,
			Payload: payload, IdempotencyKey: idempotencyKey,
			IdempotencyHash: ikHash,
			BatchID:         batchID, SchemaVersion: 1,
		}); err != nil {
			return err
		}

		if err := txStore.VoidHold(ctx, ledgerID, holdID); err != nil {
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

		p.publishEvent(ctx, ledgerID, eventID, 4)

		result = &SubmitResult{EventID: eventID}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
