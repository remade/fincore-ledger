package planner

import (
	"bytes"
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

// SubmitRevert creates a reverting transaction for an existing transaction.
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

	var result *SubmitResult
	err := withDeadlockRetry(ctx, 5, func() error {
		now := time.Now().UTC()
		eventID := ulid.Make().String()
		txID := ulid.Make().String()

		batchID, err := p.batch.CurrentBatchID(ctx, ledgerID)
		if err != nil {
			return fmt.Errorf("getting batch ID: %w", err)
		}

		txStore, err := p.store.BeginTx(ctx)
		if err != nil {
			return fmt.Errorf("beginning tx: %w", err)
		}
		defer txStore.Rollback()

		// Read original TX and check "already reverted" inside the TX to prevent TOCTOU.
		origTx, err := txStore.GetTransaction(ctx, ledgerID, originalTxID)
		if err != nil {
			return err
		}

		rels, err := txStore.GetRelationships(ctx, ledgerID, originalTxID, 1)
		if err != nil {
			return err
		}
		for _, rel := range rels {
			if rel.RelationshipType == 0 && rel.ParentTxID == originalTxID {
				return storage.ErrAlreadyReverted
			}
		}

		// Parse original postings using json.Number to preserve amount precision.
		data, err := json.Marshal(origTx.Postings)
		if err != nil {
			return fmt.Errorf("marshaling original postings for revert: %w", err)
		}
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber()
		var origPostings []map[string]any
		if err := dec.Decode(&origPostings); err != nil {
			return fmt.Errorf("parsing original postings for revert: %w", err)
		}

		if len(origPostings) == 0 {
			return fmt.Errorf("original transaction %s has no postings to revert", originalTxID)
		}

		var reversedPostings []PostingInput
		for i, posting := range origPostings {
			src, srcOK := posting["source"]
			dst, dstOK := posting["destination"]
			asset, assetOK := posting["asset"]
			if !srcOK || !dstOK || !assetOK {
				return fmt.Errorf("posting %d: missing required field (source/destination/asset) in original transaction %s", i, originalTxID)
			}

			var amtStr string
			switch v := posting["amount"].(type) {
			case string:
				amtStr = v
			case json.Number:
				amtStr = v.String()
			default:
				if posting["amount"] == nil {
					return fmt.Errorf("posting %d: missing amount in original transaction %s", i, originalTxID)
				}
				amtStr = fmt.Sprint(posting["amount"])
			}
			amt, ok := new(big.Int).SetString(amtStr, 10)
			if !ok {
				return fmt.Errorf("posting %d: invalid amount %q in original transaction %s", i, amtStr, originalTxID)
			}
			reversedPostings = append(reversedPostings, PostingInput{
				Source:      fmt.Sprint(dst),
				Destination: fmt.Sprint(src),
				Amount:      amt,
				Asset:       fmt.Sprint(asset),
			})
		}

		ledger, err := txStore.GetLedger(ctx, ledgerID)
		if err != nil {
			return fmt.Errorf("getting ledger: %w", err)
		}

		vt := now
		if atEffectiveDate {
			vt = origTx.ValidTime
		}

		// Balance check inside TX to prevent TOCTOU.
		if !force {
			for _, posting := range reversedPostings {
				if accounts.IsIssuer(posting.Source, ledger.IssuerAccounts) {
					continue
				}
				bal, err := txStore.GetBalance(ctx, ledgerID, posting.Source, posting.Asset, nil, nil)
				if err != nil {
					return err
				}
				current := new(big.Int).Sub(bal.Input, bal.Output)

				// Subtract active holds to get available balance.
				activeHolds, err := txStore.GetActiveHoldsTotal(ctx, ledgerID, posting.Source, posting.Asset)
				if err != nil {
					return fmt.Errorf("getting active holds for %s/%s: %w", posting.Source, posting.Asset, err)
				}
				current.Sub(current, activeHolds)

				if new(big.Int).Sub(current, posting.Amount).Sign() < 0 {
					return fmt.Errorf("%w: reverting would leave %s negative in %s",
						storage.ErrInsufficientFunds, posting.Source, posting.Asset)
				}
			}
		}

		seq, err := txStore.NextLedgerSeq(ctx, ledgerID)
		if err != nil {
			return fmt.Errorf("getting next seq: %w", err)
		}

		// Build posting records for the transaction projection.
		postingRecords := make([]map[string]any, len(reversedPostings))
		for i, rp := range reversedPostings {
			postingRecords[i] = map[string]any{
				"source":      rp.Source,
				"destination": rp.Destination,
				"amount":      rp.Amount.String(),
				"asset":       rp.Asset,
			}
		}

		// Serialize event payload.
		metadata := map[string]any{
			"reverts":       originalTxID,
			"revert_reason": reason,
		}
		payload, err := json.Marshal(map[string]any{
			"transaction_id": txID,
			"postings":       postingRecords,
			"metadata":       metadata,
		})
		if err != nil {
			return fmt.Errorf("marshaling event payload: %w", err)
		}

		if err := txStore.AppendLogEvent(ctx, storage.LogEventRecord{
			EventID:         eventID,
			LedgerID:        ledgerID,
			LedgerSeq:       seq,
			SystemTime:      now,
			ValidTime:       vt,
			Type:            7, // TRANSACTION_REVERTED
			Payload:         payload,
			IdempotencyKey:  idempotencyKey,
			IdempotencyHash: ikHash,
			BatchID:         batchID,
			SchemaVersion:   1,
		}); err != nil {
			return fmt.Errorf("appending log event: %w", err)
		}

		txRec := storage.TransactionRecord{
			LedgerID:      ledgerID,
			TransactionID: txID,
			EventID:       eventID,
			ValidTime:     vt,
			SystemTime:    now,
			Reference:     "",
			Postings:      postingRecords,
			Metadata:      metadata,
		}
		if err := txStore.InsertTransaction(ctx, txRec); err != nil {
			return fmt.Errorf("inserting reverting transaction: %w", err)
		}

		// Insert volume deltas for reversed postings and upsert touched accounts.
		touchedAccounts := make(map[string]bool)
		for _, posting := range reversedPostings {
			if posting.Amount.Sign() == 0 {
				continue
			}
			if err := txStore.InsertVolumeDelta(ctx, storage.VolumeDeltaRecord{
				LedgerID: ledgerID, Account: posting.Source, Asset: posting.Asset,
				EventID: eventID, ValidTime: vt, SystemTime: now,
				InputDelta: big.NewInt(0), OutputDelta: posting.Amount,
			}); err != nil {
				return fmt.Errorf("inserting source volume delta: %w", err)
			}
			if err := txStore.InsertVolumeDelta(ctx, storage.VolumeDeltaRecord{
				LedgerID: ledgerID, Account: posting.Destination, Asset: posting.Asset,
				EventID: eventID, ValidTime: vt, SystemTime: now,
				InputDelta: posting.Amount, OutputDelta: big.NewInt(0),
			}); err != nil {
				return fmt.Errorf("inserting dest volume delta: %w", err)
			}
			touchedAccounts[posting.Source] = true
			touchedAccounts[posting.Destination] = true
		}

		for addr := range touchedAccounts {
			if err := txStore.UpsertAccount(ctx, storage.AccountRecord{
				LedgerID: ledgerID, Address: addr, FirstUsage: now, UpdatedAt: now, Metadata: map[string]any{},
			}); err != nil {
				return fmt.Errorf("upserting account %s: %w", addr, err)
			}
		}

		if err := txStore.InsertRelationship(ctx, storage.RelationshipRecord{
			LedgerID: ledgerID, ParentTxID: originalTxID, ChildTxID: txID,
			RelationshipType: 0, EventID: eventID, SystemTime: now,
		}); err != nil {
			return fmt.Errorf("inserting revert relationship: %w", err)
		}

		if idempotencyKey != "" {
			if err := p.recordIdempotency(ctx, txStore, ledgerID, idempotencyKey, eventID, ikHash); err != nil {
				return err
			}
		}

		if err := txStore.Commit(); err != nil {
			return fmt.Errorf("committing revert transaction: %w", err)
		}

		if idempotencyKey != "" {
			p.postCommitIdempotency(ctx, ledgerID, idempotencyKey, eventID, ikHash)
		}

		p.publishEvent(ctx, ledgerID, eventID, 7)

		p.logger.Debug("transaction reverted",
			zap.String("event_id", eventID),
			zap.String("reverting_tx_id", txID),
			zap.String("original_tx_id", originalTxID),
			zap.String("ledger", ledgerID),
		)

		result = &SubmitResult{EventID: eventID, Transaction: &txRec}
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

	var result *SubmitResult
	err := withDeadlockRetry(ctx, 5, func() error {
		now := time.Now().UTC()
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

		// Verify original transaction exists inside the TX to prevent TOCTOU.
		if _, err := txStore.GetTransaction(ctx, ledgerID, originalTxID); err != nil {
			return err
		}

		seq, err := txStore.NextLedgerSeq(ctx, ledgerID)
		if err != nil {
			return err
		}

		payload, err := json.Marshal(map[string]any{
			"original_transaction_id": originalTxID,
			"metadata_changes":        metadataChanges,
		})
		if err != nil {
			return fmt.Errorf("marshaling payload: %w", err)
		}

		if err := txStore.AppendLogEvent(ctx, storage.LogEventRecord{
			EventID: eventID, LedgerID: ledgerID, LedgerSeq: seq,
			SystemTime: now, ValidTime: now, Type: 8, // TRANSACTION_AMENDED
			Payload: payload, IdempotencyKey: idempotencyKey,
			IdempotencyHash: ikHash,
			BatchID:         batchID, SchemaVersion: 1,
		}); err != nil {
			return err
		}

		if err := txStore.UpdateTransactionMetadata(ctx, ledgerID, originalTxID, metadataChanges); err != nil {
			return fmt.Errorf("updating transaction metadata: %w", err)
		}

		if err := txStore.InsertMetadataHistory(ctx, storage.MetadataHistoryRecord{
			LedgerID: ledgerID, TargetType: 1,
			TargetID: originalTxID, Revision: seq,
			Metadata: metadataChanges, EventID: eventID, SystemTime: now,
		}); err != nil {
			return err
		}

		// ChildTxID references the amendment event_id since amendments don't
		// create new transactions. Relationship queries should handle this.
		if err := txStore.InsertRelationship(ctx, storage.RelationshipRecord{
			LedgerID: ledgerID, ParentTxID: originalTxID,
			ChildTxID: eventID, RelationshipType: 1, // amends
			EventID: eventID, SystemTime: now,
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

		p.publishEvent(ctx, ledgerID, eventID, 8)

		result = &SubmitResult{EventID: eventID}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
