package planner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/remade/ledger/internal/storage"
	"github.com/remade/ledger/pkg/accounts"
)

// BatchIntent represents a single intent within a batch.
type BatchIntent struct {
	Type           string
	Postings       []PostingInput
	Reference      string
	Metadata       map[string]any
	IdempotencyKey string

	// Authorize fields.
	Source          string
	DestinationHint string
	Asset           string
	Amount          *big.Int
	ExpiresAt       time.Time

	// Capture fields.
	HoldID      string
	Destination string

	// Revert/amend fields.
	OriginalTxID    string
	Force           bool
	AtEffectiveDate bool
	Reason          string

	// Convert fields.
	ConvertParams *ConvertParams

	// Metadata operation fields.
	TargetType  int16
	TargetID    string
	MetadataKey string
}

// BatchIntentResult is the result of a single intent within a batch.
type BatchIntentResult struct {
	EventID string
	Success bool
	Error   string
}

// BatchResult is the output of a batch operation.
type BatchResult struct {
	Results   []BatchIntentResult
	Successes int
	Failures  int
	Failed    bool
	FailedAt  int
	Error     string
}

// SubmitBatch processes a batch of intents with the given mode.
func (p *Planner) SubmitBatch(ctx context.Context, ledgerID string, intents []BatchIntent, mode string) (*BatchResult, error) {
	if _, err := p.checkLedgerNotSealed(ctx, ledgerID); err != nil {
		return nil, err
	}

	results := make([]BatchIntentResult, len(intents))

	switch mode {
	case "ALL_OR_NOTHING":
		// True atomic execution: all intents run inside a single database
		// transaction. If any fails, the entire batch is rolled back.
		txStore, err := p.store.BeginTx(ctx)
		if err != nil {
			return nil, fmt.Errorf("beginning batch tx: %w", err)
		}
		defer txStore.Rollback()

		for i, intent := range intents {
			result, err := p.executeIntentInTx(ctx, txStore, ledgerID, intent)
			if err != nil {
				return &BatchResult{
					Results:  results,
					Failed:   true,
					FailedAt: i,
					Error:    err.Error(),
				}, err
			}
			results[i] = BatchIntentResult{EventID: result.EventID, Success: true}
		}

		if err := txStore.Commit(); err != nil {
			return nil, fmt.Errorf("committing batch: %w", err)
		}
		return &BatchResult{Results: results, Successes: len(intents)}, nil

	case "BEST_EFFORT":
		var successes, failures int
		for i, intent := range intents {
			result, err := p.executeIntent(ctx, ledgerID, intent)
			if err != nil {
				results[i] = BatchIntentResult{Success: false, Error: err.Error()}
				failures++
			} else {
				results[i] = BatchIntentResult{EventID: result.EventID, Success: true}
				successes++
			}
		}
		return &BatchResult{Results: results, Successes: successes, Failures: failures}, nil

	default:
		return nil, fmt.Errorf("unsupported batch mode: %s", mode)
	}
}

func (p *Planner) executeIntent(ctx context.Context, ledgerID string, intent BatchIntent) (*SubmitResult, error) {
	switch intent.Type {
	case "post":
		return p.SubmitPost(ctx, ledgerID, intent.Postings, intent.Reference, intent.Metadata, intent.IdempotencyKey, nil, false)
	case "authorize":
		return p.SubmitAuthorize(ctx, ledgerID, intent.Source, intent.DestinationHint, intent.Asset, intent.Amount, intent.ExpiresAt, intent.IdempotencyKey)
	case "capture":
		return p.SubmitCapture(ctx, ledgerID, intent.HoldID, intent.Amount, intent.Destination, intent.IdempotencyKey)
	case "void":
		return p.SubmitVoid(ctx, ledgerID, intent.HoldID, intent.IdempotencyKey)
	case "revert":
		return p.SubmitRevert(ctx, ledgerID, intent.OriginalTxID, intent.Force, intent.AtEffectiveDate, intent.Reason, intent.IdempotencyKey)
	case "amend":
		return p.SubmitAmend(ctx, ledgerID, intent.OriginalTxID, intent.Metadata, intent.IdempotencyKey)
	case "convert":
		if intent.ConvertParams == nil {
			return nil, fmt.Errorf("convert intent missing params")
		}
		return p.SubmitConvert(ctx, ledgerID, *intent.ConvertParams, intent.IdempotencyKey)
	case "set_metadata":
		return p.SubmitSetMetadata(ctx, ledgerID, intent.TargetType, intent.TargetID, intent.Metadata, intent.IdempotencyKey)
	case "delete_metadata":
		return p.SubmitDeleteMetadata(ctx, ledgerID, intent.TargetType, intent.TargetID, intent.MetadataKey, intent.IdempotencyKey)
	default:
		return nil, fmt.Errorf("unsupported batch intent type: %s", intent.Type)
	}
}

// executeIntentInTx executes a single batch intent against an already-open
// transaction. Used by ALL_OR_NOTHING mode to ensure atomicity.
func (p *Planner) executeIntentInTx(ctx context.Context, txStore storage.TxStore, ledgerID string, intent BatchIntent) (*SubmitResult, error) {
	switch intent.Type {
	case "post":
		return p.executePostInTx(ctx, txStore, ledgerID, intent)
	case "set_metadata":
		return p.executeSetMetadataInTx(ctx, txStore, ledgerID, intent)
	case "delete_metadata":
		return p.executeDeleteMetadataInTx(ctx, txStore, ledgerID, intent)
	case "authorize":
		return p.executeAuthorizeInTx(ctx, txStore, ledgerID, intent)
	case "capture":
		return p.executeCaptureInTx(ctx, txStore, ledgerID, intent)
	case "void":
		return p.executeVoidInTx(ctx, txStore, ledgerID, intent)
	case "revert":
		return p.executeRevertInTx(ctx, txStore, ledgerID, intent)
	case "amend":
		return p.executeAmendInTx(ctx, txStore, ledgerID, intent)
	case "convert":
		return p.executeConvertInTx(ctx, txStore, ledgerID, intent)
	default:
		return nil, fmt.Errorf("unsupported operation type %q in batch", intent.Type)
	}
}

// executePostInTx runs a post intent against the batch's open transaction. It
// reuses the same shared core (doPostInTx) as SubmitPost, so the batch path now
// performs the input + schema validation it previously skipped. Idempotency is
// recorded at the event level only (ikHash nil), matching prior batch behavior.
func (p *Planner) executePostInTx(ctx context.Context, txStore storage.TxStore, ledgerID string, intent BatchIntent) (*SubmitResult, error) {
	ledger, err := txStore.GetLedger(ctx, ledgerID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	return p.doPostInTx(ctx, txStore, ledger, intent.Postings, intent.Reference, intent.Metadata, intent.IdempotencyKey, nil, now, now, false)
}

func (p *Planner) executeSetMetadataInTx(ctx context.Context, txStore storage.TxStore, ledgerID string, intent BatchIntent) (*SubmitResult, error) {
	now := time.Now().UTC()
	eventID := ulid.Make().String()

	batchID, err := p.batch.CurrentBatchID(ctx, ledgerID)
	if err != nil {
		return nil, err
	}

	seq, err := txStore.NextLedgerSeq(ctx, ledgerID)
	if err != nil {
		return nil, err
	}

	payload, err := json.Marshal(map[string]any{
		"target_type": intent.TargetType, "target_id": intent.TargetID, "metadata": intent.Metadata,
	})
	if err != nil {
		return nil, err
	}

	if err := txStore.AppendLogEvent(ctx, storage.LogEventRecord{
		EventID: eventID, LedgerID: ledgerID, LedgerSeq: seq,
		SystemTime: now, ValidTime: now, Type: storage.EventTypeMetadataSet,
		Payload: payload, BatchID: batchID, SchemaVersion: 1,
	}); err != nil {
		return nil, err
	}

	if intent.TargetType == 0 { // ACCOUNT
		if err := txStore.UpsertAccount(ctx, storage.AccountRecord{
			LedgerID: ledgerID, Address: intent.TargetID, FirstUsage: now, UpdatedAt: now, Metadata: intent.Metadata,
		}); err != nil {
			return nil, err
		}
	} else {
		if err := txStore.UpdateTransactionMetadata(ctx, ledgerID, intent.TargetID, intent.Metadata); err != nil {
			return nil, err
		}
	}

	return &SubmitResult{EventID: eventID}, nil
}

func (p *Planner) executeDeleteMetadataInTx(ctx context.Context, txStore storage.TxStore, ledgerID string, intent BatchIntent) (*SubmitResult, error) {
	now := time.Now().UTC()
	eventID := ulid.Make().String()

	batchID, err := p.batch.CurrentBatchID(ctx, ledgerID)
	if err != nil {
		return nil, err
	}

	seq, err := txStore.NextLedgerSeq(ctx, ledgerID)
	if err != nil {
		return nil, err
	}

	payload, err := json.Marshal(map[string]any{
		"target_type": intent.TargetType, "target_id": intent.TargetID, "key": intent.MetadataKey,
	})
	if err != nil {
		return nil, err
	}

	if err := txStore.AppendLogEvent(ctx, storage.LogEventRecord{
		EventID: eventID, LedgerID: ledgerID, LedgerSeq: seq,
		SystemTime: now, ValidTime: now, Type: storage.EventTypeMetadataDeleted,
		Payload: payload, BatchID: batchID, SchemaVersion: 1,
	}); err != nil {
		return nil, err
	}

	if intent.TargetType == 1 { // TRANSACTION
		if err := txStore.DeleteTransactionMetadataKey(ctx, ledgerID, intent.TargetID, intent.MetadataKey); err != nil {
			return nil, err
		}
	}

	return &SubmitResult{EventID: eventID}, nil
}

func (p *Planner) executeAuthorizeInTx(ctx context.Context, txStore storage.TxStore, ledgerID string, intent BatchIntent) (*SubmitResult, error) {
	ledger, err := txStore.GetLedger(ctx, ledgerID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	return p.doAuthorizeInTx(ctx, txStore, ledger, intent.Source, intent.DestinationHint, intent.Asset, intent.Amount, intent.ExpiresAt, "", nil, now)
}

func (p *Planner) executeCaptureInTx(ctx context.Context, txStore storage.TxStore, ledgerID string, intent BatchIntent) (*SubmitResult, error) {
	now := time.Now().UTC()
	return p.doCaptureInTx(ctx, txStore, ledgerID, intent.HoldID, intent.Amount, intent.Destination, "", nil, now)
}

func (p *Planner) executeVoidInTx(ctx context.Context, txStore storage.TxStore, ledgerID string, intent BatchIntent) (*SubmitResult, error) {
	now := time.Now().UTC()
	return p.doVoidInTx(ctx, txStore, ledgerID, intent.HoldID, "", nil, now)
}

func (p *Planner) executeRevertInTx(ctx context.Context, txStore storage.TxStore, ledgerID string, intent BatchIntent) (*SubmitResult, error) {
	now := time.Now().UTC()
	eventID := ulid.Make().String()
	txID := ulid.Make().String()

	batchID, err := p.batch.CurrentBatchID(ctx, ledgerID)
	if err != nil {
		return nil, fmt.Errorf("getting batch ID: %w", err)
	}

	origTx, err := txStore.GetTransaction(ctx, ledgerID, intent.OriginalTxID)
	if err != nil {
		return nil, err
	}

	rels, err := txStore.GetRelationships(ctx, ledgerID, intent.OriginalTxID, 1)
	if err != nil {
		return nil, err
	}
	for _, rel := range rels {
		if rel.RelationshipType == 0 && rel.ParentTxID == intent.OriginalTxID {
			return nil, storage.ErrAlreadyReverted
		}
	}

	data, err := json.Marshal(origTx.Postings)
	if err != nil {
		return nil, fmt.Errorf("marshaling original postings: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var origPostings []map[string]any
	if err := dec.Decode(&origPostings); err != nil {
		return nil, fmt.Errorf("parsing original postings: %w", err)
	}
	if len(origPostings) == 0 {
		return nil, fmt.Errorf("original transaction %s has no postings", intent.OriginalTxID)
	}

	var reversedPostings []PostingInput
	for i, posting := range origPostings {
		var amtStr string
		switch v := posting["amount"].(type) {
		case string:
			amtStr = v
		case json.Number:
			amtStr = v.String()
		default:
			amtStr = fmt.Sprint(posting["amount"])
		}
		amt, ok := new(big.Int).SetString(amtStr, 10)
		if !ok {
			return nil, fmt.Errorf("posting %d: invalid amount %q", i, amtStr)
		}
		reversedPostings = append(reversedPostings, PostingInput{
			Source:      fmt.Sprint(posting["destination"]),
			Destination: fmt.Sprint(posting["source"]),
			Amount:      amt,
			Asset:       fmt.Sprint(posting["asset"]),
		})
	}

	ledger, err := txStore.GetLedger(ctx, ledgerID)
	if err != nil {
		return nil, err
	}

	vt := now
	if intent.AtEffectiveDate {
		vt = origTx.ValidTime
	}

	if !intent.Force {
		for _, posting := range reversedPostings {
			if accounts.IsIssuer(posting.Source, ledger.IssuerAccounts) {
				continue
			}
			bal, err := txStore.GetBalance(ctx, ledgerID, posting.Source, posting.Asset, nil, nil)
			if err != nil {
				return nil, err
			}
			current := new(big.Int).Sub(bal.Input, bal.Output)
			activeHolds, err := txStore.GetActiveHoldsTotal(ctx, ledgerID, posting.Source, posting.Asset)
			if err != nil {
				return nil, err
			}
			current.Sub(current, activeHolds)
			if new(big.Int).Sub(current, posting.Amount).Sign() < 0 {
				return nil, fmt.Errorf("%w: reverting would leave %s negative in %s",
					storage.ErrInsufficientFunds, posting.Source, posting.Asset)
			}
		}
	}

	seq, err := txStore.NextLedgerSeq(ctx, ledgerID)
	if err != nil {
		return nil, err
	}

	postingRecords := make([]map[string]any, len(reversedPostings))
	for i, rp := range reversedPostings {
		postingRecords[i] = map[string]any{
			"source": rp.Source, "destination": rp.Destination,
			"amount": rp.Amount.String(), "asset": rp.Asset,
		}
	}

	metadata := map[string]any{"reverts": intent.OriginalTxID, "revert_reason": intent.Reason}
	payload, err := json.Marshal(map[string]any{
		"transaction_id": txID, "postings": postingRecords, "metadata": metadata,
	})
	if err != nil {
		return nil, err
	}

	if err := txStore.AppendLogEvent(ctx, storage.LogEventRecord{
		EventID: eventID, LedgerID: ledgerID, LedgerSeq: seq,
		SystemTime: now, ValidTime: vt, Type: storage.EventTypeTransactionReverted,
		Payload: payload, BatchID: batchID, SchemaVersion: 1,
	}); err != nil {
		return nil, err
	}

	if err := txStore.InsertTransaction(ctx, storage.TransactionRecord{
		LedgerID: ledgerID, TransactionID: txID, EventID: eventID,
		ValidTime: vt, SystemTime: now, Postings: postingRecords, Metadata: metadata,
	}); err != nil {
		return nil, err
	}

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
			return nil, err
		}
		if err := txStore.InsertVolumeDelta(ctx, storage.VolumeDeltaRecord{
			LedgerID: ledgerID, Account: posting.Destination, Asset: posting.Asset,
			EventID: eventID, ValidTime: vt, SystemTime: now,
			InputDelta: posting.Amount, OutputDelta: big.NewInt(0),
		}); err != nil {
			return nil, err
		}
		touchedAccounts[posting.Source] = true
		touchedAccounts[posting.Destination] = true
	}

	for addr := range touchedAccounts {
		if err := txStore.UpsertAccount(ctx, storage.AccountRecord{
			LedgerID: ledgerID, Address: addr, FirstUsage: now, UpdatedAt: now, Metadata: map[string]any{},
		}); err != nil {
			return nil, err
		}
	}

	if err := txStore.InsertRelationship(ctx, storage.RelationshipRecord{
		LedgerID: ledgerID, ParentTxID: intent.OriginalTxID, ChildTxID: txID,
		RelationshipType: storage.RelationshipTypeReverts, EventID: eventID, SystemTime: now,
	}); err != nil {
		return nil, err
	}

	return &SubmitResult{EventID: eventID, Transaction: &storage.TransactionRecord{
		LedgerID: ledgerID, TransactionID: txID, EventID: eventID,
		ValidTime: vt, SystemTime: now, Postings: postingRecords, Metadata: metadata,
	}}, nil
}

func (p *Planner) executeAmendInTx(ctx context.Context, txStore storage.TxStore, ledgerID string, intent BatchIntent) (*SubmitResult, error) {
	now := time.Now().UTC()
	eventID := ulid.Make().String()

	batchID, err := p.batch.CurrentBatchID(ctx, ledgerID)
	if err != nil {
		return nil, err
	}

	if _, err := txStore.GetTransaction(ctx, ledgerID, intent.OriginalTxID); err != nil {
		return nil, err
	}

	seq, err := txStore.NextLedgerSeq(ctx, ledgerID)
	if err != nil {
		return nil, err
	}

	payload, err := json.Marshal(map[string]any{
		"original_transaction_id": intent.OriginalTxID,
		"metadata_changes":        intent.Metadata,
	})
	if err != nil {
		return nil, err
	}

	if err := txStore.AppendLogEvent(ctx, storage.LogEventRecord{
		EventID: eventID, LedgerID: ledgerID, LedgerSeq: seq,
		SystemTime: now, ValidTime: now, Type: storage.EventTypeTransactionAmended,
		Payload: payload, BatchID: batchID, SchemaVersion: 1,
	}); err != nil {
		return nil, err
	}

	if err := txStore.UpdateTransactionMetadata(ctx, ledgerID, intent.OriginalTxID, intent.Metadata); err != nil {
		return nil, err
	}

	if err := txStore.InsertMetadataHistory(ctx, storage.MetadataHistoryRecord{
		LedgerID: ledgerID, TargetType: storage.TargetTypeTransaction,
		TargetID: intent.OriginalTxID, Revision: seq,
		Metadata: intent.Metadata, EventID: eventID, SystemTime: now,
	}); err != nil {
		return nil, err
	}

	if err := txStore.InsertRelationship(ctx, storage.RelationshipRecord{
		LedgerID: ledgerID, ParentTxID: intent.OriginalTxID,
		ChildTxID: eventID, RelationshipType: storage.RelationshipTypeAmends,
		EventID: eventID, SystemTime: now,
	}); err != nil {
		return nil, err
	}

	return &SubmitResult{EventID: eventID}, nil
}

func (p *Planner) executeConvertInTx(ctx context.Context, txStore storage.TxStore, ledgerID string, intent BatchIntent) (*SubmitResult, error) {
	if intent.ConvertParams == nil {
		return nil, fmt.Errorf("convert intent missing params")
	}
	params := *intent.ConvertParams

	ledger, err := txStore.GetLedger(ctx, ledgerID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	vt := now
	if params.ValidTime != nil {
		vt = *params.ValidTime
	}
	// Batch convert events carry no idempotency key (ikHash nil), matching prior behavior.
	return p.doConvertInTx(ctx, txStore, ledger, params, "", nil, vt, now)
}
