package planner

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/remade/ledger/internal/storage"
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
	return p.doSetMetadataInTx(ctx, txStore, ledgerID, intent.TargetType, intent.TargetID, intent.Metadata, "", nil, now)
}

func (p *Planner) executeDeleteMetadataInTx(ctx context.Context, txStore storage.TxStore, ledgerID string, intent BatchIntent) (*SubmitResult, error) {
	now := time.Now().UTC()
	return p.doDeleteMetadataInTx(ctx, txStore, ledgerID, intent.TargetType, intent.TargetID, intent.MetadataKey, "", nil, now)
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
	return p.doRevertInTx(ctx, txStore, ledgerID, intent.OriginalTxID, intent.Force, intent.AtEffectiveDate, intent.Reason, "", nil, now)
}

func (p *Planner) executeAmendInTx(ctx context.Context, txStore storage.TxStore, ledgerID string, intent BatchIntent) (*SubmitResult, error) {
	now := time.Now().UTC()
	return p.doAmendInTx(ctx, txStore, ledgerID, intent.OriginalTxID, intent.Metadata, "", nil, now)
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
