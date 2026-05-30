package planner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/remade/ledger/internal/ir"
	"github.com/remade/ledger/internal/storage"
	"github.com/remade/ledger/pkg/accounts"
	"github.com/remade/ledger/pkg/assets"
)

// This file holds the shared in-transaction cores (do*InTx) used by both the
// standalone Submit* methods and the batch execute*InTx methods. Each do*InTx:
//   - runs ALL input/schema/business validation, so the batch path gets the same
//     defensive checks the standalone path always had (closing the prior gap);
//   - operates on a caller-supplied open transaction (never BeginTx/Commit/Rollback
//     itself — no nested transactions);
//   - leaves the L1/L2 idempotency *lookup* and post-commit cache/publish to the
//     Submit* layer.

// validatePostings checks the structural validity of a set of postings.
func validatePostings(postings []PostingInput) error {
	if len(postings) == 0 {
		return fmt.Errorf("at least one posting is required")
	}
	for i, posting := range postings {
		if err := accounts.Validate(posting.Source); err != nil {
			return fmt.Errorf("posting %d source: %w", i, err)
		}
		if err := accounts.Validate(posting.Destination); err != nil {
			return fmt.Errorf("posting %d destination: %w", i, err)
		}
		if err := assets.Validate(posting.Asset); err != nil {
			return fmt.Errorf("posting %d asset: %w", i, err)
		}
		if posting.Amount == nil || posting.Amount.Sign() <= 0 {
			return fmt.Errorf("posting %d: amount must be positive", i)
		}
	}
	return nil
}

// doPostInTx is the shared core of a post operation. It validates, enforces the
// chart-of-accounts schema, checks balances (TOCTOU-safe inside the tx), and
// writes the event + transaction + volume deltas + accounts. It records the
// idempotency key only when ikHash is non-nil (the standalone path supplies it;
// the batch path passes nil and manages idempotency at the event level only).
// On dryRun it returns after balance checks without writing; the caller rolls
// back. The ErrIdempotencyKeyConflict from AppendLogEvent is propagated for the
// caller to interpret.
func (p *Planner) doPostInTx(ctx context.Context, txStore storage.TxStore, ledger *storage.LedgerRecord, postings []PostingInput, reference string, metadata map[string]any, idempotencyKey string, ikHash []byte, vt, now time.Time, dryRun bool) (*SubmitResult, error) {
	if err := validatePostings(postings); err != nil {
		return nil, err
	}

	// Schema enforcement against the active chart of accounts.
	if mode := ir.SchemaEnforcementMode(ledger.Features["schema_enforcement"]); mode == ir.SchemaStrict || mode == ir.SchemaBestEffort {
		if err := p.validatePostingsAgainstSchema(ctx, ledger.ID, ledger.Features["active_schema_version"], postings, mode); err != nil {
			return nil, err
		}
	}

	// Compute net outputs/inputs per (account, asset).
	type acctAsset struct{ Account, Asset string }
	netOutputs := make(map[acctAsset]*big.Int)
	netInputs := make(map[acctAsset]*big.Int)
	for _, posting := range postings {
		if posting.Amount.Sign() == 0 {
			continue
		}
		srcKey := acctAsset{posting.Source, posting.Asset}
		if netOutputs[srcKey] == nil {
			netOutputs[srcKey] = new(big.Int)
		}
		netOutputs[srcKey].Add(netOutputs[srcKey], posting.Amount)

		dstKey := acctAsset{posting.Destination, posting.Asset}
		if netInputs[dstKey] == nil {
			netInputs[dstKey] = new(big.Int)
		}
		netInputs[dstKey].Add(netInputs[dstKey], posting.Amount)
	}

	// Balance checks for non-issuer source accounts (inside the tx, no TOCTOU).
	for key, output := range netOutputs {
		if accounts.IsIssuer(key.Account, ledger.IssuerAccounts) {
			continue
		}
		bal, err := txStore.GetBalance(ctx, ledger.ID, key.Account, key.Asset, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("getting balance for %s/%s: %w", key.Account, key.Asset, err)
		}
		currentBalance := new(big.Int).Sub(bal.Input, bal.Output)
		activeHolds, err := txStore.GetActiveHoldsTotal(ctx, ledger.ID, key.Account, key.Asset)
		if err != nil {
			return nil, fmt.Errorf("getting active holds for %s/%s: %w", key.Account, key.Asset, err)
		}
		currentBalance.Sub(currentBalance, activeHolds)
		if netIn, ok := netInputs[key]; ok {
			currentBalance.Add(currentBalance, netIn)
		}
		if new(big.Int).Sub(currentBalance, output).Sign() < 0 {
			return nil, fmt.Errorf("%w: account %s, asset %s", storage.ErrInsufficientFunds, key.Account, key.Asset)
		}
	}

	// Dry-run stops here, before any writes; the caller rolls the tx back.
	if dryRun {
		return &SubmitResult{EventID: "dry-run"}, nil
	}

	eventID := ulid.Make().String()
	txID := ulid.Make().String()
	batchID, err := p.batch.CurrentBatchID(ctx, ledger.ID)
	if err != nil {
		return nil, fmt.Errorf("getting batch ID: %w", err)
	}
	seq, err := txStore.NextLedgerSeq(ctx, ledger.ID)
	if err != nil {
		return nil, fmt.Errorf("getting next seq: %w", err)
	}

	postingRecords := make([]map[string]any, len(postings))
	for i, pp := range postings {
		postingRecords[i] = map[string]any{
			"source":      pp.Source,
			"destination": pp.Destination,
			"amount":      pp.Amount.String(),
			"asset":       pp.Asset,
		}
	}

	payload, err := json.Marshal(map[string]any{
		"transaction_id": txID,
		"postings":       postingRecords,
		"metadata":       metadata,
		"reference":      reference,
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling event payload: %w", err)
	}

	logEvent := storage.LogEventRecord{
		EventID:        eventID,
		LedgerID:       ledger.ID,
		LedgerSeq:      seq,
		SystemTime:     now,
		ValidTime:      vt,
		Type:           storage.EventTypeTransactionPosted,
		Payload:        payload,
		IdempotencyKey: idempotencyKey,
		BatchID:        batchID,
		SchemaVersion:  1,
	}
	if idempotencyKey != "" && ikHash != nil {
		logEvent.IdempotencyHash = ikHash
	}
	if err := txStore.AppendLogEvent(ctx, logEvent); err != nil {
		return nil, err
	}

	txRec := storage.TransactionRecord{
		LedgerID:      ledger.ID,
		TransactionID: txID,
		EventID:       eventID,
		ValidTime:     vt,
		SystemTime:    now,
		Reference:     reference,
		Postings:      postingRecords,
		Metadata:      metadata,
	}
	if err := txStore.InsertTransaction(ctx, txRec); err != nil {
		if errors.Is(err, storage.ErrTransactionReferenceConflict) {
			return nil, err
		}
		return nil, fmt.Errorf("inserting transaction: %w", err)
	}

	if len(metadata) > 0 {
		if err := txStore.InsertMetadataHistory(ctx, storage.MetadataHistoryRecord{
			LedgerID:   ledger.ID,
			TargetType: storage.TargetTypeTransaction,
			TargetID:   txID,
			Revision:   0,
			Metadata:   metadata,
			EventID:    eventID,
			SystemTime: now,
		}); err != nil {
			return nil, fmt.Errorf("inserting initial metadata history: %w", err)
		}
	}

	touchedAccounts := make(map[string]bool)
	for _, posting := range postings {
		if posting.Amount.Sign() == 0 {
			continue
		}
		if err := txStore.InsertVolumeDelta(ctx, storage.VolumeDeltaRecord{
			LedgerID:    ledger.ID,
			Account:     posting.Source,
			Asset:       posting.Asset,
			EventID:     eventID,
			ValidTime:   vt,
			SystemTime:  now,
			InputDelta:  big.NewInt(0),
			OutputDelta: posting.Amount,
		}); err != nil {
			return nil, fmt.Errorf("inserting source volume delta: %w", err)
		}
		if err := txStore.InsertVolumeDelta(ctx, storage.VolumeDeltaRecord{
			LedgerID:    ledger.ID,
			Account:     posting.Destination,
			Asset:       posting.Asset,
			EventID:     eventID,
			ValidTime:   vt,
			SystemTime:  now,
			InputDelta:  posting.Amount,
			OutputDelta: big.NewInt(0),
		}); err != nil {
			return nil, fmt.Errorf("inserting dest volume delta: %w", err)
		}
		touchedAccounts[posting.Source] = true
		touchedAccounts[posting.Destination] = true
	}

	for addr := range touchedAccounts {
		if err := txStore.UpsertAccount(ctx, storage.AccountRecord{
			LedgerID:   ledger.ID,
			Address:    addr,
			FirstUsage: now,
			UpdatedAt:  now,
			Metadata:   map[string]any{},
		}); err != nil {
			return nil, fmt.Errorf("upserting account %s: %w", addr, err)
		}
	}

	if idempotencyKey != "" && ikHash != nil {
		if err := p.recordIdempotency(ctx, txStore, ledger.ID, idempotencyKey, eventID, ikHash); err != nil {
			return nil, err
		}
	}

	return &SubmitResult{EventID: eventID, Transaction: &txRec}, nil
}

// validateConvertParams checks the structural validity of a conversion.
func validateConvertParams(params ConvertParams) error {
	if err := accounts.Validate(params.Source); err != nil {
		return fmt.Errorf("source: %w", err)
	}
	if err := accounts.Validate(params.Destination); err != nil {
		return fmt.Errorf("destination: %w", err)
	}
	if err := assets.Validate(params.SourceAsset); err != nil {
		return fmt.Errorf("source_asset: %w", err)
	}
	if err := assets.Validate(params.DestAsset); err != nil {
		return fmt.Errorf("destination_asset: %w", err)
	}
	if params.SlippageAccount != "" {
		if err := accounts.Validate(params.SlippageAccount); err != nil {
			return fmt.Errorf("slippage_account: %w", err)
		}
	}
	if params.SourceAmount == nil || params.SourceAmount.Sign() <= 0 {
		return fmt.Errorf("source_amount must be positive")
	}
	if params.DestAmount == nil || params.DestAmount.Sign() <= 0 {
		return fmt.Errorf("destination_amount must be positive")
	}
	if params.SourceAsset == params.DestAsset {
		return fmt.Errorf("source_asset and destination_asset must differ for conversion")
	}
	if params.Rate != "" {
		if _, _, err := big.ParseFloat(params.Rate, 10, 128, big.ToNearestEven); err != nil {
			return fmt.Errorf("invalid conversion rate %q: %w", params.Rate, err)
		}
	}
	return nil
}

// doConvertInTx is the shared core of an FX conversion: validate, balance-check
// the source (incl. slippage), and write the event + zero-sum 4-leg volume
// deltas (+ optional slippage leg) + accounts + transaction projection. Records
// idempotency only when ikHash is non-nil (standalone path).
func (p *Planner) doConvertInTx(ctx context.Context, txStore storage.TxStore, ledger *storage.LedgerRecord, params ConvertParams, idempotencyKey string, ikHash []byte, vt, now time.Time) (*SubmitResult, error) {
	if err := validateConvertParams(params); err != nil {
		return nil, err
	}

	eventID := ulid.Make().String()
	conversionID := ulid.Make().String()
	txID := ulid.Make().String()

	batchID, err := p.batch.CurrentBatchID(ctx, ledger.ID)
	if err != nil {
		return nil, fmt.Errorf("getting batch ID: %w", err)
	}
	seq, err := txStore.NextLedgerSeq(ctx, ledger.ID)
	if err != nil {
		return nil, err
	}

	// Balance check inside the tx (TOCTOU-safe); total source output includes slippage.
	if !accounts.IsIssuer(params.Source, ledger.IssuerAccounts) {
		bal, err := txStore.GetBalance(ctx, ledger.ID, params.Source, params.SourceAsset, nil, nil)
		if err != nil {
			return nil, err
		}
		current := new(big.Int).Sub(bal.Input, bal.Output)
		activeHolds, err := txStore.GetActiveHoldsTotal(ctx, ledger.ID, params.Source, params.SourceAsset)
		if err != nil {
			return nil, fmt.Errorf("getting active holds for %s/%s: %w", params.Source, params.SourceAsset, err)
		}
		current.Sub(current, activeHolds)
		totalSourceOutput := new(big.Int).Set(params.SourceAmount)
		if params.SlippageAmount != nil && params.SlippageAmount.Sign() > 0 {
			totalSourceOutput.Add(totalSourceOutput, params.SlippageAmount)
		}
		if current.Cmp(totalSourceOutput) < 0 {
			return nil, fmt.Errorf("%w: account %s, asset %s", storage.ErrInsufficientFunds, params.Source, params.SourceAsset)
		}
	}

	payload, err := json.Marshal(map[string]string{
		"conversion_id":      conversionID,
		"source":             params.Source,
		"destination":        params.Destination,
		"source_amount":      params.SourceAmount.String(),
		"source_asset":       params.SourceAsset,
		"destination_amount": params.DestAmount.String(),
		"destination_asset":  params.DestAsset,
		"rate":               params.Rate,
		"rate_source":        params.RateSource,
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling payload: %w", err)
	}

	logEvent := storage.LogEventRecord{
		EventID: eventID, LedgerID: ledger.ID, LedgerSeq: seq,
		SystemTime: now, ValidTime: vt, Type: storage.EventTypeConversionCreated,
		Payload: payload, IdempotencyKey: idempotencyKey, BatchID: batchID, SchemaVersion: 1,
	}
	if idempotencyKey != "" && ikHash != nil {
		logEvent.IdempotencyHash = ikHash
	}
	if err := txStore.AppendLogEvent(ctx, logEvent); err != nil {
		return nil, err
	}

	// Zero-sum 4-leg deltas: leg1 source_asset source->dest, leg2 dest_asset dest->source.
	legs := []storage.VolumeDeltaRecord{
		{LedgerID: ledger.ID, Account: params.Source, Asset: params.SourceAsset, EventID: eventID, ValidTime: vt, SystemTime: now, InputDelta: big.NewInt(0), OutputDelta: params.SourceAmount},
		{LedgerID: ledger.ID, Account: params.Destination, Asset: params.SourceAsset, EventID: eventID, ValidTime: vt, SystemTime: now, InputDelta: params.SourceAmount, OutputDelta: big.NewInt(0)},
		{LedgerID: ledger.ID, Account: params.Destination, Asset: params.DestAsset, EventID: eventID, ValidTime: vt, SystemTime: now, InputDelta: big.NewInt(0), OutputDelta: params.DestAmount},
		{LedgerID: ledger.ID, Account: params.Source, Asset: params.DestAsset, EventID: eventID, ValidTime: vt, SystemTime: now, InputDelta: params.DestAmount, OutputDelta: big.NewInt(0)},
	}
	hasSlippage := params.SlippageAmount != nil && params.SlippageAmount.Sign() > 0 && params.SlippageAccount != ""
	if hasSlippage {
		legs = append(legs,
			storage.VolumeDeltaRecord{LedgerID: ledger.ID, Account: params.Source, Asset: params.SourceAsset, EventID: eventID, ValidTime: vt, SystemTime: now, InputDelta: big.NewInt(0), OutputDelta: params.SlippageAmount},
			storage.VolumeDeltaRecord{LedgerID: ledger.ID, Account: params.SlippageAccount, Asset: params.SourceAsset, EventID: eventID, ValidTime: vt, SystemTime: now, InputDelta: params.SlippageAmount, OutputDelta: big.NewInt(0)},
		)
	}
	for _, leg := range legs {
		if err := txStore.InsertVolumeDelta(ctx, leg); err != nil {
			return nil, err
		}
	}

	accountsToUpsert := []string{params.Source, params.Destination}
	if hasSlippage {
		accountsToUpsert = append(accountsToUpsert, params.SlippageAccount)
	}
	for _, addr := range accountsToUpsert {
		if err := txStore.UpsertAccount(ctx, storage.AccountRecord{
			LedgerID: ledger.ID, Address: addr, FirstUsage: now, UpdatedAt: now, Metadata: map[string]any{},
		}); err != nil {
			return nil, fmt.Errorf("upserting account: %w", err)
		}
	}

	postingRecords := []map[string]any{
		{"source": params.Source, "destination": params.Destination, "amount": params.SourceAmount.String(), "asset": params.SourceAsset},
		{"source": params.Destination, "destination": params.Source, "amount": params.DestAmount.String(), "asset": params.DestAsset},
	}
	if hasSlippage {
		postingRecords = append(postingRecords, map[string]any{
			"source": params.Source, "destination": params.SlippageAccount,
			"amount": params.SlippageAmount.String(), "asset": params.SourceAsset,
		})
	}
	if err := txStore.InsertTransaction(ctx, storage.TransactionRecord{
		LedgerID: ledger.ID, TransactionID: txID, EventID: eventID,
		ValidTime: vt, SystemTime: now, Postings: postingRecords,
		Metadata: map[string]any{"type": "conversion", "rate": params.Rate, "rate_source": params.RateSource},
	}); err != nil {
		return nil, fmt.Errorf("inserting conversion transaction: %w", err)
	}

	if idempotencyKey != "" && ikHash != nil {
		if err := p.recordIdempotency(ctx, txStore, ledger.ID, idempotencyKey, eventID, ikHash); err != nil {
			return nil, err
		}
	}

	return &SubmitResult{EventID: eventID, ConversionID: conversionID}, nil
}

// doAuthorizeInTx is the shared core of an authorization (hold). It validates
// the amount, checks available balance (posted minus active holds) unless the
// source is an issuer, writes the event, inserts the hold, and upserts the
// source account.
func (p *Planner) doAuthorizeInTx(ctx context.Context, txStore storage.TxStore, ledger *storage.LedgerRecord, source, destHint, asset string, amount *big.Int, expiresAt time.Time, idempotencyKey string, ikHash []byte, now time.Time) (*SubmitResult, error) {
	if amount == nil || amount.Sign() <= 0 {
		return nil, fmt.Errorf("authorize amount must be positive")
	}

	eventID := ulid.Make().String()
	holdID := ulid.Make().String()
	batchID, err := p.batch.CurrentBatchID(ctx, ledger.ID)
	if err != nil {
		return nil, fmt.Errorf("getting batch ID: %w", err)
	}
	seq, err := txStore.NextLedgerSeq(ctx, ledger.ID)
	if err != nil {
		return nil, err
	}

	bal, err := txStore.GetBalance(ctx, ledger.ID, source, asset, nil, nil)
	if err != nil {
		return nil, err
	}
	postedBalance := new(big.Int).Sub(bal.Input, bal.Output)
	activeHolds, err := txStore.GetActiveHoldsTotal(ctx, ledger.ID, source, asset)
	if err != nil {
		return nil, err
	}
	availableBalance := new(big.Int).Sub(postedBalance, activeHolds)
	if !accounts.IsIssuer(source, ledger.IssuerAccounts) && availableBalance.Cmp(amount) < 0 {
		return nil, fmt.Errorf("%w: account %s, asset %s (available: %s, requested: %s)",
			storage.ErrInsufficientFunds, source, asset, availableBalance, amount)
	}

	payload, err := json.Marshal(map[string]string{
		"hold_id": holdID, "source": source, "destination_hint": destHint,
		"amount": amount.String(), "asset": asset,
		"expires_at": expiresAt.Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling payload: %w", err)
	}

	logEvent := storage.LogEventRecord{
		EventID: eventID, LedgerID: ledger.ID, LedgerSeq: seq,
		SystemTime: now, ValidTime: now, Type: storage.EventTypeHoldCreated,
		Payload: payload, IdempotencyKey: idempotencyKey, BatchID: batchID, SchemaVersion: 1,
	}
	if idempotencyKey != "" && ikHash != nil {
		logEvent.IdempotencyHash = ikHash
	}
	if err := txStore.AppendLogEvent(ctx, logEvent); err != nil {
		return nil, err
	}

	if err := txStore.InsertHold(ctx, storage.HoldRecord{
		LedgerID: ledger.ID, HoldID: holdID, Source: source,
		DestinationHint: destHint, Asset: asset,
		AuthorizedAmount: amount, CapturedAmount: new(big.Int),
		ExpiresAt: expiresAt, AuthorizedEventID: eventID,
		ValidTime: now, SystemTime: now,
	}); err != nil {
		return nil, err
	}

	if err := txStore.UpsertAccount(ctx, storage.AccountRecord{
		LedgerID: ledger.ID, Address: source, FirstUsage: now, UpdatedAt: now, Metadata: map[string]any{},
	}); err != nil {
		return nil, fmt.Errorf("upserting source account: %w", err)
	}

	if idempotencyKey != "" && ikHash != nil {
		if err := p.recordIdempotency(ctx, txStore, ledger.ID, idempotencyKey, eventID, ikHash); err != nil {
			return nil, err
		}
	}

	return &SubmitResult{EventID: eventID, HoldID: holdID}, nil
}

// doCaptureInTx is the shared core of a hold capture: re-reads the hold (TOCTOU),
// rejects voided/expired/over-capture, writes the event, advances captured
// amount, and moves funds source -> destination.
func (p *Planner) doCaptureInTx(ctx context.Context, txStore storage.TxStore, ledgerID, holdID string, amount *big.Int, destination, idempotencyKey string, ikHash []byte, now time.Time) (*SubmitResult, error) {
	eventID := ulid.Make().String()
	batchID, err := p.batch.CurrentBatchID(ctx, ledgerID)
	if err != nil {
		return nil, fmt.Errorf("getting batch ID: %w", err)
	}
	seq, err := txStore.NextLedgerSeq(ctx, ledgerID)
	if err != nil {
		return nil, err
	}

	hold, err := txStore.GetHold(ctx, ledgerID, holdID)
	if err != nil {
		return nil, err
	}
	if hold.Voided {
		return nil, fmt.Errorf("%w: %s", storage.ErrHoldVoided, holdID)
	}
	if hold.Expired {
		return nil, fmt.Errorf("%w: %s", storage.ErrHoldExpired, holdID)
	}

	remaining := new(big.Int).Sub(hold.AuthorizedAmount, hold.CapturedAmount)
	if amount.Cmp(remaining) > 0 {
		return nil, fmt.Errorf("%w: capture %s exceeds remaining %s on hold %s",
			storage.ErrInsufficientFunds, amount, remaining, holdID)
	}

	dest := destination
	if dest == "" {
		dest = hold.DestinationHint
	}
	if dest == "" {
		return nil, fmt.Errorf("destination required for capture (no destination_hint on hold)")
	}

	payload, err := json.Marshal(map[string]string{
		"hold_id": holdID, "amount": amount.String(), "destination": dest,
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling payload: %w", err)
	}

	logEvent := storage.LogEventRecord{
		EventID: eventID, LedgerID: ledgerID, LedgerSeq: seq,
		SystemTime: now, ValidTime: now, Type: storage.EventTypeHoldConfirmed,
		Payload: payload, IdempotencyKey: idempotencyKey, BatchID: batchID, SchemaVersion: 1,
	}
	if idempotencyKey != "" && ikHash != nil {
		logEvent.IdempotencyHash = ikHash
	}
	if err := txStore.AppendLogEvent(ctx, logEvent); err != nil {
		return nil, err
	}

	if err := txStore.UpdateHoldCaptured(ctx, ledgerID, holdID, amount); err != nil {
		return nil, err
	}

	if err := txStore.InsertVolumeDelta(ctx, storage.VolumeDeltaRecord{
		LedgerID: ledgerID, Account: hold.Source, Asset: hold.Asset,
		EventID: eventID, ValidTime: now, SystemTime: now,
		InputDelta: big.NewInt(0), OutputDelta: amount,
	}); err != nil {
		return nil, err
	}
	if err := txStore.InsertVolumeDelta(ctx, storage.VolumeDeltaRecord{
		LedgerID: ledgerID, Account: dest, Asset: hold.Asset,
		EventID: eventID, ValidTime: now, SystemTime: now,
		InputDelta: amount, OutputDelta: big.NewInt(0),
	}); err != nil {
		return nil, err
	}

	if err := txStore.UpsertAccount(ctx, storage.AccountRecord{
		LedgerID: ledgerID, Address: dest, FirstUsage: now, UpdatedAt: now, Metadata: map[string]any{},
	}); err != nil {
		return nil, fmt.Errorf("upserting destination account: %w", err)
	}

	if idempotencyKey != "" && ikHash != nil {
		if err := p.recordIdempotency(ctx, txStore, ledgerID, idempotencyKey, eventID, ikHash); err != nil {
			return nil, err
		}
	}

	return &SubmitResult{EventID: eventID}, nil
}

// reversePostings parses an original transaction's postings (json.Number to keep
// precision) and produces the swapped-direction reversing postings.
func reversePostings(rawPostings any, originalTxID string) ([]PostingInput, error) {
	data, err := json.Marshal(rawPostings)
	if err != nil {
		return nil, fmt.Errorf("marshaling original postings for revert: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var origPostings []map[string]any
	if err := dec.Decode(&origPostings); err != nil {
		return nil, fmt.Errorf("parsing original postings for revert: %w", err)
	}
	if len(origPostings) == 0 {
		return nil, fmt.Errorf("original transaction %s has no postings to revert", originalTxID)
	}

	reversed := make([]PostingInput, 0, len(origPostings))
	for i, posting := range origPostings {
		src, srcOK := posting["source"]
		dst, dstOK := posting["destination"]
		asset, assetOK := posting["asset"]
		if !srcOK || !dstOK || !assetOK {
			return nil, fmt.Errorf("posting %d: missing required field (source/destination/asset) in original transaction %s", i, originalTxID)
		}
		var amtStr string
		switch v := posting["amount"].(type) {
		case string:
			amtStr = v
		case json.Number:
			amtStr = v.String()
		default:
			if posting["amount"] == nil {
				return nil, fmt.Errorf("posting %d: missing amount in original transaction %s", i, originalTxID)
			}
			amtStr = fmt.Sprint(posting["amount"])
		}
		amt, ok := new(big.Int).SetString(amtStr, 10)
		if !ok {
			return nil, fmt.Errorf("posting %d: invalid amount %q in original transaction %s", i, amtStr, originalTxID)
		}
		reversed = append(reversed, PostingInput{
			Source:      fmt.Sprint(dst),
			Destination: fmt.Sprint(src),
			Amount:      amt,
			Asset:       fmt.Sprint(asset),
		})
	}
	return reversed, nil
}

// doRevertInTx is the shared core of a transaction reversal: reads the original,
// rejects double-revert, reverses postings, balance-checks (unless force), and
// writes the reverting event/transaction/deltas + a reverts relationship.
func (p *Planner) doRevertInTx(ctx context.Context, txStore storage.TxStore, ledgerID, originalTxID string, force, atEffectiveDate bool, reason, idempotencyKey string, ikHash []byte, now time.Time) (*SubmitResult, error) {
	eventID := ulid.Make().String()
	txID := ulid.Make().String()
	batchID, err := p.batch.CurrentBatchID(ctx, ledgerID)
	if err != nil {
		return nil, fmt.Errorf("getting batch ID: %w", err)
	}

	origTx, err := txStore.GetTransaction(ctx, ledgerID, originalTxID)
	if err != nil {
		return nil, err
	}

	rels, err := txStore.GetRelationships(ctx, ledgerID, originalTxID, 1)
	if err != nil {
		return nil, err
	}
	for _, rel := range rels {
		if rel.RelationshipType == 0 && rel.ParentTxID == originalTxID {
			return nil, storage.ErrAlreadyReverted
		}
	}

	reversedPostings, err := reversePostings(origTx.Postings, originalTxID)
	if err != nil {
		return nil, err
	}

	ledger, err := txStore.GetLedger(ctx, ledgerID)
	if err != nil {
		return nil, fmt.Errorf("getting ledger: %w", err)
	}

	vt := now
	if atEffectiveDate {
		vt = origTx.ValidTime
	}

	if !force {
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
				return nil, fmt.Errorf("getting active holds for %s/%s: %w", posting.Source, posting.Asset, err)
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
		return nil, fmt.Errorf("getting next seq: %w", err)
	}

	postingRecords := make([]map[string]any, len(reversedPostings))
	for i, rp := range reversedPostings {
		postingRecords[i] = map[string]any{
			"source": rp.Source, "destination": rp.Destination,
			"amount": rp.Amount.String(), "asset": rp.Asset,
		}
	}

	metadata := map[string]any{"reverts": originalTxID, "revert_reason": reason}
	payload, err := json.Marshal(map[string]any{
		"transaction_id": txID, "postings": postingRecords, "metadata": metadata,
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling event payload: %w", err)
	}

	logEvent := storage.LogEventRecord{
		EventID: eventID, LedgerID: ledgerID, LedgerSeq: seq,
		SystemTime: now, ValidTime: vt, Type: storage.EventTypeTransactionReverted,
		Payload: payload, IdempotencyKey: idempotencyKey, BatchID: batchID, SchemaVersion: 1,
	}
	if idempotencyKey != "" && ikHash != nil {
		logEvent.IdempotencyHash = ikHash
	}
	if err := txStore.AppendLogEvent(ctx, logEvent); err != nil {
		return nil, fmt.Errorf("appending log event: %w", err)
	}

	txRec := storage.TransactionRecord{
		LedgerID: ledgerID, TransactionID: txID, EventID: eventID,
		ValidTime: vt, SystemTime: now, Postings: postingRecords, Metadata: metadata,
	}
	if err := txStore.InsertTransaction(ctx, txRec); err != nil {
		return nil, fmt.Errorf("inserting reverting transaction: %w", err)
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
			return nil, fmt.Errorf("inserting source volume delta: %w", err)
		}
		if err := txStore.InsertVolumeDelta(ctx, storage.VolumeDeltaRecord{
			LedgerID: ledgerID, Account: posting.Destination, Asset: posting.Asset,
			EventID: eventID, ValidTime: vt, SystemTime: now,
			InputDelta: posting.Amount, OutputDelta: big.NewInt(0),
		}); err != nil {
			return nil, fmt.Errorf("inserting dest volume delta: %w", err)
		}
		touchedAccounts[posting.Source] = true
		touchedAccounts[posting.Destination] = true
	}

	for addr := range touchedAccounts {
		if err := txStore.UpsertAccount(ctx, storage.AccountRecord{
			LedgerID: ledgerID, Address: addr, FirstUsage: now, UpdatedAt: now, Metadata: map[string]any{},
		}); err != nil {
			return nil, fmt.Errorf("upserting account %s: %w", addr, err)
		}
	}

	if err := txStore.InsertRelationship(ctx, storage.RelationshipRecord{
		LedgerID: ledgerID, ParentTxID: originalTxID, ChildTxID: txID,
		RelationshipType: storage.RelationshipTypeReverts, EventID: eventID, SystemTime: now,
	}); err != nil {
		return nil, fmt.Errorf("inserting revert relationship: %w", err)
	}

	if idempotencyKey != "" && ikHash != nil {
		if err := p.recordIdempotency(ctx, txStore, ledgerID, idempotencyKey, eventID, ikHash); err != nil {
			return nil, err
		}
	}

	return &SubmitResult{EventID: eventID, Transaction: &txRec}, nil
}

// doAmendInTx is the shared core of a metadata amendment on an existing
// transaction: verifies it exists, writes the event, overlays metadata, records
// history, and links an amends relationship.
func (p *Planner) doAmendInTx(ctx context.Context, txStore storage.TxStore, ledgerID, originalTxID string, metadataChanges map[string]any, idempotencyKey string, ikHash []byte, now time.Time) (*SubmitResult, error) {
	eventID := ulid.Make().String()
	batchID, err := p.batch.CurrentBatchID(ctx, ledgerID)
	if err != nil {
		return nil, fmt.Errorf("getting batch ID: %w", err)
	}

	if _, err := txStore.GetTransaction(ctx, ledgerID, originalTxID); err != nil {
		return nil, err
	}

	seq, err := txStore.NextLedgerSeq(ctx, ledgerID)
	if err != nil {
		return nil, err
	}

	payload, err := json.Marshal(map[string]any{
		"original_transaction_id": originalTxID,
		"metadata_changes":        metadataChanges,
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling payload: %w", err)
	}

	logEvent := storage.LogEventRecord{
		EventID: eventID, LedgerID: ledgerID, LedgerSeq: seq,
		SystemTime: now, ValidTime: now, Type: storage.EventTypeTransactionAmended,
		Payload: payload, IdempotencyKey: idempotencyKey, BatchID: batchID, SchemaVersion: 1,
	}
	if idempotencyKey != "" && ikHash != nil {
		logEvent.IdempotencyHash = ikHash
	}
	if err := txStore.AppendLogEvent(ctx, logEvent); err != nil {
		return nil, err
	}

	if err := txStore.UpdateTransactionMetadata(ctx, ledgerID, originalTxID, metadataChanges); err != nil {
		return nil, fmt.Errorf("updating transaction metadata: %w", err)
	}

	if err := txStore.InsertMetadataHistory(ctx, storage.MetadataHistoryRecord{
		LedgerID: ledgerID, TargetType: storage.TargetTypeTransaction,
		TargetID: originalTxID, Revision: seq,
		Metadata: metadataChanges, EventID: eventID, SystemTime: now,
	}); err != nil {
		return nil, err
	}

	if err := txStore.InsertRelationship(ctx, storage.RelationshipRecord{
		LedgerID: ledgerID, ParentTxID: originalTxID,
		ChildTxID: eventID, RelationshipType: storage.RelationshipTypeAmends,
		EventID: eventID, SystemTime: now,
	}); err != nil {
		return nil, err
	}

	if idempotencyKey != "" && ikHash != nil {
		if err := p.recordIdempotency(ctx, txStore, ledgerID, idempotencyKey, eventID, ikHash); err != nil {
			return nil, err
		}
	}

	return &SubmitResult{EventID: eventID}, nil
}

// doVoidInTx is the shared core of a hold void: re-reads the hold, rejects
// voided/expired, writes the event, and releases the hold.
func (p *Planner) doVoidInTx(ctx context.Context, txStore storage.TxStore, ledgerID, holdID, idempotencyKey string, ikHash []byte, now time.Time) (*SubmitResult, error) {
	eventID := ulid.Make().String()
	batchID, err := p.batch.CurrentBatchID(ctx, ledgerID)
	if err != nil {
		return nil, fmt.Errorf("getting batch ID: %w", err)
	}
	seq, err := txStore.NextLedgerSeq(ctx, ledgerID)
	if err != nil {
		return nil, err
	}

	hold, err := txStore.GetHold(ctx, ledgerID, holdID)
	if err != nil {
		return nil, err
	}
	if hold.Voided {
		return nil, fmt.Errorf("%w: %s", storage.ErrHoldVoided, holdID)
	}
	if hold.Expired {
		return nil, fmt.Errorf("%w: %s", storage.ErrHoldExpired, holdID)
	}

	payload, err := json.Marshal(map[string]string{"hold_id": holdID})
	if err != nil {
		return nil, fmt.Errorf("marshaling payload: %w", err)
	}

	logEvent := storage.LogEventRecord{
		EventID: eventID, LedgerID: ledgerID, LedgerSeq: seq,
		SystemTime: now, ValidTime: now, Type: storage.EventTypeHoldVoided,
		Payload: payload, IdempotencyKey: idempotencyKey, BatchID: batchID, SchemaVersion: 1,
	}
	if idempotencyKey != "" && ikHash != nil {
		logEvent.IdempotencyHash = ikHash
	}
	if err := txStore.AppendLogEvent(ctx, logEvent); err != nil {
		return nil, err
	}

	if err := txStore.VoidHold(ctx, ledgerID, holdID); err != nil {
		return nil, err
	}

	if idempotencyKey != "" && ikHash != nil {
		if err := p.recordIdempotency(ctx, txStore, ledgerID, idempotencyKey, eventID, ikHash); err != nil {
			return nil, err
		}
	}

	return &SubmitResult{EventID: eventID}, nil
}
