package planner

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"

	"github.com/remade/ledger/internal/ir"
	"github.com/remade/ledger/internal/storage"
	"github.com/remade/ledger/pkg/accounts"
	"github.com/remade/ledger/pkg/assets"
)

// SubmitPost handles a PostOperation: validates, checks balances, writes log + projections atomically.
func (p *Planner) SubmitPost(ctx context.Context, ledgerID string, postings []PostingInput, reference string, metadata map[string]any, idempotencyKey string, validTime *time.Time, dryRun bool) (*SubmitResult, error) {
	// Validate postings.
	if len(postings) == 0 {
		return nil, fmt.Errorf("at least one posting is required")
	}
	for i, posting := range postings {
		if err := accounts.Validate(posting.Source); err != nil {
			return nil, fmt.Errorf("posting %d source: %w", i, err)
		}
		if err := accounts.Validate(posting.Destination); err != nil {
			return nil, fmt.Errorf("posting %d destination: %w", i, err)
		}
		if err := assets.Validate(posting.Asset); err != nil {
			return nil, fmt.Errorf("posting %d asset: %w", i, err)
		}
		if posting.Amount == nil || posting.Amount.Sign() <= 0 {
			return nil, fmt.Errorf("posting %d: amount must be positive", i)
		}
	}

	// Check idempotency (Redis first, then PG).
	var ikHash []byte
	if idempotencyKey != "" {
		ikHash = computeIdempotencyHash(ledgerID, postings, reference, metadata, validTime)
		if result, err := p.checkIdempotency(ctx, ledgerID, idempotencyKey, ikHash); err != nil {
			return nil, err
		} else if result != nil {
			return result, nil
		}
	}

	// Get ledger to check issuer accounts and sealed state.
	ledger, err := p.checkLedgerNotSealed(ctx, ledgerID)
	if err != nil {
		return nil, err
	}

	// Schema enforcement: validate postings against chart of accounts if schema is active.
	if mode := ir.SchemaEnforcementMode(ledger.Features["schema_enforcement"]); mode == ir.SchemaStrict || mode == ir.SchemaBestEffort {
		schemaVersion := ledger.Features["active_schema_version"]
		if err := p.validatePostingsAgainstSchema(ctx, ledgerID, schemaVersion, postings, mode); err != nil {
			return nil, err
		}
	}

	now := time.Now().UTC()
	vt := now
	if validTime != nil {
		vt = *validTime
	}

	// Check balances for non-issuer source accounts.
	// Compute net outputs per (account, asset).
	type acctAsset struct{ Account, Asset string }
	netOutputs := make(map[acctAsset]*big.Int)
	for _, posting := range postings {
		if posting.Amount.Sign() == 0 {
			continue
		}
		key := acctAsset{posting.Source, posting.Asset}
		if netOutputs[key] == nil {
			netOutputs[key] = new(big.Int)
		}
		netOutputs[key].Add(netOutputs[key], posting.Amount)

		// Credits reduce net output.
		destKey := acctAsset{posting.Destination, posting.Asset}
		if destKey.Account == posting.Source {
			// Self-posting: net zero, handled already.
		}
	}

	// Also account for credits to source accounts from other postings.
	netInputs := make(map[acctAsset]*big.Int)
	for _, posting := range postings {
		if posting.Amount.Sign() == 0 {
			continue
		}
		key := acctAsset{posting.Destination, posting.Asset}
		if netInputs[key] == nil {
			netInputs[key] = new(big.Int)
		}
		netInputs[key].Add(netInputs[key], posting.Amount)
	}

	// Execute the transaction body with deadlock retry.
	var result *SubmitResult
	err = withDeadlockRetry(ctx, 5, func() error {
		// Generate fresh IDs per attempt.
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

		seq, err := txStore.NextLedgerSeq(ctx, ledgerID)
		if err != nil {
			return fmt.Errorf("getting next seq: %w", err)
		}

		// Check balances for non-issuer source accounts (inside TX to prevent TOCTOU).
		for key, output := range netOutputs {
			if accounts.IsIssuer(key.Account, ledger.IssuerAccounts) {
				continue
			}
			bal, err := txStore.GetBalance(ctx, ledgerID, key.Account, key.Asset, nil, nil)
			if err != nil {
				return fmt.Errorf("getting balance for %s/%s: %w", key.Account, key.Asset, err)
			}
			currentBalance := new(big.Int).Sub(bal.Input, bal.Output)

			activeHolds, err := txStore.GetActiveHoldsTotal(ctx, ledgerID, key.Account, key.Asset)
			if err != nil {
				return fmt.Errorf("getting active holds for %s/%s: %w", key.Account, key.Asset, err)
			}
			currentBalance.Sub(currentBalance, activeHolds)

			if netIn, ok := netInputs[key]; ok {
				currentBalance.Add(currentBalance, netIn)
			}
			if new(big.Int).Sub(currentBalance, output).Sign() < 0 {
				return fmt.Errorf("%w: account %s, asset %s", storage.ErrInsufficientFunds, key.Account, key.Asset)
			}
		}

		// Dry-run returns after balance checks pass but before any writes.
		if dryRun {
			txStore.Rollback()
			result = &SubmitResult{EventID: "dry-run"}
			return nil
		}

		// Build posting records for the transaction projection.
		postingRecords := make([]map[string]any, len(postings))
		for i, p := range postings {
			postingRecords[i] = map[string]any{
				"source":      p.Source,
				"destination": p.Destination,
				"amount":      p.Amount.String(),
				"asset":       p.Asset,
			}
		}

		payload, err := json.Marshal(map[string]any{
			"transaction_id": txID,
			"postings":       postingRecords,
			"metadata":       metadata,
			"reference":      reference,
		})
		if err != nil {
			return fmt.Errorf("marshaling event payload: %w", err)
		}

		logEvent := storage.LogEventRecord{
			EventID:        eventID,
			LedgerID:       ledgerID,
			LedgerSeq:      seq,
			SystemTime:     now,
			ValidTime:      vt,
			Type:           storage.EventTypeTransactionPosted,
			Payload:        payload,
			IdempotencyKey: idempotencyKey,
			BatchID:        batchID,
			SchemaVersion:  1,
		}
		if idempotencyKey != "" {
			logEvent.IdempotencyHash = ikHash
		}

		if err := txStore.AppendLogEvent(ctx, logEvent); err != nil {
			if errors.Is(err, storage.ErrIdempotencyKeyConflict) {
				txStore.Rollback()
				ikRecord, err2 := p.store.GetIdempotencyKey(ctx, ledgerID, idempotencyKey)
				if err2 != nil || ikRecord == nil {
					return storage.ErrIdempotencyKeyConflict
				}
				result = &SubmitResult{EventID: ikRecord.EventID, IdempotentHit: true}
				return nil
			}
			return fmt.Errorf("appending log event: %w", err)
		}

		txRec := storage.TransactionRecord{
			LedgerID:      ledgerID,
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
				return err
			}
			return fmt.Errorf("inserting transaction: %w", err)
		}

		if len(metadata) > 0 {
			if err := txStore.InsertMetadataHistory(ctx, storage.MetadataHistoryRecord{
				LedgerID:   ledgerID,
				TargetType: storage.TargetTypeTransaction,
				TargetID:   txID,
				Revision:   0,
				Metadata:   metadata,
				EventID:    eventID,
				SystemTime: now,
			}); err != nil {
				return fmt.Errorf("inserting initial metadata history: %w", err)
			}
		}

		touchedAccounts := make(map[string]bool)
		for _, posting := range postings {
			if posting.Amount.Sign() == 0 {
				continue
			}
			if err := txStore.InsertVolumeDelta(ctx, storage.VolumeDeltaRecord{
				LedgerID:    ledgerID,
				Account:     posting.Source,
				Asset:       posting.Asset,
				EventID:     eventID,
				ValidTime:   vt,
				SystemTime:  now,
				InputDelta:  big.NewInt(0),
				OutputDelta: posting.Amount,
			}); err != nil {
				return fmt.Errorf("inserting source volume delta: %w", err)
			}

			if err := txStore.InsertVolumeDelta(ctx, storage.VolumeDeltaRecord{
				LedgerID:    ledgerID,
				Account:     posting.Destination,
				Asset:       posting.Asset,
				EventID:     eventID,
				ValidTime:   vt,
				SystemTime:  now,
				InputDelta:  posting.Amount,
				OutputDelta: big.NewInt(0),
			}); err != nil {
				return fmt.Errorf("inserting dest volume delta: %w", err)
			}

			touchedAccounts[posting.Source] = true
			touchedAccounts[posting.Destination] = true
		}

		for addr := range touchedAccounts {
			if err := txStore.UpsertAccount(ctx, storage.AccountRecord{
				LedgerID:   ledgerID,
				Address:    addr,
				FirstUsage: now,
				UpdatedAt:  now,
				Metadata:   map[string]any{},
			}); err != nil {
				return fmt.Errorf("upserting account %s: %w", addr, err)
			}
		}

		if idempotencyKey != "" {
			if err := p.recordIdempotency(ctx, txStore, ledgerID, idempotencyKey, eventID, ikHash); err != nil {
				return err
			}
		}

		if err := txStore.Commit(); err != nil {
			return fmt.Errorf("committing transaction: %w", err)
		}

		if idempotencyKey != "" {
			p.postCommitIdempotency(ctx, ledgerID, idempotencyKey, eventID, ikHash)
		}

		p.publishEvent(ctx, ledgerID, eventID, 1)

		p.logger.Debug("transaction posted",
			zap.String("event_id", eventID),
			zap.String("tx_id", txID),
			zap.String("ledger", ledgerID),
			zap.Int("postings", len(postings)),
		)

		result = &SubmitResult{
			EventID:     eventID,
			Transaction: &txRec,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// validatePostingsAgainstSchema checks postings against the active schema's chart of accounts.
func (p *Planner) validatePostingsAgainstSchema(ctx context.Context, ledgerID string, schemaVersion string, postings []PostingInput, mode ir.SchemaEnforcementMode) error {
	// Look up the active schema version from the ledger's features.
	if schemaVersion == "" {
		return nil // no active schema — no validation needed
	}

	schema, err := p.store.GetSchema(ctx, ledgerID, schemaVersion)
	if err != nil {
		// Schema not found — no validation needed.
		return nil
	}

	docBytes, ok := schema.Document.([]byte)
	if !ok {
		if mode == ir.SchemaStrict {
			return fmt.Errorf("schema document has unexpected type %T, expected []byte", schema.Document)
		}
		p.logger.Warn("schema document has unexpected type, skipping validation",
			zap.String("type", fmt.Sprintf("%T", schema.Document)),
		)
		return nil
	}

	coa, err := ir.ParseChartOfAccounts(docBytes)
	if err != nil {
		if mode == ir.SchemaStrict {
			return fmt.Errorf("invalid chart of accounts: %w", err)
		}
		p.logger.Warn("invalid chart of accounts, skipping validation", zap.Error(err))
		return nil
	}

	for i, posting := range postings {
		if err := coa.ValidatePosting(posting.Source, posting.Destination); err != nil {
			if mode == ir.SchemaStrict {
				return fmt.Errorf("posting %d: %w", i, err)
			}
			p.logger.Warn("posting failed schema validation (best_effort)",
				zap.Int("posting", i),
				zap.Error(err),
			)
		}
	}

	return nil
}

func computeIdempotencyHash(ledgerID string, postings []PostingInput, reference string, metadata map[string]any, validTime *time.Time) []byte {
	h := sha256.New()
	writeHashField(h, []byte(ledgerID))
	writeHashField(h, []byte(reference))
	for _, p := range postings {
		writeHashField(h, []byte(p.Source))
		writeHashField(h, []byte(p.Destination))
		writeHashField(h, []byte(p.Amount.String()))
		writeHashField(h, []byte(p.Asset))
	}
	if metadata != nil {
		keys := make([]string, 0, len(metadata))
		for k := range metadata {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			writeHashField(h, []byte(k))
			writeHashField(h, []byte(fmt.Sprint(metadata[k])))
		}
	}
	if validTime != nil {
		writeHashField(h, []byte(validTime.UTC().Format(time.RFC3339Nano)))
	}
	return h.Sum(nil)
}

// computeGenericIdempotencyHash hashes arbitrary fields for idempotency.
func computeGenericIdempotencyHash(fields ...string) []byte {
	h := sha256.New()
	for _, f := range fields {
		writeHashField(h, []byte(f))
	}
	return h.Sum(nil)
}

// checkIdempotency performs the 2-tier IK lookup (Redis then PG).
// Returns (result, hash, nil) if IK hit, (nil, hash, nil) if miss, or error.
func (p *Planner) checkIdempotency(ctx context.Context, ledgerID, idempotencyKey string, ikHash []byte) (*SubmitResult, error) {
	// Check Redis.
	eventID, storedHash, found, err := p.redis.GetIdempotencyKey(ctx, ledgerID, idempotencyKey)
	if err != nil {
		p.logger.Warn("redis IK lookup failed, falling back to PG", zap.Error(err))
	}
	if found {
		if string(storedHash) != string(ikHash) {
			return nil, storage.ErrInvalidIdempotencyInput
		}
		return &SubmitResult{EventID: eventID, IdempotentHit: true}, nil
	}

	// Check PG.
	ikRecord, err := p.store.GetIdempotencyKey(ctx, ledgerID, idempotencyKey)
	if err != nil {
		return nil, fmt.Errorf("checking idempotency key in PG: %w", err)
	}
	if ikRecord != nil {
		if string(ikRecord.IdempotencyHash) != string(ikHash) {
			return nil, storage.ErrInvalidIdempotencyInput
		}
		// Populate Redis for next time.
		p.redis.SetIdempotencyKey(ctx, ledgerID, idempotencyKey, ikRecord.EventID, ikRecord.IdempotencyHash)
		return &SubmitResult{EventID: ikRecord.EventID, IdempotentHit: true}, nil
	}

	return nil, nil
}

// recordIdempotency writes the IK record inside a transaction and populates Redis post-commit.
func (p *Planner) recordIdempotency(ctx context.Context, txStore storage.TxStore, ledgerID, idempotencyKey, eventID string, ikHash []byte) error {
	if err := txStore.InsertIdempotencyKey(ctx, storage.IdempotencyKeyRecord{
		LedgerID:        ledgerID,
		IdempotencyKey:  idempotencyKey,
		IdempotencyHash: ikHash,
		EventID:         eventID,
	}); err != nil && !errors.Is(err, storage.ErrIdempotencyKeyConflict) {
		return fmt.Errorf("inserting idempotency key: %w", err)
	}
	return nil
}

// postCommitIdempotency populates Redis cache after a successful commit.
func (p *Planner) postCommitIdempotency(ctx context.Context, ledgerID, idempotencyKey, eventID string, ikHash []byte) {
	p.redis.SetIdempotencyKey(ctx, ledgerID, idempotencyKey, eventID, ikHash)
}

func writeHashField(h io.Writer, data []byte) {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	h.Write(lenBuf[:])
	h.Write(data)
}

// withDeadlockRetry retries fn on deadlock errors with exponential backoff and jitter.
func withDeadlockRetry(ctx context.Context, maxRetries int, fn func() error) error {
	backoff := 10 * time.Millisecond
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if !errors.Is(err, storage.ErrDeadlock) || attempt >= maxRetries {
			return err
		}
		// Add jitter: backoff +/- 50% to prevent thundering herd.
		jitter := rand.N(backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff + jitter):
		}
		backoff *= 2
		if backoff > 5*time.Second {
			backoff = 5 * time.Second
		}
	}
}
