package planner

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/remade/ledger/internal/ir"
	"github.com/remade/ledger/internal/storage"
)

// SubmitPost handles a PostOperation: validates, checks balances, writes log + projections atomically.
// The in-transaction core is shared with the batch path via doPostInTx; this method owns the
// transaction lifecycle, deadlock retry, two-tier idempotency, dry-run rollback, and publish.
func (p *Planner) SubmitPost(ctx context.Context, ledgerID string, postings []PostingInput, reference string, metadata map[string]any, idempotencyKey string, validTime *time.Time, dryRun bool) (*SubmitResult, error) {
	// Validate up front so computeIdempotencyHash never sees a nil amount.
	if err := validatePostings(postings); err != nil {
		return nil, err
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

	now := time.Now().UTC()
	vt := now
	if validTime != nil {
		vt = *validTime
	}

	var result *SubmitResult
	err = withDeadlockRetry(ctx, 5, func() error {
		txStore, err := p.store.BeginTx(ctx)
		if err != nil {
			return fmt.Errorf("beginning tx: %w", err)
		}
		defer txStore.Rollback()

		r, err := p.doPostInTx(ctx, txStore, ledger, postings, reference, metadata, idempotencyKey, ikHash, vt, now, dryRun)
		if err != nil {
			// A concurrent writer committed the same idempotency key: return the
			// existing event as an idempotent hit instead of failing.
			if errors.Is(err, storage.ErrIdempotencyKeyConflict) {
				txStore.Rollback()
				ikRecord, err2 := p.store.GetIdempotencyKey(ctx, ledgerID, idempotencyKey)
				if err2 != nil || ikRecord == nil {
					return storage.ErrIdempotencyKeyConflict
				}
				result = &SubmitResult{EventID: ikRecord.EventID, IdempotentHit: true}
				return nil
			}
			return err
		}

		if dryRun {
			txStore.Rollback()
			result = r
			return nil
		}

		if err := txStore.Commit(); err != nil {
			return fmt.Errorf("committing transaction: %w", err)
		}

		if idempotencyKey != "" {
			p.postCommitIdempotency(ctx, ledgerID, idempotencyKey, r.EventID, ikHash)
		}
		p.publishEvent(ctx, ledgerID, r.EventID, 1)
		p.logger.Debug("transaction posted",
			zap.String("event_id", r.EventID),
			zap.String("ledger", ledgerID),
			zap.Int("postings", len(postings)),
		)
		result = r
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
