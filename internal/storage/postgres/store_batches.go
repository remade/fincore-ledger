package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/remade/ledger/internal/storage"
)

// --- Batches ---

func (q *queries) CreateBatch(ctx context.Context, batch storage.BatchRecord) error {
	_, err := q.db.Exec(ctx,
		`INSERT INTO "_default".log_batches
		 (batch_id, ledger_id, opened_at, event_count, prev_batch_id)
		 VALUES ($1, $2, $3, $4, NULLIF($5, ''))`,
		batch.BatchID, batch.LedgerID, batch.OpenedAt, batch.EventCount, batch.PrevBatchID,
	)
	if err != nil {
		return fmt.Errorf("creating batch %s: %w", batch.BatchID, err)
	}
	return nil
}

func (q *queries) CloseBatch(ctx context.Context, batchID string, merkleRoot []byte, eventCount int) error {
	_, err := q.db.Exec(ctx,
		`UPDATE "_default".log_batches
		 SET closed_at = now(), merkle_root = $2, event_count = $3
		 WHERE batch_id = $1`,
		batchID, merkleRoot, eventCount,
	)
	if err != nil {
		return fmt.Errorf("closing batch %s: %w", batchID, err)
	}
	return nil
}

func (q *queries) GetBatch(ctx context.Context, batchID string) (*storage.BatchRecord, error) {
	var rec storage.BatchRecord
	var prevBatchID *string
	err := q.db.QueryRow(ctx,
		`SELECT batch_id, ledger_id, opened_at, closed_at, event_count, merkle_root,
		        prev_batch_id, COALESCE(attestation_uri, '')
		 FROM "_default".log_batches WHERE batch_id = $1`, batchID,
	).Scan(&rec.BatchID, &rec.LedgerID, &rec.OpenedAt, &rec.ClosedAt,
		&rec.EventCount, &rec.MerkleRoot, &prevBatchID, &rec.AttestationURI)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: batch %q", storage.ErrNotFound, batchID)
		}
		return nil, fmt.Errorf("getting batch %s: %w", batchID, err)
	}
	if prevBatchID != nil {
		rec.PrevBatchID = *prevBatchID
	}
	return &rec, nil
}

func (q *queries) ListOpenBatches(ctx context.Context, olderThan time.Duration) ([]storage.BatchRecord, error) {
	rows, err := q.db.Query(ctx,
		`SELECT batch_id, ledger_id, opened_at, closed_at, event_count, merkle_root,
		        prev_batch_id, COALESCE(attestation_uri, '')
		 FROM "_default".log_batches
		 WHERE closed_at IS NULL AND opened_at < now() - $1::interval
		 ORDER BY opened_at LIMIT 100`,
		olderThan.String(),
	)
	if err != nil {
		return nil, fmt.Errorf("listing open batches: %w", err)
	}
	defer rows.Close()

	var result []storage.BatchRecord
	for rows.Next() {
		var rec storage.BatchRecord
		var prevBatchID *string
		if err := rows.Scan(&rec.BatchID, &rec.LedgerID, &rec.OpenedAt, &rec.ClosedAt,
			&rec.EventCount, &rec.MerkleRoot, &prevBatchID, &rec.AttestationURI); err != nil {
			return nil, err
		}
		if prevBatchID != nil {
			rec.PrevBatchID = *prevBatchID
		}
		result = append(result, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating open batches: %w", err)
	}
	return result, nil
}

func (q *queries) ListBatchEvents(ctx context.Context, batchID string) ([]storage.LogEventRecord, error) {
	rows, err := q.db.Query(ctx,
		`SELECT event_id, ledger_id, ledger_seq, system_time, valid_time, type, payload,
		        COALESCE(idempotency_key, ''), idempotency_hash, batch_id, schema_version
		 FROM "_default".log_events
		 WHERE batch_id = $1
		 ORDER BY ledger_seq`,
		batchID,
	)
	if err != nil {
		return nil, fmt.Errorf("listing batch events for %s: %w", batchID, err)
	}
	defer rows.Close()

	var result []storage.LogEventRecord
	for rows.Next() {
		var rec storage.LogEventRecord
		if err := rows.Scan(&rec.EventID, &rec.LedgerID, &rec.LedgerSeq, &rec.SystemTime,
			&rec.ValidTime, &rec.Type, &rec.Payload,
			&rec.IdempotencyKey, &rec.IdempotencyHash, &rec.BatchID, &rec.SchemaVersion); err != nil {
			return nil, fmt.Errorf("scanning batch event: %w", err)
		}
		result = append(result, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating batch events: %w", err)
	}
	return result, nil
}
