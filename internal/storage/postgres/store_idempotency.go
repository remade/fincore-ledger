package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/remade/ledger/internal/storage"
)

// --- Idempotency ---

func (q *queries) GetIdempotencyKey(ctx context.Context, ledgerID, key string) (*storage.IdempotencyKeyRecord, error) {
	var rec storage.IdempotencyKeyRecord
	err := q.db.QueryRow(ctx,
		`SELECT ledger_id, idempotency_key, idempotency_hash, event_id, created_at
		 FROM "_default".idempotency_keys
		 WHERE ledger_id = $1 AND idempotency_key = $2`,
		ledgerID, key,
	).Scan(&rec.LedgerID, &rec.IdempotencyKey, &rec.IdempotencyHash, &rec.EventID, &rec.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // not found is not an error
		}
		return nil, err
	}
	return &rec, nil
}

func (q *queries) InsertIdempotencyKey(ctx context.Context, record storage.IdempotencyKeyRecord) error {
	_, err := q.db.Exec(ctx,
		`INSERT INTO "_default".idempotency_keys
		 (ledger_id, idempotency_key, idempotency_hash, event_id)
		 VALUES ($1, $2, $3, $4)`,
		record.LedgerID, record.IdempotencyKey, record.IdempotencyHash, record.EventID,
	)
	if isUniqueViolation(err) {
		return storage.ErrIdempotencyKeyConflict
	}
	return err
}
