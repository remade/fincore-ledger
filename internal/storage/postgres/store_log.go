package postgres

import (
	"context"
	"fmt"

	"github.com/remade/ledger/internal/storage"
)

// --- Log ---

func (q *queries) AppendLogEvent(ctx context.Context, event storage.LogEventRecord) error {
	_, err := q.db.Exec(ctx,
		`INSERT INTO "_default".log_events
		 (event_id, ledger_id, ledger_seq, system_time, valid_time, type, payload,
		  idempotency_key, idempotency_hash, batch_id, schema_version)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		event.EventID, event.LedgerID, event.LedgerSeq, event.SystemTime,
		event.ValidTime, event.Type, event.Payload,
		nullIfEmpty(event.IdempotencyKey), event.IdempotencyHash,
		event.BatchID, event.SchemaVersion,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return storage.ErrIdempotencyKeyConflict
		}
		if isDeadlock(err) {
			return storage.ErrDeadlock
		}
		return fmt.Errorf("appending log event: %w", err)
	}
	return nil
}

func (q *queries) GetLogEvent(ctx context.Context, ledgerID, eventID string) (*storage.LogEventRecord, error) {
	var rec storage.LogEventRecord
	err := q.db.QueryRow(ctx,
		`SELECT event_id, ledger_id, ledger_seq, system_time, valid_time, type, payload,
		        COALESCE(idempotency_key, ''), idempotency_hash, batch_id, schema_version
		 FROM "_default".log_events
		 WHERE ledger_id = $1 AND event_id = $2`,
		ledgerID, eventID,
	).Scan(&rec.EventID, &rec.LedgerID, &rec.LedgerSeq, &rec.SystemTime,
		&rec.ValidTime, &rec.Type, &rec.Payload,
		&rec.IdempotencyKey, &rec.IdempotencyHash, &rec.BatchID, &rec.SchemaVersion)
	if err != nil {
		if isNoRows(err) {
			return nil, fmt.Errorf("%w: log event %q", storage.ErrNotFound, eventID)
		}
		return nil, fmt.Errorf("getting log event: %w", err)
	}
	return &rec, nil
}

func (q *queries) ListLogEvents(ctx context.Context, ledgerID string, params storage.ListParams) ([]storage.LogEventRecord, string, error) {
	if params.PageSize <= 0 {
		params.PageSize = 100
	}
	if params.PageSize > maxPageSize {
		params.PageSize = maxPageSize
	}
	limit := params.PageSize
	rows, err := q.db.Query(ctx,
		`SELECT event_id, ledger_id, ledger_seq, system_time, valid_time, type, payload,
		        COALESCE(idempotency_key, ''), idempotency_hash, batch_id, schema_version
		 FROM "_default".log_events
		 WHERE ledger_id = $1 AND ($2 = '' OR event_id > $2)
		 ORDER BY ledger_seq LIMIT $3`,
		ledgerID, params.PageToken, limit+1,
	)
	if err != nil {
		return nil, "", fmt.Errorf("listing log events: %w", err)
	}
	defer rows.Close()

	var result []storage.LogEventRecord
	for rows.Next() {
		var rec storage.LogEventRecord
		if err := rows.Scan(&rec.EventID, &rec.LedgerID, &rec.LedgerSeq, &rec.SystemTime,
			&rec.ValidTime, &rec.Type, &rec.Payload,
			&rec.IdempotencyKey, &rec.IdempotencyHash, &rec.BatchID, &rec.SchemaVersion); err != nil {
			return nil, "", fmt.Errorf("scanning log event: %w", err)
		}
		result = append(result, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterating log events: %w", err)
	}

	var nextToken string
	if len(result) > limit {
		nextToken = result[limit].EventID
		result = result[:limit]
	}
	return result, nextToken, nil
}
