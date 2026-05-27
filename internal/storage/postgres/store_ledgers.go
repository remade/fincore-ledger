package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/remade/ledger/internal/storage"
)

// --- Ledger catalog ---

func (q *queries) CreateLedger(ctx context.Context, params storage.CreateLedgerParams) (*storage.LedgerRecord, error) {
	bucketID := params.BucketID
	if bucketID == "" {
		bucketID = "_default"
	}

	// Ensure bucket exists.
	_, err := q.db.Exec(ctx,
		`INSERT INTO _system.buckets (id) VALUES ($1) ON CONFLICT DO NOTHING`,
		bucketID,
	)
	if err != nil {
		return nil, fmt.Errorf("ensuring bucket: %w", err)
	}

	metaJSON, err := json.Marshal(params.Metadata)
	if err != nil {
		return nil, fmt.Errorf("marshaling metadata: %w", err)
	}

	var rec storage.LedgerRecord
	var metaBytes []byte
	var featBytes []byte
	err = q.db.QueryRow(ctx,
		`INSERT INTO _system.ledgers (id, bucket_id, metadata, state)
		 VALUES ($1, $2, $3, 'in-use')
		 RETURNING id, bucket_id, state, features, metadata, created_at, sealed_at, issuer_accounts`,
		params.ID, bucketID, metaJSON,
	).Scan(&rec.ID, &rec.BucketID, &rec.State, &featBytes, &metaBytes,
		&rec.CreatedAt, &rec.SealedAt, &rec.IssuerAccounts)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, fmt.Errorf("%w: ledger %q", storage.ErrAlreadyExists, params.ID)
		}
		return nil, fmt.Errorf("inserting ledger: %w", err)
	}

	if err := json.Unmarshal(metaBytes, &rec.Metadata); err != nil {
		return nil, fmt.Errorf("unmarshaling metadata: %w", err)
	}
	if err := json.Unmarshal(featBytes, &rec.Features); err != nil {
		return nil, fmt.Errorf("unmarshaling features: %w", err)
	}
	return &rec, nil
}

func (q *queries) GetLedger(ctx context.Context, id string) (*storage.LedgerRecord, error) {
	var rec storage.LedgerRecord
	var metaBytes, featBytes []byte
	err := q.db.QueryRow(ctx,
		`SELECT id, bucket_id, state, features, metadata, created_at, sealed_at, issuer_accounts
		 FROM _system.ledgers WHERE id = $1`, id,
	).Scan(&rec.ID, &rec.BucketID, &rec.State, &featBytes, &metaBytes,
		&rec.CreatedAt, &rec.SealedAt, &rec.IssuerAccounts)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: ledger %q", storage.ErrNotFound, id)
		}
		return nil, fmt.Errorf("getting ledger: %w", err)
	}
	if err := json.Unmarshal(metaBytes, &rec.Metadata); err != nil {
		return nil, fmt.Errorf("unmarshaling metadata: %w", err)
	}
	if err := json.Unmarshal(featBytes, &rec.Features); err != nil {
		return nil, fmt.Errorf("unmarshaling features: %w", err)
	}
	return &rec, nil
}

func (q *queries) ListLedgers(ctx context.Context, params storage.ListParams) ([]storage.LedgerRecord, string, error) {
	if params.PageSize <= 0 {
		params.PageSize = 100
	}
	if params.PageSize > maxPageSize {
		params.PageSize = maxPageSize
	}
	limit := params.PageSize
	rows, err := q.db.Query(ctx,
		`SELECT id, bucket_id, state, features, metadata, created_at, sealed_at, issuer_accounts
		 FROM _system.ledgers
		 WHERE ($1 = '' OR id > $1)
		 ORDER BY id LIMIT $2`,
		params.PageToken, limit+1,
	)
	if err != nil {
		return nil, "", fmt.Errorf("listing ledgers: %w", err)
	}
	defer rows.Close()

	var result []storage.LedgerRecord
	for rows.Next() {
		var rec storage.LedgerRecord
		var metaBytes, featBytes []byte
		if err := rows.Scan(&rec.ID, &rec.BucketID, &rec.State, &featBytes, &metaBytes,
			&rec.CreatedAt, &rec.SealedAt, &rec.IssuerAccounts); err != nil {
			return nil, "", fmt.Errorf("scanning ledger: %w", err)
		}
		if err := json.Unmarshal(metaBytes, &rec.Metadata); err != nil {
			return nil, "", fmt.Errorf("unmarshaling metadata: %w", err)
		}
		if err := json.Unmarshal(featBytes, &rec.Features); err != nil {
			return nil, "", fmt.Errorf("unmarshaling features: %w", err)
		}
		result = append(result, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterating ledgers: %w", err)
	}

	var nextToken string
	if len(result) > limit {
		nextToken = result[limit].ID
		result = result[:limit]
	}
	return result, nextToken, nil
}

func (q *queries) SealLedger(ctx context.Context, id string) (*storage.LedgerRecord, error) {
	var rec storage.LedgerRecord
	var metaBytes, featBytes []byte
	err := q.db.QueryRow(ctx,
		`UPDATE _system.ledgers SET state = 'sealed', sealed_at = now()
		 WHERE id = $1 AND state = 'in-use'
		 RETURNING id, bucket_id, state, features, metadata, created_at, sealed_at, issuer_accounts`, id,
	).Scan(&rec.ID, &rec.BucketID, &rec.State, &featBytes, &metaBytes,
		&rec.CreatedAt, &rec.SealedAt, &rec.IssuerAccounts)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: ledger %q (may be sealed or not exist)", storage.ErrNotFound, id)
		}
		return nil, fmt.Errorf("sealing ledger: %w", err)
	}
	if err := json.Unmarshal(metaBytes, &rec.Metadata); err != nil {
		return nil, fmt.Errorf("unmarshaling metadata: %w", err)
	}
	if err := json.Unmarshal(featBytes, &rec.Features); err != nil {
		return nil, fmt.Errorf("unmarshaling features: %w", err)
	}
	return &rec, nil
}
