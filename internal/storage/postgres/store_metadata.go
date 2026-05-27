package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/remade/ledger/internal/storage"
)

// --- Metadata history ---

func (q *queries) InsertMetadataHistory(ctx context.Context, record storage.MetadataHistoryRecord) error {
	metaJSON, err := json.Marshal(record.Metadata)
	if err != nil {
		return fmt.Errorf("marshaling metadata: %w", err)
	}
	_, err = q.db.Exec(ctx,
		`INSERT INTO "_default".metadata_history
		 (ledger_id, target_type, target_id, revision, metadata, event_id, system_time)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		record.LedgerID, record.TargetType, record.TargetID, record.Revision,
		metaJSON, record.EventID, record.SystemTime,
	)
	if err != nil {
		return fmt.Errorf("inserting metadata history: %w", err)
	}
	return nil
}

// --- Schemas ---

func (q *queries) InsertSchema(ctx context.Context, schema storage.SchemaRecord) error {
	docJSON, err := json.Marshal(schema.Document)
	if err != nil {
		return fmt.Errorf("marshaling document: %w", err)
	}
	_, err = q.db.Exec(ctx,
		`INSERT INTO "_default".schemas (ledger_id, version, document, event_id)
		 VALUES ($1, $2, $3, $4)`,
		schema.LedgerID, schema.Version, docJSON, schema.EventID,
	)
	if err != nil {
		return fmt.Errorf("inserting schema %s: %w", schema.Version, err)
	}
	return nil
}

func (q *queries) GetSchema(ctx context.Context, ledgerID, version string) (*storage.SchemaRecord, error) {
	var rec storage.SchemaRecord
	var docBytes []byte
	err := q.db.QueryRow(ctx,
		`SELECT ledger_id, version, document, inserted_at, event_id
		 FROM "_default".schemas
		 WHERE ledger_id = $1 AND version = $2`,
		ledgerID, version,
	).Scan(&rec.LedgerID, &rec.Version, &docBytes, &rec.InsertedAt, &rec.EventID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: schema %q", storage.ErrNotFound, version)
		}
		return nil, err
	}
	if err := json.Unmarshal(docBytes, &rec.Document); err != nil {
		return nil, fmt.Errorf("unmarshaling document: %w", err)
	}
	return &rec, nil
}
