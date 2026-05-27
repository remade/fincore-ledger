package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/remade/ledger/internal/storage"
)

// --- Transactions ---

func (q *queries) InsertTransaction(ctx context.Context, tx storage.TransactionRecord) error {
	postingsJSON, err := json.Marshal(tx.Postings)
	if err != nil {
		return fmt.Errorf("marshaling postings: %w", err)
	}
	metaJSON, err := json.Marshal(tx.Metadata)
	if err != nil {
		return fmt.Errorf("marshaling metadata: %w", err)
	}
	_, err = q.db.Exec(ctx,
		`INSERT INTO "_default".transactions
		 (ledger_id, transaction_id, event_id, valid_time, system_time, reference, postings, metadata)
		 VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7, $8)`,
		tx.LedgerID, tx.TransactionID, tx.EventID, tx.ValidTime,
		tx.SystemTime, tx.Reference, postingsJSON, metaJSON,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return storage.ErrTransactionReferenceConflict
		}
		return err
	}
	return nil
}

func (q *queries) GetTransaction(ctx context.Context, ledgerID, txID string) (*storage.TransactionRecord, error) {
	var rec storage.TransactionRecord
	var postingsBytes, metaBytes []byte
	var ref *string
	err := q.db.QueryRow(ctx,
		`SELECT ledger_id, transaction_id, event_id, valid_time, system_time,
		        reference, postings, metadata
		 FROM "_default".transactions
		 WHERE ledger_id = $1 AND transaction_id = $2`,
		ledgerID, txID,
	).Scan(&rec.LedgerID, &rec.TransactionID, &rec.EventID, &rec.ValidTime,
		&rec.SystemTime, &ref, &postingsBytes, &metaBytes)
	if err != nil {
		if isNoRows(err) {
			return nil, fmt.Errorf("%w: transaction %q", storage.ErrNotFound, txID)
		}
		return nil, err
	}
	if ref != nil {
		rec.Reference = *ref
	}
	if err := json.Unmarshal(postingsBytes, &rec.Postings); err != nil {
		return nil, fmt.Errorf("unmarshaling postings: %w", err)
	}
	if err := json.Unmarshal(metaBytes, &rec.Metadata); err != nil {
		return nil, fmt.Errorf("unmarshaling metadata: %w", err)
	}
	return &rec, nil
}

func (q *queries) ListTransactions(ctx context.Context, ledgerID string, params storage.ListTransactionsParams) ([]storage.TransactionRecord, string, error) {
	if params.PageSize <= 0 {
		params.PageSize = 100
	}
	if params.PageSize > maxPageSize {
		params.PageSize = maxPageSize
	}
	limit := params.PageSize
	rows, err := q.db.Query(ctx,
		`SELECT ledger_id, transaction_id, event_id, valid_time, system_time,
		        reference, postings, metadata
		 FROM "_default".transactions
		 WHERE ledger_id = $1 AND ($2 = '' OR transaction_id < $2)
		 ORDER BY system_time DESC, transaction_id DESC LIMIT $3`,
		ledgerID, params.PageToken, limit+1,
	)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var result []storage.TransactionRecord
	for rows.Next() {
		var rec storage.TransactionRecord
		var postingsBytes, metaBytes []byte
		var ref *string
		if err := rows.Scan(&rec.LedgerID, &rec.TransactionID, &rec.EventID, &rec.ValidTime,
			&rec.SystemTime, &ref, &postingsBytes, &metaBytes); err != nil {
			return nil, "", err
		}
		if ref != nil {
			rec.Reference = *ref
		}
		if err := json.Unmarshal(postingsBytes, &rec.Postings); err != nil {
			return nil, "", fmt.Errorf("unmarshaling postings: %w", err)
		}
		if err := json.Unmarshal(metaBytes, &rec.Metadata); err != nil {
			return nil, "", fmt.Errorf("unmarshaling metadata: %w", err)
		}
		result = append(result, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterating transactions: %w", err)
	}

	var nextToken string
	if len(result) > limit {
		nextToken = result[limit].TransactionID
		result = result[:limit]
	}
	return result, nextToken, nil
}

func (q *queries) UpdateTransactionMetadata(ctx context.Context, ledgerID, txID string, metadata map[string]any) error {
	metaJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("marshaling metadata: %w", err)
	}
	result, err := q.db.Exec(ctx,
		`UPDATE "_default".transactions SET metadata = metadata || $3
		 WHERE ledger_id = $1 AND transaction_id = $2`,
		ledgerID, txID, metaJSON,
	)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: transaction %s", storage.ErrNotFound, txID)
	}
	return nil
}

func (q *queries) DeleteTransactionMetadataKey(ctx context.Context, ledgerID, txID, key string) error {
	result, err := q.db.Exec(ctx,
		`UPDATE "_default".transactions SET metadata = metadata - $3
		 WHERE ledger_id = $1 AND transaction_id = $2`,
		ledgerID, txID, key,
	)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: transaction %s", storage.ErrNotFound, txID)
	}
	return nil
}
