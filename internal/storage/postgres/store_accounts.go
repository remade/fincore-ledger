package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/remade/ledger/internal/storage"
)

// --- Accounts ---

func (q *queries) UpsertAccount(ctx context.Context, account storage.AccountRecord) error {
	metaJSON, err := json.Marshal(account.Metadata)
	if err != nil {
		return fmt.Errorf("marshaling metadata: %w", err)
	}
	_, err = q.db.Exec(ctx,
		`INSERT INTO "_default".accounts (ledger_id, address, first_usage, updated_at, metadata)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (ledger_id, address) DO UPDATE SET updated_at = $4, metadata = "_default".accounts.metadata || $5`,
		account.LedgerID, account.Address, account.FirstUsage, account.UpdatedAt, metaJSON,
	)
	if err != nil {
		return fmt.Errorf("upserting account %s: %w", account.Address, err)
	}
	return nil
}

func (q *queries) GetAccount(ctx context.Context, ledgerID, address string) (*storage.AccountRecord, error) {
	var rec storage.AccountRecord
	var metaBytes []byte
	err := q.db.QueryRow(ctx,
		`SELECT ledger_id, address, first_usage, updated_at, metadata
		 FROM "_default".accounts
		 WHERE ledger_id = $1 AND address = $2`,
		ledgerID, address,
	).Scan(&rec.LedgerID, &rec.Address, &rec.FirstUsage, &rec.UpdatedAt, &metaBytes)
	if err != nil {
		if isNoRows(err) {
			return nil, fmt.Errorf("%w: account %q", storage.ErrNotFound, address)
		}
		return nil, err
	}
	if err := json.Unmarshal(metaBytes, &rec.Metadata); err != nil {
		return nil, fmt.Errorf("unmarshaling metadata: %w", err)
	}
	return &rec, nil
}

func (q *queries) ListAccounts(ctx context.Context, ledgerID string, params storage.ListAccountsParams) ([]storage.AccountRecord, string, error) {
	if params.PageSize <= 0 {
		params.PageSize = 100
	}
	if params.PageSize > maxPageSize {
		params.PageSize = maxPageSize
	}
	limit := params.PageSize
	rows, err := q.db.Query(ctx,
		`SELECT ledger_id, address, first_usage, updated_at, metadata
		 FROM "_default".accounts
		 WHERE ledger_id = $1 AND ($2 = '' OR address > $2)
		 ORDER BY address LIMIT $3`,
		ledgerID, params.PageToken, limit+1,
	)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var result []storage.AccountRecord
	for rows.Next() {
		var rec storage.AccountRecord
		var metaBytes []byte
		if err := rows.Scan(&rec.LedgerID, &rec.Address, &rec.FirstUsage, &rec.UpdatedAt, &metaBytes); err != nil {
			return nil, "", err
		}
		if err := json.Unmarshal(metaBytes, &rec.Metadata); err != nil {
			return nil, "", fmt.Errorf("unmarshaling metadata: %w", err)
		}
		result = append(result, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterating accounts: %w", err)
	}

	var nextToken string
	if len(result) > limit {
		nextToken = result[limit].Address
		result = result[:limit]
	}
	return result, nextToken, nil
}
