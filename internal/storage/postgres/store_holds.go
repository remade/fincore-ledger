package postgres

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/jackc/pgx/v5"

	"github.com/remade/ledger/internal/storage"
)

// --- Holds ---

func (q *queries) InsertHold(ctx context.Context, hold storage.HoldRecord) error {
	_, err := q.db.Exec(ctx,
		`INSERT INTO "_default".holds
		 (ledger_id, hold_id, source, destination_hint, asset, authorized_amount,
		  captured_amount, voided, expired, expires_at, authorized_event_id, valid_time, system_time)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		hold.LedgerID, hold.HoldID, hold.Source, nullIfEmpty(hold.DestinationHint),
		hold.Asset, hold.AuthorizedAmount.String(),
		hold.CapturedAmount.String(), hold.Voided, hold.Expired,
		hold.ExpiresAt, hold.AuthorizedEventID, hold.ValidTime, hold.SystemTime,
	)
	return err
}

func (q *queries) GetHold(ctx context.Context, ledgerID, holdID string) (*storage.HoldRecord, error) {
	var rec storage.HoldRecord
	var authAmt, capAmt string
	var destHint *string
	err := q.db.QueryRow(ctx,
		`SELECT ledger_id, hold_id, source, destination_hint, asset, authorized_amount,
		        captured_amount, voided, expired, expires_at, authorized_event_id, valid_time, system_time
		 FROM "_default".holds WHERE ledger_id = $1 AND hold_id = $2`,
		ledgerID, holdID,
	).Scan(&rec.LedgerID, &rec.HoldID, &rec.Source, &destHint,
		&rec.Asset, &authAmt, &capAmt, &rec.Voided, &rec.Expired,
		&rec.ExpiresAt, &rec.AuthorizedEventID, &rec.ValidTime, &rec.SystemTime)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: hold %q", storage.ErrNotFound, holdID)
		}
		return nil, err
	}
	if destHint != nil {
		rec.DestinationHint = *destHint
	}
	var parseErr error
	rec.AuthorizedAmount, parseErr = parseBigInt(authAmt, "authorized_amount")
	if parseErr != nil {
		return nil, parseErr
	}
	rec.CapturedAmount, parseErr = parseBigInt(capAmt, "captured_amount")
	if parseErr != nil {
		return nil, parseErr
	}
	return &rec, nil
}

// GetHold on TxStore adds FOR UPDATE to lock the hold row during transactions.
func (t *TxStore) GetHold(ctx context.Context, ledgerID, holdID string) (*storage.HoldRecord, error) {
	var rec storage.HoldRecord
	var authAmt, capAmt string
	var destHint *string
	err := t.tx.QueryRow(ctx,
		`SELECT ledger_id, hold_id, source, destination_hint, asset, authorized_amount,
		        captured_amount, voided, expired, expires_at, authorized_event_id, valid_time, system_time
		 FROM "_default".holds WHERE ledger_id = $1 AND hold_id = $2
		 FOR UPDATE`,
		ledgerID, holdID,
	).Scan(&rec.LedgerID, &rec.HoldID, &rec.Source, &destHint,
		&rec.Asset, &authAmt, &capAmt, &rec.Voided, &rec.Expired,
		&rec.ExpiresAt, &rec.AuthorizedEventID, &rec.ValidTime, &rec.SystemTime)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: hold %q", storage.ErrNotFound, holdID)
		}
		return nil, err
	}
	if destHint != nil {
		rec.DestinationHint = *destHint
	}
	var parseErr error
	rec.AuthorizedAmount, parseErr = parseBigInt(authAmt, "authorized_amount")
	if parseErr != nil {
		return nil, parseErr
	}
	rec.CapturedAmount, parseErr = parseBigInt(capAmt, "captured_amount")
	if parseErr != nil {
		return nil, parseErr
	}
	return &rec, nil
}

func (q *queries) ListHolds(ctx context.Context, ledgerID string, params storage.ListHoldsParams) ([]storage.HoldRecord, string, error) {
	if params.PageSize <= 0 {
		params.PageSize = 100
	}
	if params.PageSize > maxPageSize {
		params.PageSize = maxPageSize
	}
	limit := params.PageSize
	rows, err := q.db.Query(ctx,
		`SELECT ledger_id, hold_id, source, destination_hint, asset, authorized_amount,
		        captured_amount, voided, expired, expires_at, authorized_event_id, valid_time, system_time
		 FROM "_default".holds
		 WHERE ledger_id = $1 AND ($2 = '' OR hold_id > $2) AND ($4 = '' OR source = $4)
		 ORDER BY hold_id LIMIT $3`,
		ledgerID, params.PageToken, limit+1, params.Account,
	)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var result []storage.HoldRecord
	for rows.Next() {
		var rec storage.HoldRecord
		var authAmt, capAmt string
		var destHint *string
		if err := rows.Scan(&rec.LedgerID, &rec.HoldID, &rec.Source, &destHint,
			&rec.Asset, &authAmt, &capAmt, &rec.Voided, &rec.Expired,
			&rec.ExpiresAt, &rec.AuthorizedEventID, &rec.ValidTime, &rec.SystemTime); err != nil {
			return nil, "", err
		}
		if destHint != nil {
			rec.DestinationHint = *destHint
		}
		rec.AuthorizedAmount, err = parseBigInt(authAmt, "authorized_amount")
		if err != nil {
			return nil, "", err
		}
		rec.CapturedAmount, err = parseBigInt(capAmt, "captured_amount")
		if err != nil {
			return nil, "", err
		}
		result = append(result, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterating holds: %w", err)
	}

	var nextToken string
	if len(result) > limit {
		nextToken = result[limit].HoldID
		result = result[:limit]
	}
	return result, nextToken, nil
}

func (q *queries) UpdateHoldCaptured(ctx context.Context, ledgerID, holdID string, capturedDelta *big.Int) error {
	result, err := q.db.Exec(ctx,
		`UPDATE "_default".holds SET captured_amount = captured_amount + $3
		 WHERE ledger_id = $1 AND hold_id = $2 AND NOT voided AND NOT expired`,
		ledgerID, holdID, capturedDelta.String(),
	)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: hold %s not updatable (voided or expired)", storage.ErrNotFound, holdID)
	}
	return nil
}

func (q *queries) VoidHold(ctx context.Context, ledgerID, holdID string) error {
	result, err := q.db.Exec(ctx,
		`UPDATE "_default".holds SET voided = true WHERE ledger_id = $1 AND hold_id = $2 AND NOT voided AND NOT expired`,
		ledgerID, holdID,
	)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: hold %s not found, already voided, or expired", storage.ErrNotFound, holdID)
	}
	return nil
}

func (q *queries) ExpireHold(ctx context.Context, ledgerID, holdID string) error {
	result, err := q.db.Exec(ctx,
		`UPDATE "_default".holds SET expired = true
		 WHERE ledger_id = $1 AND hold_id = $2 AND NOT voided AND NOT expired`,
		ledgerID, holdID,
	)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: hold %s not found or already voided/expired", storage.ErrNotFound, holdID)
	}
	return nil
}

func (q *queries) ListExpiredHolds(ctx context.Context) ([]storage.HoldRecord, error) {
	rows, err := q.db.Query(ctx,
		`SELECT ledger_id, hold_id, source, destination_hint, asset, authorized_amount,
		        captured_amount, voided, expired, expires_at, authorized_event_id, valid_time, system_time
		 FROM "_default".holds
		 WHERE expires_at < now() AND NOT voided AND NOT expired AND captured_amount < authorized_amount
		 ORDER BY expires_at ASC
		 LIMIT 1000`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []storage.HoldRecord
	for rows.Next() {
		var rec storage.HoldRecord
		var authAmt, capAmt string
		var destHint *string
		if err := rows.Scan(&rec.LedgerID, &rec.HoldID, &rec.Source, &destHint,
			&rec.Asset, &authAmt, &capAmt, &rec.Voided, &rec.Expired,
			&rec.ExpiresAt, &rec.AuthorizedEventID, &rec.ValidTime, &rec.SystemTime); err != nil {
			return nil, err
		}
		if destHint != nil {
			rec.DestinationHint = *destHint
		}
		var parseErr error
		rec.AuthorizedAmount, parseErr = parseBigInt(authAmt, "authorized_amount")
		if parseErr != nil {
			return nil, parseErr
		}
		rec.CapturedAmount, parseErr = parseBigInt(capAmt, "captured_amount")
		if parseErr != nil {
			return nil, parseErr
		}
		result = append(result, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating expired holds: %w", err)
	}
	return result, nil
}

func (q *queries) GetActiveHoldsTotal(ctx context.Context, ledgerID, account, asset string) (*big.Int, error) {
	var totalStr string
	err := q.db.QueryRow(ctx,
		`SELECT COALESCE(SUM(authorized_amount - captured_amount), 0)
		 FROM "_default".holds
		 WHERE ledger_id = $1 AND source = $2 AND asset = $3
		   AND NOT voided AND NOT expired`,
		ledgerID, account, asset,
	).Scan(&totalStr)
	if err != nil {
		return nil, err
	}
	total, err := parseBigInt(totalStr, "active_holds_total")
	if err != nil {
		return nil, err
	}
	return total, nil
}

// --- Relationships ---

func (q *queries) InsertRelationship(ctx context.Context, rel storage.RelationshipRecord) error {
	_, err := q.db.Exec(ctx,
		`INSERT INTO "_default".transaction_relationships
		 (ledger_id, parent_tx_id, child_tx_id, relationship_type, event_id, system_time)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		rel.LedgerID, rel.ParentTxID, rel.ChildTxID, rel.RelationshipType, rel.EventID, rel.SystemTime,
	)
	return err
}

func (q *queries) GetRelationships(ctx context.Context, ledgerID, txID string, depth int) ([]storage.RelationshipRecord, error) {
	if depth <= 0 {
		depth = 1
	}
	if depth > 100 {
		depth = 100
	}
	// For depth=1, just get direct relationships. For deeper, use recursive CTE.
	var query string
	if depth == 1 {
		query = `SELECT ledger_id, parent_tx_id, child_tx_id, relationship_type, event_id, system_time
		         FROM "_default".transaction_relationships
		         WHERE ledger_id = $1 AND (parent_tx_id = $2 OR child_tx_id = $2)
		         ORDER BY system_time`
	} else {
		query = `WITH RECURSIVE chain AS (
		           SELECT ledger_id, parent_tx_id, child_tx_id, relationship_type, event_id, system_time, 1 as depth
		           FROM "_default".transaction_relationships
		           WHERE ledger_id = $1 AND (parent_tx_id = $2 OR child_tx_id = $2)
		         UNION ALL
		           SELECT r.ledger_id, r.parent_tx_id, r.child_tx_id, r.relationship_type, r.event_id, r.system_time, c.depth + 1
		           FROM "_default".transaction_relationships r
		           JOIN chain c ON r.ledger_id = c.ledger_id AND (r.parent_tx_id = c.child_tx_id OR r.child_tx_id = c.parent_tx_id)
		           WHERE c.depth < $3
		         )
		         SELECT DISTINCT ledger_id, parent_tx_id, child_tx_id, relationship_type, event_id, system_time
		         FROM chain ORDER BY system_time`
	}

	args := []any{ledgerID, txID}
	if depth > 1 {
		args = append(args, depth)
	}
	rows, err := q.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []storage.RelationshipRecord
	for rows.Next() {
		var rec storage.RelationshipRecord
		if err := rows.Scan(&rec.LedgerID, &rec.ParentTxID, &rec.ChildTxID,
			&rec.RelationshipType, &rec.EventID, &rec.SystemTime); err != nil {
			return nil, err
		}
		result = append(result, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating relationships: %w", err)
	}
	return result, nil
}
