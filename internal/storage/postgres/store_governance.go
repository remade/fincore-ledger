package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/remade/ledger/internal/storage"
)

// --- Policies ---

// InsertPolicy on Store wraps in its own transaction for atomicity (deactivate + insert).
func (s *Store) InsertPolicy(ctx context.Context, policy storage.PolicyRecord) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning policy tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := insertPolicy(ctx, tx, policy); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// InsertPolicy on TxStore executes within the existing transaction.
func (t *TxStore) InsertPolicy(ctx context.Context, policy storage.PolicyRecord) error {
	return insertPolicy(ctx, t.tx, policy)
}

// insertPolicy runs the policy deactivate+insert against any DBTX.
func insertPolicy(ctx context.Context, db DBTX, policy storage.PolicyRecord) error {
	if _, err := db.Exec(ctx,
		`UPDATE "_default".policies SET active = false WHERE ledger_id = $1`, policy.LedgerID); err != nil {
		return fmt.Errorf("deactivating policies: %w", err)
	}
	if _, err := db.Exec(ctx,
		`INSERT INTO "_default".policies (ledger_id, version, cedar_policy, event_id, active)
		 VALUES ($1, $2, $3, $4, true)`,
		policy.LedgerID, policy.Version, policy.CedarPolicy, policy.EventID); err != nil {
		return fmt.Errorf("inserting policy: %w", err)
	}
	return nil
}

func (q *queries) GetActivePolicy(ctx context.Context, ledgerID string) (*storage.PolicyRecord, error) {
	var rec storage.PolicyRecord
	err := q.db.QueryRow(ctx,
		`SELECT ledger_id, version, cedar_policy, inserted_at, event_id, active
		 FROM "_default".policies
		 WHERE ledger_id = $1 AND active = true
		 ORDER BY inserted_at DESC LIMIT 1`,
		ledgerID,
	).Scan(&rec.LedgerID, &rec.Version, &rec.CedarPolicy, &rec.InsertedAt, &rec.EventID, &rec.Active)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // no policy = no restriction
		}
		return nil, err
	}
	return &rec, nil
}

// --- Approvals ---

func (q *queries) InsertPendingApproval(ctx context.Context, a storage.PendingApprovalRecord) error {
	approvalsJSON, err := json.Marshal(a.ReceivedApprovals)
	if err != nil {
		return fmt.Errorf("marshaling received approvals: %w", err)
	}
	_, err = q.db.Exec(ctx,
		`INSERT INTO "_default".pending_approvals
		 (ledger_id, intent_id, intent_payload, intent_hash, required_approvers,
		  received_approvals, expires_at, state, submitted_by)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		a.LedgerID, a.IntentID, a.IntentPayload, a.IntentHash,
		a.RequiredApprovers, approvalsJSON, a.ExpiresAt, a.State, a.SubmittedBy,
	)
	return err
}

func (q *queries) GetPendingApproval(ctx context.Context, ledgerID, intentID string) (*storage.PendingApprovalRecord, error) {
	var rec storage.PendingApprovalRecord
	var approvalsJSON []byte
	err := q.db.QueryRow(ctx,
		`SELECT ledger_id, intent_id, intent_payload, intent_hash, required_approvers,
		        received_approvals, expires_at, state, submitted_at, submitted_by
		 FROM "_default".pending_approvals
		 WHERE ledger_id = $1 AND intent_id = $2`,
		ledgerID, intentID,
	).Scan(&rec.LedgerID, &rec.IntentID, &rec.IntentPayload, &rec.IntentHash,
		&rec.RequiredApprovers, &approvalsJSON, &rec.ExpiresAt, &rec.State,
		&rec.SubmittedAt, &rec.SubmittedBy)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: pending approval %q", storage.ErrNotFound, intentID)
		}
		return nil, err
	}
	if err := json.Unmarshal(approvalsJSON, &rec.ReceivedApprovals); err != nil {
		return nil, fmt.Errorf("unmarshaling received approvals: %w", err)
	}
	return &rec, nil
}

// GetPendingApproval on TxStore adds FOR UPDATE to lock the approval row during transactions.
func (t *TxStore) GetPendingApproval(ctx context.Context, ledgerID, intentID string) (*storage.PendingApprovalRecord, error) {
	var rec storage.PendingApprovalRecord
	var approvalsJSON []byte
	err := t.tx.QueryRow(ctx,
		`SELECT ledger_id, intent_id, intent_payload, intent_hash, required_approvers,
		        received_approvals, expires_at, state, submitted_at, submitted_by
		 FROM "_default".pending_approvals
		 WHERE ledger_id = $1 AND intent_id = $2
		 FOR UPDATE`,
		ledgerID, intentID,
	).Scan(&rec.LedgerID, &rec.IntentID, &rec.IntentPayload, &rec.IntentHash,
		&rec.RequiredApprovers, &approvalsJSON, &rec.ExpiresAt, &rec.State,
		&rec.SubmittedAt, &rec.SubmittedBy)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: pending approval %q", storage.ErrNotFound, intentID)
		}
		return nil, err
	}
	if err := json.Unmarshal(approvalsJSON, &rec.ReceivedApprovals); err != nil {
		return nil, fmt.Errorf("unmarshaling received approvals: %w", err)
	}
	return &rec, nil
}

func (q *queries) AddApproval(ctx context.Context, ledgerID, intentID, principal, signature string) error {
	entry := storage.ApprovalEntry{
		Principal: principal,
		Signature: signature,
		SignedAt:  time.Now().UTC(),
	}
	entryJSON, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshaling approval entry: %w", err)
	}
	result, err := q.db.Exec(ctx,
		`UPDATE "_default".pending_approvals
		 SET received_approvals = received_approvals || jsonb_build_array($3::jsonb)
		 WHERE ledger_id = $1 AND intent_id = $2 AND state = 'pending'`,
		ledgerID, intentID, string(entryJSON),
	)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: approval %q is no longer pending", storage.ErrNotFound, intentID)
	}
	return nil
}

func (q *queries) UpdateApprovalState(ctx context.Context, ledgerID, intentID, state string) error {
	_, err := q.db.Exec(ctx,
		`UPDATE "_default".pending_approvals SET state = $3
		 WHERE ledger_id = $1 AND intent_id = $2`,
		ledgerID, intentID, state,
	)
	return err
}

func (q *queries) MarkApprovalExecuting(ctx context.Context, ledgerID, intentID string) error {
	_, err := q.db.Exec(ctx,
		`UPDATE "_default".pending_approvals
		 SET state = 'executing', executing_at = now()
		 WHERE ledger_id = $1 AND intent_id = $2`,
		ledgerID, intentID,
	)
	return err
}

// UpdateApprovalStateIf transitions the approval only if it is still in
// fromState. It returns true when a row was updated; false means another writer
// (e.g. a concurrently-completing Approve) already moved it, so the caller must
// not clobber that result.
func (q *queries) UpdateApprovalStateIf(ctx context.Context, ledgerID, intentID, fromState, toState string) (bool, error) {
	tag, err := q.db.Exec(ctx,
		`UPDATE "_default".pending_approvals SET state = $4
		 WHERE ledger_id = $1 AND intent_id = $2 AND state = $3`,
		ledgerID, intentID, fromState, toState,
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (q *queries) ListStuckApprovals(ctx context.Context, stuckThreshold time.Duration) ([]storage.PendingApprovalRecord, error) {
	rows, err := q.db.Query(ctx,
		`SELECT ledger_id, intent_id, intent_payload, intent_hash, required_approvers,
		        received_approvals, expires_at, state, submitted_at, submitted_by
		 FROM "_default".pending_approvals
		 WHERE state = 'executing' AND COALESCE(executing_at, submitted_at) < now() - $1::interval
		 LIMIT 1000`,
		stuckThreshold.String(),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []storage.PendingApprovalRecord
	for rows.Next() {
		var rec storage.PendingApprovalRecord
		var approvalsJSON []byte
		if err := rows.Scan(&rec.LedgerID, &rec.IntentID, &rec.IntentPayload, &rec.IntentHash,
			&rec.RequiredApprovers, &approvalsJSON, &rec.ExpiresAt, &rec.State,
			&rec.SubmittedAt, &rec.SubmittedBy); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(approvalsJSON, &rec.ReceivedApprovals); err != nil {
			return nil, fmt.Errorf("unmarshaling received approvals: %w", err)
		}
		result = append(result, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating stuck approvals: %w", err)
	}
	return result, nil
}

func (q *queries) ListPendingApprovals(ctx context.Context, ledgerID string, params storage.ListParams) ([]storage.PendingApprovalRecord, string, error) {
	pageSize := params.PageSize
	if pageSize <= 0 || pageSize > maxPageSize {
		pageSize = 100
	}
	// Fetch one extra to determine if there's a next page.
	limit := pageSize + 1

	var rows pgx.Rows
	var err error
	if params.PageToken != "" {
		rows, err = q.db.Query(ctx,
			`SELECT ledger_id, intent_id, intent_payload, intent_hash, required_approvers,
			        received_approvals, expires_at, state, submitted_at, submitted_by
			 FROM "_default".pending_approvals
			 WHERE ledger_id = $1 AND state = 'pending' AND intent_id > $2
			 ORDER BY intent_id
			 LIMIT $3`,
			ledgerID, params.PageToken, limit,
		)
	} else {
		rows, err = q.db.Query(ctx,
			`SELECT ledger_id, intent_id, intent_payload, intent_hash, required_approvers,
			        received_approvals, expires_at, state, submitted_at, submitted_by
			 FROM "_default".pending_approvals
			 WHERE ledger_id = $1 AND state = 'pending'
			 ORDER BY intent_id
			 LIMIT $2`,
			ledgerID, limit,
		)
	}
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var result []storage.PendingApprovalRecord
	for rows.Next() {
		var rec storage.PendingApprovalRecord
		var approvalsJSON []byte
		if err := rows.Scan(&rec.LedgerID, &rec.IntentID, &rec.IntentPayload, &rec.IntentHash,
			&rec.RequiredApprovers, &approvalsJSON, &rec.ExpiresAt, &rec.State,
			&rec.SubmittedAt, &rec.SubmittedBy); err != nil {
			return nil, "", err
		}
		if err := json.Unmarshal(approvalsJSON, &rec.ReceivedApprovals); err != nil {
			return nil, "", fmt.Errorf("unmarshaling received approvals: %w", err)
		}
		result = append(result, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterating pending approvals: %w", err)
	}

	var nextToken string
	if len(result) > pageSize {
		nextToken = result[pageSize].IntentID
		result = result[:pageSize]
	}
	return result, nextToken, nil
}

func (q *queries) ListExpiredApprovals(ctx context.Context) ([]storage.PendingApprovalRecord, error) {
	rows, err := q.db.Query(ctx,
		`SELECT ledger_id, intent_id, intent_payload, intent_hash, required_approvers,
		        received_approvals, expires_at, state, submitted_at, submitted_by
		 FROM "_default".pending_approvals
		 WHERE state = 'pending' AND expires_at < now()
		 LIMIT 1000`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []storage.PendingApprovalRecord
	for rows.Next() {
		var rec storage.PendingApprovalRecord
		var approvalsJSON []byte
		if err := rows.Scan(&rec.LedgerID, &rec.IntentID, &rec.IntentPayload, &rec.IntentHash,
			&rec.RequiredApprovers, &approvalsJSON, &rec.ExpiresAt, &rec.State,
			&rec.SubmittedAt, &rec.SubmittedBy); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(approvalsJSON, &rec.ReceivedApprovals); err != nil {
			return nil, fmt.Errorf("unmarshaling received approvals: %w", err)
		}
		result = append(result, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating expired approvals: %w", err)
	}
	return result, nil
}
