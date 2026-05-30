package planner

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/remade/ledger/internal/storage"
)

// EvaluatePolicy checks the active policy for a ledger against a proposed operation.
// Returns nil if allowed, error if denied.
// Does NOT write audit events -- callers should call WritePolicyDenialEvent explicitly
// when the denial is for an actual execution attempt (not approval submission).
func (p *Planner) EvaluatePolicy(ctx context.Context, ledgerID, principal, operationType string, accountsTouched []string) error {
	denied, reason, err := p.checkPolicy(ctx, ledgerID, principal, operationType, accountsTouched)
	if err != nil {
		return err
	}
	if denied {
		return fmt.Errorf("%w: %s", storage.ErrPolicyDenied, reason)
	}
	return nil
}

// WritePolicyDenialEvent records a POLICY_DENIAL audit event in the log.
// Should be called by the caller of EvaluatePolicy only when the denial
// is for an actual execution attempt, not an approval submission.
func (p *Planner) WritePolicyDenialEvent(ctx context.Context, ledgerID, principal, reason string) error {
	return p.writePolicyDenialEvent(ctx, ledgerID, principal, reason)
}

// checkPolicy evaluates the policy without side effects.
// Returns (denied, reason, error).
func (p *Planner) checkPolicy(ctx context.Context, ledgerID, principal, operationType string, accountsTouched []string) (bool, string, error) {
	pol, err := p.store.GetActivePolicy(ctx, ledgerID)
	if err != nil {
		return false, "", err
	}
	if pol == nil {
		return false, "", nil
	}

	result := p.evaluator.Evaluate(pol.CedarPolicy, principal, operationType, accountsTouched)
	return result.Denied, result.Reason, nil
}

// writePolicyDenialEvent records a POLICY_DENIAL audit event in the log.
func (p *Planner) writePolicyDenialEvent(ctx context.Context, ledgerID, principal, reason string) error {
	eventID := ulid.Make().String()
	now := time.Now().UTC()

	batchID, err := p.batch.CurrentBatchID(ctx, ledgerID)
	if err != nil {
		return fmt.Errorf("getting batch ID: %w", err)
	}

	txStore, err := p.store.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer txStore.Rollback()

	seq, err := txStore.NextLedgerSeq(ctx, ledgerID)
	if err != nil {
		return err
	}

	payload, err := json.Marshal(map[string]string{
		"principal": principal,
		"reason":    reason,
	})
	if err != nil {
		return fmt.Errorf("marshaling policy denial payload: %w", err)
	}

	if err := txStore.AppendLogEvent(ctx, storage.LogEventRecord{
		EventID: eventID, LedgerID: ledgerID, LedgerSeq: seq,
		SystemTime: now, ValidTime: now, Type: storage.EventTypePolicyUpdated,
		Payload: payload,
		BatchID: batchID, SchemaVersion: 1,
	}); err != nil {
		return err
	}
	return txStore.Commit()
}
