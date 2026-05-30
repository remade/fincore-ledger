package planner

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"

	"github.com/remade/ledger/internal/storage"
)

// SubmitForApproval parks an intent for approval instead of executing it immediately.
func (p *Planner) SubmitForApproval(ctx context.Context, ledgerID string, intentPayload []byte, requiredApprovers []string, submittedBy string, expiresIn time.Duration) (string, error) {
	if _, err := p.checkLedgerNotSealed(ctx, ledgerID); err != nil {
		return "", err
	}

	intentID := ulid.Make().String()
	h := sha256.Sum256(intentPayload)

	now := time.Now().UTC()
	expiresAt := now.Add(expiresIn)
	if expiresIn == 0 {
		expiresAt = now.Add(24 * time.Hour)
	}

	if err := p.store.InsertPendingApproval(ctx, storage.PendingApprovalRecord{
		LedgerID:          ledgerID,
		IntentID:          intentID,
		IntentPayload:     intentPayload,
		IntentHash:        h[:],
		RequiredApprovers: requiredApprovers,
		ExpiresAt:         expiresAt,
		State:             "pending",
		SubmittedBy:       submittedBy,
		SubmittedAt:       now,
	}); err != nil {
		return "", err
	}

	p.logger.Debug("intent submitted for approval",
		zap.String("intent_id", intentID),
		zap.Strings("approvers", requiredApprovers),
	)

	return intentID, nil
}

// Approve records an approval on a pending intent. If all required approvals are
// received, the intent is executed. The entire flow runs inside a single transaction
// with row locking to prevent double execution.
func (p *Planner) Approve(ctx context.Context, ledgerID, intentID, principal, signature string) (*SubmitResult, error) {
	txStore, err := p.store.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning approve tx: %w", err)
	}
	defer txStore.Rollback()

	// Read with FOR UPDATE lock to prevent concurrent approvals from racing.
	approval, err := txStore.GetPendingApproval(ctx, ledgerID, intentID)
	if err != nil {
		return nil, err
	}

	if approval.State != "pending" {
		return nil, fmt.Errorf("approval %s is in state %q, expected pending", intentID, approval.State)
	}

	if time.Now().UTC().After(approval.ExpiresAt) {
		if err := txStore.UpdateApprovalState(ctx, ledgerID, intentID, "expired"); err != nil {
			return nil, fmt.Errorf("marking approval %s as expired: %w", intentID, err)
		}
		if err := txStore.Commit(); err != nil {
			return nil, fmt.Errorf("committing expired state for approval %s: %w", intentID, err)
		}
		return nil, fmt.Errorf("approval %s has expired", intentID)
	}

	// Self-approval guard: submitter cannot approve their own intent.
	if principal == approval.SubmittedBy {
		return nil, fmt.Errorf("submitter %q cannot approve their own intent", principal)
	}

	// Verify principal is a required approver.
	isRequiredApprover := false
	for _, ra := range approval.RequiredApprovers {
		if ra == principal {
			isRequiredApprover = true
			break
		}
	}
	if !isRequiredApprover {
		return nil, fmt.Errorf("principal %q is not a required approver for intent %s", principal, intentID)
	}

	// Prevent duplicate approvals from the same principal.
	for _, a := range approval.ReceivedApprovals {
		if a.Principal == principal {
			return nil, fmt.Errorf("principal %q has already approved intent %s", principal, intentID)
		}
	}

	// Record the approval inside the transaction.
	if err := txStore.AddApproval(ctx, ledgerID, intentID, principal, signature); err != nil {
		return nil, err
	}

	// Re-read to check if we have enough (within same TX, sees the write above).
	approval, err = txStore.GetPendingApproval(ctx, ledgerID, intentID)
	if err != nil {
		return nil, err
	}

	// Check if all required approvers have approved.
	approvedSet := make(map[string]bool)
	for _, a := range approval.ReceivedApprovals {
		approvedSet[a.Principal] = true
	}

	allApproved := true
	for _, required := range approval.RequiredApprovers {
		if !approvedSet[required] {
			allApproved = false
			break
		}
	}

	if !allApproved {
		if err := txStore.Commit(); err != nil {
			return nil, err
		}
		return &SubmitResult{EventID: intentID}, nil // partial, still pending
	}

	// All approved -- mark as "executing" to prevent double-execution on crash recovery,
	// then commit and execute the intent.
	if err := txStore.UpdateApprovalState(ctx, ledgerID, intentID, "executing"); err != nil {
		return nil, err
	}

	if err := txStore.Commit(); err != nil {
		return nil, err
	}

	// Deserialize and execute the approved intent.
	var intent BatchIntent
	if err := json.Unmarshal(approval.IntentPayload, &intent); err != nil {
		if stateErr := p.store.UpdateApprovalState(ctx, ledgerID, intentID, "rejected"); stateErr != nil {
			p.logger.Error("failed to mark approval as rejected after deserialization failure",
				zap.String("intent_id", intentID), zap.Error(stateErr))
		}
		return nil, fmt.Errorf("deserializing approved intent: %w", err)
	}

	result, err := p.executeIntent(ctx, ledgerID, intent)
	if err != nil {
		if stateErr := p.store.UpdateApprovalState(ctx, ledgerID, intentID, "rejected"); stateErr != nil {
			p.logger.Error("failed to mark approval as rejected after execution failure",
				zap.String("intent_id", intentID), zap.Error(stateErr))
		}
		return nil, fmt.Errorf("executing approved intent: %w", err)
	}

	// Record APPROVAL_RECORDED event -- failures here are logged at ERROR level
	// so they can be detected and recovered. The intent has already executed
	// successfully, so the state change is real even if the audit event fails.
	now := time.Now().UTC()
	eventID := ulid.Make().String()

	batchID, batchErr := p.batch.CurrentBatchID(ctx, ledgerID)
	if batchErr != nil {
		p.logger.Error("AUDIT_GAP: failed to get batch ID for approval event -- audit event not written",
			zap.String("intent_id", intentID), zap.Error(batchErr))
	} else {
		auditTx, txErr := p.store.BeginTx(ctx)
		if txErr != nil {
			p.logger.Error("AUDIT_GAP: failed to begin tx for approval event",
				zap.String("intent_id", intentID), zap.Error(txErr))
		} else {
			defer auditTx.Rollback()
			seq, seqErr := auditTx.NextLedgerSeq(ctx, ledgerID)
			if seqErr != nil {
				p.logger.Error("AUDIT_GAP: failed to get seq for approval event",
					zap.String("intent_id", intentID), zap.Error(seqErr))
			} else {
				if appendErr := auditTx.AppendLogEvent(ctx, storage.LogEventRecord{
					EventID: eventID, LedgerID: ledgerID, LedgerSeq: seq,
					SystemTime: now, ValidTime: now, Type: storage.EventTypeApprovalRecorded,
					Payload: approval.IntentPayload,
					BatchID: batchID, SchemaVersion: 1,
				}); appendErr != nil {
					p.logger.Error("AUDIT_GAP: failed to append approval event",
						zap.String("intent_id", intentID), zap.Error(appendErr))
				} else if commitErr := auditTx.Commit(); commitErr != nil {
					p.logger.Error("AUDIT_GAP: failed to commit approval event",
						zap.String("intent_id", intentID), zap.Error(commitErr))
				} else {
					p.publishEvent(ctx, ledgerID, eventID, 13)
				}
			}
		}
	}

	if err := p.store.UpdateApprovalState(ctx, ledgerID, intentID, "executed"); err != nil {
		p.logger.Error("failed to mark approval as executed", zap.String("intent_id", intentID), zap.Error(err))
	}

	p.logger.Info("approval completed, intent executed",
		zap.String("intent_id", intentID),
		zap.String("event_id", result.EventID),
	)

	return result, nil
}

// ExpireStaleApprovals transitions pending approvals past their deadline to expired,
// and recovers approvals stuck in "executing" state past the stuck threshold.
func (p *Planner) ExpireStaleApprovals(ctx context.Context) error {
	expired, err := p.store.ListExpiredApprovals(ctx)
	if err != nil {
		return err
	}
	for _, a := range expired {
		if err := p.store.UpdateApprovalState(ctx, a.LedgerID, a.IntentID, "expired"); err != nil {
			p.logger.Error("failed to expire approval", zap.String("intent_id", a.IntentID), zap.Error(err))
		}
	}
	if len(expired) > 0 {
		p.logger.Info("expired stale approvals", zap.Int("count", len(expired)))
	}

	// Recover approvals stuck in "executing" state for more than 5 minutes.
	// This handles cases where the process crashed or the state update failed
	// after intent execution completed.
	stuck, err := p.store.ListStuckApprovals(ctx, 5*time.Minute)
	if err != nil {
		return err
	}
	for _, a := range stuck {
		p.logger.Error("recovering stuck approval -- transitioning from executing to expired",
			zap.String("intent_id", a.IntentID),
			zap.String("ledger_id", a.LedgerID),
		)
		if err := p.store.UpdateApprovalState(ctx, a.LedgerID, a.IntentID, "expired"); err != nil {
			p.logger.Error("failed to recover stuck approval", zap.String("intent_id", a.IntentID), zap.Error(err))
		}
	}
	if len(stuck) > 0 {
		p.logger.Warn("recovered stuck approvals", zap.Int("count", len(stuck)))
	}

	return nil
}
