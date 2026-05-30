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

	// All approved -- mark as "executing" (recording executing_at) to prevent
	// double-execution on crash recovery, then commit and execute the intent.
	if err := txStore.MarkApprovalExecuting(ctx, ledgerID, intentID); err != nil {
		return nil, err
	}

	if err := txStore.Commit(); err != nil {
		return nil, err
	}

	// Deserialize and execute the approved intent. Execution uses a deterministic
	// idempotency key (see approvalExecutionKey) so the committed financial event
	// carries it; a later recovery sweep can then tell whether the intent already
	// executed without re-running it.
	var intent BatchIntent
	if err := json.Unmarshal(approval.IntentPayload, &intent); err != nil {
		if stateErr := p.store.UpdateApprovalState(ctx, ledgerID, intentID, "rejected"); stateErr != nil {
			p.logger.Error("failed to mark approval as rejected after deserialization failure",
				zap.String("intent_id", intentID), zap.Error(stateErr))
		}
		return nil, fmt.Errorf("deserializing approved intent: %w", err)
	}
	intent.IdempotencyKey = approvalExecutionKey(intentID)

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

	// Guarded transition (executing -> executed): if a recovery sweep already
	// resolved this row, do not overwrite it. Makes the no-clobber invariant
	// local rather than relying on whole-flow reasoning.
	if updated, err := p.store.UpdateApprovalStateIf(ctx, ledgerID, intentID, "executing", "executed"); err != nil {
		p.logger.Error("failed to mark approval as executed", zap.String("intent_id", intentID), zap.Error(err))
	} else if !updated {
		p.logger.Info("approval already resolved before final state write; not overwriting",
			zap.String("intent_id", intentID))
	}

	p.logger.Info("approval completed, intent executed",
		zap.String("intent_id", intentID),
		zap.String("event_id", result.EventID),
	)

	return result, nil
}

// approvalExecutionKey returns the idempotency key used to execute an approved
// intent. It is derived solely from the server-generated intent ID (a ULID), so
// it is unique to this approval and cannot collide with an unrelated event. A
// client-supplied key inside the intent payload is deliberately NOT used: doing
// so would let a caller point recovery at an unrelated event (and let the
// Submit* idempotency short-circuit skip the real effect). Because this key is
// recorded on the committed event, recovery can detect execution unambiguously.
func approvalExecutionKey(intentID string) string {
	return "approval:" + intentID
}

// ExpireStaleApprovals transitions pending approvals past their deadline to
// expired, and recovers approvals stuck in the "executing" state for longer than
// stuckThreshold.
//
// A stuck approval means the process committed "executing" but crashed before
// recording a terminal state. Recovery is crash-safe: it checks whether the
// intent's execution actually committed (via the deterministic idempotency key
// recorded on the event) rather than re-executing. If events exist the intent
// truly ran, so the approval is marked "executed"; otherwise it never ran and is
// safely marked "expired". This avoids both double-execution and wrongly
// expiring approvals whose financial effects are already real.
func (p *Planner) ExpireStaleApprovals(ctx context.Context, stuckThreshold time.Duration) error {
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

	stuck, err := p.store.ListStuckApprovals(ctx, stuckThreshold)
	if err != nil {
		return err
	}
	var completed, abandoned int
	for _, a := range stuck {
		key := approvalExecutionKey(a.IntentID)
		events, err := p.store.ListLogEventsByIdempotencyKey(ctx, a.LedgerID, key)
		if err != nil {
			// Cannot determine execution status; leave "executing" for the next
			// sweep rather than risk a wrong terminal transition.
			p.logger.Error("failed to check stuck approval execution status; leaving for next sweep",
				zap.String("intent_id", a.IntentID), zap.Error(err))
			continue
		}

		nextState := "expired"
		if len(events) > 0 {
			nextState = "executed"
		}
		// Guarded transition: only resolve a row that is still "executing", so a
		// concurrently-completing Approve that already wrote a terminal state is
		// never clobbered (e.g. recovery does not flip a real "executed" back to
		// "expired" while execution commits in a different process).
		updated, err := p.store.UpdateApprovalStateIf(ctx, a.LedgerID, a.IntentID, "executing", nextState)
		if err != nil {
			p.logger.Error("failed to update stuck approval state",
				zap.String("intent_id", a.IntentID), zap.String("state", nextState), zap.Error(err))
			continue
		}
		if !updated {
			p.logger.Info("stuck approval already resolved by the live path; skipping",
				zap.String("intent_id", a.IntentID))
			continue
		}
		if nextState == "executed" {
			completed++
		} else {
			abandoned++
		}
		p.logger.Warn("recovered stuck approval",
			zap.String("intent_id", a.IntentID),
			zap.String("ledger_id", a.LedgerID),
			zap.String("resolved_state", nextState),
			zap.Bool("executed", len(events) > 0),
		)
	}
	if completed+abandoned > 0 {
		p.logger.Warn("recovered stuck approvals",
			zap.Int("completed_as_executed", completed),
			zap.Int("expired_unexecuted", abandoned),
		)
	}

	return nil
}
