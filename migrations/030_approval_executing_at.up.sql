-- Track when an approval transitions into the "executing" state so crash
-- recovery measures stuck-ness by execution duration, not by time since the
-- intent was submitted (which would falsely flag long-pending approvals the
-- instant they begin executing).
ALTER TABLE "_default".pending_approvals ADD COLUMN executing_at timestamptz;

CREATE INDEX idx_pending_approvals_executing
    ON "_default".pending_approvals (ledger_id, state, executing_at)
    WHERE state = 'executing';
