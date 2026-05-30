DROP INDEX IF EXISTS "_default".idx_pending_approvals_executing;
ALTER TABLE "_default".pending_approvals DROP COLUMN IF EXISTS executing_at;
