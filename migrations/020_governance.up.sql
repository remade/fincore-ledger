-- Phase 3: Policies and approval workflows

CREATE TABLE "_default".policies (
    ledger_id        text NOT NULL,
    version          text NOT NULL,
    cedar_policy     text NOT NULL,
    inserted_at      timestamptz NOT NULL DEFAULT now(),
    event_id         text NOT NULL,
    active           boolean NOT NULL DEFAULT true,
    PRIMARY KEY (ledger_id, version)
);

CREATE TABLE "_default".pending_approvals (
    ledger_id            text NOT NULL,
    intent_id            text NOT NULL,
    intent_payload       bytea NOT NULL,
    intent_hash          bytea NOT NULL,
    required_approvers   text[] NOT NULL,
    received_approvals   jsonb NOT NULL DEFAULT '[]'::jsonb,
    expires_at           timestamptz NOT NULL,
    state                text NOT NULL DEFAULT 'pending'
                         CHECK (state IN ('pending', 'approved', 'executing', 'executed', 'rejected', 'expired', 'withdrawn')),
    submitted_at         timestamptz NOT NULL DEFAULT now(),
    submitted_by         text NOT NULL,
    PRIMARY KEY (ledger_id, intent_id)
);

CREATE INDEX idx_pending_approvals_expiry ON "_default".pending_approvals (ledger_id, state, expires_at)
    WHERE state = 'pending';

CREATE INDEX idx_policies_active ON "_default".policies (ledger_id, active DESC, inserted_at DESC)
    WHERE active = true;
