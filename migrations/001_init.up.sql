-- =========================================================================
-- catalog: cross-ledger, in the _system schema
-- =========================================================================

CREATE SCHEMA IF NOT EXISTS _system;

CREATE TABLE _system.buckets (
    id              text PRIMARY KEY,
    created_at      timestamptz NOT NULL DEFAULT now(),
    metadata        jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE _system.ledgers (
    id              text PRIMARY KEY,
    bucket_id       text NOT NULL REFERENCES _system.buckets(id),
    state           text NOT NULL DEFAULT 'initializing'
                    CHECK (state IN ('initializing', 'in-use', 'sealed')),
    features        jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata        jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    sealed_at       timestamptz,
    issuer_accounts text[] NOT NULL DEFAULT ARRAY['_world']
);

-- =========================================================================
-- per-bucket: tables live in the bucket's schema, partitioned by ledger_id
-- Default bucket created automatically.
-- =========================================================================

CREATE SCHEMA IF NOT EXISTS "_default";

-- Insert the default bucket into the catalog.
INSERT INTO _system.buckets (id) VALUES ('_default');

-- The log: canonical truth. One row per state-changing operation.
CREATE TABLE "_default".log_events (
    event_id         text PRIMARY KEY,
    ledger_id        text NOT NULL,
    ledger_seq       bigint NOT NULL,
    system_time      timestamptz NOT NULL,
    valid_time       timestamptz NOT NULL,
    type             smallint NOT NULL,
    payload          bytea NOT NULL,
    idempotency_key  text,
    idempotency_hash bytea,
    batch_id         text NOT NULL,
    schema_version   bigint NOT NULL DEFAULT 1,
    UNIQUE (ledger_id, ledger_seq),
    UNIQUE (ledger_id, idempotency_key)
        DEFERRABLE INITIALLY IMMEDIATE
);
CREATE INDEX log_events_ledger_time ON "_default".log_events (ledger_id, system_time);
CREATE INDEX log_events_ledger_valid ON "_default".log_events (ledger_id, valid_time);
CREATE INDEX log_events_batch ON "_default".log_events (batch_id);

-- Merkle batches: one row per closed batch.
CREATE TABLE "_default".log_batches (
    batch_id         text PRIMARY KEY,
    ledger_id        text NOT NULL,
    opened_at        timestamptz NOT NULL,
    closed_at        timestamptz,
    event_count      integer NOT NULL DEFAULT 0,
    merkle_root      bytea,
    prev_batch_id    text REFERENCES "_default".log_batches(batch_id),
    attestation_uri  text
);
CREATE INDEX log_batches_ledger ON "_default".log_batches (ledger_id, opened_at);

-- Idempotency keys: durable copy. Redis caches recent entries.
CREATE TABLE "_default".idempotency_keys (
    ledger_id        text NOT NULL,
    idempotency_key  text NOT NULL,
    idempotency_hash bytea NOT NULL,
    event_id         text NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ledger_id, idempotency_key)
);

-- Accounts: lazily created. No balance stored here.
CREATE TABLE "_default".accounts (
    ledger_id        text NOT NULL,
    address          text NOT NULL,
    first_usage      timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL,
    metadata         jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (ledger_id, address)
);
CREATE INDEX accounts_metadata_gin ON "_default".accounts USING GIN (metadata);

-- Transactions: a structural projection of TRANSACTION_POSTED events.
CREATE TABLE "_default".transactions (
    ledger_id        text NOT NULL,
    transaction_id   text NOT NULL,
    event_id         text NOT NULL,
    valid_time       timestamptz NOT NULL,
    system_time      timestamptz NOT NULL,
    reference        text,
    postings         jsonb NOT NULL,
    metadata         jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (ledger_id, transaction_id),
    UNIQUE (ledger_id, reference)
);
CREATE INDEX transactions_valid_time ON "_default".transactions (ledger_id, valid_time);
CREATE INDEX transactions_metadata_gin ON "_default".transactions USING GIN (metadata);
CREATE INDEX transactions_postings_gin ON "_default".transactions USING GIN (postings);

-- Volumes deltas: event-sourced balance changes. One row per posting per side.
CREATE TABLE "_default".volumes_delta (
    ledger_id        text NOT NULL,
    account          text NOT NULL,
    asset            text NOT NULL,
    shard            smallint NOT NULL DEFAULT 0,
    event_id         text NOT NULL,
    valid_time       timestamptz NOT NULL,
    system_time      timestamptz NOT NULL,
    input_delta      numeric NOT NULL DEFAULT 0,
    output_delta     numeric NOT NULL DEFAULT 0,
    PRIMARY KEY (ledger_id, account, asset, shard, event_id)
);
CREATE INDEX volumes_delta_query ON "_default".volumes_delta
    (ledger_id, account, asset, valid_time, system_time);

-- Volumes checkpoints: periodic rollups for fast reads.
CREATE TABLE "_default".volumes_checkpoint (
    ledger_id        text NOT NULL,
    account          text NOT NULL,
    asset            text NOT NULL,
    shard            smallint NOT NULL DEFAULT 0,
    valid_time_upper timestamptz NOT NULL,
    system_time_upper timestamptz NOT NULL,
    total_input      numeric NOT NULL,
    total_output     numeric NOT NULL,
    last_event_id    text NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ledger_id, account, asset, shard, valid_time_upper, system_time_upper)
);

-- Schemas (chart of accounts + transaction templates).
CREATE TABLE "_default".schemas (
    ledger_id        text NOT NULL,
    version          text NOT NULL,
    document         jsonb NOT NULL,
    inserted_at      timestamptz NOT NULL DEFAULT now(),
    event_id         text NOT NULL,
    PRIMARY KEY (ledger_id, version)
);

-- Metadata history: one row per change.
CREATE TABLE "_default".metadata_history (
    ledger_id        text NOT NULL,
    target_type      smallint NOT NULL,
    target_id        text NOT NULL,
    revision         bigint NOT NULL,
    metadata         jsonb NOT NULL,
    event_id         text NOT NULL,
    system_time      timestamptz NOT NULL,
    PRIMARY KEY (ledger_id, target_type, target_id, revision)
);
