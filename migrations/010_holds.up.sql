-- Phase 2: Holds and transaction relationships

CREATE TABLE "_default".holds (
    ledger_id           text NOT NULL,
    hold_id             text NOT NULL,
    source              text NOT NULL,
    destination_hint    text,
    asset               text NOT NULL,
    authorized_amount   numeric NOT NULL,
    captured_amount     numeric NOT NULL DEFAULT 0,
    voided              boolean NOT NULL DEFAULT false,
    expired             boolean NOT NULL DEFAULT false,
    expires_at          timestamptz NOT NULL,
    authorized_event_id text NOT NULL,
    valid_time          timestamptz NOT NULL,
    system_time         timestamptz NOT NULL,
    PRIMARY KEY (ledger_id, hold_id)
);
CREATE INDEX holds_expiry ON "_default".holds (expires_at)
    WHERE NOT voided AND NOT expired AND captured_amount < authorized_amount;
CREATE INDEX holds_source ON "_default".holds (ledger_id, source);

CREATE TABLE "_default".transaction_relationships (
    ledger_id           text NOT NULL,
    parent_tx_id        text NOT NULL,
    child_tx_id         text NOT NULL,
    relationship_type   smallint NOT NULL,
    event_id            text NOT NULL,
    system_time         timestamptz NOT NULL,
    PRIMARY KEY (ledger_id, parent_tx_id, child_tx_id, relationship_type)
);
CREATE INDEX relationships_parent ON "_default".transaction_relationships
    (ledger_id, parent_tx_id, relationship_type);
CREATE INDEX relationships_child ON "_default".transaction_relationships
    (ledger_id, child_tx_id, relationship_type);
