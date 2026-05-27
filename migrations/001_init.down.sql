DROP TABLE IF EXISTS "_default".metadata_history;
DROP TABLE IF EXISTS "_default".schemas;
DROP TABLE IF EXISTS "_default".volumes_checkpoint;
DROP TABLE IF EXISTS "_default".volumes_delta;
DROP TABLE IF EXISTS "_default".transactions;
DROP TABLE IF EXISTS "_default".accounts;
DROP TABLE IF EXISTS "_default".idempotency_keys;
DROP TABLE IF EXISTS "_default".log_events;
DROP TABLE IF EXISTS "_default".log_batches;

DELETE FROM _system.buckets WHERE id = '_default';

DROP SCHEMA IF EXISTS "_default";

DROP TABLE IF EXISTS _system.ledgers;
DROP TABLE IF EXISTS _system.buckets;
DROP SCHEMA IF EXISTS _system;
