# ledgerctl

Command-line interface for the Ledger double-entry accounting system.

## Installation

```bash
go install github.com/remade/ledger/cmd/ledgerctl@latest
```

Or build from source:

```bash
go build -o ledgerctl ./cmd/ledgerctl/
```

## Global Flags

| Flag | Default | Description |
|---|---|---|
| `--server-addr` | `localhost:9090` | gRPC server address |
| `--postgres-dsn` | (see below) | PostgreSQL DSN for migration commands |
| `-o, --output` | `table` | Output format: `table` or `json` |

The `--postgres-dsn` flag falls back to `LEDGER_POSTGRES_DSN` env var, then to `postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable`.

## Commands

### Migrations

Run or roll back database migrations. These connect directly to PostgreSQL (no gRPC server required).

```bash
# Apply all pending migrations
ledgerctl migrate-up --postgres-dsn "postgres://user:pass@host:5432/db"

# Roll back migrations
ledgerctl migrate-down
```

### Submit

Submit intents to the ledger. All submit subcommands require `--ledger` and support optional `--ik` (idempotency key), `--reference`, and `--dry-run` flags.

#### Post a transaction

```bash
ledgerctl submit post \
  --ledger my-ledger \
  --posting "_world:users:42:wallet:10000:USD/2" \
  --posting "users:42:wallet:fees:platform:100:USD/2" \
  --reference "payment-123" \
  --ik "pay-123-v1"
```

The `--posting` flag format is `source:destination:amount:asset`. Multiple postings create an atomic multi-leg transaction.

#### Authorize a hold

```bash
ledgerctl submit authorize \
  --ledger my-ledger \
  --source "users:42:wallet" \
  --amount "5000" \
  --asset "USD/2" \
  --destination-hint "merchants:99" \
  --expires-at "2025-12-31T23:59:59Z"
```

#### Capture a hold

```bash
ledgerctl submit capture \
  --ledger my-ledger \
  --hold-id "01JXYZ..." \
  --amount "4500" \
  --destination "merchants:99"
```

#### Void a hold

```bash
ledgerctl submit void \
  --ledger my-ledger \
  --hold-id "01JXYZ..."
```

#### Revert a transaction

```bash
ledgerctl submit revert \
  --ledger my-ledger \
  --tx-id "01JABC..." \
  --reason "customer refund" \
  --at-effective-date
```

#### Amend transaction metadata

```bash
ledgerctl submit amend \
  --ledger my-ledger \
  --tx-id "01JABC..." \
  --metadata "status=disputed" \
  --metadata "dispute_id=D-456"
```

#### Currency conversion

```bash
ledgerctl submit convert \
  --ledger my-ledger \
  --source "treasury:usd" \
  --destination "treasury:eur" \
  --src-amount "10000" \
  --src-asset "USD/2" \
  --dst-amount "9200" \
  --dst-asset "EUR/2" \
  --rate "0.92" \
  --rate-source "ecb"
```

#### Set metadata

```bash
ledgerctl submit set-metadata \
  --ledger my-ledger \
  --target-type account \
  --target-id "users:42:wallet" \
  --metadata "tier=premium" \
  --metadata "region=eu-west"
```

#### Delete metadata

```bash
ledgerctl submit delete-metadata \
  --ledger my-ledger \
  --target-type transaction \
  --target-id "01JABC..." \
  --key "temporary_flag"
```

### Balance

Query account balances.

```bash
# Get balance for a specific account and asset
ledgerctl balance get \
  --ledger my-ledger \
  --account "users:42:wallet" \
  --asset "USD/2" \
  --include-holds

# JSON output
ledgerctl balance get \
  --ledger my-ledger \
  --account "users:42:wallet" \
  --asset "USD/2" \
  -o json
```

### Log

View the immutable event log.

```bash
# List recent events
ledgerctl log list --ledger my-ledger --page-size 50

# Get a specific event
ledgerctl log get --ledger my-ledger --event-id "01JXYZ..."

# JSON output for scripting
ledgerctl log list --ledger my-ledger -o json
```

## Environment Variables

| Variable | Flag equivalent | Description |
|---|---|---|
| `LEDGER_POSTGRES_DSN` | `--postgres-dsn` | PostgreSQL connection string |

## Output Formats

- **table** (default): Human-readable tabular output
- **json**: Machine-readable JSON, uses protobuf field names for gRPC responses
