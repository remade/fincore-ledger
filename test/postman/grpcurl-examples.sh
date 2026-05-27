#!/usr/bin/env bash
# Ledger API walkthrough using ledgerctl (and grpcurl for operations without CLI support).
#
# Prerequisites:
#   docker-compose -f deploy/docker-compose/docker-compose.yml up -d
#   go run ./cmd/server &
#   go run ./cmd/worker &
#   go build -o ledgerctl ./cmd/ledgerctl/
#
# Optional (for ledger lifecycle, account/transaction listing, batch, verify, seal):
#   brew install grpcurl
#
# Usage:
#   ./test/postman/grpcurl-examples.sh

set -euo pipefail

GRPC_HOST="${GRPC_HOST:-localhost:9090}"
LEDGERCTL="${LEDGERCTL:-./ledgerctl}"
LEDGER_ID="demo-ledger"
CTL="$LEDGERCTL --server-addr $GRPC_HOST"

has_grpcurl() { command -v grpcurl &>/dev/null; }

if ! has_grpcurl; then
  echo "NOTE: grpcurl not found. Sections requiring grpcurl will be skipped."
  echo "      Install with: brew install grpcurl"
  echo ""
fi

echo "=== Ledger API Walkthrough ==="
echo "Target: $GRPC_HOST"
echo ""

# ─────────────────────────────────────────────
# 1. LEDGER LIFECYCLE (requires grpcurl)
# ─────────────────────────────────────────────
if has_grpcurl; then
  echo "--- Create Ledger ---"
  grpcurl -plaintext -d '{
    "id": "'"$LEDGER_ID"'",
    "bucket_id": "_default",
    "metadata": {"environment": "development"}
  }' "$GRPC_HOST" ledger.v1.LedgerService/CreateLedger || true

  echo ""
  echo "--- Get Ledger ---"
  grpcurl -plaintext -d '{"id": "'"$LEDGER_ID"'"}' \
    "$GRPC_HOST" ledger.v1.LedgerService/GetLedger

  echo ""
  echo "--- List Ledgers ---"
  grpcurl -plaintext -d '{"page_size": 10}' \
    "$GRPC_HOST" ledger.v1.LedgerService/ListLedgers
else
  echo "--- Skipping ledger lifecycle (requires grpcurl) ---"
  echo "Create the ledger manually before continuing:"
  echo "  grpcurl -plaintext -d '{\"id\": \"$LEDGER_ID\", \"bucket_id\": \"_default\"}' $GRPC_HOST ledger.v1.LedgerService/CreateLedger"
fi

# ─────────────────────────────────────────────
# 2. POST TRANSACTIONS
# ─────────────────────────────────────────────
echo ""
echo "--- Post Transaction: Fund user from _world ---"
$CTL submit post \
  --ledger "$LEDGER_ID" \
  --reference "fund-user-001" \
  --ik "ik-fund-001" \
  --posting "_world:users:001:wallet:10000:USD/2"

echo ""
echo "--- Post Transaction: Multi-posting (payment + fee) ---"
$CTL submit post \
  --ledger "$LEDGER_ID" \
  --reference "payment-001" \
  --posting "users:001:wallet:merchants:shop42:5000:USD/2" \
  --posting "users:001:wallet:fees:platform:150:USD/2"

echo ""
echo "--- Post Transaction: Idempotency retry (same IK, expect idempotent hit) ---"
$CTL submit post \
  --ledger "$LEDGER_ID" \
  --reference "fund-user-001" \
  --ik "ik-fund-001" \
  --posting "_world:users:001:wallet:10000:USD/2"

echo ""
echo "--- Post Transaction: Dry run (expect insufficient funds) ---"
$CTL submit post \
  --ledger "$LEDGER_ID" \
  --dry-run \
  --posting "users:001:wallet:users:002:wallet:99999999:USD/2" || true

# ─────────────────────────────────────────────
# 3. BALANCE
# ─────────────────────────────────────────────
echo ""
echo "--- Get Balance (table) ---"
$CTL balance get \
  --ledger "$LEDGER_ID" \
  --account "users:001:wallet" \
  --asset "USD/2"

echo ""
echo "--- Get Balance (JSON) ---"
$CTL balance get \
  --ledger "$LEDGER_ID" \
  --account "users:001:wallet" \
  --asset "USD/2" \
  -o json

# ─────────────────────────────────────────────
# 4. READS (requires grpcurl)
# ─────────────────────────────────────────────
if has_grpcurl; then
  echo ""
  echo "--- Get Account ---"
  grpcurl -plaintext -d '{
    "ledger_id": "'"$LEDGER_ID"'",
    "address": "users:001:wallet"
  }' "$GRPC_HOST" ledger.v1.LedgerService/GetAccount

  echo ""
  echo "--- List Accounts ---"
  grpcurl -plaintext -d '{
    "ledger_id": "'"$LEDGER_ID"'",
    "page_size": 20
  }' "$GRPC_HOST" ledger.v1.LedgerService/ListAccounts

  echo ""
  echo "--- List Transactions ---"
  grpcurl -plaintext -d '{
    "ledger_id": "'"$LEDGER_ID"'",
    "page_size": 20
  }' "$GRPC_HOST" ledger.v1.LedgerService/ListTransactions
else
  echo ""
  echo "--- Skipping account/transaction reads (requires grpcurl) ---"
fi

# ─────────────────────────────────────────────
# 5. HOLDS (AUTH / CAPTURE / VOID)
# ─────────────────────────────────────────────
echo ""
echo "--- Fund user for hold tests ---"
$CTL submit post \
  --ledger "$LEDGER_ID" \
  --reference "fund-hold-test" \
  --posting "_world:users:hold-test:wallet:50000:USD/2"

echo ""
echo "--- Authorize Hold (25.00 USD) ---"
$CTL submit authorize \
  --ledger "$LEDGER_ID" \
  --source "users:hold-test:wallet" \
  --amount "2500" \
  --asset "USD/2" \
  --destination-hint "merchants:shop42" \
  --expires-at "2026-12-31T23:59:59Z" \
  -o json
echo "(Copy hold_id from above for capture/void below)"

if has_grpcurl; then
  echo ""
  echo "--- List Holds ---"
  grpcurl -plaintext -d '{
    "ledger_id": "'"$LEDGER_ID"'",
    "page_size": 20
  }' "$GRPC_HOST" ledger.v1.LedgerService/ListHolds
fi

echo ""
echo "--- Capture (replace <HOLD_ID>) ---"
echo "# $CTL submit capture --ledger $LEDGER_ID --hold-id <HOLD_ID> --amount 1500 --destination merchants:shop42"

echo ""
echo "--- Void Hold (replace <HOLD_ID>) ---"
echo "# $CTL submit void --ledger $LEDGER_ID --hold-id <HOLD_ID>"

# ─────────────────────────────────────────────
# 6. FX CONVERSION
# ─────────────────────────────────────────────
echo ""
echo "--- FX Conversion (USD -> EUR) ---"
$CTL submit convert \
  --ledger "$LEDGER_ID" \
  --reference "fx-001" \
  --source "users:001:wallet" \
  --destination "users:001:wallet-eur" \
  --src-amount "1000" \
  --src-asset "USD/2" \
  --dst-amount "920" \
  --dst-asset "EUR/2" \
  --rate "0.92" \
  --rate-source "ecb-daily" || true

# ─────────────────────────────────────────────
# 7. REVERT & AMEND
# ─────────────────────────────────────────────
echo ""
echo "--- Revert Transaction (replace <TX_ID>) ---"
echo "# $CTL submit revert --ledger $LEDGER_ID --tx-id <TX_ID> --reason 'Customer refund'"

echo ""
echo "--- Amend Transaction Metadata (replace <TX_ID>) ---"
echo "# $CTL submit amend --ledger $LEDGER_ID --tx-id <TX_ID> --metadata 'correction=Fixed invoice number'"

# ─────────────────────────────────────────────
# 8. METADATA
# ─────────────────────────────────────────────
echo ""
echo "--- Set Account Metadata ---"
$CTL submit set-metadata \
  --ledger "$LEDGER_ID" \
  --target-type account \
  --target-id "users:001:wallet" \
  --metadata "display_name=Alice Doe" \
  --metadata "kyc_verified=true"

echo ""
echo "--- Delete Account Metadata ---"
$CTL submit delete-metadata \
  --ledger "$LEDGER_ID" \
  --target-type account \
  --target-id "users:001:wallet" \
  --key "display_name"

# ─────────────────────────────────────────────
# 9. BATCH (requires grpcurl)
# ─────────────────────────────────────────────
if has_grpcurl; then
  echo ""
  echo "--- Batch (ALL_OR_NOTHING) ---"
  grpcurl -plaintext -d '{
    "intent": {
      "ledger_id": "'"$LEDGER_ID"'",
      "batch": {
        "mode": "ALL_OR_NOTHING",
        "intents": [
          {
            "ledger_id": "'"$LEDGER_ID"'",
            "post": {
              "postings": [{
                "source": "_world",
                "destination": "payroll:checking",
                "amount": "100000",
                "asset": "USD/2"
              }]
            }
          },
          {
            "ledger_id": "'"$LEDGER_ID"'",
            "post": {
              "postings": [{
                "source": "payroll:checking",
                "destination": "employees:alice",
                "amount": "50000",
                "asset": "USD/2"
              }]
            }
          }
        ]
      }
    }
  }' "$GRPC_HOST" ledger.v1.LedgerService/Submit
else
  echo ""
  echo "--- Skipping batch (requires grpcurl) ---"
fi

# ─────────────────────────────────────────────
# 10. LOG
# ─────────────────────────────────────────────
echo ""
echo "--- List Log Events ---"
$CTL log list --ledger "$LEDGER_ID" --page-size 50

echo ""
echo "--- Get Log Event (replace <EVENT_ID>) ---"
echo "# $CTL log get --ledger $LEDGER_ID --event-id <EVENT_ID>"

# ─────────────────────────────────────────────
# 11. VERIFY & SEAL (requires grpcurl)
# ─────────────────────────────────────────────
if has_grpcurl; then
  echo ""
  echo "--- Verify Batch (replace <BATCH_ID>) ---"
  echo '# grpcurl -plaintext -d '"'"'{"ledger_id": "'"$LEDGER_ID"'", "batch_id": "<BATCH_ID>"}'"'"' '$GRPC_HOST' ledger.v1.LedgerService/VerifyBatch'

  echo ""
  echo "--- Seal Ledger (uncomment to run - destructive!) ---"
  echo '# grpcurl -plaintext -d '"'"'{"id": "'"$LEDGER_ID"'"}'"'"' '$GRPC_HOST' ledger.v1.LedgerService/SealLedger'
fi

echo ""
echo "=== Done ==="
