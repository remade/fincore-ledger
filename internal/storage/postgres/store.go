package postgres

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/remade/ledger/internal/storage"
)

const maxPageSize = 1000

// DBTX is the common interface satisfied by *pgxpool.Pool and pgx.Tx.
type DBTX interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// queries implements data-access methods against a DBTX.
// Both Store and TxStore embed this to share implementations.
type queries struct {
	db DBTX
}

// Store implements storage.Store against PostgreSQL.
type Store struct {
	pool *pgxpool.Pool
	queries
}

// TxStore wraps a pgx.Tx to implement storage.TxStore.
type TxStore struct {
	tx   pgx.Tx
	pool *pgxpool.Pool
	queries
	ctx context.Context
}

// Compile-time interface checks.
var _ storage.Store = (*Store)(nil)
var _ storage.TxStore = (*TxStore)(nil)

// NewStore creates a new Postgres-backed Store.
func NewStore(db *DB) *Store {
	pool := db.Pool()
	return &Store{pool: pool, queries: queries{db: pool}}
}

func (s *Store) Close() error                   { return nil }
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) BeginTx(ctx context.Context) (storage.TxStore, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning transaction: %w", err)
	}
	return &TxStore{tx: tx, pool: s.pool, queries: queries{db: tx}, ctx: ctx}, nil
}

func (t *TxStore) Close() error                   { return nil }
func (t *TxStore) Ping(ctx context.Context) error { return nil }
func (t *TxStore) Commit() error                  { return t.tx.Commit(t.ctx) }
func (t *TxStore) Rollback() error                { return t.tx.Rollback(t.ctx) }

func (t *TxStore) BeginTx(ctx context.Context) (storage.TxStore, error) {
	return nil, fmt.Errorf("nested transactions not supported")
}

// NextLedgerSeq gets the next sequence number within the transaction.
// It locks the ledger row to serialize sequence allocation across concurrent TXs.
func (t *TxStore) NextLedgerSeq(ctx context.Context, ledgerID string) (int64, error) {
	// Lock the ledger row to prevent concurrent TXs from computing the same seq.
	if _, err := t.tx.Exec(ctx,
		`SELECT 1 FROM _system.ledgers WHERE id = $1 FOR UPDATE`, ledgerID); err != nil {
		if isDeadlock(err) {
			return 0, storage.ErrDeadlock
		}
		return 0, fmt.Errorf("locking ledger for seq: %w", err)
	}
	var seq int64
	err := t.tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(ledger_seq), 0) + 1 FROM "_default".log_events WHERE ledger_id = $1`,
		ledgerID,
	).Scan(&seq)
	return seq, err
}

// --- helpers ---

// parseBigInt parses a numeric string from the database into a *big.Int.
func parseBigInt(s, field string) (*big.Int, error) {
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("invalid %s value: %q", field, s)
	}
	return v, nil
}

func isNoRows(err error) bool {
	return err != nil && errors.Is(err, pgx.ErrNoRows)
}

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func isDeadlock(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40P01"
}

func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
