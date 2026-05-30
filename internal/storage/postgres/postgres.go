package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/remade/ledger/internal/config"
	"github.com/remade/ledger/internal/storage"
	"github.com/remade/ledger/migrations"
)

// DB wraps a pgxpool connection and provides migration support.
type DB struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

// New creates a new Postgres connection pool.
func New(lc fx.Lifecycle, cfg config.PostgresConfig, logger *zap.Logger) (*DB, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parsing postgres dsn: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}

	db := &DB{pool: pool, logger: logger.Named("postgres")}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := pool.Ping(ctx); err != nil {
				return fmt.Errorf("pinging postgres: %w", err)
			}
			db.logger.Info("postgres connected")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			pool.Close()
			db.logger.Info("postgres disconnected")
			return nil
		},
	})

	return db, nil
}

// Pool returns the underlying connection pool.
func (db *DB) Pool() *pgxpool.Pool {
	return db.pool
}

// MigrateUp runs all pending migrations in ascending order, skipping already-applied ones.
func (db *DB) MigrateUp(ctx context.Context) error {
	// Ensure tracking table exists.
	if _, err := db.pool.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS _ledger_schema_migrations (
			filename TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("creating schema_migrations table: %w", err)
	}

	upFiles := []string{
		"001_init.up.sql",
		"010_holds.up.sql",
		"020_governance.up.sql",
		"030_approval_executing_at.up.sql",
	}

	for _, file := range upFiles {
		// Check if already applied.
		var exists bool
		if err := db.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM _ledger_schema_migrations WHERE filename = $1)`,
			file).Scan(&exists); err != nil {
			return fmt.Errorf("checking migration status %s: %w", file, err)
		}
		if exists {
			db.logger.Debug("migration already applied, skipping", zap.String("file", file))
			continue
		}

		content, err := migrations.FS.ReadFile(file)
		if err != nil {
			return fmt.Errorf("reading migration file %s: %w", file, err)
		}

		// Run migration and record it in a single transaction.
		tx, err := db.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("beginning migration tx %s: %w", file, err)
		}

		if _, err := tx.Exec(ctx, string(content)); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("running migration %s: %w", file, err)
		}

		if _, err := tx.Exec(ctx,
			`INSERT INTO _ledger_schema_migrations (filename) VALUES ($1)`, file); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("recording migration %s: %w", file, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("committing migration %s: %w", file, err)
		}

		db.logger.Info("migration applied", zap.String("file", file))
	}

	db.logger.Info("all migrations applied")
	return nil
}

// MigrateDown reverses all migrations in descending order.
func (db *DB) MigrateDown(ctx context.Context) error {
	downFiles := []string{
		"030_approval_executing_at.down.sql",
		"020_governance.down.sql",
		"010_holds.down.sql",
		"001_init.down.sql",
	}

	for _, file := range downFiles {
		content, err := migrations.FS.ReadFile(file)
		if err != nil {
			return fmt.Errorf("reading migration file %s: %w", file, err)
		}

		_, err = db.pool.Exec(ctx, string(content))
		if err != nil {
			return fmt.Errorf("running migration rollback %s: %w", file, err)
		}

		db.logger.Info("migration rolled back", zap.String("file", file))
	}

	db.logger.Info("all migrations rolled back")
	return nil
}

// NewStandalone creates a DB without fx lifecycle management.
// The caller is responsible for calling Close().
func NewStandalone(dsn string) (*DB, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing postgres dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}
	logger, err := zap.NewProduction()
	if err != nil {
		logger = zap.NewNop()
	}
	return &DB{pool: pool, logger: logger.Named("postgres")}, nil
}

// Close closes the connection pool.
func (db *DB) Close() {
	db.pool.Close()
}

// Module provides the Postgres DB and Store to the fx container.
var Module = fx.Module("postgres",
	fx.Provide(New),
	fx.Provide(NewStore),
	fx.Provide(func(s *Store) storage.Store { return s }),
)
