package volumes

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// CheckpointBuilder periodically rolls up volumes_delta rows into volumes_checkpoint
// for faster balance reads. The checkpoint stores the running total up to a watermark.
type CheckpointBuilder struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

// NewCheckpointBuilder creates a new CheckpointBuilder.
func NewCheckpointBuilder(pool *pgxpool.Pool, logger *zap.Logger) *CheckpointBuilder {
	return &CheckpointBuilder{
		pool:   pool,
		logger: logger.Named("checkpoint"),
	}
}

// BuildCheckpoints scans for accounts with enough uncheckpointed deltas and rolls them up.
func (cb *CheckpointBuilder) BuildCheckpoints(ctx context.Context) error {
	// Find (ledger, account, asset) combos with > 100 uncheckpointed deltas.
	rows, err := cb.pool.Query(ctx, `
		SELECT d.ledger_id, d.account, d.asset, d.shard,
		       COUNT(*) as delta_count,
		       COALESCE(SUM(d.input_delta), 0) as total_input,
		       COALESCE(SUM(d.output_delta), 0) as total_output,
		       MAX(d.valid_time) as max_valid_time,
		       MAX(d.system_time) as max_system_time,
		       MAX(d.event_id) as last_event_id
		FROM "_default".volumes_delta d
		LEFT JOIN "_default".volumes_checkpoint c
		  ON d.ledger_id = c.ledger_id
		  AND d.account = c.account
		  AND d.asset = c.asset
		  AND d.shard = c.shard
		  AND d.valid_time <= c.valid_time_upper
		  AND d.system_time <= c.system_time_upper
		WHERE c.ledger_id IS NULL
		GROUP BY d.ledger_id, d.account, d.asset, d.shard
		HAVING COUNT(*) > 100
		LIMIT 1000
	`)
	if err != nil {
		return fmt.Errorf("querying uncheckpointed volumes: %w", err)
	}
	defer rows.Close()

	var built, failed int
	for rows.Next() {
		var ledgerID, account, asset, lastEventID string
		var shard int16
		var deltaCount int
		var totalInputStr, totalOutputStr string
		var maxValidTime, maxSystemTime time.Time

		if err := rows.Scan(&ledgerID, &account, &asset, &shard,
			&deltaCount, &totalInputStr, &totalOutputStr,
			&maxValidTime, &maxSystemTime, &lastEventID); err != nil {
			return fmt.Errorf("scanning row: %w", err)
		}

		totalInput, ok := new(big.Int).SetString(totalInputStr, 10)
		if !ok {
			return fmt.Errorf("parsing total_input %q for %s/%s/%s: invalid integer", totalInputStr, ledgerID, account, asset)
		}
		totalOutput, ok := new(big.Int).SetString(totalOutputStr, 10)
		if !ok {
			return fmt.Errorf("parsing total_output %q for %s/%s/%s: invalid integer", totalOutputStr, ledgerID, account, asset)
		}

		_, err := cb.pool.Exec(ctx, `
			INSERT INTO "_default".volumes_checkpoint
			  (ledger_id, account, asset, shard, valid_time_upper, system_time_upper,
			   total_input, total_output, last_event_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (ledger_id, account, asset, shard, valid_time_upper, system_time_upper)
			DO UPDATE SET total_input = $7, total_output = $8, last_event_id = $9
		`,
			ledgerID, account, asset, shard,
			maxValidTime, maxSystemTime,
			totalInput.String(), totalOutput.String(), lastEventID,
		)
		if err != nil {
			failed++
			cb.logger.Error("failed to write checkpoint",
				zap.String("ledger_id", ledgerID),
				zap.String("account", account),
				zap.String("asset", asset),
				zap.Error(err),
			)
			continue
		}
		built++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating uncheckpointed volumes: %w", err)
	}

	if built > 0 {
		cb.logger.Info("checkpoints built", zap.Int("count", built))
	}
	if failed > 0 {
		return fmt.Errorf("failed to write %d of %d checkpoints", failed, built+failed)
	}
	return nil
}
