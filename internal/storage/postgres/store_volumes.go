package postgres

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/remade/ledger/internal/storage"
)

// --- Volumes ---

func (q *queries) InsertVolumeDelta(ctx context.Context, delta storage.VolumeDeltaRecord) error {
	_, err := q.db.Exec(ctx,
		`INSERT INTO "_default".volumes_delta
		 (ledger_id, account, asset, shard, event_id, valid_time, system_time, input_delta, output_delta)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		delta.LedgerID, delta.Account, delta.Asset, delta.Shard,
		delta.EventID, delta.ValidTime, delta.SystemTime,
		delta.InputDelta.String(), delta.OutputDelta.String(),
	)
	return err
}

func (q *queries) GetBalance(ctx context.Context, ledgerID, account, asset string, asOfValid, asOfSystem *time.Time) (*storage.BalanceResult, error) {
	query := `SELECT COALESCE(SUM(input_delta), 0), COALESCE(SUM(output_delta), 0)
		 FROM "_default".volumes_delta
		 WHERE ledger_id = $1 AND account = $2 AND asset = $3`
	args := []any{ledgerID, account, asset}
	argIdx := 4

	if asOfValid != nil {
		query += fmt.Sprintf(` AND valid_time <= $%d`, argIdx)
		args = append(args, *asOfValid)
		argIdx++
	}
	if asOfSystem != nil {
		query += fmt.Sprintf(` AND system_time <= $%d`, argIdx)
		args = append(args, *asOfSystem)
	}

	var inputStr, outputStr string
	err := q.db.QueryRow(ctx, query, args...).Scan(&inputStr, &outputStr)
	if err != nil {
		return nil, fmt.Errorf("getting balance: %w", err)
	}

	input, err := parseBigInt(inputStr, "input_delta")
	if err != nil {
		return nil, err
	}
	output, err := parseBigInt(outputStr, "output_delta")
	if err != nil {
		return nil, err
	}

	return &storage.BalanceResult{Input: input, Output: output}, nil
}

// GetAggregatedBalances returns net balances (input - output) per asset across all
// accounts matching the given address pattern. If addressPattern is empty, all accounts are included.
func (q *queries) GetAggregatedBalances(ctx context.Context, ledgerID, addressPattern string, asOfValid, asOfSystem *time.Time) (map[string]*big.Int, error) {
	query := `SELECT asset, COALESCE(SUM(input_delta), 0), COALESCE(SUM(output_delta), 0)
		 FROM "_default".volumes_delta
		 WHERE ledger_id = $1`
	args := []any{ledgerID}
	argIdx := 2

	if addressPattern != "" {
		// Convert glob-style pattern (e.g., "users:*") to SQL LIKE pattern.
		// Escape SQL LIKE metacharacters before converting our glob wildcard.
		likePattern := strings.ReplaceAll(addressPattern, `\`, `\\`)
		likePattern = strings.ReplaceAll(likePattern, "%", `\%`)
		likePattern = strings.ReplaceAll(likePattern, "_", `\_`)
		likePattern = strings.ReplaceAll(likePattern, "*", "%")
		query += fmt.Sprintf(` AND account LIKE $%d ESCAPE '\'`, argIdx)
		args = append(args, likePattern)
		argIdx++
	}
	if asOfValid != nil {
		query += fmt.Sprintf(` AND valid_time <= $%d`, argIdx)
		args = append(args, *asOfValid)
		argIdx++
	}
	if asOfSystem != nil {
		query += fmt.Sprintf(` AND system_time <= $%d`, argIdx)
		args = append(args, *asOfSystem)
	}
	query += ` GROUP BY asset`

	rows, err := q.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getting aggregated balances: %w", err)
	}
	defer rows.Close()

	result := make(map[string]*big.Int)
	for rows.Next() {
		var asset, inputStr, outputStr string
		if err := rows.Scan(&asset, &inputStr, &outputStr); err != nil {
			return nil, err
		}
		input, err := parseBigInt(inputStr, "input_delta")
		if err != nil {
			return nil, err
		}
		output, err := parseBigInt(outputStr, "output_delta")
		if err != nil {
			return nil, err
		}
		result[asset] = new(big.Int).Sub(input, output)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating aggregated balances: %w", err)
	}
	return result, nil
}
