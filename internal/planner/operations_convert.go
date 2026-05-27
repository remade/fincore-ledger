package planner

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"

	"github.com/remade/ledger/internal/storage"
	"github.com/remade/ledger/pkg/accounts"
	"github.com/remade/ledger/pkg/assets"
)

// ConvertParams holds the parameters for a conversion operation.
type ConvertParams struct {
	Source          string
	Destination     string
	SourceAmount    *big.Int
	SourceAsset     string
	DestAmount      *big.Int
	DestAsset       string
	Rate            string
	RateSource      string
	SlippageAccount string
	SlippageAmount  *big.Int
	ValidTime       *time.Time
}

// SubmitConvert handles an FX ConvertOperation.
func (p *Planner) SubmitConvert(ctx context.Context, ledgerID string, params ConvertParams, idempotencyKey string) (*SubmitResult, error) {
	if err := accounts.Validate(params.Source); err != nil {
		return nil, fmt.Errorf("source: %w", err)
	}
	if err := accounts.Validate(params.Destination); err != nil {
		return nil, fmt.Errorf("destination: %w", err)
	}
	if err := assets.Validate(params.SourceAsset); err != nil {
		return nil, fmt.Errorf("source_asset: %w", err)
	}
	if err := assets.Validate(params.DestAsset); err != nil {
		return nil, fmt.Errorf("destination_asset: %w", err)
	}
	if params.SlippageAccount != "" {
		if err := accounts.Validate(params.SlippageAccount); err != nil {
			return nil, fmt.Errorf("slippage_account: %w", err)
		}
	}
	if params.SourceAmount == nil || params.SourceAmount.Sign() <= 0 {
		return nil, fmt.Errorf("source_amount must be positive")
	}
	if params.DestAmount == nil || params.DestAmount.Sign() <= 0 {
		return nil, fmt.Errorf("destination_amount must be positive")
	}
	if params.SourceAsset == params.DestAsset {
		return nil, fmt.Errorf("source_asset and destination_asset must differ for conversion")
	}
	if params.Rate != "" {
		if _, _, err := big.ParseFloat(params.Rate, 10, 128, big.ToNearestEven); err != nil {
			return nil, fmt.Errorf("invalid conversion rate %q: %w", params.Rate, err)
		}
	}

	ledger, err := p.checkLedgerNotSealed(ctx, ledgerID)
	if err != nil {
		return nil, err
	}

	var ikHash []byte
	if idempotencyKey != "" {
		ikHash = computeGenericIdempotencyHash(ledgerID, "convert", params.Source, params.Destination,
			params.SourceAmount.String(), params.SourceAsset, params.DestAmount.String(), params.DestAsset,
			params.Rate, params.RateSource)
		if result, err := p.checkIdempotency(ctx, ledgerID, idempotencyKey, ikHash); err != nil {
			return nil, err
		} else if result != nil {
			return result, nil
		}
	}

	now := time.Now().UTC()
	vt := now
	if params.ValidTime != nil {
		vt = *params.ValidTime
	}

	var result *SubmitResult
	err = withDeadlockRetry(ctx, 5, func() error {
		eventID := ulid.Make().String()
		conversionID := ulid.Make().String()

		batchID, err := p.batch.CurrentBatchID(ctx, ledgerID)
		if err != nil {
			return fmt.Errorf("getting batch ID: %w", err)
		}

		txStore, err := p.store.BeginTx(ctx)
		if err != nil {
			return err
		}
		defer txStore.Rollback()

		seq, err := txStore.NextLedgerSeq(ctx, ledgerID)
		if err != nil {
			return err
		}

		// Balance check inside TX to prevent TOCTOU.
		// Total source output includes slippage when present.
		if !accounts.IsIssuer(params.Source, ledger.IssuerAccounts) {
			bal, err := txStore.GetBalance(ctx, ledgerID, params.Source, params.SourceAsset, nil, nil)
			if err != nil {
				return err
			}
			current := new(big.Int).Sub(bal.Input, bal.Output)

			activeHolds, err := txStore.GetActiveHoldsTotal(ctx, ledgerID, params.Source, params.SourceAsset)
			if err != nil {
				return fmt.Errorf("getting active holds for %s/%s: %w", params.Source, params.SourceAsset, err)
			}
			current.Sub(current, activeHolds)

			totalSourceOutput := new(big.Int).Set(params.SourceAmount)
			if params.SlippageAmount != nil && params.SlippageAmount.Sign() > 0 {
				totalSourceOutput.Add(totalSourceOutput, params.SlippageAmount)
			}

			if current.Cmp(totalSourceOutput) < 0 {
				return fmt.Errorf("%w: account %s, asset %s", storage.ErrInsufficientFunds, params.Source, params.SourceAsset)
			}
		}

		payload, err := json.Marshal(map[string]string{
			"conversion_id":      conversionID,
			"source":             params.Source,
			"destination":        params.Destination,
			"source_amount":      params.SourceAmount.String(),
			"source_asset":       params.SourceAsset,
			"destination_amount": params.DestAmount.String(),
			"destination_asset":  params.DestAsset,
			"rate":               params.Rate,
			"rate_source":        params.RateSource,
		})
		if err != nil {
			return fmt.Errorf("marshaling payload: %w", err)
		}

		if err := txStore.AppendLogEvent(ctx, storage.LogEventRecord{
			EventID: eventID, LedgerID: ledgerID, LedgerSeq: seq,
			SystemTime: now, ValidTime: vt, Type: 6,
			Payload: payload, IdempotencyKey: idempotencyKey,
			IdempotencyHash: ikHash,
			BatchID: batchID, SchemaVersion: 1,
		}); err != nil {
			return err
		}

		// Volume deltas maintain per-asset zero-sum (double-entry invariant):
		// Leg 1 (source_asset): source -> destination
		// Leg 2 (dest_asset):   destination -> source
		// Optional: slippage in source_asset from source -> slippage_account

		// Leg 1: source sends source_asset to destination.
		if err := txStore.InsertVolumeDelta(ctx, storage.VolumeDeltaRecord{
			LedgerID: ledgerID, Account: params.Source, Asset: params.SourceAsset,
			EventID: eventID, ValidTime: vt, SystemTime: now,
			InputDelta: big.NewInt(0), OutputDelta: params.SourceAmount,
		}); err != nil {
			return err
		}
		if err := txStore.InsertVolumeDelta(ctx, storage.VolumeDeltaRecord{
			LedgerID: ledgerID, Account: params.Destination, Asset: params.SourceAsset,
			EventID: eventID, ValidTime: vt, SystemTime: now,
			InputDelta: params.SourceAmount, OutputDelta: big.NewInt(0),
		}); err != nil {
			return err
		}

		// Leg 2: destination sends dest_asset to source.
		if err := txStore.InsertVolumeDelta(ctx, storage.VolumeDeltaRecord{
			LedgerID: ledgerID, Account: params.Destination, Asset: params.DestAsset,
			EventID: eventID, ValidTime: vt, SystemTime: now,
			InputDelta: big.NewInt(0), OutputDelta: params.DestAmount,
		}); err != nil {
			return err
		}
		if err := txStore.InsertVolumeDelta(ctx, storage.VolumeDeltaRecord{
			LedgerID: ledgerID, Account: params.Source, Asset: params.DestAsset,
			EventID: eventID, ValidTime: vt, SystemTime: now,
			InputDelta: params.DestAmount, OutputDelta: big.NewInt(0),
		}); err != nil {
			return err
		}

		// Optional slippage leg: source sends slippage in source_asset to slippage account.
		if params.SlippageAmount != nil && params.SlippageAmount.Sign() > 0 && params.SlippageAccount != "" {
			if err := txStore.InsertVolumeDelta(ctx, storage.VolumeDeltaRecord{
				LedgerID: ledgerID, Account: params.Source, Asset: params.SourceAsset,
				EventID: eventID, ValidTime: vt, SystemTime: now,
				InputDelta: big.NewInt(0), OutputDelta: params.SlippageAmount,
			}); err != nil {
				return err
			}
			if err := txStore.InsertVolumeDelta(ctx, storage.VolumeDeltaRecord{
				LedgerID: ledgerID, Account: params.SlippageAccount, Asset: params.SourceAsset,
				EventID: eventID, ValidTime: vt, SystemTime: now,
				InputDelta: params.SlippageAmount, OutputDelta: big.NewInt(0),
			}); err != nil {
				return err
			}
			if err := txStore.UpsertAccount(ctx, storage.AccountRecord{
				LedgerID: ledgerID, Address: params.SlippageAccount, FirstUsage: now, UpdatedAt: now, Metadata: map[string]any{},
			}); err != nil {
				return fmt.Errorf("upserting account: %w", err)
			}
		}

		if err := txStore.UpsertAccount(ctx, storage.AccountRecord{
			LedgerID: ledgerID, Address: params.Source, FirstUsage: now, UpdatedAt: now, Metadata: map[string]any{},
		}); err != nil {
			return fmt.Errorf("upserting account: %w", err)
		}
		if err := txStore.UpsertAccount(ctx, storage.AccountRecord{
			LedgerID: ledgerID, Address: params.Destination, FirstUsage: now, UpdatedAt: now, Metadata: map[string]any{},
		}); err != nil {
			return fmt.Errorf("upserting account: %w", err)
		}

		// Transaction postings match the volume deltas for projection consistency.
		// Leg 1: source_asset flows source -> destination.
		// Leg 2: dest_asset flows destination -> source.
		txID := ulid.Make().String()
		postingRecords := []map[string]any{
			{"source": params.Source, "destination": params.Destination, "amount": params.SourceAmount.String(), "asset": params.SourceAsset},
			{"source": params.Destination, "destination": params.Source, "amount": params.DestAmount.String(), "asset": params.DestAsset},
		}
		if params.SlippageAmount != nil && params.SlippageAmount.Sign() > 0 && params.SlippageAccount != "" {
			postingRecords = append(postingRecords, map[string]any{
				"source": params.Source, "destination": params.SlippageAccount,
				"amount": params.SlippageAmount.String(), "asset": params.SourceAsset,
			})
		}
		if err := txStore.InsertTransaction(ctx, storage.TransactionRecord{
			LedgerID: ledgerID, TransactionID: txID, EventID: eventID,
			ValidTime: vt, SystemTime: now, Postings: postingRecords,
			Metadata: map[string]any{"type": "conversion", "rate": params.Rate, "rate_source": params.RateSource},
		}); err != nil {
			return fmt.Errorf("inserting conversion transaction: %w", err)
		}

		if idempotencyKey != "" {
			if err := p.recordIdempotency(ctx, txStore, ledgerID, idempotencyKey, eventID, ikHash); err != nil {
				return err
			}
		}

		if err := txStore.Commit(); err != nil {
			return err
		}

		if idempotencyKey != "" {
			p.postCommitIdempotency(ctx, ledgerID, idempotencyKey, eventID, ikHash)
		}

		p.publishEvent(ctx, ledgerID, eventID, 6)

		p.logger.Debug("conversion recorded",
			zap.String("conversion_id", conversionID),
			zap.String("source_asset", params.SourceAsset),
			zap.String("dest_asset", params.DestAsset),
			zap.String("rate", params.Rate),
		)

		result = &SubmitResult{EventID: eventID, ConversionID: conversionID}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
