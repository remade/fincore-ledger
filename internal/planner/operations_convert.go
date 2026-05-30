package planner

import (
	"context"
	"math/big"
	"time"

	"go.uber.org/zap"

	"github.com/remade/ledger/internal/storage"
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

// SubmitConvert handles an FX ConvertOperation. The in-transaction core is
// shared with the batch path via doConvertInTx; this method owns the transaction
// lifecycle, deadlock retry, two-tier idempotency, and publish.
func (p *Planner) SubmitConvert(ctx context.Context, ledgerID string, params ConvertParams, idempotencyKey string) (*SubmitResult, error) {
	// Validate up front so computeGenericIdempotencyHash never sees a nil amount.
	if err := validateConvertParams(params); err != nil {
		return nil, err
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
		txStore, err := p.store.BeginTx(ctx)
		if err != nil {
			return err
		}
		defer txStore.Rollback()

		r, err := p.doConvertInTx(ctx, txStore, ledger, params, idempotencyKey, ikHash, vt, now)
		if err != nil {
			return err
		}
		if err := txStore.Commit(); err != nil {
			return err
		}
		if idempotencyKey != "" {
			p.postCommitIdempotency(ctx, ledgerID, idempotencyKey, r.EventID, ikHash)
		}
		p.publishEvent(ctx, ledgerID, r.EventID, storage.EventTypeConversionCreated)
		p.logger.Debug("conversion recorded",
			zap.String("conversion_id", r.ConversionID),
			zap.String("source_asset", params.SourceAsset),
			zap.String("dest_asset", params.DestAsset),
			zap.String("rate", params.Rate),
		)
		result = r
		return nil
	})
	if err != nil {
		if idempotencyKey != "" && isIdempotencyConflict(err) {
			return p.resolveIdempotencyConflict(ctx, ledgerID, idempotencyKey)
		}
		return nil, err
	}
	return result, nil
}
