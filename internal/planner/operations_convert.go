package planner

import (
	"context"
	"math/big"
	"time"

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
	}

	now := time.Now().UTC()
	vt := now
	if params.ValidTime != nil {
		vt = *params.ValidTime
	}

	return p.submitWithIdempotency(ctx, ledgerID, idempotencyKey, ikHash, storage.EventTypeConversionCreated, false,
		func(txStore storage.TxStore) (*SubmitResult, error) {
			return p.doConvertInTx(ctx, txStore, ledger, params, idempotencyKey, ikHash, vt, now)
		})
}
