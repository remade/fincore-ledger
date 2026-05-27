package planner

import (
	"context"
	"fmt"
	"math/big"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/remade/ledger/internal/log/batch"
	"github.com/remade/ledger/internal/policy"
	"github.com/remade/ledger/internal/storage"
	"github.com/remade/ledger/internal/storage/redis"
	"github.com/remade/ledger/internal/subscriptions"
)

// Planner is the single safe-write path. All state-changing operations flow through it.
type Planner struct {
	store     storage.Store
	batch     *batch.Manager
	redis     *redis.Client
	subs      *subscriptions.Manager
	evaluator policy.Evaluator
	logger    *zap.Logger
}

// New creates a new Planner.
func New(store storage.Store, bm *batch.Manager, rc *redis.Client, subs *subscriptions.Manager, logger *zap.Logger) *Planner {
	return &Planner{
		store:     store,
		batch:     bm,
		redis:     rc,
		subs:      subs,
		evaluator: &policy.SimpleEvaluator{},
		logger:    logger.Named("planner"),
	}
}

// publishEvent notifies subscribers about a newly committed event.
func (p *Planner) publishEvent(ctx context.Context, ledgerID, eventID string, eventType int16) {
	if p.subs == nil {
		return
	}
	if err := p.subs.Publish(ctx, subscriptions.EventNotification{
		LedgerID: ledgerID,
		EventID:  eventID,
		Type:     eventType,
	}); err != nil {
		p.logger.Warn("failed to publish event notification", zap.String("event_id", eventID), zap.Error(err))
	}
}

// checkLedgerNotSealed verifies the ledger exists and is not sealed.
func (p *Planner) checkLedgerNotSealed(ctx context.Context, ledgerID string) (*storage.LedgerRecord, error) {
	ledger, err := p.store.GetLedger(ctx, ledgerID)
	if err != nil {
		return nil, fmt.Errorf("getting ledger: %w", err)
	}
	if ledger.State == "sealed" {
		return nil, storage.ErrLedgerSealed
	}
	return ledger, nil
}

// SubmitResult is the output of a successful submission.
type SubmitResult struct {
	EventID       string
	IdempotentHit bool
	Transaction   *storage.TransactionRecord
	HoldID        string
	ConversionID  string
}

// PostingInput represents a single posting in a post operation.
type PostingInput struct {
	Source      string
	Destination string
	Amount      *big.Int
	Asset       string
}

// Module provides the Planner and batch Manager to the fx container.
var Module = fx.Module("planner",
	fx.Provide(
		New,
		batch.NewManager,
	),
)
