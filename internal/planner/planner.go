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

// batchManager supplies the current open Merkle batch for a ledger. Defined
// here (consumer side) so the planner can be unit-tested with a fake.
type batchManager interface {
	CurrentBatchID(ctx context.Context, ledgerID string) (string, error)
}

// idempotencyCache is the L1 (Redis) side of the two-tier idempotency lookup.
type idempotencyCache interface {
	GetIdempotencyKey(ctx context.Context, ledgerID, key string) (eventID string, hash []byte, found bool, err error)
	SetIdempotencyKey(ctx context.Context, ledgerID, key, eventID string, hash []byte) error
}

// eventPublisher notifies subscribers about committed events.
type eventPublisher interface {
	Publish(ctx context.Context, notification subscriptions.EventNotification) error
}

// Planner is the single safe-write path. All state-changing operations flow through it.
type Planner struct {
	store     storage.Store
	batch     batchManager
	redis     idempotencyCache
	subs      eventPublisher
	evaluator policy.Evaluator
	logger    *zap.Logger
}

// New creates a new Planner. Concrete dependencies are accepted (so fx wiring is
// unchanged) and stored behind narrow interfaces for testability.
func New(store storage.Store, bm *batch.Manager, rc *redis.Client, subs *subscriptions.Manager, logger *zap.Logger) *Planner {
	p := &Planner{
		store:     store,
		batch:     bm,
		redis:     rc,
		evaluator: &policy.SimpleEvaluator{},
		logger:    logger.Named("planner"),
	}
	// Guard against the nil-interface trap: only store a non-nil concrete so the
	// `p.subs == nil` check in publishEvent stays correct.
	if subs != nil {
		p.subs = subs
	}
	return p
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
