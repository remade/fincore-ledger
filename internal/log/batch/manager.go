package batch

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"

	"github.com/remade/ledger/internal/log/merkle"
	"github.com/remade/ledger/internal/storage"
)

const (
	// DefaultMaxEvents is the max events per batch before auto-close.
	DefaultMaxEvents = 1000
	// DefaultMaxAge is the max time a batch stays open.
	DefaultMaxAge = 5 * time.Second
)

// Manager manages Merkle batch lifecycle per ledger.
type Manager struct {
	store      storage.Store
	logger     *zap.Logger
	maxEvents  int
	maxAge     time.Duration

	mu         sync.Mutex
	openBatch  map[string]*openBatchState // ledgerID -> state
}

type openBatchState struct {
	batchID    string
	openedAt   time.Time
	eventCount int
}

// NewManager creates a new batch Manager.
func NewManager(store storage.Store, logger *zap.Logger) *Manager {
	return &Manager{
		store:     store,
		logger:    logger.Named("batch"),
		maxEvents: DefaultMaxEvents,
		maxAge:    DefaultMaxAge,
		openBatch: make(map[string]*openBatchState),
	}
}

// CurrentBatchID returns the current open batch ID for a ledger, creating one if needed.
func (m *Manager) CurrentBatchID(ctx context.Context, ledgerID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if state, ok := m.openBatch[ledgerID]; ok {
		if state.eventCount < m.maxEvents && time.Since(state.openedAt) < m.maxAge {
			state.eventCount++
			return state.batchID, nil
		}
		// Batch is full or expired — create new batch first, then close old one async.
		oldBatchID := state.batchID

		// Create new batch linked to the old one before removing old state.
		// If creation fails, old batch state is preserved.
		batchID := ulid.Make().String()
		if err := m.store.CreateBatch(ctx, storage.BatchRecord{
			BatchID:     batchID,
			LedgerID:    ledgerID,
			OpenedAt:    time.Now().UTC(),
			PrevBatchID: oldBatchID,
		}); err != nil {
			return "", fmt.Errorf("creating batch: %w", err)
		}

		// New batch created successfully — now safe to remove old state and close async.
		delete(m.openBatch, ledgerID)

		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		go func() {
			defer closeCancel()
			m.closeBatch(closeCtx, ledgerID, oldBatchID)
		}()

		m.openBatch[ledgerID] = &openBatchState{
			batchID:    batchID,
			openedAt:   time.Now().UTC(),
			eventCount: 1,
		}
		return batchID, nil
	}

	// No open batch — create one fresh (lock held to prevent duplicates).
	batchID := ulid.Make().String()
	if err := m.store.CreateBatch(ctx, storage.BatchRecord{
		BatchID:  batchID,
		LedgerID: ledgerID,
		OpenedAt: time.Now().UTC(),
	}); err != nil {
		return "", fmt.Errorf("creating batch: %w", err)
	}

	m.openBatch[ledgerID] = &openBatchState{
		batchID:    batchID,
		openedAt:   time.Now().UTC(),
		eventCount: 1,
	}
	return batchID, nil
}

// closeBatch closes a batch by computing its Merkle root.
func (m *Manager) closeBatch(ctx context.Context, ledgerID, batchID string) {
	events, err := m.store.ListBatchEvents(ctx, batchID)
	if err != nil {
		m.logger.Error("failed to list batch events", zap.Error(err), zap.String("batch", batchID))
		return
	}

	if len(events) == 0 {
		m.logger.Debug("empty batch, skipping close", zap.String("batch", batchID))
		return
	}

	leaves := make([][]byte, len(events))
	for i, e := range events {
		typeBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(typeBytes, uint16(e.Type))
		leaves[i] = merkle.LeafHash(e.EventID, []byte(e.SystemTime.Format(time.RFC3339Nano)), typeBytes, e.Payload)
	}

	root := merkle.ComputeRoot(leaves)

	if err := m.store.CloseBatch(ctx, batchID, root, len(events)); err != nil {
		m.logger.Error("failed to close batch", zap.Error(err), zap.String("batch", batchID))
		return
	}

	m.logger.Info("batch closed",
		zap.String("batch", batchID),
		zap.String("ledger", ledgerID),
		zap.Int("events", len(events)),
	)
}

// CloseExpiredBatches scans for open batches past their max age and closes them.
// It checks both the in-memory map and PG for orphaned batches from previous processes.
// This is called periodically by the worker.
func (m *Manager) CloseExpiredBatches(ctx context.Context) {
	m.mu.Lock()
	var toClose []struct{ ledgerID, batchID string }
	for ledgerID, state := range m.openBatch {
		if time.Since(state.openedAt) >= m.maxAge {
			toClose = append(toClose, struct{ ledgerID, batchID string }{ledgerID, state.batchID})
			delete(m.openBatch, ledgerID)
		}
	}
	m.mu.Unlock()

	for _, item := range toClose {
		m.closeBatch(ctx, item.ledgerID, item.batchID)
	}

	// Also scan PG for orphaned open batches (e.g., from a previous process crash).
	orphaned, err := m.store.ListOpenBatches(ctx, m.maxAge)
	if err != nil {
		m.logger.Error("failed to list orphaned open batches", zap.Error(err))
		return
	}
	for _, b := range orphaned {
		m.closeBatch(ctx, b.LedgerID, b.BatchID)
	}
}

// VerifyBatch recomputes the Merkle root for a closed batch and compares.
func (m *Manager) VerifyBatch(ctx context.Context, batchID string) (bool, []byte, int, error) {
	batch, err := m.store.GetBatch(ctx, batchID)
	if err != nil {
		return false, nil, 0, err
	}

	if batch.MerkleRoot == nil {
		return false, nil, 0, fmt.Errorf("batch %q is not yet closed", batchID)
	}

	events, err := m.store.ListBatchEvents(ctx, batchID)
	if err != nil {
		return false, nil, 0, err
	}

	leaves := make([][]byte, len(events))
	for i, e := range events {
		typeBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(typeBytes, uint16(e.Type))
		leaves[i] = merkle.LeafHash(e.EventID, []byte(e.SystemTime.Format(time.RFC3339Nano)), typeBytes, e.Payload)
	}

	valid := merkle.Verify(leaves, batch.MerkleRoot)
	return valid, batch.MerkleRoot, len(events), nil
}
