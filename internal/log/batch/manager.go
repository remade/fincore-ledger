package batch

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/remade/ledger/internal/log/merkle"
	"github.com/remade/ledger/internal/storage"
)

const (
	// DefaultMaxEvents is the max events per batch before auto-close.
	DefaultMaxEvents = 1000
	// DefaultMaxAge is the max time a batch stays open.
	DefaultMaxAge = 5 * time.Second

	// maxCloseRetries is the number of additional close attempts after the first
	// before a batch is logged as a critical orphan (left for the next sweep).
	maxCloseRetries = 3
	// initialCloseBackoff / maxCloseBackoff bound the exponential retry backoff.
	initialCloseBackoff = 50 * time.Millisecond
	maxCloseBackoff     = 5 * time.Second
)

// Manager manages Merkle batch lifecycle per ledger.
type Manager struct {
	store     storage.Store
	logger    *zap.Logger
	maxEvents int
	maxAge    time.Duration

	mu          sync.Mutex                 // protects ledgerLocks, openBatch, pendingClose
	ledgerLocks map[string]*sync.Mutex     // per-ledger lock for CreateBatch I/O
	openBatch   map[string]*openBatchState // ledgerID -> state
	// pendingClose tracks batch IDs whose close is in flight, so the async close
	// and the orphan sweep never attempt the same batch concurrently.
	pendingClose map[string]bool

	// shutdownCtx is cancelled when the application stops, ensuring async
	// batch close goroutines do not outlive the connection pool.
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

type openBatchState struct {
	batchID    string
	openedAt   time.Time
	eventCount int
}

// NewManager creates a new batch Manager.
func NewManager(lc fx.Lifecycle, store storage.Store, logger *zap.Logger) *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		store:          store,
		logger:         logger.Named("batch"),
		maxEvents:      DefaultMaxEvents,
		maxAge:         DefaultMaxAge,
		ledgerLocks:    make(map[string]*sync.Mutex),
		openBatch:      make(map[string]*openBatchState),
		pendingClose:   make(map[string]bool),
		shutdownCtx:    ctx,
		shutdownCancel: cancel,
	}
	lc.Append(fx.Hook{
		OnStop: func(_ context.Context) error {
			cancel()
			return nil
		},
	})
	return m
}

// CurrentBatchID returns the current open batch ID for a ledger, creating one if needed.
func (m *Manager) CurrentBatchID(ctx context.Context, ledgerID string) (string, error) {
	// Acquire a per-ledger lock so different ledgers don't block each other
	// during CreateBatch I/O.
	lmu := m.getLedgerLock(ledgerID)
	lmu.Lock()
	defer lmu.Unlock()

	m.mu.Lock()
	state, ok := m.openBatch[ledgerID]
	if ok && state.eventCount < m.maxEvents && time.Since(state.openedAt) < m.maxAge {
		state.eventCount++
		m.mu.Unlock()
		return state.batchID, nil
	}
	var oldBatchID string
	if ok {
		oldBatchID = state.batchID
	}
	m.mu.Unlock()

	// Create batch outside global lock (only per-ledger lock held).
	batchID := ulid.Make().String()
	now := time.Now().UTC()
	if err := m.store.CreateBatch(ctx, storage.BatchRecord{
		BatchID:     batchID,
		LedgerID:    ledgerID,
		OpenedAt:    now,
		PrevBatchID: oldBatchID,
	}); err != nil {
		return "", fmt.Errorf("creating batch: %w", err)
	}

	// Update state under global lock.
	m.mu.Lock()
	m.openBatch[ledgerID] = &openBatchState{
		batchID:    batchID,
		openedAt:   now,
		eventCount: 1,
	}
	m.mu.Unlock()

	// Close old batch async if there was one. The batch stays open in storage
	// until the close commits. Invariant: every batch evicted from openBatch is
	// guaranteed to re-surface via ListOpenBatches once opened_at + maxAge
	// elapses -- the sole backstop if this async close exhausts its retries
	// (notably for max-events rollovers, which may not yet be age-eligible).
	if oldBatchID != "" {
		closeCtx, closeCancel := context.WithTimeout(m.shutdownCtx, 30*time.Second)
		go func() {
			defer closeCancel()
			m.closeBatchWithRetry(closeCtx, ledgerID, oldBatchID)
		}()
	}

	return batchID, nil
}

// markPendingClose claims a batch for closing; it returns false if another
// goroutine is already closing it (so the caller should skip).
func (m *Manager) markPendingClose(batchID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pendingClose[batchID] {
		return false
	}
	m.pendingClose[batchID] = true
	return true
}

func (m *Manager) unmarkPendingClose(batchID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pendingClose, batchID)
}

// closeBatchWithRetry closes a batch with bounded exponential backoff. The batch
// stays open in storage until the close commits, and is held in pendingClose for
// the duration so a concurrent sweep won't race it. If all attempts fail, it
// emits a critical, alertable log and leaves the batch open for a later sweep to
// retry (preserving Merkle-chain integrity over silent data loss).
func (m *Manager) closeBatchWithRetry(ctx context.Context, ledgerID, batchID string) {
	if !m.markPendingClose(batchID) {
		return
	}
	defer m.unmarkPendingClose(batchID)

	backoff := initialCloseBackoff
	for attempt := 0; ; attempt++ {
		err := m.closeBatch(ctx, ledgerID, batchID)
		if err == nil {
			return
		}
		if attempt >= maxCloseRetries {
			m.logger.Error("CRITICAL: batch close failed after retries; batch left open for the next sweep -- Merkle chain integrity at risk",
				zap.String("severity", "critical"),
				zap.String("batch", batchID),
				zap.String("ledger", ledgerID),
				zap.Int("attempts", attempt+1),
				zap.Error(err),
			)
			return
		}
		// Backoff with +/-50% jitter, abortable on shutdown/timeout. No time.Sleep.
		jitter := time.Duration(rand.Int64N(int64(backoff)))
		select {
		case <-ctx.Done():
			m.logger.Warn("batch close retry aborted",
				zap.String("batch", batchID), zap.Int("attempts", attempt+1), zap.Error(ctx.Err()))
			return
		case <-time.After(backoff + jitter):
		}
		if backoff *= 2; backoff > maxCloseBackoff {
			backoff = maxCloseBackoff
		}
	}
}

// getLedgerLock returns the per-ledger mutex, creating one if needed.
func (m *Manager) getLedgerLock(ledgerID string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	lmu, ok := m.ledgerLocks[ledgerID]
	if !ok {
		lmu = &sync.Mutex{}
		m.ledgerLocks[ledgerID] = lmu
	}
	return lmu
}

// closeBatch closes a batch by computing its Merkle root. It returns an error
// on failure so the caller can retry; an empty batch is a no-op (not an error).
func (m *Manager) closeBatch(ctx context.Context, ledgerID, batchID string) error {
	events, err := m.store.ListBatchEvents(ctx, batchID)
	if err != nil {
		return fmt.Errorf("listing batch events: %w", err)
	}

	if len(events) == 0 {
		m.logger.Debug("empty batch, skipping close", zap.String("batch", batchID))
		return nil
	}

	leaves := make([][]byte, len(events))
	for i, e := range events {
		typeBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(typeBytes, uint16(e.Type))
		leaves[i] = merkle.LeafHash(e.EventID, []byte(e.SystemTime.Format(time.RFC3339Nano)), typeBytes, e.Payload)
	}

	root := merkle.ComputeRoot(leaves)

	if err := m.store.CloseBatch(ctx, batchID, root, len(events)); err != nil {
		return fmt.Errorf("closing batch: %w", err)
	}

	m.logger.Info("batch closed",
		zap.String("batch", batchID),
		zap.String("ledger", ledgerID),
		zap.Int("events", len(events)),
	)
	return nil
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
		m.closeBatchWithRetry(ctx, item.ledgerID, item.batchID)
	}

	// Also scan PG for orphaned open batches (e.g., from a previous process crash
	// or a close that exhausted its retries). closeBatchWithRetry skips any batch
	// already being closed elsewhere.
	orphaned, err := m.store.ListOpenBatches(ctx, m.maxAge)
	if err != nil {
		m.logger.Error("failed to list orphaned open batches", zap.Error(err))
		return
	}
	for _, b := range orphaned {
		m.closeBatchWithRetry(ctx, b.LedgerID, b.BatchID)
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
