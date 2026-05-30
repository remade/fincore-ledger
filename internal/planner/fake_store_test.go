package planner

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/remade/ledger/internal/storage"
)

// fakeStore is an in-memory storage.Store for unit-testing the planner. It
// implements the methods the operations exercise and embeds storage.Store so
// any unexpected call panics, keeping tests honest. Transactions buffer writes
// and apply them on Commit (discarded on Rollback); reads see committed state.
//
// This is a behavioral fake (not a call-recording mock): tests assert on the
// resulting state, which makes them robust to internal call reordering — the
// right tool for verifying a behavior-preserving refactor.
type fakeStore struct {
	storage.Store

	ledgers      map[string]*storage.LedgerRecord
	schemas      map[string]*storage.SchemaRecord // key: ledger|version
	seq          map[string]int64
	balances     map[string]*balance                      // key: ledger|account|asset
	holds        map[string]*big.Int                      // key: ledger|account|asset
	ikRecords    map[string]*storage.IdempotencyKeyRecord // key: ledger|key
	events       []storage.LogEventRecord
	eventIDs     map[string]bool // ledger|event_id
	ikSeen       map[string]bool // ledger|idempotency_key (uniqueness)
	txByRef      map[string]bool // ledger|reference (reference-conflict)
	transactions []storage.TransactionRecord
	accounts     map[string]bool // ledger|address
}

type balance struct{ input, output *big.Int }

func newFakeStore() *fakeStore {
	return &fakeStore{
		ledgers:   map[string]*storage.LedgerRecord{},
		schemas:   map[string]*storage.SchemaRecord{},
		seq:       map[string]int64{},
		balances:  map[string]*balance{},
		holds:     map[string]*big.Int{},
		ikRecords: map[string]*storage.IdempotencyKeyRecord{},
		eventIDs:  map[string]bool{},
		ikSeen:    map[string]bool{},
		txByRef:   map[string]bool{},
		accounts:  map[string]bool{},
	}
}

func balKey(ledger, account, asset string) string { return ledger + "|" + account + "|" + asset }

// --- helpers to set up state ---

func (s *fakeStore) addLedger(rec *storage.LedgerRecord) { s.ledgers[rec.ID] = rec }

func (s *fakeStore) setBalance(ledger, account, asset string, input, output int64) {
	s.balances[balKey(ledger, account, asset)] = &balance{big.NewInt(input), big.NewInt(output)}
}

// --- storage.Store reads (committed state) ---

func (s *fakeStore) GetLedger(_ context.Context, id string) (*storage.LedgerRecord, error) {
	if l, ok := s.ledgers[id]; ok {
		return l, nil
	}
	return nil, fmt.Errorf("%w: ledger %q", storage.ErrNotFound, id)
}

func (s *fakeStore) GetSchema(_ context.Context, ledgerID, version string) (*storage.SchemaRecord, error) {
	if sc, ok := s.schemas[ledgerID+"|"+version]; ok {
		return sc, nil
	}
	return nil, fmt.Errorf("%w: schema %q", storage.ErrNotFound, version)
}

func (s *fakeStore) GetIdempotencyKey(_ context.Context, ledgerID, key string) (*storage.IdempotencyKeyRecord, error) {
	if r, ok := s.ikRecords[ledgerID+"|"+key]; ok {
		return r, nil
	}
	return nil, nil
}

func (s *fakeStore) GetActivePolicy(context.Context, string) (*storage.PolicyRecord, error) {
	return nil, nil // no policy => unrestricted
}

func (s *fakeStore) getBalance(ledger, account, asset string) *balance {
	if b, ok := s.balances[balKey(ledger, account, asset)]; ok {
		return &balance{new(big.Int).Set(b.input), new(big.Int).Set(b.output)}
	}
	return &balance{big.NewInt(0), big.NewInt(0)}
}

func (s *fakeStore) BeginTx(context.Context) (storage.TxStore, error) {
	return &fakeTx{parent: s}, nil
}

// --- fakeTx ---

type fakeTx struct {
	storage.TxStore
	parent   *fakeStore
	pending  []func()
	finished bool
}

func (t *fakeTx) GetLedger(ctx context.Context, id string) (*storage.LedgerRecord, error) {
	return t.parent.GetLedger(ctx, id)
}

func (t *fakeTx) NextLedgerSeq(_ context.Context, ledgerID string) (int64, error) {
	t.parent.seq[ledgerID]++
	return t.parent.seq[ledgerID], nil
}

func (t *fakeTx) GetBalance(_ context.Context, ledgerID, account, asset string, _, _ *time.Time) (*storage.BalanceResult, error) {
	b := t.parent.getBalance(ledgerID, account, asset)
	return &storage.BalanceResult{Input: b.input, Output: b.output}, nil
}

func (t *fakeTx) GetActiveHoldsTotal(_ context.Context, ledgerID, account, asset string) (*big.Int, error) {
	if h, ok := t.parent.holds[balKey(ledgerID, account, asset)]; ok {
		return new(big.Int).Set(h), nil
	}
	return big.NewInt(0), nil
}

func (t *fakeTx) AppendLogEvent(_ context.Context, e storage.LogEventRecord) error {
	if e.IdempotencyKey != "" && t.parent.ikSeen[e.LedgerID+"|"+e.IdempotencyKey] {
		return storage.ErrIdempotencyKeyConflict
	}
	if t.parent.eventIDs[e.LedgerID+"|"+e.EventID] {
		return storage.ErrAlreadyExists
	}
	t.pending = append(t.pending, func() {
		t.parent.events = append(t.parent.events, e)
		t.parent.eventIDs[e.LedgerID+"|"+e.EventID] = true
		if e.IdempotencyKey != "" {
			t.parent.ikSeen[e.LedgerID+"|"+e.IdempotencyKey] = true
		}
	})
	return nil
}

func (t *fakeTx) InsertTransaction(_ context.Context, tx storage.TransactionRecord) error {
	if tx.Reference != "" && t.parent.txByRef[tx.LedgerID+"|"+tx.Reference] {
		return storage.ErrTransactionReferenceConflict
	}
	t.pending = append(t.pending, func() {
		t.parent.transactions = append(t.parent.transactions, tx)
		if tx.Reference != "" {
			t.parent.txByRef[tx.LedgerID+"|"+tx.Reference] = true
		}
	})
	return nil
}

func (t *fakeTx) InsertMetadataHistory(context.Context, storage.MetadataHistoryRecord) error {
	return nil
}

func (t *fakeTx) InsertVolumeDelta(_ context.Context, d storage.VolumeDeltaRecord) error {
	t.pending = append(t.pending, func() {
		k := balKey(d.LedgerID, d.Account, d.Asset)
		b, ok := t.parent.balances[k]
		if !ok {
			b = &balance{big.NewInt(0), big.NewInt(0)}
			t.parent.balances[k] = b
		}
		if d.InputDelta != nil {
			b.input.Add(b.input, d.InputDelta)
		}
		if d.OutputDelta != nil {
			b.output.Add(b.output, d.OutputDelta)
		}
	})
	return nil
}

func (t *fakeTx) UpsertAccount(_ context.Context, a storage.AccountRecord) error {
	t.pending = append(t.pending, func() { t.parent.accounts[a.LedgerID+"|"+a.Address] = true })
	return nil
}

func (t *fakeTx) InsertIdempotencyKey(_ context.Context, r storage.IdempotencyKeyRecord) error {
	if t.parent.ikSeen[r.LedgerID+"|"+r.IdempotencyKey] {
		return storage.ErrIdempotencyKeyConflict
	}
	rec := r
	t.pending = append(t.pending, func() { t.parent.ikRecords[r.LedgerID+"|"+r.IdempotencyKey] = &rec })
	return nil
}

func (t *fakeTx) Commit() error {
	if t.finished {
		return nil
	}
	t.finished = true
	for _, fn := range t.pending {
		fn()
	}
	t.pending = nil
	return nil
}

func (t *fakeTx) Rollback() error {
	t.finished = true
	t.pending = nil
	return nil
}

// --- dependency fakes (batchManager / idempotencyCache) ---

type fakeBatch struct{ id string }

func (f *fakeBatch) CurrentBatchID(context.Context, string) (string, error) {
	if f.id == "" {
		return "batch-1", nil
	}
	return f.id, nil
}

type ikEntry struct {
	eventID string
	hash    []byte
}

type fakeCache struct {
	entries map[string]ikEntry // ledger|key
	getErr  error
}

func newFakeCache() *fakeCache { return &fakeCache{entries: map[string]ikEntry{}} }

func (c *fakeCache) GetIdempotencyKey(_ context.Context, ledgerID, key string) (string, []byte, bool, error) {
	if c.getErr != nil {
		return "", nil, false, c.getErr
	}
	e, ok := c.entries[ledgerID+"|"+key]
	if !ok {
		return "", nil, false, nil
	}
	return e.eventID, e.hash, true, nil
}

func (c *fakeCache) SetIdempotencyKey(_ context.Context, ledgerID, key, eventID string, hash []byte) error {
	c.entries[ledgerID+"|"+key] = ikEntry{eventID, hash}
	return nil
}
