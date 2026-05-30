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

	ledgers        map[string]*storage.LedgerRecord
	schemas        map[string]*storage.SchemaRecord // key: ledger|version
	seq            map[string]int64
	balances       map[string]*balance                      // key: ledger|account|asset
	holds          map[string]*big.Int                      // key: ledger|account|asset
	ikRecords      map[string]*storage.IdempotencyKeyRecord // key: ledger|key
	events         []storage.LogEventRecord
	eventIDs       map[string]bool // ledger|event_id
	ikSeen         map[string]bool // ledger|idempotency_key (uniqueness)
	txByRef        map[string]bool // ledger|reference (reference-conflict)
	transactions   []storage.TransactionRecord
	accounts       map[string]bool                       // ledger|address
	holdRecords    map[string]*storage.HoldRecord        // ledger|holdID
	txByID         map[string]*storage.TransactionRecord // ledger|txID
	relationships  []storage.RelationshipRecord
	accountRecords map[string]*storage.AccountRecord         // ledger|address
	activePolicy   *storage.PolicyRecord                     // nil => unrestricted
	approvals      map[string]*storage.PendingApprovalRecord // ledger|intentID
}

type balance struct{ input, output *big.Int }

func newFakeStore() *fakeStore {
	return &fakeStore{
		ledgers:        map[string]*storage.LedgerRecord{},
		schemas:        map[string]*storage.SchemaRecord{},
		seq:            map[string]int64{},
		balances:       map[string]*balance{},
		holds:          map[string]*big.Int{},
		ikRecords:      map[string]*storage.IdempotencyKeyRecord{},
		eventIDs:       map[string]bool{},
		ikSeen:         map[string]bool{},
		txByRef:        map[string]bool{},
		accounts:       map[string]bool{},
		holdRecords:    map[string]*storage.HoldRecord{},
		txByID:         map[string]*storage.TransactionRecord{},
		accountRecords: map[string]*storage.AccountRecord{},
		approvals:      map[string]*storage.PendingApprovalRecord{},
	}
}

// --- approvals (storage.Store + storage.TxStore methods) ---

func (s *fakeStore) InsertPendingApproval(_ context.Context, a storage.PendingApprovalRecord) error {
	rec := a
	s.approvals[a.LedgerID+"|"+a.IntentID] = &rec
	return nil
}

func (s *fakeStore) UpdateApprovalState(_ context.Context, ledgerID, intentID, state string) error {
	if a, ok := s.approvals[ledgerID+"|"+intentID]; ok {
		a.State = state
	}
	return nil
}

func (t *fakeTx) GetPendingApproval(_ context.Context, ledgerID, intentID string) (*storage.PendingApprovalRecord, error) {
	if a, ok := t.parent.approvals[ledgerID+"|"+intentID]; ok {
		cp := *a
		cp.ReceivedApprovals = append([]storage.ApprovalEntry(nil), a.ReceivedApprovals...)
		return &cp, nil
	}
	return nil, fmt.Errorf("%w: approval %q", storage.ErrNotFound, intentID)
}

func (t *fakeTx) AddApproval(_ context.Context, ledgerID, intentID, principal, signature string) error {
	// Applied immediately (not buffered) so the in-tx re-read sees it, matching
	// the real FOR UPDATE flow where the write is visible within the same tx.
	if a, ok := t.parent.approvals[ledgerID+"|"+intentID]; ok {
		a.ReceivedApprovals = append(a.ReceivedApprovals, storage.ApprovalEntry{Principal: principal, Signature: signature})
	}
	return nil
}

func (t *fakeTx) UpdateApprovalState(_ context.Context, ledgerID, intentID, state string) error {
	if a, ok := t.parent.approvals[ledgerID+"|"+intentID]; ok {
		a.State = state
	}
	return nil
}

func (t *fakeTx) MarkApprovalExecuting(_ context.Context, ledgerID, intentID string) error {
	if a, ok := t.parent.approvals[ledgerID+"|"+intentID]; ok {
		a.State = "executing"
	}
	return nil
}

func (s *fakeStore) UpdateApprovalStateIf(_ context.Context, ledgerID, intentID, fromState, toState string) (bool, error) {
	if a, ok := s.approvals[ledgerID+"|"+intentID]; ok && a.State == fromState {
		a.State = toState
		return true, nil
	}
	return false, nil
}

func (s *fakeStore) ListLogEventsByIdempotencyKey(_ context.Context, ledgerID, key string) ([]storage.LogEventRecord, error) {
	var out []storage.LogEventRecord
	for _, e := range s.events {
		if e.LedgerID == ledgerID && e.IdempotencyKey == key {
			out = append(out, e)
		}
	}
	return out, nil
}

func (s *fakeStore) addTransaction(rec *storage.TransactionRecord) {
	s.txByID[rec.LedgerID+"|"+rec.TransactionID] = rec
}

func (s *fakeStore) addHold(rec *storage.HoldRecord) {
	s.holdRecords[rec.LedgerID+"|"+rec.HoldID] = rec
	// Reflect the reservation in the active-holds total.
	k := balKey(rec.LedgerID, rec.Source, rec.Asset)
	if s.holds[k] == nil {
		s.holds[k] = big.NewInt(0)
	}
	s.holds[k].Add(s.holds[k], new(big.Int).Sub(rec.AuthorizedAmount, rec.CapturedAmount))
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
	return s.activePolicy, nil // nil => unrestricted
}

func (s *fakeStore) setActivePolicy(cedar string) {
	s.activePolicy = &storage.PolicyRecord{LedgerID: "L1", Version: "v1", CedarPolicy: cedar, Active: true}
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
	rec := tx
	t.pending = append(t.pending, func() {
		t.parent.transactions = append(t.parent.transactions, tx)
		t.parent.txByID[tx.LedgerID+"|"+tx.TransactionID] = &rec
		if tx.Reference != "" {
			t.parent.txByRef[tx.LedgerID+"|"+tx.Reference] = true
		}
	})
	return nil
}

func (t *fakeTx) GetTransaction(_ context.Context, ledgerID, txID string) (*storage.TransactionRecord, error) {
	if r, ok := t.parent.txByID[ledgerID+"|"+txID]; ok {
		cp := *r
		return &cp, nil
	}
	return nil, fmt.Errorf("%w: transaction %q", storage.ErrNotFound, txID)
}

func (t *fakeTx) GetRelationships(_ context.Context, ledgerID, txID string, _ int) ([]storage.RelationshipRecord, error) {
	var out []storage.RelationshipRecord
	for _, r := range t.parent.relationships {
		if r.LedgerID == ledgerID && (r.ParentTxID == txID || r.ChildTxID == txID) {
			out = append(out, r)
		}
	}
	return out, nil
}

func (t *fakeTx) InsertRelationship(_ context.Context, r storage.RelationshipRecord) error {
	t.pending = append(t.pending, func() { t.parent.relationships = append(t.parent.relationships, r) })
	return nil
}

func (t *fakeTx) UpdateTransactionMetadata(_ context.Context, ledgerID, txID string, metadata map[string]any) error {
	t.pending = append(t.pending, func() {
		if r, ok := t.parent.txByID[ledgerID+"|"+txID]; ok {
			r.Metadata = metadata
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
	rec := a
	t.pending = append(t.pending, func() {
		t.parent.accounts[a.LedgerID+"|"+a.Address] = true
		t.parent.accountRecords[a.LedgerID+"|"+a.Address] = &rec
	})
	return nil
}

func (t *fakeTx) GetAccount(_ context.Context, ledgerID, address string) (*storage.AccountRecord, error) {
	r, ok := t.parent.accountRecords[ledgerID+"|"+address]
	if !ok {
		return nil, fmt.Errorf("%w: account %q", storage.ErrNotFound, address)
	}
	cp := *r
	if r.Metadata != nil {
		m := make(map[string]any, len(r.Metadata))
		for k, v := range r.Metadata {
			m[k] = v
		}
		cp.Metadata = m
	}
	return &cp, nil
}

func (t *fakeTx) DeleteTransactionMetadataKey(_ context.Context, ledgerID, txID, key string) error {
	t.pending = append(t.pending, func() {
		if r, ok := t.parent.txByID[ledgerID+"|"+txID]; ok && r.Metadata != nil {
			delete(r.Metadata, key)
		}
	})
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

func (t *fakeTx) GetHold(_ context.Context, ledgerID, holdID string) (*storage.HoldRecord, error) {
	if r, ok := t.parent.holdRecords[ledgerID+"|"+holdID]; ok {
		cp := *r // shallow copy; callers read only
		return &cp, nil
	}
	return nil, fmt.Errorf("%w: hold %q", storage.ErrNotFound, holdID)
}

func (t *fakeTx) InsertHold(_ context.Context, h storage.HoldRecord) error {
	rec := h
	t.pending = append(t.pending, func() {
		t.parent.holdRecords[h.LedgerID+"|"+h.HoldID] = &rec
		k := balKey(h.LedgerID, h.Source, h.Asset)
		if t.parent.holds[k] == nil {
			t.parent.holds[k] = big.NewInt(0)
		}
		t.parent.holds[k].Add(t.parent.holds[k], new(big.Int).Sub(h.AuthorizedAmount, h.CapturedAmount))
	})
	return nil
}

func (t *fakeTx) UpdateHoldCaptured(_ context.Context, ledgerID, holdID string, delta *big.Int) error {
	t.pending = append(t.pending, func() {
		if r, ok := t.parent.holdRecords[ledgerID+"|"+holdID]; ok {
			r.CapturedAmount = new(big.Int).Add(r.CapturedAmount, delta)
			if cur := t.parent.holds[balKey(ledgerID, r.Source, r.Asset)]; cur != nil {
				cur.Sub(cur, delta)
			}
		}
	})
	return nil
}

func (t *fakeTx) VoidHold(_ context.Context, ledgerID, holdID string) error {
	t.pending = append(t.pending, func() {
		if r, ok := t.parent.holdRecords[ledgerID+"|"+holdID]; ok {
			r.Voided = true
			if cur := t.parent.holds[balKey(ledgerID, r.Source, r.Asset)]; cur != nil {
				cur.Sub(cur, new(big.Int).Sub(r.AuthorizedAmount, r.CapturedAmount))
			}
		}
	})
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
