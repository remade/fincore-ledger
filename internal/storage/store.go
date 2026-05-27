package storage

import (
	"context"
	"math/big"
	"time"
)

// Store defines the contract any storage backend must satisfy.
// Phase 0: interface only. Phase 1 adds Postgres implementation.
type Store interface {
	// Lifecycle
	Close() error
	Ping(ctx context.Context) error

	// Ledger catalog
	CreateLedger(ctx context.Context, params CreateLedgerParams) (*LedgerRecord, error)
	GetLedger(ctx context.Context, id string) (*LedgerRecord, error)
	ListLedgers(ctx context.Context, params ListParams) ([]LedgerRecord, string, error)
	SealLedger(ctx context.Context, id string) (*LedgerRecord, error)

	// Log
	AppendLogEvent(ctx context.Context, event LogEventRecord) error
	GetLogEvent(ctx context.Context, ledgerID, eventID string) (*LogEventRecord, error)
	ListLogEvents(ctx context.Context, ledgerID string, params ListParams) ([]LogEventRecord, string, error)

	// Batches
	CreateBatch(ctx context.Context, batch BatchRecord) error
	CloseBatch(ctx context.Context, batchID string, merkleRoot []byte, eventCount int) error
	GetBatch(ctx context.Context, batchID string) (*BatchRecord, error)
	ListBatchEvents(ctx context.Context, batchID string) ([]LogEventRecord, error)
	ListOpenBatches(ctx context.Context, olderThan time.Duration) ([]BatchRecord, error)

	// Idempotency
	GetIdempotencyKey(ctx context.Context, ledgerID, key string) (*IdempotencyKeyRecord, error)
	InsertIdempotencyKey(ctx context.Context, record IdempotencyKeyRecord) error

	// Accounts
	UpsertAccount(ctx context.Context, account AccountRecord) error
	GetAccount(ctx context.Context, ledgerID, address string) (*AccountRecord, error)
	ListAccounts(ctx context.Context, ledgerID string, params ListAccountsParams) ([]AccountRecord, string, error)

	// Transactions
	InsertTransaction(ctx context.Context, tx TransactionRecord) error
	GetTransaction(ctx context.Context, ledgerID, txID string) (*TransactionRecord, error)
	ListTransactions(ctx context.Context, ledgerID string, params ListTransactionsParams) ([]TransactionRecord, string, error)
	UpdateTransactionMetadata(ctx context.Context, ledgerID, txID string, metadata map[string]any) error
	DeleteTransactionMetadataKey(ctx context.Context, ledgerID, txID, key string) error

	// Volumes
	InsertVolumeDelta(ctx context.Context, delta VolumeDeltaRecord) error
	GetBalance(ctx context.Context, ledgerID, account, asset string, asOfValid, asOfSystem *time.Time) (*BalanceResult, error)
	GetAggregatedBalances(ctx context.Context, ledgerID, addressPattern string, asOfValid, asOfSystem *time.Time) (map[string]*big.Int, error)

	// Schemas
	InsertSchema(ctx context.Context, schema SchemaRecord) error
	GetSchema(ctx context.Context, ledgerID, version string) (*SchemaRecord, error)

	// Metadata history
	InsertMetadataHistory(ctx context.Context, record MetadataHistoryRecord) error

	// Phase 2: Holds
	InsertHold(ctx context.Context, hold HoldRecord) error
	GetHold(ctx context.Context, ledgerID, holdID string) (*HoldRecord, error)
	ListHolds(ctx context.Context, ledgerID string, params ListHoldsParams) ([]HoldRecord, string, error)
	UpdateHoldCaptured(ctx context.Context, ledgerID, holdID string, capturedDelta *big.Int) error
	VoidHold(ctx context.Context, ledgerID, holdID string) error
	ExpireHold(ctx context.Context, ledgerID, holdID string) error
	ListExpiredHolds(ctx context.Context) ([]HoldRecord, error)
	GetActiveHoldsTotal(ctx context.Context, ledgerID, account, asset string) (*big.Int, error)

	// Phase 2: Relationships
	InsertRelationship(ctx context.Context, rel RelationshipRecord) error
	GetRelationships(ctx context.Context, ledgerID, txID string, depth int) ([]RelationshipRecord, error)

	// Phase 3: Policies
	InsertPolicy(ctx context.Context, policy PolicyRecord) error
	GetActivePolicy(ctx context.Context, ledgerID string) (*PolicyRecord, error)

	// Phase 3: Approvals
	InsertPendingApproval(ctx context.Context, approval PendingApprovalRecord) error
	GetPendingApproval(ctx context.Context, ledgerID, intentID string) (*PendingApprovalRecord, error)
	AddApproval(ctx context.Context, ledgerID, intentID, principal, signature string) error
	UpdateApprovalState(ctx context.Context, ledgerID, intentID, state string) error
	ListPendingApprovals(ctx context.Context, ledgerID string, params ListParams) ([]PendingApprovalRecord, string, error)
	ListExpiredApprovals(ctx context.Context) ([]PendingApprovalRecord, error)
	ListStuckApprovals(ctx context.Context, stuckThreshold time.Duration) ([]PendingApprovalRecord, error)

	// Transaction support
	BeginTx(ctx context.Context) (TxStore, error)
}

// TxStore is a Store scoped to a database transaction.
type TxStore interface {
	Store
	Commit() error
	Rollback() error
	NextLedgerSeq(ctx context.Context, ledgerID string) (int64, error)
}

// --- Parameter and record types ---

type CreateLedgerParams struct {
	ID       string
	BucketID string
	Metadata map[string]string
}

type ListParams struct {
	PageSize  int
	PageToken string
}

type LedgerRecord struct {
	ID              string
	BucketID        string
	State           string
	Features        map[string]string
	Metadata        map[string]string
	CreatedAt       time.Time
	SealedAt        *time.Time
	IssuerAccounts  []string
}

type LogEventRecord struct {
	EventID         string
	LedgerID        string
	LedgerSeq       int64
	SystemTime      time.Time
	ValidTime       time.Time
	Type            int16
	Payload         []byte
	IdempotencyKey  string
	IdempotencyHash []byte
	BatchID         string
	SchemaVersion   int64
}

type BatchRecord struct {
	BatchID        string
	LedgerID       string
	OpenedAt       time.Time
	ClosedAt       *time.Time
	EventCount     int
	MerkleRoot     []byte
	PrevBatchID    string
	AttestationURI string
}

type IdempotencyKeyRecord struct {
	LedgerID        string
	IdempotencyKey  string
	IdempotencyHash []byte
	EventID         string
	CreatedAt       time.Time
}

type AccountRecord struct {
	LedgerID   string
	Address    string
	FirstUsage time.Time
	UpdatedAt  time.Time
	Metadata   map[string]any
}

type TransactionRecord struct {
	LedgerID      string
	TransactionID string
	EventID       string
	ValidTime     time.Time
	SystemTime    time.Time
	Reference     string
	Postings      any // JSON-serializable
	Metadata      map[string]any
}

type ListAccountsParams struct {
	ListParams
	AddressPattern string
	MetadataFilter map[string]string
}

type ListTransactionsParams struct {
	ListParams
	AsOfValid      *time.Time
	AsOfSystem     *time.Time
	Account        string
	Reference      string
	MetadataFilter map[string]string
}

type VolumeDeltaRecord struct {
	LedgerID    string
	Account     string
	Asset       string
	Shard       int16
	EventID     string
	ValidTime   time.Time
	SystemTime  time.Time
	InputDelta  *big.Int
	OutputDelta *big.Int
}

type BalanceResult struct {
	Input  *big.Int
	Output *big.Int
}

type SchemaRecord struct {
	LedgerID   string
	Version    string
	Document   any
	InsertedAt time.Time
	EventID    string
}

type MetadataHistoryRecord struct {
	LedgerID   string
	TargetType int16
	TargetID   string
	Revision   int64
	Metadata   map[string]any
	EventID    string
	SystemTime time.Time
}

// --- Phase 2 types ---

type HoldRecord struct {
	LedgerID          string
	HoldID            string
	Source            string
	DestinationHint   string
	Asset             string
	AuthorizedAmount  *big.Int
	CapturedAmount    *big.Int
	Voided            bool
	Expired           bool
	ExpiresAt         time.Time
	AuthorizedEventID string
	ValidTime         time.Time
	SystemTime        time.Time
}

type ListHoldsParams struct {
	ListParams
	Account string
}

type RelationshipRecord struct {
	LedgerID         string
	ParentTxID       string
	ChildTxID        string
	RelationshipType int16 // 0=reverts, 1=amends, 2=settles, 3=extends, 4=references
	EventID          string
	SystemTime       time.Time
}

// --- Phase 3 types ---

type PolicyRecord struct {
	LedgerID    string
	Version     string
	CedarPolicy string
	InsertedAt  time.Time
	EventID     string
	Active      bool
}

type PendingApprovalRecord struct {
	LedgerID           string
	IntentID           string
	IntentPayload      []byte
	IntentHash         []byte
	RequiredApprovers  []string
	ReceivedApprovals  []ApprovalEntry
	ExpiresAt          time.Time
	State              string
	SubmittedAt        time.Time
	SubmittedBy        string
}

type ApprovalEntry struct {
	Principal string    `json:"principal"`
	Signature string    `json:"signature"`
	SignedAt  time.Time `json:"signed_at"`
}
