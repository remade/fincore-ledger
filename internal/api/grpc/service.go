package grpc

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"time"

	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/remade/ledger/internal/api/grpc/auth"
	"github.com/remade/ledger/internal/config"
	"github.com/remade/ledger/internal/log/batch"
	"github.com/remade/ledger/internal/planner"
	"github.com/remade/ledger/internal/storage"
	"github.com/remade/ledger/internal/subscriptions"
	pb "github.com/remade/ledger/pkg/proto/ledger/v1"
)

// LedgerService implements the gRPC LedgerService.
type LedgerService struct {
	pb.UnimplementedLedgerServiceServer
	store   storage.Store
	planner *planner.Planner
	batch   *batch.Manager
	subs    *subscriptions.Manager
	logger  *zap.Logger

	// adminPrincipals are the only principals permitted to use the privileged
	// Import/Export RPCs (empty => none).
	adminPrincipals map[string]bool
}

// NewLedgerService creates a new LedgerService.
func NewLedgerService(store storage.Store, p *planner.Planner, bm *batch.Manager, sm *subscriptions.Manager, authCfg config.AuthConfig, logger *zap.Logger) *LedgerService {
	admins := make(map[string]bool, len(authCfg.AdminPrincipals))
	for _, a := range authCfg.AdminPrincipals {
		admins[a] = true
	}
	return &LedgerService{
		store:           store,
		planner:         p,
		batch:           bm,
		subs:            sm,
		logger:          logger.Named("api"),
		adminPrincipals: admins,
	}
}

// authorizeLedger enforces tenant isolation: an authenticated caller may only
// access ledgers their token is scoped to via the "ledgers" JWT claim ("*" grants
// all). Authentication is mandatory, so an absent Identity (which can only happen
// if a non-exempt RPC somehow bypassed the auth interceptor) is denied.
func (s *LedgerService) authorizeLedger(ctx context.Context, ledgerID string) error {
	id, ok := auth.IdentityFromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "authentication required")
	}
	if id.CanAccessLedger(ledgerID) {
		return nil
	}
	return status.Errorf(codes.PermissionDenied, "principal %q is not authorized for ledger %q", id.Principal, ledgerID)
}

// requireImportExportAuthz gates the privileged Import/Export RPCs. These read
// from / write to the raw source-of-truth log, so they default-deny: only the
// configured admin principals are permitted (empty set => nobody).
func (s *LedgerService) requireImportExportAuthz(principal string) error {
	if s.adminPrincipals[principal] {
		return nil
	}
	return status.Errorf(codes.PermissionDenied, "principal %q is not authorized for import/export", principal)
}

func (s *LedgerService) CreateLedger(ctx context.Context, req *pb.CreateLedgerRequest) (*pb.Ledger, error) {
	if err := s.authorizeLedger(ctx, req.Id); err != nil {
		return nil, err
	}
	meta := make(map[string]string)
	if req.Metadata != nil {
		meta = req.Metadata
	}
	rec, err := s.store.CreateLedger(ctx, storage.CreateLedgerParams{
		ID:       req.Id,
		BucketID: req.BucketId,
		Metadata: meta,
	})
	if err != nil {
		if isAlreadyExists(err) {
			return nil, status.Errorf(codes.AlreadyExists, "ledger %q already exists", req.Id)
		}
		return nil, status.Errorf(codes.Internal, "creating ledger: %v", err)
	}
	return ledgerRecordToProto(rec), nil
}

func (s *LedgerService) GetLedger(ctx context.Context, req *pb.GetLedgerRequest) (*pb.Ledger, error) {
	if err := s.authorizeLedger(ctx, req.Id); err != nil {
		return nil, err
	}
	rec, err := s.store.GetLedger(ctx, req.Id)
	if err != nil {
		if isNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "ledger %q not found", req.Id)
		}
		return nil, status.Errorf(codes.Internal, "getting ledger: %v", err)
	}
	return ledgerRecordToProto(rec), nil
}

func (s *LedgerService) ListLedgers(ctx context.Context, req *pb.ListLedgersRequest) (*pb.ListLedgersResponse, error) {
	recs, nextToken, err := s.store.ListLedgers(ctx, storage.ListParams{
		PageSize:  int(req.PageSize),
		PageToken: req.PageToken,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing ledgers: %v", err)
	}
	// Filter to the ledgers the caller's token is scoped to (tenant isolation).
	// When auth is disabled (development) no Identity is present and all are shown.
	id, scoped := auth.IdentityFromContext(ctx)
	resp := &pb.ListLedgersResponse{NextPageToken: nextToken}
	for i := range recs {
		if scoped && !id.CanAccessLedger(recs[i].ID) {
			continue
		}
		resp.Ledgers = append(resp.Ledgers, ledgerRecordToProto(&recs[i]))
	}
	return resp, nil
}

func (s *LedgerService) SealLedger(ctx context.Context, req *pb.SealLedgerRequest) (*pb.Ledger, error) {
	if err := s.authorizeLedger(ctx, req.Id); err != nil {
		return nil, err
	}
	rec, err := s.store.SealLedger(ctx, req.Id)
	if err != nil {
		if isNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "ledger %q not found or already sealed", req.Id)
		}
		return nil, status.Errorf(codes.Internal, "sealing ledger: %v", err)
	}
	return ledgerRecordToProto(rec), nil
}

func (s *LedgerService) Submit(ctx context.Context, req *pb.SubmitRequest) (*pb.SubmitResponse, error) {
	intent := req.Intent
	if intent == nil {
		return nil, status.Error(codes.InvalidArgument, "intent is required")
	}
	if intent.LedgerId == "" {
		return nil, status.Error(codes.InvalidArgument, "ledger_id is required")
	}

	// Policy enforcement: evaluate active policy before executing.
	principal, err := extractPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	opType, accountsTouched := classifyIntent(intent)
	if err := s.authorizeLedger(ctx, intent.LedgerId); err != nil {
		return nil, err
	}
	if err := s.planner.EvaluatePolicy(ctx, intent.LedgerId, principal, opType, accountsTouched); err != nil {
		if errors.Is(err, storage.ErrPolicyDenied) {
			// Write denial audit event, then return error.
			if auditErr := s.planner.WritePolicyDenialEvent(ctx, intent.LedgerId, principal, err.Error()); auditErr != nil {
				s.logger.Error("failed to write policy denial event", zap.Error(auditErr))
			}
		}
		return nil, mapPlannerError(err)
	}

	switch op := intent.Operation.(type) {
	case *pb.Intent_Post:
		return s.handlePost(ctx, intent, op.Post)
	case *pb.Intent_SetMetadata:
		return s.handleSetMetadata(ctx, intent, op.SetMetadata)
	case *pb.Intent_DeleteMetadata:
		return s.handleDeleteMetadata(ctx, intent, op.DeleteMetadata)
	case *pb.Intent_InsertSchema:
		return s.handleInsertSchema(ctx, intent, op.InsertSchema)
	case *pb.Intent_Authorize:
		return s.handleAuthorize(ctx, intent, op.Authorize)
	case *pb.Intent_Capture:
		return s.handleCapture(ctx, intent, op.Capture)
	case *pb.Intent_Void:
		return s.handleVoid(ctx, intent, op.Void)
	case *pb.Intent_Revert:
		return s.handleRevert(ctx, intent, op.Revert)
	case *pb.Intent_Amend:
		return s.handleAmend(ctx, intent, op.Amend)
	case *pb.Intent_Batch:
		return s.handleBatch(ctx, intent, op.Batch)
	case *pb.Intent_Convert:
		return s.handleConvert(ctx, intent, op.Convert)
	default:
		return nil, status.Errorf(codes.Unimplemented, "operation type not yet supported")
	}
}

func (s *LedgerService) handlePost(ctx context.Context, intent *pb.Intent, op *pb.PostOperation) (*pb.SubmitResponse, error) {
	postings := make([]planner.PostingInput, len(op.Postings))
	for i, p := range op.Postings {
		amount, ok := new(big.Int).SetString(p.Amount, 10)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "posting %d: invalid amount %q", i, p.Amount)
		}
		postings[i] = planner.PostingInput{
			Source:      p.Source,
			Destination: p.Destination,
			Amount:      amount,
			Asset:       p.Asset,
		}
	}

	var metadata map[string]any
	if intent.Metadata != nil {
		metadata = make(map[string]any, len(intent.Metadata))
		for k, v := range intent.Metadata {
			metadata[k] = metadataValueToAny(v)
		}
	}

	var validTime *timestamppb.Timestamp
	if intent.ValidTime != nil {
		validTime = intent.ValidTime
	}
	var vt *time.Time
	if validTime != nil {
		t := validTime.AsTime()
		vt = &t
	}

	result, err := s.planner.SubmitPost(ctx, intent.LedgerId, postings, intent.Reference, metadata, intent.IdempotencyKey, vt, intent.DryRun)
	if err != nil {
		return nil, mapPlannerError(err)
	}

	resp := &pb.SubmitResponse{
		EventId:       result.EventID,
		IdempotentHit: result.IdempotentHit,
	}
	if result.Transaction != nil {
		resp.Output = &pb.SubmitResponse_Transaction{
			Transaction: txRecordToProto(result.Transaction),
		}
	}
	return resp, nil
}

func (s *LedgerService) handleSetMetadata(ctx context.Context, intent *pb.Intent, op *pb.SetMetadataOperation) (*pb.SubmitResponse, error) {
	metadata := make(map[string]any, len(op.Metadata))
	for k, v := range op.Metadata {
		metadata[k] = metadataValueToAny(v)
	}

	targetType := int16(op.TargetType)
	result, err := s.planner.SubmitSetMetadata(ctx, intent.LedgerId, targetType, op.TargetId, metadata, intent.IdempotencyKey)
	if err != nil {
		return nil, mapPlannerError(err)
	}

	return &pb.SubmitResponse{
		EventId:       result.EventID,
		IdempotentHit: result.IdempotentHit,
	}, nil
}

func (s *LedgerService) handleDeleteMetadata(ctx context.Context, intent *pb.Intent, op *pb.DeleteMetadataOperation) (*pb.SubmitResponse, error) {
	targetType := int16(op.TargetType)
	result, err := s.planner.SubmitDeleteMetadata(ctx, intent.LedgerId, targetType, op.TargetId, op.Key, intent.IdempotencyKey)
	if err != nil {
		return nil, mapPlannerError(err)
	}

	return &pb.SubmitResponse{
		EventId:       result.EventID,
		IdempotentHit: result.IdempotentHit,
	}, nil
}

func (s *LedgerService) handleInsertSchema(ctx context.Context, intent *pb.Intent, op *pb.InsertSchemaOperation) (*pb.SubmitResponse, error) {
	result, err := s.planner.SubmitInsertSchema(ctx, intent.LedgerId, op.SchemaBytes, op.Version, intent.IdempotencyKey)
	if err != nil {
		return nil, mapPlannerError(err)
	}

	return &pb.SubmitResponse{
		EventId:       result.EventID,
		IdempotentHit: result.IdempotentHit,
	}, nil
}

func (s *LedgerService) GetTransaction(ctx context.Context, req *pb.GetTransactionRequest) (*pb.Transaction, error) {
	if err := s.authorizeLedger(ctx, req.LedgerId); err != nil {
		return nil, err
	}
	rec, err := s.store.GetTransaction(ctx, req.LedgerId, req.TransactionId)
	if err != nil {
		if isNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "transaction not found")
		}
		return nil, status.Errorf(codes.Internal, "getting transaction: %v", err)
	}
	return txRecordToProto(rec), nil
}

func (s *LedgerService) ListTransactions(ctx context.Context, req *pb.ListTransactionsRequest) (*pb.ListTransactionsResponse, error) {
	if err := s.authorizeLedger(ctx, req.LedgerId); err != nil {
		return nil, err
	}
	recs, nextToken, err := s.store.ListTransactions(ctx, req.LedgerId, storage.ListTransactionsParams{
		ListParams: storage.ListParams{
			PageSize:  int(req.PageSize),
			PageToken: req.PageToken,
		},
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing transactions: %v", err)
	}
	resp := &pb.ListTransactionsResponse{NextPageToken: nextToken}
	for i := range recs {
		resp.Transactions = append(resp.Transactions, txRecordToProto(&recs[i]))
	}
	return resp, nil
}

func (s *LedgerService) GetAccount(ctx context.Context, req *pb.GetAccountRequest) (*pb.Account, error) {
	if err := s.authorizeLedger(ctx, req.LedgerId); err != nil {
		return nil, err
	}
	rec, err := s.store.GetAccount(ctx, req.LedgerId, req.Address)
	if err != nil {
		if isNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "account not found")
		}
		return nil, status.Errorf(codes.Internal, "getting account: %v", err)
	}
	return accountRecordToProto(rec), nil
}

func (s *LedgerService) ListAccounts(ctx context.Context, req *pb.ListAccountsRequest) (*pb.ListAccountsResponse, error) {
	if err := s.authorizeLedger(ctx, req.LedgerId); err != nil {
		return nil, err
	}
	recs, nextToken, err := s.store.ListAccounts(ctx, req.LedgerId, storage.ListAccountsParams{
		ListParams: storage.ListParams{
			PageSize:  int(req.PageSize),
			PageToken: req.PageToken,
		},
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing accounts: %v", err)
	}
	resp := &pb.ListAccountsResponse{NextPageToken: nextToken}
	for i := range recs {
		resp.Accounts = append(resp.Accounts, accountRecordToProto(&recs[i]))
	}
	return resp, nil
}

func (s *LedgerService) GetBalance(ctx context.Context, req *pb.GetBalanceRequest) (*pb.GetBalanceResponse, error) {
	var asOfValid, asOfSystem *time.Time
	if req.AsOfValid != nil {
		t := req.AsOfValid.AsTime()
		asOfValid = &t
	}
	if req.AsOfSystem != nil {
		t := req.AsOfSystem.AsTime()
		asOfSystem = &t
	}

	asset := req.Asset
	if asset == "" {
		return nil, status.Error(codes.InvalidArgument, "asset is required for GetBalance")
	}

	if err := s.authorizeLedger(ctx, req.LedgerId); err != nil {
		return nil, err
	}
	bal, err := s.store.GetBalance(ctx, req.LedgerId, req.Account, asset, asOfValid, asOfSystem)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "getting balance: %v", err)
	}

	balance := new(big.Int).Sub(bal.Input, bal.Output)
	resp := &pb.GetBalanceResponse{
		PostedBalance: balance.String(),
		Asset:         asset,
	}
	if req.IncludeHolds {
		holdsTotal, err := s.store.GetActiveHoldsTotal(ctx, req.LedgerId, req.Account, asset)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "getting active holds total: %v", err)
		}
		available := new(big.Int).Sub(balance, holdsTotal)
		resp.AvailableBalance = available.String()
	}
	if req.AsOfValid != nil {
		resp.AsOfValid = req.AsOfValid
	}
	if req.AsOfSystem != nil {
		resp.AsOfSystem = req.AsOfSystem
	}
	return resp, nil
}

func (s *LedgerService) GetAggregatedBalances(ctx context.Context, req *pb.GetAggregatedBalancesRequest) (*pb.GetAggregatedBalancesResponse, error) {
	var asOfValid, asOfSystem *time.Time
	if req.AsOfValid != nil {
		t := req.AsOfValid.AsTime()
		asOfValid = &t
	}
	if req.AsOfSystem != nil {
		t := req.AsOfSystem.AsTime()
		asOfSystem = &t
	}

	if err := s.authorizeLedger(ctx, req.LedgerId); err != nil {
		return nil, err
	}
	balances, err := s.store.GetAggregatedBalances(ctx, req.LedgerId, req.AddressPattern, asOfValid, asOfSystem)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "getting aggregated balances: %v", err)
	}

	result := make(map[string]string, len(balances))
	for asset, bal := range balances {
		result[asset] = bal.String()
	}
	return &pb.GetAggregatedBalancesResponse{Balances: result}, nil
}

func (s *LedgerService) GetSchema(ctx context.Context, req *pb.GetSchemaRequest) (*pb.Schema, error) {
	if err := s.authorizeLedger(ctx, req.LedgerId); err != nil {
		return nil, err
	}
	rec, err := s.store.GetSchema(ctx, req.LedgerId, req.Version)
	if err != nil {
		if isNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "schema %q not found", req.Version)
		}
		return nil, status.Errorf(codes.Internal, "getting schema: %v", err)
	}

	var docBytes []byte
	switch d := rec.Document.(type) {
	case []byte:
		docBytes = d
	default:
		marshaled, err := json.Marshal(rec.Document)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "marshaling schema document: %v", err)
		}
		docBytes = marshaled
	}

	return &pb.Schema{
		LedgerId:   rec.LedgerID,
		Version:    rec.Version,
		Document:   docBytes,
		InsertedAt: timestamppb.New(rec.InsertedAt),
		EventId:    rec.EventID,
	}, nil
}

func (s *LedgerService) ListLogEvents(ctx context.Context, req *pb.ListLogEventsRequest) (*pb.ListLogEventsResponse, error) {
	if err := s.authorizeLedger(ctx, req.LedgerId); err != nil {
		return nil, err
	}
	recs, nextToken, err := s.store.ListLogEvents(ctx, req.LedgerId, storage.ListParams{
		PageSize:  int(req.PageSize),
		PageToken: req.PageToken,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing log events: %v", err)
	}
	resp := &pb.ListLogEventsResponse{NextPageToken: nextToken}
	for _, rec := range recs {
		resp.Events = append(resp.Events, logEventRecordToProto(&rec))
	}
	return resp, nil
}

func (s *LedgerService) GetLogEvent(ctx context.Context, req *pb.GetLogEventRequest) (*pb.LogEvent, error) {
	if err := s.authorizeLedger(ctx, req.LedgerId); err != nil {
		return nil, err
	}
	rec, err := s.store.GetLogEvent(ctx, req.LedgerId, req.EventId)
	if err != nil {
		if isNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "log event not found")
		}
		return nil, status.Errorf(codes.Internal, "getting log event: %v", err)
	}
	return logEventRecordToProto(rec), nil
}

func (s *LedgerService) VerifyBatch(ctx context.Context, req *pb.VerifyBatchRequest) (*pb.VerifyBatchResponse, error) {
	valid, root, count, err := s.batch.VerifyBatch(ctx, req.BatchId)
	if err != nil {
		if isNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "batch not found")
		}
		return nil, status.Errorf(codes.Internal, "verifying batch: %v", err)
	}

	return &pb.VerifyBatchResponse{
		Valid:      valid,
		MerkleRoot: hex.EncodeToString(root),
		EventCount: int32(count),
	}, nil
}

func (s *LedgerService) Export(req *pb.ExportRequest, stream pb.LedgerService_ExportServer) error {
	ctx := stream.Context()
	// Export streams the full event log; require admin authorization (default-deny)
	// before any data leaves the server, then apply any policy rules.
	principal, err := extractPrincipal(ctx)
	if err != nil {
		return err
	}
	if err := s.requireImportExportAuthz(principal); err != nil {
		return err
	}
	if err := s.planner.EvaluatePolicy(ctx, req.LedgerId, principal, "export", nil); err != nil {
		return mapPlannerError(err)
	}

	pageToken := ""
	for {
		events, nextToken, err := s.store.ListLogEvents(ctx, req.LedgerId,
			storage.ListParams{PageSize: 1000, PageToken: pageToken})
		if err != nil {
			return status.Errorf(codes.Internal, "listing events for export: %v", err)
		}
		for _, e := range events {
			if err := stream.Send(logEventRecordToProto(&e)); err != nil {
				return err
			}
		}
		if nextToken == "" {
			break
		}
		pageToken = nextToken
	}
	return nil
}

const (
	// maxImportEventsPerCall bounds the number of events a single Import stream may
	// submit, preventing an unbounded client stream from exhausting server resources.
	maxImportEventsPerCall = 100_000
	// maxImportClockSkew bounds how far an imported event's system_time may lead the
	// server clock. system_time records when an event was persisted, so a future
	// value signals a forged or replayed record.
	maxImportClockSkew = 5 * time.Minute
)

// validateImportEvent checks the structural validity of an event before it is
// written directly to the log via Import.
func validateImportEvent(event *pb.LogEvent) error {
	if event.LedgerId == "" {
		return status.Error(codes.InvalidArgument, "event ledger_id is required")
	}
	if event.EventId == "" {
		return status.Error(codes.InvalidArgument, "event event_id is required")
	}
	if len(event.Payload) == 0 {
		return status.Errorf(codes.InvalidArgument, "event %q has an empty payload", event.EventId)
	}
	if !json.Valid(event.Payload) {
		return status.Errorf(codes.InvalidArgument, "event %q payload is not valid JSON", event.EventId)
	}
	// Bound on the int32 before narrowing to int16 so a large value cannot wrap
	// into the valid range. Event types are the contiguous range 1..max.
	if int32(event.Type) < 1 || int32(event.Type) > int32(storage.EventTypeApprovalRecorded) {
		return status.Errorf(codes.InvalidArgument, "event %q has unknown event type %d", event.EventId, event.Type)
	}
	if event.SystemTime == nil || event.ValidTime == nil {
		return status.Errorf(codes.InvalidArgument, "event %q is missing system_time or valid_time", event.EventId)
	}
	return nil
}

func (s *LedgerService) Import(stream pb.LedgerService_ImportServer) error {
	ctx := stream.Context()
	principal, err := extractPrincipal(ctx)
	if err != nil {
		return err
	}
	// Import writes raw events directly to the source-of-truth log; gate it behind
	// admin authorization (default-deny) before accepting any data.
	if err := s.requireImportExportAuthz(principal); err != nil {
		return err
	}
	var count int64
	// Per-ledger import state. expectedSeq is the next ledger_seq we require: imports
	// must be a strict, contiguous sequence starting at 1 into a *fresh* ledger, so a
	// client cannot collide with or reorder existing history. batchIDs holds the
	// server-assigned batch for each ledger; client-supplied batch ids are never
	// trusted (a forged batch id could poison a closed batch's Merkle verification).
	expectedSeq := make(map[string]int64)
	batchIDs := make(map[string]string)

	for {
		event, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return status.Errorf(codes.Internal, "receiving event: %v", err)
		}

		// Bound the per-call stream so a single Import cannot exhaust server memory
		// or run unboundedly. Clients with larger migrations chunk across calls.
		if count >= maxImportEventsPerCall {
			return status.Errorf(codes.ResourceExhausted,
				"import exceeds per-call limit of %d events; split into multiple calls", maxImportEventsPerCall)
		}

		// Validate event structure (type, payload JSON, required fields) before it
		// touches the log.
		if err := validateImportEvent(event); err != nil {
			return err
		}

		// First event for a ledger: authorize, verify the ledger exists, is not
		// sealed, and is EMPTY, then allocate a server-controlled batch.
		if _, seen := expectedSeq[event.LedgerId]; !seen {
			if err := s.planner.EvaluatePolicy(ctx, event.LedgerId, principal, "import", nil); err != nil {
				return mapPlannerError(err)
			}
			ledger, err := s.store.GetLedger(ctx, event.LedgerId)
			if err != nil {
				if isNotFound(err) {
					return status.Errorf(codes.NotFound, "ledger %q not found", event.LedgerId)
				}
				return status.Errorf(codes.Internal, "checking ledger: %v", err)
			}
			if ledger.State == "sealed" {
				return status.Errorf(codes.FailedPrecondition, "ledger %q is sealed; import rejected", event.LedgerId)
			}
			existing, _, err := s.store.ListLogEvents(ctx, event.LedgerId, storage.ListParams{PageSize: 1})
			if err != nil {
				return status.Errorf(codes.Internal, "checking ledger emptiness: %v", err)
			}
			if len(existing) > 0 {
				return status.Errorf(codes.FailedPrecondition,
					"ledger %q already has events; import only restores into an empty ledger", event.LedgerId)
			}
			batchID, err := s.batch.CurrentBatchID(ctx, event.LedgerId)
			if err != nil {
				return status.Errorf(codes.Internal, "allocating import batch: %v", err)
			}
			expectedSeq[event.LedgerId] = 1
			batchIDs[event.LedgerId] = batchID
		}

		// Integrity checks against forged/replayed fields. valid_time stays
		// client-controlled by design (it is the logical effective time); system_time
		// (the persistence time) may not be in the future, and the sequence must be
		// strictly contiguous so ordering cannot be tampered with.
		if event.SystemTime.AsTime().After(time.Now().UTC().Add(maxImportClockSkew)) {
			return status.Errorf(codes.InvalidArgument, "event %q system_time is in the future", event.EventId)
		}
		if want := expectedSeq[event.LedgerId]; uint64(want) != event.LedgerSeq {
			return status.Errorf(codes.InvalidArgument,
				"event %q has non-contiguous ledger_seq %d (expected %d)", event.EventId, event.LedgerSeq, want)
		}

		if err := s.store.AppendLogEvent(ctx, storage.LogEventRecord{
			EventID:        event.EventId,
			LedgerID:       event.LedgerId,
			LedgerSeq:      int64(event.LedgerSeq),
			SystemTime:     event.SystemTime.AsTime(),
			ValidTime:      event.ValidTime.AsTime(),
			Type:           int16(event.Type),
			Payload:        event.Payload,
			IdempotencyKey: event.IdempotencyKey,
			BatchID:        batchIDs[event.LedgerId], // server-assigned; client batch_id is not trusted
			SchemaVersion:  int64(event.SchemaVersion),
		}); err != nil {
			if errors.Is(err, storage.ErrIdempotencyKeyConflict) {
				return status.Errorf(codes.AlreadyExists, "event %q already exists (idempotency conflict)", event.EventId)
			}
			return status.Errorf(codes.Internal, "importing event: %v", err)
		}
		expectedSeq[event.LedgerId]++
		count++
	}
	return stream.SendAndClose(&pb.ImportResponse{EventsImported: count})
}

func (s *LedgerService) Subscribe(req *pb.SubscribeRequest, stream pb.LedgerService_SubscribeServer) error {
	var eventTypes []int16
	for _, t := range req.Types {
		eventTypes = append(eventTypes, int16(t))
	}

	if err := s.authorizeLedger(stream.Context(), req.LedgerId); err != nil {
		return err
	}
	ch, cancel, err := s.subs.Subscribe(stream.Context(), req.LedgerId, eventTypes, req.FromEventId, s.store)
	if err != nil {
		return status.Errorf(codes.Internal, "subscribing: %v", err)
	}
	defer cancel()

	for event := range ch {
		if err := stream.Send(&pb.SubscriptionEvent{
			Event:    logEventRecordToProto(&event),
			IsReplay: req.FromEventId != "" && req.IncludeHistorical,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *LedgerService) handleAuthorize(ctx context.Context, intent *pb.Intent, op *pb.AuthorizeOperation) (*pb.SubmitResponse, error) {
	amount, ok := new(big.Int).SetString(op.Amount, 10)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid amount %q", op.Amount)
	}
	if op.ExpiresAt == nil {
		return nil, status.Error(codes.InvalidArgument, "expires_at is required for authorize")
	}

	result, err := s.planner.SubmitAuthorize(ctx, intent.LedgerId, op.Source, op.DestinationHint,
		op.Asset, amount, op.ExpiresAt.AsTime(), intent.IdempotencyKey)
	if err != nil {
		return nil, mapPlannerError(err)
	}

	return &pb.SubmitResponse{
		EventId:       result.EventID,
		IdempotentHit: result.IdempotentHit,
		Output: &pb.SubmitResponse_Hold{
			Hold: &pb.Hold{
				HoldId:           result.HoldID,
				Source:           op.Source,
				DestinationHint:  op.DestinationHint,
				Asset:            op.Asset,
				AuthorizedAmount: op.Amount,
				CapturedAmount:   "0",
				ExpiresAt:        op.ExpiresAt,
			},
		},
	}, nil
}

func (s *LedgerService) handleCapture(ctx context.Context, intent *pb.Intent, op *pb.CaptureOperation) (*pb.SubmitResponse, error) {
	amount, ok := new(big.Int).SetString(op.Amount, 10)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid amount %q", op.Amount)
	}

	result, err := s.planner.SubmitCapture(ctx, intent.LedgerId, op.HoldId, amount, op.Destination, intent.IdempotencyKey)
	if err != nil {
		return nil, mapPlannerError(err)
	}

	return &pb.SubmitResponse{
		EventId:       result.EventID,
		IdempotentHit: result.IdempotentHit,
		Output: &pb.SubmitResponse_Capture{
			Capture: &pb.Capture{
				HoldId:      op.HoldId,
				Amount:      op.Amount,
				Destination: op.Destination,
			},
		},
	}, nil
}

func (s *LedgerService) handleVoid(ctx context.Context, intent *pb.Intent, op *pb.VoidOperation) (*pb.SubmitResponse, error) {
	result, err := s.planner.SubmitVoid(ctx, intent.LedgerId, op.HoldId, intent.IdempotencyKey)
	if err != nil {
		return nil, mapPlannerError(err)
	}

	return &pb.SubmitResponse{
		EventId:       result.EventID,
		IdempotentHit: result.IdempotentHit,
	}, nil
}

func (s *LedgerService) handleRevert(ctx context.Context, intent *pb.Intent, op *pb.RevertOperation) (*pb.SubmitResponse, error) {
	result, err := s.planner.SubmitRevert(ctx, intent.LedgerId, op.OriginalTransactionId,
		op.Force, op.AtEffectiveDate, op.Reason, intent.IdempotencyKey)
	if err != nil {
		return nil, mapPlannerError(err)
	}

	resp := &pb.SubmitResponse{
		EventId:       result.EventID,
		IdempotentHit: result.IdempotentHit,
		Output: &pb.SubmitResponse_Revert{
			Revert: &pb.Revert{
				OriginalTransactionId: op.OriginalTransactionId,
			},
		},
	}
	if result.Transaction != nil {
		resp.GetRevert().RevertingTransactionId = result.Transaction.TransactionID
	}
	return resp, nil
}

func (s *LedgerService) handleAmend(ctx context.Context, intent *pb.Intent, op *pb.AmendOperation) (*pb.SubmitResponse, error) {
	metadata := make(map[string]any, len(op.MetadataChanges))
	for k, v := range op.MetadataChanges {
		metadata[k] = metadataValueToAny(v)
	}

	result, err := s.planner.SubmitAmend(ctx, intent.LedgerId, op.OriginalTransactionId, metadata, intent.IdempotencyKey)
	if err != nil {
		return nil, mapPlannerError(err)
	}

	return &pb.SubmitResponse{
		EventId:       result.EventID,
		IdempotentHit: result.IdempotentHit,
		Output: &pb.SubmitResponse_Amend{
			Amend: &pb.Amend{OriginalTransactionId: op.OriginalTransactionId},
		},
	}, nil
}

func (s *LedgerService) handleBatch(ctx context.Context, intent *pb.Intent, op *pb.BatchOperation) (*pb.SubmitResponse, error) {
	intents := make([]planner.BatchIntent, len(op.Intents))
	for i, nested := range op.Intents {
		bi := planner.BatchIntent{IdempotencyKey: nested.IdempotencyKey, Reference: nested.Reference}
		switch o := nested.Operation.(type) {
		case *pb.Intent_Post:
			bi.Type = "post"
			for _, p := range o.Post.Postings {
				amt, ok := new(big.Int).SetString(p.Amount, 10)
				if !ok {
					return nil, status.Errorf(codes.InvalidArgument, "batch intent %d posting: invalid amount %q", i, p.Amount)
				}
				bi.Postings = append(bi.Postings, planner.PostingInput{
					Source: p.Source, Destination: p.Destination, Amount: amt, Asset: p.Asset,
				})
			}
		case *pb.Intent_Authorize:
			bi.Type = "authorize"
			amt, ok := new(big.Int).SetString(o.Authorize.Amount, 10)
			if !ok {
				return nil, status.Errorf(codes.InvalidArgument, "batch intent %d: invalid amount %q", i, o.Authorize.Amount)
			}
			bi.Source = o.Authorize.Source
			bi.DestinationHint = o.Authorize.DestinationHint
			bi.Asset = o.Authorize.Asset
			bi.Amount = amt
			if o.Authorize.ExpiresAt != nil {
				bi.ExpiresAt = o.Authorize.ExpiresAt.AsTime()
			}
		case *pb.Intent_Capture:
			bi.Type = "capture"
			amt, ok := new(big.Int).SetString(o.Capture.Amount, 10)
			if !ok {
				return nil, status.Errorf(codes.InvalidArgument, "batch intent %d: invalid amount %q", i, o.Capture.Amount)
			}
			bi.HoldID = o.Capture.HoldId
			bi.Amount = amt
			bi.Destination = o.Capture.Destination
		case *pb.Intent_Void:
			bi.Type = "void"
			bi.HoldID = o.Void.HoldId
		case *pb.Intent_Revert:
			bi.Type = "revert"
			bi.OriginalTxID = o.Revert.OriginalTransactionId
			bi.Force = o.Revert.Force
			bi.AtEffectiveDate = o.Revert.AtEffectiveDate
			bi.Reason = o.Revert.Reason
		case *pb.Intent_Amend:
			bi.Type = "amend"
			bi.OriginalTxID = o.Amend.OriginalTransactionId
			bi.Metadata = make(map[string]any, len(o.Amend.MetadataChanges))
			for k, v := range o.Amend.MetadataChanges {
				bi.Metadata[k] = metadataValueToAny(v)
			}
		case *pb.Intent_Convert:
			bi.Type = "convert"
			srcAmt, ok := new(big.Int).SetString(o.Convert.SourceAmount, 10)
			if !ok {
				return nil, status.Errorf(codes.InvalidArgument, "batch intent %d: invalid source_amount %q", i, o.Convert.SourceAmount)
			}
			dstAmt, ok := new(big.Int).SetString(o.Convert.DestinationAmount, 10)
			if !ok {
				return nil, status.Errorf(codes.InvalidArgument, "batch intent %d: invalid destination_amount %q", i, o.Convert.DestinationAmount)
			}
			cp := planner.ConvertParams{
				Source:          o.Convert.Source,
				Destination:     o.Convert.Destination,
				SourceAmount:    srcAmt,
				SourceAsset:     o.Convert.SourceAsset,
				DestAmount:      dstAmt,
				DestAsset:       o.Convert.DestinationAsset,
				Rate:            o.Convert.Rate,
				RateSource:      o.Convert.RateSource,
				SlippageAccount: o.Convert.SlippageAccount,
			}
			if o.Convert.SlippageAmount != "" {
				slipAmt, ok := new(big.Int).SetString(o.Convert.SlippageAmount, 10)
				if !ok {
					return nil, status.Errorf(codes.InvalidArgument, "batch intent %d: invalid slippage_amount %q", i, o.Convert.SlippageAmount)
				}
				cp.SlippageAmount = slipAmt
			}
			bi.ConvertParams = &cp
		case *pb.Intent_SetMetadata:
			bi.Type = "set_metadata"
			bi.TargetType = int16(o.SetMetadata.TargetType)
			bi.TargetID = o.SetMetadata.TargetId
			bi.Metadata = make(map[string]any, len(o.SetMetadata.Metadata))
			for k, v := range o.SetMetadata.Metadata {
				bi.Metadata[k] = metadataValueToAny(v)
			}
		case *pb.Intent_DeleteMetadata:
			bi.Type = "delete_metadata"
			bi.TargetType = int16(o.DeleteMetadata.TargetType)
			bi.TargetID = o.DeleteMetadata.TargetId
			bi.MetadataKey = o.DeleteMetadata.Key
		default:
			return nil, status.Errorf(codes.InvalidArgument, "batch intent %d: unsupported operation type", i)
		}
		intents[i] = bi
	}

	mode := "ALL_OR_NOTHING"
	switch op.Mode {
	case pb.BatchOperation_BEST_EFFORT:
		mode = "BEST_EFFORT"
	case pb.BatchOperation_CHECKPOINTED:
		mode = "CHECKPOINTED"
	}

	batchResult, err := s.planner.SubmitBatch(ctx, intent.LedgerId, intents, mode)
	if err != nil {
		if mode == "ALL_OR_NOTHING" || batchResult == nil {
			return nil, mapPlannerError(err)
		}
		s.logger.Warn("batch partial failure", zap.Error(err))
	}
	if batchResult == nil {
		return nil, status.Error(codes.Internal, "batch produced no results")
	}

	pbResults := make([]*pb.SubmitResponse, len(batchResult.Results))
	for i, r := range batchResult.Results {
		resp := &pb.SubmitResponse{EventId: r.EventID}
		if !r.Success && r.Error != "" {
			resp.Error = r.Error
		}
		pbResults[i] = resp
	}

	return &pb.SubmitResponse{
		Output: &pb.SubmitResponse_BatchResult{
			BatchResult: &pb.BatchResult{
				Results:   pbResults,
				Successes: int32(batchResult.Successes),
				Failures:  int32(batchResult.Failures),
			},
		},
	}, nil
}

func (s *LedgerService) handleConvert(ctx context.Context, intent *pb.Intent, op *pb.ConvertOperation) (*pb.SubmitResponse, error) {
	srcAmt, ok := new(big.Int).SetString(op.SourceAmount, 10)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid source_amount %q", op.SourceAmount)
	}
	dstAmt, ok := new(big.Int).SetString(op.DestinationAmount, 10)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid destination_amount %q", op.DestinationAmount)
	}
	var slipAmt *big.Int
	if op.SlippageAmount != "" {
		slipAmt, ok = new(big.Int).SetString(op.SlippageAmount, 10)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "invalid slippage_amount %q", op.SlippageAmount)
		}
	}

	result, err := s.planner.SubmitConvert(ctx, intent.LedgerId, planner.ConvertParams{
		Source:          op.Source,
		Destination:     op.Destination,
		SourceAmount:    srcAmt,
		SourceAsset:     op.SourceAsset,
		DestAmount:      dstAmt,
		DestAsset:       op.DestinationAsset,
		Rate:            op.Rate,
		RateSource:      op.RateSource,
		SlippageAccount: op.SlippageAccount,
		SlippageAmount:  slipAmt,
	}, intent.IdempotencyKey)
	if err != nil {
		return nil, mapPlannerError(err)
	}

	return &pb.SubmitResponse{
		EventId: result.EventID,
		Output: &pb.SubmitResponse_Conversion{
			Conversion: &pb.Conversion{
				ConversionId:      result.ConversionID,
				Source:            op.Source,
				Destination:       op.Destination,
				SourceAmount:      op.SourceAmount,
				SourceAsset:       op.SourceAsset,
				DestinationAmount: op.DestinationAmount,
				DestinationAsset:  op.DestinationAsset,
				Rate:              op.Rate,
				RateSource:        op.RateSource,
			},
		},
	}, nil
}

func (s *LedgerService) SubmitForApproval(ctx context.Context, req *pb.SubmitForApprovalRequest) (*pb.SubmitForApprovalResponse, error) {
	if req.LedgerId == "" {
		return nil, status.Error(codes.InvalidArgument, "ledger_id is required")
	}
	if len(req.IntentPayload) == 0 {
		return nil, status.Error(codes.InvalidArgument, "intent_payload is required")
	}
	if len(req.RequiredApprovers) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one required_approver is needed")
	}

	// Always derive principal from authenticated context, never trust client input.
	submittedBy, err := extractPrincipal(ctx)
	if err != nil {
		return nil, err
	}

	expiresIn := time.Duration(req.ExpiresInSeconds) * time.Second

	if err := s.authorizeLedger(ctx, req.LedgerId); err != nil {
		return nil, err
	}
	intentID, err := s.planner.SubmitForApproval(ctx, req.LedgerId, req.IntentPayload, req.RequiredApprovers, submittedBy, expiresIn)
	if err != nil {
		return nil, mapPlannerError(err)
	}

	return &pb.SubmitForApprovalResponse{IntentId: intentID}, nil
}

func (s *LedgerService) ApproveIntent(ctx context.Context, req *pb.ApproveIntentRequest) (*pb.ApproveIntentResponse, error) {
	if req.LedgerId == "" {
		return nil, status.Error(codes.InvalidArgument, "ledger_id is required")
	}
	if req.IntentId == "" {
		return nil, status.Error(codes.InvalidArgument, "intent_id is required")
	}

	// Always derive principal from authenticated context, never trust client input.
	principal, err := extractPrincipal(ctx)
	if err != nil {
		return nil, err
	}

	if err := s.authorizeLedger(ctx, req.LedgerId); err != nil {
		return nil, err
	}
	result, err := s.planner.Approve(ctx, req.LedgerId, req.IntentId, principal, req.Signature)
	if err != nil {
		return nil, mapPlannerError(err)
	}

	resp := &pb.ApproveIntentResponse{}
	if result.EventID != "" && result.EventID != req.IntentId {
		// Intent was fully approved and executed.
		resp.FullyApproved = true
		resp.EventId = result.EventID
		if result.Transaction != nil {
			resp.Output = &pb.ApproveIntentResponse_Transaction{
				Transaction: txRecordToProto(result.Transaction),
			}
		}
	}

	return resp, nil
}

func (s *LedgerService) ListPendingApprovals(ctx context.Context, req *pb.ListPendingApprovalsRequest) (*pb.ListPendingApprovalsResponse, error) {
	if req.LedgerId == "" {
		return nil, status.Error(codes.InvalidArgument, "ledger_id is required")
	}

	if err := s.authorizeLedger(ctx, req.LedgerId); err != nil {
		return nil, err
	}
	recs, nextToken, err := s.store.ListPendingApprovals(ctx, req.LedgerId, storage.ListParams{
		PageSize:  int(req.PageSize),
		PageToken: req.PageToken,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing pending approvals: %v", err)
	}

	resp := &pb.ListPendingApprovalsResponse{NextPageToken: nextToken}
	for i := range recs {
		resp.Approvals = append(resp.Approvals, pendingApprovalRecordToProto(&recs[i]))
	}
	return resp, nil
}

func pendingApprovalRecordToProto(rec *storage.PendingApprovalRecord) *pb.PendingApproval {
	pa := &pb.PendingApproval{
		LedgerId:          rec.LedgerID,
		IntentId:          rec.IntentID,
		IntentHash:        rec.IntentHash,
		RequiredApprovers: rec.RequiredApprovers,
		ExpiresAt:         timestamppb.New(rec.ExpiresAt),
		State:             rec.State,
		SubmittedAt:       timestamppb.New(rec.SubmittedAt),
		SubmittedBy:       rec.SubmittedBy,
	}
	for _, a := range rec.ReceivedApprovals {
		pa.ReceivedApprovals = append(pa.ReceivedApprovals, &pb.ApprovalEntry{
			Principal: a.Principal,
			Signature: a.Signature,
			SignedAt:  timestamppb.New(a.SignedAt),
		})
	}
	return pa
}

func (s *LedgerService) GetHold(ctx context.Context, req *pb.GetHoldRequest) (*pb.Hold, error) {
	if err := s.authorizeLedger(ctx, req.LedgerId); err != nil {
		return nil, err
	}
	rec, err := s.store.GetHold(ctx, req.LedgerId, req.HoldId)
	if err != nil {
		if isNotFound(err) {
			return nil, status.Errorf(codes.NotFound, "hold not found")
		}
		return nil, status.Errorf(codes.Internal, "getting hold: %v", err)
	}
	return holdRecordToProto(rec), nil
}

func (s *LedgerService) ListHolds(ctx context.Context, req *pb.ListHoldsRequest) (*pb.ListHoldsResponse, error) {
	if err := s.authorizeLedger(ctx, req.LedgerId); err != nil {
		return nil, err
	}
	recs, nextToken, err := s.store.ListHolds(ctx, req.LedgerId, storage.ListHoldsParams{
		ListParams: storage.ListParams{PageSize: int(req.PageSize), PageToken: req.PageToken},
		Account:    req.Account,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing holds: %v", err)
	}
	resp := &pb.ListHoldsResponse{NextPageToken: nextToken}
	for i := range recs {
		resp.Holds = append(resp.Holds, holdRecordToProto(&recs[i]))
	}
	return resp, nil
}

func (s *LedgerService) GetRelationships(ctx context.Context, req *pb.GetRelationshipsRequest) (*pb.GetRelationshipsResponse, error) {
	if err := s.authorizeLedger(ctx, req.LedgerId); err != nil {
		return nil, err
	}
	rels, err := s.store.GetRelationships(ctx, req.LedgerId, req.TransactionId, int(req.Depth))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "getting relationships: %v", err)
	}
	resp := &pb.GetRelationshipsResponse{}
	for _, r := range rels {
		resp.Relationships = append(resp.Relationships, &pb.Relationship{
			ParentTxId: r.ParentTxID,
			ChildTxId:  r.ChildTxID,
			Type:       pb.RelationshipType(r.RelationshipType + 1), // +1 because 0 is UNSPECIFIED
			EventId:    r.EventID,
			SystemTime: timestamppb.New(r.SystemTime),
		})
	}
	return resp, nil
}

func holdRecordToProto(rec *storage.HoldRecord) *pb.Hold {
	return &pb.Hold{
		LedgerId:         rec.LedgerID,
		HoldId:           rec.HoldID,
		Source:           rec.Source,
		DestinationHint:  rec.DestinationHint,
		Asset:            rec.Asset,
		AuthorizedAmount: rec.AuthorizedAmount.String(),
		CapturedAmount:   rec.CapturedAmount.String(),
		Voided:           rec.Voided,
		Expired:          rec.Expired,
		ExpiresAt:        timestamppb.New(rec.ExpiresAt),
		ValidTime:        timestamppb.New(rec.ValidTime),
		SystemTime:       timestamppb.New(rec.SystemTime),
	}
}

// extractPrincipal returns the authenticated principal placed in the context by
// the auth interceptor. The identity is derived from a validated credential —
// never from a client-supplied header. Authentication is mandatory, so a missing
// principal (only possible if a non-exempt RPC bypassed the auth interceptor) is
// rejected rather than attributed to an anonymous caller.
func extractPrincipal(ctx context.Context) (string, error) {
	if principal, ok := auth.PrincipalFromContext(ctx); ok && principal != "" {
		return principal, nil
	}
	return "", status.Error(codes.Unauthenticated, "authentication required")
}

// classifyIntent returns the operation type name and list of accounts touched by the intent.
func classifyIntent(intent *pb.Intent) (string, []string) {
	var accounts []string
	var opType string
	switch op := intent.Operation.(type) {
	case *pb.Intent_Post:
		opType = "post"
		for _, p := range op.Post.Postings {
			accounts = append(accounts, p.Source, p.Destination)
		}
	case *pb.Intent_Authorize:
		opType = "authorize"
		accounts = append(accounts, op.Authorize.Source)
	case *pb.Intent_Capture:
		opType = "capture"
	case *pb.Intent_Void:
		opType = "void"
	case *pb.Intent_Revert:
		opType = "revert"
	case *pb.Intent_Amend:
		opType = "amend"
	case *pb.Intent_Convert:
		opType = "convert"
		accounts = append(accounts, op.Convert.Source, op.Convert.Destination)
	case *pb.Intent_Batch:
		opType = "batch"
	case *pb.Intent_SetMetadata:
		opType = "set_metadata"
		if op.SetMetadata.TargetType == pb.SetMetadataOperation_ACCOUNT {
			accounts = append(accounts, op.SetMetadata.TargetId)
		}
	case *pb.Intent_DeleteMetadata:
		opType = "delete_metadata"
		if op.DeleteMetadata.TargetType == pb.DeleteMetadataOperation_ACCOUNT {
			accounts = append(accounts, op.DeleteMetadata.TargetId)
		}
	case *pb.Intent_InsertSchema:
		opType = "insert_schema"
	default:
		opType = "unknown"
	}
	return opType, accounts
}

// Module provides the LedgerService to the fx container.
var Module = fx.Module("api",
	fx.Provide(
		NewLedgerService,
		subscriptions.NewManager,
	),
)
