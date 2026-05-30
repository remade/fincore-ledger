package grpc

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/remade/ledger/internal/storage"
	pb "github.com/remade/ledger/pkg/proto/ledger/v1"
)

func ledgerRecordToProto(rec *storage.LedgerRecord) *pb.Ledger {
	l := &pb.Ledger{
		Id:              rec.ID,
		BucketId:        rec.BucketID,
		State:           rec.State,
		Features:        rec.Features,
		Metadata:        rec.Metadata,
		CreatedAt:       timestamppb.New(rec.CreatedAt),
		IssuerAccounts:  rec.IssuerAccounts,
	}
	if rec.SealedAt != nil {
		l.SealedAt = timestamppb.New(*rec.SealedAt)
	}
	return l
}

func txRecordToProto(rec *storage.TransactionRecord) *pb.Transaction {
	tx := &pb.Transaction{
		LedgerId:      rec.LedgerID,
		TransactionId: rec.TransactionID,
		EventId:       rec.EventID,
		ValidTime:     timestamppb.New(rec.ValidTime),
		SystemTime:    timestamppb.New(rec.SystemTime),
		Reference:     rec.Reference,
	}

	// Convert postings from the stored format.
	// After JSON round-trip through the DB, []map[string]any becomes []any,
	// so we must assert each element individually.
	if rawPostings, ok := rec.Postings.([]any); ok {
		for _, rp := range rawPostings {
			if p, ok := rp.(map[string]any); ok {
				tx.Postings = append(tx.Postings, postingMapToProto(p))
			}
		}
	} else if postings, ok := rec.Postings.([]map[string]any); ok {
		// Direct from planner (no JSON round-trip).
		for _, p := range postings {
			tx.Postings = append(tx.Postings, postingMapToProto(p))
		}
	}

	return tx
}

func accountRecordToProto(rec *storage.AccountRecord) *pb.Account {
	return &pb.Account{
		LedgerId:  rec.LedgerID,
		Address:   rec.Address,
		FirstUsage: timestamppb.New(rec.FirstUsage),
		UpdatedAt:  timestamppb.New(rec.UpdatedAt),
	}
}

func logEventRecordToProto(rec *storage.LogEventRecord) *pb.LogEvent {
	return &pb.LogEvent{
		EventId:        rec.EventID,
		LedgerId:       rec.LedgerID,
		LedgerSeq:      uint64(rec.LedgerSeq),
		SystemTime:     timestamppb.New(rec.SystemTime),
		ValidTime:      timestamppb.New(rec.ValidTime),
		Type:           pb.EventType(rec.Type),
		Payload:        rec.Payload,
		IdempotencyKey: rec.IdempotencyKey,
		BatchId:        rec.BatchID,
		SchemaVersion:  uint64(rec.SchemaVersion),
	}
}

func metadataValueToAny(v *pb.MetadataValue) any {
	if v == nil {
		return nil
	}
	switch val := v.Value.(type) {
	case *pb.MetadataValue_StringValue:
		return val.StringValue
	case *pb.MetadataValue_IntValue:
		return val.IntValue
	case *pb.MetadataValue_BoolValue:
		return val.BoolValue
	case *pb.MetadataValue_DecimalValue:
		return val.DecimalValue
	case *pb.MetadataValue_TimestampValue:
		return val.TimestampValue.AsTime()
	default:
		return nil
	}
}

func isNotFound(err error) bool {
	return errors.Is(err, storage.ErrNotFound)
}

func isAlreadyExists(err error) bool {
	return errors.Is(err, storage.ErrAlreadyExists)
}

func postingMapToProto(p map[string]any) *pb.Posting {
	return &pb.Posting{
		Source:      mapStr(p, "source"),
		Destination: mapStr(p, "destination"),
		Amount:      mapStr(p, "amount"),
		Asset:       mapStr(p, "asset"),
	}
}

func mapStr(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

func mapPlannerError(err error) error {
	switch {
	case errors.Is(err, storage.ErrInsufficientFunds):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, storage.ErrInvalidIdempotencyInput):
		return status.Error(codes.FailedPrecondition, "idempotency key exists with different input")
	case errors.Is(err, storage.ErrLedgerSealed):
		return status.Error(codes.FailedPrecondition, "ledger is sealed")
	case errors.Is(err, storage.ErrTransactionReferenceConflict):
		return status.Error(codes.AlreadyExists, "transaction reference already exists")
	case errors.Is(err, storage.ErrAlreadyReverted):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, storage.ErrHoldExpired):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, storage.ErrHoldVoided):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, storage.ErrPolicyDenied):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, storage.ErrApprovalExpired):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, storage.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Errorf(codes.Internal, "internal error")
	}
}
