package storage

// Event types used in log_events.type. These codes are the wire contract: the
// gRPC layer casts them directly to/from the protobuf EventType enum
// (pb.EventType(rec.Type)), so they MUST stay numerically aligned with
// proto/ledger/v1/ledger.proto's EventType. Do not renumber without updating the
// proto enum in lockstep.
const (
	EventTypeTransactionPosted   int16 = 1
	EventTypeHoldCreated         int16 = 2
	EventTypeHoldConfirmed       int16 = 3
	EventTypeHoldVoided          int16 = 4
	EventTypeHoldExpired         int16 = 5
	EventTypeConversionCreated   int16 = 6
	EventTypeTransactionReverted int16 = 7
	EventTypeTransactionAmended  int16 = 8
	EventTypeMetadataSet         int16 = 9
	EventTypeMetadataDeleted     int16 = 10
	EventTypeSchemaInserted      int16 = 11
	EventTypePolicyDenied        int16 = 12
	EventTypeApprovalRecorded    int16 = 13
)

// IsValidEventType reports whether t is a known log-event type code. Used to
// reject unknown types on the Import path before they reach the source-of-truth
// log.
func IsValidEventType(t int16) bool {
	return t >= EventTypeTransactionPosted && t <= EventTypeApprovalRecorded
}

// Target types for metadata records.
const (
	TargetTypeTransaction int16 = 1
)

// Relationship types for transaction relationships.
const (
	RelationshipTypeReverts int16 = 0
	RelationshipTypeAmends  int16 = 1
)
