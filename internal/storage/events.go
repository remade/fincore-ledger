package storage

// Event types used in log_events.type.
const (
	EventTypeTransactionPosted int16 = 1
	EventTypeHoldCreated       int16 = 2
	EventTypeHoldConfirmed     int16 = 3
	EventTypeHoldVoided        int16 = 4
	EventTypeHoldExpired       int16 = 5
	EventTypeConversionCreated int16 = 6
	EventTypeTransactionReverted int16 = 7
	EventTypeTransactionAmended int16 = 8
	EventTypeMetadataSet       int16 = 9
	EventTypeMetadataDeleted   int16 = 10
	EventTypeSchemaInserted    int16 = 11
	EventTypePolicyUpdated     int16 = 12
	EventTypeApprovalRecorded  int16 = 13
)

// Target types for metadata records.
const (
	TargetTypeTransaction int16 = 1
)

// Relationship types for transaction relationships.
const (
	RelationshipTypeReverts int16 = 0
	RelationshipTypeAmends  int16 = 1
)
