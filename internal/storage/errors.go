package storage

import "errors"

var (
	ErrNotFound                     = errors.New("not found")
	ErrAlreadyExists                = errors.New("already exists")
	ErrInsufficientFunds            = errors.New("insufficient funds")
	ErrInvalidIdempotencyInput      = errors.New("idempotency key exists with different input")
	ErrIdempotencyKeyConflict       = errors.New("concurrent idempotency key insert")
	ErrLedgerSealed                 = errors.New("ledger is sealed")
	ErrTransactionReferenceConflict = errors.New("transaction reference already exists")
	ErrAlreadyReverted              = errors.New("transaction already reverted")
	ErrDeadlock                     = errors.New("deadlock detected")
	ErrPolicyDenied                 = errors.New("policy denied")
	ErrHoldExpired                  = errors.New("hold is expired")
	ErrHoldVoided                   = errors.New("hold is voided")
	ErrApprovalExpired              = errors.New("approval has expired")
)
