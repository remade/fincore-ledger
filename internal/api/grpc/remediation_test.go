package grpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/remade/ledger/internal/api/grpc/auth"
	"github.com/remade/ledger/internal/storage"
)

// authorizeLedger enforces tenant isolation: a scoped token may only touch the
// ledgers it names; "*" grants all; and an absent identity is denied because
// authentication is mandatory.
func TestAuthorizeLedger(t *testing.T) {
	s := &LedgerService{}

	// No identity in context: denied (authentication is required).
	err0 := s.authorizeLedger(context.Background(), "L1")
	require.Error(t, err0)
	assert.Equal(t, codes.Unauthenticated, status.Code(err0))

	// Scoped token, matching ledger: allowed.
	scoped := auth.ContextWithIdentity(context.Background(), auth.NewIdentity("alice", []string{"L1"}, false))
	require.NoError(t, s.authorizeLedger(scoped, "L1"))

	// Scoped token, non-matching ledger: denied.
	err := s.authorizeLedger(scoped, "L2")
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))

	// Wildcard token: allowed for any ledger.
	all := auth.ContextWithIdentity(context.Background(), auth.NewIdentity("svc", nil, true))
	require.NoError(t, s.authorizeLedger(all, "anything"))
}

// A concurrent idempotency-key conflict must surface as a retryable Aborted, not
// an opaque Internal error, so a caller can re-issue and observe the committed
// result instead of treating a successful write as a server fault.
func TestMapPlannerError_IdempotencyConflictIsAborted(t *testing.T) {
	err := mapPlannerError(fmt.Errorf("appending event: %w", storage.ErrIdempotencyKeyConflict))
	assert.Equal(t, codes.Aborted, status.Code(err))
}
