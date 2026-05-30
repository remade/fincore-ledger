package grpc

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/remade/ledger/internal/storage"
)

func TestTxRecordToProto_Postings(t *testing.T) {
	rec := &storage.TransactionRecord{
		LedgerID:      "L1",
		TransactionID: "tx-1",
		EventID:       "evt-1",
		ValidTime:     time.Unix(1000, 0).UTC(),
		SystemTime:    time.Unix(2000, 0).UTC(),
		Reference:     "ref-1",
		Postings: []storage.PostingRecord{
			{Source: "alice", Destination: "bob", Amount: "100", Asset: "USD"},
			{Source: "bob", Destination: "carol", Amount: "25", Asset: "USD"},
		},
	}

	out := txRecordToProto(rec)

	require.Len(t, out.Postings, 2)
	assert.Equal(t, "alice", out.Postings[0].Source)
	assert.Equal(t, "bob", out.Postings[0].Destination)
	assert.Equal(t, "100", out.Postings[0].Amount)
	assert.Equal(t, "USD", out.Postings[0].Asset)
	assert.Equal(t, "carol", out.Postings[1].Destination)
	assert.Equal(t, "ref-1", out.Reference)
	assert.Equal(t, "tx-1", out.TransactionId)
}

func TestTxRecordToProto_NoPostings(t *testing.T) {
	out := txRecordToProto(&storage.TransactionRecord{LedgerID: "L1", TransactionID: "tx-1"})
	assert.Empty(t, out.Postings)
}

func TestMapPlannerError(t *testing.T) {
	cases := []struct {
		in   error
		want codes.Code
	}{
		{storage.ErrInsufficientFunds, codes.FailedPrecondition},
		{storage.ErrLedgerSealed, codes.FailedPrecondition},
		{storage.ErrPolicyDenied, codes.PermissionDenied},
		{storage.ErrNotFound, codes.NotFound},
		{storage.ErrTransactionReferenceConflict, codes.AlreadyExists},
		{errors.New("some unexpected internal failure"), codes.Internal},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, status.Code(mapPlannerError(tc.in)), "error %v", tc.in)
	}
}
