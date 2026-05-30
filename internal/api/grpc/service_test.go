package grpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/remade/ledger/pkg/proto/ledger/v1"
)

func validImportEvent() *pb.LogEvent {
	return &pb.LogEvent{
		EventId:    "evt-1",
		LedgerId:   "L1",
		Type:       pb.EventType(1),
		Payload:    []byte(`{}`),
		SystemTime: timestamppb.Now(),
		ValidTime:  timestamppb.Now(),
	}
}

func TestValidateImportEvent_Valid(t *testing.T) {
	require.NoError(t, validateImportEvent(validImportEvent()))
}

func TestValidateImportEvent_Rejects(t *testing.T) {
	cases := map[string]func(*pb.LogEvent){
		"empty ledger_id": func(e *pb.LogEvent) { e.LedgerId = "" },
		"empty event_id":  func(e *pb.LogEvent) { e.EventId = "" },
		"zero type":       func(e *pb.LogEvent) { e.Type = pb.EventType(0) },
		"negative type":   func(e *pb.LogEvent) { e.Type = pb.EventType(-1) },
		"unknown type":    func(e *pb.LogEvent) { e.Type = pb.EventType(9999) },
		"nil payload":     func(e *pb.LogEvent) { e.Payload = nil },
		"empty payload":   func(e *pb.LogEvent) { e.Payload = []byte{} },
		"no system_time":  func(e *pb.LogEvent) { e.SystemTime = nil },
		"no valid_time":   func(e *pb.LogEvent) { e.ValidTime = nil },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			e := validImportEvent()
			mutate(e)
			err := validateImportEvent(e)
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err), "malformed import events must be rejected as InvalidArgument")
		})
	}
}

func TestRequireImportExportAuthz(t *testing.T) {
	// Auth disabled (dev): Import/Export are open for convenience.
	open := &LedgerService{authEnabled: false}
	require.NoError(t, open.requireImportExportAuthz("anyone"))

	// Auth enabled, empty admin set: default-deny.
	closed := &LedgerService{authEnabled: true, adminPrincipals: map[string]bool{}}
	err := closed.requireImportExportAuthz("alice")
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))

	// Auth enabled with an admin allow-list: only listed principals pass.
	gated := &LedgerService{authEnabled: true, adminPrincipals: map[string]bool{"ops-admin": true}}
	require.NoError(t, gated.requireImportExportAuthz("ops-admin"))
	err = gated.requireImportExportAuthz("alice")
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}
