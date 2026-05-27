//go:build conformance

package conformance

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// T-010 [P1] Non-issuer balance never negative.
func TestT010_NonIssuerBalanceNeverNegative(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t010")

	// Try to send 100 from empty account.
	_, err := h.Client.NewTransaction(ledger).
		Post("users:1", "users:2", "100", "USD/2").
		Submit(ctx)
	if err == nil {
		t.Fatal("expected ErrInsufficientFunds, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Errorf("expected FailedPrecondition, got %v", err)
	}
}

// T-011 [P1] Issuer balance can be negative.
func TestT011_IssuerBalanceCanBeNegative(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t011")

	// _world can go negative.
	_, err := h.Client.NewTransaction(ledger).
		Post("_world", "users:1", "100", "USD/2").
		Submit(ctx)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	assertBalance(t, h, ledger, "_world", "USD/2", "-100")
}
