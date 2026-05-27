//go:build conformance

package conformance

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// T-120 [P1] Dry-run produces no state change.
func TestT120_DryRunNoStateChange(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t120")

	// Dry-run a transaction.
	resp, err := h.Client.NewTransaction(ledger).
		Post("_world", "users:1", "100", "USD/2").
		DryRun().
		Submit(ctx)
	if err != nil {
		t.Fatalf("dry-run failed: %v", err)
	}
	if resp.EventId == "" {
		t.Error("expected non-empty event ID from dry-run")
	}

	// Balance should be 0 — nothing committed.
	assertBalance(t, h, ledger, "users:1", "USD/2", "0")
}

// T-121 [P1] Reference uniqueness enforced.
func TestT121_ReferenceUniqueness(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t121")

	_, err := h.Client.NewTransaction(ledger).
		Post("_world", "users:1", "100", "USD/2").
		WithReference("ref-unique").
		Submit(ctx)
	if err != nil {
		t.Fatalf("first submit failed: %v", err)
	}

	// Same reference should fail.
	_, err = h.Client.NewTransaction(ledger).
		Post("_world", "users:1", "200", "USD/2").
		WithReference("ref-unique").
		Submit(ctx)
	if err == nil {
		t.Fatal("expected error for duplicate reference")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.AlreadyExists {
		t.Errorf("expected AlreadyExists, got %v", err)
	}
}

// T-122 [P1] Reserved ledger names rejected.
func TestT122_ReservedLedgerNames(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()

	_, err := h.Client.CreateLedger(ctx, "_info")
	if err == nil {
		t.Fatal("expected error for reserved ledger name _info")
	}
}
