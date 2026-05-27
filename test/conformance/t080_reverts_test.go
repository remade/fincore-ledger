//go:build conformance

package conformance

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/remade/ledger/pkg/proto/ledger/v1"
)

// T-080 [P2] Default revert refuses negative-leaving.
func TestT080_DefaultRevertRefusesNegative(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t080")

	// Post 100 to users:1, then users:1 spends 80.
	r1, _ := h.Client.NewTransaction(ledger).Post("_world", "users:1", "100", "USD/2").Submit(ctx)
	h.Client.NewTransaction(ledger).Post("users:1", "users:2", "80", "USD/2").Submit(ctx)

	// Reverting the 100 would leave users:1 at -80. Should fail without force.
	_, err := h.Client.GRPC().Submit(ctx, &pb.SubmitRequest{Intent: &pb.Intent{
		LedgerId: ledger,
		Operation: &pb.Intent_Revert{Revert: &pb.RevertOperation{
			OriginalTransactionId: r1.GetTransaction().TransactionId,
		}},
	}})
	if err == nil {
		t.Fatal("expected error for revert causing negative balance")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Errorf("expected FailedPrecondition, got %v", st.Code())
	}
}

// T-081 [P2] Force revert succeeds.
func TestT081_ForceRevertSucceeds(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t081")

	r1, _ := h.Client.NewTransaction(ledger).Post("_world", "users:1", "100", "USD/2").Submit(ctx)
	h.Client.NewTransaction(ledger).Post("users:1", "users:2", "80", "USD/2").Submit(ctx)

	_, err := h.Client.GRPC().Submit(ctx, &pb.SubmitRequest{Intent: &pb.Intent{
		LedgerId: ledger,
		Operation: &pb.Intent_Revert{Revert: &pb.RevertOperation{
			OriginalTransactionId: r1.GetTransaction().TransactionId,
			Force:                 true,
		}},
	}})
	if err != nil {
		t.Fatalf("force revert should succeed: %v", err)
	}
}

// T-082 [P2] Cannot revert twice.
func TestT082_CannotRevertTwice(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t082")

	r1, _ := h.Client.NewTransaction(ledger).Post("_world", "users:1", "100", "USD/2").Submit(ctx)
	txID := r1.GetTransaction().TransactionId

	// First revert.
	h.Client.GRPC().Submit(ctx, &pb.SubmitRequest{Intent: &pb.Intent{
		LedgerId:  ledger,
		Operation: &pb.Intent_Revert{Revert: &pb.RevertOperation{OriginalTransactionId: txID}},
	}})

	// Second revert should fail.
	_, err := h.Client.GRPC().Submit(ctx, &pb.SubmitRequest{Intent: &pb.Intent{
		LedgerId:  ledger,
		Operation: &pb.Intent_Revert{Revert: &pb.RevertOperation{OriginalTransactionId: txID}},
	}})
	if err == nil {
		t.Fatal("expected error for double revert")
	}
}

// T-083 [P2] Revert relationship queryable.
func TestT083_RevertRelationshipQueryable(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t083")

	r1, _ := h.Client.NewTransaction(ledger).Post("_world", "users:1", "100", "USD/2").Submit(ctx)
	txID := r1.GetTransaction().TransactionId

	h.Client.GRPC().Submit(ctx, &pb.SubmitRequest{Intent: &pb.Intent{
		LedgerId:  ledger,
		Operation: &pb.Intent_Revert{Revert: &pb.RevertOperation{OriginalTransactionId: txID}},
	}})

	rels, err := h.Client.GRPC().GetRelationships(ctx, &pb.GetRelationshipsRequest{
		LedgerId: ledger, TransactionId: txID, Depth: 1,
	})
	if err != nil {
		t.Fatalf("get relationships failed: %v", err)
	}
	if len(rels.Relationships) == 0 {
		t.Fatal("expected at least one relationship")
	}
	if rels.Relationships[0].Type != pb.RelationshipType_REVERTS {
		t.Errorf("expected REVERTS type, got %v", rels.Relationships[0].Type)
	}
}
