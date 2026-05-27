//go:build conformance

package conformance

import (
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/remade/ledger/pkg/proto/ledger/v1"
)

// T-070 [P2] Authorized funds are unavailable.
func TestT070_AuthorizedFundsUnavailable(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t070")

	// Fund users:1 with 100.
	h.Client.NewTransaction(ledger).Post("_world", "users:1", "100", "USD/2").Submit(ctx)

	// Authorize 30 from users:1.
	h.Client.GRPC().Submit(ctx, &pb.SubmitRequest{Intent: &pb.Intent{
		LedgerId: ledger,
		Operation: &pb.Intent_Authorize{Authorize: &pb.AuthorizeOperation{
			Source: "users:1", Asset: "USD/2", Amount: "30",
			ExpiresAt: timestamppb.New(time.Now().Add(1 * time.Hour)),
		}},
	}})

	// Try to post 80 from users:1 — should fail (only 70 available).
	_, err := h.Client.NewTransaction(ledger).Post("users:1", "users:2", "80", "USD/2").Submit(ctx)
	if err == nil {
		t.Fatal("expected insufficient funds, got nil")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Errorf("expected FailedPrecondition, got %v", st.Code())
	}
}

// T-071 [P2] Capture up to authorized amount.
func TestT071_CaptureUpToAuthorized(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t071")

	h.Client.NewTransaction(ledger).Post("_world", "users:1", "100", "USD/2").Submit(ctx)

	resp, _ := h.Client.GRPC().Submit(ctx, &pb.SubmitRequest{Intent: &pb.Intent{
		LedgerId: ledger,
		Operation: &pb.Intent_Authorize{Authorize: &pb.AuthorizeOperation{
			Source: "users:1", DestinationHint: "merchant:1", Asset: "USD/2", Amount: "30",
			ExpiresAt: timestamppb.New(time.Now().Add(1 * time.Hour)),
		}},
	}})

	holdID := resp.GetHold().HoldId

	// Capture 25 — should succeed.
	_, err := h.Client.GRPC().Submit(ctx, &pb.SubmitRequest{Intent: &pb.Intent{
		LedgerId: ledger,
		Operation: &pb.Intent_Capture{Capture: &pb.CaptureOperation{
			HoldId: holdID, Amount: "25", Destination: "merchant:1",
		}},
	}})
	if err != nil {
		t.Fatalf("capture failed: %v", err)
	}
}

// T-072 [P2] Capture exceeding authorized fails.
func TestT072_CaptureExceedingAuthorizedFails(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t072")

	h.Client.NewTransaction(ledger).Post("_world", "users:1", "100", "USD/2").Submit(ctx)

	resp, _ := h.Client.GRPC().Submit(ctx, &pb.SubmitRequest{Intent: &pb.Intent{
		LedgerId: ledger,
		Operation: &pb.Intent_Authorize{Authorize: &pb.AuthorizeOperation{
			Source: "users:1", Asset: "USD/2", Amount: "30",
			ExpiresAt: timestamppb.New(time.Now().Add(1 * time.Hour)),
		}},
	}})

	holdID := resp.GetHold().HoldId

	// Capture 35 — exceeds 30, should fail.
	_, err := h.Client.GRPC().Submit(ctx, &pb.SubmitRequest{Intent: &pb.Intent{
		LedgerId: ledger,
		Operation: &pb.Intent_Capture{Capture: &pb.CaptureOperation{
			HoldId: holdID, Amount: "35", Destination: "merchant:1",
		}},
	}})
	if err == nil {
		t.Fatal("expected error for over-capture")
	}
}

// T-073 [P2] Void releases authorized.
func TestT073_VoidReleasesAuthorized(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t073")

	h.Client.NewTransaction(ledger).Post("_world", "users:1", "100", "USD/2").Submit(ctx)

	resp, _ := h.Client.GRPC().Submit(ctx, &pb.SubmitRequest{Intent: &pb.Intent{
		LedgerId: ledger,
		Operation: &pb.Intent_Authorize{Authorize: &pb.AuthorizeOperation{
			Source: "users:1", Asset: "USD/2", Amount: "30",
			ExpiresAt: timestamppb.New(time.Now().Add(1 * time.Hour)),
		}},
	}})

	holdID := resp.GetHold().HoldId

	// Void the hold.
	_, err := h.Client.GRPC().Submit(ctx, &pb.SubmitRequest{Intent: &pb.Intent{
		LedgerId: ledger,
		Operation: &pb.Intent_Void{Void: &pb.VoidOperation{HoldId: holdID}},
	}})
	if err != nil {
		t.Fatalf("void failed: %v", err)
	}

	// Now 100 should be available again — post 90.
	_, err = h.Client.NewTransaction(ledger).Post("users:1", "users:2", "90", "USD/2").Submit(ctx)
	if err != nil {
		t.Fatalf("post after void should succeed: %v", err)
	}
}
