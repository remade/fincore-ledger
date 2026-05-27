//go:build conformance

package conformance

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/remade/ledger/pkg/proto/ledger/v1"
)

// T-040 [P1] balance(t_v, t_s) is stable forever.
func TestT040_BalanceStableForever(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t040")

	_, err := h.Client.NewTransaction(ledger).
		Post("_world", "users:1", "100", "USD/2").
		Submit(ctx)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	// Snapshot the balance at a point in time.
	now := time.Now().UTC()
	resp1, err := h.Client.GRPC().GetBalance(ctx, &pb.GetBalanceRequest{
		LedgerId:   ledger,
		Account:    "users:1",
		Asset:      "USD/2",
		AsOfSystem: timestamppb.New(now),
	})
	if err != nil {
		t.Fatalf("get balance failed: %v", err)
	}

	// Post more events.
	for i := 0; i < 10; i++ {
		_, err := h.Client.NewTransaction(ledger).
			Post("_world", "users:1", "50", "USD/2").
			Submit(ctx)
		if err != nil {
			t.Fatalf("submit %d failed: %v", i, err)
		}
	}

	// Re-query at the same historical point.
	resp2, err := h.Client.GRPC().GetBalance(ctx, &pb.GetBalanceRequest{
		LedgerId:   ledger,
		Account:    "users:1",
		Asset:      "USD/2",
		AsOfSystem: timestamppb.New(now),
	})
	if err != nil {
		t.Fatalf("get historical balance failed: %v", err)
	}

	if resp1.PostedBalance != resp2.PostedBalance {
		t.Errorf("historical balance changed: %s -> %s", resp1.PostedBalance, resp2.PostedBalance)
	}
}

// T-043 [P1] AS OF returns as-known.
func TestT043_AsOfReturnsAsKnown(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t043")

	beforePost := time.Now().UTC().Add(-1 * time.Second)

	_, err := h.Client.NewTransaction(ledger).
		Post("_world", "users:1", "100", "USD/2").
		Submit(ctx)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	// Query before the post — should not see it.
	resp, err := h.Client.GRPC().GetBalance(ctx, &pb.GetBalanceRequest{
		LedgerId:   ledger,
		Account:    "users:1",
		Asset:      "USD/2",
		AsOfSystem: timestamppb.New(beforePost),
	})
	if err != nil {
		t.Fatalf("get balance failed: %v", err)
	}

	if resp.PostedBalance != "0" {
		t.Errorf("expected 0 before post, got %s", resp.PostedBalance)
	}
}
