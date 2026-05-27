//go:build conformance

package conformance

import (
	"testing"

	pb "github.com/remade/ledger/pkg/proto/ledger/v1"
)

// T-031 [P2] Batch ALL_OR_NOTHING rollback.
// Submit a batch of 5 intents where the 3rd fails; assert no log events written.
func TestT031_BatchAllOrNothingRollback(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t031")

	// Seed account with 100 USD/2.
	_, err := h.Client.NewTransaction(ledger).
		Post("_world", "users:1", "100", "USD/2").
		Submit(ctx)
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	// Count log events before batch.
	beforeEvents := countLogEvents(t, h, ledger)

	// Build a batch of 5 intents. The 3rd will fail because it tries to send
	// more than users:1 has (overdraft from a non-issuer account).
	batchReq := &pb.SubmitRequest{
		Intent: &pb.Intent{
			LedgerId: ledger,
			Operation: &pb.Intent_Batch{
				Batch: &pb.BatchOperation{
					Mode: pb.BatchOperation_ALL_OR_NOTHING,
					Intents: []*pb.Intent{
						postIntent(ledger, "_world", "users:2", "10", "USD/2"),
						postIntent(ledger, "_world", "users:3", "10", "USD/2"),
						postIntent(ledger, "users:1", "users:4", "9999", "USD/2"), // will fail: insufficient funds
						postIntent(ledger, "_world", "users:5", "10", "USD/2"),
						postIntent(ledger, "_world", "users:6", "10", "USD/2"),
					},
				},
			},
		},
	}

	_, err = h.Client.GRPC().Submit(ctx, batchReq)
	if err == nil {
		t.Fatal("expected batch to fail, but it succeeded")
	}

	// Verify no new log events were written (full rollback).
	afterEvents := countLogEvents(t, h, ledger)
	if afterEvents != beforeEvents {
		t.Errorf("ALL_OR_NOTHING batch should not write events on failure: before=%d, after=%d",
			beforeEvents, afterEvents)
	}

	// Verify no balances changed for the would-be recipients.
	assertBalance(t, h, ledger, "users:1", "USD/2", "100") // unchanged
}

// T-032 [P2] Batch BEST_EFFORT commits successes.
// Submit a batch of 5 intents where the 3rd fails; assert successes committed.
func TestT032_BatchBestEffortPartialSuccess(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t032")

	// Seed account.
	_, err := h.Client.NewTransaction(ledger).
		Post("_world", "users:1", "100", "USD/2").
		Submit(ctx)
	if err != nil {
		t.Fatalf("seed failed: %v", err)
	}

	// Build a batch of 5 intents. The 3rd will fail (insufficient funds).
	batchReq := &pb.SubmitRequest{
		Intent: &pb.Intent{
			LedgerId: ledger,
			Operation: &pb.Intent_Batch{
				Batch: &pb.BatchOperation{
					Mode: pb.BatchOperation_BEST_EFFORT,
					Intents: []*pb.Intent{
						postIntent(ledger, "_world", "users:2", "10", "USD/2"),
						postIntent(ledger, "_world", "users:3", "10", "USD/2"),
						postIntent(ledger, "users:1", "users:4", "9999", "USD/2"), // will fail
						postIntent(ledger, "_world", "users:5", "10", "USD/2"),
						postIntent(ledger, "_world", "users:6", "10", "USD/2"),
					},
				},
			},
		},
	}

	resp, err := h.Client.GRPC().Submit(ctx, batchReq)
	if err != nil {
		t.Fatalf("BEST_EFFORT batch should not return top-level error: %v", err)
	}

	batchResult := resp.GetBatchResult()
	if batchResult == nil {
		t.Fatal("expected batch_result in response")
	}

	if batchResult.Successes != 4 {
		t.Errorf("expected 4 successes, got %d", batchResult.Successes)
	}
	if batchResult.Failures != 1 {
		t.Errorf("expected 1 failure, got %d", batchResult.Failures)
	}

	if len(batchResult.Results) != 5 {
		t.Fatalf("expected 5 per-intent results, got %d", len(batchResult.Results))
	}

	// The 3rd result (index 2) should have an error.
	if batchResult.Results[2].Error == "" {
		t.Error("expected error in 3rd batch result")
	}

	// The other 4 should have event IDs.
	for i, r := range batchResult.Results {
		if i == 2 {
			continue
		}
		if r.EventId == "" && r.Error == "" {
			t.Errorf("batch result %d: expected event_id or error", i)
		}
	}

	// Verify the successful postings are reflected in balances.
	assertBalance(t, h, ledger, "users:2", "USD/2", "10")
	assertBalance(t, h, ledger, "users:3", "USD/2", "10")
	assertBalance(t, h, ledger, "users:5", "USD/2", "10")
	assertBalance(t, h, ledger, "users:6", "USD/2", "10")

	// users:1 should be unchanged (the failing intent didn't execute).
	assertBalance(t, h, ledger, "users:1", "USD/2", "100")
}

// --- helpers ---

func postIntent(ledgerID, source, destination, amount, asset string) *pb.Intent {
	return &pb.Intent{
		LedgerId: ledgerID,
		Operation: &pb.Intent_Post{
			Post: &pb.PostOperation{
				Postings: []*pb.Posting{{
					Source:      source,
					Destination: destination,
					Amount:      amount,
					Asset:       asset,
				}},
			},
		},
	}
}

func countLogEvents(t *testing.T, h *Harness, ledger string) int {
	t.Helper()
	resp, err := h.Client.GRPC().ListLogEvents(h.Context(), &pb.ListLogEventsRequest{
		LedgerId: ledger,
		PageSize: 1000,
	})
	if err != nil {
		t.Fatalf("ListLogEvents failed: %v", err)
	}
	return len(resp.Events)
}
