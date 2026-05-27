//go:build conformance

package conformance

import (
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// T-020 [P1] Same IK + same input + sequential = single commit.
func TestT020_IdempotencySameIKSameInputSequential(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t020")

	resp1, err := h.Client.NewTransaction(ledger).
		Post("_world", "users:1", "100", "USD/2").
		WithIdempotencyKey("ik-t020").
		Submit(ctx)
	if err != nil {
		t.Fatalf("first submit failed: %v", err)
	}

	resp2, err := h.Client.NewTransaction(ledger).
		Post("_world", "users:1", "100", "USD/2").
		WithIdempotencyKey("ik-t020").
		Submit(ctx)
	if err != nil {
		t.Fatalf("second submit failed: %v", err)
	}

	if !resp2.IdempotentHit {
		t.Error("expected idempotent_hit=true on second call")
	}
	if resp1.EventId != resp2.EventId {
		t.Errorf("event IDs differ: %s vs %s", resp1.EventId, resp2.EventId)
	}

	// Only one event in the log.
	assertBalance(t, h, ledger, "users:1", "USD/2", "100")
}

// T-021 [P1] Same IK + same input + concurrent = single commit.
func TestT021_IdempotencySameIKConcurrent(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t021")

	var wg sync.WaitGroup
	results := make(chan string, 10)
	errs := make(chan error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := h.Client.NewTransaction(ledger).
				Post("_world", "users:1", "100", "USD/2").
				WithIdempotencyKey("ik-t021").
				Submit(ctx)
			if err != nil {
				errs <- err
				return
			}
			results <- resp.EventId
		}()
	}
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Errorf("concurrent submit error: %v", err)
	}

	// All should return the same event ID.
	var ids []string
	for id := range results {
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		t.Fatal("no successful responses")
	}
	for _, id := range ids[1:] {
		if id != ids[0] {
			t.Errorf("different event IDs returned: %s vs %s", ids[0], id)
		}
	}

	assertBalance(t, h, ledger, "users:1", "USD/2", "100")
}

// T-022 [P1] Same IK + different input = error.
func TestT022_IdempotencyDifferentInput(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t022")

	_, err := h.Client.NewTransaction(ledger).
		Post("_world", "users:1", "100", "USD/2").
		WithIdempotencyKey("ik-t022").
		Submit(ctx)
	if err != nil {
		t.Fatalf("first submit failed: %v", err)
	}

	// Different amount, same IK.
	_, err = h.Client.NewTransaction(ledger).
		Post("_world", "users:1", "200", "USD/2").
		WithIdempotencyKey("ik-t022").
		Submit(ctx)
	if err == nil {
		t.Fatal("expected error for different input with same IK")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Errorf("expected FailedPrecondition, got %v", err)
	}
}

// T-023 [P1] Different IK + same input = two commits.
func TestT023_DifferentIKSameInput(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t023")

	resp1, err := h.Client.NewTransaction(ledger).
		Post("_world", "users:1", "100", "USD/2").
		WithIdempotencyKey("ik-t023-a").
		Submit(ctx)
	if err != nil {
		t.Fatalf("first submit failed: %v", err)
	}

	resp2, err := h.Client.NewTransaction(ledger).
		Post("_world", "users:1", "100", "USD/2").
		WithIdempotencyKey("ik-t023-b").
		Submit(ctx)
	if err != nil {
		t.Fatalf("second submit failed: %v", err)
	}

	if resp1.EventId == resp2.EventId {
		t.Error("expected different event IDs for different IKs")
	}

	assertBalance(t, h, ledger, "users:1", "USD/2", "200")
}
