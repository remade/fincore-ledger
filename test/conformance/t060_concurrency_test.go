//go:build conformance

package conformance

import (
	"fmt"
	"sync"
	"testing"
)

// T-060 [P1] N concurrent writers, sum correct.
func TestT060_ConcurrentWritersSumCorrect(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t060")

	var wg sync.WaitGroup
	var errCount int
	var mu sync.Mutex

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, err := h.Client.NewTransaction(ledger).
				Post("_world", "users:target", "10", "USD/2").
				WithIdempotencyKey(fmt.Sprintf("t060-%d", n)).
				Submit(ctx)
			if err != nil {
				mu.Lock()
				errCount++
				mu.Unlock()
				t.Logf("submit %d error: %v", n, err)
			}
		}(i)
	}
	wg.Wait()

	if errCount > 0 {
		t.Logf("%d/%d submissions failed (retryable errors expected)", errCount, 50)
	}

	// Final balance should equal successful submissions * 10.
	assertBalance(t, h, ledger, "users:target", "USD/2", fmt.Sprintf("%d", (50-errCount)*10))
}
