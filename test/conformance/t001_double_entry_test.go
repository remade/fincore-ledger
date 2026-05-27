//go:build conformance

package conformance

import (
	"math/big"
	"testing"

	pb "github.com/remade/ledger/pkg/proto/ledger/v1"
)

// T-001 [P1] Per-asset zero-sum within transaction.
func TestT001_PerAssetZeroSumWithinTransaction(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t001")

	// Submit a 3-posting transaction.
	resp, err := h.Client.NewTransaction(ledger).
		Post("_world", "users:1", "100", "USD/2").
		Post("users:1", "users:2", "30", "USD/2").
		Post("users:1", "fees:platform", "10", "USD/2").
		Submit(ctx)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	if resp.EventId == "" {
		t.Fatal("expected non-empty event ID")
	}

	// Verify balances sum correctly.
	// _world: -100 (source of 100)
	// users:1: +100 - 30 - 10 = +60
	// users:2: +30
	// fees:platform: +10
	// Total: -100 + 60 + 30 + 10 = 0

	assertBalance(t, h, ledger, "_world", "USD/2", "-100")
	assertBalance(t, h, ledger, "users:1", "USD/2", "60")
	assertBalance(t, h, ledger, "users:2", "USD/2", "30")
	assertBalance(t, h, ledger, "fees:platform", "USD/2", "10")
}

// T-002 [P1] Per-asset zero-sum across replay.
func TestT002_PerAssetZeroSumAcrossReplay(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t002")

	// Submit 10 transactions.
	for i := 0; i < 10; i++ {
		_, err := h.Client.NewTransaction(ledger).
			Post("_world", "users:1", "100", "USD/2").
			Submit(ctx)
		if err != nil {
			t.Fatalf("submit %d failed: %v", i, err)
		}
	}

	// Move some around.
	_, err := h.Client.NewTransaction(ledger).
		Post("users:1", "users:2", "500", "USD/2").
		Submit(ctx)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	// Verify: _world + users:1 + users:2 = 0
	b1 := getBalance(t, h, ledger, "_world", "USD/2")
	b2 := getBalance(t, h, ledger, "users:1", "USD/2")
	b3 := getBalance(t, h, ledger, "users:2", "USD/2")

	sum := new(big.Int).Add(b1, new(big.Int).Add(b2, b3))
	if sum.Sign() != 0 {
		t.Errorf("zero-sum violated: _world=%s + users:1=%s + users:2=%s = %s",
			b1, b2, b3, sum)
	}
}

// T-003 [P1] Cross-asset isolation.
func TestT003_CrossAssetIsolation(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t003")

	// Post 100 USD/2 from _world to users:1.
	_, err := h.Client.NewTransaction(ledger).
		Post("_world", "users:1", "100", "USD/2").
		Submit(ctx)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	// EUR/2 balance for users:1 should be 0.
	assertBalance(t, h, ledger, "users:1", "EUR/2", "0")
}

// T-004 [P1] Self-posting produces zero net change.
func TestT004_SelfPostingZeroNetChange(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t004")

	// Fund users:1.
	_, err := h.Client.NewTransaction(ledger).
		Post("_world", "users:1", "100", "USD/2").
		Submit(ctx)
	if err != nil {
		t.Fatalf("fund failed: %v", err)
	}

	assertBalance(t, h, ledger, "users:1", "USD/2", "100")

	// Self-posting.
	_, err = h.Client.NewTransaction(ledger).
		Post("users:1", "users:1", "50", "USD/2").
		Submit(ctx)
	if err != nil {
		t.Fatalf("self-post failed: %v", err)
	}

	// Balance should be unchanged.
	assertBalance(t, h, ledger, "users:1", "USD/2", "100")
}

// --- Helpers ---

func assertBalance(t *testing.T, h *Harness, ledger, account, asset, expected string) {
	t.Helper()
	bal := getBalance(t, h, ledger, account, asset)
	if bal.String() != expected {
		t.Errorf("balance(%s, %s) = %s, want %s", account, asset, bal, expected)
	}
}

func getBalance(t *testing.T, h *Harness, ledger, account, asset string) *big.Int {
	t.Helper()
	resp, err := h.Client.GetBalance(h.Context(), ledger, account, asset)
	if err != nil {
		t.Fatalf("GetBalance(%s, %s) failed: %v", account, asset, err)
	}
	bal, ok := new(big.Int).SetString(resp.PostedBalance, 10)
	if !ok {
		t.Fatalf("invalid balance string: %q", resp.PostedBalance)
	}
	return bal
}

// silence unused import
var _ = (*pb.Ledger)(nil)
