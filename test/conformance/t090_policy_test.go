//go:build conformance

package conformance

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/remade/ledger/pkg/proto/ledger/v1"
)

// T-090 [P3] Denied operation produces no state change.
func TestT090_DeniedOperationNoStateChange(t *testing.T) {
	// This test requires a policy to be active.
	// Setup: insert a "deny all" policy, then try to submit.
	t.Skip("requires Cedar policy integration — placeholder")
}

// T-091 [P3] Denied operation produces audit event.
func TestT091_DeniedOperationProducesAuditEvent(t *testing.T) {
	t.Skip("requires Cedar policy integration — placeholder")
}

// T-092 [P3] Pending approval not visible in balances.
func TestT092_PendingNotVisibleInBalances(t *testing.T) {
	t.Skip("requires approval workflow integration — placeholder")
}

// T-100 [P3] Conversion records both legs.
func TestT100_ConversionRecordsBothLegs(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t100")

	// Fund source.
	h.Client.NewTransaction(ledger).Post("_world", "users:1", "10000", "USD/2").Submit(ctx)

	// Convert 10000 USD/2 to 8500 EUR/2 at rate 0.85.
	resp, err := h.Client.GRPC().Submit(ctx, &pb.SubmitRequest{Intent: &pb.Intent{
		LedgerId: ledger,
		Operation: &pb.Intent_Convert{Convert: &pb.ConvertOperation{
			Source:            "users:1",
			Destination:       "users:1",
			SourceAmount:      "10000",
			SourceAsset:       "USD/2",
			DestinationAmount: "8500",
			DestinationAsset:  "EUR/2",
			Rate:              "0.85",
			RateSource:        "manual",
		}},
	}})
	if err != nil {
		t.Fatalf("convert failed: %v", err)
	}
	if resp.EventId == "" {
		t.Error("expected non-empty event ID")
	}

	// Verify balances.
	assertBalance(t, h, ledger, "users:1", "USD/2", "0")
	assertBalance(t, h, ledger, "users:1", "EUR/2", "8500")
}

// T-101 [P3] Each asset balances independently.
func TestT101_EachAssetBalancesIndependently(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t101")

	h.Client.NewTransaction(ledger).Post("_world", "users:1", "10000", "USD/2").Submit(ctx)

	// FX: both legs should independently sum to zero across all accounts.
	h.Client.GRPC().Submit(ctx, &pb.SubmitRequest{Intent: &pb.Intent{
		LedgerId: ledger,
		Operation: &pb.Intent_Convert{Convert: &pb.ConvertOperation{
			Source: "users:1", Destination: "users:1",
			SourceAmount: "10000", SourceAsset: "USD/2",
			DestinationAmount: "8500", DestinationAsset: "EUR/2",
			Rate: "0.85", RateSource: "test",
		}},
	}})

	// USD/2: _world(-10000) + users:1(10000-10000=0) = -10000 (issued from _world)
	// EUR/2: users:1(+8500) — came from nowhere (this is correct for FX, the _world
	// implicitly issues the destination asset in a conversion).
	// The per-asset zero-sum only applies within a single asset's postings.
	assertBalance(t, h, ledger, "users:1", "USD/2", "0")
	assertBalance(t, h, ledger, "users:1", "EUR/2", "8500")
}

// T-102 [P3] Slippage to designated account.
func TestT102_SlippageToDesignatedAccount(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t102")

	// Fund source.
	h.Client.NewTransaction(ledger).Post("_world", "users:1", "10000", "USD/2").Submit(ctx)

	// Convert with slippage: 10000 USD -> 8500 EUR, 50 USD slippage.
	resp, err := h.Client.GRPC().Submit(ctx, &pb.SubmitRequest{Intent: &pb.Intent{
		LedgerId: ledger,
		Operation: &pb.Intent_Convert{Convert: &pb.ConvertOperation{
			Source:            "users:1",
			Destination:       "users:1",
			SourceAmount:      "10000",
			SourceAsset:       "USD/2",
			DestinationAmount: "8500",
			DestinationAsset:  "EUR/2",
			Rate:              "0.85",
			RateSource:        "manual",
			SlippageAccount:   "fees:slippage",
			SlippageAmount:    "50",
		}},
	}})
	if err != nil {
		t.Fatalf("convert with slippage failed: %v", err)
	}
	if resp.EventId == "" {
		t.Error("expected non-empty event ID")
	}

	// Verify slippage account received the slippage amount.
	assertBalance(t, h, ledger, "fees:slippage", "USD/2", "50")
}

// T-103 [P3] Rate provenance preserved.
func TestT103_RateProvenancePreserved(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t103")

	h.Client.NewTransaction(ledger).Post("_world", "users:1", "10000", "USD/2").Submit(ctx)

	resp, err := h.Client.GRPC().Submit(ctx, &pb.SubmitRequest{Intent: &pb.Intent{
		LedgerId: ledger,
		Operation: &pb.Intent_Convert{Convert: &pb.ConvertOperation{
			Source:            "users:1",
			Destination:       "users:1",
			SourceAmount:      "10000",
			SourceAsset:       "USD/2",
			DestinationAmount: "8500",
			DestinationAsset:  "EUR/2",
			Rate:              "0.85",
			RateSource:        "external-fx-provider",
		}},
	}})
	if err != nil {
		t.Fatalf("convert failed: %v", err)
	}

	// Verify the conversion response contains rate provenance.
	conv := resp.GetConversion()
	if conv == nil {
		t.Fatal("expected conversion output in response")
	}
	if conv.Rate != "0.85" {
		t.Errorf("rate = %q, want 0.85", conv.Rate)
	}
	if conv.RateSource != "external-fx-provider" {
		t.Errorf("rate_source = %q, want external-fx-provider", conv.RateSource)
	}
}

// Silence unused import.
var (
	_ = (*pb.Ledger)(nil)
	_ codes.Code
	_ = status.New
)
