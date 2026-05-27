//go:build conformance

package conformance

import (
	"context"
	"testing"
	"time"

	pb "github.com/remade/ledger/pkg/proto/ledger/v1"
)

// T-050 [P2] Live delivery: subscribe, post a transaction, assert event received.
func TestT050_SubscriptionLiveDelivery(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t050")

	// Start subscription in background.
	subCtx, subCancel := context.WithTimeout(ctx, 10*time.Second)
	defer subCancel()

	stream, err := h.Client.GRPC().Subscribe(subCtx, &pb.SubscribeRequest{
		LedgerId: ledger,
	})
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	// Post a transaction after subscribing.
	resp, err := h.Client.NewTransaction(ledger).
		Post("_world", "users:1", "100", "USD/2").
		Submit(ctx)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	// Read from stream — expect the event to arrive.
	event, err := stream.Recv()
	if err != nil {
		t.Fatalf("stream recv failed: %v", err)
	}

	if event.Event.EventId != resp.EventId {
		t.Errorf("received event %s, want %s", event.Event.EventId, resp.EventId)
	}
	if event.Event.LedgerId != ledger {
		t.Errorf("received event ledger %s, want %s", event.Event.LedgerId, ledger)
	}
}

// T-051 [P2] Subscription with event type filter.
func TestT051_SubscriptionEventTypeFilter(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	ledger := h.CreateTestLedger("t051")

	// Subscribe only to TRANSACTION_POSTED events (type 1).
	subCtx, subCancel := context.WithTimeout(ctx, 10*time.Second)
	defer subCancel()

	stream, err := h.Client.GRPC().Subscribe(subCtx, &pb.SubscribeRequest{
		LedgerId: ledger,
		Types:    []pb.EventType{pb.EventType_TRANSACTION_POSTED},
	})
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	// Post a transaction — should be received.
	_, err = h.Client.NewTransaction(ledger).
		Post("_world", "users:1", "100", "USD/2").
		Submit(ctx)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	event, err := stream.Recv()
	if err != nil {
		t.Fatalf("stream recv failed: %v", err)
	}
	if event.Event.Type != pb.EventType_TRANSACTION_POSTED {
		t.Errorf("expected TRANSACTION_POSTED, got %v", event.Event.Type)
	}
}
