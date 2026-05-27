//go:build conformance

package conformance

import (
	"io"
	"testing"

	pb "github.com/remade/ledger/pkg/proto/ledger/v1"
)

// T-053 [P1] Export-import round-trip.
// Export a ledger, import into a new ledger, compare log events.
func TestT053_ExportImportRoundTrip(t *testing.T) {
	h := NewHarness(t)
	ctx := h.Context()
	srcLedger := h.CreateTestLedger("t053-src")

	// Post a variety of transactions.
	for i := 0; i < 10; i++ {
		_, err := h.Client.NewTransaction(srcLedger).
			Post("_world", "users:1", "100", "USD/2").
			Submit(ctx)
		if err != nil {
			t.Fatalf("submit %d failed: %v", i, err)
		}
	}
	// Move some around.
	_, err := h.Client.NewTransaction(srcLedger).
		Post("users:1", "users:2", "500", "USD/2").
		Submit(ctx)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	// Export all events.
	exportStream, err := h.Client.GRPC().Export(ctx, &pb.ExportRequest{
		LedgerId: srcLedger,
	})
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}

	var exportedEvents []*pb.LogEvent
	for {
		event, err := exportStream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("export recv failed: %v", err)
		}
		exportedEvents = append(exportedEvents, event)
	}

	if len(exportedEvents) == 0 {
		t.Fatal("exported zero events")
	}
	t.Logf("exported %d events from %s", len(exportedEvents), srcLedger)

	// Import into a new ledger.
	dstLedger := h.CreateTestLedger("t053-dst")
	importStream, err := h.Client.GRPC().Import(ctx)
	if err != nil {
		t.Fatalf("import stream creation failed: %v", err)
	}

	for _, event := range exportedEvents {
		// Rewrite ledger_id to destination.
		event.LedgerId = dstLedger
		if err := importStream.Send(event); err != nil {
			t.Fatalf("import send failed: %v", err)
		}
	}
	importResp, err := importStream.CloseAndRecv()
	if err != nil {
		t.Fatalf("import close failed: %v", err)
	}
	if importResp.EventsImported != int64(len(exportedEvents)) {
		t.Errorf("imported %d events, expected %d", importResp.EventsImported, len(exportedEvents))
	}

	// Verify: log event counts match.
	srcCount := countLogEvents(t, h, srcLedger)
	dstCount := countLogEvents(t, h, dstLedger)
	if srcCount != dstCount {
		t.Errorf("source has %d events, destination has %d", srcCount, dstCount)
	}
}
