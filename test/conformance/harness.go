package conformance

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/remade/ledger/pkg/sdk"
)

// ServerAddr returns the address of the server under test.
func ServerAddr() string {
	if addr := os.Getenv("LEDGER_TEST_SERVER_ADDR"); addr != "" {
		return addr
	}
	return "localhost:9090"
}

// Harness provides a test fixture for conformance tests.
type Harness struct {
	Client *sdk.Client
	t      *testing.T
}

// NewHarness creates a new test harness connected to the server.
func NewHarness(t *testing.T) *Harness {
	t.Helper()

	client, err := sdk.New(ServerAddr())
	if err != nil {
		t.Fatalf("connecting to server at %s: %v", ServerAddr(), err)
	}

	t.Cleanup(func() {
		client.Close()
	})

	return &Harness{
		Client: client,
		t:      t,
	}
}

// Context returns a background context.
func (h *Harness) Context() context.Context {
	return context.Background()
}

// CreateTestLedger creates a uniquely-named ledger for test isolation.
func (h *Harness) CreateTestLedger(prefix string) string {
	h.t.Helper()
	id := fmt.Sprintf("test-%s-%d", prefix, nextID())
	_, err := h.Client.CreateLedger(h.Context(), id)
	if err != nil {
		h.t.Fatalf("creating test ledger %q: %v", id, err)
	}
	return id
}

var counter uint64

func nextID() uint64 {
	counter++
	return counter
}
