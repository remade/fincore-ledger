package conformance

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/remade/ledger/pkg/sdk"
)

// defaultTokenURL mints a conformance-admin token scoped to all ledgers, which
// satisfies both per-ledger authorization and the Import/Export admin gate.
const defaultTokenURL = "http://localhost:8081/token?sub=conformance-admin&ledgers=*"

// ServerAddr returns the address of the server under test.
func ServerAddr() string {
	if addr := os.Getenv("LEDGER_TEST_SERVER_ADDR"); addr != "" {
		return addr
	}
	return "localhost:9090"
}

// testToken returns the bearer token used to authenticate conformance RPCs.
// Authentication is mandatory, so every test connects with a token. It reads
// LEDGER_TEST_TOKEN when set, otherwise fetches one from the devtoken service at
// LEDGER_TEST_TOKEN_URL (default: the local devtoken /token endpoint).
func testToken(t *testing.T) string {
	t.Helper()
	if tok := os.Getenv("LEDGER_TEST_TOKEN"); tok != "" {
		return tok
	}
	url := os.Getenv("LEDGER_TEST_TOKEN_URL")
	if url == "" {
		url = defaultTokenURL
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("building token request for %s: %v", url, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("fetching test token from %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading test token from %s: %v", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("fetching test token from %s: status %d: %s", url, resp.StatusCode, body)
	}
	return strings.TrimSpace(string(body))
}

// Harness provides a test fixture for conformance tests.
type Harness struct {
	Client *sdk.Client
	t      *testing.T
}

// NewHarness creates a new test harness connected to the server.
func NewHarness(t *testing.T) *Harness {
	t.Helper()

	client, err := sdk.New(ServerAddr(), sdk.WithBearerToken(testToken(t)))
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
