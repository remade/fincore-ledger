package conformance

import (
	"testing"
)

func TestHarnessCreation(t *testing.T) {
	addr := ServerAddr()
	if addr == "" {
		t.Fatal("ServerAddr() returned empty string")
	}
	t.Logf("conformance test harness configured for server at %s", addr)
}
