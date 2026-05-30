package merkle

import (
	"crypto/sha256"
	"testing"
)

func TestComputeRootEmpty(t *testing.T) {
	root := ComputeRoot(nil)
	if root != nil {
		t.Error("expected nil root for empty leaves")
	}
}

func TestComputeRootSingle(t *testing.T) {
	leaf := testHash([]byte("event1"))
	root := ComputeRoot([][]byte{leaf})
	if string(root) != string(leaf) {
		t.Error("single leaf should be its own root")
	}
}

func TestComputeRootEvenLeaves(t *testing.T) {
	a := testHash([]byte("a"))
	b := testHash([]byte("b"))
	c := testHash([]byte("c"))
	d := testHash([]byte("d"))

	root := ComputeRoot([][]byte{a, b, c, d})
	if root == nil {
		t.Fatal("expected non-nil root")
	}

	// Manual computation.
	ab := internalHash(a, b)
	cd := internalHash(c, d)
	expected := internalHash(ab, cd)

	if string(root) != string(expected) {
		t.Errorf("root mismatch: got %x, want %x", root, expected)
	}
}

func TestComputeRootOddLeaves(t *testing.T) {
	a := testHash([]byte("a"))
	b := testHash([]byte("b"))
	c := testHash([]byte("c"))

	root := ComputeRoot([][]byte{a, b, c})
	if root == nil {
		t.Fatal("expected non-nil root")
	}

	// Manual: ab = H(a,b), cc = H(c,c), root = H(ab, cc).
	ab := internalHash(a, b)
	cc := internalHash(c, c)
	expected := internalHash(ab, cc)

	if string(root) != string(expected) {
		t.Errorf("root mismatch: got %x, want %x", root, expected)
	}
}

func TestVerifyEmpty(t *testing.T) {
	// Empty leaves with nil expected root — both nil roots are invalid.
	if Verify(nil, nil) {
		t.Error("empty leaves with nil expected root should verify as false")
	}
	// Empty leaves with non-nil expected root should not match.
	if Verify(nil, []byte{0x01}) {
		t.Error("empty leaves with non-nil expected root should verify as false")
	}
	// Non-empty leaves with nil expected root should not match.
	leaf := testHash([]byte("event1"))
	if Verify([][]byte{leaf}, nil) {
		t.Error("non-empty leaves with nil expected root should verify as false")
	}
}

func TestVerify(t *testing.T) {
	a := testHash([]byte("a"))
	b := testHash([]byte("b"))

	leaves := [][]byte{a, b}
	root := ComputeRoot(leaves)

	if !Verify(leaves, root) {
		t.Error("verify should pass for correct root")
	}

	// Tamper with root.
	tampered := make([]byte, len(root))
	copy(tampered, root)
	tampered[0] ^= 0xFF
	if Verify(leaves, tampered) {
		t.Error("verify should fail for tampered root")
	}
}

func testHash(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}
