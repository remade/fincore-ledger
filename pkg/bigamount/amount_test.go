package bigamount

import (
	"math/big"
	"testing"
)

func TestNew(t *testing.T) {
	a := New(100)
	if a.String() != "100" {
		t.Errorf("New(100).String() = %q, want %q", a.String(), "100")
	}
}

func TestFromString(t *testing.T) {
	a, err := FromString("123456789012345678901234567890")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if a.String() != "123456789012345678901234567890" {
		t.Errorf("got %q", a.String())
	}

	_, err = FromString("not-a-number")
	if err == nil {
		t.Error("expected error for invalid string")
	}
}

func TestArithmetic(t *testing.T) {
	a := New(100)
	b := New(30)

	sum := a.Add(b)
	if sum.String() != "130" {
		t.Errorf("100 + 30 = %s", sum.String())
	}

	diff := a.Sub(b)
	if diff.String() != "70" {
		t.Errorf("100 - 30 = %s", diff.String())
	}

	neg := a.Neg()
	if neg.String() != "-100" {
		t.Errorf("-100 = %s", neg.String())
	}
}

func TestComparison(t *testing.T) {
	a := New(100)
	b := New(200)
	c := New(100)

	if a.Cmp(b) >= 0 {
		t.Error("expected 100 < 200")
	}
	if a.Cmp(c) != 0 {
		t.Error("expected 100 == 100")
	}
	if b.Cmp(a) <= 0 {
		t.Error("expected 200 > 100")
	}
}

func TestPredicates(t *testing.T) {
	zero := New(0)
	pos := New(42)
	neg := New(-1)

	if !zero.IsZero() {
		t.Error("0 should be zero")
	}
	if zero.IsPositive() {
		t.Error("0 should not be positive")
	}
	if zero.IsNegative() {
		t.Error("0 should not be negative")
	}
	if !pos.IsPositive() {
		t.Error("42 should be positive")
	}
	if !neg.IsNegative() {
		t.Error("-1 should be negative")
	}
}

func TestFromBigInt(t *testing.T) {
	orig := big.NewInt(999)
	a := FromBigInt(orig)

	// Modify the original — should not affect the Amount.
	orig.SetInt64(0)
	if a.String() != "999" {
		t.Errorf("FromBigInt should copy: got %s", a.String())
	}
}

func TestCopy(t *testing.T) {
	a := New(50)
	b := a.Copy()

	// They should be equal but independent.
	if a.Cmp(b) != 0 {
		t.Error("copy should be equal")
	}
	if a.BigInt() == b.BigInt() {
		t.Error("copy should not share underlying pointer")
	}
}
