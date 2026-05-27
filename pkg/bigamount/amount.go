package bigamount

import (
	"fmt"
	"math/big"
)

// Zero returns a new zero-value big.Int. Returns a new instance each call to prevent mutation.
func Zero() *big.Int { return new(big.Int) }

// Amount wraps *big.Int for type safety in the ledger domain.
// Amounts are always non-negative; direction is encoded by source→destination.
type Amount struct {
	value *big.Int
}

// New creates an Amount from an int64.
func New(v int64) Amount {
	return Amount{value: big.NewInt(v)}
}

// FromBigInt creates an Amount from an existing *big.Int.
// The value is copied.
func FromBigInt(v *big.Int) Amount {
	if v == nil {
		return Amount{value: new(big.Int)}
	}
	return Amount{value: new(big.Int).Set(v)}
}

// FromString parses a decimal string representation into an Amount.
func FromString(s string) (Amount, error) {
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return Amount{}, fmt.Errorf("invalid amount string %q", s)
	}
	return Amount{value: v}, nil
}

// MustFromString is like FromString but panics on error.
func MustFromString(s string) Amount {
	a, err := FromString(s)
	if err != nil {
		panic(err)
	}
	return a
}

// BigInt returns the underlying *big.Int. The caller must not modify it.
func (a Amount) BigInt() *big.Int {
	if a.value == nil {
		return new(big.Int)
	}
	return a.value
}

// String returns the decimal string representation.
func (a Amount) String() string {
	if a.value == nil {
		return "0"
	}
	return a.value.String()
}

// IsZero returns true if the amount is zero.
func (a Amount) IsZero() bool {
	return a.value == nil || a.value.Sign() == 0
}

// IsNegative returns true if the amount is negative.
func (a Amount) IsNegative() bool {
	return a.value != nil && a.value.Sign() < 0
}

// IsPositive returns true if the amount is positive (> 0).
func (a Amount) IsPositive() bool {
	return a.value != nil && a.value.Sign() > 0
}

// Cmp compares two amounts: -1, 0, or +1.
func (a Amount) Cmp(other Amount) int {
	return a.BigInt().Cmp(other.BigInt())
}

// Add returns a + b.
func (a Amount) Add(b Amount) Amount {
	result := new(big.Int).Add(a.BigInt(), b.BigInt())
	return Amount{value: result}
}

// Sub returns a - b.
func (a Amount) Sub(b Amount) Amount {
	result := new(big.Int).Sub(a.BigInt(), b.BigInt())
	return Amount{value: result}
}

// Neg returns -a.
func (a Amount) Neg() Amount {
	result := new(big.Int).Neg(a.BigInt())
	return Amount{value: result}
}

// Copy returns a deep copy.
func (a Amount) Copy() Amount {
	return FromBigInt(a.BigInt())
}
