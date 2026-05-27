package accounts

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	// SegmentRegex matches a single address segment.
	SegmentRegex = `[a-zA-Z0-9_-]+`

	// MaxAddressLength is the maximum length of an account address.
	MaxAddressLength = 1024

	// World is the canonical issuer account that can have negative balances.
	World = "_world"
)

var (
	segmentRe = regexp.MustCompile(`^` + SegmentRegex + `$`)
	addressRe = regexp.MustCompile(`^` + SegmentRegex + `(:` + SegmentRegex + `)*$`)
)

// ReservedRoots are account address roots reserved for system use.
var ReservedRoots = map[string]bool{
	"_": true,
}

// Validate checks whether the given address is a valid account address.
func Validate(address string) error {
	if address == "" {
		return fmt.Errorf("account address cannot be empty")
	}
	if len(address) > MaxAddressLength {
		return fmt.Errorf("account address exceeds maximum length of %d characters", MaxAddressLength)
	}
	if !addressRe.MatchString(address) {
		return fmt.Errorf("invalid account address %q: must match %s", address, addressRe.String())
	}
	return nil
}

// IsIssuer returns true if the address is an issuer account (allowed to go negative).
func IsIssuer(address string, issuerAccounts []string) bool {
	for _, issuer := range issuerAccounts {
		if address == issuer {
			return true
		}
	}
	return false
}

// Segments splits an account address into its colon-separated segments.
func Segments(address string) []string {
	return strings.Split(address, ":")
}

// Root returns the first segment of an account address.
func Root(address string) string {
	parts := Segments(address)
	if len(parts) == 0 {
		return ""
	}
	return parts[0]
}

// IsReserved returns true if the address root is reserved for system use.
func IsReserved(address string) bool {
	root := Root(address)
	return ReservedRoots[root]
}

// MatchPattern checks if an address matches a pattern where '*' matches any single segment.
func MatchPattern(address, pattern string) bool {
	addrParts := Segments(address)
	patParts := Segments(pattern)

	if len(addrParts) != len(patParts) {
		return false
	}

	for i, pat := range patParts {
		if pat == "*" {
			continue
		}
		if pat != addrParts[i] {
			return false
		}
	}
	return true
}
