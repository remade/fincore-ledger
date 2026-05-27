package accounts

import (
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		address string
		wantErr bool
	}{
		{"users:1234", false},
		{"users:1234:wallet", false},
		{"_world", false},
		{"fees:platform", false},
		{"a", false},
		{"a-b_c:d-e", false},
		{"", true},
		{"users:1234:wallet:sub:account:deep", false},
		{"users:", true},
		{":users", true},
		{"users::wallet", true},
		{"users:1234!wallet", true},
		{"users 1234", true},
		{strings.Repeat("a", MaxAddressLength), false},
		{strings.Repeat("a", MaxAddressLength+1), true},
	}

	for _, tt := range tests {
		t.Run(tt.address, func(t *testing.T) {
			err := Validate(tt.address)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.address, err, tt.wantErr)
			}
		})
	}
}

func TestIsIssuer(t *testing.T) {
	issuers := []string{"_world", "_treasury"}

	if !IsIssuer("_world", issuers) {
		t.Error("expected _world to be an issuer")
	}
	if !IsIssuer("_treasury", issuers) {
		t.Error("expected _treasury to be an issuer")
	}
	if IsIssuer("users:1234", issuers) {
		t.Error("expected users:1234 to not be an issuer")
	}
}

func TestSegments(t *testing.T) {
	got := Segments("users:1234:wallet")
	if len(got) != 3 || got[0] != "users" || got[1] != "1234" || got[2] != "wallet" {
		t.Errorf("Segments(\"users:1234:wallet\") = %v", got)
	}
}

func TestMatchPattern(t *testing.T) {
	tests := []struct {
		address string
		pattern string
		want    bool
	}{
		{"users:1234", "users:*", true},
		{"users:1234:wallet", "users:*:wallet", true},
		{"users:1234:hold", "users:*:wallet", false},
		{"fees:platform", "fees:*", true},
		{"users:1234", "merchants:*", false},
		{"users:1234:wallet", "users:*", false}, // different depth
	}

	for _, tt := range tests {
		t.Run(tt.address+"~"+tt.pattern, func(t *testing.T) {
			got := MatchPattern(tt.address, tt.pattern)
			if got != tt.want {
				t.Errorf("MatchPattern(%q, %q) = %v, want %v", tt.address, tt.pattern, got, tt.want)
			}
		})
	}
}
