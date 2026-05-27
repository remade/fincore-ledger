package ir

import (
	"testing"
)

func TestChartOfAccountsValidation(t *testing.T) {
	chart := `{
		"users": {
			"$userId": {
				".pattern": "^[0-9]+$",
				"wallet": { ".self": {} },
				"hold":   { ".self": {} }
			}
		},
		"fees": {
			"platform": { ".self": {} }
		},
		"_world": { ".self": {} }
	}`

	coa, err := ParseChartOfAccounts([]byte(chart))
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	tests := []struct {
		address string
		wantErr bool
	}{
		{"users:42:wallet", false},
		{"users:42:hold", false},
		{"users:abc:wallet", true}, // pattern ^[0-9]+$ fails
		{"fees:platform", false},
		{"_world", false},
		{"unknown:account", true},
		{"users:42:unknown", true},
	}

	for _, tt := range tests {
		t.Run(tt.address, func(t *testing.T) {
			err := coa.ValidateAccount(tt.address)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateAccount(%q) error = %v, wantErr %v", tt.address, err, tt.wantErr)
			}
		})
	}
}

func TestChartOfAccountsValidatePosting(t *testing.T) {
	chart := `{
		"users": { "$id": { "wallet": { ".self": {} } } },
		"_world": { ".self": {} }
	}`

	coa, err := ParseChartOfAccounts([]byte(chart))
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	// Valid posting.
	if err := coa.ValidatePosting("_world", "users:1:wallet"); err != nil {
		t.Errorf("expected valid posting: %v", err)
	}

	// Invalid source.
	if err := coa.ValidatePosting("invalid:source", "users:1:wallet"); err == nil {
		t.Error("expected error for invalid source")
	}
}

func TestNilChart(t *testing.T) {
	var coa *ChartOfAccounts
	if err := coa.ValidateAccount("anything:goes"); err != nil {
		t.Errorf("nil chart should accept everything: %v", err)
	}
}
