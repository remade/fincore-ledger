package assets

import (
	"testing"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		asset   string
		wantErr bool
	}{
		{"USD", false},
		{"USD/2", false},
		{"EUR/2", false},
		{"BTC/8", false},
		{"USDC_ETH/6", false},
		{"USDC_POLYGON/6", false},
		{"A", false},
		{"A1B2C3", false},
		{"", true},
		{"usd", true},          // lowercase
		{"USD/", true},         // trailing slash
		{"123", true},          // starts with digit
		{"USD/100000", false}, // 6 digit precision
		{"USD/1000000", true}, // 7 digit precision
	}

	for _, tt := range tests {
		t.Run(tt.asset, func(t *testing.T) {
			err := Validate(tt.asset)
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate(%q) error = %v, wantErr %v", tt.asset, err, tt.wantErr)
			}
		})
	}
}

func TestParse(t *testing.T) {
	tests := []struct {
		raw       string
		ticker    string
		suffix    string
		precision int
	}{
		{"USD", "USD", "", -1},
		{"USD/2", "USD", "", 2},
		{"USDC_ETH/6", "USDC", "ETH", 6},
		{"BTC/8", "BTC", "", 8},
		{"USDC_POLYGON", "USDC", "POLYGON", -1},
	}

	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			a, err := Parse(tt.raw)
			if err != nil {
				t.Fatalf("Parse(%q) unexpected error: %v", tt.raw, err)
			}
			if a.Ticker != tt.ticker {
				t.Errorf("ticker = %q, want %q", a.Ticker, tt.ticker)
			}
			if a.Suffix != tt.suffix {
				t.Errorf("suffix = %q, want %q", a.Suffix, tt.suffix)
			}
			if a.Precision != tt.precision {
				t.Errorf("precision = %d, want %d", a.Precision, tt.precision)
			}
		})
	}
}
