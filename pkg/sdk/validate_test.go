package sdk

import "testing"

func TestValidatePosting(t *testing.T) {
	if err := validatePosting("_world", "users:1:wallet", "100", "USD/2"); err != nil {
		t.Fatalf("valid posting rejected: %v", err)
	}

	bad := []struct {
		name                    string
		src, dst, amount, asset string
	}{
		{"bad source", "bad address!", "users:1", "100", "USD"},
		{"bad destination", "users:1", "", "100", "USD"},
		{"non-numeric amount", "users:1", "users:2", "abc", "USD"},
		{"zero amount", "users:1", "users:2", "0", "USD"},
		{"negative amount", "users:1", "users:2", "-5", "USD"},
		{"bad asset", "users:1", "users:2", "100", "bad asset!"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			if err := validatePosting(tc.src, tc.dst, tc.amount, tc.asset); err == nil {
				t.Errorf("expected validation error for %s, got nil", tc.name)
			}
		})
	}
}

func TestValidateAmount(t *testing.T) {
	if err := validateAmount("amount", "1"); err != nil {
		t.Errorf("positive amount rejected: %v", err)
	}
	for _, in := range []string{"", "0", "-1", "1.5", "x"} {
		if err := validateAmount("amount", in); err == nil {
			t.Errorf("amount %q should be rejected", in)
		}
	}
}
