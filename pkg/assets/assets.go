package assets

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Pattern matches valid asset identifiers.
// Format: TICKER[_SUFFIX][/PRECISION]
// Examples: USD, USD/2, BTC/8, USDC_ETH/6
const Pattern = `^[A-Z][A-Z0-9]{0,16}(_[A-Z]{1,16})?(\/\d{1,6})?$`

var assetRe = regexp.MustCompile(Pattern)

// Asset represents a parsed asset identifier.
type Asset struct {
	// Raw is the full asset string.
	Raw string
	// Ticker is the base asset identifier (e.g., "USD", "BTC").
	Ticker string
	// Suffix is the optional chain/network suffix (e.g., "ETH" from "USDC_ETH").
	Suffix string
	// Precision is the number of decimal places, or -1 if not specified.
	Precision int
}

// Validate checks whether the given string is a valid asset identifier.
func Validate(asset string) error {
	if asset == "" {
		return fmt.Errorf("asset cannot be empty")
	}
	if !assetRe.MatchString(asset) {
		return fmt.Errorf("invalid asset %q: must match %s", asset, Pattern)
	}
	return nil
}

// Parse parses a valid asset string into its components.
// Returns an error if the asset string is invalid.
func Parse(raw string) (Asset, error) {
	if err := Validate(raw); err != nil {
		return Asset{}, err
	}

	a := Asset{Raw: raw, Precision: -1}

	remaining := raw

	// Extract precision if present.
	if idx := strings.LastIndex(remaining, "/"); idx >= 0 {
		precStr := remaining[idx+1:]
		prec, err := strconv.Atoi(precStr)
		if err != nil {
			return Asset{}, fmt.Errorf("invalid precision in asset %q: %w", raw, err)
		}
		a.Precision = prec
		remaining = remaining[:idx]
	}

	// Extract suffix if present.
	if idx := strings.Index(remaining, "_"); idx >= 0 {
		a.Ticker = remaining[:idx]
		a.Suffix = remaining[idx+1:]
	} else {
		a.Ticker = remaining
	}

	return a, nil
}
