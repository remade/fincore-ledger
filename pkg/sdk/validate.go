package sdk

import (
	"fmt"

	"github.com/remade/ledger/pkg/accounts"
	"github.com/remade/ledger/pkg/assets"
	"github.com/remade/ledger/pkg/bigamount"
)

// These client-side validators mirror the server's input rules (they reuse the
// same pkg/accounts and pkg/assets validators) so obvious mistakes are caught
// before a network round-trip. They are intentionally conservative: they only
// reject inputs the server would also reject.

// validateAccount checks an account address, prefixing errors with the field name.
func validateAccount(field, addr string) error {
	if err := accounts.Validate(addr); err != nil {
		return fmt.Errorf("%s: %w", field, err)
	}
	return nil
}

// validateAsset checks an asset identifier.
func validateAsset(field, asset string) error {
	if err := assets.Validate(asset); err != nil {
		return fmt.Errorf("%s: %w", field, err)
	}
	return nil
}

// validateAmount checks that amount is a well-formed positive integer string.
func validateAmount(field, amount string) error {
	a, err := bigamount.FromString(amount)
	if err != nil {
		return fmt.Errorf("%s: %w", field, err)
	}
	if !a.IsPositive() {
		return fmt.Errorf("%s: amount must be positive, got %q", field, amount)
	}
	return nil
}

// validatePosting validates a single posting's accounts, asset, and amount.
func validatePosting(source, destination, amount, asset string) error {
	if err := validateAccount("source", source); err != nil {
		return err
	}
	if err := validateAccount("destination", destination); err != nil {
		return err
	}
	if err := validateAsset("asset", asset); err != nil {
		return err
	}
	return validateAmount("amount", amount)
}
