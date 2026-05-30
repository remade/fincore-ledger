package policy

import (
	"fmt"
	"strings"
)

// Result represents the outcome of a policy evaluation.
type Result struct {
	Denied bool
	Reason string
}

// Evaluator evaluates access control policies against a request context.
// Implementations may use different policy languages (simple rules, Cedar, etc.).
type Evaluator interface {
	// Evaluate checks whether the given request is denied by the policy.
	// policyText is the raw policy document; the format depends on the implementation.
	Evaluate(policyText, principal, operation string, touchedAccounts []string) Result
	// ValidatePolicy reports whether policyText is syntactically well-formed,
	// returning an error describing the first malformed rule line. Callers must
	// treat a validation failure as fail-closed: a deny-list policy that cannot be
	// parsed must block the operation, never silently default to allow.
	ValidatePolicy(policyText string) error
}

// SimpleEvaluator is a rule-based deny-list policy evaluator.
// It supports a line-based format:
//
//	deny all                           — deny everything
//	deny <principal> <operation> <account_pattern>
//
// Principal matching: exact match or "*" for any.
// Operation matching: exact match or "*" for any.
// Account pattern: exact match, "*" for any, or "prefix:*" for prefix match.
// Lines starting with "#" are comments. Empty lines are ignored.
// Multiple rules are evaluated top-to-bottom; first match wins.
type SimpleEvaluator struct{}

// Evaluate checks the policy text against the request context.
func (e *SimpleEvaluator) Evaluate(policyText, principal, operation string, touchedAccounts []string) Result {
	if policyText == "" {
		return Result{}
	}

	lines := strings.Split(policyText, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if line == "deny all" {
			return Result{Denied: true, Reason: "blanket deny policy"}
		}

		parts := strings.Fields(line)
		if len(parts) < 2 || parts[0] != "deny" {
			continue
		}

		// Parse: deny <principal_pattern> [<operation_pattern> [<account_pattern>]]
		principalPattern := parts[1]
		operationPattern := "*"
		accountPattern := "*"
		if len(parts) >= 3 {
			operationPattern = parts[2]
		}
		if len(parts) >= 4 {
			accountPattern = parts[3]
		}

		if !matchPattern(principalPattern, principal) {
			continue
		}
		if !matchPattern(operationPattern, operation) {
			continue
		}

		// Check if any touched account matches.
		if accountPattern == "*" {
			return Result{Denied: true, Reason: fmt.Sprintf("denied by rule: %s", line)}
		}
		for _, acct := range touchedAccounts {
			if matchPattern(accountPattern, acct) {
				return Result{Denied: true, Reason: fmt.Sprintf("denied by rule: %s (account %s)", line, acct)}
			}
		}
	}

	return Result{}
}

// ValidatePolicy checks that every rule line is well-formed, returning an error
// describing the first malformed line. Unlike Evaluate (which skips lines it does
// not recognize), this surfaces typos so a misconfigured deny rule is rejected
// up front instead of being silently ignored.
func (e *SimpleEvaluator) ValidatePolicy(policyText string) error {
	lines := strings.Split(policyText, "\n")
	for i, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") || line == "deny all" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 || parts[0] != "deny" {
			return fmt.Errorf("line %d: unrecognized rule %q (expected %q or \"deny <principal> [operation] [account]\")", i+1, line, "deny all")
		}
		if len(parts) > 4 {
			return fmt.Errorf("line %d: too many fields in rule %q (expected at most: deny <principal> <operation> <account>)", i+1, line)
		}
	}
	return nil
}

// matchPattern matches a value against a pattern supporting "*" wildcard
// and "prefix:*" prefix matching.
func matchPattern(pattern, value string) bool {
	if pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, ":*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(value, prefix)
	}
	return pattern == value
}
