package policy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleEvaluator_EmptyPolicy(t *testing.T) {
	e := &SimpleEvaluator{}
	r := e.Evaluate("", "alice", "post", []string{"users:1"})
	assert.False(t, r.Denied)
}

func TestSimpleEvaluator_DenyAll(t *testing.T) {
	e := &SimpleEvaluator{}
	r := e.Evaluate("deny all", "alice", "post", []string{"users:1"})
	assert.True(t, r.Denied)
	assert.Equal(t, "blanket deny policy", r.Reason)
}

func TestSimpleEvaluator_DenySpecificPrincipal(t *testing.T) {
	e := &SimpleEvaluator{}
	policyText := "deny bob post *"

	r := e.Evaluate(policyText, "bob", "post", []string{"users:1"})
	assert.True(t, r.Denied)

	r = e.Evaluate(policyText, "alice", "post", []string{"users:1"})
	assert.False(t, r.Denied)
}

func TestSimpleEvaluator_DenySpecificOperation(t *testing.T) {
	e := &SimpleEvaluator{}
	policyText := "deny * authorize *"

	r := e.Evaluate(policyText, "alice", "authorize", []string{"users:1"})
	assert.True(t, r.Denied)

	r = e.Evaluate(policyText, "alice", "post", []string{"users:1"})
	assert.False(t, r.Denied)
}

func TestSimpleEvaluator_DenyAccountPattern(t *testing.T) {
	e := &SimpleEvaluator{}
	policyText := "deny * * restricted:*"

	r := e.Evaluate(policyText, "alice", "post", []string{"restricted:vault"})
	assert.True(t, r.Denied)
	assert.Contains(t, r.Reason, "restricted:vault")

	r = e.Evaluate(policyText, "alice", "post", []string{"users:1"})
	assert.False(t, r.Denied)
}

func TestSimpleEvaluator_DenyExactAccount(t *testing.T) {
	e := &SimpleEvaluator{}
	policyText := "deny * * treasury"

	r := e.Evaluate(policyText, "alice", "post", []string{"treasury"})
	assert.True(t, r.Denied)

	r = e.Evaluate(policyText, "alice", "post", []string{"treasury:sub"})
	assert.False(t, r.Denied)
}

func TestSimpleEvaluator_CommentsAndBlankLines(t *testing.T) {
	e := &SimpleEvaluator{}
	policyText := `# This is a comment

deny bob post *
# Another comment
`
	r := e.Evaluate(policyText, "bob", "post", []string{"users:1"})
	assert.True(t, r.Denied)

	r = e.Evaluate(policyText, "alice", "post", []string{"users:1"})
	assert.False(t, r.Denied)
}

func TestSimpleEvaluator_MultipleRulesFirstMatchWins(t *testing.T) {
	e := &SimpleEvaluator{}
	policyText := `deny alice post *
deny bob authorize *`

	r := e.Evaluate(policyText, "alice", "post", []string{"users:1"})
	assert.True(t, r.Denied)

	r = e.Evaluate(policyText, "alice", "authorize", []string{"users:1"})
	assert.False(t, r.Denied)

	r = e.Evaluate(policyText, "bob", "authorize", []string{"users:1"})
	assert.True(t, r.Denied)
}

func TestSimpleEvaluator_PrincipalOnlyRule(t *testing.T) {
	e := &SimpleEvaluator{}
	// "deny bob" means deny bob for all operations and all accounts
	policyText := "deny bob"

	r := e.Evaluate(policyText, "bob", "post", []string{"users:1"})
	assert.True(t, r.Denied)

	r = e.Evaluate(policyText, "alice", "post", []string{"users:1"})
	assert.False(t, r.Denied)
}

func TestSimpleEvaluator_NoTouchedAccounts(t *testing.T) {
	e := &SimpleEvaluator{}
	// When no accounts are touched, account-specific rules should not match
	policyText := "deny * * restricted:*"

	r := e.Evaluate(policyText, "alice", "void", nil)
	assert.False(t, r.Denied)

	// But wildcard account rules should still match
	policyText2 := "deny * * *"
	r = e.Evaluate(policyText2, "alice", "void", nil)
	assert.True(t, r.Denied)
}

func TestMatchPattern(t *testing.T) {
	tests := []struct {
		pattern string
		value   string
		want    bool
	}{
		{"*", "anything", true},
		{"exact", "exact", true},
		{"exact", "other", false},
		{"users:*", "users:123", true},
		{"users:*", "users:", true},
		{"users:*", "admin:123", false},
		{"a:b:*", "a:b:c", true},
		{"a:b:*", "a:b:", true},
		{"a:b:*", "a:c:d", false},
	}
	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.value, func(t *testing.T) {
			assert.Equal(t, tt.want, matchPattern(tt.pattern, tt.value))
		})
	}
}
