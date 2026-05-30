package auth

import (
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mintTokenWithLedgers signs a JWT carrying the optional "ledgers" claim.
func mintTokenWithLedgers(t *testing.T, key jwk.Key, sub string, ledgers []string) string {
	t.Helper()
	b := jwt.NewBuilder().Subject(sub).Expiration(time.Now().Add(time.Hour))
	if ledgers != nil {
		b = b.Claim("ledgers", ledgers)
	}
	tok, err := b.Build()
	require.NoError(t, err)
	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.RS256, key))
	require.NoError(t, err)
	return string(signed)
}

// AuthenticateIdentity must surface the token's ledger scope from the "ledgers"
// claim: an explicit list grants exactly those ledgers, "*" grants all, and an
// absent claim is default-deny.
func TestAuthenticateIdentity_LedgerScope(t *testing.T) {
	signKey := newSigningKey(t, testKeyID)
	authn := newAuthenticator(t, signKey, "", "")
	ia, ok := authn.(IdentityAuthenticator)
	require.True(t, ok, "JWT authenticator must expose Identity")

	t.Run("explicit list", func(t *testing.T) {
		token := mintTokenWithLedgers(t, signKey, "alice", []string{"L1", "L2"})
		id, err := ia.AuthenticateIdentity(incomingCtx("Bearer " + token))
		require.NoError(t, err)
		assert.Equal(t, "alice", id.Principal)
		assert.True(t, id.CanAccessLedger("L1"))
		assert.True(t, id.CanAccessLedger("L2"))
		assert.False(t, id.CanAccessLedger("L3"))
		assert.False(t, id.AllLedgers())
	})

	t.Run("wildcard", func(t *testing.T) {
		token := mintTokenWithLedgers(t, signKey, "svc", []string{"*"})
		id, err := ia.AuthenticateIdentity(incomingCtx("Bearer " + token))
		require.NoError(t, err)
		assert.True(t, id.AllLedgers())
		assert.True(t, id.CanAccessLedger("anything"))
	})

	t.Run("absent claim is default-deny", func(t *testing.T) {
		token := mintTokenWithLedgers(t, signKey, "bob", nil)
		id, err := ia.AuthenticateIdentity(incomingCtx("Bearer " + token))
		require.NoError(t, err)
		assert.Equal(t, "bob", id.Principal)
		assert.False(t, id.CanAccessLedger("L1"))
		assert.False(t, id.AllLedgers())
	})
}
