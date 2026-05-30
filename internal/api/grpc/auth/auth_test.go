package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	testKeyID = "test-key-1"
	testAud   = "ledger"
	testIss   = "https://issuer.example.com"
)

// newSigningKey returns a private jwk.Key (with kid + alg set) for signing.
func newSigningKey(t *testing.T, kid string) jwk.Key {
	t.Helper()
	raw, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	key, err := jwk.FromRaw(raw)
	require.NoError(t, err)
	require.NoError(t, key.Set(jwk.KeyIDKey, kid))
	require.NoError(t, key.Set(jwk.AlgorithmKey, jwa.RS256))
	return key
}

// serveJWKS publishes the public half of signKey at an httptest endpoint and
// returns its URL.
func serveJWKS(t *testing.T, signKey jwk.Key) string {
	t.Helper()
	pub, err := jwk.PublicKeyOf(signKey)
	require.NoError(t, err)
	require.NoError(t, pub.Set(jwk.KeyIDKey, signKey.KeyID()))
	require.NoError(t, pub.Set(jwk.AlgorithmKey, jwa.RS256))

	set := jwk.NewSet()
	require.NoError(t, set.AddKey(pub))
	body, err := json.Marshal(set)
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// mintToken builds and signs a JWT with the given claims. A zero exp omits the
// expiration claim.
func mintToken(t *testing.T, key jwk.Key, sub, aud, iss string, exp time.Time) string {
	t.Helper()
	b := jwt.NewBuilder()
	if sub != "" {
		b.Subject(sub)
	}
	if aud != "" {
		b.Audience([]string{aud})
	}
	if iss != "" {
		b.Issuer(iss)
	}
	if !exp.IsZero() {
		b.Expiration(exp)
	}
	tok, err := b.Build()
	require.NoError(t, err)
	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.RS256, key))
	require.NoError(t, err)
	return string(signed)
}

func incomingCtx(authorization string) context.Context {
	md := metadata.New(map[string]string{"authorization": authorization})
	return metadata.NewIncomingContext(context.Background(), md)
}

// newAuthenticator builds a JWT authenticator backed by a freshly served JWKS.
func newAuthenticator(t *testing.T, signKey jwk.Key, audience, issuer string) Authenticator {
	t.Helper()
	jwksURL := serveJWKS(t, signKey)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	authn, err := NewJWTAuthenticator(ctx, jwksURL, audience, issuer)
	require.NoError(t, err)
	return authn
}

func TestJWTAuthenticator_ValidToken(t *testing.T) {
	signKey := newSigningKey(t, testKeyID)
	authn := newAuthenticator(t, signKey, testAud, testIss)

	token := mintToken(t, signKey, "user-42", testAud, testIss, time.Now().Add(time.Hour))
	principal, err := authn.Authenticate(incomingCtx("Bearer " + token))
	require.NoError(t, err)
	assert.Equal(t, "user-42", principal)
}

func TestJWTAuthenticator_ValidToken_CaseInsensitiveBearer(t *testing.T) {
	signKey := newSigningKey(t, testKeyID)
	authn := newAuthenticator(t, signKey, "", "")

	token := mintToken(t, signKey, "user-7", "", "", time.Now().Add(time.Hour))
	principal, err := authn.Authenticate(incomingCtx("bearer " + token))
	require.NoError(t, err)
	assert.Equal(t, "user-7", principal)
}

func TestJWTAuthenticator_RejectsInvalidTokens(t *testing.T) {
	signKey := newSigningKey(t, testKeyID)
	otherKey := newSigningKey(t, "attacker-key")
	authn := newAuthenticator(t, signKey, testAud, testIss)

	tests := []struct {
		name  string
		token string
	}{
		{"expired", mintToken(t, signKey, "user-1", testAud, testIss, time.Now().Add(-time.Hour))},
		{"wrong audience", mintToken(t, signKey, "user-1", "other-aud", testIss, time.Now().Add(time.Hour))},
		{"wrong issuer", mintToken(t, signKey, "user-1", testAud, "https://evil.example.com", time.Now().Add(time.Hour))},
		{"empty subject", mintToken(t, signKey, "", testAud, testIss, time.Now().Add(time.Hour))},
		{"signed by unknown key", mintToken(t, otherKey, "user-1", testAud, testIss, time.Now().Add(time.Hour))},
		{"not a jwt", "garbage-token"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := authn.Authenticate(incomingCtx("Bearer " + tt.token))
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrUnauthenticated)
		})
	}
}

func TestJWTAuthenticator_MissingCredentials(t *testing.T) {
	signKey := newSigningKey(t, testKeyID)
	authn := newAuthenticator(t, signKey, "", "")

	tests := []struct {
		name string
		ctx  context.Context
	}{
		{"no metadata", context.Background()},
		{"no authorization header", metadata.NewIncomingContext(context.Background(), metadata.New(nil))},
		{"missing bearer prefix", incomingCtx("Token abc")},
		{"bearer prefix only", incomingCtx("Bearer ")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := authn.Authenticate(tt.ctx)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrUnauthenticated)
		})
	}
}

func TestNewJWTAuthenticator_Errors(t *testing.T) {
	t.Run("empty jwks url", func(t *testing.T) {
		_, err := NewJWTAuthenticator(context.Background(), "", "", "")
		require.Error(t, err)
	})
	t.Run("unreachable jwks url fails fast", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		_, err := NewJWTAuthenticator(ctx, "http://127.0.0.1:1/jwks.json", "", "")
		require.Error(t, err)
	})
}

// fixedAuthenticator is a test double returning a constant principal or error.
type fixedAuthenticator struct {
	principal string
	err       error
}

func (f fixedAuthenticator) Authenticate(context.Context) (string, error) {
	return f.principal, f.err
}

func TestUnaryInterceptor_InjectsPrincipal(t *testing.T) {
	interceptor := NewUnaryServerInterceptor(fixedAuthenticator{principal: "alice"}, zap.NewNop())

	var seen string
	var ok bool
	handler := func(ctx context.Context, _ any) (any, error) {
		seen, ok = PrincipalFromContext(ctx)
		return "resp", nil
	}
	resp, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/ledger.v1.LedgerService/Submit"}, handler)
	require.NoError(t, err)
	assert.Equal(t, "resp", resp)
	assert.True(t, ok)
	assert.Equal(t, "alice", seen)
}

func TestUnaryInterceptor_RejectsUnauthenticated(t *testing.T) {
	interceptor := NewUnaryServerInterceptor(fixedAuthenticator{err: ErrUnauthenticated}, zap.NewNop())

	called := false
	handler := func(context.Context, any) (any, error) {
		called = true
		return nil, nil
	}
	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/ledger.v1.LedgerService/Submit"}, handler)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.False(t, called, "handler must not run when authentication fails")
}

func TestUnaryInterceptor_ExemptMethodsBypassAuth(t *testing.T) {
	// Even an always-failing authenticator must not block exempt methods.
	interceptor := NewUnaryServerInterceptor(fixedAuthenticator{err: ErrUnauthenticated}, zap.NewNop())

	for _, method := range []string{
		"/grpc.health.v1.Health/Check",
		"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
	} {
		t.Run(method, func(t *testing.T) {
			called := false
			handler := func(context.Context, any) (any, error) {
				called = true
				return "ok", nil
			}
			_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: method}, handler)
			require.NoError(t, err)
			assert.True(t, called)
		})
	}
}

func TestMethodIsExempt(t *testing.T) {
	tests := map[string]bool{
		"/grpc.health.v1.Health/Check":                                   true,
		"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo":      true,
		"/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo": true,
		"/ledger.v1.LedgerService/Submit":                                false,
		"/ledger.v1.LedgerService/Export":                                false,
	}
	for method, want := range tests {
		t.Run(method, func(t *testing.T) {
			assert.Equal(t, want, methodIsExempt(method))
		})
	}
}

func TestPrincipalFromContext_Absent(t *testing.T) {
	_, ok := PrincipalFromContext(context.Background())
	assert.False(t, ok)
}

func TestJWTAuthenticator_RequiresExpiry(t *testing.T) {
	signKey := newSigningKey(t, testKeyID)
	authn := newAuthenticator(t, signKey, "", "")

	// A zero exp makes mintToken omit the expiration claim entirely.
	token := mintToken(t, signKey, "user-1", "", "", time.Time{})
	_, err := authn.Authenticate(incomingCtx("Bearer " + token))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnauthenticated)
}

// TestJWTAuthenticator_KeyWithoutAlg ensures verification still succeeds when
// the published JWKS key omits the optional "alg" member (as Auth0/Azure AD do),
// relying on algorithm inference from the key type.
func TestJWTAuthenticator_KeyWithoutAlg(t *testing.T) {
	signKey := newSigningKey(t, testKeyID)

	pub, err := jwk.PublicKeyOf(signKey)
	require.NoError(t, err)
	require.NoError(t, pub.Set(jwk.KeyIDKey, signKey.KeyID()))
	// Deliberately do NOT set jwk.AlgorithmKey on the published key.
	set := jwk.NewSet()
	require.NoError(t, set.AddKey(pub))
	body, err := json.Marshal(set)
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	authn, err := NewJWTAuthenticator(ctx, srv.URL, "", "")
	require.NoError(t, err)

	token := mintToken(t, signKey, "user-noalg", "", "", time.Now().Add(time.Hour))
	principal, err := authn.Authenticate(incomingCtx("Bearer " + token))
	require.NoError(t, err, "token must verify even when the JWKS key omits alg")
	assert.Equal(t, "user-noalg", principal)
}

// fakeServerStream is a minimal grpc.ServerStream whose Context() the stream
// interceptor overrides; the embedded nil interface is never otherwise used.
type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeServerStream) Context() context.Context { return f.ctx }

func TestStreamInterceptor_InjectsPrincipal(t *testing.T) {
	interceptor := NewStreamServerInterceptor(fixedAuthenticator{principal: "bob"}, zap.NewNop())

	var seen string
	var ok bool
	handler := func(_ any, ss grpc.ServerStream) error {
		seen, ok = PrincipalFromContext(ss.Context())
		return nil
	}
	err := interceptor(nil, &fakeServerStream{ctx: context.Background()},
		&grpc.StreamServerInfo{FullMethod: "/ledger.v1.LedgerService/Subscribe"}, handler)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "bob", seen)
}

func TestStreamInterceptor_RejectsUnauthenticated(t *testing.T) {
	interceptor := NewStreamServerInterceptor(fixedAuthenticator{err: ErrUnauthenticated}, zap.NewNop())

	called := false
	handler := func(any, grpc.ServerStream) error {
		called = true
		return nil
	}
	err := interceptor(nil, &fakeServerStream{ctx: context.Background()},
		&grpc.StreamServerInfo{FullMethod: "/ledger.v1.LedgerService/Subscribe"}, handler)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.False(t, called, "handler must not run when stream authentication fails")
}

func TestStreamInterceptor_ExemptMethodBypassesAuth(t *testing.T) {
	interceptor := NewStreamServerInterceptor(fixedAuthenticator{err: ErrUnauthenticated}, zap.NewNop())

	called := false
	handler := func(any, grpc.ServerStream) error {
		called = true
		return nil
	}
	err := interceptor(nil, &fakeServerStream{ctx: context.Background()},
		&grpc.StreamServerInfo{FullMethod: "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"}, handler)
	require.NoError(t, err)
	assert.True(t, called)
}

func TestInterceptors_NilAuthenticatorFailsClosed(t *testing.T) {
	t.Run("unary", func(t *testing.T) {
		interceptor := NewUnaryServerInterceptor(nil, zap.NewNop())
		called := false
		_, err := interceptor(context.Background(), nil,
			&grpc.UnaryServerInfo{FullMethod: "/ledger.v1.LedgerService/Submit"},
			func(context.Context, any) (any, error) { called = true; return nil, nil })
		require.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
		assert.False(t, called)
	})
	t.Run("stream", func(t *testing.T) {
		interceptor := NewStreamServerInterceptor(nil, zap.NewNop())
		called := false
		err := interceptor(nil, &fakeServerStream{ctx: context.Background()},
			&grpc.StreamServerInfo{FullMethod: "/ledger.v1.LedgerService/Subscribe"},
			func(any, grpc.ServerStream) error { called = true; return nil })
		require.Error(t, err)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
		assert.False(t, called)
	})
}
