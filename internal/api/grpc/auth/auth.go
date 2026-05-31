// Package auth provides gRPC server interceptors that authenticate incoming
// requests and make the resulting principal identity available to handlers via
// the request context. Authentication is JWT-based: a bearer token carried in
// the request metadata is verified against keys published at a JWKS endpoint,
// and the token's subject (sub) claim becomes the principal.
package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jws"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// jwksMinRefreshInterval bounds how often the JWKS cache refetches keys when it
// encounters an unknown key ID, protecting the issuer from refresh storms.
const jwksMinRefreshInterval = 15 * time.Minute

// authorizationMetadataKey is the gRPC metadata key carrying the bearer token.
// The REST gateway forwards the HTTP Authorization header to this key.
const authorizationMetadataKey = "authorization"

const bearerPrefix = "Bearer "

// principalContextKey is an unexported context key type, preventing collisions
// with keys defined by other packages.
type principalContextKey struct{}

// ErrUnauthenticated indicates missing or invalid credentials. Interceptors
// translate it into codes.Unauthenticated without leaking the underlying reason
// to clients.
var ErrUnauthenticated = errors.New("unauthenticated")

// PrincipalFromContext returns the authenticated principal stored by an auth
// interceptor. ok is false only for methods exempt from authentication (health
// checks, reflection), which carry no principal.
func PrincipalFromContext(ctx context.Context) (principal string, ok bool) {
	p, ok := ctx.Value(principalContextKey{}).(string)
	return p, ok
}

func contextWithPrincipal(ctx context.Context, principal string) context.Context {
	return context.WithValue(ctx, principalContextKey{}, principal)
}

// identityContextKey is an unexported context key for the full authenticated
// Identity (principal + the ledgers the token may access).
type identityContextKey struct{}

// allLedgersWildcard in the token's "ledgers" claim grants access to every
// ledger. Use it for admin/service tokens that are not tenant-scoped.
const allLedgersWildcard = "*"

// Identity is the authenticated caller: the principal plus the set of ledgers the
// token is scoped to. It is the tenant-isolation boundary — a caller may only
// touch ledgers their token names (or all ledgers when the token carries "*").
type Identity struct {
	Principal  string
	ledgers    map[string]bool
	allLedgers bool
}

// CanAccessLedger reports whether this identity is permitted to access ledgerID.
func (i Identity) CanAccessLedger(ledgerID string) bool {
	return i.allLedgers || i.ledgers[ledgerID]
}

// AllLedgers reports whether the identity is scoped to every ledger.
func (i Identity) AllLedgers() bool { return i.allLedgers }

// IdentityFromContext returns the authenticated Identity stored by an auth
// interceptor. ok is false only for methods exempt from authentication (health
// checks, reflection), which carry no identity.
func IdentityFromContext(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(identityContextKey{}).(Identity)
	return id, ok
}

func contextWithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityContextKey{}, id)
}

// NewIdentity constructs an Identity from a principal and an explicit ledger
// allow-list (allLedgers grants access to every ledger). Exported for tests and
// for callers that establish an identity outside the JWT path.
func NewIdentity(principal string, ledgers []string, allLedgers bool) Identity {
	m := make(map[string]bool, len(ledgers))
	for _, l := range ledgers {
		m[l] = true
	}
	return Identity{Principal: principal, ledgers: m, allLedgers: allLedgers}
}

// ContextWithIdentity returns a context carrying id. Exported for tests and for
// establishing an identity outside the interceptor path.
func ContextWithIdentity(ctx context.Context, id Identity) context.Context {
	return contextWithIdentity(ctx, id)
}

// IdentityAuthenticator is implemented by authenticators that can surface the
// full Identity (principal + ledger scope), not just the principal. Interceptors
// use it when available so handlers can enforce per-ledger authorization.
type IdentityAuthenticator interface {
	AuthenticateIdentity(ctx context.Context) (Identity, error)
}

// Authenticator validates the credentials carried by an incoming request and
// returns the principal identity they represent.
type Authenticator interface {
	Authenticate(ctx context.Context) (principal string, err error)
}

// jwtAuthenticator verifies bearer JWTs against a remote JWKS endpoint.
type jwtAuthenticator struct {
	keySet   jwk.Set
	audience string
	issuer   string
}

// NewJWTAuthenticator builds an Authenticator that verifies bearer JWTs using
// the keys published at jwksURL. A non-empty audience or issuer adds the
// corresponding claim check. The provided context governs the lifetime of the
// background JWKS refresh goroutine — cancel it to release resources. The JWKS
// is fetched eagerly so a misconfigured endpoint fails fast at startup.
func NewJWTAuthenticator(ctx context.Context, jwksURL, audience, issuer string) (Authenticator, error) {
	if jwksURL == "" {
		return nil, fmt.Errorf("auth: jwks url is required")
	}
	cache := jwk.NewCache(ctx)
	if err := cache.Register(jwksURL, jwk.WithMinRefreshInterval(jwksMinRefreshInterval)); err != nil {
		return nil, fmt.Errorf("auth: registering jwks url: %w", err)
	}
	if _, err := cache.Refresh(ctx, jwksURL); err != nil {
		return nil, fmt.Errorf("auth: fetching jwks from %s: %w", jwksURL, err)
	}
	return &jwtAuthenticator{
		keySet:   jwk.NewCachedSet(cache, jwksURL),
		audience: audience,
		issuer:   issuer,
	}, nil
}

func (a *jwtAuthenticator) Authenticate(ctx context.Context) (string, error) {
	id, err := a.AuthenticateIdentity(ctx)
	if err != nil {
		return "", err
	}
	return id.Principal, nil
}

// AuthenticateIdentity verifies the bearer JWT and returns the full Identity:
// the subject (principal) plus the ledger scope from the optional "ledgers" claim.
func (a *jwtAuthenticator) AuthenticateIdentity(ctx context.Context) (Identity, error) {
	raw, err := bearerToken(ctx)
	if err != nil {
		return Identity{}, err
	}
	opts := []jwt.ParseOption{
		// InferAlgorithmFromKey lets verification succeed against JWKS keys that
		// omit the optional "alg" member (common with Auth0/Azure AD). The
		// algorithm stays constrained to those compatible with the key type, so
		// alg:none and RSA/HMAC confusion remain impossible.
		jwt.WithKeySet(a.keySet, jws.WithInferAlgorithmFromKey(true)),
		jwt.WithValidate(true),
		// Require an expiry so a token cannot remain valid indefinitely.
		jwt.WithRequiredClaim(jwt.ExpirationKey),
	}
	if a.audience != "" {
		opts = append(opts, jwt.WithAudience(a.audience))
	}
	if a.issuer != "" {
		opts = append(opts, jwt.WithIssuer(a.issuer))
	}
	token, err := jwt.Parse([]byte(raw), opts...)
	if err != nil {
		return Identity{}, fmt.Errorf("%w: %v", ErrUnauthenticated, err)
	}
	sub := token.Subject()
	if sub == "" {
		return Identity{}, fmt.Errorf("%w: token has empty subject claim", ErrUnauthenticated)
	}
	ledgers, all := parseLedgersClaim(token)
	return Identity{Principal: sub, ledgers: ledgers, allLedgers: all}, nil
}

// parseLedgersClaim reads the optional "ledgers" claim — a JSON array of ledger
// IDs the token may access, where the element "*" grants access to all ledgers.
// An absent/empty/malformed claim yields no ledger access (default-deny), so a
// token must explicitly carry its scope.
func parseLedgersClaim(token jwt.Token) (ledgers map[string]bool, allLedgers bool) {
	v, ok := token.Get("ledgers")
	if !ok {
		return nil, false
	}
	raw, ok := v.([]any)
	if !ok {
		return nil, false
	}
	ledgers = make(map[string]bool, len(raw))
	for _, e := range raw {
		s, ok := e.(string)
		if !ok {
			continue
		}
		if s == allLedgersWildcard {
			return nil, true
		}
		ledgers[s] = true
	}
	return ledgers, false
}

// bearerToken extracts the raw token from the authorization request metadata.
func bearerToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", fmt.Errorf("%w: missing request metadata", ErrUnauthenticated)
	}
	vals := md.Get(authorizationMetadataKey)
	if len(vals) == 0 {
		return "", fmt.Errorf("%w: missing authorization metadata", ErrUnauthenticated)
	}
	header := vals[0]
	if len(header) <= len(bearerPrefix) || !strings.EqualFold(header[:len(bearerPrefix)], bearerPrefix) {
		return "", fmt.Errorf("%w: authorization metadata must be a Bearer token", ErrUnauthenticated)
	}
	token := strings.TrimSpace(header[len(bearerPrefix):])
	if token == "" {
		return "", fmt.Errorf("%w: empty bearer token", ErrUnauthenticated)
	}
	return token, nil
}

// methodIsExempt reports whether a gRPC method bypasses authentication. Health
// checks and server reflection must stay reachable for load-balancer probes and
// developer tooling.
func methodIsExempt(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, "/grpc.health.v1.Health/") ||
		strings.HasPrefix(fullMethod, "/grpc.reflection.")
}

// authenticateAndInject authenticates the request and returns a context carrying
// the principal and, when the authenticator exposes it, the full Identity (so
// handlers can enforce per-ledger authorization). The original context is
// returned unchanged on error.
func authenticateAndInject(ctx context.Context, authn Authenticator) (context.Context, error) {
	if ia, ok := authn.(IdentityAuthenticator); ok {
		id, err := ia.AuthenticateIdentity(ctx)
		if err != nil {
			return ctx, err
		}
		return contextWithIdentity(contextWithPrincipal(ctx, id.Principal), id), nil
	}
	principal, err := authn.Authenticate(ctx)
	if err != nil {
		return ctx, err
	}
	return contextWithPrincipal(ctx, principal), nil
}

// NewUnaryServerInterceptor authenticates unary RPCs and injects the principal
// into the handler context. Requests that fail authentication are rejected with
// codes.Unauthenticated before reaching the handler.
func NewUnaryServerInterceptor(authn Authenticator, logger *zap.Logger) grpc.UnaryServerInterceptor {
	log := logger.Named("auth")
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if methodIsExempt(info.FullMethod) {
			return handler(ctx, req)
		}
		if authn == nil {
			// Defense in depth: a nil authenticator must never fall open.
			return nil, status.Error(codes.Unauthenticated, "authentication required")
		}
		authCtx, err := authenticateAndInject(ctx, authn)
		if err != nil {
			log.Debug("authentication rejected", zap.String("method", info.FullMethod), zap.Error(err))
			return nil, status.Error(codes.Unauthenticated, "authentication required")
		}
		return handler(authCtx, req)
	}
}

// NewStreamServerInterceptor authenticates streaming RPCs and injects the
// principal into the stream context.
func NewStreamServerInterceptor(authn Authenticator, logger *zap.Logger) grpc.StreamServerInterceptor {
	log := logger.Named("auth")
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if methodIsExempt(info.FullMethod) {
			return handler(srv, ss)
		}
		if authn == nil {
			// Defense in depth: a nil authenticator must never fall open.
			return status.Error(codes.Unauthenticated, "authentication required")
		}
		authCtx, err := authenticateAndInject(ss.Context(), authn)
		if err != nil {
			log.Debug("authentication rejected", zap.String("method", info.FullMethod), zap.Error(err))
			return status.Error(codes.Unauthenticated, "authentication required")
		}
		return handler(srv, &principalStream{ServerStream: ss, ctx: authCtx})
	}
}

// principalStream overrides the embedded stream's context so downstream handlers
// observe the authenticated principal.
type principalStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *principalStream) Context() context.Context { return s.ctx }
