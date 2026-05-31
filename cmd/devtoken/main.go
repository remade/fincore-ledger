// Command devtoken is a development-only helper for the ledger's mandatory JWT
// authentication. It generates an ephemeral RSA signing key, serves the matching
// public JWKS over HTTP (point the server's LEDGER_AUTH_JWKS_URL at it), and
// mints signed bearer tokens on demand. It must never be used in production.
package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/spf13/cobra"
)

const (
	defaultAddr       = ":8081"
	defaultKeyID      = "devtoken-key-1"
	defaultSubject    = "dev"
	defaultLedgers    = "*"
	defaultTTL        = time.Hour
	rsaKeyBits        = 2048
	readHeaderTimeout = 5 * time.Second
	shutdownTimeout   = 5 * time.Second
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "devtoken",
		Short:         "Development JWKS server and JWT minter for local ledger auth",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	cmd.AddCommand(newServeCmd())
	return cmd
}

func newServeCmd() *cobra.Command {
	var (
		addr     string
		issuer   string
		audience string
		keyID    string
	)
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Serve a JWKS at /jwks.json and mint tokens at /token",
		Long: "Serve a development JWKS and token minter.\n\n" +
			"  GET /jwks.json                       public key set for the server's LEDGER_AUTH_JWKS_URL\n" +
			"  GET /token?sub=&ledgers=&ttl=        a signed bearer token (text/plain)\n\n" +
			"ledgers is a comma-separated list of ledger IDs, or \"*\" for all.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runServe(cmd.Context(), addr, issuer, audience, keyID)
		},
	}
	cmd.Flags().StringVar(&addr, "addr", defaultAddr, "Address to listen on")
	cmd.Flags().StringVar(&issuer, "issuer", "", "Issuer (iss) claim to stamp on minted tokens")
	cmd.Flags().StringVar(&audience, "audience", "", "Audience (aud) claim to stamp on minted tokens")
	cmd.Flags().StringVar(&keyID, "kid", defaultKeyID, "Key ID (kid) for the signing key")
	return cmd
}

func runServe(parent context.Context, addr, issuer, audience, keyID string) error {
	if parent == nil {
		parent = context.Background()
	}
	signKey, err := newSigningKey(keyID)
	if err != nil {
		return fmt.Errorf("generating signing key: %w", err)
	}
	jwksBody, err := publicJWKS(signKey)
	if err != nil {
		return fmt.Errorf("building jwks: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/jwks.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(jwksBody)
	})
	mux.HandleFunc("/token", tokenHandler(signKey, issuer, audience))

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		fmt.Fprintf(os.Stderr, "devtoken serving JWKS on %s (issuer=%q audience=%q)\n", addr, issuer, audience)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("serving: %w", err)
		}
		return nil
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}

// tokenHandler mints a signed bearer token from the request's query parameters.
func tokenHandler(signKey jwk.Key, issuer, audience string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		sub := q.Get("sub")
		if sub == "" {
			sub = defaultSubject
		}
		ledgersParam := q.Get("ledgers")
		if ledgersParam == "" {
			ledgersParam = defaultLedgers
		}
		ttl := defaultTTL
		if raw := q.Get("ttl"); raw != "" {
			d, err := time.ParseDuration(raw)
			if err != nil {
				http.Error(w, fmt.Sprintf("invalid ttl: %v", err), http.StatusBadRequest)
				return
			}
			ttl = d
		}

		token, err := mintToken(signKey, sub, audience, issuer, splitLedgers(ledgersParam), time.Now().Add(ttl))
		if err != nil {
			http.Error(w, fmt.Sprintf("minting token: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(token))
	}
}

// newSigningKey generates an RSA private key as a jwk.Key with kid and RS256 set.
func newSigningKey(kid string) (jwk.Key, error) {
	raw, err := rsa.GenerateKey(rand.Reader, rsaKeyBits)
	if err != nil {
		return nil, err
	}
	key, err := jwk.FromRaw(raw)
	if err != nil {
		return nil, err
	}
	if err := key.Set(jwk.KeyIDKey, kid); err != nil {
		return nil, err
	}
	if err := key.Set(jwk.AlgorithmKey, jwa.RS256); err != nil {
		return nil, err
	}
	return key, nil
}

// publicJWKS marshals the public half of signKey as a JWKS JSON document.
func publicJWKS(signKey jwk.Key) ([]byte, error) {
	pub, err := jwk.PublicKeyOf(signKey)
	if err != nil {
		return nil, err
	}
	if err := pub.Set(jwk.KeyIDKey, signKey.KeyID()); err != nil {
		return nil, err
	}
	if err := pub.Set(jwk.AlgorithmKey, jwa.RS256); err != nil {
		return nil, err
	}
	set := jwk.NewSet()
	if err := set.AddKey(pub); err != nil {
		return nil, err
	}
	return json.Marshal(set)
}

// mintToken builds and signs a JWT with the given claims. ledgers becomes the
// "ledgers" claim (a JSON array); pass {"*"} to grant all ledgers.
func mintToken(signKey jwk.Key, sub, audience, issuer string, ledgers []string, exp time.Time) (string, error) {
	b := jwt.NewBuilder().Subject(sub).Expiration(exp)
	if audience != "" {
		b = b.Audience([]string{audience})
	}
	if issuer != "" {
		b = b.Issuer(issuer)
	}
	if len(ledgers) > 0 {
		b = b.Claim("ledgers", ledgers)
	}
	tok, err := b.Build()
	if err != nil {
		return "", err
	}
	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.RS256, signKey))
	if err != nil {
		return "", err
	}
	return string(signed), nil
}

// splitLedgers parses a comma-separated ledgers parameter, trimming whitespace
// and dropping empty entries.
func splitLedgers(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}
