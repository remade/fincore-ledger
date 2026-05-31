// Package ratelimit provides Redis-backed gRPC interceptors that cap the request
// rate per principal, with separate limits for read and write operations. The
// fixed-window counter is shared across server instances via Redis.
package ratelimit

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/remade/ledger/internal/api/grpc/auth"
)

// windowTTL bounds the lifetime of a fixed 1-second window's counter key.
const windowTTL = 2 * time.Second

// errLogInterval throttles fail-open WARN logs during a sustained counter outage.
const errLogInterval = 100

// Counter is the minimal distributed counter the limiter needs.
type Counter interface {
	Incr(ctx context.Context, key string) (int64, error)
	Expire(ctx context.Context, key string, ttl time.Duration) error
}

// RedisCounter adapts a go-redis client to Counter.
func RedisCounter(rdb *goredis.Client) Counter { return redisCounter{rdb: rdb} }

type redisCounter struct{ rdb *goredis.Client }

func (r redisCounter) Incr(ctx context.Context, key string) (int64, error) {
	return r.rdb.Incr(ctx, key).Result()
}

func (r redisCounter) Expire(ctx context.Context, key string, ttl time.Duration) error {
	return r.rdb.Expire(ctx, key, ttl).Err()
}

// writeMethods are the state-changing RPCs subject to the write limit; all other
// methods use the (typically higher) read limit.
var writeMethods = map[string]bool{
	"Submit":            true,
	"SubmitForApproval": true,
	"ApproveIntent":     true,
	"Import":            true,
	"CreateLedger":      true,
	"SealLedger":        true,
}

// Limiter enforces per-principal fixed-window rate limits. A non-positive limit
// means unlimited for that class.
type Limiter struct {
	counter  Counter
	readRPS  int
	writeRPS int
	logger   *zap.Logger
	errCount uint64 // fail-open occurrences, for throttled logging
}

// New creates a Limiter. counter is the Redis-backed shared counter.
func New(counter Counter, readRPS, writeRPS int, logger *zap.Logger) *Limiter {
	return &Limiter{counter: counter, readRPS: readRPS, writeRPS: writeRPS, logger: logger.Named("ratelimit")}
}

func (l *Limiter) limitFor(fullMethod string) (limit int, class string) {
	name := fullMethod[strings.LastIndex(fullMethod, "/")+1:]
	if writeMethods[name] {
		return l.writeRPS, "w"
	}
	return l.readRPS, "r"
}

// allow reports whether the request may proceed. On a counter error it fails
// open (allows) so a Redis blip does not take down all traffic; the caller logs.
//
// This is a fixed (not sliding) 1-second window, so a burst straddling a window
// boundary can briefly admit up to ~2x the configured rate. That is an accepted
// tradeoff for a simple, bounded, distributed counter.
func (l *Limiter) allow(ctx context.Context, fullMethod string) (bool, error) {
	limit, class := l.limitFor(fullMethod)
	if limit <= 0 {
		return true, nil
	}
	window := time.Now().Unix()
	key := fmt.Sprintf("rl:%s:%s:%d", class, principalKey(ctx), window)
	n, err := l.counter.Incr(ctx, key)
	if err != nil {
		return true, err
	}
	if n == 1 {
		// Best-effort TTL so window keys are reclaimed; an error here is harmless.
		_ = l.counter.Expire(ctx, key, windowTTL)
	}
	return n <= int64(limit), nil
}

// principalKey identifies the rate-limit subject: the authenticated principal
// when present, else the peer IP, else a shared anonymous bucket. The fallbacks
// remain necessary because the rate-limit interceptor also runs for methods
// exempt from authentication (health checks, reflection), which carry no
// principal.
func principalKey(ctx context.Context) string {
	if p, ok := auth.PrincipalFromContext(ctx); ok && p != "" {
		return "p:" + p
	}
	if pr, ok := peer.FromContext(ctx); ok && pr.Addr != nil {
		host := pr.Addr.String()
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		return "ip:" + host
	}
	return "anon"
}

// UnaryServerInterceptor enforces the rate limit on unary RPCs.
func (l *Limiter) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := l.check(ctx, info.FullMethod); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamServerInterceptor enforces the rate limit when a stream is opened.
func (l *Limiter) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := l.check(ss.Context(), info.FullMethod); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

func (l *Limiter) check(ctx context.Context, fullMethod string) error {
	allowed, err := l.allow(ctx, fullMethod)
	if err != nil {
		// Fail open, but throttle the log so a sustained Redis outage does not flood.
		if n := atomic.AddUint64(&l.errCount, 1); n == 1 || n%errLogInterval == 0 {
			l.logger.Warn("rate-limit counter error; allowing request",
				zap.String("method", fullMethod), zap.Uint64("error_total", n), zap.Error(err))
		}
		return nil
	}
	if !allowed {
		return status.Error(codes.ResourceExhausted, "rate limit exceeded; retry later")
	}
	return nil
}
