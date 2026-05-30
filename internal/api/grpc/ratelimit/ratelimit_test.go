package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeCounter struct {
	counts map[string]int64
	err    error
}

func newFakeCounter() *fakeCounter { return &fakeCounter{counts: map[string]int64{}} }

func (f *fakeCounter) Incr(_ context.Context, key string) (int64, error) {
	if f.err != nil {
		return 0, f.err
	}
	f.counts[key]++
	return f.counts[key], nil
}

func (f *fakeCounter) Expire(context.Context, string, time.Duration) error { return nil }

const (
	readMethod  = "/ledger.v1.LedgerService/GetLedger"
	writeMethod = "/ledger.v1.LedgerService/Submit"
)

func TestLimiter_EnforcesReadLimit(t *testing.T) {
	l := New(newFakeCounter(), 3, 100, zap.NewNop())
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		allowed, err := l.allow(ctx, readMethod)
		require.NoError(t, err)
		assert.True(t, allowed, "read %d within limit", i)
	}
	allowed, err := l.allow(ctx, readMethod)
	require.NoError(t, err)
	assert.False(t, allowed, "4th read exceeds the read limit of 3")
}

func TestLimiter_ReadAndWriteLimitsAreSeparate(t *testing.T) {
	l := New(newFakeCounter(), 100, 1, zap.NewNop())
	ctx := context.Background()

	allowed, _ := l.allow(ctx, writeMethod)
	assert.True(t, allowed, "first write allowed")
	allowed, _ = l.allow(ctx, writeMethod)
	assert.False(t, allowed, "second write exceeds the write limit of 1")

	// Reads use the separate (generous) read limit and are unaffected.
	allowed, _ = l.allow(ctx, readMethod)
	assert.True(t, allowed)
}

func TestLimiter_ZeroMeansUnlimited(t *testing.T) {
	l := New(newFakeCounter(), 0, 0, zap.NewNop())
	for i := 0; i < 500; i++ {
		allowed, err := l.allow(context.Background(), writeMethod)
		require.NoError(t, err)
		require.True(t, allowed)
	}
}

func TestLimiter_FailsOpenOnCounterError(t *testing.T) {
	l := New(&fakeCounter{err: errors.New("redis unavailable")}, 1, 1, zap.NewNop())
	allowed, err := l.allow(context.Background(), readMethod)
	require.Error(t, err)
	assert.True(t, allowed, "a counter error must fail open, not block all traffic")
}

func TestLimitFor_Classification(t *testing.T) {
	l := New(newFakeCounter(), 10, 5, zap.NewNop())
	for _, m := range []string{"Submit", "Import", "CreateLedger", "SealLedger", "SubmitForApproval", "ApproveIntent"} {
		limit, class := l.limitFor("/ledger.v1.LedgerService/" + m)
		assert.Equal(t, "w", class, m)
		assert.Equal(t, 5, limit, m)
	}
	for _, m := range []string{"GetLedger", "ListLedgers", "Export", "Subscribe", "GetBalance"} {
		limit, class := l.limitFor("/ledger.v1.LedgerService/" + m)
		assert.Equal(t, "r", class, m)
		assert.Equal(t, 10, limit, m)
	}
}

func TestUnaryInterceptor_RejectsOverLimit(t *testing.T) {
	l := New(newFakeCounter(), 1, 1, zap.NewNop())
	interceptor := l.UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: readMethod}
	called := 0
	handler := func(context.Context, any) (any, error) { called++; return "ok", nil }

	_, err := interceptor(context.Background(), nil, info, handler)
	require.NoError(t, err)
	_, err = interceptor(context.Background(), nil, info, handler)
	require.Error(t, err)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	assert.Equal(t, 1, called, "handler not invoked for the rejected request")
}

func TestPrincipalKey_AnonymousFallback(t *testing.T) {
	assert.Equal(t, "anon", principalKey(context.Background()))
}
