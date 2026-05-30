package planner

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithBackoffRetry_SucceedsAfterTransientFailures(t *testing.T) {
	calls := 0
	err := withBackoffRetry(context.Background(), 3, func() error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, calls, "should retry until success")
}

func TestWithBackoffRetry_ExhaustsAndReturnsLastError(t *testing.T) {
	calls := 0
	sentinel := errors.New("persistent")
	err := withBackoffRetry(context.Background(), 2, func() error {
		calls++
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)
	assert.Equal(t, 3, calls, "maxRetries=2 means 1 initial + 2 retries")
}

func TestWithBackoffRetry_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	err := withBackoffRetry(ctx, 5, func() error {
		calls++
		return errors.New("transient")
	})
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, calls, "cancelled context must stop further attempts")
}
