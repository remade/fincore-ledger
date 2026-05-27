package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/remade/ledger/internal/config"
)

// Client wraps a Redis client for the ledger's caching and coordination needs.
type Client struct {
	rdb    *goredis.Client
	logger *zap.Logger
}

// New creates a new Redis client.
func New(lc fx.Lifecycle, cfg config.RedisConfig, logger *zap.Logger) (*Client, error) {
	opts, err := goredis.ParseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parsing redis url: %w", err)
	}

	rdb := goredis.NewClient(opts)
	c := &Client{rdb: rdb, logger: logger.Named("redis")}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := rdb.Ping(ctx).Err(); err != nil {
				return fmt.Errorf("pinging redis: %w", err)
			}
			c.logger.Info("redis connected")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			c.logger.Info("redis disconnecting")
			return rdb.Close()
		},
	})

	return c, nil
}

// Underlying returns the raw go-redis client for advanced operations.
func (c *Client) Underlying() *goredis.Client {
	return c.rdb
}

// --- Idempotency key cache ---

func idempotencyKey(ledgerID, key string) string {
	return fmt.Sprintf("ik:%s:%s", ledgerID, key)
}

// SetIdempotencyKey stores an idempotency key with a 24h TTL.
func (c *Client) SetIdempotencyKey(ctx context.Context, ledgerID, key, eventID string, hash []byte) error {
	rkey := idempotencyKey(ledgerID, key)
	pipe := c.rdb.Pipeline()
	pipe.HSet(ctx, rkey, "event_id", eventID, "hash", hash)
	pipe.Expire(ctx, rkey, 24*time.Hour)
	_, err := pipe.Exec(ctx)
	return err
}

// GetIdempotencyKey retrieves an idempotency key from cache.
// Returns eventID, hash, and whether the key was found.
func (c *Client) GetIdempotencyKey(ctx context.Context, ledgerID, key string) (string, []byte, bool, error) {
	rkey := idempotencyKey(ledgerID, key)
	result, err := c.rdb.HGetAll(ctx, rkey).Result()
	if err != nil {
		return "", nil, false, err
	}
	if len(result) == 0 {
		return "", nil, false, nil
	}
	return result["event_id"], []byte(result["hash"]), true, nil
}

// --- Balance cache ---

// BalanceKey formats a Redis key for balance cache.
func BalanceKey(ledgerID, account, asset string) string {
	return fmt.Sprintf("bal:%s:%s:%s", ledgerID, account, asset)
}

// --- Pub/Sub ---

// SubscriptionChannel formats a Redis pub/sub channel for a ledger.
func SubscriptionChannel(ledgerID string) string {
	return fmt.Sprintf("sub:%s", ledgerID)
}

// Publish publishes a message to a ledger's subscription channel.
func (c *Client) Publish(ctx context.Context, ledgerID string, message []byte) error {
	return c.rdb.Publish(ctx, SubscriptionChannel(ledgerID), message).Err()
}

// Module provides the Redis Client to the fx container.
var Module = fx.Module("redis",
	fx.Provide(New),
)
