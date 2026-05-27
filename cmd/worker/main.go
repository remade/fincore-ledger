package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/remade/ledger/internal/config"
	"github.com/remade/ledger/internal/log/batch"
	"github.com/remade/ledger/internal/observability"
	"github.com/remade/ledger/internal/planner"
	"github.com/remade/ledger/internal/projection/volumes"
	"github.com/remade/ledger/internal/storage"
	"github.com/remade/ledger/internal/storage/postgres"
	"github.com/remade/ledger/internal/storage/redis"
)

const jobTimeout = 5 * time.Minute

func main() {
	fs := pflag.NewFlagSet("worker", pflag.ExitOnError)
	config.RegisterFlags(fs)
	if err := fs.Parse(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "error parsing flags: %v\n", err)
		os.Exit(1)
	}

	fx.New(
		fx.Supply(fs),
		config.Module,
		observability.Module,
		postgres.Module,
		redis.Module,
		fx.Provide(postgres.NewStore),
		fx.Provide(func(s *postgres.Store) storage.Store { return s }),
		planner.Module,
		fx.Provide(func(db *postgres.DB, logger *zap.Logger) *volumes.CheckpointBuilder {
			return volumes.NewCheckpointBuilder(db.Pool(), logger)
		}),
		fx.Invoke(startWorker),
	).Run()
}

func startWorker(lc fx.Lifecycle, p *planner.Planner, bm *batch.Manager, cb *volumes.CheckpointBuilder, cfg config.WorkerConfig, logger *zap.Logger) {
	workerCtx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("worker starting",
				zap.Duration("batch_close_interval", cfg.BatchCloseInterval),
				zap.Duration("checkpoint_interval", cfg.CheckpointInterval),
				zap.Duration("hold_expiry_interval", cfg.HoldExpiryInterval),
				zap.Duration("approval_expiry_interval", cfg.ApprovalExpiryInterval),
			)

			// Batch close — close expired Merkle batches.
			wg.Add(1)
			go func() {
				defer wg.Done()
				runTickerWithBackoff(workerCtx, cfg.BatchCloseInterval, logger.Named("batch-close"), func(ctx context.Context) error {
					bm.CloseExpiredBatches(ctx)
					return nil
				})
			}()

			// Checkpoint builder — roll up volume deltas.
			wg.Add(1)
			go func() {
				defer wg.Done()
				runTickerWithBackoff(workerCtx, cfg.CheckpointInterval, logger.Named("checkpoint"), func(ctx context.Context) error {
					return cb.BuildCheckpoints(ctx)
				})
			}()

			// Hold expiry sweeper — expire holds past deadline.
			wg.Add(1)
			go func() {
				defer wg.Done()
				runTickerWithBackoff(workerCtx, cfg.HoldExpiryInterval, logger.Named("hold-expiry"), func(ctx context.Context) error {
					return p.ExpireHolds(ctx)
				})
			}()

			// Approval expiry — expire pending approvals past deadline.
			wg.Add(1)
			go func() {
				defer wg.Done()
				runTickerWithBackoff(workerCtx, cfg.ApprovalExpiryInterval, logger.Named("approval-expiry"), func(ctx context.Context) error {
					return p.ExpireStaleApprovals(ctx)
				})
			}()

			logger.Info("worker ready")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("worker stopping")
			cancel()

			// Wait for goroutines to finish with a timeout.
			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()
			select {
			case <-done:
				logger.Info("worker stopped cleanly")
			case <-time.After(30 * time.Second):
				logger.Warn("worker shutdown timed out after 30s")
			}
			return nil
		},
	})
}

func runTickerWithBackoff(ctx context.Context, interval time.Duration, logger *zap.Logger, fn func(ctx context.Context) error) {
	const maxConsecutiveFailures = 5
	consecutiveFailures := 0

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Per-job timeout prevents stuck jobs from blocking the ticker.
			jobCtx, jobCancel := context.WithTimeout(ctx, jobTimeout)
			err := fn(jobCtx)
			jobCancel()

			if err != nil {
				consecutiveFailures++
				logger.Error("job failed",
					zap.Error(err),
					zap.Int("consecutive_failures", consecutiveFailures),
				)
				// Apply exponential backoff: skip next N-1 ticks where N = min(failures, max).
				backoffTicks := consecutiveFailures
				if backoffTicks > maxConsecutiveFailures {
					backoffTicks = maxConsecutiveFailures
				}
				for i := 1; i < backoffTicks; i++ {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						// Skip tick during backoff.
					}
				}
			} else {
				consecutiveFailures = 0
			}
		}
	}
}
