package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/spf13/pflag"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	grpcapi "github.com/remade/ledger/internal/api/grpc"
	"github.com/remade/ledger/internal/config"
	"github.com/remade/ledger/internal/observability"
	"github.com/remade/ledger/internal/planner"
	"github.com/remade/ledger/internal/storage/postgres"
	"github.com/remade/ledger/internal/storage/redis"
	pb "github.com/remade/ledger/pkg/proto/ledger/v1"
)

func main() {
	fs := pflag.NewFlagSet("server", pflag.ExitOnError)
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
		planner.Module,
		grpcapi.Module,
		fx.Provide(newGRPCServer),
		fx.Provide(newHTTPServer),
		fx.Invoke(registerLedgerService),
		fx.Invoke(startGRPC),
		fx.Invoke(startHTTP),
	).Run()
}

func newGRPCServer(logger *zap.Logger) *grpc.Server {
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			loggingUnaryInterceptor(logger),
		),
		grpc.ChainStreamInterceptor(
			loggingStreamInterceptor(logger),
		),
	)
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(srv, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	reflection.Register(srv)
	return srv
}

func loggingUnaryInterceptor(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		logger.Debug("unary rpc",
			zap.String("method", info.FullMethod),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err),
		)
		return resp, err
	}
}

func loggingStreamInterceptor(logger *zap.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		logger.Debug("stream rpc",
			zap.String("method", info.FullMethod),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err),
		)
		return err
	}
}

func registerLedgerService(srv *grpc.Server, svc *grpcapi.LedgerService) {
	pb.RegisterLedgerServiceServer(srv, svc)
}

func startGRPC(lc fx.Lifecycle, srv *grpc.Server, cfg config.GRPCConfig, logger *zap.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
			if err != nil {
				return fmt.Errorf("listening on gRPC port %d: %w", cfg.Port, err)
			}
			logger.Info("gRPC server starting", zap.Int("port", cfg.Port))
			go srv.Serve(lis)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("gRPC server stopping")
			srv.GracefulStop()
			return nil
		},
	})
}

func newHTTPServer(cfg config.HTTPConfig, grpcCfg config.GRPCConfig, db *postgres.DB, rc *redis.Client, logger *zap.Logger) (*http.Server, error) {
	// Create the grpc-gateway mux that proxies REST requests to the gRPC server.
	gwMux := runtime.NewServeMux()
	grpcEndpoint := fmt.Sprintf("127.0.0.1:%d", grpcCfg.Port)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	if err := pb.RegisterLedgerServiceHandlerFromEndpoint(context.Background(), gwMux, grpcEndpoint, opts); err != nil {
		return nil, fmt.Errorf("registering grpc-gateway: %w", err)
	}

	// Combine health endpoints with the gateway mux.
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if err := db.Pool().Ping(ctx); err != nil {
			http.Error(w, "postgres not ready", http.StatusServiceUnavailable)
			return
		}
		if err := rc.Underlying().Ping(ctx).Err(); err != nil {
			http.Error(w, "redis not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})

	// All other paths go to the grpc-gateway (REST API).
	mux.Handle("/", gwMux)

	logger.Info("REST gateway registered",
		zap.String("grpc_endpoint", grpcEndpoint),
		zap.Int("http_port", cfg.Port),
	)

	return &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: mux,
	}, nil
}

func startHTTP(lc fx.Lifecycle, srv *http.Server, logger *zap.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("HTTP server starting", zap.String("addr", srv.Addr))
			go func() {
				if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
					logger.Fatal("HTTP server failed", zap.Error(err))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("HTTP server stopping")
			return srv.Shutdown(ctx)
		},
	})
}
