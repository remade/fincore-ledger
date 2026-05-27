package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/remade/ledger/internal/config"
)

// TracerShutdown is the function returned to shut down the trace provider.
type TracerShutdown func(context.Context) error

// NewTracer initializes the OpenTelemetry trace provider.
func NewTracer(lc fx.Lifecycle, cfg config.OTelConfig, logger *zap.Logger) (TracerShutdown, error) {
	if cfg.Endpoint == "" {
		logger.Info("otel disabled (no endpoint configured)")
		return func(context.Context) error { return nil }, nil
	}

	ctx := context.Background()
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("ledger"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating otel resource: %w", err)
	}

	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
	}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
		opts = append(opts, otlptracegrpc.WithInsecure())
	}

	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating otel exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			logger.Info("shutting down otel tracer")
			return tp.Shutdown(ctx)
		},
	})

	logger.Info("otel tracer initialized", zap.String("endpoint", cfg.Endpoint))
	return tp.Shutdown, nil
}

// NewLogger creates a production zap logger.
func NewLogger(cfg *config.Config) (*zap.Logger, error) {
	var logger *zap.Logger
	var err error

	if cfg.Environment == "development" {
		logger, err = zap.NewDevelopment()
	} else {
		logger, err = zap.NewProduction()
	}
	if err != nil {
		return nil, fmt.Errorf("creating logger: %w", err)
	}

	return logger, nil
}

// Module provides observability components to the fx container.
var Module = fx.Module("observability",
	fx.Provide(
		NewLogger,
		NewTracer,
	),
)
