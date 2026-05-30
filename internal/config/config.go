package config

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

// Config holds all configuration for the ledger application.
type Config struct {
	Postgres    PostgresConfig
	Redis       RedisConfig
	GRPC        GRPCConfig
	HTTP        HTTPConfig
	OTel        OTelConfig
	Worker      WorkerConfig
	Auth        AuthConfig
	Environment string
	LogLevel    string
}

type PostgresConfig struct {
	DSN string
}

type RedisConfig struct {
	URL string
}

type GRPCConfig struct {
	Port int
}

type HTTPConfig struct {
	Port int
}

type OTelConfig struct {
	Endpoint string
	Insecure bool
}

type WorkerConfig struct {
	BatchCloseInterval     time.Duration
	CheckpointInterval     time.Duration
	HoldExpiryInterval     time.Duration
	ApprovalExpiryInterval time.Duration
	// StuckApprovalThreshold is how long an approval may remain in the
	// "executing" state before the expiry sweep treats it as stuck and recovers it.
	StuckApprovalThreshold time.Duration
}

// AuthConfig configures gRPC authentication. When Enabled, every non-exempt
// request must carry a valid bearer JWT, verified against the keys published at
// JWKSURL. The principal is derived from the token's subject (sub) claim.
type AuthConfig struct {
	Enabled  bool
	Method   string
	JWKSURL  string
	Audience string
	Issuer   string
}

// RegisterFlags binds CLI flags to viper keys. Call this before New().
// Flags use dot-separated names matching the viper key structure.
func RegisterFlags(fs *pflag.FlagSet) {
	fs.String("postgres-dsn", "", "PostgreSQL connection string")
	fs.String("redis-url", "", "Redis connection URL")
	fs.Int("grpc-port", 0, "gRPC server port")
	fs.Int("http-port", 0, "HTTP server port")
	fs.String("otel-endpoint", "", "OpenTelemetry collector endpoint")
	fs.Bool("otel-insecure", false, "Use insecure gRPC for OTel")
	fs.String("environment", "", "Runtime environment (development|production)")
	fs.String("log-level", "", "Log level (debug|info|warn|error)")
	fs.Duration("worker-batch-close-interval", 0, "Batch close interval")
	fs.Duration("worker-checkpoint-interval", 0, "Checkpoint build interval")
	fs.Duration("worker-hold-expiry-interval", 0, "Hold expiry sweep interval")
	fs.Duration("worker-approval-expiry-interval", 0, "Approval expiry interval")
	fs.Duration("worker-stuck-approval-threshold", 0, "How long an approval may stay executing before recovery")
	fs.Bool("auth-enabled", false, "Require authenticated requests (JWT bearer tokens)")
	fs.String("auth-method", "", "Authentication method (jwt)")
	fs.String("auth-jwks-url", "", "JWKS URL used to verify JWT signatures")
	fs.String("auth-audience", "", "Expected JWT audience (aud) claim; empty disables the check")
	fs.String("auth-issuer", "", "Expected JWT issuer (iss) claim; empty disables the check")
	fs.String("env-file", ".env", "Path to .env file (set to empty to disable)")
}

// New creates a Config by reading from (in priority order):
//  1. CLI flags (if RegisterFlags was called and flags were parsed)
//  2. Environment variables (LEDGER_ prefix)
//  3. .env file (if present)
//  4. Defaults
func New(fs *pflag.FlagSet) (*Config, error) {
	v := viper.New()

	// --- Defaults ---
	v.SetDefault("postgres.dsn", "postgres://ledger:ledger@localhost:5432/ledger?sslmode=require")
	v.SetDefault("redis.url", "redis://localhost:6379/0")
	v.SetDefault("grpc.port", 9090)
	v.SetDefault("http.port", 8080)
	v.SetDefault("otel.endpoint", "")
	v.SetDefault("otel.insecure", true)
	v.SetDefault("environment", "development")
	v.SetDefault("log_level", "info")
	v.SetDefault("worker.batch_close_interval", 5*time.Second)
	v.SetDefault("worker.checkpoint_interval", 30*time.Second)
	v.SetDefault("worker.hold_expiry_interval", 30*time.Second)
	v.SetDefault("worker.approval_expiry_interval", 60*time.Second)
	v.SetDefault("worker.stuck_approval_threshold", 5*time.Minute)
	v.SetDefault("auth.enabled", false)
	v.SetDefault("auth.method", "jwt")
	v.SetDefault("auth.jwks_url", "")
	v.SetDefault("auth.audience", "")
	v.SetDefault("auth.issuer", "")

	// --- Environment variables ---
	v.SetEnvPrefix("LEDGER")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// --- .env file ---
	// Load .env into process environment so AutomaticEnv picks up prefixed keys.
	// Values already in the environment are NOT overwritten.
	envFile := ".env"
	if fs != nil {
		if f := fs.Lookup("env-file"); f != nil && f.Changed {
			envFile = f.Value.String()
		}
	}
	if envFile != "" {
		if err := loadDotEnv(envFile); err != nil {
			return nil, err
		}
	}

	// --- CLI flags (highest priority) ---
	if fs != nil {
		bindFlag(v, fs, "postgres-dsn", "postgres.dsn")
		bindFlag(v, fs, "redis-url", "redis.url")
		bindFlag(v, fs, "grpc-port", "grpc.port")
		bindFlag(v, fs, "http-port", "http.port")
		bindFlag(v, fs, "otel-endpoint", "otel.endpoint")
		bindFlag(v, fs, "otel-insecure", "otel.insecure")
		bindFlag(v, fs, "environment", "environment")
		bindFlag(v, fs, "log-level", "log_level")
		bindFlag(v, fs, "worker-batch-close-interval", "worker.batch_close_interval")
		bindFlag(v, fs, "worker-checkpoint-interval", "worker.checkpoint_interval")
		bindFlag(v, fs, "worker-hold-expiry-interval", "worker.hold_expiry_interval")
		bindFlag(v, fs, "worker-approval-expiry-interval", "worker.approval_expiry_interval")
		bindFlag(v, fs, "worker-stuck-approval-threshold", "worker.stuck_approval_threshold")
		bindFlag(v, fs, "auth-enabled", "auth.enabled")
		bindFlag(v, fs, "auth-method", "auth.method")
		bindFlag(v, fs, "auth-jwks-url", "auth.jwks_url")
		bindFlag(v, fs, "auth-audience", "auth.audience")
		bindFlag(v, fs, "auth-issuer", "auth.issuer")
	}

	// --- Build config struct ---
	cfg := &Config{
		Postgres: PostgresConfig{
			DSN: v.GetString("postgres.dsn"),
		},
		Redis: RedisConfig{
			URL: v.GetString("redis.url"),
		},
		GRPC: GRPCConfig{
			Port: v.GetInt("grpc.port"),
		},
		HTTP: HTTPConfig{
			Port: v.GetInt("http.port"),
		},
		OTel: OTelConfig{
			Endpoint: v.GetString("otel.endpoint"),
			Insecure: v.GetBool("otel.insecure"),
		},
		Worker: WorkerConfig{
			BatchCloseInterval:     v.GetDuration("worker.batch_close_interval"),
			CheckpointInterval:     v.GetDuration("worker.checkpoint_interval"),
			HoldExpiryInterval:     v.GetDuration("worker.hold_expiry_interval"),
			ApprovalExpiryInterval: v.GetDuration("worker.approval_expiry_interval"),
			StuckApprovalThreshold: v.GetDuration("worker.stuck_approval_threshold"),
		},
		Auth: AuthConfig{
			Enabled:  v.GetBool("auth.enabled"),
			Method:   v.GetString("auth.method"),
			JWKSURL:  v.GetString("auth.jwks_url"),
			Audience: v.GetString("auth.audience"),
			Issuer:   v.GetString("auth.issuer"),
		},
		Environment: v.GetString("environment"),
		LogLevel:    v.GetString("log_level"),
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) validate() error {
	if c.Postgres.DSN == "" {
		return fmt.Errorf("config: postgres.dsn is required (set LEDGER_POSTGRES_DSN or --postgres-dsn)")
	}
	if c.Redis.URL == "" {
		return fmt.Errorf("config: redis.url is required (set LEDGER_REDIS_URL or --redis-url)")
	}
	if c.GRPC.Port <= 0 || c.GRPC.Port > 65535 {
		return fmt.Errorf("config: grpc.port must be between 1 and 65535, got %d", c.GRPC.Port)
	}
	if c.HTTP.Port <= 0 || c.HTTP.Port > 65535 {
		return fmt.Errorf("config: http.port must be between 1 and 65535, got %d", c.HTTP.Port)
	}
	validEnvs := map[string]bool{"development": true, "production": true, "test": true}
	if !validEnvs[c.Environment] {
		return fmt.Errorf("config: environment must be one of development, production, test; got %q", c.Environment)
	}
	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLevels[c.LogLevel] {
		return fmt.Errorf("config: log_level must be one of debug, info, warn, error; got %q", c.LogLevel)
	}
	if c.Worker.BatchCloseInterval <= 0 {
		return fmt.Errorf("config: worker.batch_close_interval must be positive, got %v", c.Worker.BatchCloseInterval)
	}
	if c.Worker.CheckpointInterval <= 0 {
		return fmt.Errorf("config: worker.checkpoint_interval must be positive, got %v", c.Worker.CheckpointInterval)
	}
	if c.Worker.HoldExpiryInterval <= 0 {
		return fmt.Errorf("config: worker.hold_expiry_interval must be positive, got %v", c.Worker.HoldExpiryInterval)
	}
	if c.Worker.ApprovalExpiryInterval <= 0 {
		return fmt.Errorf("config: worker.approval_expiry_interval must be positive, got %v", c.Worker.ApprovalExpiryInterval)
	}
	if c.Worker.StuckApprovalThreshold <= 0 {
		return fmt.Errorf("config: worker.stuck_approval_threshold must be positive, got %v", c.Worker.StuckApprovalThreshold)
	}
	if c.Environment != "development" && strings.Contains(c.Postgres.DSN, "sslmode=disable") {
		return fmt.Errorf("config: postgres.dsn uses sslmode=disable in %s environment; use sslmode=require or sslmode=verify-full", c.Environment)
	}
	if c.Auth.Enabled {
		if c.Auth.Method != "jwt" {
			return fmt.Errorf("config: auth.method must be \"jwt\" (the only supported method); got %q", c.Auth.Method)
		}
		if c.Auth.JWKSURL == "" {
			return fmt.Errorf("config: auth.jwks_url is required when auth.enabled is true")
		}
		if c.Environment != "development" && !strings.HasPrefix(c.Auth.JWKSURL, "https://") {
			return fmt.Errorf("config: auth.jwks_url must use https outside the development environment, got %q", c.Auth.JWKSURL)
		}
	}
	if c.Environment == "production" && !c.Auth.Enabled {
		return fmt.Errorf("config: auth.enabled must be true in the production environment")
	}
	return nil
}

// loadDotEnv reads a .env file and sets environment variables for any keys not
// already present in the environment. This preserves the priority:
// real env vars > .env file values.
func loadDotEnv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // .env file is optional
		}
		return fmt.Errorf("opening env file %s: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		// Remove surrounding quotes if present.
		if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') ||
			(value[0] == '\'' && value[len(value)-1] == '\'')) {
			value = value[1 : len(value)-1]
		}
		// Only set if not already in environment.
		if _, exists := os.LookupEnv(key); !exists {
			os.Setenv(key, value)
		}
	}
	return scanner.Err()
}

// bindFlag binds a pflag to a viper key only if the flag was explicitly set.
func bindFlag(v *viper.Viper, fs *pflag.FlagSet, flagName, viperKey string) {
	if f := fs.Lookup(flagName); f != nil && f.Changed {
		v.BindPFlag(viperKey, f)
	}
}

// Module provides Config and its sub-configs to the fx container.
// Binaries should provide a *pflag.FlagSet to the container before including this module.
var Module = fx.Module("config",
	fx.Provide(
		func(fs *pflag.FlagSet) (*Config, error) {
			return New(fs)
		},
		func(c *Config) PostgresConfig { return c.Postgres },
		func(c *Config) RedisConfig { return c.Redis },
		func(c *Config) GRPCConfig { return c.GRPC },
		func(c *Config) HTTPConfig { return c.HTTP },
		func(c *Config) OTelConfig { return c.OTel },
		func(c *Config) WorkerConfig { return c.Worker },
		func(c *Config) AuthConfig { return c.Auth },
	),
)
