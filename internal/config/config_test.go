package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/spf13/pflag"
)

func TestSplitCSV(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"a", []string{"a"}},
		{" ops-admin , , billing ", []string{"ops-admin", "billing"}},
		{"x,y,z", []string{"x", "y", "z"}},
		{"  ,  ", nil},
	}
	for _, tt := range tests {
		if got := splitCSV(tt.in); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("splitCSV(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestNew_AdminPrincipalsParsed(t *testing.T) {
	t.Setenv("LEDGER_AUTH_ADMIN_PRINCIPALS", "ops-admin, billing ")
	cfg, err := New(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"ops-admin", "billing"}
	if !reflect.DeepEqual(cfg.Auth.AdminPrincipals, want) {
		t.Errorf("admin principals = %v, want %v (exact match against JWT subject)", cfg.Auth.AdminPrincipals, want)
	}
}

func TestNew_Defaults(t *testing.T) {
	// Clear any env vars that might interfere.
	envVars := []string{
		"LEDGER_POSTGRES_DSN", "LEDGER_REDIS_URL", "LEDGER_GRPC_PORT",
		"LEDGER_HTTP_PORT", "LEDGER_ENVIRONMENT", "LEDGER_LOG_LEVEL",
	}
	for _, k := range envVars {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}

	cfg, err := New(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Postgres.DSN != "postgres://ledger:ledger@localhost:5432/ledger?sslmode=require" {
		t.Errorf("postgres DSN = %q, want default", cfg.Postgres.DSN)
	}
	if cfg.Redis.URL != "redis://localhost:6379/0" {
		t.Errorf("redis URL = %q, want default", cfg.Redis.URL)
	}
	if cfg.GRPC.Port != 9090 {
		t.Errorf("grpc port = %d, want 9090", cfg.GRPC.Port)
	}
	if cfg.HTTP.Port != 8080 {
		t.Errorf("http port = %d, want 8080", cfg.HTTP.Port)
	}
	if cfg.Environment != "development" {
		t.Errorf("environment = %q, want development", cfg.Environment)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("log level = %q, want info", cfg.LogLevel)
	}
	if cfg.Worker.BatchCloseInterval != 5*time.Second {
		t.Errorf("batch close interval = %v, want 5s", cfg.Worker.BatchCloseInterval)
	}
	if cfg.Worker.StuckApprovalThreshold != 5*time.Minute {
		t.Errorf("stuck approval threshold = %v, want 5m", cfg.Worker.StuckApprovalThreshold)
	}
}

func TestNew_EnvVarsOverrideDefaults(t *testing.T) {
	t.Setenv("LEDGER_GRPC_PORT", "9999")
	t.Setenv("LEDGER_LOG_LEVEL", "debug")
	t.Setenv("LEDGER_ENVIRONMENT", "production")
	// Production requires authentication to be configured.
	t.Setenv("LEDGER_AUTH_ENABLED", "true")
	t.Setenv("LEDGER_AUTH_JWKS_URL", "https://issuer.example.com/.well-known/jwks.json")

	cfg, err := New(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.GRPC.Port != 9999 {
		t.Errorf("grpc port = %d, want 9999", cfg.GRPC.Port)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("log level = %q, want debug", cfg.LogLevel)
	}
	if cfg.Environment != "production" {
		t.Errorf("environment = %q, want production", cfg.Environment)
	}
}

func TestNew_FlagsOverrideEnvVars(t *testing.T) {
	t.Setenv("LEDGER_GRPC_PORT", "7777")

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	RegisterFlags(fs)
	if err := fs.Parse([]string{"--grpc-port=8888"}); err != nil {
		t.Fatalf("flag parse: %v", err)
	}

	cfg, err := New(fs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.GRPC.Port != 8888 {
		t.Errorf("grpc port = %d, want 8888 (flag should override env)", cfg.GRPC.Port)
	}
}

func TestNew_DotEnvFile(t *testing.T) {
	// Create a temp .env file.
	dir := t.TempDir()
	envFile := filepath.Join(dir, ".env")
	content := "LEDGER_GRPC_PORT=6666\nLEDGER_LOG_LEVEL=warn\n"
	if err := os.WriteFile(envFile, []byte(content), 0644); err != nil {
		t.Fatalf("writing .env: %v", err)
	}

	// Clear env vars so .env values are visible.
	os.Unsetenv("LEDGER_GRPC_PORT")
	os.Unsetenv("LEDGER_LOG_LEVEL")

	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	RegisterFlags(fs)
	if err := fs.Parse([]string{"--env-file=" + envFile}); err != nil {
		t.Fatalf("flag parse: %v", err)
	}

	cfg, err := New(fs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.GRPC.Port != 6666 {
		t.Errorf("grpc port = %d, want 6666 (from .env file)", cfg.GRPC.Port)
	}
	if cfg.LogLevel != "warn" {
		t.Errorf("log level = %q, want warn (from .env file)", cfg.LogLevel)
	}
}

func TestNew_ValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		envVars map[string]string
		wantErr string
	}{
		{
			name:    "invalid grpc port zero",
			envVars: map[string]string{"LEDGER_GRPC_PORT": "0"},
			wantErr: "grpc.port must be between",
		},
		{
			name:    "invalid grpc port",
			envVars: map[string]string{"LEDGER_GRPC_PORT": "99999"},
			wantErr: "grpc.port must be between",
		},
		{
			name:    "invalid environment",
			envVars: map[string]string{"LEDGER_ENVIRONMENT": "staging"},
			wantErr: "environment must be one of",
		},
		{
			name:    "invalid log level",
			envVars: map[string]string{"LEDGER_LOG_LEVEL": "verbose"},
			wantErr: "log_level must be one of",
		},
		{
			name:    "production requires auth enabled",
			envVars: map[string]string{"LEDGER_ENVIRONMENT": "production"},
			wantErr: "auth.enabled must be true",
		},
		{
			name:    "auth enabled requires jwks url",
			envVars: map[string]string{"LEDGER_AUTH_ENABLED": "true"},
			wantErr: "auth.jwks_url is required",
		},
		{
			name:    "auth method must be jwt",
			envVars: map[string]string{"LEDGER_AUTH_ENABLED": "true", "LEDGER_AUTH_METHOD": "mtls", "LEDGER_AUTH_JWKS_URL": "https://x/jwks"},
			wantErr: `auth.method must be "jwt"`,
		},
		{
			name:    "stuck approval threshold must be positive",
			envVars: map[string]string{"LEDGER_WORKER_STUCK_APPROVAL_THRESHOLD": "0s"},
			wantErr: "stuck_approval_threshold must be positive",
		},
		{
			name:    "rate limit enabled requires positive rps",
			envVars: map[string]string{"LEDGER_RATE_LIMIT_ENABLED": "true"},
			wantErr: "rate_limit.read_rps and rate_limit.write_rps must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set the specific env var for this test case.
			for k, v := range tt.envVars {
				if v == "" {
					os.Unsetenv(k)
				} else {
					t.Setenv(k, v)
				}
			}

			_, err := New(nil)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if got := err.Error(); !contains(got, tt.wantErr) {
				t.Errorf("error = %q, want containing %q", got, tt.wantErr)
			}
		})
	}
}

func TestNew_EmptyRequiredValueViaFlag(t *testing.T) {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	RegisterFlags(fs)
	if err := fs.Parse([]string{"--postgres-dsn="}); err != nil {
		t.Fatalf("flag parse: %v", err)
	}

	_, err := New(fs)
	if err == nil {
		t.Fatal("expected error for empty postgres DSN, got nil")
	}
	if !contains(err.Error(), "postgres.dsn is required") {
		t.Errorf("error = %q, want containing 'postgres.dsn is required'", err.Error())
	}
}

func TestNew_WorkerIntervalFromEnv(t *testing.T) {
	t.Setenv("LEDGER_WORKER_BATCH_CLOSE_INTERVAL", "10s")
	t.Setenv("LEDGER_WORKER_CHECKPOINT_INTERVAL", "1m")
	t.Setenv("LEDGER_WORKER_STUCK_APPROVAL_THRESHOLD", "2m")

	cfg, err := New(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Worker.BatchCloseInterval != 10*time.Second {
		t.Errorf("batch close interval = %v, want 10s", cfg.Worker.BatchCloseInterval)
	}
	if cfg.Worker.CheckpointInterval != time.Minute {
		t.Errorf("checkpoint interval = %v, want 1m", cfg.Worker.CheckpointInterval)
	}
	if cfg.Worker.StuckApprovalThreshold != 2*time.Minute {
		t.Errorf("stuck approval threshold = %v, want 2m", cfg.Worker.StuckApprovalThreshold)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
