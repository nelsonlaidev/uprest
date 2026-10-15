package config

import (
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nelsonlaidev/uprest/internal/redisproxy"
)

func TestLoadEnvironmentMode(t *testing.T) {
	clearConfiguration(t)
	t.Setenv("UPREST_TOKEN", "test-token")
	t.Setenv("UPREST_CONNECTION_STRING", "redis://localhost:6379")

	cfg, err := Load()

	if err != nil {
		t.Fatal(err)
	}

	if cfg.Mode != "env" || cfg.Port != 80 || cfg.IPv6 || cfg.IdleTimeout != 15*time.Minute || cfg.LogLevel != slog.LevelInfo || cfg.MetricsToken != "" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}

	wantBackends := []redisproxy.Backend{{
		Token:            "test-token",
		ID:               "env",
		ConnectionString: "redis://localhost:6379",
		MaxConnections:   3,
	}}

	if !reflect.DeepEqual(cfg.Backends, wantBackends) {
		t.Fatalf("got backends %#v, want %#v", cfg.Backends, wantBackends)
	}

	t.Setenv("UPREST_MAX_CONNECTIONS", "5")
	t.Setenv("UPREST_PORT", "8079")
	t.Setenv("UPREST_IPV6", "true")
	t.Setenv("UPREST_IDLE_TIMEOUT", "30s")
	t.Setenv("UPREST_LOG_LEVEL", "debug")
	t.Setenv("UPREST_METRICS_TOKEN", "monitoring-secret")

	cfg, err = Load()

	if err != nil {
		t.Fatal(err)
	}

	if cfg.Backends[0].MaxConnections != 5 || cfg.Port != 8079 || !cfg.IPv6 || cfg.IdleTimeout != 30*time.Second || cfg.LogLevel != slog.LevelDebug || cfg.MetricsToken != "monitoring-secret" {
		t.Fatalf("unexpected overrides: %+v", cfg)
	}
}

func TestLoadFileMode(t *testing.T) {
	clearConfiguration(t)
	t.Setenv("UPREST_MODE", "file")

	path := filepath.Join(t.TempDir(), "tokens.json")
	contents := `{
		"second-token": {
			"id": "second",
			"connection_string": "redis://second:6379",
			"max_connections": 7
		},
		"first-token": {
			"id": "first",
			"connection_string": "redis://first:6379"
		}
	}`

	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("UPREST_TOKENS_FILE", path)

	cfg, err := Load()

	if err != nil {
		t.Fatal(err)
	}

	wantBackends := []redisproxy.Backend{
		{
			Token:            "first-token",
			ID:               "first",
			ConnectionString: "redis://first:6379",
			MaxConnections:   3,
		},
		{
			Token:            "second-token",
			ID:               "second",
			ConnectionString: "redis://second:6379",
			MaxConnections:   7,
		},
	}

	if cfg.Mode != "file" || cfg.TokensFile != path || !reflect.DeepEqual(cfg.Backends, wantBackends) {
		t.Fatalf("unexpected file configuration: %+v", cfg)
	}
}

func TestMetricsConfiguration(t *testing.T) {
	for _, test := range []struct {
		name        string
		token       string
		duplicateID bool
		wantError   bool
	}{
		{name: "disabled preserves duplicate IDs", duplicateID: true},
		{name: "enabled", token: "monitoring-secret"},
		{name: "duplicate IDs", token: "monitoring-secret", duplicateID: true, wantError: true},
		{name: "backend token collision", token: "backend-secret", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			clearConfiguration(t)
			t.Setenv("UPREST_MODE", "file")
			t.Setenv("UPREST_METRICS_TOKEN", test.token)
			secondID := "second"

			if test.duplicateID {
				secondID = "first"
			}

			contents := `{"backend-secret":{"id":"first","connection_string":"redis://localhost:6379"},"other-secret":{"id":"` + secondID + `","connection_string":"redis://localhost:6380"}}`
			path := filepath.Join(t.TempDir(), "tokens.json")

			if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}

			t.Setenv("UPREST_TOKENS_FILE", path)
			cfg, err := Load()

			if test.wantError {
				if err == nil || strings.Contains(err.Error(), test.token) || strings.Contains(err.Error(), "backend-secret") {
					t.Fatalf("unexpected validation error: %v", err)
				}

				return
			}

			if err != nil || cfg.MetricsToken != test.token {
				t.Fatalf("unexpected metrics configuration: token configured=%t, error=%v", cfg.MetricsToken != "", err)
			}
		})
	}
}

func TestTimeoutConfiguration(t *testing.T) {
	for _, mode := range []string{"env", "file"} {
		t.Run(mode, func(t *testing.T) {
			for _, test := range []struct {
				name        string
				acquire     string
				command     string
				wantAcquire time.Duration
				wantCommand time.Duration
				wantHTTP    time.Duration
				wantError   string
			}{
				{name: "defaults", wantAcquire: 30 * time.Second, wantCommand: 30 * time.Second, wantHTTP: 80 * time.Second},
				{name: "acquisition override", acquire: "250ms", wantAcquire: 250 * time.Millisecond, wantCommand: 30 * time.Second, wantHTTP: 50*time.Second + 250*time.Millisecond},
				{name: "command override", command: "2m", wantAcquire: 30 * time.Second, wantCommand: 2 * time.Minute, wantHTTP: 170 * time.Second},
				{name: "stream initialization budget", acquire: "1s", command: "2s", wantAcquire: time.Second, wantCommand: 2 * time.Second, wantHTTP: 51 * time.Second},
				{name: "maximum HTTP budget", acquire: (time.Duration(math.MaxInt64) - 50*time.Second).String(), wantAcquire: time.Duration(math.MaxInt64) - 50*time.Second, wantCommand: 30 * time.Second, wantHTTP: math.MaxInt64},
				{name: "acquisition budget overflow", acquire: (time.Duration(math.MaxInt64) - 50*time.Second + 1).String(), wantError: "HTTP response timeout budget"},
				{name: "command budget overflow", command: time.Duration(math.MaxInt64).String(), wantError: "HTTP response timeout budget"},
				{name: "combined budget overflow", acquire: "2000000h", command: "2000000h", wantError: "HTTP response timeout budget"},
				{name: "malformed acquisition", acquire: "invalid", wantError: "UPREST_ACQUIRE_TIMEOUT must be a positive duration"},
				{name: "zero acquisition", acquire: "0s", wantError: "UPREST_ACQUIRE_TIMEOUT must be a positive duration"},
				{name: "negative acquisition", acquire: "-1s", wantError: "UPREST_ACQUIRE_TIMEOUT must be a positive duration"},
				{name: "overflowing acquisition", acquire: "3000000h", wantError: "UPREST_ACQUIRE_TIMEOUT must be a positive duration"},
				{name: "malformed command", command: "invalid", wantError: "UPREST_COMMAND_TIMEOUT must be a positive duration"},
				{name: "zero command", command: "0s", wantError: "UPREST_COMMAND_TIMEOUT must be a positive duration"},
				{name: "negative command", command: "-1s", wantError: "UPREST_COMMAND_TIMEOUT must be a positive duration"},
				{name: "overflowing command", command: "3000000h", wantError: "UPREST_COMMAND_TIMEOUT must be a positive duration"},
			} {
				t.Run(test.name, func(t *testing.T) {
					clearConfiguration(t)
					t.Setenv("UPREST_MODE", mode)
					t.Setenv("UPREST_TOKEN", "test-token")
					t.Setenv("UPREST_CONNECTION_STRING", "redis://localhost:6379")
					t.Setenv("UPREST_ACQUIRE_TIMEOUT", test.acquire)
					t.Setenv("UPREST_COMMAND_TIMEOUT", test.command)

					if mode == "file" {
						path := filepath.Join(t.TempDir(), "tokens.json")
						contents := `{"test-token":{"id":"test","connection_string":"redis://localhost:6379"}}`

						if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
							t.Fatal(err)
						}

						t.Setenv("UPREST_TOKENS_FILE", path)
					}

					cfg, err := Load()

					if test.wantError != "" {
						if err == nil || !strings.Contains(err.Error(), test.wantError) {
							t.Fatalf("got error %v, want %q", err, test.wantError)
						}

						return
					}

					if err != nil {
						t.Fatal(err)
					}

					if cfg.AcquireTimeout != test.wantAcquire || cfg.CommandTimeout != test.wantCommand || cfg.HTTPWriteTimeout != test.wantHTTP {
						t.Fatalf("unexpected timeouts: acquire=%s, command=%s, HTTP=%s", cfg.AcquireTimeout, cfg.CommandTimeout, cfg.HTTPWriteTimeout)
					}
				})
			}
		})
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		value   string
		wantErr string
	}{
		{"missing token", "UPREST_TOKEN", "", "UPREST_TOKEN is required"},
		{"missing connection", "UPREST_CONNECTION_STRING", "", "UPREST_CONNECTION_STRING is required"},
		{"unsupported mode", "UPREST_MODE", "other", "use env or file"},
		{"invalid pool size", "UPREST_MAX_CONNECTIONS", "0", "positive integer"},
		{"invalid port", "UPREST_PORT", "65536", "1 to 65535"},
		{"invalid IPv6", "UPREST_IPV6", "perhaps", "true or false"},
		{"invalid idle timeout", "UPREST_IDLE_TIMEOUT", "0s", "positive duration"},
		{"invalid log level", "UPREST_LOG_LEVEL", "trace", "debug, info, warn, or error"},
		{"custom log level", "UPREST_LOG_LEVEL", "INFO+1", "debug, info, warn, or error"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearConfiguration(t)
			t.Setenv("UPREST_MODE", "env")
			t.Setenv("UPREST_TOKEN", "test-token")
			t.Setenv("UPREST_CONNECTION_STRING", "redis://localhost:6379")
			t.Setenv(test.key, test.value)

			_, err := Load()

			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("got error %v, want one containing %q", err, test.wantErr)
			}
		})
	}
}

func TestLoadRejectsInvalidTokenFile(t *testing.T) {
	tests := []struct {
		name     string
		contents string
		wantErr  string
	}{
		{"invalid JSON", `{`, "decode tokens file"},
		{"empty file", `{}`, "at least one token"},
		{"empty token", `{"":{"id":"empty","connection_string":"redis://localhost:6379"}}`, "empty token"},
		{"missing ID", `{"token":{"connection_string":"redis://localhost:6379"}}`, "without id"},
		{"missing connection", `{"token":{"id":"missing"}}`, "requires connection_string"},
		{"invalid max connections", `{"token":{"id":"invalid","connection_string":"redis://localhost:6379","max_connections":0}}`, "positive max_connections"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearConfiguration(t)
			t.Setenv("UPREST_MODE", "file")

			path := filepath.Join(t.TempDir(), "tokens.json")

			if err := os.WriteFile(path, []byte(test.contents), 0o600); err != nil {
				t.Fatal(err)
			}

			t.Setenv("UPREST_TOKENS_FILE", path)

			_, err := Load()

			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("got error %v, want one containing %q", err, test.wantErr)
			}
		})
	}
}

func clearConfiguration(t *testing.T) {
	t.Helper()

	keys := []string{
		"UPREST_MODE",
		"UPREST_TOKEN",
		"UPREST_CONNECTION_STRING",
		"UPREST_MAX_CONNECTIONS",
		"UPREST_PORT",
		"UPREST_IPV6",
		"UPREST_IDLE_TIMEOUT",
		"UPREST_ACQUIRE_TIMEOUT",
		"UPREST_COMMAND_TIMEOUT",
		"UPREST_LOG_LEVEL",
		"UPREST_TOKENS_FILE",
		"UPREST_METRICS_TOKEN",
	}

	for _, key := range keys {
		t.Setenv(key, "")
	}
}
