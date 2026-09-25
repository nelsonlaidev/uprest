package config

import (
	"log/slog"
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

	if cfg.Mode != "env" || cfg.Port != 80 || cfg.IPv6 || cfg.IdleTimeout != 15*time.Minute || cfg.LogLevel != slog.LevelInfo {
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

	cfg, err = Load()

	if err != nil {
		t.Fatal(err)
	}

	if cfg.Backends[0].MaxConnections != 5 || cfg.Port != 8079 || !cfg.IPv6 || cfg.IdleTimeout != 30*time.Second || cfg.LogLevel != slog.LevelDebug {
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
		"UPREST_LOG_LEVEL",
		"UPREST_TOKENS_FILE",
	}

	for _, key := range keys {
		t.Setenv(key, "")
	}
}
