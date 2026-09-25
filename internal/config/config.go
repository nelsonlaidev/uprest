package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nelsonlaidev/uprest/internal/redisproxy"
)

const defaultMaxConnections = 3
const defaultPort = 80
const defaultIdleTimeout = 15 * time.Minute
const defaultTokensFile = "/app/uprest-config/tokens.json" //nolint:gosec // G101: file path, not a credential

type Config struct {
	Mode        string
	Backends    []redisproxy.Backend
	TokensFile  string
	Port        int
	IPv6        bool
	IdleTimeout time.Duration
	LogLevel    slog.Level
}

type fileBackend struct {
	ID               string `json:"id"`
	ConnectionString string `json:"connection_string"`
	MaxConnections   *int   `json:"max_connections"`
}

func Load() (Config, error) {
	mode := os.Getenv("UPREST_MODE")

	if mode == "" {
		mode = "env"
	}

	cfg := Config{
		Mode:        mode,
		Port:        defaultPort,
		IdleTimeout: defaultIdleTimeout,
		LogLevel:    slog.LevelInfo,
	}

	if value := os.Getenv("UPREST_PORT"); value != "" {
		port, err := strconv.Atoi(value)

		if err != nil || port < 1 || port > 65535 {
			return Config{}, fmt.Errorf("UPREST_PORT must be an integer from 1 to 65535")
		}

		cfg.Port = port
	}

	if value := os.Getenv("UPREST_IPV6"); value != "" {
		ipv6, err := strconv.ParseBool(value)

		if err != nil {
			return Config{}, fmt.Errorf("UPREST_IPV6 must be true or false: %w", err)
		}

		cfg.IPv6 = ipv6
	}

	if value := os.Getenv("UPREST_IDLE_TIMEOUT"); value != "" {
		idleTimeout, err := time.ParseDuration(value)

		if err != nil || idleTimeout <= 0 {
			return Config{}, fmt.Errorf("UPREST_IDLE_TIMEOUT must be a positive duration")
		}

		cfg.IdleTimeout = idleTimeout
	}

	if value := os.Getenv("UPREST_LOG_LEVEL"); value != "" {
		switch strings.ToLower(value) {
		case "debug":
			cfg.LogLevel = slog.LevelDebug

		case "info":
			cfg.LogLevel = slog.LevelInfo

		case "warn":
			cfg.LogLevel = slog.LevelWarn

		case "error":
			cfg.LogLevel = slog.LevelError

		default:
			return Config{}, fmt.Errorf("UPREST_LOG_LEVEL must be debug, info, warn, or error")
		}
	}

	var err error

	switch mode {
	case "env":
		cfg.Backends, err = loadEnvironmentBackend()

	case "file":
		cfg.TokensFile = os.Getenv("UPREST_TOKENS_FILE")

		if cfg.TokensFile == "" {
			cfg.TokensFile = defaultTokensFile
		}

		cfg.Backends, err = loadFileBackends(cfg.TokensFile)

	default:
		return Config{}, fmt.Errorf("UPREST_MODE %q is not supported; use env or file", mode)
	}

	if err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func loadEnvironmentBackend() ([]redisproxy.Backend, error) {
	token := os.Getenv("UPREST_TOKEN")

	if token == "" {
		return nil, fmt.Errorf("UPREST_TOKEN is required in env mode")
	}

	connectionString := os.Getenv("UPREST_CONNECTION_STRING")

	if connectionString == "" {
		return nil, fmt.Errorf("UPREST_CONNECTION_STRING is required in env mode")
	}

	maxConnections := defaultMaxConnections

	if value := os.Getenv("UPREST_MAX_CONNECTIONS"); value != "" {
		parsed, err := strconv.Atoi(value)

		if err != nil || parsed < 1 {
			return nil, fmt.Errorf("UPREST_MAX_CONNECTIONS must be a positive integer")
		}

		maxConnections = parsed
	}

	return []redisproxy.Backend{{
		Token:            token,
		ID:               "env",
		ConnectionString: connectionString,
		MaxConnections:   maxConnections,
	}}, nil
}

func loadFileBackends(path string) ([]redisproxy.Backend, error) {
	contents, err := os.ReadFile(path) //nolint:gosec // G304: path is operator-configured

	if err != nil {
		return nil, fmt.Errorf("read tokens file %q: %w", path, err)
	}

	var entries map[string]fileBackend

	if err := json.Unmarshal(contents, &entries); err != nil {
		return nil, fmt.Errorf("decode tokens file %q: %w", path, err)
	}

	if len(entries) == 0 {
		return nil, fmt.Errorf("tokens file %q must contain at least one token", path)
	}

	tokens := make([]string, 0, len(entries))

	for token := range entries {
		tokens = append(tokens, token)
	}

	sort.Strings(tokens)

	backends := make([]redisproxy.Backend, 0, len(tokens))

	for _, token := range tokens {
		entry := entries[token]

		if token == "" {
			return nil, fmt.Errorf("tokens file %q contains an empty token", path)
		}

		if entry.ID == "" {
			return nil, fmt.Errorf("tokens file %q contains an entry without id", path)
		}

		if entry.ConnectionString == "" {
			return nil, fmt.Errorf("tokens file %q entry %q requires connection_string", path, entry.ID)
		}

		maxConnections := defaultMaxConnections

		if entry.MaxConnections != nil {
			if *entry.MaxConnections < 1 {
				return nil, fmt.Errorf("tokens file %q entry %q requires a positive max_connections", path, entry.ID)
			}

			maxConnections = *entry.MaxConnections
		}

		backends = append(backends, redisproxy.Backend{
			Token:            token,
			ID:               entry.ID,
			ConnectionString: entry.ConnectionString,
			MaxConnections:   maxConnections,
		})
	}

	return backends, nil
}
