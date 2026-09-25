package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nelsonlaidev/uprest/internal/config"
	"github.com/nelsonlaidev/uprest/internal/redisproxy"
	"github.com/nelsonlaidev/uprest/internal/server"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	if err := run(); err != nil {
		slog.Error("uprest stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()

	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(logger)

	pools, err := redisproxy.NewManager(cfg.Backends, cfg.IdleTimeout, logger)

	if err != nil {
		return fmt.Errorf("initialize Redis pools: %w", err)
	}

	defer func() {
		if err := pools.Close(); err != nil {
			logger.Error("Redis pool close failed", "error", err)
		}
	}()

	host := "0.0.0.0"

	if cfg.IPv6 {
		host = "::"
	}

	address := net.JoinHostPort(host, strconv.Itoa(cfg.Port))
	httpServer := &http.Server{
		Addr:              address,
		Handler:           server.NewHandler(pools, logger),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      50 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	defer stop()

	logger.Info("listening",
		"address", address,
		"mode", cfg.Mode,
		"backends", len(cfg.Backends),
		"idle_timeout", cfg.IdleTimeout,
	)

	serveErr := make(chan error, 1)

	go func() {
		serveErr <- httpServer.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("HTTP server failed: %w", err)
		}

	case <-ctx.Done():
		logger.Info("shutdown signal received")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		defer cancel()

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("HTTP shutdown failed: %w", err)
		}

		if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("HTTP server failed during shutdown: %w", err)
		}
	}

	logger.Info("HTTP server stopped")

	return nil
}
