package redisproxy

import (
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestManagerCreatesSeparateBoundedPools(t *testing.T) {
	manager := newTestManager(t, []Backend{
		{
			Token:            "first-token",
			ID:               "first",
			ConnectionString: "redis://localhost:6379",
			MaxConnections:   2,
		},
		{
			Token:            "second-token",
			ID:               "second",
			ConnectionString: "redis://localhost:6379",
			MaxConnections:   5,
		},
	}, time.Minute)

	first, releaseFirst, err := manager.Acquire("first-token")

	if err != nil {
		t.Fatal(err)
	}

	defer releaseFirst()

	firstAgain, releaseFirstAgain, err := manager.Acquire("first-token")

	if err != nil {
		t.Fatal(err)
	}

	defer releaseFirstAgain()

	second, releaseSecond, err := manager.Acquire("second-token")

	if err != nil {
		t.Fatal(err)
	}

	defer releaseSecond()

	if first != firstAgain {
		t.Fatal("the same token did not reuse its Redis pool")
	}

	if first == second {
		t.Fatal("different tokens unexpectedly shared a Redis pool")
	}

	if first.Options().PoolSize != 2 || first.Options().MaxActiveConns != 2 {
		t.Fatalf("unexpected first pool limits: %+v", first.Options())
	}

	if second.Options().PoolSize != 5 || second.Options().MaxActiveConns != 5 {
		t.Fatalf("unexpected second pool limits: %+v", second.Options())
	}

	stats := manager.Stats()

	if stats.Active != 2 || stats.Created != 2 || stats.Evicted != 0 {
		t.Fatalf("unexpected pool stats: %+v", stats)
	}
}

func TestManagerRejectsUnknownToken(t *testing.T) {
	manager := newTestManager(t, []Backend{{
		Token:            "test-token",
		ID:               "test",
		ConnectionString: "redis://localhost:6379",
		MaxConnections:   3,
	}}, time.Minute)

	_, _, err := manager.Acquire("wrong-token")

	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("got %v, want ErrUnauthorized", err)
	}

	if stats := manager.Stats(); stats.Active != 0 || stats.Created != 0 {
		t.Fatalf("unauthorized access created a pool: %+v", stats)
	}

	if !manager.Authorized("test-token") || manager.Authorized("wrong-token") {
		t.Fatal("unexpected token authorization result")
	}
}

func TestManagerEvictsAndRecreatesIdlePool(t *testing.T) {
	manager := newTestManager(t, []Backend{{
		Token:            "test-token",
		ID:               "test",
		ConnectionString: "redis://localhost:6379",
		MaxConnections:   3,
	}}, 30*time.Millisecond)

	first, release, err := manager.Acquire("test-token")

	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(60 * time.Millisecond)

	if stats := manager.Stats(); stats.Active != 1 || stats.Evicted != 0 {
		t.Fatalf("pool was evicted while leased: %+v", stats)
	}

	release()
	waitForPoolCount(t, manager, 0)

	second, releaseSecond, err := manager.Acquire("test-token")

	if err != nil {
		t.Fatal(err)
	}

	releaseSecond()

	if first == second {
		t.Fatal("idle pool was not recreated")
	}

	stats := manager.Stats()

	if stats.Active != 1 || stats.Created != 2 || stats.Evicted != 1 {
		t.Fatalf("unexpected recreated pool stats: %+v", stats)
	}
}

func TestNewManagerRejectsInvalidBackends(t *testing.T) {
	tests := []struct {
		name     string
		backends []Backend
	}{
		{"no backends", nil},
		{"empty token", []Backend{{ID: "test", ConnectionString: "redis://localhost:6379", MaxConnections: 3}}},
		{"empty ID", []Backend{{Token: "token", ConnectionString: "redis://localhost:6379", MaxConnections: 3}}},
		{"invalid max connections", []Backend{{Token: "token", ID: "test", ConnectionString: "redis://localhost:6379"}}},
		{"invalid URL", []Backend{{Token: "token", ID: "test", ConnectionString: "://", MaxConnections: 3}}},
		{"duplicate token", []Backend{
			{Token: "token", ID: "first", ConnectionString: "redis://localhost:6379", MaxConnections: 3},
			{Token: "token", ID: "second", ConnectionString: "redis://localhost:6380", MaxConnections: 3},
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager, err := NewManager(test.backends, time.Minute, nil)

			if err == nil {
				_ = manager.Close()
				t.Fatal("expected an error")
			}
		})
	}
}

func TestManagerCloseWaitsForLeases(t *testing.T) {
	manager := newTestManager(t, []Backend{{
		Token:            "test-token",
		ID:               "test",
		ConnectionString: "redis://localhost:6379",
		MaxConnections:   3,
	}}, time.Minute)

	_, release, err := manager.Acquire("test-token")

	if err != nil {
		t.Fatal(err)
	}

	defer release()

	closed := make(chan error, 1)

	go func() {
		closed <- manager.Close()
	}()

	deadline := time.Now().Add(time.Second)

	for {
		_, temporaryRelease, err := manager.Acquire("test-token")

		if errors.Is(err, ErrClosed) {
			break
		}

		if err != nil {
			t.Fatal(err)
		}

		temporaryRelease()

		if time.Now().After(deadline) {
			t.Fatal("manager did not stop accepting leases")
		}

		time.Sleep(time.Millisecond)
	}

	select {
	case err := <-closed:
		t.Fatalf("Close returned before the active lease was released: %v", err)
	default:
	}

	release()

	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}

	case <-time.After(time.Second):
		t.Fatal("Close did not return after the active lease was released")
	}
}

func newTestManager(t *testing.T, backends []Backend, idleTimeout time.Duration) *Manager {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager, err := NewManager(backends, idleTimeout, logger)

	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})

	return manager
}

func waitForPoolCount(t *testing.T, manager *Manager, want int) {
	t.Helper()

	deadline := time.Now().Add(time.Second)

	for manager.Stats().Active != want {
		if time.Now().After(deadline) {
			t.Fatalf("got %d active pools, want %d", manager.Stats().Active, want)
		}

		time.Sleep(5 * time.Millisecond)
	}
}
