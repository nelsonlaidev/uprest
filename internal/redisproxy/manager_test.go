package redisproxy

import (
	"context"
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

	first, releaseFirst, err := manager.Acquire(context.Background(), "first-token")

	if err != nil {
		t.Fatal(err)
	}

	defer releaseFirst()

	firstAgain, releaseFirstAgain, err := manager.Acquire(context.Background(), "first-token")

	if err != nil {
		t.Fatal(err)
	}

	defer releaseFirstAgain()

	second, releaseSecond, err := manager.Acquire(context.Background(), "second-token")

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

	_, _, err := manager.Acquire(context.Background(), "wrong-token")

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

func TestManagerSharesCapacityWithDedicatedClients(t *testing.T) {
	manager := newTestManager(t, []Backend{{
		Token:            "test-token",
		ID:               "test",
		ConnectionString: "redis://localhost:6379",
		MaxConnections:   1,
	}}, time.Minute)

	shared, releaseShared, err := manager.Acquire(context.Background(), "test-token")

	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, _, err = manager.AcquireDedicated(ctx, "test-token")
	cancel()

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context deadline exceeded", err)
	}

	releaseShared()

	dedicated, releaseDedicated, err := manager.AcquireDedicated(context.Background(), "test-token")

	if err != nil {
		t.Fatal(err)
	}

	if dedicated == shared {
		t.Fatal("dedicated acquisition reused the shared Redis client")
	}

	if dedicated.Options().PoolSize != 1 || dedicated.Options().MaxActiveConns != 1 {
		t.Fatalf("unexpected dedicated client limits: %+v", dedicated.Options())
	}

	// go-redis normalizes ReadTimeout=-1 to 0, meaning no socket read deadline.
	if dedicated.Options().ReadTimeout != 0 {
		t.Fatalf("got dedicated client read timeout %s, want disabled", dedicated.Options().ReadTimeout)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, _, err = manager.Acquire(ctx, "test-token")
	cancel()

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context deadline exceeded", err)
	}

	releaseDedicated()
	releaseDedicated()

	_, release, err := manager.Acquire(context.Background(), "test-token")

	if err != nil {
		t.Fatal(err)
	}

	release()
}

func TestManagerCloseUnblocksCapacityWaiters(t *testing.T) {
	manager := newTestManager(t, []Backend{{
		Token:            "test-token",
		ID:               "test",
		ConnectionString: "redis://localhost:6379",
		MaxConnections:   1,
	}}, time.Minute)

	_, release, err := manager.Acquire(context.Background(), "test-token")

	if err != nil {
		t.Fatal(err)
	}

	waiting := make(chan error, 1)

	go func() {
		_, _, err := manager.Acquire(context.Background(), "test-token")
		waiting <- err
	}()

	closed := make(chan error, 1)

	go func() {
		closed <- manager.Close()
	}()

	select {
	case err := <-waiting:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("waiting acquisition returned %v, want ErrClosed", err)
		}

	case <-time.After(time.Second):
		t.Fatal("manager close did not unblock a capacity waiter")
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

func TestManagerEvictsAndRecreatesIdlePool(t *testing.T) {
	manager := newTestManager(t, []Backend{{
		Token:            "test-token",
		ID:               "test",
		ConnectionString: "redis://localhost:6379",
		MaxConnections:   3,
	}}, 30*time.Millisecond)

	first, release, err := manager.Acquire(context.Background(), "test-token")

	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(60 * time.Millisecond)

	if stats := manager.Stats(); stats.Active != 1 || stats.Evicted != 0 {
		t.Fatalf("pool was evicted while leased: %+v", stats)
	}

	release()
	waitForPoolCount(t, manager, 0)

	second, releaseSecond, err := manager.Acquire(context.Background(), "test-token")

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

	_, release, err := manager.Acquire(context.Background(), "test-token")

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
		_, temporaryRelease, err := manager.Acquire(context.Background(), "test-token")

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
