package redisproxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

func TestCapacityMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	manager, err := NewManager([]Backend{
		{Token: "first-secret", ID: "first", ConnectionString: "redis://localhost:6379", MaxConnections: 1},
		{Token: "second-secret", ID: "second", ConnectionString: "redis://localhost:6380", MaxConnections: 2},
	}, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)), registry)

	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = manager.Close() })

	_, release, err := manager.Acquire(context.Background(), "first-secret")

	if err != nil {
		t.Fatal(err)
	}

	defer release()

	_, releaseSecond, err := manager.AcquireDedicated(context.Background(), "second-secret")

	if err != nil {
		release()
		t.Fatal(err)
	}

	defer releaseSecond()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)

	defer cancel()

	if _, _, err := manager.Acquire(ctx, "first-secret"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected capacity timeout, got %v", err)
	}

	canceled, stop := context.WithCancel(context.Background())
	stop()

	if _, _, err := manager.Acquire(canceled, "first-secret"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled acquisition, got %v", err)
	}

	if _, _, err := manager.Acquire(context.Background(), "invalid-secret"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected unauthorized acquisition, got %v", err)
	}

	expected := `
# HELP uprest_redis_capacity_in_use Occupied backend capacity slots, including commands and streams.
# TYPE uprest_redis_capacity_in_use gauge
uprest_redis_capacity_in_use{backend_id="first"} 1
uprest_redis_capacity_in_use{backend_id="second"} 1
# HELP uprest_redis_capacity_limit Configured backend capacity limit.
# TYPE uprest_redis_capacity_limit gauge
uprest_redis_capacity_limit{backend_id="first"} 1
uprest_redis_capacity_limit{backend_id="second"} 2
`

	if err := testutil.GatherAndCompare(registry, strings.NewReader(expected), "uprest_redis_capacity_in_use", "uprest_redis_capacity_limit"); err != nil {
		t.Fatal(err)
	}

	for _, outcome := range []string{"acquired", "timeout", "canceled"} {
		var metric dto.Metric

		if err := manager.acquireDuration.WithLabelValues("first", outcome).(prometheus.Metric).Write(&metric); err != nil {
			t.Fatal(err)
		}

		if metric.Histogram.GetSampleCount() != 1 {
			t.Fatalf("got %d observations for %s, want 1", metric.Histogram.GetSampleCount(), outcome)
		}
	}

	release()
	release()
	releaseSecond()

	_, recovered, err := manager.Acquire(context.Background(), "first-secret")

	if err != nil {
		t.Fatal(err)
	}

	recovered()
	manager.mu.Lock()
	var idleBefore time.Time

	for _, pool := range manager.pools {
		idleBefore = pool.lastUsed
	}

	manager.mu.Unlock()

	if _, err := registry.Gather(); err != nil {
		t.Fatal(err)
	}

	manager.evictBefore(idleBefore)

	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	if _, _, err := manager.Acquire(context.Background(), "first-secret"); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected closed acquisition, got %v", err)
	}

	var closed dto.Metric
	_ = manager.acquireDuration.WithLabelValues("first", "closed").(prometheus.Metric).Write(&closed)

	if closed.Histogram.GetSampleCount() != 1 {
		t.Fatalf("unexpected closed acquisitions: %d", closed.Histogram.GetSampleCount())
	}

	expected = `
# HELP uprest_redis_capacity_in_use Occupied backend capacity slots, including commands and streams.
# TYPE uprest_redis_capacity_in_use gauge
uprest_redis_capacity_in_use{backend_id="first"} 0
uprest_redis_capacity_in_use{backend_id="second"} 0
# HELP uprest_redis_pools_active Current number of shared Redis pools.
# TYPE uprest_redis_pools_active gauge
uprest_redis_pools_active 0
# HELP uprest_redis_pools_created_total Total shared Redis pools created.
# TYPE uprest_redis_pools_created_total counter
uprest_redis_pools_created_total 1
# HELP uprest_redis_pools_evicted_total Total shared Redis pools evicted after becoming idle.
# TYPE uprest_redis_pools_evicted_total counter
uprest_redis_pools_evicted_total 1
`

	if err := testutil.GatherAndCompare(registry, strings.NewReader(expected), "uprest_redis_capacity_in_use", "uprest_redis_pools_active", "uprest_redis_pools_created_total", "uprest_redis_pools_evicted_total"); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentCapacityMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	manager, err := NewManager([]Backend{{
		Token: "test-token", ID: "concurrent", ConnectionString: "redis://localhost:6379", MaxConnections: 4,
	}}, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)), registry)

	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = manager.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()

	var pending sync.WaitGroup
	start := make(chan struct{})

	for i := range 32 {
		pending.Add(1)

		go func() {
			defer pending.Done()

			<-start
			var release func()
			var err error

			if i%2 == 0 {
				_, release, err = manager.Acquire(ctx, "test-token")
			} else {
				_, release, err = manager.AcquireDedicated(ctx, "test-token")
			}

			if err != nil {
				t.Error(err)
				return
			}

			defer release()

			if _, err := registry.Gather(); err != nil {
				t.Error(err)
			}
		}()
	}

	close(start)
	pending.Wait()
	var metric dto.Metric

	if err := manager.acquireDuration.WithLabelValues("concurrent", "acquired").(prometheus.Metric).Write(&metric); err != nil {
		t.Fatal(err)
	}

	if metric.Histogram.GetSampleCount() != 32 {
		t.Fatalf("got %d acquired observations, want 32", metric.Histogram.GetSampleCount())
	}

	expected := `
# HELP uprest_redis_capacity_in_use Occupied backend capacity slots, including commands and streams.
# TYPE uprest_redis_capacity_in_use gauge
uprest_redis_capacity_in_use{backend_id="concurrent"} 0
# HELP uprest_redis_pools_created_total Total shared Redis pools created.
# TYPE uprest_redis_pools_created_total counter
uprest_redis_pools_created_total 1
`

	if err := testutil.GatherAndCompare(registry, strings.NewReader(expected), "uprest_redis_capacity_in_use", "uprest_redis_pools_created_total"); err != nil {
		t.Fatal(err)
	}
}

func TestMetricsRegistrationRejectsDuplicateBackendIDs(t *testing.T) {
	backends := []Backend{
		{Token: "first-secret", ID: "duplicate", ConnectionString: "redis://localhost:6379", MaxConnections: 1},
		{Token: "second-secret", ID: "duplicate", ConnectionString: "redis://localhost:6380", MaxConnections: 1},
	}

	if _, err := NewManager(backends, time.Minute, nil, prometheus.NewRegistry()); err == nil {
		t.Fatal("expected metrics registration to reject duplicate backend IDs")
	}

	manager, err := NewManager(backends, time.Minute, nil, nil)

	if err != nil {
		t.Fatal(err)
	}

	_ = manager.Close()
}
