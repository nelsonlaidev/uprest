package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/nelsonlaidev/uprest/internal/redisproxy"
)

func TestMetricsEndpoint(t *testing.T) {
	registry := prometheus.NewRegistry()
	var logs bytes.Buffer
	handler, manager := newTestServer(t, []redisproxy.Backend{{
		Token: "backend-secret", ID: "offline", ConnectionString: "redis://localhost:1", MaxConnections: 1,
	}}, &logs, "metrics-secret", registry)

	for _, test := range []struct {
		name          string
		method        string
		path          string
		authorization string
		status        int
	}{
		{"successful scrape", http.MethodGet, "/metrics", "Bearer metrics-secret", http.StatusOK},
		{"missing token", http.MethodGet, "/metrics", "", http.StatusUnauthorized},
		{"backend token", http.MethodGet, "/metrics", "Bearer backend-secret", http.StatusUnauthorized},
		{"query token", http.MethodGet, "/metrics?_token=metrics-secret", "", http.StatusUnauthorized},
		{"header takes priority", http.MethodGet, "/metrics?_token=metrics-secret", "Bearer invalid", http.StatusUnauthorized},
		{"malformed header", http.MethodGet, "/metrics", "Basic metrics-secret", http.StatusUnauthorized},
		{"authentication before method", http.MethodPost, "/metrics", "", http.StatusUnauthorized},
		{"POST", http.MethodPost, "/metrics", "Bearer metrics-secret", http.StatusMethodNotAllowed},
		{"HEAD", http.MethodHead, "/metrics", "Bearer metrics-secret", http.StatusMethodNotAllowed},
	} {
		t.Run(test.name, func(t *testing.T) {
			logs.Reset()
			request := httptest.NewRequest(test.method, test.path, nil)
			request.Header.Set("Authorization", test.authorization)
			request.Header.Set(responseFormatHeader, "invalid")
			request.Header.Set(responseEncodingHeader, "base64")
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != test.status || response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("unexpected scrape response: %d %v", response.Code, response.Header())
			}

			if test.status == http.StatusOK {
				if !strings.HasPrefix(response.Header().Get("Content-Type"), "text/plain") || !strings.Contains(response.Body.String(), "uprest_redis_capacity_limit") {
					t.Fatalf("unexpected exposition: %v %q", response.Header(), response.Body.String())
				}
			} else {
				want := `{"error":"Unauthorized"}`

				if test.status == http.StatusMethodNotAllowed {
					want = `{"error":"Method Not Allowed"}`

					if response.Header().Get("Allow") != http.MethodGet {
						t.Fatalf("unexpected Allow header: %v", response.Header())
					}
				}

				if strings.TrimSpace(response.Body.String()) != want || response.Header().Get("Content-Type") != "application/json" {
					t.Fatalf("unexpected error response: %d %q", response.Code, response.Body.String())
				}
			}

			var entry map[string]any

			if err := json.Unmarshal(logs.Bytes(), &entry); err != nil {
				t.Fatal(err)
			}

			wantLevel := "INFO"

			if test.status == http.StatusOK {
				wantLevel = "DEBUG"
			}

			if entry["level"] != wantLevel || strings.Contains(logs.String(), "metrics-secret") {
				t.Fatalf("unexpected scrape log: %s", logs.String())
			}
		})
	}

	if stats := manager.Stats(); stats.Active != 0 || stats.Created != 0 {
		t.Fatalf("scrapes created a Redis pool: %+v", stats)
	}

	families, err := registry.Gather()

	if err != nil {
		t.Fatal(err)
	}

	for _, family := range families {
		if strings.HasPrefix(family.GetName(), "uprest_http_") {
			t.Fatal("metrics scrapes were included in HTTP metrics")
		}
	}

	_, release, err := manager.AcquireDedicated(context.Background(), "backend-secret")

	if err != nil {
		t.Fatal(err)
	}

	defer release()

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	request.Header.Set("Authorization", "Bearer metrics-secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK || metricSample(t, registry, "uprest_redis_capacity_in_use", map[string]string{"backend_id": "offline"}).Gauge.GetValue() != 1 {
		t.Fatal("scrape failed or changed exhausted backend capacity")
	}

	if !reservedPath("/metrics", true) || allowedMethods("/metrics", true) != http.MethodGet || reservedPath("/metrics", false) {
		t.Fatal("metrics route reservation is inconsistent")
	}
}

func TestHTTPMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	handler, _ := newTestServer(t, []redisproxy.Backend{{
		Token: "backend-secret", ID: "offline", ConnectionString: "redis://localhost:1", MaxConnections: 1,
	}}, io.Discard, "metrics-secret", registry)

	for _, test := range []struct {
		name   string
		method string
		path   string
		body   string
		token  string
		cancel bool
		route  string
		status string
	}{
		{"success", http.MethodGet, "/health", "", "", false, "health", "200"},
		{"unauthorized", http.MethodPost, "/", "", "", false, "command", "401"},
		{"invalid body", http.MethodPost, "/", "invalid JSON", "backend-secret", false, "command", "400"},
		{"invalid pipeline", http.MethodPost, "/pipeline", "invalid JSON", "backend-secret", false, "pipeline", "400"},
		{"invalid transaction", http.MethodPost, "/multi-exec", "invalid JSON", "backend-secret", false, "transaction", "400"},
		{"invalid subscription", http.MethodPost, "/subscribe", "", "backend-secret", false, "pubsub", "400"},
		{"monitor method", http.MethodGet, "/monitor", "", "backend-secret", false, "monitor", "405"},
		{"unsupported endpoint", http.MethodGet, "/monitor/private-channel", "", "backend-secret", false, "unsupported", "404"},
		{"Redis unavailable", http.MethodGet, "/GET/private-key", "", "backend-secret", false, "command", "502"},
		{"not ready", http.MethodGet, "/ready", "", "backend-secret", false, "ready", "503"},
		{"canceled without response", http.MethodGet, "/ready", "", "backend-secret", true, "ready", "canceled"},
		{"unknown method", "PRIVATE_METHOD", "/health", "", "", false, "health", "405"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))

			if test.token != "" {
				request.Header.Set("Authorization", "Bearer "+test.token)
			}

			if test.cancel {
				ctx, cancel := context.WithCancel(request.Context())
				cancel()
				request = request.WithContext(ctx)
			}

			handler.ServeHTTP(httptest.NewRecorder(), request)
			method := test.method

			if method == "PRIVATE_METHOD" {
				method = "OTHER"
			}

			sample := metricSample(t, registry, "uprest_http_requests_total", map[string]string{"route": test.route, "method": method, "status": test.status})

			if sample.Counter.GetValue() != 1 {
				t.Fatalf("request counted %v times", sample.Counter.GetValue())
			}
		})
	}

	sample := metricSample(t, registry, "uprest_http_request_duration_seconds", map[string]string{"route": "ready", "method": "GET"})

	if sample.Histogram.GetSampleCount() != 2 || len(sample.Histogram.Bucket) != len(redisproxy.MetricsDurationBuckets) {
		t.Fatalf("unexpected duration histogram: %v", sample.Histogram)
	}

	families, err := registry.Gather()

	if err != nil {
		t.Fatal(err)
	}

	for _, family := range families {
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				for _, private := range []string{"backend-secret", "metrics-secret", "private-key", "PRIVATE_METHOD", "redis://"} {
					if strings.Contains(label.GetValue(), private) {
						t.Fatalf("private or unbounded label value: %v", label)
					}
				}
			}
		}
	}
}

func TestMetricsDisabledPreservesPathCommands(t *testing.T) {
	handler, manager := newTestServer(t, []redisproxy.Backend{{
		Token: "backend-secret", ID: "offline", ConnectionString: "redis://localhost:1", MaxConnections: 1,
	}}, io.Discard, "", nil)
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	request.Header.Set("Authorization", "Bearer backend-secret")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadGateway || manager.Stats().Created != 1 || response.Header().Get("Cache-Control") == "no-store" {
		t.Fatalf("disabled metrics changed path command behavior: %d %v %+v", response.Code, response.Header(), manager.Stats())
	}
}

func TestFailedStreamsDoNotIncreaseMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	handler, _ := newTestServer(t, []redisproxy.Backend{{
		Token: "backend-secret", ID: "offline", ConnectionString: "redis://localhost:1", MaxConnections: 1,
	}}, io.Discard, "metrics-secret", registry)

	for _, path := range []string{"/subscribe/private-channel", "/psubscribe/private-pattern", "/monitor"} {
		request := httptest.NewRequest(http.MethodPost, path, nil)
		request.Header.Set("Authorization", "Bearer backend-secret")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)

		if response.Code != http.StatusBadGateway || metricSample(t, registry, "uprest_redis_capacity_in_use", map[string]string{"backend_id": "offline"}).Gauge.GetValue() != 0 {
			t.Fatalf("failed stream leaked capacity or returned %d", response.Code)
		}
	}

	families, err := registry.Gather()

	if err != nil {
		t.Fatal(err)
	}

	for _, family := range families {
		if family.GetName() == "uprest_streams_active" {
			t.Fatal("failed stream initialization created an active stream metric")
		}
	}
}

func TestConcurrentMetricsAndRegistryIsolation(t *testing.T) {
	registries := []*prometheus.Registry{prometheus.NewRegistry(), prometheus.NewRegistry()}
	var handlers []http.Handler

	for _, registry := range registries {
		handler, _ := newTestServer(t, []redisproxy.Backend{{
			Token: "backend-secret", ID: "same-id", ConnectionString: "redis://localhost:1", MaxConnections: 1,
		}}, io.Discard, "metrics-secret", registry)
		handlers = append(handlers, handler)
	}

	var pending sync.WaitGroup

	for i := range 32 {
		pending.Add(1)

		go func() {
			defer pending.Done()

			handlers[0].ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/health", nil))
			request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			request.Header.Set("Authorization", "Bearer metrics-secret")
			response := httptest.NewRecorder()
			handlers[i%2].ServeHTTP(response, request)

			if response.Code != http.StatusOK {
				t.Errorf("concurrent scrape returned %d", response.Code)
			}
		}()
	}

	pending.Wait()
	handlers[1].ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/health", nil))

	for i, want := range []float64{32, 1} {
		metric := metricSample(t, registries[i], "uprest_http_requests_total", map[string]string{"route": "health", "method": "GET", "status": "200"})

		if metric.Counter.GetValue() != want {
			t.Fatalf("registry %d: got %v requests, want %v", i, metric.Counter.GetValue(), want)
		}
	}
}

func metricSample(t *testing.T, gatherer prometheus.Gatherer, name string, labels map[string]string) *dto.Metric {
	t.Helper()

	families, err := gatherer.Gather()

	if err != nil {
		t.Fatal(err)
	}

	for _, family := range families {
		if family.GetName() != name {
			continue
		}

		for _, metric := range family.Metric {
			if len(metric.Label) != len(labels) {
				continue
			}

			matches := true

			for _, label := range metric.Label {
				value, exists := labels[label.GetName()]
				matches = matches && exists && value == label.GetValue()
			}

			if matches {
				return metric
			}
		}
	}

	t.Fatalf("metric %s with labels %v was not found", name, labels)

	return nil
}
