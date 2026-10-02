package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nelsonlaidev/uprest/internal/redisproxy"
)

type sseDeadlineWriter struct {
	header    http.Header
	body      bytes.Buffer
	deadlines []time.Time
	flushed   bool
}

func (w *sseDeadlineWriter) Header() http.Header {
	return w.header
}

func (w *sseDeadlineWriter) Write(body []byte) (int, error) {
	return w.body.Write(body)
}

func (w *sseDeadlineWriter) WriteHeader(int) {}

func (w *sseDeadlineWriter) FlushError() error {
	w.flushed = true

	return nil
}

func (w *sseDeadlineWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadlines = append(w.deadlines, deadline)

	return nil
}

func TestHealthAndReadyValidation(t *testing.T) {
	handler, manager := newTestServer(t, []redisproxy.Backend{{
		Token:            "test-token",
		ID:               "test",
		ConnectionString: "redis://localhost:1",
		MaxConnections:   1,
	}}, io.Discard)

	tests := []struct {
		name   string
		method string
		path   string
		token  string
		status int
		want   string
		allow  string
	}{
		{"health without token", http.MethodGet, "/health", "", http.StatusOK, `{"status":"ok"}`, ""},
		{"health with invalid token", http.MethodGet, "/health", "Bearer wrong", http.StatusOK, `{"status":"ok"}`, ""},
		{"health POST", http.MethodPost, "/health", "", http.StatusMethodNotAllowed, `{"error":"Method Not Allowed"}`, http.MethodGet},
		{"health HEAD", http.MethodHead, "/health", "", http.StatusMethodNotAllowed, `{"error":"Method Not Allowed"}`, http.MethodGet},
		{"ready without token", http.MethodGet, "/ready", "", http.StatusUnauthorized, `{"error":"Unauthorized"}`, ""},
		{"ready with invalid token", http.MethodGet, "/ready", "Bearer wrong", http.StatusUnauthorized, `{"error":"Unauthorized"}`, ""},
		{"ready header overrides query", http.MethodGet, "/ready?_token=test-token", "Bearer wrong", http.StatusUnauthorized, `{"error":"Unauthorized"}`, ""},
		{"ready malformed header overrides query", http.MethodGet, "/ready?_token=test-token", "Basic test-token", http.StatusUnauthorized, `{"error":"Unauthorized"}`, ""},
		{"ready POST", http.MethodPost, "/ready", "Bearer test-token", http.StatusMethodNotAllowed, `{"error":"Method Not Allowed"}`, http.MethodGet},
		{"ready HEAD", http.MethodHead, "/ready", "Bearer test-token", http.StatusMethodNotAllowed, `{"error":"Method Not Allowed"}`, http.MethodGet},
		{"ready authenticates before method", http.MethodPost, "/ready", "", http.StatusUnauthorized, `{"error":"Unauthorized"}`, ""},
		{"ready query token POST", http.MethodPost, "/ready?_token=test-token", "", http.StatusMethodNotAllowed, `{"error":"Method Not Allowed"}`, http.MethodGet},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, nil)
			request.Header.Set("Upstash-Response-Format", "resp2")
			request.Header.Set("Upstash-Encoding", "base64")

			if test.token != "" {
				request.Header.Set("Authorization", test.token)
			}

			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != test.status || strings.TrimSpace(response.Body.String()) != test.want {
				t.Fatalf("got %d %q, want %d %q", response.Code, response.Body.String(), test.status, test.want)
			}

			if response.Header().Get("Allow") != test.allow || response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("unexpected probe headers: %v", response.Header())
			}
		})
	}

	for _, path := range []string{"/health", "/ready"} {
		if !reservedPath(path) || allowedMethods(path) != http.MethodGet {
			t.Fatalf("probe %q has inconsistent routing classification", path)
		}
	}

	for _, path := range []string{"/Health", "/health/", "/Ready", "/ready/", "/healthz", "/readyz"} {
		if reservedPath(path) || allowedMethods(path) != http.MethodGet+", "+http.MethodPost {
			t.Fatalf("path command %q unexpectedly classified as a probe", path)
		}

		response := httptest.NewRecorder()

		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))

		if response.Code != http.StatusUnauthorized {
			t.Fatalf("path %q unexpectedly matched a probe: %d", path, response.Code)
		}
	}

	if stats := manager.Stats(); stats.Active != 0 || stats.Created != 0 {
		t.Fatalf("probe validation created a Redis pool: %+v", stats)
	}
}

func TestHealthShutdownAndReadyCancellation(t *testing.T) {
	handler, manager := newTestServer(t, []redisproxy.Backend{{
		Token:            "test-token",
		ID:               "test",
		ConnectionString: "redis://localhost:1",
		MaxConnections:   1,
	}}, io.Discard)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	request := httptest.NewRequest(http.MethodGet, "/health", nil).WithContext(ctx)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable || strings.TrimSpace(response.Body.String()) != `{"status":"shutting_down"}` {
		t.Fatalf("unexpected shutdown health response: %d %q", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, "/ready", nil).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer test-token")
	response = httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Body.Len() != 0 {
		t.Fatalf("canceled request wrote a response: %q", response.Body.String())
	}

	if stats := manager.Stats(); stats.Active != 0 || stats.Created != 0 {
		t.Fatalf("canceled readiness created a Redis pool: %+v", stats)
	}
}

func TestReadyColdConnectionDeadline(t *testing.T) {
	for _, test := range []struct {
		name   string
		wait   bool
		cancel bool
	}{
		{name: "stalled handshake"},
		{name: "capacity and handshake share deadline", wait: true},
		{name: "cancel during handshake", cancel: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")

			if err != nil {
				t.Fatal(err)
			}

			t.Cleanup(func() { _ = listener.Close() })

			accepted := make(chan struct{})

			go func() {
				connection, acceptErr := listener.Accept()

				if acceptErr != nil {
					return
				}

				t.Cleanup(func() { _ = connection.Close() })

				defer func() { _ = connection.Close() }()
				close(accepted)
				_, _ = io.Copy(io.Discard, connection)
			}()

			handler, manager := newTestServer(t, []redisproxy.Backend{{
				Token:            "test-token",
				ID:               "test",
				ConnectionString: "redis://" + listener.Addr().String(),
				MaxConnections:   1,
			}}, io.Discard)

			if test.wait {
				_, release, acquireErr := manager.Acquire(context.Background(), "test-token")

				if acquireErr != nil {
					t.Fatal(acquireErr)
				}

				defer release()

				timer := time.AfterFunc(time.Second, release)

				defer timer.Stop()
			}

			ctx, cancel := context.WithCancel(context.Background())

			defer cancel()

			if test.cancel {
				go func() {
					select {
					case <-accepted:
						cancel()
					case <-ctx.Done():
					}
				}()
			}

			request := httptest.NewRequest(http.MethodGet, "/ready", nil).WithContext(ctx)
			request.Header.Set("Authorization", "Bearer test-token")
			response := httptest.NewRecorder()
			started := time.Now()

			handler.ServeHTTP(response, request)

			if elapsed := time.Since(started); (!test.cancel && elapsed < readinessTimeout-100*time.Millisecond) || elapsed > readinessTimeout+2*time.Second {
				t.Fatalf("cold connection probe took %s, want approximately %s", elapsed, readinessTimeout)
			}

			if test.cancel {
				if response.Body.Len() != 0 {
					t.Fatalf("canceled probe wrote a response: %q", response.Body.String())
				}
			} else if response.Code != http.StatusServiceUnavailable || strings.TrimSpace(response.Body.String()) != `{"status":"not_ready"}` {
				t.Fatalf("unexpected stalled probe response: %d %q", response.Code, response.Body.String())
			}

			leaseCtx, stop := context.WithTimeout(context.Background(), time.Second)

			defer stop()

			_, release, acquireErr := manager.Acquire(leaseCtx, "test-token")

			if acquireErr != nil {
				t.Fatalf("probe leaked capacity: %v", acquireErr)
			}

			release()
		})
	}
}

func TestHandlerRejectsUnauthorizedAndMalformedRequests(t *testing.T) {
	handler, _ := newTestServer(t, []redisproxy.Backend{{
		Token:            "test-token",
		ID:               "test",
		ConnectionString: "redis://localhost:1",
		MaxConnections:   3,
	}}, io.Discard)

	tests := []struct {
		name   string
		method string
		path   string
		token  string
		body   string
		status int
		want   string
	}{
		{"missing token", http.MethodPost, "/", "", `["PING"]`, http.StatusUnauthorized, `{"error":"Unauthorized"}`},
		{"empty bearer token", http.MethodPost, "/", "Bearer ", `["PING"]`, http.StatusUnauthorized, `{"error":"Unauthorized"}`},
		{"wrong token", http.MethodPost, "/", "Bearer wrong", `["PING"]`, http.StatusUnauthorized, `{"error":"Unauthorized"}`},
		{"invalid body", http.MethodPost, "/", "Bearer test-token", `[]`, http.StatusBadRequest, `{"error":"Invalid command"}`},
		{"invalid JSON", http.MethodPost, "/", "Bearer test-token", `["PING"`, http.StatusBadRequest, `{"error":"Invalid command"}`},
		{"pipeline missing token", http.MethodPost, "/pipeline", "", `[["PING"]]`, http.StatusUnauthorized, `{"error":"Unauthorized"}`},
		{"invalid pipeline", http.MethodPost, "/pipeline", "Bearer test-token", `[["GET",null]]`, http.StatusBadRequest, `{"error":"Invalid pipeline"}`},
		{"invalid transaction", http.MethodPost, "/multi-exec", "Bearer test-token", `[]`, http.StatusBadRequest, `{"error":"Invalid transaction"}`},
		{"path command missing token", http.MethodGet, "/ping", "", "", http.StatusUnauthorized, `{"error":"Unauthorized"}`},
		{"root method not allowed", http.MethodGet, "/", "Bearer test-token", "", http.StatusMethodNotAllowed, `{"error":"Method Not Allowed"}`},
		{"root method without token", http.MethodGet, "/", "", "", http.StatusUnauthorized, `{"error":"Unauthorized"}`},
		{"subscription method not allowed", http.MethodGet, "/subscribe/channel", "Bearer test-token", "", http.StatusMethodNotAllowed, `{"error":"Method Not Allowed"}`},
		{"monitor without token", http.MethodPost, "/monitor", "", "", http.StatusUnauthorized, `{"error":"Unauthorized"}`},
		{"subscription without token", http.MethodPost, "/subscribe/channel", "", "", http.StatusUnauthorized, `{"error":"Unauthorized"}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))

			if test.token != "" {
				request.Header.Set("Authorization", test.token)
			}

			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != test.status || strings.TrimSpace(response.Body.String()) != test.want {
				t.Fatalf("got %d %q, want %d %q", response.Code, response.Body.String(), test.status, test.want)
			}
		})
	}
}

func TestQueryTokenAuthentication(t *testing.T) {
	handler, _ := newTestServer(t, []redisproxy.Backend{{
		Token:            "test-token",
		ID:               "test",
		ConnectionString: "redis://localhost:1",
		MaxConnections:   3,
	}}, io.Discard)

	tests := []struct {
		name          string
		path          string
		authorization string
		setHeader     bool
		wantStatus    int
	}{
		{"query token", "/?_token=test-token", "", false, http.StatusBadRequest},
		{"encoded query token", "/?_token=test%2Dtoken", "", false, http.StatusBadRequest},
		{"missing query token", "/", "", false, http.StatusUnauthorized},
		{"empty query token", "/?_token=", "", false, http.StatusUnauthorized},
		{"invalid query token", "/?_token=wrong", "", false, http.StatusUnauthorized},
		{"valid header takes precedence", "/?_token=wrong", "Bearer test-token", true, http.StatusBadRequest},
		{"invalid header does not fall back", "/?_token=test-token", "Bearer wrong", true, http.StatusUnauthorized},
		{"malformed header does not fall back", "/?_token=test-token", "Basic test-token", true, http.StatusUnauthorized},
		{"empty header does not fall back", "/?_token=test-token", "", true, http.StatusUnauthorized},
		{"empty bearer does not fall back", "/?_token=test-token", "Bearer ", true, http.StatusUnauthorized},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(`[]`))

			if test.setHeader {
				request.Header.Set("Authorization", test.authorization)
			}

			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != test.wantStatus {
				t.Fatalf("got %d %q, want %d", response.Code, response.Body.String(), test.wantStatus)
			}
		})
	}

	for _, path := range []string{"/pipeline?_token=test-token", "/multi-exec?_token=test-token"} {
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`[]`))
		response := httptest.NewRecorder()

		handler.ServeHTTP(response, request)

		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s returned %d %q, want 400", path, response.Code, response.Body.String())
		}
	}

	request := httptest.NewRequest(http.MethodGet, "/monitor?_token=test-token", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("path request returned %d %q, want 405", response.Code, response.Body.String())
	}
}

func TestRequestLogging(t *testing.T) {
	for _, test := range []struct {
		name     string
		method   string
		path     string
		token    string
		cancel   bool
		occupy   bool
		level    slog.Level
		silent   bool
		status   int
		logLevel string
	}{
		{name: "command authentication failure", method: http.MethodGet, path: "/ping", status: http.StatusUnauthorized, logLevel: "INFO"},
		{name: "successful health at debug", method: http.MethodGet, path: "/health", level: slog.LevelDebug, status: http.StatusOK, logLevel: "DEBUG"},
		{name: "successful health hidden at info", method: http.MethodGet, path: "/health", silent: true, status: http.StatusOK},
		{name: "failed readiness visible at info", method: http.MethodGet, path: "/ready", status: http.StatusUnauthorized, logLevel: "INFO"},
		{name: "unsupported health method visible at info", method: http.MethodPost, path: "/health", status: http.StatusMethodNotAllowed, logLevel: "INFO"},
		{name: "shutdown health visible at info", method: http.MethodGet, path: "/health", cancel: true, status: http.StatusServiceUnavailable, logLevel: "INFO"},
		{name: "canceled readiness without response", method: http.MethodGet, path: "/ready", token: "test-token", cancel: true, logLevel: "INFO"},
		{name: "canceled command capacity wait", method: http.MethodPost, path: "/", token: "test-token", cancel: true, occupy: true, logLevel: "INFO"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			_, manager := newTestServer(t, []redisproxy.Backend{{
				Token:            "test-token",
				ID:               "test",
				ConnectionString: "redis://localhost:1",
				MaxConnections:   1,
			}}, &logs)
			handler := NewHandler(manager, slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: test.level})))

			if test.occupy {
				_, release, err := manager.Acquire(context.Background(), "test-token")

				if err != nil {
					t.Fatal(err)
				}

				defer release()
				logs.Reset()
			}

			request := httptest.NewRequest(test.method, test.path, nil)

			if test.token != "" {
				request.Header.Set("Authorization", "Bearer "+test.token)
			}

			if test.cancel {
				ctx, cancel := context.WithCancel(request.Context())
				cancel()
				request = request.WithContext(ctx)
			}

			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if test.status != 0 && response.Code != test.status {
				t.Fatalf("got %d %q, want %d", response.Code, response.Body.String(), test.status)
			}

			if response.Header().Get("X-Request-ID") != "" {
				t.Fatal("unexpected X-Request-ID response header")
			}

			if test.silent {
				if logs.Len() != 0 {
					t.Fatalf("successful probe logged at info: %s", logs.String())
				}

				return
			}

			var entry map[string]any

			if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &entry); err != nil {
				t.Fatal(err)
			}

			if entry["request_id"] == "" || entry["method"] != test.method || entry["path"] != test.path || entry["level"] != test.logLevel {
				t.Fatalf("unexpected request log: %#v", entry)
			}

			if test.status == 0 {
				if response.Body.Len() != 0 || entry["msg"] != "request canceled" || entry["canceled"] != true || entry["error"] != context.Canceled.Error() {
					t.Fatalf("unexpected canceled request response/log: %q %#v", response.Body.String(), entry)
				}

				if _, exists := entry["status"]; exists {
					t.Fatalf("canceled request logged an unwritten HTTP status: %#v", entry)
				}
			} else if entry["msg"] != "request completed" || entry["status"] != float64(test.status) {
				t.Fatalf("unexpected completed request log: %#v", entry)
			}
		})
	}
}

func TestQueryTokenNotLogged(t *testing.T) {
	for _, test := range []struct {
		name   string
		token  string
		status int
	}{
		{"valid", "test-token", http.StatusMethodNotAllowed},
		{"invalid", "query-secret", http.StatusUnauthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			handler, _ := newTestServer(t, []redisproxy.Backend{{
				Token:            "test-token",
				ID:               "test",
				ConnectionString: "redis://localhost:1",
				MaxConnections:   3,
			}}, &logs)
			request := httptest.NewRequest(http.MethodGet, "/monitor?_token="+test.token, nil)
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != test.status {
				t.Fatalf("got %d %q, want %d", response.Code, response.Body.String(), test.status)
			}

			if strings.Contains(logs.String(), test.token) || strings.Contains(logs.String(), "_token") {
				t.Fatalf("query token appeared in request logs: %s", logs.String())
			}
		})
	}
}

func TestMonitorRequestValidationDoesNotCreatePool(t *testing.T) {
	handler, manager := newTestServer(t, []redisproxy.Backend{{
		Token:            "test-token",
		ID:               "test",
		ConnectionString: "redis://localhost:1",
		MaxConnections:   3,
	}}, io.Discard)

	for _, test := range []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantAllow  string
	}{
		{"monitor requires POST", http.MethodGet, "/monitor", http.StatusMethodNotAllowed, http.MethodPost},
		{"monitor rejects PUT", http.MethodPut, "/MONITOR", http.StatusMethodNotAllowed, http.MethodPost},
		{"monitor rejects trailing slash", http.MethodPost, "/monitor/", http.StatusNotFound, ""},
		{"monitor rejects arguments", http.MethodPost, "/monitor/extra", http.StatusNotFound, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, nil)
			request.Header.Set("Authorization", "Bearer test-token")
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != test.wantStatus {
				t.Fatalf("monitor returned %d, want %d", response.Code, test.wantStatus)
			}

			if response.Header().Get("Allow") != test.wantAllow {
				t.Fatalf("got Allow %q, want %q", response.Header().Get("Allow"), test.wantAllow)
			}
		})
	}

	if stats := manager.Stats(); stats.Active != 0 || stats.Created != 0 {
		t.Fatalf("invalid monitor request created a Redis pool: %+v", stats)
	}
}

func TestMonitorCancellationBeforeStartDoesNotWriteError(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:1",
		Protocol: 2,
	})
	request := httptest.NewRequest(http.MethodPost, "/monitor", nil)
	ctx, cancel := context.WithCancel(request.Context())
	request = request.WithContext(ctx)
	cancel()

	response := httptest.NewRecorder()
	released := false
	handler := &apiHandler{}

	handler.serveMonitor(response, request, client, func() {
		released = true
		_ = client.Close()
	})

	if !released {
		t.Fatal("canceled monitor did not release its dedicated client")
	}

	if response.Body.Len() != 0 || response.Header().Get("Content-Type") != "" {
		t.Fatalf("canceled monitor wrote a response: headers=%v body=%q", response.Header(), response.Body.String())
	}
}

func TestSubscriptionRequestValidation(t *testing.T) {
	handler, manager := newTestServer(t, []redisproxy.Backend{{
		Token:            "test-token",
		ID:               "test",
		ConnectionString: "redis://localhost:1",
		MaxConnections:   3,
	}}, io.Discard)

	for _, test := range []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantBody   string
		wantAllow  string
	}{
		{"subscribe requires POST", http.MethodGet, "/subscribe/channel", http.StatusMethodNotAllowed, `{"error":"Method Not Allowed"}`, http.MethodPost},
		{"psubscribe requires POST", http.MethodPut, "/psubscribe/pattern", http.StatusMethodNotAllowed, `{"error":"Method Not Allowed"}`, http.MethodPost},
		{"missing channel", http.MethodPost, "/subscribe", http.StatusBadRequest, `{"error":"Invalid subscription"}`, ""},
		{"empty channel", http.MethodPost, "/subscribe/", http.StatusBadRequest, `{"error":"Invalid subscription"}`, ""},
		{"empty middle channel", http.MethodPost, "/subscribe/first//second", http.StatusBadRequest, `{"error":"Invalid subscription"}`, ""},
		{"channel with LF", http.MethodPost, "/subscribe/line%0Abreak", http.StatusBadRequest, `{"error":"Invalid subscription"}`, ""},
		{"missing pattern", http.MethodPost, "/psubscribe", http.StatusBadRequest, `{"error":"Invalid subscription"}`, ""},
		{"empty pattern", http.MethodPost, "/psubscribe/", http.StatusBadRequest, `{"error":"Invalid subscription"}`, ""},
		{"pattern with CR", http.MethodPost, "/psubscribe/line%0Dbreak", http.StatusBadRequest, `{"error":"Invalid subscription"}`, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, nil)
			request.Header.Set("Authorization", "Bearer test-token")
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != test.wantStatus || strings.TrimSpace(response.Body.String()) != test.wantBody {
				t.Fatalf("got %d %q, want %d %q", response.Code, response.Body.String(), test.wantStatus, test.wantBody)
			}

			if response.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("got content type %q, want application/json", response.Header().Get("Content-Type"))
			}

			if response.Header().Get("Allow") != test.wantAllow {
				t.Fatalf("got Allow %q, want %q", response.Header().Get("Allow"), test.wantAllow)
			}
		})
	}

	if stats := manager.Stats(); stats.Active != 0 || stats.Created != 0 {
		t.Fatalf("invalid subscription requests created a Redis pool: %+v", stats)
	}
}

func TestDecodeSubscriptionNames(t *testing.T) {
	for _, test := range []struct {
		name string
		path string
		want []string
	}{
		{"channel", "/subscribe/channel", []string{"channel"}},
		{"multiple channels", "/subscribe/first/second", []string{"first", "second"}},
		{"decoded channels", "/subscribe/hello%20world/path%2Fchannel", []string{"hello world", "path/channel"}},
		{"decoded patterns", "/psubscribe/prefix%3A%2A/other%3F", []string{"prefix:*", "other?"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := decodeSubscriptionNames(test.path)

			if err != nil {
				t.Fatal(err)
			}

			if strings.Join(got, "\x00") != strings.Join(test.want, "\x00") {
				t.Fatalf("got %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestFormatSubscriptionEvent(t *testing.T) {
	for _, test := range []struct {
		name    string
		event   any
		want    string
		wantErr error
	}{
		{"subscribe", &redis.Subscription{Kind: "subscribe", Channel: "news", Count: 2}, "data: subscribe,news,2\n\n", nil},
		{"psubscribe", &redis.Subscription{Kind: "psubscribe", Channel: "news:*", Count: 1}, "data: psubscribe,news:*,1\n\n", nil},
		{"message", &redis.Message{Channel: "news", Payload: `{"id":1}`}, "data: message,news,{\"id\":1}\n\n", nil},
		{"pmessage", &redis.Message{Pattern: "news:*", Channel: "news:1", Payload: "hello"}, "data: pmessage,news:*,news:1,hello\n\n", nil},
		{"message with LF", &redis.Message{Channel: "news", Payload: "first\nsecond"}, "", errInvalidSubscriptionEvent},
		{"pmessage with CR", &redis.Message{Pattern: "news:*", Channel: "news:1", Payload: "first\rsecond"}, "", errInvalidSubscriptionEvent},
		{"unsupported", &redis.Pong{Payload: "pong"}, "", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := formatSubscriptionEvent(test.event)

			if string(got) != test.want || !errors.Is(err, test.wantErr) {
				t.Fatalf("got (%q, %v), want (%q, %v)", got, err, test.want, test.wantErr)
			}
		})
	}
}

func TestFormatMonitorEvent(t *testing.T) {
	for _, test := range []struct {
		name         string
		event        string
		confirmation bool
		want         string
		wantErr      error
	}{
		{"confirmation", "OK", true, "data: \"OK\"\n\n", nil},
		{"command", `1721284008.663811 [0 127.0.0.1:6379] "SET" "key" "value"`, false, "data: 1721284008.663811 [0 127.0.0.1:6379] \"SET\" \"key\" \"value\"\n\n", nil},
		{"invalid confirmation", "PONG", true, "", errInvalidMonitorConfirmation},
		{"event with LF", "first\nsecond", false, "", errInvalidMonitorEvent},
		{"event with CR", "first\rsecond", false, "", errInvalidMonitorEvent},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := formatMonitorEvent(test.event, test.confirmation)

			if string(got) != test.want || !errors.Is(err, test.wantErr) {
				t.Fatalf("got (%q, %v), want (%q, %v)", got, err, test.want, test.wantErr)
			}
		})
	}
}

func TestWriteSSEUsesBoundedWriteDeadline(t *testing.T) {
	writer := &sseDeadlineWriter{header: make(http.Header)}
	controller := http.NewResponseController(writer)
	started := time.Now()

	if err := writeSSE(writer, controller, []byte("data: message,news,hello\n\n")); err != nil {
		t.Fatal(err)
	}

	if writer.body.String() != "data: message,news,hello\n\n" || !writer.flushed {
		t.Fatalf("unexpected SSE write: body=%q flushed=%v", writer.body.String(), writer.flushed)
	}

	if len(writer.deadlines) != 2 {
		t.Fatalf("got %d write deadlines, want bounded and cleared deadlines", len(writer.deadlines))
	}

	if writer.deadlines[0].Before(started) || writer.deadlines[0].After(started.Add(streamWriteTimeout+time.Second)) {
		t.Fatalf("got initial write deadline %s, want approximately %s", writer.deadlines[0], started.Add(streamWriteTimeout))
	}

	if !writer.deadlines[1].IsZero() {
		t.Fatalf("got final write deadline %s, want it cleared between writes", writer.deadlines[1])
	}
}

func TestResponseFormatValidation(t *testing.T) {
	handler, manager := newTestServer(t, []redisproxy.Backend{{
		Token:            "test-token",
		ID:               "test",
		ConnectionString: "redis://localhost:1",
		MaxConnections:   3,
	}}, io.Discard)

	for _, test := range []struct {
		name      string
		path      string
		format    string
		encoding  string
		wantError string
	}{
		{"unknown format", "/", "xml", "", invalidResponseFormatMessage},
		{"RESP2 with base64", "/", "resp2", "base64", invalidResponseEncodingMessage},
		{"multi-exec with RESP2 and base64", "/multi-exec", "resp2", "base64", invalidResponseEncodingMessage},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(`["PING"]`))
			request.Header.Set("Authorization", "Bearer test-token")
			request.Header.Set(responseFormatHeader, test.format)

			if test.encoding != "" {
				request.Header.Set(responseEncodingHeader, test.encoding)
			}

			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			wantBody := `{"error":"` + test.wantError + `"}`

			if response.Code != http.StatusBadRequest || strings.TrimSpace(response.Body.String()) != wantBody {
				t.Fatalf("got %d %q, want 400 %q", response.Code, response.Body.String(), wantBody)
			}

			if response.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("got content type %q, want application/json", response.Header().Get("Content-Type"))
			}
		})
	}

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer test-token")
	request.Header.Set(responseFormatHeader, "xml")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusMethodNotAllowed || strings.TrimSpace(response.Body.String()) != `{"error":"Method Not Allowed"}` {
		t.Fatalf("got %d %q, want a method error before response format validation", response.Code, response.Body.String())
	}

	if response.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("got Allow %q, want POST", response.Header().Get("Allow"))
	}

	if stats := manager.Stats(); stats.Active != 0 || stats.Created != 0 {
		t.Fatalf("invalid response options created a Redis pool: %+v", stats)
	}

	request = httptest.NewRequest(http.MethodPost, "/multi-exec", strings.NewReader(`[]`))
	request.Header.Set("Authorization", "Bearer test-token")
	request.Header.Set(responseFormatHeader, "RESP2")
	response = httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest || strings.TrimSpace(response.Body.String()) != `{"error":"Invalid transaction"}` {
		t.Fatalf("got %d %q, want a JSON transaction error", response.Code, response.Body.String())
	}

	if response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("got content type %q, want application/json", response.Header().Get("Content-Type"))
	}
}

func TestRESP2InfrastructureErrorsUseJSON(t *testing.T) {
	handler, _ := newTestServer(t, []redisproxy.Backend{{
		Token:            "test-token",
		ID:               "test",
		ConnectionString: "redis://127.0.0.1:1",
		MaxConnections:   3,
	}}, io.Discard)

	for _, test := range []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"command", http.MethodGet, "/ping", ""},
		{"pipeline", http.MethodPost, "/pipeline", `[["PING"]]`},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
			request.Header.Set("Authorization", "Bearer test-token")
			request.Header.Set(responseFormatHeader, "resp2")
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != http.StatusBadGateway || strings.TrimSpace(response.Body.String()) != `{"error":"Redis unavailable"}` {
				t.Fatalf("got %d %q, want JSON Redis unavailable response", response.Code, response.Body.String())
			}

			if response.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("got content type %q, want application/json", response.Header().Get("Content-Type"))
			}
		})
	}
}

func TestStreamingInfrastructureErrorsUseJSON(t *testing.T) {
	handler, _ := newTestServer(t, []redisproxy.Backend{{
		Token:            "test-token",
		ID:               "test",
		ConnectionString: "redis://127.0.0.1:1",
		MaxConnections:   3,
	}}, io.Discard)

	for _, test := range []struct {
		path     string
		format   string
		encoding string
	}{
		{"/subscribe/channel?_token=test-token", "xml", "base64"},
		{"/psubscribe/channel:*?_token=test-token", "resp2", "base64"},
		{"/monitor?_token=test-token", "xml", "base64"},
	} {
		request := httptest.NewRequest(http.MethodPost, test.path, nil)
		request.Header.Set(responseFormatHeader, test.format)
		request.Header.Set(responseEncodingHeader, test.encoding)
		response := httptest.NewRecorder()

		handler.ServeHTTP(response, request)

		if response.Code != http.StatusBadGateway || strings.TrimSpace(response.Body.String()) != `{"error":"Redis unavailable"}` {
			t.Fatalf("%s returned %d %q, want JSON Redis unavailable response", test.path, response.Code, response.Body.String())
		}

		if response.Header().Get("Content-Type") != "application/json" {
			t.Fatalf("got content type %q, want application/json", response.Header().Get("Content-Type"))
		}
	}
}

func TestResponseUsesRESP2(t *testing.T) {
	for _, test := range []struct {
		name     string
		format   string
		encoding string
		want     bool
		wantErr  error
	}{
		{"default", "", "", false, nil},
		{"JSON", "JSON", "", false, nil},
		{"RESP2", "RESP2", "", true, nil},
		{"invalid", "other", "", false, errInvalidResponseFormat},
		{"RESP2 with base64", "resp2", "BASE64", false, errInvalidResponseEncoding},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/ping", nil)
			request.Header.Set(responseFormatHeader, test.format)
			request.Header.Set(responseEncodingHeader, test.encoding)

			got, err := responseUsesRESP2(request)

			if got != test.want || !errors.Is(err, test.wantErr) {
				t.Fatalf("got (%v, %v), want (%v, %v)", got, err, test.want, test.wantErr)
			}
		})
	}
}

func TestAuthenticatedResponsesIncludeSyncToken(t *testing.T) {
	handler, _ := newTestServer(t, []redisproxy.Backend{{
		Token:            "test-token",
		ID:               "test",
		ConnectionString: "redis://localhost:1",
		MaxConnections:   3,
	}}, io.Discard)

	var previous string

	for range 2 {
		request := httptest.NewRequest(http.MethodGet, "/monitor", nil)
		request.Header.Set("Authorization", "Bearer test-token")
		request.Header.Set(syncTokenHeader, previous)
		response := httptest.NewRecorder()

		handler.ServeHTTP(response, request)

		current := response.Header().Get(syncTokenHeader)

		if current == "" || current == previous {
			t.Fatalf("got sync token %q after %q, want a new non-empty token", current, previous)
		}

		previous = current
	}
}

func newTestServer(t *testing.T, backends []redisproxy.Backend, logOutput io.Writer) (http.Handler, *redisproxy.Manager) {
	t.Helper()

	logger := slog.New(slog.NewJSONHandler(logOutput, &slog.HandlerOptions{Level: slog.LevelDebug}))
	manager, err := redisproxy.NewManager(backends, time.Minute, logger)

	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
	})

	return NewHandler(manager, logger), manager
}
