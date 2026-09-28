package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nelsonlaidev/uprest/internal/redisproxy"
)

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
		{"unsupported endpoint", http.MethodGet, "/subscribe/channel", "Bearer test-token", "", http.StatusNotFound, `{"error":"Not Found"}`},
		{"unsupported monitor endpoint", http.MethodPost, "/monitor", "Bearer test-token", "", http.StatusNotFound, `{"error":"Not Found"}`},
		{"unsupported endpoint without token", http.MethodGet, "/subscribe/channel", "", "", http.StatusUnauthorized, `{"error":"Unauthorized"}`},
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

	if response.Code != http.StatusNotFound {
		t.Fatalf("path request returned %d %q, want 404", response.Code, response.Body.String())
	}
}

func TestRequestLogging(t *testing.T) {
	var logs bytes.Buffer
	handler, _ := newTestServer(t, []redisproxy.Backend{{
		Token:            "test-token",
		ID:               "test",
		ConnectionString: "redis://localhost:1",
		MaxConnections:   3,
	}}, &logs)

	request := httptest.NewRequest(http.MethodGet, "/ping", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("got %d %q, want unauthorized response", response.Code, response.Body.String())
	}

	if response.Header().Get("X-Request-ID") != "" {
		t.Fatalf("unexpected X-Request-ID response header")
	}

	var entry map[string]any

	if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &entry); err != nil {
		t.Fatal(err)
	}

	if entry["msg"] != "request completed" || entry["request_id"] == "" || entry["method"] != http.MethodGet || entry["path"] != "/ping" || entry["status"] != float64(http.StatusUnauthorized) {
		t.Fatalf("unexpected request log: %#v", entry)
	}
}

func TestQueryTokenNotLogged(t *testing.T) {
	for _, test := range []struct {
		name   string
		token  string
		status int
	}{
		{"valid", "test-token", http.StatusNotFound},
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

func TestUnsupportedEndpointDoesNotCreatePool(t *testing.T) {
	handler, manager := newTestServer(t, []redisproxy.Backend{{
		Token:            "test-token",
		ID:               "test",
		ConnectionString: "redis://localhost:1",
		MaxConnections:   3,
	}}, io.Discard)

	for _, path := range []string{"/monitor", "/subscribe/channel", "/psubscribe/channel"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Authorization", "Bearer test-token")
		response := httptest.NewRecorder()

		handler.ServeHTTP(response, request)

		if response.Code != http.StatusNotFound {
			t.Fatalf("%s returned %d, want 404", path, response.Code)
		}
	}

	if stats := manager.Stats(); stats.Active != 0 || stats.Created != 0 {
		t.Fatalf("unsupported endpoints created a Redis pool: %+v", stats)
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
