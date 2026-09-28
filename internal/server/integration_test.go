package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nelsonlaidev/uprest/internal/redisproxy"
)

func TestPipelineWithRedis(t *testing.T) {
	client := redisIntegrationClient(t)

	key := fmt.Sprintf("uprest:integration:%d", time.Now().UnixNano())

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		defer cancel()

		if err := client.Del(ctx, key).Err(); err != nil {
			t.Error(err)
		}
	})

	handler := redisIntegrationHandler(t)
	body := fmt.Sprintf(`[["SET",%q,"1"],["GET",%q],["INCR",%q],["GET",%q],["GET",%q]]`, key, key, key, key, key+":missing")
	request := httptest.NewRequest(http.MethodPost, "/pipeline", strings.NewReader(body))

	request.Header.Set("Authorization", "Bearer test-token")
	request.Header.Set("Upstash-Encoding", "base64")

	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("got %d %q, want 200", response.Code, response.Body.String())
	}

	var results []map[string]any

	if err := json.Unmarshal(response.Body.Bytes(), &results); err != nil {
		t.Fatal(err)
	}

	if len(results) != 5 {
		t.Fatalf("got %d pipeline results, want 5: %#v", len(results), results)
	}

	missing, present := results[4]["result"]

	if results[0]["result"] != "OK" || results[1]["result"] != "MQ==" || results[2]["result"] != float64(2) || results[3]["result"] != "Mg==" || !present || missing != nil {
		t.Fatalf("unexpected pipeline results: %#v", results)
	}

	body = fmt.Sprintf(`[["SET",%q,"text"],["INCR",%q],["GET",%q]]`, key, key, key)
	request = httptest.NewRequest(http.MethodPost, "/pipeline", strings.NewReader(body))

	request.Header.Set("Authorization", "Bearer test-token")

	response = httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("got %d %q, want 200", response.Code, response.Body.String())
	}

	if err := json.Unmarshal(response.Body.Bytes(), &results); err != nil {
		t.Fatal(err)
	}

	if len(results) != 3 || results[0]["result"] != "OK" || !strings.Contains(fmt.Sprint(results[1]["error"]), "integer") || results[2]["result"] != "text" {
		t.Fatalf("unexpected pipeline error results: %#v", results)
	}

	request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(fmt.Sprintf(`["INCR",%q]`, key)))

	request.Header.Set("Authorization", "Bearer test-token")

	response = httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "integer") {
		t.Fatalf("got %d %q, want a Redis command error", response.Code, response.Body.String())
	}
}

func TestRESP2WithRedis(t *testing.T) {
	client := redisIntegrationClient(t)
	key := fmt.Sprintf("uprest:resp2:%d", time.Now().UnixNano())
	counterKey := key + ":counter"
	listKey := key + ":list"
	binaryKey := key + ":binary"
	binaryValue := []byte{0x00, 0xff, 'a', '/', '\n'}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		defer cancel()

		if err := client.Del(ctx, key, counterKey, listKey, binaryKey).Err(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	if err := client.Set(ctx, binaryKey, binaryValue, 0).Err(); err != nil {
		t.Fatal(err)
	}

	handler := redisIntegrationHandler(t)
	serve := func(path string, command any, headers map[string]string) *httptest.ResponseRecorder {
		t.Helper()

		body, err := json.Marshal(command)

		if err != nil {
			t.Fatal(err)
		}

		request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
		request.Header.Set("Authorization", "Bearer test-token")

		for name, value := range headers {
			request.Header.Set(name, value)
		}

		response := httptest.NewRecorder()

		handler.ServeHTTP(response, request)

		return response
	}
	resp2Headers := map[string]string{responseFormatHeader: "resp2"}

	response := serve("/", []any{"PING"}, resp2Headers)

	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/octet-stream" || response.Body.String() != "+PONG\r\n" {
		t.Fatalf("unexpected RESP2 PING response: %d %q %q", response.Code, response.Header().Get("Content-Type"), response.Body.String())
	}

	response = serve("/", []any{"SET", key, "hello"}, resp2Headers)

	if response.Code != http.StatusOK || response.Body.String() != "+OK\r\n" {
		t.Fatalf("unexpected RESP2 SET response: %d %q", response.Code, response.Body.String())
	}

	response = serve("/", []any{"GET", key}, resp2Headers)

	if response.Code != http.StatusOK || response.Body.String() != "$5\r\nhello\r\n" {
		t.Fatalf("unexpected RESP2 GET response: %d %q", response.Code, response.Body.String())
	}

	response = serve("/", []any{"GET", key + ":missing"}, resp2Headers)

	if response.Code != http.StatusOK || response.Body.String() != "$-1\r\n" {
		t.Fatalf("unexpected RESP2 nil response: %d %q", response.Code, response.Body.String())
	}

	response = serve("/", []any{"INCR", counterKey}, resp2Headers)

	if response.Code != http.StatusOK || response.Body.String() != ":1\r\n" {
		t.Fatalf("unexpected RESP2 integer response: %d %q", response.Code, response.Body.String())
	}

	response = serve("/", []any{"RPUSH", listKey, "a", "b"}, resp2Headers)

	if response.Code != http.StatusOK || response.Body.String() != ":2\r\n" {
		t.Fatalf("unexpected RESP2 RPUSH response: %d %q", response.Code, response.Body.String())
	}

	response = serve("/", []any{"LRANGE", listKey, 0, -1}, resp2Headers)

	if response.Code != http.StatusOK || response.Body.String() != "*2\r\n$1\r\na\r\n$1\r\nb\r\n" {
		t.Fatalf("unexpected RESP2 array response: %d %q", response.Code, response.Body.String())
	}

	response = serve("/", []any{"INCR", key}, resp2Headers)

	if response.Code != http.StatusBadRequest || response.Header().Get("Content-Type") != "application/octet-stream" || !strings.HasPrefix(response.Body.String(), "-ERR ") {
		t.Fatalf("unexpected RESP2 error response: %d %q %q", response.Code, response.Header().Get("Content-Type"), response.Body.String())
	}

	response = serve("/pipeline", []any{
		[]any{"PING"},
		[]any{"GET", key},
		[]any{"GET", binaryKey},
		[]any{"GET", key + ":missing"},
		[]any{"EVAL", "return {1, 2, {3, 'hello'}}", 0},
		[]any{"INCR", key},
	}, resp2Headers)
	wantPipelinePrefix := []byte("+PONG\r\n$5\r\nhello\r\n")
	wantPipelinePrefix = fmt.Appendf(wantPipelinePrefix, "$%d\r\n", len(binaryValue))
	wantPipelinePrefix = append(wantPipelinePrefix, binaryValue...)
	wantPipelinePrefix = append(wantPipelinePrefix, []byte("\r\n$-1\r\n*3\r\n:1\r\n:2\r\n*2\r\n:3\r\n$5\r\nhello\r\n-ERR ")...)

	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/octet-stream" || !bytes.HasPrefix(response.Body.Bytes(), wantPipelinePrefix) {
		t.Fatalf("unexpected RESP2 pipeline response: %d %q %q", response.Code, response.Header().Get("Content-Type"), response.Body.String())
	}

	response = serve("/multi-exec", []any{
		[]any{"GET", key},
	}, resp2Headers)

	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/json" || strings.TrimSpace(response.Body.String()) != `[{"result":"hello"}]` {
		t.Fatalf("unexpected multi-exec response: %d %q %q", response.Code, response.Header().Get("Content-Type"), response.Body.String())
	}
}

func TestScriptNormalizationWithRedis(t *testing.T) {
	client := redisIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	if err := client.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	handler := redisIntegrationHandler(t)
	original := "#!lua flags=allow-key-locking\nreturn ARGV[1]"
	normalized := "return ARGV[1]"

	response := serveCommand(t, handler, "/", []any{"EVAL", original, 0, "normalized"})

	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != `{"result":"normalized"}` {
		t.Fatalf("got %d %q, want normalized EVAL result", response.Code, response.Body.String())
	}

	response = serveCommand(t, handler, "/", []any{"EVALSHA", redis.NewScript(original).Hash(), 0, "cached"})

	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != `{"result":"cached"}` {
		t.Fatalf("got %d %q, want cached EVALSHA result", response.Code, response.Body.String())
	}

	readOnlyOriginal := "#!lua flags=no-writes,allow-key-locking\r\nreturn 'loaded'\r\n"
	readOnlyNormalized := "#!lua flags=no-writes\r\nreturn 'loaded'\r\n"
	response = serveCommand(t, handler, "/", []any{"SCRIPT", "LOAD", readOnlyOriginal})

	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != fmt.Sprintf(`{"result":%q}`, redis.NewScript(readOnlyNormalized).Hash()) {
		t.Fatalf("got %d %q, want normalized SCRIPT LOAD hash", response.Code, response.Body.String())
	}

	response = serveCommand(t, handler, "/", []any{"EVALSHA_RO", redis.NewScript(readOnlyOriginal).Hash(), 0})

	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != `{"result":"loaded"}` {
		t.Fatalf("got %d %q, want aliased EVALSHA_RO result", response.Code, response.Body.String())
	}

	pipelineScript := "#!lua flags=allow-key-locking\nreturn 'pipeline'"
	pipeline := []any{
		[]any{"EVAL", pipelineScript, 0},
		[]any{"EVALSHA", redis.NewScript(pipelineScript).Hash(), 0},
	}
	response = serveCommand(t, handler, "/pipeline", pipeline)

	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != `[{"result":"pipeline"},{"result":"pipeline"}]` {
		t.Fatalf("got %d %q, want normalized pipeline results", response.Code, response.Body.String())
	}

	if redis.NewScript(original).Hash() == redis.NewScript(normalized).Hash() {
		t.Fatal("test requires normalization to change the Redis script hash")
	}
}

func TestMultiExecWithRedis(t *testing.T) {
	client := redisIntegrationClient(t)
	key := fmt.Sprintf("uprest:transaction:%d", time.Now().UnixNano())

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		defer cancel()

		if err := client.Del(ctx, key).Err(); err != nil {
			t.Error(err)
		}
	})

	handler := redisIntegrationHandler(t)
	response := serveCommand(t, handler, "/multi-exec", []any{
		[]any{"SET", key, "0"},
		[]any{"INCR", key},
		[]any{"GET", key},
	})

	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != `[{"result":"OK"},{"result":1},{"result":"1"}]` {
		t.Fatalf("got %d %q, want successful transaction results", response.Code, response.Body.String())
	}

	response = serveCommand(t, handler, "/multi-exec", []any{
		[]any{"SET", key, "text"},
		[]any{"INCR", key},
		[]any{"GET", key},
	})

	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `[{"result":"OK"},{"error":"ERR`) || !strings.Contains(response.Body.String(), `},{"result":"text"}]`) {
		t.Fatalf("got %d %q, want an in-transaction command error", response.Code, response.Body.String())
	}

	response = serveCommand(t, handler, "/multi-exec", []any{
		[]any{"SET", key, "discarded"},
		[]any{"GET"},
	})

	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "EXECABORT") {
		t.Fatalf("got %d %q, want discarded transaction error", response.Code, response.Body.String())
	}

	value, err := client.Get(context.Background(), key).Result()

	if err != nil {
		t.Fatal(err)
	}

	if value != "text" {
		t.Fatalf("got %q after discarded transaction, want text", value)
	}

	script := "#!lua flags=allow-key-locking\nreturn 'transaction'"
	response = serveCommand(t, handler, "/multi-exec", []any{
		[]any{"EVAL", script, 0},
	})

	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != `[{"result":"transaction"}]` {
		t.Fatalf("got %d %q, want normalized transaction script result", response.Code, response.Body.String())
	}
}

func TestPathCommandsWithRedis(t *testing.T) {
	client := redisIntegrationClient(t)
	key := fmt.Sprintf("uprest:path:%d/encoded", time.Now().UnixNano())
	queryKey := key + ":query"

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		defer cancel()

		if err := client.Del(ctx, key, queryKey).Err(); err != nil {
			t.Error(err)
		}
	})

	handler := redisIntegrationHandler(t)
	payload := []byte{0x00, 0xff, 'a', '/', '\n'}
	response := servePathCommand(t, handler, http.MethodPost, "/set/"+url.PathEscape(key), payload, nil)

	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != `{"result":"OK"}` {
		t.Fatalf("got %d %q, want raw binary SET result", response.Code, response.Body.String())
	}

	response = servePathCommand(t, handler, http.MethodGet, "/get/"+url.PathEscape(key), nil, map[string]string{
		responseEncodingHeader: "base64",
		responseFormatHeader:   "json",
	})

	wantPayload := base64.StdEncoding.EncodeToString(payload)

	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != fmt.Sprintf(`{"result":%q}`, wantPayload) {
		t.Fatalf("got %d %q, want base64 binary GET result", response.Code, response.Body.String())
	}

	response = servePathCommand(t, handler, http.MethodGet, "/get/"+url.PathEscape(key), nil, map[string]string{
		responseFormatHeader: "resp2",
	})
	wantRESP2Payload := fmt.Appendf(nil, "$%d\r\n", len(payload))
	wantRESP2Payload = append(wantRESP2Payload, payload...)
	wantRESP2Payload = append(wantRESP2Payload, '\r', '\n')

	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/octet-stream" || !bytes.Equal(response.Body.Bytes(), wantRESP2Payload) {
		t.Fatalf("got %d %q %q, want binary RESP2 payload %q", response.Code, response.Header().Get("Content-Type"), response.Body.Bytes(), wantRESP2Payload)
	}

	request := httptest.NewRequest(http.MethodPost, "/set/"+url.PathEscape(queryKey)+"?PX=60000&_token=test-token", bytes.NewReader([]byte("query-value")))
	response = httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != `{"result":"OK"}` {
		t.Fatalf("got %d %q, want query-authenticated SET result", response.Code, response.Body.String())
	}

	ttl, err := client.PTTL(context.Background(), queryKey).Result()

	if err != nil {
		t.Fatal(err)
	}

	if ttl <= 0 {
		t.Fatalf("got TTL %s, want a positive TTL", ttl)
	}
}

func TestMultiTenantTokenRoutingWithRedis(t *testing.T) {
	redisURL, err := url.Parse(redisIntegrationURL(t))

	if err != nil {
		t.Fatal(err)
	}

	primaryURL := *redisURL
	primaryURL.Path = "/0"
	analyticsURL := *redisURL
	analyticsURL.Path = "/1"
	handler, manager := newTestServer(t, []redisproxy.Backend{
		{
			Token:            "primary-token",
			ID:               "primary",
			ConnectionString: primaryURL.String(),
			MaxConnections:   2,
		},
		{ //nolint:gosec // G101: test fixture token
			Token:            "analytics-token",
			ID:               "analytics",
			ConnectionString: analyticsURL.String(),
			MaxConnections:   4,
		},
	}, io.Discard)
	key := fmt.Sprintf("uprest:tenant:%d", time.Now().UnixNano())

	t.Cleanup(func() {
		serveCommandWithToken(t, handler, "primary-token", "/", []any{"DEL", key})
		serveCommandWithToken(t, handler, "analytics-token", "/", []any{"DEL", key})
	})

	response := serveCommandWithToken(t, handler, "primary-token", "/", []any{"SET", key, "primary"})

	if response.Code != http.StatusOK {
		t.Fatalf("primary SET returned %d: %s", response.Code, response.Body.String())
	}

	response = serveCommandWithToken(t, handler, "analytics-token", "/", []any{"SET", key, "analytics"})

	if response.Code != http.StatusOK {
		t.Fatalf("analytics SET returned %d: %s", response.Code, response.Body.String())
	}

	response = serveCommandWithToken(t, handler, "primary-token", "/", []any{"GET", key})

	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != `{"result":"primary"}` {
		t.Fatalf("got primary response %d %q", response.Code, response.Body.String())
	}

	response = serveCommandWithToken(t, handler, "analytics-token", "/", []any{"GET", key})

	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != `{"result":"analytics"}` {
		t.Fatalf("got analytics response %d %q", response.Code, response.Body.String())
	}

	if stats := manager.Stats(); stats.Active != 2 || stats.Created != 2 {
		t.Fatalf("unexpected multi-tenant pool stats: %+v", stats)
	}
}

func redisIntegrationClient(t *testing.T) *redis.Client {
	t.Helper()

	redisURL := redisIntegrationURL(t)
	options, err := redis.ParseURL(redisURL)

	if err != nil {
		t.Fatal(err)
	}

	options.Protocol = 2
	client := redis.NewClient(options)

	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})

	return client
}

func redisIntegrationHandler(t *testing.T) http.Handler {
	t.Helper()

	handler, _ := newTestServer(t, []redisproxy.Backend{{
		Token:            "test-token",
		ID:               "integration",
		ConnectionString: redisIntegrationURL(t),
		MaxConnections:   3,
	}}, io.Discard)

	return handler
}

func redisIntegrationURL(t *testing.T) string {
	t.Helper()

	redisURL := os.Getenv("UPREST_TEST_REDIS_URL")

	if redisURL == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("UPREST_TEST_REDIS_URL is required in CI")
		}

		t.Skip("set UPREST_TEST_REDIS_URL to run the Redis integration test")
	}

	return redisURL
}

func serveCommand(t *testing.T, handler http.Handler, path string, command any) *httptest.ResponseRecorder {
	t.Helper()

	return serveCommandWithToken(t, handler, "test-token", path, command)
}

func serveCommandWithToken(t *testing.T, handler http.Handler, token string, path string, command any) *httptest.ResponseRecorder {
	t.Helper()

	body, err := json.Marshal(command)

	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))

	request.Header.Set("Authorization", "Bearer "+token)

	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	return response
}

func servePathCommand(t *testing.T, handler http.Handler, method string, path string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()

	request := httptest.NewRequest(method, path, bytes.NewReader(body))

	request.Header.Set("Authorization", "Bearer test-token")

	for name, value := range headers {
		request.Header.Set(name, value)
	}

	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	return response
}
