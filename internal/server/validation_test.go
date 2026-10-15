package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/nelsonlaidev/uprest/internal/redisproxy"
)

func TestCommandValidationBeforeAcquisition(t *testing.T) {
	for _, occupied := range []bool{false, true} {
		for _, format := range []string{"json", "resp2"} {
			for _, test := range []struct {
				name   string
				method string
				path   string
				body   string
				route  string
			}{
				{"body", http.MethodPost, "/", `["sElEcT",1]`, "command"},
				{"credentials", http.MethodPost, "/", `["AUTH","private-user","private-password"]`, "command"},
				{"escaped GET path", http.MethodGet, "/%53eLeCt/1", "", "command"},
				{"POST path", http.MethodPost, "/cLiEnT/rEpLy", "OFF", "command"},
				{"CLIENT body subcommand", http.MethodPost, "/cLiEnT?OFF", "rEpLy", "command"},
				{"script debug", http.MethodGet, "/SCRIPT/DEBUG/NO", "", "command"},
				{"SCRIPT DEBUG body subcommand", http.MethodPost, "/sCrIpT?YES", "dEbUg", "command"},
				{"pipeline", http.MethodPost, "/pipeline", `[["SET","private-key","value"],["SCRIPT","LOAD","#!lua flags=allow-key-locking\nreturn 1"],["SELECT",1]]`, "pipeline"},
				{"transaction", http.MethodPost, "/multi-exec", `[["SET","private-key","value"],["SCRIPT","LOAD","#!lua flags=allow-key-locking\nreturn 1"],["SELECT",1]]`, "transaction"},
			} {
				name := test.name + "/" + format + "/offline"

				if occupied {
					name = test.name + "/" + format + "/occupied"
				}

				t.Run(name, func(t *testing.T) {
					registry := prometheus.NewRegistry()
					handler, manager := newTestServer(t, []redisproxy.Backend{{
						Token: "backend-secret", ID: "offline", ConnectionString: "redis://localhost:1", MaxConnections: 1,
					}}, io.Discard, "metrics-secret", registry)
					handler.(*apiHandler).acquireTimeout = 20 * time.Millisecond
					var acquired float64

					if occupied {
						_, release, err := manager.Acquire(context.Background(), "backend-secret")

						if err != nil {
							t.Fatal(err)
						}

						defer release()

						acquired = 1
					}

					before := manager.Stats()
					request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
					request.Header.Set("Authorization", "Bearer backend-secret")
					request.Header.Set(responseFormatHeader, format)
					response := httptest.NewRecorder()
					handler.ServeHTTP(response, request)

					if response.Code != http.StatusBadRequest || response.Header().Get("Content-Type") != "application/json" || strings.TrimSpace(response.Body.String()) != `{"error":"Command is not supported over HTTP"}` {
						t.Fatalf("unexpected policy error: %d %q", response.Code, response.Body.String())
					}

					if after := manager.Stats(); after != before {
						t.Fatalf("rejected command changed pools: before=%+v, after=%+v", before, after)
					}

					for _, outcome := range []string{"acquired", "timeout", "canceled", "closed"} {
						want := uint64(0)

						if outcome == "acquired" {
							want = uint64(acquired)
						}

						sample := metricSample(t, registry, "uprest_redis_acquire_duration_seconds", map[string]string{"backend_id": "offline", "outcome": outcome})

						if sample.Histogram.GetSampleCount() != want {
							t.Fatalf("rejected command acquired capacity: %s count=%d", outcome, sample.Histogram.GetSampleCount())
						}
					}

					if metricSample(t, registry, "uprest_redis_capacity_in_use", map[string]string{"backend_id": "offline"}).Gauge.GetValue() != acquired {
						t.Fatal("rejected command changed capacity usage")
					}

					if metricSample(t, registry, "uprest_http_requests_total", map[string]string{"route": test.route, "method": test.method, "status": "400"}).Counter.GetValue() != 1 {
						t.Fatal("policy rejection was not counted exactly once")
					}

					hash := redis.NewScript("#!lua flags=allow-key-locking\nreturn 1").Hash()
					probe := []any{"EVALSHA", hash, "0"}

					if handler.(*apiHandler).normalizer.NormalizeCommand(probe) || probe[1] != hash {
						t.Fatal("rejected batch changed script aliases")
					}
				})
			}
		}
	}
}

func TestRejectedBatchesWithRedis(t *testing.T) {
	client := redisIntegrationClient(t)
	handler, manager := newTestServer(t, []redisproxy.Backend{{
		Token: "test-token", ID: "integration", ConnectionString: redisIntegrationURL(t), MaxConnections: 1,
	}}, io.Discard, "", nil)

	for _, path := range []string{"/pipeline", "/multi-exec"} {
		for _, format := range []string{"json", "resp2"} {
			for _, position := range []int{0, 2, 3} {
				t.Run(fmt.Sprintf("%s/%s/position-%d", path, format, position), func(t *testing.T) {
					key := fmt.Sprintf("uprest:rejected:%d", time.Now().UnixNano())
					script := "#!lua flags=allow-key-locking\nreturn '" + key + "'"
					commands := [][]any{{"SET", key, "changed"}, {"SCRIPT", "LOAD", script}, {"EVAL", script, 0}}
					commands = append(commands, nil)
					copy(commands[position+1:], commands[position:])
					commands[position] = []any{"SELECT", "1"}
					body, err := json.Marshal(commands)

					if err != nil {
						t.Fatal(err)
					}

					t.Cleanup(func() { _ = client.Del(context.Background(), key).Err() })

					response := servePathCommand(t, handler, http.MethodPost, path, body, map[string]string{responseFormatHeader: format})

					if response.Code != http.StatusBadRequest || strings.TrimSpace(response.Body.String()) != `{"error":"Command is not supported over HTTP"}` {
						t.Fatalf("unexpected batch rejection: %d %q", response.Code, response.Body.String())
					}

					if count, err := client.Exists(context.Background(), key).Result(); err != nil || count != 0 {
						t.Fatalf("rejected batch performed a write: count=%d, error=%v", count, err)
					}

					hash := redis.NewScript(script).Hash()
					probe := []any{"EVALSHA", hash, "0"}

					if handler.(*apiHandler).normalizer.NormalizeCommand(probe) || probe[1] != hash {
						t.Fatal("rejected batch changed script aliases")
					}

					normalized := redis.NewScript("return '" + key + "'").Hash()
					loaded, err := client.ScriptExists(context.Background(), hash, normalized).Result()

					if err != nil || len(loaded) != 2 || loaded[0] || loaded[1] {
						t.Fatalf("rejected batch loaded a script: %v, error=%v", loaded, err)
					}
				})
			}
		}
	}

	if stats := manager.Stats(); stats.Active != 0 || stats.Created != 0 {
		t.Fatalf("rejected batches created a pool: %+v", stats)
	}
}

func TestSharedConnectionIsolationWithRedis(t *testing.T) {
	backendURL, err := url.Parse(redisIntegrationURL(t))

	if err != nil {
		t.Fatal(err)
	}

	backendURL.Path = "/1"
	handler, manager := newTestServer(t, []redisproxy.Backend{{
		Token: "test-token", ID: "integration", ConnectionString: backendURL.String(), MaxConnections: 1,
	}}, io.Discard, "", nil)
	key := fmt.Sprintf("uprest:isolation:%d", time.Now().UnixNano())

	t.Cleanup(func() { serveCommand(t, handler, "/", []any{"DEL", key}) })

	if response := serveCommand(t, handler, "/", []any{"SET", key, "SELECT"}); response.Code != http.StatusOK {
		t.Fatalf("initial SET failed: %d %q", response.Code, response.Body.String())
	}

	checks := [][]any{{"CLIENT", "ID"}, {"CLIENT", "GETNAME"}, {"ACL", "WHOAMI"}, {"GET", key}, {"PING"}, {"CLIENT", "TRACKINGINFO"}}
	baseline := make([]string, len(checks))

	for i, command := range checks {
		response := serveCommand(t, handler, "/", command)

		if response.Code != http.StatusOK {
			t.Fatalf("baseline check %v failed: %d %q", command, response.Code, response.Body.String())
		}

		baseline[i] = response.Body.String()
	}

	response := servePathCommand(t, handler, http.MethodPost, "/CLIENT", []byte("gEtNaMe"), nil)

	if response.Code != http.StatusOK || response.Body.String() != baseline[1] {
		t.Fatalf("CLIENT body subcommand changed: %d %q", response.Code, response.Body.String())
	}

	for _, command := range [][]any{
		{"SELECT", "0"}, {"AUTH", "other-user", "private-password"}, {"HELLO", "3"},
		{"MULTI"}, {"EXEC"}, {"WATCH", key}, {"UNWATCH"}, {"DISCARD"},
		{"CLIENT", "REPLY", "OFF"}, {"CLIENT", "TRACKING", "ON"}, {"CLIENT", "SETNAME", "changed"},
		{"SUBSCRIBE", key}, {"PSUBSCRIBE", key}, {"SSUBSCRIBE", key}, {"MONITOR"},
		{"SCRIPT", "DEBUG", "YES"}, {"HIMPORT", "PREPARE", "fields", "name"},
		{"SYNC"}, {"PSYNC", "?", "-1"}, {"REPLCONF", "listening-port", "0"},
		{"QUIT"}, {"RESET"}, {"READONLY"}, {"READWRITE"}, {"ASKING"},
	} {
		t.Run(fmt.Sprint(command[:1]), func(t *testing.T) {
			response := serveCommand(t, handler, "/", command)

			if response.Code != http.StatusBadRequest || strings.TrimSpace(response.Body.String()) != `{"error":"Command is not supported over HTTP"}` {
				t.Fatalf("unexpected command rejection: %d %q", response.Code, response.Body.String())
			}

			for i, check := range checks {
				response := serveCommand(t, handler, "/", check)

				if response.Code != http.StatusOK || response.Body.String() != baseline[i] {
					t.Fatalf("shared connection changed after %v: check=%v, status=%d, body=%q", command[:1], check, response.Code, response.Body.String())
				}
			}

			response = serveCommand(t, handler, "/", []any{"CLIENT", "INFO"})

			if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), " db=1 ") {
				t.Fatalf("connection changed database: %d %q", response.Code, response.Body.String())
			}
		})
	}

	if stats := manager.Stats(); stats.Active != 1 || stats.Created != 1 {
		t.Fatalf("connection isolation replaced the shared pool: %+v", stats)
	}
}

func TestConcurrentTransactionsWithRedis(t *testing.T) {
	handler, manager := newTestServer(t, []redisproxy.Backend{{
		Token: "test-token", ID: "integration", ConnectionString: redisIntegrationURL(t), MaxConnections: 4,
	}}, io.Discard, "", nil)
	key := fmt.Sprintf("uprest:concurrent-transactions:%d", time.Now().UnixNano())

	t.Cleanup(func() { serveCommand(t, handler, "/", []any{"DEL", key}) })

	var pending sync.WaitGroup
	start := make(chan struct{})

	for i := range 32 {
		pending.Add(1)

		go func() {
			defer pending.Done()

			<-start
			path := "/"
			body := `["PING"]`
			want := `{"result":"PONG"}`

			if i%2 == 0 {
				path = "/multi-exec"
				value := fmt.Sprintf("value-%d", i)
				body = fmt.Sprintf(`[["SET",%q,%q],["GET",%q]]`, key, value, key)
				want = fmt.Sprintf(`[{"result":"OK"},{"result":%q}]`, value)
			}

			request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
			request.Header.Set("Authorization", "Bearer test-token")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != want {
				t.Errorf("concurrent %s failed: %d %q, want %q", path, response.Code, response.Body.String(), want)
			}
		}()
	}

	close(start)
	pending.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)

	defer cancel()

	var releases []func()

	defer func() {
		for _, release := range releases {
			release()
		}
	}()

	for range 4 {
		_, release, err := manager.Acquire(ctx, "test-token")

		if err != nil {
			t.Fatalf("concurrent transactions leaked capacity: %v", err)
		}

		releases = append(releases, release)
	}
}
