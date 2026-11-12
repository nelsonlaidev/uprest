package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nelsonlaidev/uprest/internal/redisproxy"
)

func TestXAutoClaimZeroCountWithRedis(t *testing.T) {
	client := redisIntegrationClient(t)
	handler := redisIntegrationHandler(t)

	for _, test := range []struct {
		name   string
		path   string
		format string
	}{
		{name: "JSON", path: "/"},
		{name: "base64", path: "/", format: "base64"},
		{name: "RESP2", path: "/", format: "resp2"},
		{name: "path GET", path: "GET"},
		{name: "path POST", path: "POST"},
		{name: "pipeline", path: "/pipeline"},
		{name: "RESP2 pipeline", path: "/pipeline", format: "resp2"},
		{name: "transaction", path: "/multi-exec"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			key := fmt.Sprintf("uprest:xautoclaim:%d", time.Now().UnixNano())

			t.Cleanup(func() {
				if err := client.Del(ctx, key).Err(); err != nil {
					t.Error(err)
				}
			})

			for _, command := range [][]any{
				{"XADD", key, "1-0", "field", "value"},
				{"XADD", key, "2-0", "field", "deleted"},
				{"XGROUP", "CREATE", key, "group", "0"},
				{"XREADGROUP", "GROUP", "group", "owner", "STREAMS", key, ">"},
				{"XDEL", key, "2-0"},
			} {
				if err := client.Do(ctx, command...).Err(); err != nil {
					t.Fatal(err)
				}
			}

			for _, justID := range []bool{false, true} {
				command := []any{"xautoclaim", key, "group", "new-owner", 0, "0-0", "count", 0}

				if justID {
					command = append(command, "justid")
				}

				var response *httptest.ResponseRecorder

				if test.path == "GET" || test.path == "POST" {
					path := "/xautoclaim/" + key + "/group/new-owner/0/0-0/count"
					var body []byte

					if test.path == "POST" {
						body = []byte("0")
					} else {
						path += "/0"
					}

					if justID {
						path += "?justid"
					}

					response = servePathCommand(t, handler, test.path, path, body, nil)
				} else {
					var commands any = command

					if test.path != "/" {
						commands = []any{[]any{"PING"}, command, []any{"PING"}}
					}

					body, err := json.Marshal(commands)

					if err != nil {
						t.Fatal(err)
					}

					request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(string(body)))
					request.Header.Set("Authorization", "Bearer test-token")

					switch test.format {
					case "base64":
						request.Header.Set("Upstash-Encoding", "base64")
					case "resp2":
						request.Header.Set("Upstash-Response-Format", "resp2")
					}

					response = httptest.NewRecorder()
					handler.ServeHTTP(response, request)
				}

				want := `{"result":["0-0",[],[]]}`

				if test.format == "base64" {
					want = `{"result":["MC0w",[],[]]}`
				}

				if test.format == "resp2" {
					want = "*3\r\n$3\r\n0-0\r\n*0\r\n*0\r\n"

					if test.path == "/pipeline" {
						want = "+PONG\r\n" + want + "+PONG\r\n"
					}
				} else if test.path == "/pipeline" || test.path == "/multi-exec" {
					want = `[{"result":"PONG"},` + want + `,{"result":"PONG"}]`
				}

				if response.Code != http.StatusOK || strings.TrimSuffix(response.Body.String(), "\n") != strings.TrimSuffix(want, "\n") {
					t.Fatalf("JUSTID=%v: got %d %q, want 200 %q", justID, response.Code, response.Body.String(), want)
				}
			}

			pending, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: key, Group: "group", Start: "-", End: "+", Count: 10}).Result()

			if err != nil {
				t.Fatal(err)
			}

			if len(pending) != 2 {
				t.Fatalf("COUNT 0 removed pending entries: %#v", pending)
			}

			for _, entry := range pending {
				if entry.Consumer != "owner" || entry.RetryCount != 1 {
					t.Fatalf("COUNT 0 changed pending entry: %#v", entry)
				}
			}

			consumers, err := client.XInfoConsumers(ctx, key, "group").Result()

			if err != nil || len(consumers) != 1 || consumers[0].Name != "owner" {
				t.Fatalf("COUNT 0 changed consumers: %#v, %v", consumers, err)
			}

			response := serveCommand(t, handler, "/", []any{"XAUTOCLAIM", key, "group", "new-owner", 0, "0-0", "COUNT", 1, "JUSTID"})
			var body map[string]any

			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}

			if response.Code != http.StatusOK || !reflect.DeepEqual(body["result"], []any{"2-0", []any{"1-0"}, []any{}}) {
				t.Fatalf("positive COUNT no longer claims entries: %d %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestXAutoClaimZeroCountErrorsWithRedis(t *testing.T) {
	client := redisIntegrationClient(t)
	handler := redisIntegrationHandler(t)
	ctx := context.Background()
	key := fmt.Sprintf("uprest:xautoclaim-errors:%d", time.Now().UnixNano())

	t.Cleanup(func() {
		if err := client.Del(ctx, key, key+":string").Err(); err != nil {
			t.Error(err)
		}
	})

	if err := client.XGroupCreateMkStream(ctx, key, "group", "0").Err(); err != nil {
		t.Fatal(err)
	}

	if err := client.Set(ctx, key+":string", "value", 0).Err(); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name    string
		index   int
		value   string
		options []any
		want    string
	}{
		{name: "missing stream", index: 1, value: key + ":missing", want: "NOGROUP"},
		{name: "missing group", index: 2, value: "missing", want: "NOGROUP"},
		{name: "wrong type", index: 1, value: key + ":string", want: "WRONGTYPE"},
		{name: "invalid idle", index: 4, value: "invalid", want: "Invalid min-idle-time"},
		{name: "overflow idle", index: 4, value: "9223372036854775808", want: "Invalid min-idle-time"},
		{name: "invalid ID", index: 5, value: "invalid", want: "Invalid stream ID"},
		{name: "negative count", index: 7, value: "-1", want: "COUNT must be > 0"},
		{name: "non-integer count", index: 7, value: "0.5", want: "COUNT must be > 0"},
		{name: "unknown option", options: []any{"UNKNOWN"}, want: "COUNT must be > 0"},
		{name: "duplicate count", options: []any{"COUNT", "1"}, want: "COUNT must be > 0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			command := []any{"XAUTOCLAIM", key, "group", "consumer", "0", "0-0", "COUNT", "0"}

			if test.index != 0 {
				command[test.index] = test.value
			}

			command = append(command, test.options...)
			response := serveCommand(t, handler, "/", command)
			var body map[string]string

			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}

			if response.Code != http.StatusBadRequest || !strings.Contains(body["error"], test.want) {
				t.Fatalf("got %d %s, want error containing %q", response.Code, response.Body.String(), test.want)
			}
		})
	}

	for _, denied := range []string{"xautoclaim", "xpending", "eval"} {
		t.Run("ACL denies "+denied, func(t *testing.T) {
			username := fmt.Sprintf("uprest-xautoclaim-%d", time.Now().UnixNano())

			t.Cleanup(func() {
				if err := client.ACLDelUser(ctx, username).Err(); err != nil {
					t.Error(err)
				}
			})

			if err := client.ACLSetUser(ctx, username, "on", ">test-password", "~"+key, "+eval", "+xautoclaim", "+xpending", "-"+denied).Err(); err != nil {
				t.Fatal(err)
			}

			backendURL, err := url.Parse(redisIntegrationURL(t))

			if err != nil {
				t.Fatal(err)
			}

			backendURL.User = url.UserPassword(username, "test-password")
			restricted, _ := newTestServer(t, []redisproxy.Backend{{
				Token: "test-token", ID: "acl", ConnectionString: backendURL.String(), MaxConnections: 1,
			}}, io.Discard, "", nil)
			response := serveCommand(t, restricted, "/", []any{"XAUTOCLAIM", key, "group", "consumer", 0, "0-0", "COUNT", 0})
			body := response.Body.String()
			permissionError := strings.Contains(body, "NOPERM") || strings.Contains(body, "ACL failure in script")

			if response.Code != http.StatusBadRequest || !permissionError || !strings.Contains(body, "'"+denied+"'") {
				t.Fatalf("COUNT 0 bypassed ACL: %d %s", response.Code, response.Body.String())
			}
		})
	}
}
