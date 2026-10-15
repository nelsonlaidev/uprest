# Uprest

Uprest is a local HTTP-to-Redis proxy for applications that use the Upstash Redis REST API. It forwards requests to a real Redis server, so `@upstash/redis` keeps working during local development and CI.

It supports Redis commands, pipelines, transactions, Pub/Sub and `MONITOR` over Server-Sent Events (SSE), JSON and RESP2 responses, bearer or query-token authentication, and recent `@upstash/ratelimit` versions.

## Quick start

Docker and Docker Compose are required. Create a `docker-compose.yml` file:

```yaml
services:
  redis:
    image: redis:8.10.2
  uprest:
    image: ghcr.io/nelsonlaidev/uprest:latest
    ports:
      - '8079:8080'
    environment:
      UPREST_TOKEN: example-token
      UPREST_CONNECTION_STRING: redis://redis:6379
```

Start both services with `docker compose up -d`, then point `@upstash/redis` at Uprest:

```ts
import { Redis } from '@upstash/redis'

const redis = new Redis({
  url: 'http://localhost:8079',
  token: 'example-token',
})

await redis.set('hello', 'world')
await redis.get('hello') // "world"
```

`Redis.fromEnv()` picks up the same values from `UPSTASH_REDIS_REST_URL` and `UPSTASH_REDIS_REST_TOKEN`. To use an existing Redis server instead, run only the proxy:

```sh
docker run --rm -d -p 8079:8080 --name uprest \
  -e UPREST_TOKEN=example-token \
  -e UPREST_CONNECTION_STRING=redis://your-redis-host:6379 \
  ghcr.io/nelsonlaidev/uprest:latest
```

The image listens on port `8080`. Use a private token and an appropriate Redis URL outside local development, and pin a version tag such as `ghcr.io/nelsonlaidev/uprest:v0.3.0` for reproducible environments.

## Configuration

Uprest reads `UPREST_*` environment variables. `UPREST_MODE` selects a single backend from environment variables (`env`) or multiple backends from a JSON file (`file`).

| Variable                   | Default                          | Purpose                                               |
| -------------------------- | -------------------------------- | ----------------------------------------------------- |
| `UPREST_MODE`              | `env`                            | Configuration source: `env` or `file`                 |
| `UPREST_TOKEN`             | Required in `env` mode           | HTTP authentication token                             |
| `UPREST_CONNECTION_STRING` | Required in `env` mode           | Redis URL, such as `redis://redis:6379`               |
| `UPREST_MAX_CONNECTIONS`   | `3`                              | Maximum active connections for the `env` mode backend |
| `UPREST_TOKENS_FILE`       | `/app/uprest-config/tokens.json` | Tokens file path in `file` mode                       |
| `UPREST_PORT`              | `80` (`8080` in the image)       | HTTP listen port                                      |
| `UPREST_IPV6`              | `false`                          | Listen on IPv6                                        |
| `UPREST_IDLE_TIMEOUT`      | `15m`                            | Time before an idle Redis pool closes                 |
| `UPREST_ACQUIRE_TIMEOUT`   | `30s`                            | Maximum wait for backend capacity                     |
| `UPREST_COMMAND_TIMEOUT`   | `30s`                            | Maximum execution time for a command or batch          |
| `UPREST_LOG_LEVEL`         | `info`                           | JSON log level: `debug`, `info`, `warn`, or `error`   |
| `UPREST_METRICS_TOKEN`     | Empty (disabled)                 | Dedicated Bearer token for `GET /metrics`            |

### Multiple tokens or backends

Set `UPREST_MODE=file` and mount a JSON file at `/app/uprest-config/tokens.json`, or change the location with `UPREST_TOKENS_FILE`:

```json
{
  "example-token": {
    "id": "primary",
    "connection_string": "redis://redis:6379",
    "max_connections": 3
  }
}
```

Each token selects its backend; `max_connections` is optional and defaults to `3`. `UPREST_TOKEN` and `UPREST_CONNECTION_STRING` are ignored in file mode.

### Timeouts

`UPREST_ACQUIRE_TIMEOUT` and `UPREST_COMMAND_TIMEOUT` apply process-wide in both modes. Use positive Go durations such as `250ms`, `5s`, or `2m`. Unset or empty values default to `30s`; invalid, zero, negative, or overflowing durations prevent startup. Configurations that overflow the combined HTTP response timeout also prevent startup.

Capacity acquisition and execution have independent budgets. Acquisition starts before waiting for a backend slot and applies to ordinary commands, Pub/Sub, and MONITOR. Command execution starts after command/body decoding, and each pipeline or transaction shares one execution budget across the entire batch. Acquisition timeout returns JSON `503`; execution timeout returns JSON `502`, including for RESP2 requests.

The command timeout is an upper bound. Earlier caller deadlines and shorter Redis connection or socket timeouts still apply, including go-redis's default five-second socket read/write limits and settings in the Redis URL. Increasing `UPREST_COMMAND_TIMEOUT` does not override those limits. Client cancellation does not immediately interrupt an in-flight socket read.

`/ready` retains its shared two-second acquisition-and-PING budget. Pub/Sub and MONITOR retain their existing 30-second initialization limits; the command timeout does not limit established SSE streams.

The HTTP response write timeout is automatically calculated as `max(50s, 15s + acquire timeout + max(command timeout, 30s) + 5s)`, giving a default of `80s`. The extra time accommodates the existing request-read limit, stream initialization, and writing the final response. The effective acquisition, command, and HTTP write timeouts appear in the startup log.

## API

### Routes

| Endpoint                   | Method | Request                                            |
| -------------------------- | ------ | -------------------------------------------------- |
| `/health`                  | `GET`  | HTTP liveness check; no authentication required    |
| `/ready`                   | `GET`  | Authenticated Redis backend readiness check        |
| `/metrics`                 | `GET`  | Prometheus metrics; enabled by `UPREST_METRICS_TOKEN` |
| `/`                        | `POST` | JSON command array                                 |
| `/pipeline`                | `POST` | JSON array of commands                             |
| `/multi-exec`              | `POST` | JSON array of commands run in Redis `MULTI`/`EXEC` |
| `/monitor`                 | `POST` | Redis `MONITOR` output as an SSE stream            |
| `/subscribe/<channel...>`  | `POST` | One or more URL-encoded Pub/Sub channels           |
| `/psubscribe/<pattern...>` | `POST` | One or more URL-encoded Pub/Sub patterns           |
| `/<command>/<args...>`     | `GET`  | URL-encoded arguments                              |
| `/<command>/<args...>`     | `POST` | URL arguments and the body as the final Redis value |

Authenticate with `Authorization: Bearer <token>` or the `_token` query parameter. An `Authorization` header takes precedence and does not fall back to `_token`; only `/health` is public. When enabled, `/metrics` accepts only its dedicated Bearer token and does not accept backend tokens or `_token`.

An empty path-command `POST` body becomes an empty Redis argument; use `GET` when no body argument is needed. Request bodies are limited to 10 MiB, and `HEAD`/`PUT` command requests are unsupported.

### Responses and errors

JSON is the default response format. Add `Upstash-Encoding: base64` to base64-encode string results, or set `Upstash-Response-Format: resp2` for raw RESP2 bytes on commands and pipelines. Transactions at `/multi-exec` always return JSON, and RESP2 cannot be combined with base64 encoding.

Errors use `{"error":"..."}`. Validation and Redis command errors return `400`, authentication failures `401`, unsupported methods `405`, unavailable Redis backends `502`, and connection acquisition failures `503`.

`/health` and `/ready` are reserved paths and return JSON status objects; send Redis commands with those names through `POST /`.

### Shared connection isolation

Ordinary body and path commands, pipelines, and transactions reject commands that can change a shared Redis connection's state:

- Connection state: `AUTH`, `HELLO`, `SELECT`, `QUIT`, `RESET`, `READONLY`, `READWRITE`, `ASKING`.
- Transaction state: `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`.
- Streaming state: `SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`, `UNSUBSCRIBE`, `PUNSUBSCRIBE`, `SUNSUBSCRIBE`, `MONITOR`.
- Replication and session protocols: `SYNC`, `PSYNC`, `REPLCONF`, all `HIMPORT` subcommands.
- Script debugging: `SCRIPT DEBUG` in any mode.

`CLIENT` permits only `GETNAME`, `GETREDIR`, `ID`, `INFO`, `LIST`, `TRACKINGINFO`, and `HELP`; missing or other subcommands are rejected. Matching is case-insensitive and does not inspect keys, values, or script contents. Redis still validates arguments for permitted commands.

Configure Redis credentials and the database number in each backend's connection URL. Use `/multi-exec` for transactions and the dedicated `/subscribe`, `/psubscribe`, and `/monitor` routes for SSE streams. Previously forwarded stateful commands now return HTTP `400` with `{"error":"Command is not supported over HTTP"}`, including when RESP2 is requested.

The complete request is decoded and validated before waiting for backend capacity or contacting Redis. If any command in a batch is prohibited, the entire batch is rejected without executing any commands or updating script aliases. Malformed requests retain their existing validation errors.

All other commands retain existing forwarding behavior, including blocking commands, Lua scripting, `SCRIPT LOAD`/`EXISTS`, functions, and data cleanup commands such as `FLUSHDB`. This policy is always enabled and assumes standard Redis command names; it does not sandbox scripts or module-defined commands. Redis ACLs remain responsible for authorization.

### Metrics

Set `UPREST_METRICS_TOKEN` to enable authenticated Prometheus metrics on the same HTTP port. The monitoring token must differ from every backend token, and backend IDs must be unique when metrics are enabled. Invalid configuration prevents startup without printing tokens. An unset or empty value disables metrics, including collection, and preserves `/metrics` as an ordinary Redis path command. The bundled Compose example keeps metrics disabled.

```sh
curl --fail -H "Authorization: Bearer $UPREST_METRICS_TOKEN" http://localhost:8079/metrics
```

Only the exact `/metrics` path accepts monitoring credentials. GET returns Prometheus exposition with `Cache-Control: no-store`; invalid credentials return JSON 401, and other methods return JSON 405 with `Allow: GET` after authentication. Upstash response headers are ignored. Scraping does not contact Redis, acquire capacity, or keep pools active. Scrapes are excluded from HTTP business metrics; successful scrapes log at debug level and failures at info level.

Configure Prometheus to read the token from a file containing only the token value:

```yaml
scrape_configs:
  - job_name: uprest
    metrics_path: /metrics
    static_configs:
      - targets: ['uprest:8080']
    authorization:
      type: Bearer
      credentials_file: /etc/prometheus/secrets/uprest_metrics_token
```

| Metric | Type | Labels |
| --- | --- | --- |
| `uprest_http_requests_total` | Counter | `route`, `method`, `status` |
| `uprest_http_request_duration_seconds` | Histogram | `route`, `method` |
| `uprest_redis_capacity_limit` | Gauge | `backend_id` |
| `uprest_redis_capacity_in_use` | Gauge | `backend_id` |
| `uprest_redis_acquire_duration_seconds` | Histogram | `backend_id`, `outcome` |
| `uprest_streams_active` | Gauge | `backend_id`, `kind` |
| `uprest_redis_pools_active` | Gauge | None |
| `uprest_redis_pools_created_total` | Counter | None |
| `uprest_redis_pools_evicted_total` | Counter | None |

HTTP metrics count requests when handlers finish, including authentication and validation failures. Routes are fixed categories: `command`, `pipeline`, `transaction`, `pubsub`, `monitor`, `health`, `ready`, and `unsupported`. Standard HTTP methods retain their names; other methods use `OTHER`. Requests canceled before writing a response use `status="canceled"`; requests that already wrote a status retain it. HTTP duration includes the entire SSE stream lifetime, so filter by route when comparing command latency. Stream gauges count Pub/Sub and MONITOR only after Redis confirmation and a successful first event, and return to zero on exit.

Capacity gauges measure occupied slots, including commands and streams, rather than physical Redis sockets. Acquisition duration measures capacity acquisition only, excluding Redis handshakes and command execution; readiness acquisition is included. Outcomes are `acquired`, `timeout`, `canceled`, and `closed`. Histograms use second buckets of `0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30`, plus `+Inf`. Counters reset when the process restarts. Go runtime metrics and process metrics are also exported; process metric availability depends on the operating system. Labels never include tokens, Redis URLs, keys, channels, raw paths, or error messages.

Example PromQL for HTTP 5xx error rate, capacity timeouts, and capacity utilization:

```promql
sum(rate(uprest_http_requests_total{status=~"5.."}[5m]))
  / sum(rate(uprest_http_requests_total[5m]))

sum by (backend_id) (rate(uprest_redis_acquire_duration_seconds_count{outcome="timeout"}[5m]))

uprest_redis_capacity_in_use / uprest_redis_capacity_limit
```

### Pub/Sub

`@upstash/redis` subscriptions work through authenticated SSE streams:

```ts
const subscriber = redis.subscribe<{ text: string }>('updates')

subscriber.on('message', ({ channel, message }) => {
  console.log(channel, message.text)
})

await redis.publish('updates', { text: 'hello' })
```

Each stream holds one Redis connection and counts toward the backend's connection limit. Idle streams receive a heartbeat every 15 seconds, and closing or aborting the request unsubscribes it.

### Monitor

Open an authenticated `POST /monitor` stream with the same bearer token. The first SSE event is `data: "OK"`; subsequent events contain raw `MONITOR` lines, and closing or aborting the request stops monitoring and releases its connection.

### Compatibility notes

- `@upstash/ratelimit` 2.1.0 and later works; Uprest strips the Upstash-only `allow-key-locking` Lua flag before forwarding scripts.
- `scan` with `{ withType: true }` is supported for commands, pipelines, and transactions. The Redis user needs permission for `EVAL`, `SCAN`, and `TYPE`.
- Upstash Search and Vector commands and some RedisJSON response details are unsupported; see [`tests/compatibility/exclusions.txt`](tests/compatibility/exclusions.txt).

## Deployment

### GitHub Actions

Uprest can run as a service container alongside Redis, so CI does not need an Upstash database:

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    services:
      redis:
        image: redis:8.10.2
      uprest:
        image: ghcr.io/nelsonlaidev/uprest:v0.3.0
        env:
          UPREST_TOKEN: example-token
          UPREST_CONNECTION_STRING: redis://redis:6379
    steps:
      - uses: actions/checkout@v4
      - run: npm test
        env:
          UPSTASH_REDIS_REST_URL: http://uprest:8080
          UPSTASH_REDIS_REST_TOKEN: example-token
```

Service containers require a Linux runner; services reach each other by name, so no port needs to be published.

### Images

Multi-architecture Linux images are published to GHCR (`ghcr.io/nelsonlaidev/uprest`) and Docker Hub (`nelsonlaidev/uprest`), alongside Linux, macOS, and Windows archives on the [GitHub Releases page](https://github.com/nelsonlaidev/uprest/releases). Stable releases also use the `latest` tag; see [`CHANGELOG.md`](CHANGELOG.md) for release history.

## Migrate from SRH

When migrating from [`hiett/serverless-redis-http`](https://github.com/hiett/serverless-redis-http) (SRH), rename these variables:

| SRH                     | Uprest                     |
| ----------------------- | -------------------------- |
| `SRH_MODE`              | `UPREST_MODE`              |
| `SRH_TOKEN`             | `UPREST_TOKEN`             |
| `SRH_CONNECTION_STRING` | `UPREST_CONNECTION_STRING` |
| `SRH_MAX_CONNECTIONS`   | `UPREST_MAX_CONNECTIONS`   |
| `SRH_PORT`              | `UPREST_PORT`              |
| `SRH_IPV6`              | `UPREST_IPV6`              |

`UPREST_MODE=env` is the default, and existing token and Redis URL values can stay the same.

For file mode, set `UPREST_MODE=file`, rename each entry's `srh_id` field to `id`, and mount the file at `/app/uprest-config/tokens.json` or set `UPREST_TOKENS_FILE`. A file containing only `srh_id` fails validation.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, tests, and release steps.

## License

Uprest is licensed under the [MIT License](LICENSE).
