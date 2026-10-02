# Uprest

Uprest is a local HTTP-to-Redis proxy for applications that use the Upstash Redis REST API. It forwards requests to a real Redis server, so applications can keep using `@upstash/redis` during local development and CI.

It supports Redis commands, pipelines, transactions, Pub/Sub over Server-Sent Events (SSE), JSON and RESP2 responses, bearer or query-token authentication, and recent `@upstash/ratelimit` versions.

- [Quick start](#quick-start)
- [Configuration](#configuration)
- [API compatibility](#api-compatibility)
- [Deployment](#deployment)
- [Migrate from SRH](#migrate-from-srh)
- [Development and verification](#development-and-verification)

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

Start both services:

```sh
docker compose up -d
```

Then point `@upstash/redis` at Uprest:

```ts
import { Redis } from '@upstash/redis'

const redis = new Redis({
  url: 'http://localhost:8079',
  token: 'example-token',
})

await redis.set('hello', 'world')
await redis.get('hello') // "world"
```

`Redis.fromEnv()` works without application changes when these variables are set:

```sh
export UPSTASH_REDIS_REST_URL=http://localhost:8079
export UPSTASH_REDIS_REST_TOKEN=example-token
```

Verify the service directly with curl:

```sh
curl -X POST http://localhost:8079/ \
  -H 'Authorization: Bearer example-token' \
  -H 'Content-Type: application/json' \
  -d '["SET","hello","world"]'
```

The response is `{"result":"OK"}`.

To connect Uprest to an existing Redis server instead, run only the proxy:

```sh
docker run --rm -d -p 8079:8080 --name uprest \
  -e UPREST_TOKEN=example-token \
  -e UPREST_CONNECTION_STRING=redis://your-redis-host:6379 \
  ghcr.io/nelsonlaidev/uprest:latest
```

The image listens on port `8080`. These examples use the `latest` tag, which tracks the newest stable release; pin a version tag such as `ghcr.io/nelsonlaidev/uprest:v0.3.0` for reproducible environments. On macOS and Windows, Docker Desktop can reach Redis on the host at `redis://host.docker.internal:6379`. Use a private token and an appropriate Redis URL outside local development.

## Configuration

Uprest reads `UPREST_*` environment variables. `UPREST_MODE` selects one Redis backend from environment variables or multiple backends from a JSON file.

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
| `UPREST_LOG_LEVEL`         | `info`                           | JSON log level: `debug`, `info`, `warn`, or `error`   |

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

Each token selects its configured Redis backend. `max_connections` is optional and defaults to `3`. Redis pools open on first use and close after the idle timeout. `UPREST_TOKEN` and `UPREST_CONNECTION_STRING` are ignored in file mode.

## API compatibility

### Routes

| Endpoint                   | Method | Request                                             |
| -------------------------- | ------ | --------------------------------------------------- |
| `/health`                  | `GET`  | HTTP liveness check; no authentication required     |
| `/ready`                   | `GET`  | Authenticated Redis backend readiness check         |
| `/`                        | `POST` | JSON command array                                  |
| `/pipeline`                | `POST` | JSON array of commands                              |
| `/multi-exec`              | `POST` | JSON array of commands run in Redis `MULTI`/`EXEC`  |
| `/monitor`                 | `POST` | Redis `MONITOR` output as an SSE stream             |
| `/subscribe/<channel...>`  | `POST` | One or more URL-encoded Pub/Sub channels            |
| `/psubscribe/<pattern...>` | `POST` | One or more URL-encoded Pub/Sub patterns            |
| `/<command>/<args...>`     | `GET`  | URL-encoded arguments                               |
| `/<command>/<args...>`     | `POST` | URL arguments and the body as the final Redis value |

Authenticate with `Authorization: Bearer <token>` or the `_token` query parameter. An `Authorization` header takes precedence and does not fall back to `_token` when malformed or invalid. Only `/health` is available without authentication.

An empty path-command `POST` body becomes an empty Redis argument; use `GET` when no body argument is needed. Request bodies are limited to 10 MiB. `HEAD` and `PUT` command requests are unsupported.

### Responses and errors

JSON is the default response format. Add `Upstash-Encoding: base64` to base64-encode string results, or set `Upstash-Response-Format: resp2` for raw RESP2 bytes on individual commands and pipelines. Transactions at `/multi-exec` always return JSON. RESP2 cannot be combined with base64 encoding.

JSON errors use `{"error":"..."}`. Validation and Redis command errors return `400`; authentication failures return `401`; unsupported methods return `405`; unavailable Redis backends return `502`; and connection acquisition failures return `503`. A Redis error inside a RESP2 pipeline remains one reply in the concatenated `200` response.

### Pub/Sub

`@upstash/redis` subscriptions work through authenticated SSE streams:

```ts
const subscriber = redis.subscribe<{ text: string }>('updates')

subscriber.on('message', ({ channel, message }) => {
  console.log(channel, message.text)
})

await new Promise<void>((resolve, reject) => {
  subscriber.on('subscribe', () => resolve())
  subscriber.on('error', reject)
})

await redis.publish('updates', { text: 'hello' })
await subscriber.unsubscribe()
```

An idle stream receives an SSE heartbeat every 15 seconds. Closing or aborting the request unsubscribes it. Each stream holds one Redis connection and counts toward `UPREST_MAX_CONNECTIONS` or the backend's `max_connections` limit.

Channel and pattern names containing CR or LF are rejected. Commas are unsupported by the SDK parser, and a message payload containing CR or LF ends the stream.

### Monitor

Open an authenticated Redis `MONITOR` stream with `POST /monitor`:

```sh
curl -N -X POST \
  -H 'Authorization: Bearer example-token' \
  http://localhost:8079/monitor
```

The response is an SSE stream. Its first event is `data: "OK"`; subsequent events contain Redis's raw `MONITOR` lines, and idle streams receive a heartbeat every 15 seconds. Closing or aborting the request stops monitoring and releases its connection.

Each monitor uses a dedicated Redis connection and shares the backend's `UPREST_MAX_CONNECTIONS` or `max_connections` limit with commands and Pub/Sub streams. Redis warns that `MONITOR` can reduce throughput significantly, so use it temporarily for diagnostics and ensure the configured Redis user is allowed to run the command. Response-format and base64 headers do not change the stream.

### Rate limiting

Uprest works with `@upstash/ratelimit`, including versions 2.1.0 and later. It removes the Upstash-only `allow-key-locking` Lua flag before forwarding scripts to Redis and maps subsequent `EVALSHA` requests to the normalized script.

### SCAN with key types

The `@upstash/redis` `scan` option `{ withType: true }` is supported for commands, pipelines, and transactions. Uprest translates `SCAN WITHTYPE` into a read-only Lua script, so the configured Redis user must be allowed to run `EVAL`, `SCAN`, and `TYPE`.

### Known differences

Upstash Search and Vector commands, some RedisJSON response details, and selected Upstash-specific command behavior are not supported. The compatibility exclusions are documented in [`tests/compatibility/exclusions.txt`](tests/compatibility/exclusions.txt).

Uprest returns an opaque `Upstash-Sync-Token`, but it does not coordinate replicas with an incoming token because each authentication token routes to one Redis backend.

### Operations

Use `GET /health` to check HTTP liveness without contacting Redis. It returns `200` with `{"status":"ok"}`, or `503` with `{"status":"shutting_down"}` when the server's shutdown context is canceled. Redis outages and connection saturation do not affect this check.

Use authenticated `GET /ready` to check whether the token's backend can accept a request and respond to Redis `PING`:

```sh
curl --fail http://localhost:8079/health
curl --fail -H 'Authorization: Bearer example-token' http://localhost:8079/ready
```

Readiness returns `200` with `{"status":"ready"}` on success, or `503` with `{"status":"not_ready"}` when acquiring capacity or probing Redis fails. Capacity acquisition, connection initialization, and `PING` share a 2-second deadline. Missing or invalid authentication returns `401`. In file mode, only the backend selected by the token is checked; another backend's outage does not affect it.

Both endpoints accept only `GET`, return JSON with `Cache-Control: no-store`, and ignore Upstash response-format and encoding headers. The configured Redis user must be allowed to run `PING`. Readiness uses the normal pool and connection limit, does not cache results, and keeps the selected pool active when probed regularly. Use `/health` for liveness and `/ready` for readiness so a Redis outage does not trigger a process restart. The bundled Compose example waits for Redis's healthcheck before starting Uprest.

The exact paths `/health` and `/ready` are reserved HTTP endpoints and no longer forward Redis path commands. Applications needing to send commands with those names must use the JSON command endpoint at `POST /`.

Request, pool lifecycle, and shutdown events use structured JSON logs without bearer tokens or Redis connection strings. Successful health and readiness requests are logged at `debug`; failed probes remain at `info`. Requests canceled before writing a response are logged as `request canceled` with `canceled: true` and no HTTP status. Set `UPREST_LOG_LEVEL=debug` to include successful probes and script-normalization events.

All commands using shared Redis pools now respect context deadlines for socket reads and writes, including individual commands, pipelines, and transactions. This applies beyond readiness checks. Client disconnection and shutdown cancel request contexts, but cancellation does not guarantee immediate interruption of an in-flight socket operation or stop a command already executing in Redis.

The server uses a 5-second header-read timeout, 15-second read timeout, 50-second write timeout, and 60-second keep-alive timeout. Redis commands, backend connection acquisition, and initial Pub/Sub or `MONITOR` confirmation have a 30-second deadline. When a backend reaches its connection limit, a request waits for capacity until that deadline and then returns `503`. On shutdown, active request contexts are cancelled before the server waits up to 10 seconds for handlers to exit.

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

Service containers require a Linux runner. Services reach each other by name on the internal network, so no port needs to be published.

### Releases

Linux, macOS, and Windows archives are available on the [GitHub Releases page](https://github.com/nelsonlaidev/uprest/releases). Each archive includes the executable, README, changelog, and MIT license. Verify downloads with the published `checksums.txt` file.

Multi-architecture Linux images are published to both registries:

```sh
docker pull ghcr.io/nelsonlaidev/uprest:v0.3.0
docker pull nelsonlaidev/uprest:v0.3.0
```

Stable releases also use the `latest` tag. See [`CHANGELOG.md`](CHANGELOG.md) for release history.

To publish a release, push the release commits first, then create and push an annotated tag:

```sh
git tag -a v0.4.0 -F - <<'EOF'
### Highlights

- Add public `GET /health` for HTTP liveness and authenticated `GET /ready` for Redis backend readiness.

### Breaking changes

- The exact `/health` and `/ready` paths are now reserved. Send Redis commands with those names through `POST /` using a JSON command array.
- Shared Redis pools now honor request context deadlines for socket I/O across all commands. Cancellation does not guarantee immediate socket interruption or stop commands already executing in Redis.
EOF
git push origin v0.4.0
```

The tag message supports Markdown and appears in both the release notes and changelog; use it for release highlights and migration instructions. Use level-three headings such as `### Highlights` and `### Breaking changes` so they sit below the changelog's level-two version headings. Conventional Commits supply the individual change entries. The release workflow generates the changelog included in each archive and, after publication succeeds, commits an updated `CHANGELOG.md` to the default branch. The default branch must allow `github-actions[bot]` to push with `GITHUB_TOKEN`. Manual changelog edits are overwritten by generation; use `just changelog <version>` for a local preview.

## Migrate from SRH

When migrating from [`hiett/serverless-redis-http`](https://github.com/hiett/serverless-redis-http) (SRH), update these environment variables:

| SRH                     | Uprest                     |
| ----------------------- | -------------------------- |
| `SRH_MODE`              | `UPREST_MODE`              |
| `SRH_TOKEN`             | `UPREST_TOKEN`             |
| `SRH_CONNECTION_STRING` | `UPREST_CONNECTION_STRING` |
| `SRH_MAX_CONNECTIONS`   | `UPREST_MAX_CONNECTIONS`   |
| `SRH_PORT`              | `UPREST_PORT`              |
| `SRH_IPV6`              | `UPREST_IPV6`              |

`UPREST_MODE=env` is the default. Existing token and Redis URL values can stay the same. The container listens on port `8080`; update the client REST URL if the Compose service name changes.

For file mode, set `UPREST_MODE=file`, rename each token entry's `srh_id` field to `id`, and change the default mount destination from `/app/srh-config/tokens.json` to `/app/uprest-config/tokens.json`. A file containing only `srh_id` fails validation.

After switching, send a `PING` request with the existing token and run the application's SDK tests against the new REST URL. Applications using a recent `@upstash/ratelimit` version should also exercise a Lua-based limiter.

## Development and verification

Run the standard Go checks:

```sh
go test ./...
go vet ./...
```

The bundled [`examples/docker-compose.yml`](examples/docker-compose.yml) builds Uprest from source:

```sh
just up
just sdk-install
just sdk-test
```

The SDK tests cover `@upstash/redis` commands, pipelines, and transactions, plus the fixed-window, sliding-window, and token-bucket algorithms from `@upstash/ratelimit`.

The compatibility workflow also runs the official `upstash/redis-js` `packages/redis` suite. Pull requests and pushes to `main` use pinned upstream commit `6b2772753067e7d1aa103de25a3ee37bfd98093a`; the scheduled workflow tests upstream `main`.

To run that suite locally, start Redis and Uprest, clone `upstash/redis-js`, then run:

```sh
tests/compatibility/run.sh /path/to/redis-js
```

The script modifies the checkout without committing, so use a disposable clone. Its exclusions and adaptations live in [`tests/compatibility/exclusions.txt`](tests/compatibility/exclusions.txt) and [`tests/compatibility/upstream.patch`](tests/compatibility/upstream.patch).

## License

Uprest is licensed under the [MIT License](LICENSE).
