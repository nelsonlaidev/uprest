# uprest

uprest is a local HTTP-to-Redis proxy for applications that use the Upstash Redis REST API. It forwards requests to a real Redis server, so applications can keep using `@upstash/redis` during local development and CI.

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
    image: ghcr.io/nelsonlaidev/uprest:v0.2.0
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

Then point `@upstash/redis` at uprest:

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

To connect uprest to an existing Redis server instead, run only the proxy:

```sh
docker run --rm -d -p 8079:8080 --name uprest \
  -e UPREST_TOKEN=example-token \
  -e UPREST_CONNECTION_STRING=redis://your-redis-host:6379 \
  ghcr.io/nelsonlaidev/uprest:v0.2.0
```

The image listens on port `8080`. On macOS and Windows, Docker Desktop can reach Redis on the host at `redis://host.docker.internal:6379`. Use a private token and an appropriate Redis URL outside local development.

## Configuration

uprest reads `UPREST_*` environment variables. `UPREST_MODE` selects one Redis backend from environment variables or multiple backends from a JSON file.

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
| `/`                        | `POST` | JSON command array                                  |
| `/pipeline`                | `POST` | JSON array of commands                              |
| `/multi-exec`              | `POST` | JSON array of commands run in Redis `MULTI`/`EXEC`  |
| `/monitor`                 | `POST` | Redis `MONITOR` output as an SSE stream             |
| `/subscribe/<channel...>`  | `POST` | One or more URL-encoded Pub/Sub channels            |
| `/psubscribe/<pattern...>` | `POST` | One or more URL-encoded Pub/Sub patterns            |
| `/<command>/<args...>`     | `GET`  | URL-encoded arguments                               |
| `/<command>/<args...>`     | `POST` | URL arguments and the body as the final Redis value |

Authenticate with `Authorization: Bearer <token>` or the `_token` query parameter. An `Authorization` header takes precedence and does not fall back to `_token` when malformed or invalid.

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

uprest works with `@upstash/ratelimit`, including versions 2.1.0 and later. It removes the Upstash-only `allow-key-locking` Lua flag before forwarding scripts to Redis and maps subsequent `EVALSHA` requests to the normalized script.

### Known differences

Upstash Search and Vector commands, some RedisJSON response details, and selected Upstash-specific command behavior are not supported. The compatibility exclusions are documented in [`tests/compatibility/exclusions.txt`](tests/compatibility/exclusions.txt).

uprest returns an opaque `Upstash-Sync-Token`, but it does not coordinate replicas with an incoming token because each authentication token routes to one Redis backend.

### Operations

Request, pool lifecycle, and shutdown events use structured JSON logs without bearer tokens or Redis connection strings. Set `UPREST_LOG_LEVEL=debug` to include script-normalization events.

The server uses a 5-second header-read timeout, 15-second read timeout, 50-second write timeout, and 60-second keep-alive timeout. Redis commands, backend connection acquisition, and initial Pub/Sub or `MONITOR` confirmation have a 30-second deadline. When a backend reaches its connection limit, a request waits for capacity until that deadline and then returns `503`. On shutdown, active request contexts are cancelled before the server waits up to 10 seconds for handlers to exit.

## Deployment

### GitHub Actions

uprest can run as a service container alongside Redis, so CI does not need an Upstash database:

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    services:
      redis:
        image: redis:8.10.2
      uprest:
        image: ghcr.io/nelsonlaidev/uprest:v0.2.0
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
docker pull ghcr.io/nelsonlaidev/uprest:v0.2.0
docker pull nelsonlaidev/uprest:v0.2.0
```

Stable releases also use the `latest` tag. See [`CHANGELOG.md`](CHANGELOG.md) for release history.

## Migrate from SRH

When migrating from [`hiett/serverless-redis-http`](https://github.com/hiett/serverless-redis-http) (SRH), update these environment variables:

| SRH                     | uprest                     |
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

The bundled [`examples/docker-compose.yml`](examples/docker-compose.yml) builds uprest from source:

```sh
just up
just sdk-install
just sdk-test
```

The SDK tests cover `@upstash/redis` commands, pipelines, and transactions, plus the fixed-window, sliding-window, and token-bucket algorithms from `@upstash/ratelimit`.

The compatibility workflow also runs the official `upstash/redis-js` `packages/redis` suite. Pull requests and pushes to `main` use pinned upstream commit `6b2772753067e7d1aa103de25a3ee37bfd98093a`; the scheduled workflow tests upstream `main`.

To run that suite locally, start Redis and uprest, clone `upstash/redis-js`, then run:

```sh
tests/compatibility/run.sh /path/to/redis-js
```

The script modifies the checkout without committing, so use a disposable clone. Its exclusions and adaptations live in [`tests/compatibility/exclusions.txt`](tests/compatibility/exclusions.txt) and [`tests/compatibility/upstream.patch`](tests/compatibility/upstream.patch).

## License

uprest is licensed under the [MIT License](LICENSE).
