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

Each token selects its backend; `max_connections` is optional and defaults to `3`. `UPREST_TOKEN` and `UPREST_CONNECTION_STRING` are ignored in file mode.

## API

### Routes

| Endpoint                   | Method | Request                                            |
| -------------------------- | ------ | -------------------------------------------------- |
| `/health`                  | `GET`  | HTTP liveness check; no authentication required    |
| `/ready`                   | `GET`  | Authenticated Redis backend readiness check        |
| `/`                        | `POST` | JSON command array                                 |
| `/pipeline`                | `POST` | JSON array of commands                             |
| `/multi-exec`              | `POST` | JSON array of commands run in Redis `MULTI`/`EXEC` |
| `/monitor`                 | `POST` | Redis `MONITOR` output as an SSE stream            |
| `/subscribe/<channel...>`  | `POST` | One or more URL-encoded Pub/Sub channels           |
| `/psubscribe/<pattern...>` | `POST` | One or more URL-encoded Pub/Sub patterns           |
| `/<command>/<args...>`     | `GET`  | URL-encoded arguments                              |
| `/<command>/<args...>`     | `POST` | URL arguments and the body as the final Redis value |

Authenticate with `Authorization: Bearer <token>` or the `_token` query parameter. An `Authorization` header takes precedence and does not fall back to `_token`; only `/health` is public.

An empty path-command `POST` body becomes an empty Redis argument; use `GET` when no body argument is needed. Request bodies are limited to 10 MiB, and `HEAD`/`PUT` command requests are unsupported.

### Responses and errors

JSON is the default response format. Add `Upstash-Encoding: base64` to base64-encode string results, or set `Upstash-Response-Format: resp2` for raw RESP2 bytes on commands and pipelines. Transactions at `/multi-exec` always return JSON, and RESP2 cannot be combined with base64 encoding.

Errors use `{"error":"..."}`. Validation and Redis command errors return `400`, authentication failures `401`, unsupported methods `405`, unavailable Redis backends `502`, and connection acquisition failures `503`.

`/health` and `/ready` are reserved paths and return JSON status objects; send Redis commands with those names through `POST /`.

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
