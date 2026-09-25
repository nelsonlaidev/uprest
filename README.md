# uprest

uprest is a local HTTP-to-Redis proxy for applications that use the Upstash Redis REST API. It forwards requests to a real Redis server, so your application can use `@upstash/redis` during local development and CI.

It supports commands, pipelines, transactions, and bearer-token or `_token` query authentication. It also adapts the Upstash-specific Lua flag used by recent `@upstash/ratelimit` versions so those scripts run on Redis.

## Quick start

Docker and Docker Compose are required. The examples use the published `ghcr.io/nelsonlaidev/uprest:v0.1.0` image.

### Start Redis and uprest

The quickest option is Compose, which starts Redis and uprest together:

```yaml
services:
  redis:
    image: redis:8.10.2
  uprest:
    image: ghcr.io/nelsonlaidev/uprest:v0.1.0
    ports:
      - '8079:8080'
    environment:
      UPREST_TOKEN: example-token
      UPREST_CONNECTION_STRING: redis://redis:6379
```

Save it as `docker-compose.yml` and run `docker compose up -d`. If you already have a Redis server, start uprest on its own:

```sh
docker run --rm -d -p 8079:8080 --name uprest \
  -e UPREST_TOKEN=example-token \
  -e UPREST_CONNECTION_STRING=redis://your-redis-host:6379 \
  ghcr.io/nelsonlaidev/uprest:v0.1.0
```

uprest listens on `8080` inside the image, and the port mapping above exposes it on `8079`. On macOS and Windows, Docker Desktop can reach a host Redis at `redis://host.docker.internal:6379`. Set your own token and Redis connection string outside local development.

### Point `@upstash/redis` at it

Use the uprest URL and token wherever you would use your Upstash credentials:

```ts
import { Redis } from '@upstash/redis'

const redis = new Redis({
  url: 'http://localhost:8079',
  token: 'example-token',
})

await redis.set('hello', 'world')
await redis.get('hello') // "world"
```

`Redis.fromEnv()` reads `UPSTASH_REDIS_REST_URL` and `UPSTASH_REDIS_REST_TOKEN`, so you can set those and leave application code unchanged.

### Verify with curl

```sh
curl -X POST http://localhost:8079/ \
  -H 'Authorization: Bearer example-token' \
  -H 'Content-Type: application/json' \
  -d '["SET","hello","world"]'
```

The response is `{"result":"OK"}`.

### `@upstash/ratelimit` (optional)

uprest works with `@upstash/ratelimit` regardless of version. From 2.1.0 onward, the SDK prepends the Upstash-only `allow-key-locking` Lua flag; uprest strips it before forwarding scripts to Redis, so those scripts run on stock Redis:

```ts
import { Redis } from '@upstash/redis'
import { Ratelimit } from '@upstash/ratelimit'

const ratelimit = new Ratelimit({
  redis: new Redis({ url: 'http://localhost:8079', token: 'example-token' }),
  limiter: Ratelimit.slidingWindow(10, '10 s'),
})

const { success } = await ratelimit.limit('user-123')
```

## CI (GitHub Actions)

uprest works as a service container alongside Redis, so CI jobs do not need a real Upstash database:

```yml
jobs:
  test:
    runs-on: ubuntu-latest
    services:
      redis:
        image: redis:8.10.2
      uprest:
        image: ghcr.io/nelsonlaidev/uprest:v0.1.0
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

Service containers require a Linux runner such as `ubuntu-latest`. Reference the uprest service by its name (`http://uprest:8080`) on the internal network; no ports need to be published.

## Releases

Release archives for Linux, macOS, and Windows are available on the [GitHub Releases page](https://github.com/nelsonlaidev/uprest/releases). Each archive includes the executable, README, CHANGELOG, and MIT license. Verify downloads with the published `checksums.txt` file.

The Linux container image is published for amd64 and arm64 in both registries:

```sh
docker pull ghcr.io/nelsonlaidev/uprest:v0.1.0
docker pull nelsonlaidev/uprest:v0.1.0
```

Both registries also publish a `latest` tag for stable releases. See [CHANGELOG.md](CHANGELOG.md) for release history.

## Configuration

uprest reads `UPREST_*` environment variables. `UPREST_MODE` selects the configuration source: `env` (the default) serves one Redis backend, while `file` loads one or more tokens from a JSON file.

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

For multiple tokens or Redis backends, set `UPREST_MODE=file` and mount a JSON file at `/app/uprest-config/tokens.json`, or set `UPREST_TOKENS_FILE` to another path. `UPREST_TOKEN` and `UPREST_CONNECTION_STRING` are only used in `env` mode; in `file` mode they are ignored.

```json
{
  "example-token": {
    "id": "primary",
    "connection_string": "redis://redis:6379",
    "max_connections": 3
  }
}
```

Each token selects its configured Redis backend. `max_connections` is optional and defaults to `3`. Redis pools are opened when first used and closed after the idle timeout.

## Documentation

- [Migrate from SRH](docs/migrating-from-srh.md) for configuration and token-file changes.
- [API coverage and verification](docs/compatibility.md) for supported routes, known gaps, and test instructions.

## Development

Run `go test ./...` and `go vet ./...` for the Go code. The bundled [Compose example](examples/docker-compose.yml) builds uprest from source; with it running, `just sdk-install` and `just sdk-test` exercise the installed `@upstash/redis` and `@upstash/ratelimit` SDKs.

## License

uprest is licensed under the [MIT License](LICENSE).
