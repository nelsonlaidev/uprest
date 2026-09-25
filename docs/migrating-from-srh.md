# Migrate from SRH to uprest

This guide is for users of [`hiett/serverless-redis-http`](https://github.com/hiett/serverless-redis-http) (SRH). uprest uses the same Upstash-style HTTP requests for the supported endpoints, but its configuration uses uprest names. Update your deployment configuration before switching the service.

## Environment mode

Replace these environment variables in your Compose file or deployment:

| SRH                     | uprest                     |
| ----------------------- | -------------------------- |
| `SRH_MODE`              | `UPREST_MODE`              |
| `SRH_TOKEN`             | `UPREST_TOKEN`             |
| `SRH_CONNECTION_STRING` | `UPREST_CONNECTION_STRING` |
| `SRH_MAX_CONNECTIONS`   | `UPREST_MAX_CONNECTIONS`   |
| `SRH_PORT`              | `UPREST_PORT`              |
| `SRH_IPV6`              | `UPREST_IPV6`              |

`UPREST_MODE=env` is the default. Keep the token and Redis URL values if they still point to the correct backend. For example:

```yaml
services:
  uprest:
    build: .
    ports:
      - '8079:8080'
    environment:
      UPREST_TOKEN: example-token
      UPREST_CONNECTION_STRING: redis://redis:6379
```

The container image listens on port `8080` by default. If applications address the old service by its Compose hostname, update their REST URL to the new hostname, or keep the old Compose service name during the transition. Keep the same bearer token if you do not want to change client credentials.

## File mode

Set `UPREST_MODE=file`. Rename each token entry's `srh_id` field to `id`; keep the token, `connection_string`, and optional `max_connections` values. For example:

```json
{
  "example-token": {
    "id": "primary",
    "connection_string": "redis://redis:6379",
    "max_connections": 3
  }
}
```

If you previously mounted `/app/srh-config/tokens.json`, change the destination to `/app/uprest-config/tokens.json`. You can also set `UPREST_TOKENS_FILE` to another mounted path. A file containing only `srh_id` will fail validation because `id` is required.

## Verify the switch

Start uprest with Redis available, then send a request using your existing token:

```sh
curl -X POST http://localhost:8079/ \
  -H 'Authorization: Bearer example-token' \
  -H 'Content-Type: application/json' \
  -d '["PING"]'
```

The expected response is `{"result":"PONG"}`. Run your application's SDK tests against the new REST URL. If you use a recent `@upstash/ratelimit`, exercise one of its Lua-based limiters as well.

See [API coverage and verification](compatibility.md) for supported routes and known differences. `SRH_*` variables and `srh_id` are migration inputs only; uprest does not read them.
