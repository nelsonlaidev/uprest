# API coverage and verification

uprest accepts authenticated Upstash-style commands over HTTP and forwards them to Redis. The included Compose example uses Redis 8.10.2.

## Routes

| Endpoint                   | Method | Request                                                   |
| -------------------------- | ------ | --------------------------------------------------------- |
| `/`                        | `POST` | JSON command array                                        |
| `/pipeline`                | `POST` | JSON array of commands                                    |
| `/multi-exec`              | `POST` | JSON array of commands, run in Redis `MULTI`/`EXEC`       |
| `/subscribe/<channel...>`  | `POST` | One or more URL-encoded Pub/Sub channels                  |
| `/psubscribe/<pattern...>` | `POST` | One or more URL-encoded Pub/Sub patterns                  |
| `/<command>/<args...>`     | `GET`  | URL-encoded arguments                                     |
| `/<command>/<args...>`     | `POST` | URL arguments plus raw request body as the final argument |

An empty path-command `POST` body is an empty Redis argument. Use `GET` when no body argument is needed. Add `Upstash-Encoding: base64` to encode string results in JSON responses. Set `Upstash-Response-Format: resp2` to return raw RESP2 bytes with an `application/octet-stream` content type for individual commands and pipelines. Pipeline replies are concatenated in command order, matching Redis pipelining. The `/multi-exec` endpoint always returns JSON. Combining RESP2 with base64 encoding or requesting a response format other than `json` or `resp2` returns JSON 400.

The subscription routes return `text/event-stream`, ignore `Upstash-Encoding` and `Upstash-Response-Format`, and emit the following records:

```text
data: subscribe,<channel>,<count>
data: message,<channel>,<payload>
data: psubscribe,<pattern>,<count>
data: pmessage,<pattern>,<channel>,<payload>
```

Each record is followed by a blank line. An idle stream receives `: keepalive` as an SSE comment every 15 seconds. Channel and pattern names containing CR or LF are rejected, and a message payload containing either character ends the stream because the redis-js wire format does not escape line breaks. Commas in channel and pattern names remain unsupported by the SDK parser. Closing or aborting the request unsubscribes it; there is no separate HTTP unsubscribe endpoint. Every stream holds one Redis connection from the backend's `max_connections` or `UPREST_MAX_CONNECTIONS` limit. `MONITOR` remains unsupported and returns JSON 404 after authentication.

Authenticate with `Authorization: Bearer <token>` or the `_token` query parameter. If an `Authorization` header is present, it takes precedence; an invalid or malformed header returns 401 without falling back to `_token`. `HEAD` and `PUT` command requests are unsupported.

JSON errors use `{"error":"..."}`. Invalid commands, pipelines, and transactions return 400, as does a single command rejected by Redis. With RESP2 enabled, a Redis command error is returned as a raw error reply with status 400, while an error inside a pipeline remains one of the concatenated replies in a 200 response. Request validation and infrastructure failures remain JSON. Missing or invalid tokens return 401, unsupported methods return 405, unavailable Redis backends return 502, and connection acquisition failures return 503. Request bodies are limited to 10 MiB.

## Lua scripts

`@upstash/ratelimit` 2.1.0 and later can prepend the Upstash-only `allow-key-locking` Lua flag. uprest removes unsupported flags before forwarding the script to Redis, preserves the script body, and maps the original script hash to the normalized script hash for later `EVALSHA` calls.

## Verification

With the Compose example running, use `just sdk-install` and `just sdk-test` to exercise `@upstash/redis` commands, pipelines, and transactions, plus `@upstash/ratelimit` fixed window, sliding window, and token bucket algorithms. For direct `npm test --prefix tests/sdk` runs, set `UPREST_TEST_URL` and `UPREST_TEST_TOKEN`.

The compatibility workflow also runs the official `upstash/redis-js` `packages/redis` suite. Pull requests and pushes to `main` use the pinned upstream commit `6b2772753067e7d1aa103de25a3ee37bfd98093a`; the scheduled run tests upstream `main`. The baseline exclusions and adaptations are in `tests/compatibility/exclusions.txt` and `tests/compatibility/upstream.patch`. They fail closed if an excluded test disappears or a patch no longer applies.

Known intentional differences include Upstash Search and Vector commands, `MONITOR` streaming, some RedisJSON response details, and selected Upstash-specific command behavior documented by the exclusions. uprest returns an opaque `Upstash-Sync-Token` but does not coordinate replicas with the incoming token because each token routes to one Redis backend.

To run the upstream suite locally, start uprest and Redis 8, export `UPSTASH_REDIS_REST_URL` and `UPSTASH_REDIS_REST_TOKEN`, clone `upstash/redis-js`, then run `tests/compatibility/run.sh /path/to/redis-js`. The script modifies that checkout without committing; use a disposable clone.

## Operations

Request, pool lifecycle, and shutdown events are written as structured JSON without bearer tokens or Redis connection strings. Request logs include an internal request ID. Set `UPREST_LOG_LEVEL=debug` for script-normalization events.

The HTTP server has a 5-second header read timeout, 15-second read timeout, 50-second write timeout, and 60-second keep-alive idle timeout. Subscription streams use a fresh 5-second deadline for each write and a 15-second heartbeat. Redis commands and the initial Pub/Sub confirmation have a 30-second deadline. A process shutdown signal immediately cancels active request contexts, including ordinary commands and subscription streams, before the server waits up to 10 seconds for handlers to exit.
