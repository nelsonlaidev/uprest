# Changelog

All notable changes to this project will be documented in this file.

## v0.5.0

[Compare v0.4.0...v0.5.0](https://github.com/nelsonlaidev/uprest/compare/v0.4.0...v0.5.0) - 2026-10-15


### Highlights

- Add opt-in Prometheus metrics on `GET /metrics`, authenticated with a dedicated `UPREST_METRICS_TOKEN`. Export HTTP latency and errors, Redis capacity and pool activity, and active SSE streams without exposing credentials or contacting Redis during scrapes.
- Add process-wide `UPREST_ACQUIRE_TIMEOUT` and `UPREST_COMMAND_TIMEOUT`, each defaulting to `30s`, with independent capacity and execution budgets and an automatically calculated HTTP write timeout.
- Validate complete command requests before acquiring backend capacity. Reject prohibited batches before executing any commands or updating script aliases to protect shared Redis connections.

### Breaking changes

- Ordinary commands, pipelines, and transactions now reject connection-changing, transaction-control, streaming, replication/session, and script-debugging commands with HTTP `400` and `{"error":"Command is not supported over HTTP"}`. This includes `AUTH`, `HELLO`, `SELECT`, `MULTI`, `EXEC`, `WATCH`, subscription commands, `MONITOR`, and `SCRIPT DEBUG`.
- `CLIENT` now permits only `GETNAME`, `GETREDIR`, `ID`, `INFO`, `LIST`, `TRACKINGINFO`, and `HELP`. Configure Redis credentials and database selection in backend connection URLs; use `/multi-exec` for transactions and `/subscribe`, `/psubscribe`, or `/monitor` for SSE streams. See the README's shared connection isolation section for the complete policy.
- When metrics are enabled, the exact `/metrics` path is reserved and accepts only its dedicated Bearer token. Use `POST /` for a Redis command named `metrics`; leave `UPREST_METRICS_TOKEN` unset to preserve the existing path behavior.

### Features

- Add authenticated Prometheus metrics - ([b82e410](https://github.com/nelsonlaidev/uprest/commit/b82e4105f1e94922524712a87a1bfc1347394fa2))
- Add configurable acquisition and command timeouts - ([ece98cb](https://github.com/nelsonlaidev/uprest/commit/ece98cb1c328738ef29705dde5dac6069cba8a58))
- Validate commands to protect shared Redis connections - ([14f2901](https://github.com/nelsonlaidev/uprest/commit/14f2901e6f3bdf74e4444b6dfe717af669c15f0a))

## v0.4.0

[Compare v0.3.0...v0.4.0](https://github.com/nelsonlaidev/uprest/compare/v0.3.0...v0.4.0) - 2026-10-02


### Highlights

- Add public `GET /health` for HTTP liveness and authenticated `GET /ready` for Redis backend readiness.
- Readiness shares a two-second deadline across capacity acquisition, connection initialization, and PING.

### Breaking changes

- The exact `/health` and `/ready` paths are now reserved. Send Redis commands with those names through `POST /` using a JSON command array.
- Shared Redis pools now honor request context deadlines for socket I/O across all commands. Cancellation does not guarantee immediate socket interruption or stop commands already executing in Redis.

### Operational changes

- Successful probes log at debug level; failed probes remain at info level.
- Requests canceled before writing a response no longer report HTTP 200 in logs.

### Features

- [**breaking**] Add health and readiness checks (#5) - ([3fa4e36](https://github.com/nelsonlaidev/uprest/commit/3fa4e36f7836659fd3bc0664e9d143e464251dac))

### Documentation

- Capitalize project name in README - ([1307fed](https://github.com/nelsonlaidev/uprest/commit/1307fedef3e49b072ab35dd006f5efc5333aeb63))
- Update readme - ([2e9494f](https://github.com/nelsonlaidev/uprest/commit/2e9494fc295d06a31bca667761918da58f8402e3))
- Simplify readme and add contributing guide - ([6b0a2de](https://github.com/nelsonlaidev/uprest/commit/6b0a2dea428d2ae0226b5145851fa43e233ec420))

## v0.3.0

[Compare v0.2.0...v0.3.0](https://github.com/nelsonlaidev/uprest/compare/v0.2.0...v0.3.0) - 2026-09-29

### Features

- Add Redis MONITOR SSE stream (#3) - ([85ecf31](https://github.com/nelsonlaidev/uprest/commit/85ecf31597436a2db6739aa3de85de216cf09f27))
- Support Upstash SCAN WITHTYPE option (#4) - ([35a706b](https://github.com/nelsonlaidev/uprest/commit/35a706b3590a8c6fbc719ae3100d8f8aacae8359))

### Documentation

- Consolidate documentation into readme - ([43b48f9](https://github.com/nelsonlaidev/uprest/commit/43b48f9d9a5758a0a9553c0adf5e3eda665006a3))
- Use latest tag in local README examples - ([f07cc06](https://github.com/nelsonlaidev/uprest/commit/f07cc063cc9588f8627921c85b3afeb0b7db4436))
- Update pinned image tags to v0.3.0 - ([a757c1b](https://github.com/nelsonlaidev/uprest/commit/a757c1b007db4376a674b1018dbd06969f6d69fb))

## v0.2.0

[Compare v0.1.0...v0.2.0](https://github.com/nelsonlaidev/uprest/compare/v0.1.0...v0.2.0) - 2026-09-28

### Features

- Add RESP2 response support (#1) - ([8661acf](https://github.com/nelsonlaidev/uprest/commit/8661acf2754e55b893cec6ed048ea171752aca1c))
- Add SSE pubsub support (#2) - ([d89f32d](https://github.com/nelsonlaidev/uprest/commit/d89f32d83949ca8bb2736f7a41b6c529e083cc1c))

## v0.1.0

[View v0.1.0](https://github.com/nelsonlaidev/uprest/tree/v0.1.0) - 2026-09-25

### Features

- Initial release - ([399a8b1](https://github.com/nelsonlaidev/uprest/commit/399a8b1d7c5dea91901c85828a827b8f453470e4))


