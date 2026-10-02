# Contributing to Uprest

Thanks for contributing. This guide covers local development, tests, and releases.

## Prerequisites

- Go 1.24 or later
- [just](https://github.com/casey/just)
- Docker and Docker Compose
- Node.js and npm for the SDK smoke tests
- pnpm and Bun for the upstream compatibility suite

## Common commands

Run all commands from the repository root:

| Command          | Purpose                           |
| ---------------- | --------------------------------- |
| `just build`     | Build `bin/uprest`                |
| `just run`       | Run from source                   |
| `just test`      | Unit tests                        |
| `just test-race` | Unit tests with the race detector |
| `just lint`      | `go vet` and `golangci-lint`      |
| `just fmt`       | Format Go code                    |

Integration tests connect to Redis through `UPREST_TEST_REDIS_URL`:

```sh
just test-integration
just test-integration redis://localhost:6380
```

`just test-integration` defaults to `redis://localhost:6379`.

## SDK smoke tests

Start Uprest and Redis with the bundled Compose example, then run the SDK tests:

```sh
just up
just sdk-install
just sdk-test
just down
```

The smoke tests cover `@upstash/redis` commands, pipelines, and transactions, plus the fixed-window, sliding-window, and token-bucket `@upstash/ratelimit` algorithms. `just sdk-test` accepts `url` and `token` arguments for a custom instance.

## Compatibility suite

The compatibility workflow runs the official `upstash/redis-js` `packages/redis` suite. Pull requests and pushes to `main` use pinned upstream commit `6b2772753067e7d1aa103de25a3ee37bfd98093a`; the scheduled workflow tests upstream `main`.

To run the suite locally, start Redis and Uprest, clone `upstash/redis-js`, then run:

```sh
tests/compatibility/run.sh /path/to/redis-js
```

The script removes the files listed in [`tests/compatibility/exclusions.txt`](tests/compatibility/exclusions.txt) and applies [`tests/compatibility/upstream.patch`](tests/compatibility/upstream.patch) without committing, so use a disposable clone. It requires pnpm and Bun.

## Commits

Use [Conventional Commits](https://www.conventionalcommits.org/). Mark breaking changes with `!` or a `BREAKING CHANGE` footer.

## Releasing

1. Commit and push the release changes.
2. Create and push an annotated tag:

```sh
git tag -a v0.4.0 --cleanup=verbatim -F - <<'EOF'
### Highlights

- Add public `GET /health` for HTTP liveness and authenticated `GET /ready` for Redis backend readiness.

### Breaking changes

- The exact `/health` and `/ready` paths are now reserved. Send Redis commands with those names through `POST /`.
EOF
git push origin v0.4.0
```

Use `--cleanup=verbatim` to preserve Markdown headings, and level-three headings such as `### Highlights` and `### Breaking changes` so they sit below the changelog's version headings.

Pushing a `v*` tag triggers [`.github/workflows/release.yml`](.github/workflows/release.yml), which publishes release archives and multi-arch container images to GHCR and Docker Hub. After publication, the workflow regenerates and commits `CHANGELOG.md` to the default branch. Do not edit `CHANGELOG.md` manually; `just changelog <version>` previews it locally.

## CI

- [`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs `just test-race`, `just lint`, and `just build`.
- [`.github/workflows/compatibility.yml`](.github/workflows/compatibility.yml) runs the upstream compatibility suite on pull requests, pushes to `main`, and a daily schedule.
- [`.github/workflows/release.yml`](.github/workflows/release.yml) publishes releases on `v*` tags.
