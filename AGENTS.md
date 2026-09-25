# AGENTS.md

## Go Code Style

- Match the intentional blank-line spacing in nearby Go files, including tests.
- Put a blank line between setup and a following validation or control-flow block.
- Separate distinct logical steps with a blank line, including around `defer` and goroutine setup, after completed blocks, and before a final return when preceding work is complete.
- Keep statements that form one step together, such as consecutive option assignments and struct fields. Do not insert blank lines mechanically after every statement.
- Preserve user-added blank lines when editing existing code. Run `gofmt`, then review spacing manually because it does not add these logical blank lines.

## Releasing

1. Run `just changelog <version>` to regenerate `CHANGELOG.md`, then review and commit it.
2. Create and push a tag matching the version, for example `git tag v0.1.0 && git push origin v0.1.0`.

Pushing a `v*` tag triggers `.github/workflows/release.yml`, which publishes the release archives and the multi-arch container images to GHCR and Docker Hub.
