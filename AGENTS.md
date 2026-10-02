# AGENTS.md

## Go Code Style

- Match the intentional blank-line spacing in nearby Go files, including tests.
- Put a blank line between setup and a following validation or control-flow block.
- Separate distinct logical steps with a blank line, including around `defer` and goroutine setup, after completed blocks, and before a final return when preceding work is complete.
- Keep statements that form one step together, such as consecutive option assignments and struct fields. Do not insert blank lines mechanically after every statement.
- Preserve user-added blank lines when editing existing code. Run `gofmt`, then review spacing manually because it does not add these logical blank lines.

## Releasing

1. Ensure the release changes are committed and pushed. Use Conventional Commits; record breaking changes with `!` or a `BREAKING CHANGE` footer.
2. Create and push an annotated version tag. Follow the README's multi-line annotation example, using `### Highlights` and `### Breaking changes` for release notes and migration instructions.

Pushing a `v*` tag triggers `.github/workflows/release.yml`, which generates release notes and an archive changelog from Git history and annotated tag messages, then publishes the release archives and the multi-arch container images to GHCR and Docker Hub. After publication succeeds, the workflow regenerates `CHANGELOG.md` and opens a pull request from `github-actions[bot]`; a maintainer reviews and merges it. The repository must enable **Allow GitHub Actions to create and approve pull requests** in Settings → Actions → General → Workflow permissions so `GITHUB_TOKEN` can open the pull request. Do not manually edit `CHANGELOG.md`; `just changelog <version>` is available for a local preview.
