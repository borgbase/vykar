# Contributing to Vykar

Thanks for your interest in contributing!

## How to contribute

1. **Open an issue first.** Before writing any code, please [open an issue](https://github.com/borgbase/vykar/issues) describing the bug or feature. This lets us discuss the approach before you invest time on a PR.

2. **PRs require an approved issue.** Pull requests are only accepted for issues that have been triaged and approved. If you open a PR without a corresponding issue, you'll be asked to create one first.

3. **Prefer detailed feature requests over PRs.** A well-written feature request — with motivation, use cases, and expected behavior — is often more valuable than a code contribution. It helps us design the feature to fit the project's architecture and long-term direction.

## AI-generated content

AI-assisted contributions are welcome, but they must meet the same quality bar as any other contribution: complete, tested, and ready to merge. Partial or rough drafts that need significant rework will be closed.

## Development

```bash
cargo build --release
cargo test
make pre-commit          # fmt-check + clippy -D warnings + tests
```

See [CLAUDE.md](CLAUDE.md) for the full project structure and architecture overview.
