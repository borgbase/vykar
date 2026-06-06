---
name: release
description: "Cut a new vykar release: bump versions, tag, push, wait for CI, and draft release notes."
---

# vykar Release

Release workflow for vykar. The CI pipeline lives in `.github/workflows/release.yml` and triggers on `v*` tags pushed to `main`. It builds binaries for Linux (x86_64), macOS (aarch64), and Windows (x86_64), then publishes a GitHub Release with the artifacts and SHA256 checksums.

## Steps

### 1. Pre-flight checks

Run `make pre-commit` first to catch formatting, clippy, and test issues before bumping versions. Fix any problems before proceeding.

### 2. Bump version

Update the `version` field in **all** workspace crates and the macOS Info.plist:

- `crates/vykar-types/Cargo.toml`
- `crates/vykar-common/Cargo.toml`
- `crates/vykar-crypto/Cargo.toml`
- `crates/vykar-storage/Cargo.toml`
- `crates/vykar-protocol/Cargo.toml`
- `crates/vykar-core/Cargo.toml`
- `crates/vykar-cli/Cargo.toml`
- `crates/vykar-server/Cargo.toml`
- `crates/vykar-gui/Cargo.toml`
- `crates/vykar-gui/macos/Info.plist` (update both `CFBundleVersion` and `CFBundleShortVersionString`)

Run `cargo check` to regenerate `Cargo.lock` with the new versions.

### 3. Commit, tag, and push

Commit the version bump (include any other pending changes that should ship). Create a git tag `v<version>` and push both the commit and tag to `origin main`. Pushing the tag triggers the release workflow.

```
git add <changed files> && git commit -m "Bump version to <version>"
git tag v<version>
git push origin main --tags
```

### 4. Wait for the release workflow

Use the GitHub CLI to find the triggered workflow run and watch it until all jobs complete:

```
gh run list --limit 5
gh run watch <run-id> --exit-status
```

The workflow builds on three runners (Linux, macOS, Windows), then a `publish` job downloads the artifacts, generates SHA256 checksums, and creates the GitHub Release via `softprops/action-gh-release`.

### 5. Draft release notes

Review all commits since the previous tag:

```
git log <prev-tag>..v<version> --oneline --no-merges
```

Categorize the changes into sections (e.g. Features, Performance, Bug Fixes, Infrastructure) and update the release:

```
gh release edit v<version> --notes "<release notes>"
```

Include a downloads table at the bottom listing each platform artifact.
