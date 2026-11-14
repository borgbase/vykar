# Configure and Initialize

## Generate a configuration file

Create a starter config:

```bash
vykar config
```

Or write it to a specific path:

```bash
vykar config --dest ~/.config/vykar/config.yaml
```

See [Config file locations](configuration.md#config-file-locations) for where vykar looks for it.

## Configure repositories and sources

Set the repository URL and the directories to back up:

```yaml
repositories:
  - label: "main"
    url: "/backup/repo"

sources:
  - "/home/user/documents"
  - "/home/user/photos"
```

The URL selects the backend — a local path, S3, SFTP, or a REST server. See [Storage Backends](backends.md) for remote repositories and [Configuration](configuration.md) for all available options.

## Encryption

Encryption is enabled by default (`mode: "auto"`). During `init`, vykar benchmarks AES-256-GCM and ChaCha20-Poly1305, chooses one, and stores that concrete mode in the repository config. No config is needed unless you want to force a mode or disable encryption with `mode: "none"`.

The passphrase is requested interactively at init time. You can also supply it via:

- `VYKAR_PASSPHRASE` environment variable
- `passcommand` in the config (e.g. `passcommand: "pass show vykar"`)
- `passphrase` in the config

## Initialize the repository

```bash
vykar init
```

This creates the repository structure at the configured URL. For encrypted repositories, you will be prompted to enter a passphrase.

If your config has multiple repositories, use `--repo` / `-R` to initialize one entry at a time:

```bash
vykar init --repo main
```

## Validate

Confirm the repository was created:

```bash
vykar info
```

Run a first backup and check results:

```bash
vykar backup
vykar list
```

## Back up your repository key

The passphrase alone is not enough to open an encrypted repository.

The master key is 64 bytes of random data generated at `init`; it is **not**
derived from your passphrase. The passphrase only derives a wrapping key
(Argon2id) that encrypts the master key, and the result — about 180 bytes — is
what lives at `keys/repokey` inside the repository. **Both are required, and
neither can be recovered from the other.** If the key blob is gone, the data is
gone, even with the correct passphrase.

Export it once and store it in the same password-manager entry as the
passphrase:

```sh
vykar key export -R main -o vykar-repokey-main.txt
```

The export is a small armored text block, and it stays passphrase-protected —
there is deliberately no way to export the unwrapped master key. `key export`
unwraps the key before printing it, so it doubles as a check that your
passphrase and key file still work together. Without `-o` the block goes to
stdout, ready to pipe into a password manager. One export holds one
repository's key, so with several repositories configured `key export` and
`key import` require `-R`.

To restore it — including into a repository that can no longer be opened,
which is exactly when you need it:

```sh
vykar key import -R main vykar-repokey-main.txt
```

vykar also keeps a **second copy of the key inside the repository**
(`keys/repokey.2`), written at `init` and backfilled into older repositories
the next time they are opened. If one copy rots, vykar opens off the other and
`vykar check --repair` rewrites the damaged one. That covers bit rot and
localized corruption — **not** loss of the storage as a whole, so it
complements an off-repository copy rather than replacing one.

## Repository format

`vykar init` always creates a **format v3** repository, whose chunk and pack IDs use BLAKE3. There is no flag or config key to choose otherwise. `vykar info` reports the format and hash of any repository.

Existing **format v2** repositories (created by vykar 0.19 and earlier, using BLAKE2b) stay v2 for life and remain fully readable and writable — nothing needs migrating. The hash is part of every chunk's dedup identity and every pack's storage key, so it cannot be changed in place.

This matters in two situations:

- **Older vykar binaries cannot open a v3 repository** (`unsupported repository version: 3`). If several machines share one repository, upgrade them together — or keep using the existing repository, which needs no coordination at all.
- **A self-hosted `vykar-server` must be upgraded first** (0.20 or later). `init` checks this up front and refuses with a clear message rather than creating a repository whose first backup would fail.

Next: [Back Up](backup.md)
