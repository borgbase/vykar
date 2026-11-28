# AGENTS.md — vykar

Fast, encrypted, deduplicated backup tool in Rust. Workspace crates:
`vykar-types` (leaf: IDs, errors, hash primitives), `vykar-common`,
`vykar-crypto`, `vykar-protocol` (client<->server wire types),
`vykar-storage` (backends), `vykar-core` (library), `vykar-cli`,
`vykar-server`, `vykar-gui`. Dependency direction is
`vykar-core` -> `vykar-storage` -> `vykar-protocol` -> `vykar-types`;
`vykar-types` depends on nothing internal, so anything that must cross a layer
boundary lives there. `fuzz/` is **excluded** from the workspace with its own
lockfile.

Repository layout, protocol and format details are in
`docs/src/content/docs/architecture.md` and `server-internals.md`. Read the
relevant section there, and the code, before changing them; do not rely on a
summary here.

## Verification

`make pre-commit` runs format check, clippy with `-D warnings`, doc check and the full test suite including ignored tests, matching CI. It does **not** cover `fuzz/` — run `cargo check --bins` inside `fuzz/` after touching anything it uses. It is slow. Run it **once** at the end of a change and capture its output to a file so downstream checks can consult the logs instead of re-running:

```
make pre-commit 2>&1 | tee /tmp/vykar-pre-commit.log
```

Do not keep re-invoking `cargo check`, `cargo test`, `cargo clippy`, or `make pre-commit` to inspect partial progress — they duplicate the same work. When iterating on a single test, run that test only (`cargo test -p <crate> --lib <test path>`).

## Conventions

- **Formats are compatibility surfaces.** Everything written to a repository or sent over the wire is read by other versions. Change an on-disk or wire encoding only as a deliberate, versioned format change, and check how an older binary fails on the new bytes: it must refuse by version, not with a parse error.
- **Serialization**: on-disk and wire types use `rmp_serde` (msgpack). Many are positional arrays, where adding, removing, reordering or conditionally skipping a field (`#[serde(skip_serializing_if)]`) breaks decoding. The repository `config` and the index head are msgpack **maps**: a new field needs `#[serde(default)]`. A field without a feature name is advisory: an older binary drops keys it does not know whenever it rewrites the object (every index write re-encodes the head from its struct, `vykar upgrade --commit-mode` re-encodes `config`), so a field that must survive, or that readers must understand, needs a name in `features`, which older binaries refuse. Tests pin the dropping (`unknown_keys_are_dropped_by_successor`, `unknown_map_keys_are_dropped_on_rewrite`).
- **Protocol between v6 clients**: the session-marker JSON, the key shapes under `sessions/` and the timing constants (`repo/ages.rs`, `SESSION_STALE_SECS`, `REFRESH_INTERVAL`, `MARKER_GAP_LIMIT`, `COMMIT_MAX_WAIT`, `HEADS_KEPT`) are part of format v6 and pinned in `crates/vykar-core/src/tests/protocol_v6.rs`. Changing one needs a new `config` feature name so older clients refuse; never just update the pinned value.
- **Frozen fixtures** of released repository formats live in `crates/vykar-core/tests/fixtures/` and must never be regenerated.
- **Output split**: `vykar-core` never prints — it communicates via return values and progress callbacks. `vykar-cli` uses stdout for results/tables and stderr for status/errors.
