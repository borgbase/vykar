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
- **Serialization**: on-disk and wire types use `rmp_serde` (msgpack). Many are positional arrays, where adding, removing, reordering or conditionally skipping a field (`#[serde(skip_serializing_if)]`) breaks decoding.
- **Frozen fixtures** of released repository formats live in `crates/vykar-core/tests/fixtures/` and must never be regenerated.
- **Output split**: `vykar-core` never prints — it communicates via return values and progress callbacks. `vykar-cli` uses stdout for results/tables and stderr for status/errors.
