# Frozen repository-format-v2 fixtures

Each `v2-<mode>.tar.gz` is a complete repository **written by a binary that
predates BLAKE3** (commit `bfd0a85`), together with the source tree it was
taken from and the local cache state that run produced:

| entry | contents |
|---|---|
| `repo/` | the repository (format `version: 2`) |
| `source/` | the exact tree the last snapshot contains |
| `cache/` | pin file, file cache, dedup/index/snapshot caches |
| `meta.json` | encryption mode, passphrase, snapshot IDs, source commit |

## Do not regenerate these

They exist so `tests/v2_fixture_compat.rs` can prove today's binary still reads
repositories that already exist in the wild. Rewriting them with a current
binary would hide exactly the class of bug they catch: a regression shared by
the writer and the reader. A failure in that suite is a compatibility break,
not a stale expectation.

`scripts/gen-v2-fixtures.sh` documents how they were produced and can build a
fixture for a genuinely new scenario from the same pre-change commit.

# Frozen repository-format-v4/v5 fixtures

Each `v4-<mode>.tar.gz` and `v5-<mode>.tar.gz` is a segmented-index repository
written by the binary of commit `d0a2c1b`, with the same four entries as above.
Its index head names a base, exactly two segments over it, and a non-empty
`retired` set: the objects the last fold replaced. `meta.json` also lists the
`index.d/` object names, the expected segment count and, for v4, the v2
archive it derives from.

- **v5** starts from `vykar init`.
- **v4** starts from `v2-<mode>.tar.gz` (its `repo/` and `source/`), upgraded
  in place with `vykar upgrade`, which is the only way a v4 repository comes
  to exist.

Both then run 15 backups and one snapshot delete, `compact --threshold 0` (the
16 segments reach the soft limit, so the chain folds into a base and the
previous objects become `retired`), and two more backups.

## Do not regenerate these either

The reason is the same as for v2: `tests/segmented_fixture_compat.rs` must prove
that today's binary reads and extends segmented repositories an older binary
wrote. That suite pins the BLAKE3 digest of each archive, so a regenerated or
replaced archive fails it; restore the committed file with `git checkout`.

Since 05 step 8 no binary opens v4/v5; `segmented_fixture_compat.rs` only
checks that they are refused by version and unchanged. They are deleted in
05 step 10.

`scripts/gen-segmented-fixtures.sh <commit>` documents how they were produced.
It refuses to overwrite an existing archive; `--new-scenario NAME` writes a
fixture for a new scenario as `v{4,5}-NAME-<mode>.tar.gz`.

# Frozen repository-format-v3 fixtures

Each `v3-<mode>.tar.gz` is a repository written by the binary of release tag
`v0.24.0` (commit `cd6782db29bff6638581758af72efcfda1267985`), with the same
four entries as above. Format v3 (BLAKE3 IDs, monolithic `index` and
`index.gen`) is what vykar 0.20 to 0.24 write, so it is the format upgrading
users arrive with, and the upgrade source for the segmented and later formats.

Per mode: `init` and backup A; backup B, which adds a transient file and a
file that stays in the source; backup C after removing the transient file and
changing the source; `snapshot delete` B and `compact --threshold 0`, which
repacks B's partly live pack (the generator refuses a run without a repack);
one more file and backup D; `check --verify-data`. `source/` is the tree D
contains. `meta.json` lists the snapshots in creation order (A, C, D), the
deleted snapshot (B), and the tag and commit that wrote them.

## Do not regenerate these either

`tests/v3_fixture_compat.rs` pins the BLAKE3 digest of each archive and proves
that today's binary opens, restores, verifies and upgrades them with the
`cache/` 0.24 wrote present. Restore a changed archive with `git checkout`.

`scripts/gen-v3-fixtures.sh v0.24.0` documents how they were produced. It
refuses to overwrite an existing archive and to build from a commit that is
not a release tag.

# Vendored release types

`tests/vendored/v0_19_1.rs` and `v0_24_0.rs` hold the `config` types of those
releases, copied verbatim from the tags between `BEGIN`/`END` markers that name
the tag, path and line range. Do not edit them: each block must diff against
`git show <tag>:<path> | sed -n '<range>p'` with only visibility and `use` lines
differing. `tests/legacy_config_refusal.rs` uses them to prove a future
map-encoded `config` is refused by version, and
`scripts/check-old-release-refusal.sh` runs the same check against the real
release binaries.
