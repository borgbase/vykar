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

`scripts/gen-segmented-fixtures.sh <commit>` documents how they were produced.
It refuses to overwrite an existing archive; `--new-scenario NAME` writes a
fixture for a new scenario as `v{4,5}-NAME-<mode>.tar.gz`.
