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
