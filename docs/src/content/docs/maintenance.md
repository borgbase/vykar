---
title: "Maintain the Repository"
---

Running `vykar` with no subcommand, the [daemon](/daemon), and the desktop GUI all run the full cycle: `backup`, then `prune`, `compact`, and `check`. The commands below run each step on its own.

## Prune old snapshots

Apply the [retention policy](/configuration#retention) defined in your configuration to remove expired snapshots. Optionally `compact` the repository after pruning. With `--compact`, the prune [hooks](/configuration#hooks) run around prune and the compact hooks then run around compact, as two separate runs; if prune fails or is skipped, compact does not run.

```bash
# Preview which snapshots would be kept and pruned, with reasons
vykar prune --dry-run --list

# Prune and reclaim space
vykar prune --compact
```

## Compact (reclaim space)

After `delete` or `prune`, blob data remains in pack files. Run `compact` to rewrite packs and reclaim disk space.

```bash
# Preview what would be repacked
vykar compact --dry-run

# Repack to reclaim space
vykar compact
```

## Verify repository integrity

```bash
# Structural integrity check
vykar check

# Full data verification (reads and verifies every chunk)
vykar check --verify-data
```

How much the scheduled cycle checks is set by the [`check`](/configuration#check) config section.

### Repair

`vykar check --repair` plans and applies fixes for the problems `check` detects:
removing corrupt or dangling snapshots, dropping index entries for missing packs,
and (always) rebuilding chunk refcounts from the surviving snapshots.

```bash
# Preview the repair plan without changing anything
vykar check --repair --dry-run

# Apply the repair (prompts for confirmation)
vykar check --repair

# Apply non-interactively (for scripting)
vykar check --repair --yes
```

When the index cannot be read (its head is missing or damaged, or an object of
`index.d/` is missing or damaged), repair rebuilds it from the packs, which
describe their own contents: each pack ends in a trailer listing its chunks,
and packs written before trailers are covered by pack maps under `packmaps/`
or, failing both, by this machine's index cache. Nothing is lost for packs that
are described this way. The plan says how the index was rebuilt and lists any
pack that nothing describes; snapshots whose data lies only in such packs are
removed by the plan. Run it with `--dry-run` first, and run
`vykar check --verify-data` after a rebuild: the rebuild trusts what the packs
say, and only a full verification finds chunks that rotted inside them.

A storage error while reading the index or the packs is not treated as damage:
repair changes nothing and reports the error. Neither is a pack trailer or pack
map written by a newer vykar: repair refuses, names the object and asks for an
upgrade. If no newer vykar has written to the repository, an object under
`packmaps/` that this vykar cannot attribute does not belong there and may be
removed by hand. Repair then describes the packs it named from their trailers
or from this machine's chunk tables; a pack with neither becomes uncovered, and
the snapshots that need it are removed.
`check --verify-data` also checks pack trailers; a damaged trailer is repaired
by writing a pack map for its pack, which removes nothing.

The refcount rebuild also reclaims the space leak left by an **interrupted backup**.
If a backup crashes after its index was committed but before its snapshot was
written, the index keeps orphan chunk entries (and inflated refcounts) that no
snapshot references. `compact` alone will not free these — it treats any in-index
chunk as live. `vykar check --repair` recomputes refcounts from the surviving
snapshots and drops the orphan entries; a follow-up `vykar compact` then reclaims
the disk space.

## Delete a snapshot

```bash
# Delete a specific snapshot by ID
vykar snapshot delete a1b2c3d4

# Preview without deleting
vykar snapshot delete a1b2c3d4 --dry-run
```

## Delete a repository

Permanently delete an entire repository and all its snapshots.

```bash
# Interactive confirmation (prompts you to type "delete")
vykar delete

# Non-interactive (for scripting)
vykar delete --yes-delete-this-repo
```

## Remove stale locks

A process that was killed can leave its repository lock behind, which blocks later operations with a lock conflict. Once you are sure no vykar process is still using the repository, remove it:

```bash
vykar break-lock

# Also remove session markers left by crashed backups
vykar break-lock --sessions
```

## Upgrade the repository format

Repositories created before 0.25 (format v2 or v3) store the chunk index as one object, which those releases rewrote on every backup, prune and compact. On a large repository that was hundreds of megabytes per write. This vykar opens them only to upgrade them: every other command refuses and names `vykar upgrade` (`key export`/`key import`, `delete-repo` and `break-lock` still work). `vykar upgrade` converts the repository in place to the segmented index (v2 → v4, v3 → v5), after which each write uploads only what changed. Chunk and pack hashes stay the same, so nothing is re-uploaded except the index itself, once.

```bash
vykar upgrade -R <repo>          # asks for confirmation
vykar upgrade -R <repo> --yes    # for scripts
```

- **One-way.** vykar releases before 0.25 can no longer read or write the repository afterwards. Upgrade vykar on every host that uses it, and stop all of them, before upgrading.
- **No active backups.** The upgrade takes the maintenance lock and refuses while backup sessions are active, like `compact` and `prune`.
- **Append-only servers.** The upgrade rewrites `config` and `index`, which an append-only `vykar-server` refuses; it fails before changing anything. Disable append-only for the upgrade.
- **Interrupted upgrades** are completed by running `vykar upgrade` again; until then, every other command refuses and names `vykar upgrade`. If the interrupted upgrade left a damaged index, `vykar upgrade` still completes the switch to the new format and then reports the damage: run `vykar check --repair` (with `--dry-run` first to see what it would change). If `index` turns out to be a damaged pre-0.25 index instead, the upgrade changes nothing and says so; restore `index` from a copy of the repository.
- If a pre-0.25 client still wrote to the repository after the upgrade, commands refuse and name `vykar check --repair`, which rebuilds the index.

New repositories are created in the current format; there is nothing to upgrade.

## Sharing one repository between hosts

Several machines can back up to the same repository, but they should run
**compatible vykar versions**. Compatibility is backward-readable, not
forward-compatible: a newer binary reads everything older ones wrote, but an
older binary cannot read snapshots written by a newer one.

If you see this when listing or backing up:

```text
WARN snapshot 3f9c…: stored envelope has 15 fields, this build reads 14 —
     written by a newer vykar; upgrade vykar on this host. Skipping —
     this repository listing is incomplete
1 snapshot hidden: written by a newer vykar — upgrade vykar on this host to see it
```

…then another machine using this repository is running a newer vykar than this
one. Specifically:

- **Nothing is lost or damaged.** The snapshot is intact on the server and
  fully readable from the up-to-date host. This machine simply cannot decode
  its metadata, so it leaves it alone.
- **This host's view of the repository is incomplete.** The hidden snapshots
  are missing from `vykar list`, and cannot be restored from *this* machine.
- **The fix is to upgrade vykar on the host printing the warning.** Compare
  `vykar --version` across your machines and bring the oldest up to date.

Older releases of vykar reported this as a bare deserialization error and
silently dropped the snapshot from the listing. If you are running a version
older than 0.17.0 alongside newer ones, do **not** run `vykar check --repair`
on the older host: it misclassifies the unreadable snapshot as corrupt and
deletes it. Current versions refuse to repair while any such snapshot is
present.

## Related pages

- [Server Setup](/server-setup) (server-side compaction)
- [Architecture](/architecture#compact) (compact algorithm details)
