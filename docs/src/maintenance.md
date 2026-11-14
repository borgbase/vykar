# Maintain the Repository

Running `vykar` with no subcommand, the [daemon](daemon.md), and the desktop GUI all run the full cycle: `backup`, then `prune`, `compact`, and `check`. The commands below run each step on its own.

## Prune old snapshots

Apply the [retention policy](configuration.md#retention) defined in your configuration to remove expired snapshots. Optionally `compact` the repository after pruning.

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

How much the scheduled cycle checks is set by the [`check`](configuration.md#check) config section.

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

- [Server Setup](server-setup.md) (server-side compaction)
- [Architecture](architecture.md#compact) (compact algorithm details)
