---
title: "Maintain the Repository"
---

Running `vykar` with no subcommand, the [daemon](/daemon), and the desktop GUI all run the full cycle: `backup`, then `prune`, `compact`, and `check`. The commands below run each step on its own.

## Prune old snapshots

Apply the [retention policy](/configuration#retention) defined in your configuration to remove expired snapshots. Prune only deletes the snapshot objects; the space their data uses is reclaimed by the next `compact`. Optionally `compact` the repository after pruning. With `--compact`, the prune [hooks](/configuration#hooks) run around prune and the compact hooks then run around compact, as two separate runs; if prune fails or is skipped, compact does not run.

```bash
# Preview which snapshots would be kept and pruned, with reasons
vykar prune --dry-run --list

# Prune and reclaim space
vykar prune --compact
```

## Compact (reclaim space)

`delete` and `prune` free no space: they only delete snapshot objects. `compact` reclaims it. On every run it first marks every live snapshot and removes the index entries none of them references (the sweep), then rewrites and deletes packs.

A snapshot's space is reclaimed by a compact at least 12 hours after the backup that wrote its chunks: compact keeps the chunks that backups of the last 12 hours added, referenced or not, because a backup running meanwhile may reuse them. Deleting a fresh snapshot and compacting at once therefore frees its own chunks only at a compact 12 hours later; the compact output says how many it kept ("unreferenced chunks … kept: added by backups in the last 12 h"). Packs compact empties are deleted once they are 12 hours old too. Index objects no index head names any more (the base a fold replaced, the objects of a commit that lost a race) are kept 12 hours as well before backups or compact delete them: every fold of the last 12 hours leaves its previous base, and every fold attempt that lost to a concurrent commit its own, each about the size of the whole index. For a stream of backups that is little: they leave none or one extra base. A long compact leaves more: it saves its progress once per batch of packs (64 packs or 4 GiB) and every save folds once 64 foldable segments have gathered, so a compact of B batches writes about B/64 full bases, and all but the newest stay 12 hours (a compact of 2,500 batches: about 39 bases). An index write that stalls for more than 11 hours 20 minutes between its first index upload and its head (a machine suspended mid-commit) fails with "the index write took longer than …; retry" rather than publish objects that may already be collected; running the backup again writes fresh index objects.

Restores, listings, `find`, `diff` and `check` running elsewhere do not hold compact (or `prune`, `delete`) back: readers register nothing, and read what compact moved from its new place. A restore of a snapshot that is deleted and compacted meanwhile fails and says so. A snapshot that `prune` or `delete` removes while `check` or `find` runs is skipped with a warning; `diff` of such a snapshot fails with "snapshot not found".

Compact, `prune` and `delete` also run while backups upload. Compact keeps what a running backup has uploaded (orphan packs younger than 12 hours, and those a live backup may still need; the summary counts the ones it kept for a running backup), and also keeps the uploads of a crashed backup until they are 12 hours old. A backup that finishes uploading while a compact runs waits for it, up to 2 hours, then commits; a backup that starts while maintenance holds the lock fails with "locked" after a few seconds, as before. A backup that reused data a concurrent compact removed fails and asks to be retried. On an encrypted repository whose storage offers put-if-absent (and passed vykar's check), backups commit without the lock (see [Lock-free commits](/architecture#lock-free-commits-and-the-marker-handshake)), and compact, prune and delete take no lock either (see [Lock-free maintenance](/architecture#lock-free-maintenance)): backups start and commit while a compact runs, and two compacts can run at once. A compact that meets a backup in the middle of its commit (a few seconds per backup) removes no unreferenced chunk in that run and says so ("No unreferenced chunks removed this run: a backup was committing; a later compact removes them"), and so does one during which backups committed on each of three tries; it still repacks and deletes what it can (a compact skips its sweep as soon as it sees the committing backup, before any mark, so with backups committing continuously, every second or so, most compact runs remove no unreferenced chunk: run compact when backups are not committing back to back), except packs younger than 12 hours or written since a running backup started, which it leaves for a later compact (the summary counts them). A compact whose machine was suspended for more than half an hour stops when it resumes ("this run's session marker was reaped, lost, or not refreshed in time while it was writing ..."): run it again. A backup committing under the lock waits for a running lock-free compact to finish. When two compacts run at once and repack the same packs, the one that loses reports no pack repacked for them and says how many of its repacked packs were abandoned; a compact deletes those 12 hours later. A backup that crashed while committing makes compacts skip their sweep that way for up to 45 minutes. A client whose storage offers no put-if-absent (or failed the check) still takes the lock; its compact, prune or delete that starts while a lock-free backup is committing or a lock-free compact runs fails with "repository is locked by <host> …" naming that machine, before it changes anything: run it again.

Compact fails closed: when a snapshot object cannot be read or decoded, an object under `snapshots/` is not a snapshot, or a listing chunk or a data reference is missing from the index, it refuses with "compact refused: the live snapshots could not be marked, nothing was changed" (no object was changed; stale upload temp files older than an hour may already have been removed). Every compact refuses until `vykar check --repair` removes the object. A snapshot written by a newer vykar also makes compact refuse, and repair deliberately does not remove it: upgrade vykar instead. Compact also refuses when no snapshot is listed while the index still holds chunks (it does not sweep a repository to empty; `vykar delete` removes a repository).

```bash
# Preview what would be repacked
vykar compact --dry-run

# Repack to reclaim space
vykar compact

# Report index entries no snapshot references (changes nothing)
vykar compact --report-unreferenced
```

`--report-unreferenced` reads every snapshot and prints how many index entries
no snapshot references (what the next compact sweeps), their size and the packs
that hold them. It takes the
maintenance lock (it runs while backups upload), and fails rather than
reporting anything when a snapshot or a listing chunk cannot be read.

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
and (always) dropping index entries no surviving snapshot references.

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
removed by the plan. When this machine's index cache still holds intact copies
of the missing or damaged `index.d/` objects, repair uses them instead of
rebuilding, and uploads the index afresh. Run it with `--dry-run` first, and
run
`vykar check --verify-data` after a rebuild: the rebuild trusts what the packs
say, and only a full verification finds chunks that rotted inside them.

A storage error while reading the index or the packs is not treated as damage:
repair changes nothing and reports the error. Neither is a pack trailer or pack
map written by a newer vykar: repair refuses, names the object and asks for an
upgrade. If no newer vykar has written to the repository, an object under
`packmaps/` that this vykar cannot attribute does not belong there and may be
removed by hand. Repair then describes the packs it named from their trailers
or from this machine's base tables; a pack with neither becomes uncovered, and
the snapshots that need it are removed.
`check --verify-data` also checks pack trailers; a damaged trailer is repaired
by writing a pack map for its pack, which removes nothing.

If a backup crashes after its index was committed but before its snapshot was
written, the index keeps orphan chunk entries that no snapshot references; a
`compact` at least 12 hours later sweeps them and reclaims the space.

## Delete a snapshot

Deleting a snapshot only deletes its snapshot object; run `compact` to reclaim its space.

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

`--sessions` also removes the markers of backups that are committing, and of compacts, prunes and deletes that are running, without the lock right now (encrypted repositories on storage with put-if-absent): run it only when no backup or maintenance command is running.

A crashed process's lock or marker is removed automatically, without `break-lock`, once it has not been written for 6 hours (locks) or 45 minutes (markers), measured on the storage's clock (the object's modification time), so a client with a wrong clock neither removes live entries early nor keeps dead ones; an unreadable marker is removed the same way. Where the storage's listings report no modification time (a vykar-server too old to report them), an unreadable marker is never removed by age: on an encrypted repository it then makes every command that takes the lock (a maintenance command or backup in lock mode, pack maps, `check --repair`, upgrade) fail with, and every lock-free compact skip its sweep beside it, with "a session marker that may belong to a committing backup is unreadable or was just removed", naming the marker's key (`sessions/<id>.json`). A marker that was just removed (its backup deregistered while the command read it) gives the same message once: run the command again. If it persists and no backup is running, `vykar break-lock --sessions` removes it. A vykar process on the same machine removes it at once, but only when it can tell the entry is its own machine's: the same hostname and installation ID (`<config dir>/vykar/installation-id`, per user), the same boot and the same PID namespace. A root CLI and a daemon running under the documented systemd unit have different config directories, so the CLI waits for the age limit on a crashed daemon's entries; so does a restarted container for the entries of its previous run. Entries written by vykar 0.24 and older are judged by age only. Locks and markers are not encrypted, so the installation ID is readable by anyone who can read the storage and links the repositories one installation writes to; deleting the file gives a new ID. See [Same-host reaping](/architecture#same-host-reaping).

## Upgrade the repository format

Repositories created before 0.25 (format v2 or v3) store the chunk index as one object, which those releases rewrote on every backup, prune and compact. On a large repository that was hundreds of megabytes per write. This vykar opens them only to upgrade them: every other command refuses and names `vykar upgrade` (`key export`/`key import`, `delete-repo` and `break-lock` still work). `vykar upgrade` converts the repository in place to format v6, the segmented index (a v2 repository keeps BLAKE2b, a v3 one BLAKE3), after which each write uploads only what changed. Chunk and pack hashes stay the same, so nothing is re-uploaded except the index itself, once. Before the switch it writes a pack map for every existing pack (those packs have no trailer), so the repository's index can always be rebuilt from the packs; it reports how many packs it mapped, and warns about any pack too large for a map.

```bash
vykar upgrade -R <repo>          # asks for confirmation
vykar upgrade -R <repo> --yes    # for scripts
```

- **One-way.** vykar releases before 0.25 can no longer read or write the repository afterwards. Upgrade vykar on every host that uses it, and stop all of them, before upgrading.
- **No active backups.** The upgrade takes the maintenance lock and refuses while backup sessions are active, like `check --repair` (unlike `compact` and `prune`, which run beside them).
- **Append-only servers.** The upgrade rewrites `config` and deletes `index`, which an append-only `vykar-server` refuses; it fails before changing anything. The procedure: turn append-only off for the repository (restart `vykar-server` without `--append-only`), run `vykar upgrade`, then turn append-only on again.
- **Interrupted upgrades** are completed by running `vykar upgrade` again; until then, every other command refuses and names `vykar upgrade`. If the interrupted upgrade left a damaged index, `vykar upgrade` still completes the switch to the new format and then reports the damage: run `vykar check --repair` (with `--dry-run` first to see what it would change). If `index` turns out to be a damaged pre-0.25 index instead, the upgrade changes nothing and says so; restore `index` from a copy of the repository.
- **Before the switch, older clients still work.** An upgrade that stopped before switching `config` leaves the repository in its old format with extra directories beside it (`packmaps/`, and possibly `index.h/` and `index.d/`). Clients before 0.25 ignore them, but a `vykar-server` before 0.25 refuses to start on a data directory that contains them ("contains unexpected entries"). Run `vykar upgrade` again to finish; to stay on the old format instead, remove those directories while `config` is still the old one.
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
