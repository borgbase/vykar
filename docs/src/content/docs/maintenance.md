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

Compact, `prune` and `delete` also run while backups upload. Compact keeps what a running backup has uploaded (orphan packs younger than 12 hours, and those a live backup may still need; the summary counts the ones it kept for a running backup), and also keeps the uploads of a crashed backup until they are 12 hours old. On a single-writer repository a backup that finishes uploading while a compact runs waits for it, up to 2 hours, then commits (it fails if the compact still runs then); a backup that starts while `check --repair`, an upgrade or a pack-map write is running (an exclusive session) fails with "an exclusive maintenance run … holds the repository" after a few seconds. A backup that is still uploading when one of them starts makes it refuse with "active sessions prevent maintenance", naming the backup, before it changes anything: run it again once the backup has finished. Only a backup whose session marker was already removed (its machine was suspended for more than 45 minutes, or `vykar clear-sessions` ran) can finish uploading while one of them runs; it then waits for it before it commits. A backup that reused data a concurrent compact removed fails and asks to be retried. On a multi-writer repository (see [Commit mode](#commit-mode)) backups commit side by side (see [Commit modes](/architecture#commit-modes)), and compact, prune and delete run beside them (see [Maintenance sessions](/architecture#maintenance-sessions)): backups start and commit while a compact runs, and two compacts can run at once. A compact that meets a backup in the middle of its commit (a few seconds per backup) removes no unreferenced chunk in that run and says so ("No unreferenced chunks removed this run: a backup was committing; a later compact removes them"), and so does one during which backups committed on each of three tries; it still repacks and deletes what it can (a compact skips its sweep as soon as it sees the committing backup, before any mark, so with backups committing continuously, every second or so, most compact runs remove no unreferenced chunk: run compact when backups are not committing back to back), except packs younger than 12 hours or written since a running backup started, which it leaves for a later compact (the summary counts them). A compact whose machine was suspended for more than half an hour stops when it resumes ("this run's session marker was reaped, lost, or not refreshed in time while it was writing ..."): run it again. When two compacts run at once and repack the same packs, the one that loses reports no pack repacked for them and says how many of its repacked packs were abandoned; a compact deletes those 12 hours later. A backup that crashed while committing makes compacts skip their sweep that way for up to 45 minutes. On a single-writer repository compact, prune and delete register a session as well, and a compact that starts while a backup is committing or another compact runs fails with "a backup committing or a compact running on <host> … holds the repository", before it changes anything: run it again.

Compact fails closed: when a snapshot object cannot be read or decoded, an object under `snapshots/` is not a snapshot, or a listing chunk or a data reference is missing from the index, it refuses with "compact refused: the live snapshots could not be marked, nothing was changed" (no object was changed; stale upload temp files older than an hour may already have been removed). Every compact refuses until `vykar check --repair` removes the object. When the cause is an index that lost entries a live snapshot needs (a lost index update, a damaged maintenance write), the message says so and names `vykar check --repair`, which re-indexes those chunks from the packs; until a compact marks again or a repair is applied, `vykar info` (and the GUI overview) on the machine that ran the compact shows "Compact: refused since …". A snapshot written by a newer vykar also makes compact refuse, and repair deliberately does not remove it: upgrade vykar instead. Compact also refuses when no snapshot is listed while the index still holds chunks (it does not sweep a repository to empty; `vykar delete` removes a repository).

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
that hold them. It registers no session and writes no repository data (it runs
beside backups and compacts; on a multi-writer repository it first runs the
put-if-absent check, which writes small probe objects under `sessions/`, so it
needs write access there), and fails rather than reporting anything when a snapshot or a listing chunk cannot be read.

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

When the index is readable but a snapshot references chunks it lacks (a lost
index update: the packs are on storage but no index head names them), repair
looks them up in the packs the index names none of (their pack maps, then
their trailers, then this machine's index cache, in that order), re-indexes
what it finds ("Re-index N chunk(s) the index lost …", a safe action), and only
removes snapshots or drops files for what is still missing. It then scans the
snapshots a second time, to plan on the re-indexed index. Run
`vykar check --verify-data` afterwards, as after a rebuild.

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

If a pack upload fails while repair rewrites snapshots, repair stops before
publishing any rewritten snapshot and changes nothing it had not already
applied; run it again.

A repair interrupted between publishing a rewritten snapshot and deleting the
original leaves two snapshot objects of one name (`vykar check` reports a name
carried by more than one object); the next repair removes the
rewrite (it holds nothing the original lacks) and rewrites the original again.
Of several objects of one name, repair also removes one whose listing cannot
be read when another can be (data loss, as for a lone snapshot), and refuses,
changing nothing, any other name carried by more than one object: the
refusal names the objects and the one `vykar snapshot delete <name>` would
remove (the first listed, by time, then ID).

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

## Remove stale session markers

A process that was killed can leave its session marker behind, which blocks later operations ("... holds the repository"). Once you are sure no vykar process is still using the repository, remove the markers:

```bash
vykar clear-sessions
# Unattended (cron, scripts): remove only markers that are already dead
vykar clear-sessions --stale
```

This command was called `vykar break-lock` until this release; `break-lock` is no longer accepted (nor its `--sessions` flag), so replace it in scripts.

It first removes any lock files under `locks/` that vykar 0.24 or an interrupted upgrade left, at once and without asking, then lists every session marker and asks before removing them all. Without a terminal it removes the lock files, then fails (exit status 1) whenever a session marker exists, also one of a healthy running backup, so never run the plain command unattended. In the desktop GUI, **Clear Sessions** on the Advanced page does the same after a confirmation.

`vykar clear-sessions --stale` removes only the markers that are already dead, by the same rule and with the same limits as automatic reaping (see [Known limits](#known-limits)): not written for 45 minutes on the storage's clock, or left on this machine by a vykar process that is gone (see the same-host rule below). It never asks and needs no terminal; it prints each marker it removed and how many live markers it left (listing them on stderr), and exits with status 0 even when live markers remain. It removes the leftover lock files under `locks/` first, as the plain command does, only on a repository of the current format (v6), where nothing there is live; on a v2/v3 repository, where they can be the locks of running vykar 0.24 clients or the key of a running `vykar upgrade`, or when the repository config cannot be read, it leaves `locks/` alone and says so on stderr. It removes nothing else: crash-recovery journals (`sessions/<id>.index`) stay for the next backup to recover from. It does not remove the marker of a running backup, compact, prune, delete, repair, upgrade or pack-map write. This is the form for cron jobs and scripts. It reads the storage's clock by writing and deleting one small probe object under `sessions/`. Since every command that a marker would block removes such markers itself, `--stale` unblocks nothing sooner than that command would: it tidies up and shows what is still live.

A killed `check --repair`, `upgrade` or pack-map write leaves its session marker (`sessions/x-…`): it blocks backups and maintenance until 45 minutes after its last refresh, at once for a vykar process on the same machine, or until `vykar clear-sessions`, run on a terminal, removes it. These commands also stop when their machine was suspended or the storage unreachable for more than 30 minutes, or one marker write took over 10 minutes ("this run's session marker was reaped, lost, or not refreshed in time ..."): run them again.

It also removes the markers of backups that are committing, and of compacts, prunes and deletes that are running right now: run it only when no backup or maintenance command is running.

A crashed process's marker is removed automatically, without `clear-sessions`, once it has not been written for 45 minutes, measured on the storage's clock (the object's modification time), so a client with a wrong clock neither removes live entries early nor keeps dead ones; an unreadable marker is removed the same way. Where the storage's listings report no modification time (a vykar-server too old to report them), an unreadable marker is never removed by age: it then makes every `check --repair`, upgrade and pack-map write refuse with "active sessions prevent maintenance" (listing it as a malformed marker), every compact on a single-writer repository fail with "a session marker that may belong to a committing backup is unreadable or was just removed", naming the marker's key (`sessions/<id>.json`), and every compact on a multi-writer repository skip its sweep beside it. A marker that was just removed (its backup deregistered while the command read it) gives the same message once: run the command again. If it persists and no backup is running, `vykar clear-sessions` removes it. A vykar process on the same machine removes it at once, but only when it can tell the entry is its own machine's: the same hostname and installation ID (`<config dir>/vykar/installation-id`, per user), the same boot and the same PID namespace. A root CLI and a daemon running under the documented systemd unit have different config directories, so the CLI waits for the age limit on a crashed daemon's entries; so does a restarted container for the entries of its previous run. Entries written by vykar 0.24 and older are judged by age only. Session markers are not encrypted, so the installation ID is readable by anyone who can read the storage and links the repositories one installation writes to; deleting the file gives a new ID. See [Same-host reaping](/architecture#same-host-reaping).

## Upgrade the repository format

Repositories created before 0.25 (format v2 or v3) store the chunk index as one object, which those releases rewrote on every backup, prune and compact. On a large repository that was hundreds of megabytes per write. This vykar opens them only to upgrade them: every other command refuses and names `vykar upgrade` (`key export`/`key import`, `delete-repo` and `clear-sessions` still work). `vykar upgrade` converts the repository in place to format v6, the segmented index (a v2 repository keeps BLAKE2b, a v3 one BLAKE3), after which each write uploads only what changed. Chunk and pack hashes stay the same, so nothing is re-uploaded except the index itself, once. Before the switch it writes a pack map for every existing pack (those packs have no trailer), so the repository's index can always be rebuilt from the packs; it reports how many packs it mapped, and warns about any pack too large for a map.

```bash
vykar upgrade -R <repo>          # asks for confirmation
vykar upgrade -R <repo> --yes    # for scripts
```

- **One-way.** vykar releases before 0.25 can no longer read or write the repository afterwards. Upgrade vykar on every host that uses it, and stop all of them, before upgrading.
- **No active backups.** The upgrade refuses while backup sessions are active, like `check --repair` (unlike `compact` and `prune`, which run beside them). It runs as an exclusive session, so backups and maintenance that start while it runs fail with "an exclusive maintenance run … holds the repository". The upgrade of a v2/v3 repository holds a lock key in the format vykar 0.24 reads, so 0.24 backups, reads and maintenance refuse with "repository is locked" while it runs. The key is rewritten at the first write once 5 minutes have passed since its last rewrite, and checked before every write; an upgrade whose key was removed, or whose key could not be rewritten for 5.5 hours, stops before switching the format (run it again). The 5.5 hours count from the last successful rewrite, at most 5 minutes before the last write: a stretch without writes (reading a very large legacy index) of up to 5 hours 25 minutes is always tolerated.
- **0.24 clients with the same hostname.** A vykar 0.24 process on a machine with the upgrading machine's hostname (a clone, the same machine after a reboot, another container with that hostname) takes the upgrade's lock key for stale at once; the upgrade then stops before the switch (unless that happens between two steps of the upgrade's next rewrite of its key, which then recreates the key and runs on beside the 0.24 process). Stop vykar 0.24 on such machines before upgrading.
- **Append-only servers.** The upgrade rewrites `config` and deletes `index`, which an append-only `vykar-server` refuses; it fails before changing anything. The procedure: turn append-only off for the repository (restart `vykar-server` without `--append-only`), run `vykar upgrade`, then turn append-only on again.
- **Interrupted upgrades** are completed by running `vykar upgrade` again; until then, every other command refuses and names `vykar upgrade`. If the interrupted upgrade left a damaged index, `vykar upgrade` still completes the switch to the new format and then reports the damage: run `vykar check --repair` (with `--dry-run` first to see what it would change). If `index` turns out to be a damaged pre-0.25 index instead, the upgrade changes nothing and says so; restore `index` from a copy of the repository.
- **Before the switch, older clients still work.** An upgrade that stopped before switching `config` leaves the repository in its old format with extra directories beside it (`packmaps/`, and possibly `index.h/` and `index.d/`). Clients before 0.25 ignore them, but a `vykar-server` before 0.25 refuses to start on a data directory that contains them ("contains unexpected entries"). Run `vykar upgrade` again to finish; to stay on the old format instead, remove those directories while `config` is still the old one.
- If a pre-0.25 client still wrote to the repository after the upgrade, commands refuse and name `vykar check --repair`, which rebuilds the index.

New repositories are created in the current format; there is nothing to upgrade.

### Commit mode

A repository is **multi-writer** or **single-writer**, as its `config` records it; `vykar info` shows which. `vykar init` (and `vykar upgrade` from v2/v3) records multi-writer when the storage passes a put-if-absent check, including a concurrent race of creates: backups then commit side by side. Otherwise the repository is single-writer and `vykar init` says why: one writer at a time on this storage — a backup that finishes while another backup commits or a compact runs waits for it (up to 2 hours), and a compact refuses while a backup commits.

- A multi-writer repository refuses every write (backup, compact, prune, snapshot delete, `check --repair`) from a client whose storage does not pass the check (on S3, check `s3_conditional_writes`), before it writes anything but its session marker and the check's small probe objects under `sessions/`. `compact --dry-run` and `compact --report-unreferenced` refuse there too.
- `vykar upgrade --commit-mode single` makes the repository single-writer (no check needed, so such a client can run it); `vykar upgrade --commit-mode multi` makes it multi-writer again, and runs the check first. Both run with no other client active, like the format upgrade, and are refused on an append-only `vykar-server` (they rewrite `config`).
- Development v6 repositories made before the mode was recorded open as single-writer. When the storage offers put-if-absent, `vykar info` and a plain `vykar upgrade` say so and name `vykar upgrade --commit-mode multi`.
- A backup whose machine was suspended for more than 30 minutes before its commit reads `config` again at its commit and commits in the mode it finds there (or refuses, and the next run reuses its uploads).

### Known limits

vykar's protection of concurrent writers has limits it does not close. Most need an unusual stall or a misbehaving service; the first four and the cloned-machine case are reproduced by the protocol simulator (see [Testing](/testing)). Two kinds of damage recur below:

- **A committed snapshot's data drops out of the index.** That snapshot does not restore, and every compact refuses ("compact refused ... run `vykar check --repair`"), until `vykar check --repair` runs; the repair re-indexes the data from the packs, and nothing is lost.
- **The index names data a compact removed.** Nothing refuses: `vykar check` reports packs missing from storage, and a restore that needs them fails. `vykar check --repair` removes the entries. Snapshots committed before are not affected, but a backup made before the repair can reuse the missing data and then does not restore completely. Run `vykar check` after any backup that failed while committing (with "this run's session marker was reaped, lost, or not refreshed in time ...", or with a timeout while writing the index), and repair before the next backup if it reports missing packs.

The limits:

- **Single-writer: a commit that stalls for more than 45 minutes while writing its index.** If the machine of a backup suspends, or its network hangs, for more than 45 minutes in the middle of its index write, other clients take it for crashed. A compact or another backup can then write the next index version, and the stalled write replaces it when the machine resumes. The stalled backup then fails ("this run's session marker was reaped, lost, or not refreshed in time ..."). When the replaced version was a compact's, the index names data that compact removed; when it was another backup's, that backup's snapshot drops out of the index. This cannot happen on a `vykar-server` repository, which refuses the late write.
- **Single-writer: an index write that times out but is applied later.** A storage service that times out a request and applies it afterwards can let an old index version replace a newer one: the snapshot committed in between drops out of the index, or, when a compact ran in between, the index names data it removed. Multi-writer repositories are not affected.
- **Multi-writer: storage whose conditional writes are not atomic.** If you set `s3_conditional_writes: on` for a service that answers vykar's checks correctly but lets two simultaneous creates of one object both succeed, two backups committing at the same moment can lose one of their index updates (one snapshot drops out of the index), and a compact committing beside a backup can leave the index naming data it removed. vykar refuses conditional writes on IDrive e2, the one such service known; for others `on` is your assertion. If in doubt, run `vykar upgrade --commit-mode single`.
- **Both modes: a backup that stalls for more than 45 minutes in the last seconds of its commit.** A backup that reused data of a snapshot deleted meanwhile, and whose machine suspends for more than 45 minutes between writing its index and writing its snapshot while a compact runs elsewhere, can commit a snapshot whose reused data that compact removed. That snapshot does not restore completely, and `vykar check --repair` cannot bring the data back (it removes what is lost). Avoid suspending machines during backups, and prefer running compact when no backup runs.
- **Both modes: a storage request delayed for more than about 40 minutes.** A request that the network or a proxy holds that long and the storage applies afterwards can publish an index that names data a compact removed meanwhile (the second kind of damage above). The backup that sent it fails; run `vykar check`, and `vykar check --repair` removes the entries.
- **Cloned machines during repair or upgrade.** A clone of a machine (a VM clone, a restored disk image) that keeps vykar's installation ID and booted after the original's last session write can take a running `check --repair`, `upgrade` or pack-map write on the original for crashed, and start a backup or compact beside it for a few seconds; run `vykar check` after such an overlap. A backup that commits beside a repair this way, or one that stalls for more than 45 minutes just before writing its snapshot while a repair starts, can lose its index entries to the repair: its snapshot drops out of the index (the first kind of damage above) until the next `vykar check --repair`, which re-indexes them from the packs. Delete `<config dir>/vykar/installation-id` on a clone before it uses the repository.
- **A crashed repair, upgrade or pack-map write blocks for 45 minutes**, unless `vykar clear-sessions` removes its marker on a terminal (see [Remove stale session markers](#remove-stale-session-markers)). Unattended, `vykar clear-sessions --stale` removes it at once only on the machine where it ran (where a command it blocks removes it at once too); run on another machine it does nothing until the 45 minutes have passed.
- **`vykar clear-sessions --stale` leaves `locks/` alone on a repository not yet upgraded.** On a v2/v3 repository the files there can be live (vykar 0.24 locks, the key of a running `vykar upgrade`), so `--stale` removes them only once `config` is of format v6; on a v2/v3 repository, the plain `vykar clear-sessions` removes leftover lock files, and must not run while a 0.24 client or an upgrade runs.
- **Upgrading from v2/v3 beside vykar 0.24 on a machine with the same hostname**: see "0.24 clients with the same hostname" above.
- **`config` is not authenticated.** Someone who can write to the storage can make a multi-writer repository single-writer by editing `config`; the single-writer limits above then apply.

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

Development builds of this release must not write to a repository beside a current build: builds from before exclusive sessions take a repository lock that current builds no longer read, and nothing detects the mix.

## Related pages

- [Server Setup](/server-setup) (server-side compaction)
- [Architecture](/architecture#compact) (compact algorithm details)
