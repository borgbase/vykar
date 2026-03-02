# Maintenance

## Delete a snapshot

```bash
# Delete a specific snapshot by ID
vykar snapshot delete a1b2c3d4
```

## Delete a repository

Permanently delete an entire repository and all its snapshots.

```bash
# Interactive confirmation (prompts you to type "delete")
vykar delete

# Non-interactive (for scripting)
vykar delete --yes-delete-this-repo
```

## Prune old snapshots

Apply the retention policy defined in your configuration to remove expired snapshots. Optionally `compact` the repository after pruning.

```bash
vykar prune --compact
```

## Verify repository integrity

```bash
# Structural integrity check
vykar check

# Full data verification (reads and verifies every chunk)
vykar check --verify-data
```

## Compact (reclaim space)

After `delete` or `prune`, blob data remains in pack files. Run `compact` to rewrite packs and reclaim disk space.

```bash
# Preview what would be repacked
vykar compact --dry-run

# Repack to reclaim space
vykar compact
```

## Related pages

- [Quick Start](quickstart.md)
- [Server Setup](server-setup.md) (server-side compaction)
- [Architecture](architecture.md) (compact algorithm details)
