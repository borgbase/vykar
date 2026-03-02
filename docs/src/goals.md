# Design Goals

Vykar synthesizes the best ideas from a decade of backup tool development into a single Rust binary. These are the principles behind its design.


## One tool, not an assembly

Configuration, scheduling, monitoring, hooks, and health checks belong in the backup tool itself — not in a constellation of wrappers and scripts bolted on after the fact.


## Config-first

Your entire backup strategy lives in a single YAML file that can be version-controlled, reviewed, and deployed across machines. A repository path and a list of sources is enough to get going.

```yaml
repositories:
  - url: /backups/myrepo
sources:
  - path: /home/user/documents
  - path: /home/user/photos
```


## Universal primitives over specific integrations

Vykar doesn't have dedicated flags for specific databases or services. Instead, hooks and command dumps let you capture the output of any command — the same mechanism works for every database, container, or workflow.

```yaml
sources:
  - path: /var/backups/db
    label: databases
    hooks:
      before: "pg_dump -Fc mydb > /var/backups/db/mydb.dump"
      after:  "rm -f /var/backups/db/mydb.dump"
```


## Labels, not naming schemes

Snapshots get auto-generated IDs. Labels like `personal` or `databases` represent what you're backing up and group snapshots for retention, filtering, and restore — without requiring unique names or opaque hashes.

```bash
vykar list -S databases --last 5
vykar restore --source personal latest
```


## Encryption by default

Encryption is always on. Vykar auto-selects AES-256-GCM or ChaCha20-Poly1305 based on hardware support. Chunk IDs use keyed hashing to prevent content fingerprinting against the repository.


## The repository is untrusted

All data is encrypted and authenticated before it leaves the client. The optional REST server enforces append-only access and quotas, so even a compromised client cannot delete historical backups.


## Browse without dependencies

`vykar mount` starts a built-in WebDAV server and web interface. Browse and restore snapshots from any browser or file manager — on any platform, in containers, with zero external dependencies.


## Performance through Rust

No GIL bottleneck, no garbage collection pauses, predictable memory usage. FastCDC chunking, parallel compression, and streaming uploads keep the pipeline saturated. Built-in rate limiting for CPU, disk I/O, and network lets Vykar run during business hours.


## Discoverability in the CLI

Common operations are short top-level commands. Everything targeting a specific snapshot lives under `vykar snapshot`. Flags are consistent everywhere: `-R` is always a repository, `-S` is always a source label.

```bash
vykar backup
vykar list
vykar snapshot find -name "*.xlsx"
vykar snapshot diff a3f7c2 b8d4e1
```


## No lock-in

The repository format is documented, the source is open under GPL-3.0 license, and the REST server is optional. The config is plain YAML with no proprietary syntax.
