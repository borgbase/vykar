# Testing Improvements TODO

High-ROI small extensions to vykar's existing test machines, ranked by impact per effort.

## 1. Protocol Simulation (`crates/vykar-core/src/sim/`)

- [ ] **Add a concurrent `Restore` command to `driver::Command`**: Currently `Restore` runs only post-case via the checker. Adding a read-only `Restore` step lets DFS and proptest interleave real readers against concurrent `Backup`, `DeleteControl`, and `Compact`, directly exercising Invariant I7 under live GC/repack.
- [ ] **Add an intermediate time jump to `driver::ADVANCES`**: `ADVANCES = [61, 960, 2760, 21660]` leaves a gap between 46 min and 6 h 1 min where `HEAD_FRESHNESS_LIMIT` (50 min), `T_IDX` (1 h), and `COMMIT_MAX_WAIT` (2 h) sit. Add one intermediate jump (e.g. ~70–130 min) so single cases reach those thresholds.
- [ ] **Exercise existing `SimStore` knobs in `sim_explore` / `sim_campaign`**: Wire `batch_delete: true` (S3/REST delete path) into random cases, add an `append_only: true` strategy to verify refusals never wedge or corrupt state, and include `HostPlan::LateClone` where appropriate.
- [ ] **Broaden `driver::FaultSpec` to read/list and pack-delete faults**: Extend `FaultSpec` beyond write faults on 4 key classes to inject `List` `Fail` and `packs/` `Delete` `Fail`/`Delay`, exercising transient S3/SFTP failure paths during commit and compact.
- [ ] **Implement `StaleList` (weak LIST consistency)**: Measure which invariants still hold when `LIST` omits recently written keys, validating the protocol's list-after-write coordination requirement.
- [ ] **Add Invariant I5 to `checker.rs`**: Verify "no removal without a completed mark" directly in the checker rather than relying solely on scripted regressions.

## 2. Fuzzing (`fuzz/` + CI)

- [ ] **Compile-check `fuzz/` on every PR in `.github/workflows/ci.yml`**: Add `cargo check --manifest-path fuzz/Cargo.toml --bins` to the `lint` job so broken fuzz targets fail PR CI instead of waiting for Sunday's weekly run.
- [ ] **Add a `fuzz_sim_driver` target**: Drive `driver::run_case` via `arbitrary::Arbitrary` for coverage-guided exploration of protocol schedules and fault placements, seeded from `crates/vykar-core/tests/sim-regressions/`.
- [ ] **Seed parser fuzzers from `SimStore` artifacts**: Export realistic multi-client `index.h/`, `index.d/`, `snapshots/`, and `sessions/` blobs from simulation runs to enrich the `fuzz/` corpora.

## 3. Corruption Matrix (`crates/vykar-core/src/tests/corruption.rs`)

- [ ] **Add v6 segmented-index corruption variants**: Extend the existing `check` → `repair` → `backup` matrix with `#[test_case]` entries for corrupt/truncated `index.h/` head, corrupt/missing `index.d/` segment or base, and corrupt `sessions/` marker.

## 4. Property Tests (`crates/vykar-core/src/tests/`)

- [ ] **Inject occasional write failures in `segmented_index_consistency.rs`**: Run the random `Store`/`Remove`/`Relocate`/`Sweep`/`Fold`/`Backup`/`Age` sequence over `SimStore` with occasional `Fail` faults to verify failed folds/sweeps never leave in-memory `LiveIndex` diverged from disk.
- [ ] **Add a `proptest` for `prune` retention policies**: Generate arbitrary snapshot timestamp lists and `RetentionConfig` rules to verify idempotence (pruning twice removes nothing the second time) and boundary invariants (e.g. `keep_last >= 1` always keeps the newest snapshot).

## 5. Scenario & Stress Testbench (`scripts/testbench`)

- [ ] **Add a concurrent multi-client YAML scenario**: Spawn two concurrent `vykar backup` processes alongside a `vykar compact` against real MinIO (S3) and `vykar-server` (REST) backends as an end-to-end check that real backend semantics match `SimStore`.
