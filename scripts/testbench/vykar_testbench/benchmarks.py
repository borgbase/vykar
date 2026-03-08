"""Benchmark harness comparing vykar with restic, rustic, borg, kopia.

Merges and replaces:
- scripts/benchmarks/benchmark_runner/cli.py
- scripts/benchmarks/benchmark_runner/defaults.py
- scripts/benchmarks/benchmark_runner/runner.py
- scripts/benchmarks/benchmark_runner/host.py
- scripts/benchmarks/benchmark_runner/tools.py
"""

from __future__ import annotations

import argparse
import os
import random
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from . import bench_report as report
from .vykar import drop_caches as _drop_caches_shared


# ---------------------------------------------------------------------------
# Defaults / Config
# ---------------------------------------------------------------------------

TOOLS = ("vykar", "restic", "rustic", "borg", "kopia")
PHASES = ("backup", "restore")


class ConfigError(ValueError):
    """Raised when benchmark configuration is invalid."""


@dataclass(frozen=True)
class BenchmarkConfig:
    runs: int
    dataset_dir: Path
    selected_tools: tuple[str, ...]
    selected_tool_arg: str | None
    repo_root: Path
    runtime_root: Path
    out_root: Path
    logs_dir: Path
    passphrase: str
    user: str
    vykar_repo: Path
    restic_repo: Path
    rustic_repo: Path
    borg_repo: Path
    kopia_repo: Path
    kopia_config: Path
    kopia_cache: Path
    vykar_config_path: Path
    restore_dirs: dict[str, Path] = field(default_factory=dict)

    @property
    def dataset_snapshot1(self) -> Path:
        return self.dataset_dir / "snapshot-1"

    @property
    def dataset_snapshot2(self) -> Path:
        return self.dataset_dir / "snapshot-2"

    @property
    def dataset_benchmark(self) -> Path:
        return self.dataset_dir

    def repo_dir_for_tool(self, tool: str) -> Path:
        return {
            "vykar": self.vykar_repo,
            "restic": self.restic_repo,
            "rustic": self.rustic_repo,
            "borg": self.borg_repo,
            "kopia": self.kopia_repo,
        }[tool]

    def restore_dir_for_tool(self, tool: str) -> Path:
        return self.restore_dirs[tool]


def default_dataset_dir() -> Path:
    return Path.home() / "corpus-local"


def resolve_selected_tools(tool: str | None) -> tuple[str, ...]:
    if tool is None:
        return TOOLS
    if tool not in TOOLS:
        raise ConfigError(f"--tool must be one of: {', '.join(TOOLS)}")
    return (tool,)


def ensure_required_commands(selected_tools: tuple[str, ...]) -> None:
    if not Path("/usr/bin/time").exists():
        raise ConfigError("missing required command: /usr/bin/time")
    for tool in selected_tools:
        if shutil.which(tool) is None:
            raise ConfigError(f"missing required command: {tool}")


def build_config(*, runs: int, tool: str | None, dataset: str | None) -> BenchmarkConfig:
    if runs <= 0:
        raise ConfigError("--runs must be a positive integer")

    dataset_dir = Path(dataset).expanduser() if dataset else default_dataset_dir()
    dataset_dir = dataset_dir.resolve()
    if not dataset_dir.is_dir():
        raise ConfigError(f"dataset dir not found: {dataset_dir}")
    if not (dataset_dir / "snapshot-1").is_dir():
        raise ConfigError(f"missing required seed folder: {dataset_dir / 'snapshot-1'}")
    if not (dataset_dir / "snapshot-2").is_dir():
        raise ConfigError(f"missing required benchmark folder: {dataset_dir / 'snapshot-2'}")

    selected_tools = resolve_selected_tools(tool)
    ensure_required_commands(selected_tools)

    repo_root = Path(os.environ.get("REPO_ROOT", "/mnt/repos")).expanduser().resolve()
    runtime_root = Path(os.environ.get("RUNTIME_ROOT", str(Path.home() / "runtime"))).expanduser().resolve()
    passphrase = os.environ.get("PASSPHRASE", "123")
    user = os.environ.get("USER", "unknown")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_root = runtime_root / "benchmarks" / stamp
    logs_dir = out_root / "logs"

    return BenchmarkConfig(
        runs=runs,
        dataset_dir=dataset_dir,
        selected_tools=selected_tools,
        selected_tool_arg=tool,
        repo_root=repo_root,
        runtime_root=runtime_root,
        out_root=out_root,
        logs_dir=logs_dir,
        passphrase=passphrase,
        user=user,
        vykar_repo=repo_root / "bench-vykar",
        restic_repo=repo_root / "bench-restic",
        rustic_repo=repo_root / "bench-rustic",
        borg_repo=repo_root / "bench-borg",
        kopia_repo=repo_root / "bench-kopia",
        kopia_config=out_root / "kopia.repository.config",
        kopia_cache=out_root / "kopia-cache",
        vykar_config_path=out_root / "vykar.bench.yaml",
        restore_dirs={
            "vykar": out_root / "restore-vykar",
            "restic": out_root / "restore-restic",
            "rustic": out_root / "restore-rustic",
            "borg": out_root / "restore-borg",
            "kopia": out_root / "restore-kopia",
        },
    )


# ---------------------------------------------------------------------------
# Host helpers
# ---------------------------------------------------------------------------


def append_log(log_path: Path, message: str) -> None:
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"{message}\n")


def _capture_text(argv: list[str]) -> str:
    try:
        return subprocess.check_output(argv, text=True, stderr=subprocess.DEVNULL).strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        return ""


def mount_target_for_path(path: Path) -> str | None:
    if subprocess.run(["bash", "-lc", "command -v findmnt >/dev/null 2>&1"], check=False).returncode == 0:
        out = _capture_text(["findmnt", "-n", "-o", "TARGET", "--target", str(path)])
        return out.splitlines()[0] if out else None
    out = _capture_text(["df", "-P", str(path)])
    lines = out.splitlines()
    if len(lines) < 2:
        return None
    parts = lines[1].split()
    return parts[5] if len(parts) >= 6 else None


def mount_source_for_path(path: Path) -> str | None:
    if subprocess.run(["bash", "-lc", "command -v findmnt >/dev/null 2>&1"], check=False).returncode == 0:
        out = _capture_text(["findmnt", "-n", "-o", "SOURCE", "--target", str(path)])
        return out.splitlines()[0] if out else None
    out = _capture_text(["df", "-P", str(path)])
    lines = out.splitlines()
    if len(lines) < 2:
        return None
    parts = lines[1].split()
    return parts[0] if parts else None


def nvme_namespace_for_source(source: str | None) -> str | None:
    if not source:
        return None
    if source.startswith("/dev/nvme") and "p" in source:
        return source.rsplit("p", 1)[0]
    if source.startswith("/dev/nvme"):
        return source
    return None


def build_storage_settle_targets(paths: list[Path]) -> tuple[list[str], list[str]]:
    trim_mounts: list[str] = []
    nvme_devices: list[str] = []
    for path in paths:
        mount = mount_target_for_path(path)
        if mount and mount not in trim_mounts:
            trim_mounts.append(mount)
        source = mount_source_for_path(path)
        nvme_device = nvme_namespace_for_source(source)
        if nvme_device and nvme_device not in nvme_devices:
            nvme_devices.append(nvme_device)
    return trim_mounts, nvme_devices


def _run_logged(argv: list[str], log_path: Path) -> bool:
    with log_path.open("a", encoding="utf-8") as handle:
        rc = subprocess.run(argv, stdout=handle, stderr=handle, check=False).returncode
    return rc == 0


def run_storage_settle(log_path: Path, trim_mounts: list[str], nvme_devices: list[str]) -> None:
    append_log(log_path, "[measure-prepare] sync storage")
    _run_logged(["sync"], log_path)

    if trim_mounts:
        for mount in trim_mounts:
            if not _run_logged(["sudo", "-n", "fstrim", "-v", mount], log_path):
                append_log(log_path, f"[measure-prepare] fstrim skipped/failed mount={mount}")
    else:
        append_log(log_path, "[measure-prepare] fstrim skipped (no mount targets resolved)")

    if subprocess.run(["bash", "-lc", "command -v nvme >/dev/null 2>&1"], check=False).returncode == 0:
        if nvme_devices:
            for dev in nvme_devices:
                if not _run_logged(["sudo", "-n", "nvme", "flush", dev], log_path):
                    append_log(log_path, f"[measure-prepare] nvme flush skipped/failed device={dev}")
        else:
            append_log(log_path, "[measure-prepare] nvme flush skipped (no nvme devices resolved)")
    else:
        append_log(log_path, "[measure-prepare] nvme cli missing; skipping nvme flush")

    append_log(log_path, "[measure-prepare] cooldown sleep 20s")
    time.sleep(20)


def drop_caches(log_path: Path) -> None:
    _run_logged(["sync"], log_path)
    if not Path("/proc/sys/vm/drop_caches").exists():
        return
    if subprocess.run(["sudo", "-n", "true"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode != 0:
        append_log(log_path, "drop_caches: passwordless sudo unavailable; skipping")
        return
    with log_path.open("a", encoding="utf-8") as handle:
        subprocess.run(
            ["sudo", "-n", "tee", "/proc/sys/vm/drop_caches"],
            input="3\n",
            text=True,
            stdout=subprocess.DEVNULL,
            stderr=handle,
            check=False,
        )


# ---------------------------------------------------------------------------
# Tool-specific commands
# ---------------------------------------------------------------------------


@dataclass
class CommandSpec:
    argv: list[str]
    cwd: Path | None = None
    env_overrides: dict[str, str] = field(default_factory=dict)


@dataclass
class RunState:
    vykar_restore_snapshot: str = ""
    borg_backup_archive: str = ""
    borg_restore_archive: str = ""


def write_vykar_config(config: BenchmarkConfig) -> None:
    config.vykar_config_path.write_text(
        "\n".join(
            [
                "repositories:",
                f'  - url: "{config.vykar_repo}"',
                "    label: bench",
                "compression:",
                "  algorithm: zstd",
                "",
            ]
        ),
        encoding="utf-8",
    )
    os.chmod(config.vykar_config_path, 0o600)


def make_base_env(config: BenchmarkConfig) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "VYKAR_CONFIG": str(config.vykar_config_path),
            "VYKAR_PASSPHRASE": config.passphrase,
            "RESTIC_REPOSITORY": str(config.restic_repo),
            "RESTIC_PASSWORD": config.passphrase,
            "RUSTIC_REPOSITORY": str(config.rustic_repo),
            "RUSTIC_PASSWORD": config.passphrase,
            "BORG_REPO": str(config.borg_repo),
            "BORG_PASSPHRASE": config.passphrase,
            "KOPIA_PASSWORD": config.passphrase,
        }
    )
    return env


def _run(argv: list[str], env: dict[str, str], log_path: Path | None = None, cwd: Path | None = None) -> int:
    stdout = stderr = subprocess.DEVNULL
    if log_path is not None:
        handle = log_path.open("a", encoding="utf-8")
        try:
            return subprocess.run(argv, env=env, cwd=cwd, stdout=handle, stderr=handle, check=False).returncode
        finally:
            handle.close()
    return subprocess.run(argv, env=env, cwd=cwd, stdout=stdout, stderr=stderr, check=False).returncode


def clear_dir_contents(path: Path, log_path: Path) -> bool:
    path.mkdir(parents=True, exist_ok=True)
    okay = True
    for child in path.iterdir():
        try:
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()
        except FileNotFoundError:
            continue
        except OSError:
            okay = False
    if not okay:
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(f"[measure-prepare] failed to clean restore dir: {path}\n")
    return okay


def reset_repo_for_tool(tool: str, config: BenchmarkConfig, log_path: Path) -> int:
    repo = config.repo_dir_for_tool(tool)
    if _run(["sudo", "-n", "rm", "-rf", str(repo)], os.environ.copy(), log_path) != 0:
        return 1
    if _run(["sudo", "-n", "mkdir", "-p", str(repo)], os.environ.copy(), log_path) != 0:
        return 1
    if _run(["sudo", "-n", "chown", "-R", f"{config.user}:{config.user}", str(repo)], os.environ.copy(), log_path) != 0:
        return 1
    if tool == "kopia":
        if config.kopia_config.exists():
            config.kopia_config.unlink()
        if config.kopia_cache.exists():
            shutil.rmtree(config.kopia_cache)
        config.kopia_cache.mkdir(parents=True, exist_ok=True)
    return 0


def cleanup_repo_for_tool(tool: str, config: BenchmarkConfig, log_path: Path | None = None) -> int:
    repo = config.repo_dir_for_tool(tool)
    env = os.environ.copy()
    if _run(["sudo", "-n", "rm", "-rf", str(repo)], env, log_path) != 0:
        return 1
    if _run(["sudo", "-n", "mkdir", "-p", str(repo)], env, log_path) != 0:
        return 1
    return _run(["sudo", "-n", "chown", "-R", f"{config.user}:{config.user}", str(repo)], env, log_path)


def cleanup_restore_for_tool(tool: str, config: BenchmarkConfig) -> None:
    restore_dir = config.restore_dir_for_tool(tool)
    if restore_dir.exists():
        shutil.rmtree(restore_dir)
    restore_dir.mkdir(parents=True, exist_ok=True)


def init_repo_for_tool(tool: str, config: BenchmarkConfig) -> CommandSpec:
    if tool == "vykar":
        return CommandSpec(["vykar", "init", "-R", "bench"])
    if tool == "restic":
        return CommandSpec(["restic", "init"])
    if tool == "rustic":
        return CommandSpec(["rustic", "init"])
    if tool == "borg":
        return CommandSpec(["borg", "init", "--encryption=repokey-blake2"])
    return CommandSpec(
        [
            "kopia", "--config-file", str(config.kopia_config),
            "repository", "create", "filesystem",
            f"--path={config.kopia_repo}",
            f"--cache-directory={config.kopia_cache}",
        ]
    )


def post_init_for_tool(tool: str, config: BenchmarkConfig) -> CommandSpec | None:
    if tool != "kopia":
        return None
    return CommandSpec(
        [
            "kopia", "--config-file", str(config.kopia_config),
            "policy", "set", "--global", "--compression=zstd",
        ]
    )


def seed_backup_for_tool(tool: str, config: BenchmarkConfig) -> CommandSpec:
    src = str(config.dataset_snapshot1)
    if tool == "vykar":
        return CommandSpec(["vykar", "backup", "-R", "bench", "-l", "bench", src])
    if tool == "restic":
        return CommandSpec(["restic", "backup", src])
    if tool == "rustic":
        return CommandSpec(["rustic", "backup", src])
    if tool == "borg":
        return CommandSpec(["borg", "create", "--compression", "zstd,3", "::bench-seed", src])
    return CommandSpec(["kopia", "--config-file", str(config.kopia_config), "snapshot", "create", src])


def resolve_latest_vykar_snapshot(config: BenchmarkConfig, env: dict[str, str]) -> str | None:
    proc = subprocess.run(
        ["vykar", "list", "-R", "bench", "--last", "1"],
        env=env, text=True, capture_output=True, check=False,
    )
    if proc.returncode != 0:
        return None
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    if len(lines) < 2:
        return None
    return lines[1].split()[0]


def resolve_latest_borg_archive(env: dict[str, str]) -> str | None:
    proc = subprocess.run(
        ["borg", "list", "--short"],
        env=env, text=True, capture_output=True, check=False,
    )
    if proc.returncode != 0:
        return None
    lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    return lines[-1] if lines else None


def measurement_command(tool: str, phase: str, config: BenchmarkConfig, state: RunState) -> CommandSpec:
    dataset = str(config.dataset_benchmark)
    restore_dir = str(config.restore_dir_for_tool(tool))
    if tool == "vykar" and phase == "backup":
        return CommandSpec(["vykar", "backup", "-R", "bench", "-l", "bench", dataset])
    if tool == "vykar" and phase == "restore":
        return CommandSpec(["vykar", "restore", "-R", "bench", state.vykar_restore_snapshot, restore_dir])
    if tool == "restic" and phase == "backup":
        return CommandSpec(["restic", "backup", dataset])
    if tool == "restic" and phase == "restore":
        return CommandSpec(["restic", "restore", "latest", "--target", restore_dir])
    if tool == "rustic" and phase == "backup":
        return CommandSpec(["rustic", "backup", dataset])
    if tool == "rustic" and phase == "restore":
        return CommandSpec(["rustic", "restore", "latest", restore_dir])
    if tool == "borg" and phase == "backup":
        return CommandSpec(["borg", "create", "--compression", "zstd,3", f"::{state.borg_backup_archive}", dataset])
    if tool == "borg" and phase == "restore":
        return CommandSpec(["borg", "extract", f"::{state.borg_restore_archive}"], cwd=config.restore_dir_for_tool(tool))
    if tool == "kopia" and phase == "backup":
        return CommandSpec(["kopia", "--config-file", str(config.kopia_config), "snapshot", "create", dataset])
    return CommandSpec(
        [
            "kopia", "--config-file", str(config.kopia_config),
            "snapshot", "restore", dataset, restore_dir,
            "--snapshot-time", "latest",
        ]
    )


def commands_manifest_entries(config: BenchmarkConfig) -> list[str]:
    entries: list[str] = []
    for tool in config.selected_tools:
        for phase in ("backup", "restore"):
            op = f"{tool}_{phase}"
            entries.append(f"{op}: {' '.join(measurement_command(tool, phase, config, RunState()).argv)}")
    return entries


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


@dataclass
class RunResult:
    failed_runs: int = 0


def _run_command(spec_argv: list[str], *, env: dict[str, str], stdout_path: Path | None = None, stderr_path: Path | None = None, cwd: Path | None = None) -> int:
    stdout = subprocess.DEVNULL
    stderr = subprocess.DEVNULL
    stdout_handle = None
    stderr_handle = None
    try:
        if stdout_path is not None:
            stdout_handle = stdout_path.open("w", encoding="utf-8")
            stdout = stdout_handle
        if stderr_path is not None:
            stderr_handle = stderr_path.open("w", encoding="utf-8")
            stderr = stderr_handle
        return subprocess.run(spec_argv, env=env, cwd=cwd, stdout=stdout, stderr=stderr, check=False).returncode
    finally:
        if stdout_handle is not None:
            stdout_handle.close()
        if stderr_handle is not None:
            stderr_handle.close()


def _append_command(spec_argv: list[str], *, env: dict[str, str], log_path: Path, cwd: Path | None = None) -> int:
    with log_path.open("a", encoding="utf-8") as handle:
        return subprocess.run(spec_argv, env=env, cwd=cwd, stdout=handle, stderr=handle, check=False).returncode


def _ensure_out_dirs(config: BenchmarkConfig) -> None:
    config.logs_dir.mkdir(parents=True, exist_ok=True)
    config.out_root.mkdir(parents=True, exist_ok=True)
    for restore_dir in config.restore_dirs.values():
        restore_dir.mkdir(parents=True, exist_ok=True)


def _write_commands_manifest(config: BenchmarkConfig) -> None:
    lines = [
        "workflow: per run => reset repo + init + untimed backup snapshot-1 + storage settle(sync/fstrim/nvme flush + 20s) + drop caches",
        "restore workflow: no repo prep; restore uses state from preceding timed <tool>_backup op",
    ]
    lines.extend(commands_manifest_entries(config))
    (config.out_root / "commands.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_readme(config: BenchmarkConfig) -> None:
    selected_tool = config.selected_tool_arg or "all"
    (config.out_root / "README.txt").write_text(
        "\n".join(
            [
                f"Benchmark run: {config.out_root.name}",
                f"Dataset root: {config.dataset_dir}",
                f"Seed snapshot (untimed): {config.dataset_snapshot1}",
                f"Benchmark dataset (timed): {config.dataset_benchmark}",
                f"Runs per benchmark: {config.runs}",
                f"Selected tool: {selected_tool}",
                "",
                "Workflow per run:",
                "1) reset/init tool repo",
                "2) untimed backup of snapshot-1",
                "3) storage settle (sync + fstrim + nvme flush + cooldown) + drop caches",
                "4) timed benchmark step:",
                "   - backup ops: backup top-level dataset (snapshot-1 + snapshot-2)",
                "   - restore ops: timed restore of latest from preceding timed backup op state",
                "",
                "Outputs:",
                "- commands.txt / repo-sizes.txt",
                "- profile.<op>/runs/run-*.timev.txt",
                "- profile.<op>/runs/run-*.repo-size-bytes.txt",
                "- reports/summary.{tsv,md,json}",
                "- reports/benchmark.summary.png",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _write_meta(config: BenchmarkConfig, op_dir: Path, tool: str, phase: str, timed_argv: list[str]) -> None:
    op_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.strptime(config.out_root.name, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    meta = [
        f"name={tool}_{phase}",
        f"dataset={config.dataset_dir}",
        f"dataset_snapshot_1={config.dataset_snapshot1}",
        f"dataset_snapshot_2={config.dataset_snapshot2}",
        f"dataset_benchmark={config.dataset_benchmark}",
        f"timed_cmd={' '.join(timed_argv)}",
        f"runs={config.runs}",
        "warmup_runs=0",
        f"timestamp_utc={stamp.strftime('%Y-%m-%dT%H:%M:%SZ')}",
    ]
    (op_dir / "meta.txt").write_text("\n".join(meta) + "\n", encoding="utf-8")


def _write_repo_size_bytes(config: BenchmarkConfig, tool: str, out_file: Path) -> None:
    repo = config.repo_dir_for_tool(tool)
    value = "NA"
    if repo.exists():
        proc = subprocess.run(["du", "-sb", str(repo)], text=True, capture_output=True, check=False)
        if proc.returncode == 0:
            first = proc.stdout.split()
            if first and first[0].isdigit():
                value = first[0]
    out_file.write_text(f"{value}\n", encoding="utf-8")


def _prepare_backup(tool: str, config: BenchmarkConfig, env: dict[str, str], prep_log: Path) -> bool:
    append_log(prep_log, f"[prepare] op={tool}_backup")
    append_log(prep_log, "[prepare] reset repo")
    if reset_repo_for_tool(tool, config, prep_log) != 0:
        return False
    append_log(prep_log, "[prepare] init repo")
    if _append_command(init_repo_for_tool(tool, config).argv, env=env, log_path=prep_log) != 0:
        return False
    post_init = post_init_for_tool(tool, config)
    if post_init is not None:
        append_log(prep_log, "[prepare] post-init config")
        if _append_command(post_init.argv, env=env, log_path=prep_log) != 0:
            return False
    append_log(prep_log, "[prepare] seed backup snapshot-1 (untimed)")
    return _append_command(seed_backup_for_tool(tool, config).argv, env=env, log_path=prep_log) == 0


def _prepare_measurement(tool: str, phase: str, config: BenchmarkConfig, env: dict[str, str], log_path: Path, trim_mounts: list[str], nvme_devices: list[str], state: RunState) -> bool:
    append_log(log_path, f"[measure-prepare] op={tool}_{phase}")
    state.vykar_restore_snapshot = ""
    state.borg_backup_archive = ""
    state.borg_restore_archive = ""

    if phase == "restore":
        restore_dir = config.restore_dir_for_tool(tool)
        append_log(log_path, f"[measure-prepare] clean restore dir: {restore_dir}")
        cleanup_restore_for_tool(tool, config)
        if tool == "vykar":
            snapshot = resolve_latest_vykar_snapshot(config, env)
            if not snapshot:
                append_log(log_path, "[measure-prepare] failed to resolve latest vykar snapshot")
                return False
            state.vykar_restore_snapshot = snapshot
            append_log(log_path, f"[measure-prepare] vykar snapshot={snapshot}")
        elif tool == "borg":
            archive = resolve_latest_borg_archive(env)
            if not archive:
                append_log(log_path, "[measure-prepare] failed to resolve latest borg archive")
                return False
            state.borg_restore_archive = archive
            append_log(log_path, f"[measure-prepare] borg archive={archive}")
    elif tool == "borg":
        state.borg_backup_archive = f"bench-{config.out_root.name}-{random.randint(1000, 999999)}"
        append_log(log_path, f"[measure-prepare] borg archive={state.borg_backup_archive}")

    run_storage_settle(log_path, trim_mounts, nvme_devices)
    append_log(log_path, "[measure-prepare] drop caches")
    drop_caches(log_path)
    return True


def _timed_run(tool: str, phase: str, config: BenchmarkConfig, env: dict[str, str], op_dir: Path, run_idx: int, trim_mounts: list[str], nvme_devices: list[str]) -> int:
    run_label = f"{run_idx:03d}"
    runs_dir = op_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    prep_log = runs_dir / f"run-{run_label}.prep.log"
    measure_prep_log = runs_dir / f"run-{run_label}.measure-prep.log"
    stdout_path = runs_dir / f"run-{run_label}.stdout.txt"
    timev_path = runs_dir / f"run-{run_label}.timev.txt"
    rc_path = runs_dir / f"run-{run_label}.rc"
    repo_size_path = runs_dir / f"run-{run_label}.repo-size-bytes.txt"
    state = RunState()

    if phase == "backup" and not _prepare_backup(tool, config, env, prep_log):
        timev_path.write_text("", encoding="utf-8")
        rc_path.write_text("1\n", encoding="utf-8")
        _write_repo_size_bytes(config, tool, repo_size_path)
        return 1

    if not _prepare_measurement(tool, phase, config, env, measure_prep_log, trim_mounts, nvme_devices, state):
        timev_path.write_text("", encoding="utf-8")
        rc_path.write_text("1\n", encoding="utf-8")
        _write_repo_size_bytes(config, tool, repo_size_path)
        return 1

    command = measurement_command(tool, phase, config, state)
    argv = ["/usr/bin/time", "-v", *command.argv]
    rc = _run_command(argv, env=env, stdout_path=stdout_path, stderr_path=timev_path, cwd=command.cwd)
    rc_path.write_text(f"{rc}\n", encoding="utf-8")
    _write_repo_size_bytes(config, tool, repo_size_path)
    return rc


def _write_status(op_dir: Path, config: BenchmarkConfig, failures: int) -> None:
    status_path = op_dir / "status.txt"
    if failures == 0:
        text = f"OK (time -v runs={config.runs} warmups=0 failed_warmups=0)\n"
    else:
        text = f"FAILED (time -v failed_runs={failures}/{config.runs} failed_warmups=0/0)\n"
    status_path.write_text(text, encoding="utf-8")


def _collect_repo_sizes(config: BenchmarkConfig) -> None:
    lines: list[str] = []
    for tool in ("vykar", "restic", "rustic", "borg", "kopia"):
        repo = config.repo_dir_for_tool(tool)
        proc = subprocess.run(["du", "-sh", str(repo)], text=True, capture_output=True, check=False)
        output = proc.stdout.strip() if proc.returncode == 0 else f"NA\t{repo}"
        lines.append(output)
    (config.out_root / "repo-sizes.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_tool_stats(config: BenchmarkConfig, env: dict[str, str]) -> None:
    commands = {
        "vykar.info.txt": [["vykar", "info", "-R", "bench"]],
        "restic.stats.txt": [["restic", "snapshots"], ["restic", "stats", "--mode", "raw-data"]],
        "rustic.stats.txt": [["rustic", "snapshots"], ["rustic", "stats"]],
        "borg.stats.txt": [["borg", "info"], ["borg", "list"]],
        "kopia.stats.txt": [
            ["kopia", "--config-file", str(config.kopia_config), "repository", "status"],
            ["kopia", "--config-file", str(config.kopia_config), "snapshot", "list"],
            ["kopia", "--config-file", str(config.kopia_config), "content", "stats"],
        ],
    }
    for filename, groups in commands.items():
        out_path = config.out_root / filename
        with out_path.open("w", encoding="utf-8") as handle:
            for argv in groups:
                handle.write(f"== {' '.join(argv)} ==\n")
                proc = subprocess.run(argv, env=env, text=True, capture_output=True, check=False)
                if proc.stdout:
                    handle.write(proc.stdout)
                    if not proc.stdout.endswith("\n"):
                        handle.write("\n")
                if proc.stderr:
                    handle.write(proc.stderr)
                    if not proc.stderr.endswith("\n"):
                        handle.write("\n")
                handle.write("\n")


def list_previous_run_roots(config: BenchmarkConfig) -> list[str]:
    base = config.runtime_root / "benchmarks"
    if not base.is_dir():
        return []
    roots: list[str] = []
    for child in sorted(base.iterdir(), reverse=True):
        if not child.is_dir():
            continue
        name = child.name
        if len(name) != 16 or name[8] != "T" or not name.endswith("Z"):
            continue
        if name >= config.out_root.name:
            continue
        roots.append(str(child))
    return roots


def _run_report(config: BenchmarkConfig) -> int:
    argv: list[str] = ["all", str(config.out_root), "--out-dir", str(config.out_root / "reports")]
    if config.selected_tool_arg is not None:
        previous = list_previous_run_roots(config)
        for root in previous:
            argv.extend(["--backfill-root", root])
        if previous:
            argv.extend(["--backfill-mode", "nonselected", "--selected-tool", config.selected_tool_arg])
    return report.main(argv)


def _cleanup_transient_dirs(config: BenchmarkConfig) -> None:
    for restore_dir in config.restore_dirs.values():
        if restore_dir.exists():
            shutil.rmtree(restore_dir, ignore_errors=True)
    if config.kopia_cache.exists():
        shutil.rmtree(config.kopia_cache, ignore_errors=True)


def run_benchmarks(config: BenchmarkConfig) -> int:
    _ensure_out_dirs(config)
    write_vykar_config(config)
    env = make_base_env(config)
    trim_mounts, nvme_devices = build_storage_settle_targets([config.repo_root, config.out_root])
    _write_commands_manifest(config)
    _write_readme(config)

    print(f"[config] dataset={config.dataset_dir} runs={config.runs} tool={config.selected_tool_arg or 'all'}")
    print(f"[dataset] seed={config.dataset_snapshot1}")
    print(f"[dataset] benchmark={config.dataset_benchmark}")

    try:
        for tool in config.selected_tools:
            for phase in ("backup", "restore"):
                op_dir = config.out_root / f"profile.{tool}_{phase}"
                timed_preview = measurement_command(tool, phase, config, RunState()).argv
                _write_meta(config, op_dir, tool, phase, timed_preview)

                failures = 0
                for run_idx in range(1, config.runs + 1):
                    if phase == "restore":
                        print(f"[run] {tool}_{phase} {run_idx}/{config.runs} (no prep; uses preceding timed backup state)")
                    else:
                        print(f"[run] {tool}_{phase} {run_idx}/{config.runs}")
                    rc = _timed_run(tool, phase, config, env, op_dir, run_idx, trim_mounts, nvme_devices)
                    if rc != 0:
                        failures += 1

                _write_status(op_dir, config, failures)
                if phase == "restore":
                    cleanup_repo_for_tool(tool, config)
                    cleanup_restore_for_tool(tool, config)

        _collect_repo_sizes(config)
        _write_tool_stats(config, env)
        rc = _run_report(config)
    finally:
        _cleanup_transient_dirs(config)

    if rc == 0:
        print(f"OK: results in {config.out_root}")
    return rc


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="benchmark",
        description="Reproducible benchmark harness for vykar vs restic vs rustic vs borg vs kopia.",
    )
    parser.add_argument("--runs", type=int, required=True, help="timed runs per operation")
    parser.add_argument("--tool", choices=TOOLS, default=None, help="limit to a single tool")
    parser.add_argument("--dataset", default=None, help="dataset directory (default: ~/corpus-local)")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = build_config(runs=args.runs, tool=args.tool, dataset=args.dataset)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return run_benchmarks(config)
