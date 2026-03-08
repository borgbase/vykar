"""Shared defaults, config generation, and backend setup.

Merges and replaces:
- scripts/lib/defaults.sh
- scripts/lib/vykar-repo.sh (write_vykar_config, resolve_repo_url, reset_minio, ensure_s3_bucket)
- scripts/scenarios/scenario_runner/config.py
"""

import os
import subprocess
import time
from dataclasses import dataclass
from urllib.parse import urlparse

import yaml


@dataclass(frozen=True)
class Defaults:
    """Environment-based defaults matching scripts/lib/defaults.sh."""

    # Paths
    repo_root: str
    corpus_local: str
    corpus_remote: str
    runtime_root: str
    passphrase: str

    # REST backend
    rest_url: str
    rest_token: str
    rest_data_dir: str

    # S3 / MinIO
    s3_region: str
    s3_access_key: str
    s3_secret_key: str
    minio_service: str
    minio_data_dir: str
    minio_health_url: str

    # SFTP
    sftp_host: str
    sftp_port: str
    sftp_user: str
    sftp_base_dir: str
    sftp_key: str
    sftp_known_hosts: str
    sftp_max_connections: str


def load_defaults() -> Defaults:
    """Load defaults from environment variables with hardcoded fallbacks."""
    return Defaults(
        repo_root=os.environ.get("REPO_ROOT", "/mnt/repos"),
        corpus_local=os.environ.get("CORPUS_LOCAL", os.path.expanduser("~/corpus-local")),
        corpus_remote=os.environ.get("CORPUS_REMOTE", os.path.expanduser("~/corpus-remote")),
        runtime_root=os.environ.get("RUNTIME_ROOT", os.path.expanduser("~/runtime")),
        passphrase=os.environ.get("PASSPHRASE", "123"),
        rest_url=os.environ.get("REST_URL", "http://127.0.0.1:8585"),
        rest_token=os.environ.get(
            "REST_TOKEN",
            os.environ.get(
                "VYKAR_REST_TOKEN",
                os.environ.get(
                    "VYKAR_TOKEN",
                    os.environ.get("VGER_TOKEN", "vger-e2e-local-token"),
                ),
            ),
        ),
        rest_data_dir=os.environ.get("REST_DATA_DIR", "/mnt/repos/bench-vykar/vykar-server-data"),
        s3_region=os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1")),
        s3_access_key=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        s3_secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        minio_service=os.environ.get("MINIO_SERVICE", "minio.service"),
        minio_data_dir=os.environ.get("MINIO_DATA_DIR", "/mnt/repos/bench-vykar/minio-data"),
        minio_health_url=os.environ.get("MINIO_HEALTH_URL", "http://127.0.0.1:9000/minio/health/live"),
        sftp_host=os.environ.get("SFTP_HOST", "127.0.0.1"),
        sftp_port=os.environ.get("SFTP_PORT", "22"),
        sftp_user=os.environ.get("SFTP_USER", os.environ.get("USER", "root")),
        sftp_base_dir=os.environ.get("SFTP_BASE_DIR", "/mnt/repos"),
        sftp_key=os.environ.get("SFTP_KEY", os.path.expanduser("~/.ssh/id_ed25519")),
        sftp_known_hosts=os.environ.get("SFTP_KNOWN_HOSTS", ""),
        sftp_max_connections=os.environ.get("SFTP_MAX_CONNECTIONS", ""),
    )


def resolve_repo_url(backend: str, repo_label: str, defaults: Defaults | None = None) -> str:
    """Compute the repository URL for the given backend."""
    if defaults is None:
        defaults = load_defaults()

    if backend == "local":
        return os.environ.get("REPO_URL", "/mnt/repos/scenario-repo")
    elif backend == "rest":
        return defaults.rest_url
    elif backend == "s3":
        return f"s3+http://127.0.0.1:9000/vykar-scenario/{repo_label}"
    elif backend == "sftp":
        base = defaults.sftp_base_dir.rstrip("/")
        return f"sftp://{defaults.sftp_user}@{defaults.sftp_host}:{defaults.sftp_port}{base}/{repo_label}"
    else:
        raise ValueError(f"unknown backend: {backend}")


def write_vykar_config(
    out_path: str,
    *,
    backend: str,
    repo_label: str,
    corpus_path: str,
    repo_url: str | None = None,
    defaults: Defaults | None = None,
) -> str:
    """Write a vykar YAML config file. Returns the repo URL used."""
    if defaults is None:
        defaults = load_defaults()
    if repo_url is None:
        repo_url = resolve_repo_url(backend, repo_label, defaults)

    repo_entry: dict = {
        "label": repo_label,
        "url": repo_url,
    }

    if repo_url.startswith("http://") or repo_url.startswith("s3+http://"):
        repo_entry["allow_insecure_http"] = True

    if backend == "rest":
        repo_entry["access_token"] = defaults.rest_token

    if backend == "s3":
        repo_entry["region"] = defaults.s3_region
        repo_entry["access_key_id"] = defaults.s3_access_key
        repo_entry["secret_access_key"] = defaults.s3_secret_key

    if backend == "sftp":
        if defaults.sftp_key:
            repo_entry["sftp_key"] = defaults.sftp_key
        if defaults.sftp_known_hosts:
            repo_entry["sftp_known_hosts"] = defaults.sftp_known_hosts

    config: dict = {
        "repositories": [repo_entry],
        "encryption": {
            "mode": "auto",
            "passphrase": defaults.passphrase,
        },
        "compression": {
            "algorithm": "zstd",
            "zstd_level": 3,
        },
        "retention": {
            "keep_last": 1,
        },
        "git_ignore": False,
        "xattrs": {
            "enabled": False,
        },
        "sources": [
            {
                "path": corpus_path,
                "label": "corpus",
            }
        ],
    }

    if backend == "sftp" and defaults.sftp_max_connections:
        config["limits"] = {"connections": int(defaults.sftp_max_connections)}

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)

    return repo_url


def ensure_backend_ready(backend: str, repo_url: str, defaults: Defaults | None = None) -> None:
    """Prepare backend prerequisites required before vykar init."""
    if backend != "s3":
        return

    if defaults is None:
        defaults = load_defaults()

    try:
        from minio import Minio
    except ImportError as exc:
        raise RuntimeError("minio package is not installed; required for S3 bucket setup") from exc

    parsed = urlparse(repo_url.replace("s3+http://", "http://", 1).replace("s3://", "https://", 1))
    bucket = parsed.path.lstrip("/").split("/", 1)[0]
    if not parsed.netloc or not bucket:
        raise ValueError(f"unable to parse S3 bucket from URL: {repo_url}")

    client = Minio(
        parsed.netloc,
        access_key=defaults.s3_access_key,
        secret_key=defaults.s3_secret_key,
        secure=parsed.scheme == "https",
        region=defaults.s3_region,
    )
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def reset_minio(defaults: Defaults | None = None) -> None:
    """Reset MinIO service and data dir, then wait for health."""
    if defaults is None:
        defaults = load_defaults()

    subprocess.run(["systemctl", "--user", "stop", defaults.minio_service], check=True)
    subprocess.run(["rm", "-rf", defaults.minio_data_dir], check=True)
    os.makedirs(defaults.minio_data_dir, exist_ok=True)
    subprocess.run(["systemctl", "--user", "start", defaults.minio_service], check=True)

    for attempt in range(30):
        try:
            result = subprocess.run(
                ["curl", "-fsS", defaults.minio_health_url],
                capture_output=True,
                check=False,
            )
            if result.returncode == 0:
                return
        except FileNotFoundError:
            raise RuntimeError("curl is required for MinIO health check")
        time.sleep(1)

    raise RuntimeError(f"MinIO did not become healthy at {defaults.minio_health_url} after reset")
