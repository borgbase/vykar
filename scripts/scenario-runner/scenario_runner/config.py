"""Generate vykar YAML config files, ported from scripts/lib/vykar-repo.sh."""

import os

import yaml


# Backend defaults matching scripts/lib/defaults.sh
_DEFAULTS = {
    "passphrase": os.environ.get("PASSPHRASE", "123"),
    "rest_url": os.environ.get("REST_URL", "http://127.0.0.1:8585"),
    "rest_token": os.environ.get(
        "REST_TOKEN",
        os.environ.get("VYKAR_REST_TOKEN",
                        os.environ.get("VYKAR_TOKEN",
                                       os.environ.get("VGER_TOKEN", "vger-e2e-local-token")))),
    "rest_data_dir": os.environ.get("REST_DATA_DIR", "/mnt/repos/bench-vykar/vykar-server-data"),
    "s3_region": os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1")),
    "s3_access_key": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
    "s3_secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "sftp_host": os.environ.get("SFTP_HOST", "127.0.0.1"),
    "sftp_port": os.environ.get("SFTP_PORT", "22"),
    "sftp_user": os.environ.get("SFTP_USER", os.environ.get("USER", "root")),
    "sftp_base_dir": os.environ.get("SFTP_BASE_DIR", "/mnt/repos"),
    "sftp_key": os.environ.get("SFTP_KEY", os.path.expanduser("~/.ssh/id_ed25519")),
    "sftp_known_hosts": os.environ.get("SFTP_KNOWN_HOSTS", ""),
    "sftp_max_connections": os.environ.get("SFTP_MAX_CONNECTIONS", ""),
}


def resolve_repo_url(backend: str, repo_label: str, output_dir: str) -> str:
    """Compute the repository URL for the given backend."""
    if backend == "local":
        return os.environ.get("REPO_URL", "/mnt/repos/scenario-repo")
    elif backend == "rest":
        return _DEFAULTS["rest_url"]
    elif backend == "s3":
        return f"s3+http://127.0.0.1:9000/vykar-scenario/{repo_label}"
    elif backend == "sftp":
        user = _DEFAULTS["sftp_user"]
        host = _DEFAULTS["sftp_host"]
        port = _DEFAULTS["sftp_port"]
        base = _DEFAULTS["sftp_base_dir"].rstrip("/")
        return f"sftp://{user}@{host}:{port}{base}/{repo_label}"
    else:
        raise ValueError(f"unknown backend: {backend}")


def write_vykar_config(out_path: str, *, backend: str, repo_label: str, corpus_path: str) -> str:
    """Write a vykar YAML config file. Returns the repo URL used."""
    output_dir = os.path.dirname(out_path)
    repo_url = resolve_repo_url(backend, repo_label, output_dir)

    repo_entry: dict = {
        "label": repo_label,
        "url": repo_url,
    }

    # Insecure HTTP flag for http:// and s3+http:// URLs
    if repo_url.startswith("http://") or repo_url.startswith("s3+http://"):
        repo_entry["allow_insecure_http"] = True

    if backend == "rest":
        repo_entry["access_token"] = _DEFAULTS["rest_token"]

    if backend == "s3":
        repo_entry["region"] = _DEFAULTS["s3_region"]
        repo_entry["access_key_id"] = _DEFAULTS["s3_access_key"]
        repo_entry["secret_access_key"] = _DEFAULTS["s3_secret_key"]

    if backend == "sftp":
        if _DEFAULTS["sftp_key"]:
            repo_entry["sftp_key"] = _DEFAULTS["sftp_key"]
        if _DEFAULTS["sftp_known_hosts"]:
            repo_entry["sftp_known_hosts"] = _DEFAULTS["sftp_known_hosts"]

    config: dict = {
        "repositories": [repo_entry],
        "encryption": {
            "mode": "auto",
            "passphrase": _DEFAULTS["passphrase"],
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
        "sources": [{
            "path": corpus_path,
            "label": "corpus",
        }],
    }

    if backend == "sftp" and _DEFAULTS["sftp_max_connections"]:
        config["limits"] = {"connections": int(_DEFAULTS["sftp_max_connections"])}

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)

    return repo_url
