"""Corpus generation and churn mutations.

Fast path: bin/txt/csv/json/xml/zip/tar are generated directly with stdlib.
Slow path: docx/xlsx/png still use faker-file for format-specific libraries.

Moved from scripts/scenarios/scenario_runner/corpus.py with import path updates only.
"""

import csv
import io
import json
import os
import random
import re
import tarfile
import tempfile
import zipfile

from faker import Faker

# Optional providers — available only when their extra deps are installed.
_OPTIONAL_PROVIDERS: dict[str, type | None] = {}
for _name, _mod, _cls in [
    ("docx", "faker_file.providers.docx_file", "DocxFileProvider"),
    ("xlsx", "faker_file.providers.xlsx_file", "XlsxFileProvider"),
    ("png", "faker_file.providers.png_file", "PngFileProvider"),
]:
    try:
        _m = __import__(_mod, fromlist=[_cls])
        _OPTIONAL_PROVIDERS[_name] = getattr(_m, _cls)
    except ImportError:
        _OPTIONAL_PROVIDERS[_name] = None


_SIZE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(b|kb|mb|gb|tb)\s*$", re.IGNORECASE)

_SIZE_UNITS = {
    "b": 1,
    "kb": 1024,
    "mb": 1024 ** 2,
    "gb": 1024 ** 3,
    "tb": 1024 ** 4,
}


def parse_size(s: str) -> int:
    """Convert human-readable size string to bytes. E.g. '100mb' -> 104857600."""
    m = _SIZE_RE.match(str(s))
    if not m:
        raise ValueError(f"invalid size string: {s!r}")
    return int(float(m.group(1)) * _SIZE_UNITS[m.group(2).lower()])


# Default per-file size when not specified in the YAML mix entry.
_DEFAULT_FILE_SIZE = parse_size("500kb")

# Types that use the fast direct-write path (no faker-file).
_FAST_TYPES = {"bin", "txt", "csv", "json", "xml", "zip", "tar"}

# Types that need faker-file providers.
_FAKER_TYPES = {"docx", "xlsx", "png"}
_ALL_TYPES = _FAST_TYPES | _FAKER_TYPES
_VALIDATED_OPTIONAL_TYPES: set[str] = set()


class CorpusDependencyError(RuntimeError):
    """Raised when a requested corpus file type cannot be generated."""


def _raise_optional_type_error(file_type: str, message: str, exc: Exception | None = None) -> None:
    details = (
        f"corpus type '{file_type}' is unavailable: {message}. "
        f"Reinstall compatible dependencies or remove '{file_type}' from the scenario mix."
    )
    if exc is not None:
        raise CorpusDependencyError(
            f"{details} ({exc.__class__.__name__}: {exc})"
        ) from exc
    raise CorpusDependencyError(details)


def _make_subdirs(root: str, max_depth: int, rng: random.Random) -> list[str]:
    """Create a tree of subdirectories and return their relative paths."""
    dirs = [""]
    for depth in range(1, max_depth + 1):
        parent = rng.choice(dirs)
        name = f"d{depth}_{rng.randint(0, 999):03d}"
        rel = os.path.join(parent, name) if parent else name
        os.makedirs(os.path.join(root, rel), exist_ok=True)
        dirs.append(rel)
    return dirs


def _pick_subdir(dirs: list[str], rng: random.Random) -> str:
    return rng.choice(dirs)


_TEXT_EXTENSIONS = (".txt", ".csv", ".json", ".xml", ".svg")

_TEXT_BLOCK_SIZE = 64 * 1024  # 64 KB


def _generate_text_block(rng: random.Random) -> str:
    """Generate a ~64KB block of semi-realistic text for tiling into txt files."""
    fake = Faker()
    Faker.seed(rng.randint(0, 2**31))
    paragraphs = []
    total = 0
    while total < _TEXT_BLOCK_SIZE:
        p = fake.paragraph(nb_sentences=5)
        paragraphs.append(p)
        total += len(p) + 1
    return "\n".join(paragraphs)


def _make_faker(rng: random.Random) -> Faker:
    """Create a Faker instance with faker-file providers for docx/xlsx/png."""
    fake = Faker()
    Faker.seed(rng.randint(0, 2**31))
    for prov in _OPTIONAL_PROVIDERS.values():
        if prov is not None:
            fake.add_provider(prov)
    return fake


def _probe_optional_type(file_type: str) -> None:
    if file_type in _VALIDATED_OPTIONAL_TYPES:
        return

    provider = _OPTIONAL_PROVIDERS.get(file_type)
    if provider is None:
        _raise_optional_type_error(file_type, "optional provider is not installed")

    rng = random.Random(0)
    text_block = _generate_text_block(rng)
    fake = _make_faker(rng)

    options = {}
    if file_type == "png":
        options["size"] = [8, 8]

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _generate_one(
                file_type,
                tmpdir,
                1024,
                options,
                rng,
                text_block,
                fake,
                [0],
            )
            if not path or not os.path.exists(path):
                _raise_optional_type_error(file_type, "sample generation produced no file")
    except CorpusDependencyError:
        raise
    except Exception as exc:
        _raise_optional_type_error(file_type, "sample generation failed", exc)

    _VALIDATED_OPTIONAL_TYPES.add(file_type)


def validate_corpus_mix(corpus_config: dict) -> None:
    mix = corpus_config.get("mix", [{"type": "bin", "weight": 1}])

    for entry in mix:
        file_type = entry["type"]
        if file_type not in _ALL_TYPES:
            raise ValueError(f"unknown corpus file type: {file_type!r}")
        if file_type in _FAKER_TYPES:
            _probe_optional_type(file_type)


def _generate_one(file_type: str, dest_dir: str, file_size_bytes: int,
                   options: dict, rng: random.Random, text_block: str,
                   fake: Faker | None, counter: list[int]) -> str:
    """Generate a single file of the given type, return absolute path."""
    counter[0] += 1
    seq = counter[0]

    if file_type == "bin":
        path = os.path.join(dest_dir, f"{seq:06d}.bin")
        with open(path, "wb") as f:
            f.write(rng.randbytes(file_size_bytes))
        return path

    if file_type == "txt":
        path = os.path.join(dest_dir, f"{seq:06d}.txt")
        block_len = len(text_block)
        repeats = file_size_bytes // block_len
        remainder = file_size_bytes % block_len
        with open(path, "w") as f:
            for _ in range(repeats):
                f.write(text_block)
                f.write("\n")
            if remainder > 0:
                f.write(text_block[:remainder])
        return path

    if file_type == "csv":
        path = os.path.join(dest_dir, f"{seq:06d}.csv")
        num_rows = max(1, file_size_bytes // 100)
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "col_a", "col_b", "col_c", "col_d", "col_e"])
            for i in range(num_rows):
                writer.writerow([
                    i,
                    rng.randint(0, 999999),
                    f"{rng.getrandbits(32):08x}",
                    rng.uniform(0, 1000),
                    rng.choice(["alpha", "beta", "gamma", "delta", "epsilon"]),
                    f"{rng.getrandbits(64):016x}",
                ])
        return path

    if file_type == "json":
        path = os.path.join(dest_dir, f"{seq:06d}.json")
        num_rows = max(1, file_size_bytes // 200)
        records = []
        for i in range(num_rows):
            records.append({
                "id": i,
                "value": rng.randint(0, 999999),
                "hex": f"{rng.getrandbits(64):016x}",
                "score": round(rng.uniform(0, 100), 4),
                "tag": rng.choice(["alpha", "beta", "gamma", "delta", "epsilon"]),
            })
        with open(path, "w") as f:
            json.dump(records, f)
        return path

    if file_type == "xml":
        path = os.path.join(dest_dir, f"{seq:06d}.xml")
        num_rows = max(1, file_size_bytes // 150)
        parts = ['<?xml version="1.0" encoding="UTF-8"?>\n<data>\n']
        for i in range(num_rows):
            parts.append(
                f'  <row id="{i}" value="{rng.randint(0, 999999)}" '
                f'hex="{rng.getrandbits(64):016x}" '
                f'tag="{rng.choice(["alpha", "beta", "gamma", "delta", "epsilon"])}"/>\n'
            )
        parts.append("</data>\n")
        with open(path, "w") as f:
            f.write("".join(parts))
        return path

    if file_type == "zip":
        path = os.path.join(dest_dir, f"{seq:06d}.zip")
        inner_size = max(options.get("inner_size", 10000), 1)
        count = max(1, file_size_bytes // inner_size)
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
            for j in range(count):
                zf.writestr(f"inner_{j:04d}.bin", rng.randbytes(inner_size))
        return path

    if file_type == "tar":
        path = os.path.join(dest_dir, f"{seq:06d}.tar")
        inner_size = max(options.get("inner_size", 10000), 1)
        count = max(1, file_size_bytes // inner_size)
        with tarfile.open(path, "w") as tf:
            for j in range(count):
                data = rng.randbytes(inner_size)
                info = tarfile.TarInfo(name=f"inner_{j:04d}.bin")
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
        return path

    # Faker-file types: docx, xlsx, png
    if fake is None:
        _raise_optional_type_error(file_type, "faker-backed generator was not initialized")

    from faker_file.storages.filesystem import FileSystemStorage
    storage = FileSystemStorage(root_path=dest_dir, rel_path="")

    try:
        if file_type == "docx":
            result = fake.docx_file(storage=storage, max_nb_chars=file_size_bytes)
        elif file_type == "xlsx":
            rows = max(1, file_size_bytes // 100)
            result = fake.xlsx_file(storage=storage, num_rows=rows)
        elif file_type == "png":
            size = options.get("size", [256, 256])
            result = fake.png_file(storage=storage, size=tuple(size))
        else:
            raise ValueError(f"unknown corpus file type: {file_type!r}")
    except CorpusDependencyError:
        raise
    except Exception as exc:
        _raise_optional_type_error(file_type, "sample generation failed", exc)

    return os.path.join(storage.root_path, str(result)) if result else ""


def generate_corpus(target_dir: str, corpus_config: dict, rng: random.Random | None = None) -> dict:
    """Generate files in target_dir until size_gib is reached.

    Returns dict with keys: total_bytes, file_count, files_by_type.
    """
    if rng is None:
        rng = random.Random()

    os.makedirs(target_dir, exist_ok=True)

    validate_corpus_mix(corpus_config)

    target_bytes = int(corpus_config.get("size_gib", 0.1) * 1024 ** 3)
    max_depth = corpus_config.get("max_depth", 2)

    types, weights, file_sizes, file_options = _resolve_mix_config(corpus_config)

    text_block = _generate_text_block(rng)

    needs_faker = any(t in _FAKER_TYPES for t in types)
    fake = _make_faker(rng) if needs_faker else None

    counter = [0]
    subdirs = _make_subdirs(target_dir, max_depth, rng)
    total_bytes = 0
    file_count = 0
    files_by_type: dict[str, int] = {}

    while total_bytes < target_bytes:
        file_type = rng.choices(types, weights=weights, k=1)[0]
        subdir = _pick_subdir(subdirs, rng)
        full_subdir = os.path.join(target_dir, subdir) if subdir else target_dir
        os.makedirs(full_subdir, exist_ok=True)

        size = file_sizes[file_type]
        path = _generate_one(file_type, full_subdir, size, file_options[file_type],
                             rng, text_block, fake, counter)
        if not path or not os.path.exists(path):
            continue

        actual_size = os.path.getsize(path)
        total_bytes += actual_size
        file_count += 1
        files_by_type[file_type] = files_by_type.get(file_type, 0) + 1

    return {
        "total_bytes": total_bytes,
        "file_count": file_count,
        "files_by_type": files_by_type,
    }


def _walk_files(target_dir: str) -> list[str]:
    """Return list of absolute file paths in target_dir."""
    result = []
    for dirpath, _, filenames in os.walk(target_dir):
        for f in filenames:
            result.append(os.path.join(dirpath, f))
    return result


def _dir_size_bytes(target_dir: str) -> int:
    total = 0
    for path in _walk_files(target_dir):
        try:
            total += os.path.getsize(path)
        except OSError:
            pass
    return total


def dir_size_bytes(target_dir: str) -> int:
    """Return total size of files under target_dir in bytes."""
    return _dir_size_bytes(target_dir)


def _resolve_mix_config(corpus_config: dict) -> tuple[list[str], list[int], dict[str, int], dict[str, dict]]:
    validate_corpus_mix(corpus_config)
    mix = corpus_config.get("mix", [{"type": "bin", "weight": 1}])
    types = []
    weights = []
    file_sizes = {}
    file_options = {}
    for entry in mix:
        t = entry["type"]
        types.append(t)
        weights.append(entry.get("weight", 1))
        file_sizes[t] = parse_size(entry["file_size"]) if "file_size" in entry else _DEFAULT_FILE_SIZE
        file_options[t] = entry.get("options", {})
    return types, weights, file_sizes, file_options


def apply_churn(target_dir: str, corpus_config: dict, churn_config: dict,
                initial_corpus_bytes: int,
                rng: random.Random | None = None) -> dict:
    """Mutate the existing corpus: add, delete, modify files.

    Returns dict with keys: added, deleted, modified, dirs_added.
    """
    if rng is None:
        rng = random.Random()

    max_growth_factor = float(churn_config.get("max_growth_factor", 2.0))
    if max_growth_factor < 1.0:
        raise ValueError("churn.max_growth_factor must be >= 1.0")

    types, weights, file_sizes, file_options = _resolve_mix_config(corpus_config)

    text_block = _generate_text_block(rng)
    needs_faker = any(t in _FAKER_TYPES for t in types)
    fake = _make_faker(rng) if needs_faker else None
    counter = [0]

    total_bytes_before = _dir_size_bytes(target_dir)
    max_allowed_bytes = int(initial_corpus_bytes * max_growth_factor)
    current_bytes = total_bytes_before

    stats = {
        "added": 0,
        "added_bytes": 0,
        "deleted": 0,
        "deleted_bytes": 0,
        "modified": 0,
        "modified_delta_bytes": 0,
        "dirs_added": 0,
        "skipped_add_files": 0,
        "skipped_add_dirs": 0,
        "total_bytes_before": total_bytes_before,
        "total_bytes_after": total_bytes_before,
        "max_allowed_bytes": max_allowed_bytes,
    }

    # Collect existing subdirectories
    subdirs = []
    for dirpath, dirnames, _ in os.walk(target_dir):
        rel = os.path.relpath(dirpath, target_dir)
        subdirs.append(rel if rel != "." else "")

    if not subdirs:
        subdirs = [""]

    # Delete random files
    files = _walk_files(target_dir)
    n_delete = min(churn_config.get("delete_files", 0), len(files))
    if n_delete > 0:
        to_delete = rng.sample(files, n_delete)
        for f in to_delete:
            try:
                size = os.path.getsize(f)
            except OSError:
                size = 0
            try:
                os.unlink(f)
                current_bytes = max(0, current_bytes - size)
                stats["deleted"] += 1
                stats["deleted_bytes"] += size
            except OSError:
                pass
        files = _walk_files(target_dir)

    # Modify random files
    n_modify = min(churn_config.get("modify_files", 0), len(files))
    if n_modify > 0:
        to_modify = rng.sample(files, n_modify)
        for f in to_modify:
            try:
                before_size = os.path.getsize(f)
                is_text = f.endswith(_TEXT_EXTENSIONS)
                if is_text:
                    with open(f, "a") as fh:
                        fh.write(f"\n# churn modification {rng.randint(0, 999999)}\n")
                else:
                    size = os.path.getsize(f)
                    trunc = max(1, int(size * 0.75))
                    with open(f, "r+b") as fh:
                        fh.truncate(trunc)
                        fh.seek(0, 2)
                        fh.write(rng.randbytes(rng.randint(64, 4096)))
                after_size = os.path.getsize(f)
                delta = after_size - before_size
                current_bytes = max(0, current_bytes + delta)
                stats["modified"] += 1
                stats["modified_delta_bytes"] += delta
            except (OSError, PermissionError):
                pass

    def _try_add_file(dest_dir: str) -> bool:
        nonlocal current_bytes

        ft = rng.choices(types, weights=weights, k=1)[0]
        estimated_size = file_sizes[ft]
        if current_bytes + estimated_size > max_allowed_bytes:
            stats["skipped_add_files"] += 1
            return False

        path = _generate_one(ft, dest_dir, estimated_size, file_options[ft],
                             rng, text_block, fake, counter)
        if not path or not os.path.exists(path):
            return False

        try:
            actual_size = os.path.getsize(path)
            current_bytes += actual_size
            stats["added_bytes"] += actual_size
        except OSError:
            pass
        stats["added"] += 1
        return True

    # Add new directories
    for _ in range(churn_config.get("add_dirs", 0)):
        parent = rng.choice(subdirs)
        name = f"churn_{rng.randint(0, 99999):05d}"
        rel = os.path.join(parent, name) if parent else name
        full = os.path.join(target_dir, rel)

        planned = [
            rng.choices(types, weights=weights, k=1)[0]
            for _ in range(rng.randint(1, 3))
        ]
        if not any(current_bytes + file_sizes[ft] <= max_allowed_bytes for ft in planned):
            stats["skipped_add_dirs"] += 1
            continue

        os.makedirs(full, exist_ok=True)
        subdirs.append(rel)
        stats["dirs_added"] += 1
        for ft in planned:
            estimated_size = file_sizes[ft]
            if current_bytes + estimated_size > max_allowed_bytes:
                stats["skipped_add_files"] += 1
                continue
            path = _generate_one(ft, full, estimated_size, file_options[ft],
                                 rng, text_block, fake, counter)
            if path and os.path.exists(path):
                try:
                    actual_size = os.path.getsize(path)
                    current_bytes += actual_size
                    stats["added_bytes"] += actual_size
                except OSError:
                    pass
                stats["added"] += 1

    # Add files in existing dirs
    for _ in range(churn_config.get("add_files", 0)):
        subdir = rng.choice(subdirs)
        full_subdir = os.path.join(target_dir, subdir) if subdir else target_dir
        os.makedirs(full_subdir, exist_ok=True)
        _try_add_file(full_subdir)

    stats["total_bytes_after"] = current_bytes
    return stats
