"""File-Management job entry helpers (copy/move/zip/unzip/HTTP/…).

Pure functions used by ``handlers.handle_*`` for File Management entries.
Driver-side / Databricks-compatible (stdlib + optional ``requests``).
"""

from __future__ import annotations

import filecmp
import hashlib
import logging
import re
import shutil
import time
import zipfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import (
    HTTPBasicAuthHandler,
    HTTPPasswordMgrWithDefaultRealm,
    ProxyHandler,
    Request,
    build_opener,
    install_opener,
    urlopen,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def yn_true(raw: Any, default: bool = False) -> bool:
    if raw is None or raw == "":
        return default
    return str(raw).strip().upper() in {"Y", "YES", "TRUE", "1"}


def attr(attrs: Mapping[str, Any], *keys: str, default: str = "") -> str:
    for key in keys:
        if key in attrs and attrs[key] is not None and str(attrs[key]) != "":
            return str(attrs[key])
    return default


def attr_yn(attrs: Mapping[str, Any], *keys: str, default: bool = False) -> bool:
    for key in keys:
        if key in attrs and attrs[key] is not None and str(attrs[key]) != "":
            return yn_true(attrs[key], default)
    return default


def iter_warning_logs(prefix: str, warnings: Iterable[str]) -> None:
    for warning in warnings:
        logger.warning("%s | %s", prefix, warning)


def compile_wildcard(pattern: str | None, *, default_all: bool = True) -> re.Pattern[str] | None:
    """Compile a PDI Java-style regex wildcard; empty → match-all or None."""
    text = (pattern or "").strip()
    if not text:
        return re.compile(r".*") if default_all else None
    try:
        return re.compile(text)
    except re.error:
        # Fallback: treat as glob-ish literal
        escaped = re.escape(text).replace(r"\*", ".*").replace(r"\?", ".")
        return re.compile(escaped)


def matches_wildcard(name: str, pattern: re.Pattern[str] | None) -> bool:
    """Return True when ``pattern`` is None (no include filter) or matches."""
    if pattern is None:
        return True
    return bool(pattern.search(name))


def excluded_by(name: str, pattern: re.Pattern[str] | None) -> bool:
    """Return True only when an exclude pattern is set and matches."""
    if pattern is None:
        return False
    return bool(pattern.search(name))


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def stamp_path(path: Path, *, add_date: bool, add_time: bool, fmt: str = "") -> Path:
    if fmt:
        stamp = datetime.now().strftime(fmt)
    else:
        stamp = datetime.now().strftime(
            ("_%Y%m%d" if add_date else "") + ("_%H%M%S" if add_time else "")
        )
    if not stamp:
        return path
    return path.with_name(path.stem + stamp + path.suffix)


@dataclass
class FileOpOutcome:
    success: bool
    message: str = ""
    paths: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    error: BaseException | None = None
    extra: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Result filename list (PDI ResultFile)
# ---------------------------------------------------------------------------


def result_paths(runtime: Any) -> list[dict[str, Any]]:
    files = getattr(runtime, "result_filenames", None)
    if files is None:
        runtime.result_filenames = []
        return runtime.result_filenames
    return files


def add_result_file(runtime: Any, path: str, *, file_type: str = "GENERAL") -> None:
    text = str(path or "").strip()
    if not text:
        return
    files = result_paths(runtime)
    # Deduplicate by path
    for item in files:
        if item.get("path") == text:
            return
    files.append({"path": text, "type": file_type})


def clear_result_files(runtime: Any) -> None:
    result_paths(runtime).clear()


def delete_result_filenames(
    runtime: Any,
    *,
    wildcard: str = "",
    wildcardexclude: str = "",
    specify_wildcard: bool = False,
) -> FileOpOutcome:
    files = result_paths(runtime)
    if not specify_wildcard:
        removed = len(files)
        files.clear()
        return FileOpOutcome(True, f"Cleared {removed} result filenames", [])

    include = compile_wildcard(wildcard, default_all=True)
    exclude = compile_wildcard(wildcardexclude, default_all=False) if wildcardexclude else None
    kept: list[dict[str, Any]] = []
    removed_paths: list[str] = []
    for item in files:
        name = Path(item.get("path", "")).name
        if matches_wildcard(name, include) and not excluded_by(name, exclude):
            removed_paths.append(str(item.get("path", "")))
        else:
            kept.append(item)
    files[:] = kept
    return FileOpOutcome(True, f"Removed {len(removed_paths)} result filenames", removed_paths)


# ---------------------------------------------------------------------------
# Path listing / matching
# ---------------------------------------------------------------------------


def iter_matching_files(
    root: Path,
    wildcard: str = "",
    *,
    recursive: bool = False,
    include_dirs: bool = False,
) -> list[Path]:
    if not root.exists():
        return []
    pattern = compile_wildcard(wildcard, default_all=True)
    results: list[Path] = []
    if root.is_file():
        if matches_wildcard(root.name, pattern):
            results.append(root)
        return results

    iterator = root.rglob("*") if recursive else root.iterdir()
    for fp in iterator:
        try:
            if fp.is_file() and matches_wildcard(fp.name, pattern):
                results.append(fp)
            elif include_dirs and fp.is_dir() and matches_wildcard(fp.name, pattern):
                results.append(fp)
        except OSError:
            continue
    return results


def source_dest_pairs(attrs: Mapping[str, Any]) -> list[dict[str, str]]:
    """Return source/destination/wildcard rows from fields or flat attributes."""
    pairs: list[dict[str, str]] = []
    for row in attrs.get("fields") or []:
        if not isinstance(row, Mapping):
            continue
        src = str(
            row.get("source_filefolder")
            or row.get("name")
            or row.get("filename")
            or ""
        ).strip()
        if not src:
            continue
        pairs.append(
            {
                "source": src,
                "destination": str(
                    row.get("destination_filefolder")
                    or row.get("destination")
                    or ""
                ).strip(),
                "wildcard": str(
                    row.get("wildcard")
                    or row.get("filemask")
                    or row.get("source_wildcard")
                    or ""
                ).strip(),
            }
        )
    if pairs:
        return pairs

    src = attr(attrs, "source_filefolder", "filename", "foldername", "name")
    if src:
        pairs.append(
            {
                "source": src,
                "destination": attr(
                    attrs, "destination_filefolder", "destination_folder", "destination"
                ),
                "wildcard": attr(
                    attrs, "source_wildcard", "wildcard", "filemask", "wildCard"
                ),
            }
        )
    return pairs


# ---------------------------------------------------------------------------
# Create folder / Create file / Write to file
# ---------------------------------------------------------------------------


def create_folder(
    folder: str,
    *,
    fail_if_exists: bool = False,
) -> FileOpOutcome:
    path = Path(folder)
    if path.exists():
        if fail_if_exists:
            return FileOpOutcome(
                False,
                f"Folder already exists: {folder}",
                error=FileExistsError(f"Folder already exists: {folder}"),
            )
        return FileOpOutcome(True, f"Folder exists: {folder}", [str(path)])
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return FileOpOutcome(False, str(exc), error=exc)
    return FileOpOutcome(True, f"Created folder: {folder}", [str(path)])


def create_file(
    filename: str,
    *,
    fail_if_exists: bool = False,
    create_parent: bool = True,
) -> FileOpOutcome:
    path = Path(filename)
    if path.exists():
        if fail_if_exists:
            return FileOpOutcome(
                False,
                f"File already exists: {filename}",
                error=FileExistsError(f"File already exists: {filename}"),
            )
        return FileOpOutcome(True, f"File exists: {filename}", [str(path)])
    try:
        if create_parent:
            ensure_parent(path)
        path.touch(exist_ok=True)
    except OSError as exc:
        return FileOpOutcome(False, str(exc), error=exc)
    return FileOpOutcome(True, f"Created file: {filename}", [str(path)])


def write_to_file(
    filename: str,
    content: str,
    *,
    append: bool = False,
    create_parent: bool = True,
    encoding: str = "UTF-8",
) -> FileOpOutcome:
    path = Path(filename)
    enc = encoding or "UTF-8"
    try:
        if create_parent:
            ensure_parent(path)
        mode = "a" if append else "w"
        with path.open(mode, encoding=enc, newline="") as fh:
            fh.write(content if content is not None else "")
    except OSError as exc:
        return FileOpOutcome(False, str(exc), error=exc)
    return FileOpOutcome(True, f"Wrote file: {filename}", [str(path)])


# ---------------------------------------------------------------------------
# Delete file / files / folders
# ---------------------------------------------------------------------------


def delete_file(
    filename: str,
    *,
    fail_if_not_exists: bool = False,
) -> FileOpOutcome:
    path = Path(filename)
    if not path.exists():
        if fail_if_not_exists:
            return FileOpOutcome(
                False,
                f"File does not exist: {filename}",
                error=FileNotFoundError(f"File does not exist: {filename}"),
            )
        return FileOpOutcome(True, f"File already absent: {filename}", [])
    try:
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    except OSError as exc:
        return FileOpOutcome(False, str(exc), error=exc)
    return FileOpOutcome(True, f"Deleted: {filename}", [str(path)])


def delete_files(
    pairs: Sequence[Mapping[str, str]],
    *,
    recursive: bool = False,
    resolve: Callable[[str], str] | None = None,
) -> FileOpOutcome:
    resolve = resolve or (lambda s: s)
    deleted: list[str] = []
    warnings: list[str] = []
    for pair in pairs:
        src_raw = resolve(pair.get("source", ""))
        wildcard = resolve(pair.get("wildcard", ""))
        root = Path(src_raw)
        if not root.exists():
            warnings.append(f"Path not found (skipped): {src_raw}")
            continue
        targets = iter_matching_files(root, wildcard, recursive=recursive)
        if root.is_file() and not targets:
            targets = [root]
        for fp in targets:
            try:
                fp.unlink()
                deleted.append(str(fp))
            except OSError as exc:
                warnings.append(f"Failed to delete {fp}: {exc}")
    return FileOpOutcome(True, f"Deleted {len(deleted)} file(s)", deleted, warnings)


def delete_folders(
    folders: Sequence[str],
    *,
    fail_if_not_exists: bool = False,
    resolve: Callable[[str], str] | None = None,
) -> FileOpOutcome:
    resolve = resolve or (lambda s: s)
    deleted: list[str] = []
    warnings: list[str] = []
    for raw in folders:
        folder = resolve(raw)
        path = Path(folder)
        if not path.exists():
            if fail_if_not_exists:
                return FileOpOutcome(
                    False,
                    f"Folder does not exist: {folder}",
                    deleted,
                    warnings,
                    error=FileNotFoundError(f"Folder does not exist: {folder}"),
                )
            warnings.append(f"Folder already absent: {folder}")
            continue
        try:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
            deleted.append(str(path))
        except OSError as exc:
            return FileOpOutcome(False, str(exc), deleted, warnings, error=exc)
    return FileOpOutcome(True, f"Deleted {len(deleted)} folder(s)", deleted, warnings)


# ---------------------------------------------------------------------------
# Copy / Move files
# ---------------------------------------------------------------------------


def _unique_name(dest: Path) -> Path:
    stem, suffix = dest.stem, dest.suffix
    n = 1
    while True:
        candidate = dest.with_name(f"{stem}_{n}{suffix}")
        if not candidate.exists():
            return candidate
        n += 1


def _copy_one(
    src: Path,
    dest: Path,
    *,
    overwrite: bool,
    if_exists: str = "overwrite",
) -> Path | None:
    """Copy/move target resolution. Returns dest path or None if skipped."""
    ensure_parent(dest)
    if dest.exists():
        mode = (if_exists or "overwrite").lower()
        if mode in {"do_nothing", "nothing", "2", "4"} and not overwrite:
            return None
        if mode in {"fail", "3"} and not overwrite:
            raise FileExistsError(f"Destination exists: {dest}")
        if mode in {"unique_name", "unique", "1"}:
            dest = _unique_name(dest)
        elif not overwrite and mode not in {"overwrite", "overwrite_file", "0"}:
            return None
        # overwrite path falls through
    return dest


def copy_files(
    pairs: Sequence[Mapping[str, str]],
    *,
    overwrite: bool = False,
    recursive: bool = False,
    create_destination: bool = False,
    remove_source: bool = False,
    destination_is_file: bool = False,
    resolve: Callable[[str], str] | None = None,
) -> FileOpOutcome:
    resolve = resolve or (lambda s: s)
    copied: list[str] = []
    warnings: list[str] = []
    for pair in pairs:
        src_raw = resolve(pair.get("source", ""))
        dst_raw = resolve(pair.get("destination", ""))
        wildcard = resolve(pair.get("wildcard", ""))
        src = Path(src_raw)
        dst = Path(dst_raw)
        if not src.exists():
            warnings.append(f"Source not found: {src_raw}")
            continue
        if create_destination and not destination_is_file:
            ensure_dir(dst if not destination_is_file else dst.parent)

        files = iter_matching_files(src, wildcard, recursive=recursive)
        if src.is_file():
            files = [src]

        for fp in files:
            if destination_is_file and len(files) == 1:
                target = dst
            elif src.is_dir() and recursive:
                try:
                    rel = fp.relative_to(src)
                except ValueError:
                    rel = Path(fp.name)
                target = dst / rel
            else:
                target = dst / fp.name if dst.is_dir() or not destination_is_file else dst
            try:
                final = _copy_one(fp, target, overwrite=overwrite)
                if final is None:
                    warnings.append(f"Skipped existing: {target}")
                    continue
                ensure_parent(final)
                shutil.copy2(fp, final)
                copied.append(str(final))
                if remove_source:
                    fp.unlink(missing_ok=True)
            except OSError as exc:
                warnings.append(f"Copy failed {fp} → {target}: {exc}")
    ok = bool(copied) or not warnings
    # PDI often succeeds even when nothing matched; treat empty+no errors as success
    return FileOpOutcome(True, f"Copied {len(copied)} file(s)", copied, warnings)


def move_files(
    pairs: Sequence[Mapping[str, str]],
    *,
    overwrite: bool = False,
    recursive: bool = False,
    create_destination: bool = False,
    destination_is_file: bool = False,
    if_file_exists: str = "overwrite",
    resolve: Callable[[str], str] | None = None,
) -> FileOpOutcome:
    resolve = resolve or (lambda s: s)
    moved: list[str] = []
    warnings: list[str] = []
    for pair in pairs:
        src_raw = resolve(pair.get("source", ""))
        dst_raw = resolve(pair.get("destination", ""))
        wildcard = resolve(pair.get("wildcard", ""))
        src = Path(src_raw)
        dst = Path(dst_raw)
        if not src.exists():
            warnings.append(f"Source not found: {src_raw}")
            continue
        if create_destination and not destination_is_file:
            ensure_dir(dst)

        files = iter_matching_files(src, wildcard, recursive=recursive)
        if src.is_file():
            files = [src]

        for fp in files:
            if destination_is_file and len(files) == 1:
                target = dst
            elif src.is_dir() and recursive:
                try:
                    rel = fp.relative_to(src)
                except ValueError:
                    rel = Path(fp.name)
                target = dst / rel
            else:
                target = dst / fp.name if not destination_is_file else dst
            try:
                final = _copy_one(fp, target, overwrite=overwrite, if_exists=if_file_exists)
                if final is None:
                    warnings.append(f"Skipped existing: {target}")
                    continue
                ensure_parent(final)
                shutil.move(str(fp), str(final))
                moved.append(str(final))
            except (OSError, FileExistsError) as exc:
                return FileOpOutcome(False, str(exc), moved, warnings, error=exc)
    return FileOpOutcome(True, f"Moved {len(moved)} file(s)", moved, warnings)


# ---------------------------------------------------------------------------
# Compare files / folders
# ---------------------------------------------------------------------------


def file_compare(
    filename1: str,
    filename2: str,
    *,
    compare_size: bool = False,
    compare_timestamp: bool = False,
) -> FileOpOutcome:
    p1, p2 = Path(filename1), Path(filename2)
    warnings: list[str] = []
    if not p1.exists() or not p2.exists():
        missing = []
        if not p1.exists():
            missing.append(filename1)
        if not p2.exists():
            missing.append(filename2)
        err = FileNotFoundError(f"Missing file(s): {', '.join(missing)}")
        return FileOpOutcome(False, str(err), error=err)

    if compare_size and p1.stat().st_size != p2.stat().st_size:
        return FileOpOutcome(False, "Files differ in size", [filename1, filename2])
    if compare_timestamp and int(p1.stat().st_mtime) != int(p2.stat().st_mtime):
        return FileOpOutcome(False, "Files differ in timestamp", [filename1, filename2])

    same = filecmp.cmp(p1, p2, shallow=False)
    if same:
        # Optional checksum confirmation
        def _md5(p: Path) -> str:
            h = hashlib.md5()
            with p.open("rb") as fh:
                for chunk in iter(lambda: fh.read(65536), b""):
                    h.update(chunk)
            return h.hexdigest()

        if _md5(p1) != _md5(p2):
            same = False
            warnings.append("Binary compare passed shallow check but MD5 differed")
    return FileOpOutcome(
        same,
        "Files identical" if same else "Files differ",
        [filename1, filename2],
        warnings,
        error=None if same else ValueError("Files differ"),
    )


def folders_compare(
    folder1: str,
    folder2: str,
    *,
    include_subfolders: bool = False,
    compare_filesize: bool = False,
    compare_content: bool = True,
    compare_only: str = "all",
    wildcard: str = "",
) -> FileOpOutcome:
    """Compare two folders. ``compare_only``: all | size | time | content-ish."""
    d1, d2 = Path(folder1), Path(folder2)
    if not d1.is_dir() or not d2.is_dir():
        err = NotADirectoryError(f"Both paths must be directories: {folder1!r}, {folder2!r}")
        return FileOpOutcome(False, str(err), error=err)

    pattern = compile_wildcard(wildcard, default_all=True)
    mode = (compare_only or "all").strip().lower()

    def _collect(root: Path) -> dict[str, Path]:
        out: dict[str, Path] = {}
        it = root.rglob("*") if include_subfolders else root.iterdir()
        for fp in it:
            if not fp.is_file():
                continue
            if not matches_wildcard(fp.name, pattern):
                continue
            try:
                rel = fp.relative_to(root).as_posix()
            except ValueError:
                rel = fp.name
            out[rel] = fp
        return out

    left = _collect(d1)
    right = _collect(d2)
    only_left = sorted(set(left) - set(right))
    only_right = sorted(set(right) - set(left))
    diffs: list[str] = []

    for key in sorted(set(left) & set(right)):
        a, b = left[key], right[key]
        if mode in {"size", "filesize"} or compare_filesize:
            if a.stat().st_size != b.stat().st_size:
                diffs.append(f"size:{key}")
                continue
            if mode in {"size", "filesize"}:
                continue
        if mode in {"time", "timestamp"}:
            if int(a.stat().st_mtime) != int(b.stat().st_mtime):
                diffs.append(f"time:{key}")
            continue
        if compare_content or mode in {"all", "content", "filecontent"}:
            if not filecmp.cmp(a, b, shallow=False):
                diffs.append(f"content:{key}")

    identical = not only_left and not only_right and not diffs
    extra = {
        "only_in_folder1": only_left,
        "only_in_folder2": only_right,
        "differences": diffs,
    }
    if identical:
        return FileOpOutcome(True, "Folders identical", [folder1, folder2], extra=extra)
    return FileOpOutcome(
        False,
        "Folders differ",
        [folder1, folder2],
        error=ValueError("Folders differ"),
        extra=extra,
    )


# ---------------------------------------------------------------------------
# DOS / Unix converter
# ---------------------------------------------------------------------------


def convert_dos_unix(
    pairs: Sequence[Mapping[str, str]],
    *,
    conversion_type: str = "guess",
    recursive: bool = False,
    resolve: Callable[[str], str] | None = None,
) -> FileOpOutcome:
    """``conversion_type``: 0/guess, 1/dos_to_unix, 2/unix_to_dos (PDI codes)."""
    resolve = resolve or (lambda s: s)
    code = str(conversion_type).strip().lower()
    converted: list[str] = []
    warnings: list[str] = []

    for pair in pairs:
        src_raw = resolve(pair.get("source", ""))
        wildcard = resolve(pair.get("wildcard", ""))
        root = Path(src_raw)
        files = iter_matching_files(root, wildcard, recursive=recursive)
        if root.is_file():
            files = [root]
        for fp in files:
            try:
                data = fp.read_bytes()
                # Preserve encoding by operating on bytes newlines only
                mode = code
                if mode in {"0", "guess", ""}:
                    mode = "dos_to_unix" if b"\r\n" in data else "unix_to_dos"
                if mode in {"1", "dos_to_unix", "dottounix", "dos"}:
                    out = data.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
                elif mode in {"2", "unix_to_dos", "unixtodos", "unix"}:
                    # Normalize to LF first then CRLF
                    out = data.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
                    out = out.replace(b"\n", b"\r\n")
                else:
                    warnings.append(f"Unknown ConversionType={conversion_type!r}")
                    continue
                if out != data:
                    fp.write_bytes(out)
                converted.append(str(fp))
            except OSError as exc:
                warnings.append(f"Convert failed {fp}: {exc}")
    return FileOpOutcome(True, f"Converted {len(converted)} file(s)", converted, warnings)


# ---------------------------------------------------------------------------
# Zip / Unzip
# ---------------------------------------------------------------------------


_ZIP_COMPRESS = {
    "0": zipfile.ZIP_STORED,
    "none": zipfile.ZIP_STORED,
    "1": zipfile.ZIP_DEFLATED,
    "default": zipfile.ZIP_DEFLATED,
    "2": zipfile.ZIP_DEFLATED,  # best speed — stdlib has no separate flag
    "best_speed": zipfile.ZIP_DEFLATED,
    "3": zipfile.ZIP_DEFLATED,  # best compression
    "best_compression": zipfile.ZIP_DEFLATED,
}


def zip_files(
    zipfilename: str,
    sourcedirectory: str,
    *,
    wildcard: str = "",
    wildcardexclude: str = "",
    recursive: bool = True,
    compressionrate: str = "1",
    create_parent: bool = False,
    add_date: bool = False,
    add_time: bool = False,
    if_zip_exists: str = "0",
    from_previous_paths: Sequence[str] | None = None,
) -> FileOpOutcome:
    warnings: list[str] = []
    zip_path = Path(zipfilename)
    if add_date or add_time:
        zip_path = stamp_path(zip_path, add_date=add_date, add_time=add_time)

    exists_mode = str(if_zip_exists).strip()
    if zip_path.exists():
        if exists_mode in {"2", "do_nothing"}:
            return FileOpOutcome(True, f"Zip exists — skipped: {zip_path}", [str(zip_path)])
        if exists_mode in {"3", "fail"}:
            err = FileExistsError(f"Zip already exists: {zip_path}")
            return FileOpOutcome(False, str(err), error=err)
        if exists_mode in {"0", "create_new", ""}:
            zip_path = _unique_name(zip_path) if exists_mode == "0" and zip_path.exists() else zip_path
            # PDI 0 = create unique when exists
            if zip_path.exists() and exists_mode in {"0", ""}:
                zip_path = _unique_name(zip_path)
        # 1 = append
    if create_parent:
        ensure_parent(zip_path)

    include = compile_wildcard(wildcard, default_all=True)
    exclude = compile_wildcard(wildcardexclude, default_all=False) if wildcardexclude else None
    compress = _ZIP_COMPRESS.get(str(compressionrate).strip().lower(), zipfile.ZIP_DEFLATED)

    sources: list[tuple[Path, str]] = []  # (file, arcname)
    if from_previous_paths:
        for p in from_previous_paths:
            fp = Path(p)
            if fp.is_file():
                sources.append((fp, fp.name))
    else:
        src = Path(sourcedirectory)
        if not src.exists():
            warnings.append(f"Source directory missing: {sourcedirectory}")
        elif src.is_file():
            if matches_wildcard(src.name, include) and not excluded_by(src.name, exclude):
                sources.append((src, src.name))
        else:
            files = iter_matching_files(src, "", recursive=recursive)
            for fp in files:
                if not matches_wildcard(fp.name, include):
                    continue
                if excluded_by(fp.name, exclude):
                    continue
                try:
                    arc = fp.relative_to(src).as_posix()
                except ValueError:
                    arc = fp.name
                sources.append((fp, arc))

    mode = "a" if zip_path.exists() and exists_mode in {"1", "append"} else "w"
    try:
        with zipfile.ZipFile(zip_path, mode, compression=compress) as zf:
            for fp, arc in sources:
                zf.write(fp, arc)
    except OSError as exc:
        return FileOpOutcome(False, str(exc), error=exc, warnings=warnings)

    if str(compressionrate) in {"2", "3"}:
        warnings.append(
            f"compressionrate={compressionrate} approximated with ZIP_DEFLATED "
            "(stdlib has no separate best-speed/best-compression flag)"
        )
    return FileOpOutcome(
        True,
        f"Zipped {len(sources)} file(s) → {zip_path}",
        [str(zip_path)],
        warnings,
        extra={"count": len(sources)},
    )


def unzip_file(
    zipfilename: str,
    targetdirectory: str,
    *,
    wildcard: str = "",
    wildcardexclude: str = "",
    create_folder: bool = True,
    overwrite: bool = True,
    rootzip: bool = False,
    password: str | None = None,
    after_unzip: str = "0",
    move_to: str = "",
) -> FileOpOutcome:
    warnings: list[str] = []
    zpath = Path(zipfilename)
    if not zpath.exists():
        err = FileNotFoundError(f"Zip not found: {zipfilename}")
        return FileOpOutcome(False, str(err), error=err)

    dest_root = Path(targetdirectory)
    if rootzip:
        dest_root = dest_root / zpath.stem
    if create_folder:
        ensure_dir(dest_root)

    include = compile_wildcard(wildcard, default_all=True)
    exclude = compile_wildcard(wildcardexclude, default_all=False) if wildcardexclude else None
    extracted: list[str] = []
    pwd = password.encode("utf-8") if password else None

    try:
        with zipfile.ZipFile(zpath, "r") as zf:
            for info in zf.infolist():
                name = Path(info.filename).name
                if info.is_dir():
                    continue
                if not matches_wildcard(name, include) or excluded_by(name, exclude):
                    continue
                target = dest_root / info.filename
                if target.exists() and not overwrite:
                    warnings.append(f"Skipped existing: {target}")
                    continue
                ensure_parent(target)
                try:
                    data = zf.read(info, pwd=pwd)
                except RuntimeError as exc:
                    # Wrong password / encrypted
                    return FileOpOutcome(False, str(exc), extracted, warnings, error=exc)
                target.write_bytes(data)
                extracted.append(str(target))
    except zipfile.BadZipFile as exc:
        return FileOpOutcome(False, str(exc), error=exc)

    # afterunzip: 0=do nothing, 1=delete zip, 2=move zip
    after = str(after_unzip).strip()
    if after in {"1", "delete"}:
        try:
            zpath.unlink()
        except OSError as exc:
            warnings.append(f"Could not delete zip: {exc}")
    elif after in {"2", "move"} and move_to:
        try:
            move_dir = Path(move_to)
            ensure_dir(move_dir)
            shutil.move(str(zpath), str(move_dir / zpath.name))
        except OSError as exc:
            warnings.append(f"Could not move zip: {exc}")

    return FileOpOutcome(
        True, f"Extracted {len(extracted)} file(s)", extracted, warnings
    )


# ---------------------------------------------------------------------------
# Wait for file
# ---------------------------------------------------------------------------


def wait_for_file(
    filename: str,
    *,
    timeout: float = 0,
    cycle: float = 1,
    success_on_timeout: bool = False,
    file_size_check: bool = False,
    exists_fn: Callable[[str], bool] | None = None,
) -> FileOpOutcome:
    exists_fn = exists_fn or (lambda p: Path(p).exists())
    # Wildcard support: if path contains regex-ish parent+name
    path = Path(filename)
    parent, name_pat = path.parent, path.name
    is_wild = any(ch in name_pat for ch in "*?[]()|\\") or name_pat.startswith(".*")

    def _present() -> str | None:
        if is_wild and parent.exists():
            pat = compile_wildcard(name_pat, default_all=False)
            for fp in parent.iterdir():
                if fp.is_file() and matches_wildcard(fp.name, pat):
                    if file_size_check:
                        size1 = fp.stat().st_size
                        time.sleep(min(max(cycle, 0.1), 1.0))
                        if fp.stat().st_size != size1:
                            return None
                    return str(fp)
            return None
        if exists_fn(filename):
            if file_size_check and Path(filename).is_file():
                size1 = Path(filename).stat().st_size
                time.sleep(min(max(cycle, 0.1), 1.0))
                if Path(filename).stat().st_size != size1:
                    return None
            return filename
        return None

    deadline = time.time() + max(float(timeout), 0)
    # timeout 0 → wait forever in PDI; we treat 0 as "check once" unless cycle loops
    # Match existing handler: timeout 0 means immediate deadline (fail unless present)
    while True:
        found = _present()
        if found:
            return FileOpOutcome(True, f"File available: {found}", [found])
        if time.time() >= deadline:
            if success_on_timeout:
                return FileOpOutcome(True, "Timed out (successOnTimeout)", [])
            err = TimeoutError(f"File not found before timeout: {filename}")
            return FileOpOutcome(False, str(err), error=err)
        time.sleep(max(float(cycle), 0.1))


# ---------------------------------------------------------------------------
# Process / copy-move result filenames
# ---------------------------------------------------------------------------


def process_result_filenames(
    runtime: Any,
    *,
    action: str = "copy",
    destination_folder: str = "",
    wildcard: str = "",
    wildcardexclude: str = "",
    specify_wildcard: bool = False,
    overwrite: bool = False,
    create_destination: bool = True,
    remove_source_from_result: bool = False,
    add_destination_to_result: bool = True,
    resolve: Callable[[str], str] | None = None,
) -> FileOpOutcome:
    resolve = resolve or (lambda s: s)
    dest_root = Path(resolve(destination_folder)) if destination_folder else None
    action_l = (action or "copy").strip().lower()
    include = compile_wildcard(wildcard, default_all=True) if specify_wildcard or wildcard else None
    exclude = (
        compile_wildcard(wildcardexclude, default_all=False) if wildcardexclude else None
    )

    files = list(result_paths(runtime))
    processed: list[str] = []
    warnings: list[str] = []
    remaining: list[dict[str, Any]] = []

    if dest_root is not None and create_destination and action_l in {"copy", "move"}:
        ensure_dir(dest_root)

    for item in files:
        path_str = str(item.get("path", ""))
        name = Path(path_str).name
        if specify_wildcard or wildcard:
            if not matches_wildcard(name, include) or excluded_by(name, exclude):
                remaining.append(item)
                continue
        src = Path(path_str)
        if action_l in {"delete", "remove"}:
            try:
                if src.exists():
                    src.unlink()
                processed.append(path_str)
                if not remove_source_from_result:
                    # still drop from result when deleting file
                    pass
            except OSError as exc:
                warnings.append(f"Delete failed {src}: {exc}")
                remaining.append(item)
            continue

        if dest_root is None:
            warnings.append("destination_folder is empty — skipped")
            remaining.append(item)
            continue
        target = dest_root / name
        if target.exists() and not overwrite:
            warnings.append(f"Skipped existing: {target}")
            remaining.append(item)
            continue
        try:
            ensure_parent(target)
            if action_l == "move":
                shutil.move(str(src), str(target))
            else:
                shutil.copy2(src, target)
            processed.append(str(target))
            if add_destination_to_result:
                remaining.append({"path": str(target), "type": item.get("type", "GENERAL")})
            if not remove_source_from_result and action_l == "copy":
                remaining.append(item)
            elif not remove_source_from_result and action_l == "move":
                pass  # source gone
            # if remove_source_from_result: drop original
        except OSError as exc:
            warnings.append(f"{action_l} failed {src}: {exc}")
            remaining.append(item)

    result_paths(runtime)[:] = remaining
    return FileOpOutcome(
        True, f"Processed {len(processed)} result filename(s)", processed, warnings
    )


def add_filenames_to_result(
    runtime: Any,
    pairs: Sequence[Mapping[str, str]],
    *,
    recursive: bool = False,
    delete_all_before: bool = False,
    resolve: Callable[[str], str] | None = None,
) -> FileOpOutcome:
    resolve = resolve or (lambda s: s)
    if delete_all_before:
        clear_result_files(runtime)
    added: list[str] = []
    warnings: list[str] = []
    for pair in pairs:
        src_raw = resolve(pair.get("source", ""))
        wildcard = resolve(pair.get("wildcard", ""))
        root = Path(src_raw)
        if not root.exists():
            warnings.append(f"Path not found: {src_raw}")
            continue
        files = iter_matching_files(root, wildcard, recursive=recursive)
        if root.is_file():
            files = [root]
        for fp in files:
            add_result_file(runtime, str(fp))
            added.append(str(fp))
    return FileOpOutcome(True, f"Added {len(added)} result filename(s)", added, warnings)


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


@dataclass
class HttpConfig:
    url: str
    target_filename: str = ""
    upload_filename: str = ""
    username: str = ""
    password: str = ""
    proxy_host: str = ""
    proxy_port: str = ""
    non_proxy_hosts: str = ""
    file_appended: bool = False
    date_time_added: bool = False
    target_extension: str = ""
    add_filename_result: bool = True
    headers: list[tuple[str, str]] = field(default_factory=list)
    timeout: float = 60.0
    ignore_ssl: bool = False
    method: str = "GET"


def http_config_from_attributes(
    attrs: Mapping[str, Any],
    *,
    resolve: Callable[[str], str] | None = None,
) -> tuple[HttpConfig, list[str]]:
    resolve = resolve or (lambda s: s)
    warnings: list[str] = []
    headers: list[tuple[str, str]] = []
    for row in attrs.get("headers") or []:
        if isinstance(row, Mapping):
            name = resolve(str(row.get("header_name") or ""))
            value = resolve(str(row.get("header_value") or ""))
            if name:
                headers.append((name, value))

    password = resolve(attr(attrs, "password"))
    if password.startswith("Encrypted "):
        warnings.append(
            "Password is PDI-encrypted; decryption is not supported — "
            "use a plain password or ${VAR} substitution"
        )

    cfg = HttpConfig(
        url=resolve(attr(attrs, "url")),
        target_filename=resolve(attr(attrs, "targetfilename", "targetFilename")),
        upload_filename=resolve(attr(attrs, "uploadfilename", "uploadFilename")),
        username=resolve(attr(attrs, "username")),
        password=password,
        proxy_host=resolve(attr(attrs, "proxy_host", "proxyHostname")),
        proxy_port=resolve(attr(attrs, "proxy_port", "proxyPort")),
        non_proxy_hosts=resolve(attr(attrs, "non_proxy_hosts", "nonProxyHosts")),
        file_appended=attr_yn(attrs, "file_appended", "fileAppended"),
        date_time_added=attr_yn(attrs, "date_time_added", "dateTimeAdded"),
        target_extension=resolve(
            attr(attrs, "targetfilename_extension", "targetfilename_extention", "targetFilenameExtention")
        ),
        add_filename_result=attr_yn(attrs, "addfilenameresult", "add_filename_result", default=True),
        headers=headers,
        timeout=float(attr(attrs, "timeout", default="60") or 60),
        ignore_ssl=attr_yn(attrs, "ignoreSsl", "ignoressl", "ignore_ssl"),
        method="POST" if attr(attrs, "uploadfilename", "uploadFilename") else "GET",
    )
    if attr_yn(attrs, "run_every_row", "runForEveryRow"):
        warnings.append("run_every_row=Y is unsupported — executing once")
    if cfg.ignore_ssl:
        warnings.append("ignoreSsl=Y — SSL verification disabled for this request")
    return cfg, warnings


def http_request(cfg: HttpConfig) -> FileOpOutcome:
    if not cfg.url:
        err = ValueError("HTTP url is empty")
        return FileOpOutcome(False, str(err), error=err)

    warnings: list[str] = []
    target = cfg.target_filename
    if target and cfg.date_time_added:
        p = Path(target)
        ext = cfg.target_extension or p.suffix.lstrip(".")
        stamped = stamp_path(p.with_suffix(""), add_date=True, add_time=True)
        target = str(stamped) + (f".{ext}" if ext else "")

    # Prefer requests when available (Databricks); fall back to urllib
    try:
        import requests  # type: ignore

        return _http_via_requests(cfg, target, warnings)
    except ImportError:
        return _http_via_urllib(cfg, target, warnings)


def _http_via_requests(
    cfg: HttpConfig, target: str, warnings: list[str]
) -> FileOpOutcome:
    import requests

    proxies = None
    if cfg.proxy_host:
        port = cfg.proxy_port or "8080"
        proxies = {
            "http": f"http://{cfg.proxy_host}:{port}",
            "https": f"http://{cfg.proxy_host}:{port}",
        }
    auth = (cfg.username, cfg.password) if cfg.username else None
    headers = dict(cfg.headers)
    verify = not cfg.ignore_ssl
    files = None
    data = None
    method = "GET"
    if cfg.upload_filename and Path(cfg.upload_filename).exists():
        method = "POST"
        files = {
            "file": (
                Path(cfg.upload_filename).name,
                open(cfg.upload_filename, "rb"),  # noqa: SIM115
            )
        }

    try:
        resp = requests.request(
            method,
            cfg.url,
            headers=headers,
            auth=auth,
            proxies=proxies,
            timeout=cfg.timeout,
            verify=verify,
            files=files,
            data=data,
        )
    except Exception as exc:  # noqa: BLE001
        return FileOpOutcome(False, str(exc), error=exc, warnings=warnings)
    finally:
        if files:
            files["file"][1].close()

    paths: list[str] = []
    if target:
        ensure_parent(Path(target))
        mode = "ab" if cfg.file_appended else "wb"
        with open(target, mode) as fh:
            fh.write(resp.content)
        paths.append(target)

    ok = 200 <= resp.status_code < 400
    extra = {"status_code": resp.status_code, "method": method}
    if not ok:
        return FileOpOutcome(
            False,
            f"HTTP {resp.status_code}",
            paths,
            warnings,
            error=HTTPError(cfg.url, resp.status_code, resp.reason, hdrs=None, fp=None),
            extra=extra,
        )
    return FileOpOutcome(True, f"HTTP {resp.status_code}", paths, warnings, extra=extra)


def _http_via_urllib(
    cfg: HttpConfig, target: str, warnings: list[str]
) -> FileOpOutcome:
    handlers: list[Any] = []
    if cfg.proxy_host:
        port = cfg.proxy_port or "8080"
        proxy = f"http://{cfg.proxy_host}:{port}"
        handlers.append(ProxyHandler({"http": proxy, "https": proxy}))
    if cfg.username:
        pwd = HTTPPasswordMgrWithDefaultRealm()
        pwd.add_password(None, cfg.url, cfg.username, cfg.password)
        handlers.append(HTTPBasicAuthHandler(pwd))
    if handlers:
        install_opener(build_opener(*handlers))

    data = None
    method = "GET"
    headers = dict(cfg.headers)
    if cfg.upload_filename and Path(cfg.upload_filename).exists():
        method = "POST"
        data = Path(cfg.upload_filename).read_bytes()
        headers.setdefault("Content-Type", "application/octet-stream")

    req = Request(cfg.url, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=cfg.timeout) as resp:  # noqa: S310
            body = resp.read()
            status = getattr(resp, "status", 200)
    except HTTPError as exc:
        return FileOpOutcome(False, str(exc), error=exc, warnings=warnings)
    except URLError as exc:
        return FileOpOutcome(False, str(exc), error=exc, warnings=warnings)

    paths: list[str] = []
    if target:
        ensure_parent(Path(target))
        mode = "ab" if cfg.file_appended else "wb"
        with open(target, mode) as fh:
            fh.write(body)
        paths.append(target)

    ok = 200 <= int(status) < 400
    extra = {"status_code": status, "method": method}
    if not ok:
        return FileOpOutcome(
            False, f"HTTP {status}", paths, warnings, error=ValueError(f"HTTP {status}"), extra=extra
        )
    return FileOpOutcome(True, f"HTTP {status}", paths, warnings, extra=extra)
