"""Resolve Pentaho file paths to Databricks-friendly generated code expressions."""

from __future__ import annotations

import re


_INTERNAL_DIR_VARS = (
    "Internal.Transformation.Filename.Directory",
    "Internal.Job.Filename.Directory",
    "Internal.Entry.Current.Directory",
)

_DRIVE_PATH_RE = re.compile(r"^[A-Za-z]:[\\/]")
_CLOUD_OR_VOLUME_PREFIXES = (
    "/Volumes/",
    "s3://",
    "s3a://",
    "dbfs:",
    "abfss://",
    "wasbs://",
    "gs://",
)


def _basename_from_pentaho_path(raw_path: str) -> str:
    text = (raw_path or "").strip().replace("\\", "/")
    if not text:
        return ""
    if "/" in text:
        return text.rsplit("/", 1)[-1]
    return text


def uses_pentaho_directory_variable(raw_path: str) -> bool:
    """Return True when the path references a Pentaho internal directory variable."""
    text = raw_path or ""
    return any(token in text for token in _INTERNAL_DIR_VARS) or "${" in text


def is_local_machine_path(raw_path: str) -> bool:
    """Return True for Windows drive / UNC paths that must not appear in Databricks code."""
    text = (raw_path or "").strip()
    if not text:
        return False
    if _DRIVE_PATH_RE.match(text):
        return True
    if text.startswith("\\\\") or text.startswith("//"):
        # UNC shares (\\server\share) — exclude cloud URIs handled elsewhere.
        if "://" not in text:
            return True
    return False


def _is_cloud_or_volume_path(text: str) -> bool:
    return any(text.startswith(prefix) for prefix in _CLOUD_OR_VOLUME_PREFIXES)


def spark_load_path_expr(raw_path: str) -> str:
    """Return a Python expression for ``.load(...)`` / ``.save(...)`` in generated PySpark.

    Local/Windows paths and Pentaho directory variables are remapped under
    ``PENTAHO_DATA_DIR``. Cloud and Unity Catalog Volumes paths are kept as-is.
    """
    text = (raw_path or "").strip()
    if not text:
        return "''"
    if uses_pentaho_directory_variable(text) or is_local_machine_path(text):
        cleaned = re.sub(r"\$\{[^}]+\}", "", text)
        filename = _basename_from_pentaho_path(cleaned) or "<input_file>"
        return f"f'{{PENTAHO_DATA_DIR}}/{filename}'"
    return repr(text)


def spark_save_path_expr(raw_path: str, *, placeholder: str = "<output_name>") -> str:
    """Return a Python expression for output ``.save(...)`` / ``.json(...)`` paths.

    Never emits Windows local paths. Prefer ``f'{PENTAHO_DATA_DIR}/<name>'`` while
    preserving the original basename when available. Cloud/Volumes URIs are unchanged.
    """
    text = (raw_path or "").strip()
    if not text:
        return f"f'{{PENTAHO_DATA_DIR}}/{placeholder}'"

    if _is_cloud_or_volume_path(text.replace("\\", "/")):
        return repr(text.replace("\\", "/"))

    if uses_pentaho_directory_variable(text) or is_local_machine_path(text):
        cleaned = re.sub(r"\$\{[^}]+\}", "", text)
        filename = _basename_from_pentaho_path(cleaned) or placeholder
        return f"f'{{PENTAHO_DATA_DIR}}/{filename}'"

    # Absolute POSIX or relative paths: keep basename under PENTAHO_DATA_DIR for
    # Databricks portability (never invent folder hierarchy).
    filename = _basename_from_pentaho_path(text) or placeholder
    return f"f'{{PENTAHO_DATA_DIR}}/{filename}'"


def normalize_text_file_basename(raw_path: str, extension: str = "") -> str:
    """Normalize a Pentaho file name so Text File Output / Input use the same basename.

    Appends ``extension`` when the last path segment has no dot (matches Text File Output).
    """
    text = (raw_path or "").strip().replace("\\", "/")
    if not text:
        return ""
    ext = (extension or "").strip().lstrip(".")
    last = text.rsplit("/", 1)[-1]
    if ext and not last.lower().endswith(f".{ext.lower()}") and "." not in last:
        text = f"{text}.{ext}"
    return text


def spark_text_file_path_expr(
    raw_path: str,
    extension: str = "",
    *,
    placeholder: str = "<file>",
) -> str:
    """Shared runtime path expression for Text File Output writes and Input reads.

    Always remaps local / variable paths under ``PENTAHO_DATA_DIR`` so write→read
    workflows resolve to the same Spark CSV directory path.
    """
    normalized = normalize_text_file_basename(raw_path, extension)
    return spark_save_path_expr(normalized or raw_path, placeholder=placeholder)
