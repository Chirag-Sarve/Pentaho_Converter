"""Resolve Pentaho file paths to Databricks-friendly generated code expressions."""

from __future__ import annotations

import re


_INTERNAL_DIR_VARS = (
    "Internal.Transformation.Filename.Directory",
    "Internal.Job.Filename.Directory",
    "Internal.Entry.Current.Directory",
)


def _basename_from_pentaho_path(raw_path: str) -> str:
    text = (raw_path or "").strip().replace("\\", "/")
    if not text:
        return "input.csv"
    if "/" in text:
        return text.rsplit("/", 1)[-1]
    return text


def uses_pentaho_directory_variable(raw_path: str) -> bool:
    """Return True when the path references a Pentaho internal directory variable."""
    text = raw_path or ""
    return any(token in text for token in _INTERNAL_DIR_VARS) or "${" in text


def spark_load_path_expr(raw_path: str) -> str:
    """Return a Python expression for ``.load(...)`` in generated PySpark code."""
    text = (raw_path or "").strip()
    if not text:
        return "''"
    if uses_pentaho_directory_variable(text):
        filename = _basename_from_pentaho_path(re.sub(r"\$\{[^}]+\}", "", text))
        return f"f'{{PENTAHO_DATA_DIR}}/{filename}'"
    return repr(text)
