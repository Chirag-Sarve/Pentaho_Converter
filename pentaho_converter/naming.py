"""Safe module / package naming for generated Databricks projects."""

from __future__ import annotations

import re
from pathlib import Path


def safe_module_name(name: str) -> str:
    """Convert a Pentaho job/transformation name into a valid Python module stem.

    Preserves PascalCase / underscores from Pentaho filenames where possible
    (e.g. ``Load_Customer_Data.kjb`` → ``Load_Customer_Data``).
    """
    stem = Path(str(name).replace("\\", "/")).stem if "/" in str(name) or "\\" in str(name) else str(name)
    if stem.lower().endswith(".kjb") or stem.lower().endswith(".ktr"):
        stem = Path(stem).stem
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", stem.strip())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    if not cleaned:
        cleaned = "unnamed"
    if cleaned[0].isdigit():
        cleaned = f"m_{cleaned}"
    return cleaned


def safe_package_root(project_name: str) -> str:
    """Root folder name for the generated project (e.g. Retail_ETL)."""
    name = project_name.strip() or "Pentaho_ETL"
    # Common ZIP suffixes
    for suffix in ("_Project", "_project", "-Project", " Project"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    return safe_module_name(name) or "Pentaho_ETL"


def module_import_path(folder: str, module: str) -> str:
    """Return ``folder.ModuleName`` import path."""
    return f"{folder}.{module}"
