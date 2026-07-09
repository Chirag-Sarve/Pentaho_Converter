"""Recursive project scanner for Pentaho .kjb and .ktr files."""

from __future__ import annotations

import logging
from pathlib import Path

from .models import ScanResult

logger = logging.getLogger(__name__)

JOB_SUFFIX = ".kjb"
TRANS_SUFFIX = ".ktr"


def scan_project(root: Path, logs: list[str] | None = None) -> ScanResult:
    """Recursively scan *root* for Pentaho job and transformation files."""
    log = logs if logs is not None else []
    result = ScanResult(root=root)

    if not root.exists():
        log.append(f"ERROR: Workspace path does not exist: {root}")
        return result

    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        suffix = path.suffix.lower()
        if suffix == JOB_SUFFIX:
            result.job_files.append(path)
        elif suffix == TRANS_SUFFIX:
            result.transformation_files.append(path)
        else:
            result.other_files.append(path)

    log.append(
        f"Scan completed: {len(result.job_files)} job(s), "
        f"{len(result.transformation_files)} transformation(s), "
        f"{len(result.other_files)} other file(s)."
    )
    if result.job_files:
        log.append("Jobs found: " + ", ".join(p.name for p in result.job_files))
    if result.transformation_files:
        log.append(
            "Transformations found: "
            + ", ".join(p.name for p in result.transformation_files)
        )

    return result
