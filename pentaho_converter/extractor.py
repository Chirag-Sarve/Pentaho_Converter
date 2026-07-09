"""ZIP extraction and validation for Pentaho projects."""

from __future__ import annotations

import io
import logging
import shutil
import tempfile
import zipfile
from pathlib import Path

logger = logging.getLogger(__name__)


class ZipExtractionError(Exception):
    """Raised when the uploaded ZIP cannot be processed."""


def validate_zip(data: bytes) -> None:
    """Validate that *data* is a non-empty, readable ZIP archive."""
    if not data:
        raise ZipExtractionError("Uploaded file is empty.")

    if len(data) < 4 or data[:2] != b"PK":
        raise ZipExtractionError("Invalid ZIP file: file does not start with ZIP signature.")

    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            names = zf.namelist()
            if not names:
                raise ZipExtractionError("ZIP file is empty (no entries).")
            bad = zf.testzip()
            if bad is not None:
                raise ZipExtractionError(f"Corrupted ZIP entry: {bad}")
    except zipfile.BadZipFile as exc:
        raise ZipExtractionError(f"Corrupted or invalid ZIP file: {exc}") from exc


def extract_zip_to_workspace(data: bytes, logs: list[str] | None = None) -> Path:
    """Extract *data* into a fresh temporary directory and return its path.

    The caller is responsible for cleaning up the directory (see
    ``cleanup_workspace``).
    """
    log = logs if logs is not None else []
    validate_zip(data)
    log.append("ZIP validated successfully.")

    workspace = Path(tempfile.mkdtemp(prefix="pentaho_project_"))
    log.append(f"Created temporary workspace: {workspace}")

    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            zf.extractall(workspace)
        log.append("ZIP extracted successfully.")
    except Exception as exc:
        cleanup_workspace(workspace)
        raise ZipExtractionError(f"Failed to extract ZIP: {exc}") from exc

    return workspace


def cleanup_workspace(workspace: Path) -> None:
    """Remove a temporary workspace directory."""
    if workspace and workspace.exists():
        shutil.rmtree(workspace, ignore_errors=True)
        logger.debug("Cleaned up workspace: %s", workspace)
