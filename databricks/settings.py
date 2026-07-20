"""Default Databricks target settings for the UI.

Connection credentials (workspace URL and personal access token) are entered in
the browser and sent with each API request. This module only supplies
non-secret defaults for the settings panel.
"""

from __future__ import annotations

import os
from pathlib import Path

_ENV_LOADED = False


def _load_dotenv_once() -> None:
    """Load KEY=VALUE pairs from a project-local ``.env`` into os.environ."""
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    _ENV_LOADED = True

    candidates = [
        Path(__file__).resolve().parent.parent / ".env",
        Path.cwd() / ".env",
    ]
    for env_path in candidates:
        if not env_path.is_file():
            continue
        try:
            for raw in env_path.read_text(encoding="utf-8").splitlines():
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
        except OSError:
            continue


def _get(name: str, default: str = "") -> str:
    _load_dotenv_once()
    return os.environ.get(name, default).strip()


def public_config() -> dict:
    """Non-secret default settings for the browser settings panel."""
    return {
        "catalog": _get("DATABRICKS_CATALOG", "workspace"),
        "schema": _get("DATABRICKS_SCHEMA", "default"),
        "storage_account": _get("DATABRICKS_STORAGE_ACCOUNT"),
        "container": _get("DATABRICKS_CONTAINER"),
        "uc_path_prefix": _get("DATABRICKS_UC_PATH_PREFIX"),
        "managed_tables": _get("DATABRICKS_MANAGED_TABLES", "false").lower() == "true",
        "target_format": _get("DATABRICKS_TARGET_FORMAT", "delta"),
        "notebook_dir": _get("DATABRICKS_NOTEBOOK_DIR", "/Workspace/Pentaho_Migration"),
        "data_dir": _get("DATABRICKS_DATA_DIR", "/Volumes/workspace/default/rawdata"),
    }
