"""Runtime options for PySpark code generation (Databricks targets)."""

from __future__ import annotations

from dataclasses import dataclass

# Free Edition default: Unity Catalog volume under catalog ``workspace``
# (not ``main``, and not /Workspace/... which Spark cannot use for ETL writes).
DEFAULT_DATA_DIR = "/Volumes/workspace/default/rawdata"


@dataclass(frozen=True)
class GenerationConfig:
    """Catalog/schema defaults injected into generated PySpark modules.

    Defaults target Databricks Free Edition:
    * catalog ``workspace`` (common Free Edition UC catalog)
    * data under a UC Volume Spark can read/write
    """

    catalog: str = "workspace"
    schema: str = "default"
    data_dir: str = DEFAULT_DATA_DIR

    @classmethod
    def defaults(cls) -> GenerationConfig:
        try:
            from databricks import settings as db_settings

            cfg = db_settings.public_config()
            return cls(
                catalog=cfg.get("catalog") or "workspace",
                schema=cfg.get("schema") or "default",
                data_dir=cfg.get("data_dir") or DEFAULT_DATA_DIR,
            )
        except Exception:
            return cls()
