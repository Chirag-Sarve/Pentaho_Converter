"""Runtime options for PySpark code generation (Databricks targets)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GenerationConfig:
    """Catalog/schema defaults injected into generated PySpark modules."""

    catalog: str = "main"
    schema: str = "default"
    data_dir: str = "/Volumes/main/default/pentaho_data"

    @classmethod
    def defaults(cls) -> GenerationConfig:
        try:
            from databricks import settings as db_settings

            cfg = db_settings.public_config()
            return cls(
                catalog=cfg.get("catalog") or "main",
                schema=cfg.get("schema") or "default",
                data_dir=cfg.get("data_dir") or "/Volumes/main/default/pentaho_data",
            )
        except Exception:
            return cls()
