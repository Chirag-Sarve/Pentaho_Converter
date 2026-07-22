"""Project configuration for Databricks Free Edition / Workspace.

Generated from Pentaho project: Test_104_1
Generated: 2026-07-22 11:20:37 UTC

Edit TARGET_CATALOG / TARGET_SCHEMA / PENTAHO_DATA_DIR to match your workspace.

Free Edition: use a Unity Catalog Volume path such as
``/Volumes/workspace/default/pentaho_data`` (Spark cannot write ETL data under
``/Workspace/...``). Paid workspaces may use ``main`` or another catalog.
"""

from __future__ import annotations

import logging
from typing import Any, Mapping

# Databricks Unity Catalog targets
TARGET_CATALOG = 'workspace'
TARGET_SCHEMA = 'default'

# Spark-readable/writable data root (UC Volume recommended on Free Edition)
PENTAHO_DATA_DIR = '/Volumes/workspace/default/rawdata'

# Optional JDBC / secret-backed connection placeholders (override via widgets / job params)
JDBC_URL = ""
JDBC_USER = ""
JDBC_PASSWORD = ""

DEFAULT_CONFIG: dict[str, Any] = {
    "TARGET_CATALOG": TARGET_CATALOG,
    "TARGET_SCHEMA": TARGET_SCHEMA,
    "PENTAHO_DATA_DIR": PENTAHO_DATA_DIR,
    "spark_aqe": True,
}


def merge_config(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return DEFAULT_CONFIG merged with runtime overrides."""
    cfg = dict(DEFAULT_CONFIG)
    if overrides:
        cfg.update({k: v for k, v in overrides.items() if v is not None})
    return cfg


def resolve_data_path(path: str, cfg: Mapping[str, Any] | None = None) -> str:
    """Resolve a Pentaho-relative or absolute path against PENTAHO_DATA_DIR.

    Matches Text File Output remapping: local absolute paths such as
    ``/output/high_value_customers.csv`` become
    ``{PENTAHO_DATA_DIR}/high_value_customers.csv`` so job FILE_EXISTS
    checks look at the same place Spark wrote.
    """
    cfg = cfg or {}
    base = str(cfg.get("PENTAHO_DATA_DIR") or PENTAHO_DATA_DIR).rstrip("/")
    text = (path or "").strip().replace("\\", "/")
    if not text:
        return base
    if text.startswith(("dbfs:", "s3://", "abfss://", "wasbs://", "/Volumes/", "file:")):
        return text
    if text.startswith("/data/") or text == "/data":
        return base + text[len("/data") :]
    # Local absolute (/output/...) or relative → basename under data dir
    name = text.rstrip("/").rsplit("/", 1)[-1]
    return f"{base}/{name}"


def ensure_data_dir(spark: Any = None, cfg: Mapping[str, Any] | None = None) -> str:
    """Create the UC volume (if needed) and data directory for Free Edition / paid.

    ``/Volumes/<catalog>/<schema>/<volume>/...`` requires the volume object to
    exist before Spark or dbutils can write. Workspace paths are left alone.
    """
    cfg = cfg or {}
    path = str(cfg.get("PENTAHO_DATA_DIR") or PENTAHO_DATA_DIR).rstrip("/") or PENTAHO_DATA_DIR
    if path.startswith("/Volumes/"):
        parts = [p for p in path.split("/") if p]
        # ["Volumes", catalog, schema, volume, ...]
        if len(parts) >= 4:
            catalog, schema, volume = parts[1], parts[2], parts[3]
            try:
                if spark is not None:
                    spark.sql(
                        f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`"
                    )
                    logging.info(
                        "Ensured UC volume %s.%s.%s for data dir %s",
                        catalog,
                        schema,
                        volume,
                        path,
                    )
            except Exception as exc:
                logging.warning(
                    "Could not CREATE VOLUME %s.%s.%s (%s). "
                    "Create it in Catalog Explorer if writes fail.",
                    catalog,
                    schema,
                    volume,
                    exc,
                )
    try:
        from pathlib import Path as _Path

        _Path(path).mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        _spark = spark or SparkSession.getActiveSession()
        if _spark is not None:
            DBUtils(_spark).fs.mkdirs(path)
    except Exception as exc:
        logging.warning("ensure_data_dir mkdirs soft-fail for %s: %s", path, exc)
    return path


def apply_spark_runtime_hints(spark: Any, cfg: Mapping[str, Any] | None = None) -> None:
    """Apply optional Spark session hints when the runtime allows them.

    Paid / classic clusters accept most of these keys. Databricks Free Edition
    and serverless often reject them with CONFIG_NOT_AVAILABLE — skip quietly
    so the same generated project runs on both.
    """
    cfg = cfg or {}

    def _set(key: str, value: str) -> None:
        try:
            spark.conf.set(key, value)
        except Exception:
            # Free Edition / serverless / restricted runtimes may forbid the key.
            pass

    shuffle = cfg.get("spark_shuffle_partitions")
    if shuffle:
        _set("spark.sql.shuffle.partitions", str(int(shuffle)))
    aqe = cfg.get("spark_aqe", True)
    _set("spark.sql.adaptive.enabled", "true" if aqe else "false")
    _set("spark.databricks.delta.optimizeWrite.enabled", "true")
    _set("spark.databricks.delta.autoCompact.enabled", "true")
