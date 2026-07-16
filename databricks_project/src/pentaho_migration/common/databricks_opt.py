"""Databricks-oriented helpers for migrated Pentaho PySpark modules.

Optimizations only — helpers must not alter filter/join/aggregate semantics.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Iterable, Mapping, Sequence

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, col


def get_logger(name: str) -> logging.Logger:
    """Return a module logger (Databricks captures std logging as structured records)."""
    logger = logging.getLogger(name)
    if not logger.handlers and not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
    return logger


def log_event(logger: logging.Logger, event: str, **fields: Any) -> None:
    """Emit one structured log line as JSON for Databricks log analytics."""
    payload = {"event": event, **{k: _jsonable(v) for k, v in fields.items()}}
    logger.info("%s", json.dumps(payload, default=str, separators=(",", ":")))


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return str(value)


def apply_spark_runtime_hints(spark: SparkSession, config: Mapping[str, Any]) -> None:
    """Apply non-semantic Spark/Databricks session hints from config."""
    shuffle = config.get("spark_shuffle_partitions")
    if shuffle:
        spark.conf.set("spark.sql.shuffle.partitions", str(int(shuffle)))
    aqe = config.get("spark_aqe", True)
    spark.conf.set("spark.sql.adaptive.enabled", "true" if aqe else "false")
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")


def maybe_broadcast(df: DataFrame, *, enabled: bool = True) -> DataFrame:
    """Mark a small side for broadcast — join results unchanged."""
    return broadcast(df) if enabled else df


def broadcast_join(
    left: DataFrame,
    right: DataFrame,
    *,
    on: str | Sequence[str] | DataFrame | None = None,
    how: str = "left",
    broadcast_right: bool = True,
) -> DataFrame:
    """DataFrame join with optional broadcast of the right side (lookup pattern)."""
    rhs = maybe_broadcast(right, enabled=broadcast_right)
    if on is None:
        return left.join(rhs, how=how)
    return left.join(rhs, on=on, how=how)


def cache_for_reuse(
    df: DataFrame,
    *,
    enabled: bool = True,
    level: StorageLevel = StorageLevel.MEMORY_AND_DISK,
) -> DataFrame:
    """Cache a DataFrame that fans out to multiple sinks/actions (same rows)."""
    if not enabled:
        return df
    return df.persist(level)


def unpersist_quiet(df: DataFrame) -> None:
    try:
        df.unpersist()
    except Exception:  # noqa: BLE001 — best-effort cleanup
        pass


def existing_columns(df: DataFrame, candidates: Iterable[str]) -> list[str]:
    cols = set(df.columns)
    return [c for c in candidates if c in cols]


def repartition_for_write(
    df: DataFrame,
    partition_cols: Sequence[str] | None,
    *,
    target_files: int | None = None,
) -> DataFrame:
    """Repartition by partition columns (and optional file count) before Delta write.

    Does not filter/transform rows — physical layout only.
    """
    cols = existing_columns(df, partition_cols or [])
    if cols and target_files:
        return df.repartition(int(target_files), *[col(c) for c in cols])
    if cols:
        return df.repartition(*[col(c) for c in cols])
    if target_files:
        return df.repartition(int(target_files))
    return df


def write_delta(
    df: DataFrame,
    table: str,
    *,
    mode: str = "append",
    partition_by: Sequence[str] | None = None,
    target_files: int | None = None,
    options: Mapping[str, str] | None = None,
    spark: SparkSession | None = None,
) -> DataFrame:
    """Write DataFrame to a Delta Lake table (Unity Catalog / Hive metastore).

    Row content is unchanged; only write format/layout options are applied.
    """
    if spark is not None and "." in table:
        parts = table.split(".")
        if len(parts) >= 2:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {'.'.join(parts[:-1])}")

    part_cols = existing_columns(df, partition_by or [])
    out = repartition_for_write(df, part_cols, target_files=target_files)
    writer = out.write.format("delta").mode(mode)
    if part_cols:
        writer = writer.partitionBy(*part_cols)
    for key, value in (options or {}).items():
        writer = writer.option(key, value)
    # Optimize write path defaults for Databricks Delta
    writer = writer.option("overwriteSchema", "false")
    writer.saveAsTable(table)
    return df


def timed(logger: logging.Logger, event: str, **fields: Any):
    """Context manager for structured duration logging."""

    class _Timer:
        def __enter__(self):
            self.t0 = time.perf_counter()
            log_event(logger, f"{event}_start", **fields)
            return self

        def __exit__(self, exc_type, exc, tb):
            ms = int((time.perf_counter() - self.t0) * 1000)
            if exc_type is None:
                log_event(logger, f"{event}_end", duration_ms=ms, **fields)
            else:
                log_event(
                    logger,
                    f"{event}_error",
                    duration_ms=ms,
                    error=str(exc),
                    **fields,
                )
            return False

    return _Timer()
