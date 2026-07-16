"""Transformation module for missing Pentaho file: Payments_Validation.ktr

Referenced by a child JOB but .ktr was not present under Retail_ETL_Project/transformations.

Provides ``run(spark, config)`` so parent JOB orchestration is complete.
When the source .ktr is added to the Retail project, regenerate this module.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pentaho_migration.common.databricks_opt import (
    apply_spark_runtime_hints,
    cache_for_reuse,
    get_logger,
    log_event,
    timed,
    unpersist_quiet,
    write_delta,
)

_LOG = get_logger("pentaho_migration.transformations.retail.payments_validation")

from pyspark.sql import types as T


SOURCE_KTR = 'Payments_Validation.ktr'
SOURCE_MISSING = True


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    config = dict(config or {})
    apply_spark_runtime_hints(spark, config)
    log_event(_LOG, "transformation_start")
    # Return empty typed frame — orchestration continues; no silent skip flag.
    schema = T.StructType(
        [
            T.StructField("_missing_source_ktr", T.StringType(), False),
            T.StructField("run_id", T.StringType(), True),
        ]
    )
    log_event(_LOG, "transformation_end")
    return spark.createDataFrame(
        [('Payments_Validation.ktr', config.get("RUN_ID"))],
        schema=schema,
    )
