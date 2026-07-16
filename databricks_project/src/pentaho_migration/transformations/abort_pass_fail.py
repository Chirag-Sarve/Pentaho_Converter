"""PySpark module migrated from Pentaho transformation: Abort_Pass_Fail.

Source: Abort_Pass_Fail.ktr
Independent module — does not call other transformations.
Exposes ``run(spark, config)`` and returns a DataFrame.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    lit,
)
from pentaho_migration.common.databricks_opt import (
    apply_spark_runtime_hints,
    cache_for_reuse,
    get_logger,
    log_event,
    timed,
    unpersist_quiet,
    write_delta,
)

_LOG = get_logger("pentaho_migration.transformations.abort_pass_fail")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``Abort_Pass_Fail`` step-for-step.

    Parameters
    ----------
    spark:
        Active SparkSession.
    config:
        Runtime configuration. Recognized keys include ``catalog``, ``schema``,
        ``data_dir``, transformation parameters, and path overrides.

    Returns
    -------
    DataFrame
        Downstream DataFrame after the final hop(s).
    """
    config = dict(config or {})
    apply_spark_runtime_hints(spark, config)
    log_event(_LOG, "transformation_start")
    catalog = config.get("catalog", "main")
    schema = config.get("schema", 'default')
    data_dir = config.get("data_dir", "/Volumes/main/default/pentaho_data")


    # Step: Generate (DataGrid) [converted]
    # Generate Rows: Generate
    data = [
        ('A',),
        ('B',),
    ]
    df_Generate = spark.createDataFrame(data, ['status'])

    # Step: Filter (FilterRows) [converted]
    # Filter Rows: Filter
    df_OK = df_Generate.filter((col("status") == lit('A')))
    df_Abort = df_Generate.filter(~((col("status") == lit('A'))))
    df_Filter = df_OK

    # Step: Abort (Abort) [converted]
    # Abort: Abort
    # preserved.row_threshold=0
    # preserved.message='failure rows reached Abort'
    # preserved.always_log_rows=False
    # preserved.row_threshold_raw='0'
    # Abort operates on its own failure/branch stream df_Abort (already assigned by upstream Filter/Switch; not overwritten)
    _abort_count_df_Abort = df_Abort.count()
    if _abort_count_df_Abort > 0:  # Abort when any row reaches this step (threshold<=0)
        raise RuntimeError('failure rows reached Abort')

    # Step: OK (Dummy) [converted]
    # Dummy: OK
    # Pass-through step - DataFrame unchanged
    df_Dummy_OK = df_OK

    log_event(_LOG, "transformation_end")
    return df_Dummy_OK
