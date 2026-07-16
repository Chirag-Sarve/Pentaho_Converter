"""PySpark module migrated from Pentaho transformation: Generator_Audit_Workflow.

Source: Generator_Audit_Workflow.ktr
Independent module — does not call other transformations.
Exposes ``run(spark, config)`` and returns a DataFrame.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_date,
    current_timestamp,
    lit,
    lower,
    upper,
    when,
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

_LOG = get_logger("pentaho_migration.transformations.generator_audit_workflow")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``Generator_Audit_Workflow`` step-for-step.

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


    # Step: Generate Rows (DataGrid) [converted]
    # Generate Rows: Generate Rows
    data = [
        (1, 'Alice', 2, 10, 'A'),
        (2, 'Bob', 3, 20, 'B'),
    ]
    df_Generate_Rows = spark.createDataFrame(data, ['id', 'name', 'qty', 'price', 'status'])

    # Step: Add constants (Constant) [converted]
    # Add Constants: Add constants
    df_Add_constants = df_Generate_Rows
    df_Add_constants = df_Add_constants.withColumn("currency", lit('USD'))

    # Step: Calc metrics (Calculator) [converted]
    # Calculator: Calc metrics
    df_Calc_metrics = df_Add_constants
    df_Calc_metrics = df_Calc_metrics.withColumn("total", ((col("qty") * col("price"))).cast('double'))
    df_Calc_metrics = df_Calc_metrics.withColumn("name_u", (upper(col("name"))).cast('string'))

    # Step: Filter active (FilterRows) [converted]
    # Filter Rows: Filter active
    df_Filter_active = df_Calc_metrics.filter((col("status") == lit('A')))

    # Step: Select values (SelectValues) [converted]
    # Select Values: Select values
    df_Select_values = df_Filter_active.select(col("id"), col("name_u").alias("name"), col("total"), col("status"), col("currency"))

    # Step: Map status (ValueMapper) [converted]
    # Value Mapper: Map status
    df_Map_status = df_Select_values.withColumn("status_label", when((col("status").isNull() | (col("status") == lit(''))), lit('EMPTY')).when((lower(col("status")) == lower(lit('A'))), lit('ACTIVE')).when((lower(col("status")) == lower(lit('B'))), lit('BLOCKED')).otherwise(lit('UNKNOWN')))
    # preserved.case_sensitive=False mappings=3 default='UNKNOWN'

    # Step: System info (SystemInfo) [converted]
    # System Info: System info
    df_System_info = df_Map_status
    df_System_info = df_System_info.withColumn("run_date", current_date())
    df_System_info = df_System_info.withColumn("run_ts", current_timestamp())

    # Step: Guard abort (Abort) [converted]
    # Abort: Guard abort
    # preserved.row_threshold=1000000
    # preserved.message='Should not abort on audit sample'
    # preserved.always_log_rows=False
    # preserved.row_threshold_raw='1000000'
    df_Guard_abort = df_System_info
    _abort_count_df_Guard_abort = df_Guard_abort.count()
    if _abort_count_df_Guard_abort >= 1000000:  # Abort after 1000000 row(s)
        raise RuntimeError('Should not abort on audit sample')

    log_event(_LOG, "transformation_end")
    return df_Guard_abort
