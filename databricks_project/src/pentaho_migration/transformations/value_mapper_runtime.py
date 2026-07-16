"""PySpark module migrated from Pentaho transformation: ValueMapper_Runtime.

Source: ValueMapper_Runtime.ktr
Independent module — does not call other transformations.
Exposes ``run(spark, config)`` and returns a DataFrame.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    lit,
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

_LOG = get_logger("pentaho_migration.transformations.value_mapper_runtime")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``ValueMapper_Runtime`` step-for-step.

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
        ('Z',),
        (None,),
    ]
    df_Generate = spark.createDataFrame(data, ['status'])

    # Step: Map status (ValueMapper) [converted]
    # Value Mapper: Map status
    df_Map_status = df_Generate.withColumn("label", when((col("status").isNull() | (col("status") == lit(''))), lit('EMPTY')).when((col("status") == lit('A')), lit('ACTIVE')).when((col("status") == lit('B')), lit('BETA')).otherwise(lit('OTHER')))
    # preserved.case_sensitive=True mappings=3 default='OTHER'

    log_event(_LOG, "transformation_end")
    return df_Map_status
