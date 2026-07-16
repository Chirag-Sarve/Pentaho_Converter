"""PySpark module migrated from Pentaho transformation: SelectValues_Variants.

Source: SelectValues_Variants.ktr
Independent module — does not call other transformations.
Exposes ``run(spark, config)`` and returns a DataFrame.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    length,
    substring,
    to_date,
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

_LOG = get_logger("pentaho_migration.transformations.select_values_variants")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``SelectValues_Variants`` step-for-step.

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
        (1, 'Alice', 12.5, 'ACTIVE', 'x', 'y', '2024-06-01'),
    ]
    df_Generate_Rows = spark.createDataFrame(data, ['customer_id', 'customer_name', 'amount', 'status', 'tmp_col', 'debug', 'as_of'])

    # Step: SV select rename (SelectValues) [converted]
    # Select Values: SV select rename
    df_SV_select_rename = df_Generate_Rows.select(col("customer_id").alias("id"), col("customer_name").alias("name"), col("amount"), col("status"), col("tmp_col"), col("debug"), col("as_of"))
    # preserved.meta length='10' precision='0' for customer_id

    # Step: SV remove under fields (SelectValues) [converted]
    # Select Values: SV remove under fields
    df_SV_remove_under_fields = df_SV_select_rename.select(col("id"), col("name"), col("amount"), col("status"), col("as_of"))

    # Step: SV meta only (SelectValues) [converted]
    # Select Values: SV meta only
    df_SV_meta_only = df_SV_remove_under_fields
    df_SV_meta_only = df_SV_meta_only.withColumn("amount", col("amount").cast("bigint"))
    # preserved.meta length='9' precision='0' for amount
    df_SV_meta_only = df_SV_meta_only.withColumn("as_of", to_date(col("as_of").cast("string"), 'yyyy-MM-dd'))

    # Step: SV remove step level (SelectValues) [converted]
    # Select Values: SV remove step level
    df_SV_remove_step_level = df_SV_meta_only.select(col("id"), col("name"), col("amount"), col("as_of"))

    # Step: SV select unspecified (SelectValues) [converted]
    # Select Values: SV select unspecified
    df_SV_select_unspecified = df_SV_remove_step_level.select(col("id").alias("cust_id"), col("amount"), col("as_of"), col("name"))
    # preserved.select_unspecified=Y — unspecified columns appended

    # Step: SV full meta (SelectValues) [converted]
    # Select Values: SV full meta
    df_SV_full_meta = df_SV_select_unspecified.select(col("cust_id"), substring(col("name").cast("string"), 1, 40).alias("full_name"), col("amount").cast("double").alias("amt"))
    # preserved.meta length='12' precision='2' for amount
    # preserved.meta conversion_mask='#,##0.00' for amount
    # preserved.meta decimal_symbol='.' for amount
    # preserved.meta grouping_symbol=',' for amount
    # preserved.meta currency_symbol='$' for amount
    # preserved.meta encoding='UTF-8' for amount
    # preserved.meta storage_type='normal' for amount

    log_event(_LOG, "transformation_end")
    return df_SV_full_meta
