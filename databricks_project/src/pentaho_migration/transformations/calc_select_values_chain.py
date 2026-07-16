"""PySpark module migrated from Pentaho transformation: Calc_SelectValues_Chain.

Source: Calc_SelectValues_Chain.ktr
Independent module — does not call other transformations.
Exposes ``run(spark, config)`` and returns a DataFrame.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    length,
    lit,
    upper,
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

_LOG = get_logger("pentaho_migration.transformations.calc_select_values_chain")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``Calc_SelectValues_Chain`` step-for-step.

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
        (3, 20, 10, 'SKU-1', 'x'),
    ]
    df_Generate_Rows = spark.createDataFrame(data, ['qty', 'unit_price', 'discount_pct', 'sku', 'scratch'])

    # Step: Calc amounts (Calculator) [converted]
    # Calculator: Calc amounts
    df_Calc_amounts = df_Generate_Rows
    df_Calc_amounts = df_Calc_amounts.withColumn("gross", ((col("qty") * col("unit_price"))).cast('double'))
    df_Calc_amounts = df_Calc_amounts.withColumn("net", ((col("gross") - (col("gross") * col("discount_pct") / lit(100)))).cast('double'))
    df_Calc_amounts = df_Calc_amounts.withColumn("sku_upper", (upper(col("sku"))).cast('string'))

    # Step: Select values (SelectValues) [converted]
    # Select Values: Select values
    df_Select_values = df_Calc_amounts.select(col("sku_upper").alias("sku"), col("gross"), col("net").cast("double").alias("net"), col("qty"))
    # preserved.meta length='18' precision='4' for net

    # Step: Calc final (Calculator) [converted]
    # Calculator: Calc final
    df_Calc_final = df_Select_values
    df_Calc_final = df_Calc_final.withColumn("unit_net", ((col("net") / col("qty"))).cast('double'))

    log_event(_LOG, "transformation_end")
    return df_Calc_final
