"""PySpark module migrated from Pentaho transformation: Step_Converter_Test.

Source: Step_Converter_Test.ktr
Independent module — does not call other transformations.
Exposes ``run(spark, config)`` and returns a DataFrame.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    trim,
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

_LOG = get_logger("pentaho_migration.transformations.step_converter_test")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``Step_Converter_Test`` step-for-step.

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
        (1, 'John', 20),
        (2, 'David', 30),
    ]
    df_Generate_Rows = spark.createDataFrame(data, ['id', 'name', 'age'])

    # Step: Add constants (Constant) [converted]
    # Add Constants: Add constants
    df_Add_constants = df_Generate_Rows
    df_Add_constants = df_Add_constants.withColumn("Country", lit('India'))

    # Step: Calculator (Calculator) [converted]
    # Calculator: Calculator
    df_Calculator = df_Add_constants
    df_Calculator = df_Calculator.withColumn("age_plus_one", ((col("age") + col("id"))).cast('int'))

    # Step: String ops (StringOperations) [converted]
    # String Operations: String ops
    df_String_ops = df_Calculator
    df_String_ops = df_String_ops.withColumn("name_upper", upper(trim(col("name").cast("string"))))

    # Step: Filter rows (FilterRows) [converted]
    # Filter Rows: Filter rows
    df_Filter_rows = df_String_ops.filter(((col("age") > lit(18)) & (col("Country") == lit('India'))))

    log_event(_LOG, "transformation_end")
    return df_Filter_rows
