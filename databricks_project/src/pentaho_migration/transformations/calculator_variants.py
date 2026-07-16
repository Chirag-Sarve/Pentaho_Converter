"""PySpark module migrated from Pentaho transformation: Calculator_Variants.

Source: Calculator_Variants.ktr
Independent module — does not call other transformations.
Exposes ``run(spark, config)`` and returns a DataFrame.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_add,
    length,
    lit,
    lower,
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

_LOG = get_logger("pentaho_migration.transformations.calculator_variants")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``Calculator_Variants`` step-for-step.

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
        (2, 10.5, 5, 'alice', '2024-01-15', 1),
    ]
    df_Generate_Rows = spark.createDataFrame(data, ['qty', 'price', 'rate', 'name', 'order_date', 'tmp_a'])

    # Step: Calc short names (Calculator) [converted]
    # Calculator: Calc short names
    df_Calc_short_names = df_Generate_Rows
    df_Calc_short_names = df_Calc_short_names.withColumn("line_total", ((col("qty") * col("price"))).cast('double'))
    df_Calc_short_names = df_Calc_short_names.withColumn("name_upper", (upper(col("name"))).cast('string'))

    # Step: Calc numeric IDs (Calculator) [converted]
    # Calculator: Calc numeric IDs
    df_Calc_numeric_IDs = df_Calc_short_names
    df_Calc_numeric_IDs = df_Calc_numeric_IDs.withColumn("sum_ids", ((col("qty") + col("tmp_a"))).cast('int'))
    df_Calc_numeric_IDs = df_Calc_numeric_IDs.withColumn("name_len", (length(col("name"))).cast('int'))

    # Step: Calc long desc (Calculator) [converted]
    # Calculator: Calc long desc
    df_Calc_long_desc = df_Calc_numeric_IDs
    df_Calc_long_desc = df_Calc_long_desc.withColumn("pct", ((lit(100) * col("line_total") / col("rate"))).cast('double'))
    df_Calc_long_desc = df_Calc_long_desc.withColumn("diff", ((col("line_total") - col("tmp_a"))).cast('double'))

    # Step: Calc multi ops (Calculator) [converted]
    # Calculator: Calc multi ops
    df_Calc_multi_ops = df_Calc_long_desc
    df_Calc_multi_ops = df_Calc_multi_ops.withColumn("days_out", (date_add(col("order_date"), lit(1).cast('int'))).cast('timestamp'))
    df_Calc_multi_ops = df_Calc_multi_ops.withColumn("name_lower", (lower(col("name"))).cast('string'))
    df_Calc_multi_ops = df_Calc_multi_ops.withColumn("discounted", ((col("line_total") - (col("line_total") * col("rate") / lit(100)))).cast('double'))
    df_Calc_multi_ops = df_Calc_multi_ops.drop("line_total", "rate")

    # Step: Calc unsupported (Calculator) [converted]
    # Calculator: Calc unsupported — JARO similarity (Pentaho Calculator JARO)
    df_Calc_unsupported = df_Calc_multi_ops

    def _jaro_similarity(s1: str | None, s2: str | None) -> float:
        """Classic Jaro similarity used by PDI Calculator JARO."""
        if s1 is None or s2 is None:
            return 0.0
        s1, s2 = str(s1), str(s2)
        if s1 == s2:
            return 1.0
        len1, len2 = len(s1), len(s2)
        if len1 == 0 or len2 == 0:
            return 0.0
        match_dist = max(len1, len2) // 2 - 1
        match_dist = max(0, match_dist)
        s1_matches = [False] * len1
        s2_matches = [False] * len2
        matches = 0
        transpositions = 0
        for i in range(len1):
            start = max(0, i - match_dist)
            end = min(i + match_dist + 1, len2)
            for j in range(start, end):
                if s2_matches[j] or s1[i] != s2[j]:
                    continue
                s1_matches[i] = True
                s2_matches[j] = True
                matches += 1
                break
        if matches == 0:
            return 0.0
        k = 0
        for i in range(len1):
            if not s1_matches[i]:
                continue
            while not s2_matches[k]:
                k += 1
            if s1[i] != s2[k]:
                transpositions += 1
            k += 1
        return (
            matches / len1
            + matches / len2
            + (matches - transpositions / 2) / matches
        ) / 3.0

    from pyspark.sql.functions import udf
    from pyspark.sql.types import DoubleType

    _jaro_udf = udf(_jaro_similarity, DoubleType())
    # field_a=name, field_b=name_upper (from Calculator_Variants.ktr)
    df_Calc_unsupported = df_Calc_unsupported.withColumn(
        "jaro_score",
        _jaro_udf(col("name"), col("name_upper")).cast("double"),
    )

    # Step: Calc empty (Calculator) [partial]
    # Calculator: Calc empty
    # WARNING: Calculator 'Calc empty': no calculation metadata found; preserving upstream DataFrame
    df_Calc_empty = df_Calc_unsupported

    log_event(_LOG, "transformation_end")
    return df_Calc_empty
