"""PySpark module migrated from Pentaho transformation: Sales_Load.

Source: Sales_Load.ktr
Databricks optimizations without changing sort/group semantics.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from pentaho_migration.common.databricks_opt import (
    apply_spark_runtime_hints,
    get_logger,
    log_event,
    repartition_for_write,
    timed,
)

_LOG = get_logger("pentaho_migration.transformations.sales_load")


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``Sales_Load`` step-for-step."""
    config = dict(config or {})
    catalog = config.get("catalog", "main")
    schema = config.get("schema", "analytics")
    data_dir = config.get("data_dir", "/Volumes/main/default/pentaho_data")
    apply_spark_runtime_hints(spark, config)

    with timed(_LOG, "transformation", name="Sales_Load"):
        # Step: CSV file input (CsvInput) [converted]
        df_CSV_file_input = (
            spark.read.format("csv")
            .option("header", True)
            .option("sep", ",")
            .load(config.get("sales_csv", "/data/sales.csv"))
        )

        # Step: Sort rows (SortRows) [converted]
        df_Sort_rows = df_CSV_file_input.orderBy(col("sale_date").asc_nulls_last())

        # Step: Group by (GroupBy) [converted]
        df_Group_by = df_Sort_rows.select(col("region")).distinct()

        # Optional physical partitioning of returned frame for downstream Delta writers.
        # Rows/columns unchanged.
        if config.get("partition_by") or config.get("target_files"):
            df_Group_by = repartition_for_write(
                df_Group_by,
                config.get("partition_by") or ["region"],
                target_files=config.get("target_files"),
            )
            log_event(_LOG, "repartition_for_downstream", columns=config.get("partition_by"))

        return df_Group_by
