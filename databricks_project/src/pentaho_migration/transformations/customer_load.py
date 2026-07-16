"""PySpark module migrated from Pentaho transformation: Customer_Load.

Source: Customer_Load.ktr
Independent module — does not call other transformations.
Exposes ``run(spark, config)`` and returns a DataFrame.

Databricks optimizations (no business-logic change): DataFrame API, Delta Lake
writes, optional partitioning, structured logging, session AQE hints.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

from pentaho_migration.common.databricks_opt import (
    apply_spark_runtime_hints,
    get_logger,
    log_event,
    timed,
    write_delta,
)

_LOG = get_logger("pentaho_migration.transformations.customer_load")


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``Customer_Load`` step-for-step."""
    config = dict(config or {})
    catalog = config.get("catalog", "main")
    schema = config.get("schema", "analytics")
    data_dir = config.get("data_dir", "/Volumes/main/default/pentaho_data")
    BATCH_DATE = config.get("BATCH_DATE", "2026-01-01")
    apply_spark_runtime_hints(spark, config)

    with timed(_LOG, "transformation", name="Customer_Load", BATCH_DATE=BATCH_DATE):
        # Step: Table input (TableInput) [converted]
        df_Table_input = spark.sql(
            "SELECT customer_id, customer_name, status FROM customers"
        )

        # Step: Filter rows (FilterRows) [converted]
        df_Filter_rows = df_Table_input.filter(col("status") == lit("ACTIVE"))

        # Step: Select values (SelectValues) [converted]
        df_Select_values = df_Filter_rows.select(
            col("customer_id").cast("bigint").alias("customer_id"),
            col("customer_name").cast("string").alias("customer_name"),
        )

        # Step: Table output (TableOutput) [converted]
        df_Table_output = df_Select_values
        table = f"{catalog}.{schema}.dim_customer"
        write_delta(
            df_Table_output,
            table,
            mode="append",
            partition_by=config.get("partition_by") or [],
            target_files=config.get("target_files"),
            spark=spark,
        )
        log_event(_LOG, "delta_write", table=table, mode="append")
        return df_Table_output
