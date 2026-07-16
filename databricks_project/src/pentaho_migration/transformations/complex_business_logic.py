"""PySpark module migrated from Pentaho transformation: Complex_Business_Logic.

Source: Complex_Business_Logic.ktr
Independent module — DataFrame API with Databricks optimizations.
Business logic (filters, calcs, joins, aggs, sinks) is unchanged.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    count,
    length,
    lit,
    regexp_replace,
    substring,
    to_date,
    trim,
    upper,
    when,
    sum as _sum,
)

from pentaho_migration.common.databricks_opt import (
    apply_spark_runtime_hints,
    cache_for_reuse,
    get_logger,
    log_event,
    maybe_broadcast,
    timed,
    unpersist_quiet,
    write_delta,
)

_LOG = get_logger("pentaho_migration.transformations.complex_business_logic")


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``Complex_Business_Logic`` step-for-step."""
    config = dict(config or {})
    catalog = config.get("catalog", "main")
    schema = config.get("schema", "analytics")
    data_dir = config.get("data_dir", "/Volumes/main/default/pentaho_data")
    BATCH_DATE = config.get("BATCH_DATE", "2026-07-01")
    MIN_ORDER_AMOUNT = config.get("MIN_ORDER_AMOUNT", "100")
    DISCOUNT_RATE = config.get("DISCOUNT_RATE", "0.05")
    apply_spark_runtime_hints(spark, config)

    with timed(
        _LOG,
        "transformation",
        name="Complex_Business_Logic",
        BATCH_DATE=BATCH_DATE,
    ):
        # Step: CSV product prices (CsvInput) [converted]
        df_CSV_product_prices = (
            spark.read.format("csv")
            .option("header", True)
            .option("sep", ",")
            .load(config.get("product_prices_csv", "/data/inbound/product_prices.csv"))
        )

        # Step: Region reference grid (DataGrid) [converted]
        data = [
            ("NA", "North America", "T1"),
            ("EU", "Europe", "T1"),
            ("APAC", "Asia Pacific", "T2"),
            ("LATAM", "Latin America", "T2"),
            ("MEA", "Middle East Africa", "T3"),
        ]
        df_Region_reference_grid = spark.createDataFrame(
            data, ["region_code", "region_name", "region_tier"]
        )

        # Step: Table input orders (TableInput) [converted]
        df_Table_input_orders = spark.sql(
            "SELECT\n"
            "  o.order_id,\n"
            "  o.customer_id,\n"
            "  o.customer_name,\n"
            "  o.product_code,\n"
            "  o.quantity,\n"
            "  o.unit_price,\n"
            "  o.order_amount,\n"
            "  o.order_date,\n"
            "  o.status,\n"
            "  o.region_code\n"
            "FROM staging.customer_orders o\n"
            "WHERE o.order_date >= '2026-07-01'"
        )

        # Step: Filter active orders (FilterRows) [converted]
        df_Filter_active_orders = df_Table_input_orders.filter(
            (
                (col("status") == lit("ACTIVE"))
                & (col("order_amount") >= lit(100.0))
            )
            & (col("quantity") > lit(0))
        )

        # Step: Replace null amounts (ReplaceNull) [converted]
        df_Replace_null_amounts = df_Filter_active_orders
        df_Replace_null_amounts = df_Replace_null_amounts.withColumn(
            "order_amount",
            when(col("order_amount").isNull(), 0).otherwise(col("order_amount")),
        )
        df_Replace_null_amounts = df_Replace_null_amounts.withColumn(
            "unit_price",
            when(col("unit_price").isNull(), 0).otherwise(col("unit_price")),
        )
        df_Replace_null_amounts = df_Replace_null_amounts.withColumn(
            "quantity",
            when(col("quantity").isNull(), 0).otherwise(col("quantity")),
        )

        # Step: Add batch constants (Constant) [converted]
        df_Add_batch_constants = df_Replace_null_amounts
        df_Add_batch_constants = df_Add_batch_constants.withColumn(
            "batch_date", to_date(lit("2026-07-01"))
        )
        df_Add_batch_constants = df_Add_batch_constants.withColumn(
            "source_system", lit("ERP_ORDERS")
        )
        df_Add_batch_constants = df_Add_batch_constants.withColumn(
            "is_premium", lit(True)
        )

        # Step: Calculate line metrics (Calculator) [converted]
        df_Calculate_line_metrics = df_Add_batch_constants
        df_Calculate_line_metrics = df_Calculate_line_metrics.withColumn(
            "line_total", ((col("quantity") * col("unit_price"))).cast("double")
        )
        df_Calculate_line_metrics = df_Calculate_line_metrics.withColumn(
            "discount_amount", ((col("line_total") * col("unit_price"))).cast("double")
        )
        df_Calculate_line_metrics = df_Calculate_line_metrics.withColumn(
            "net_revenue",
            ((col("line_total") - col("discount_amount"))).cast("double"),
        )
        df_Calculate_line_metrics = df_Calculate_line_metrics.withColumn(
            "customer_name_upper", (upper(col("customer_name"))).cast("string")
        )
        df_Calculate_line_metrics = df_Calculate_line_metrics.withColumn(
            "name_length", (length(col("customer_name"))).cast("int")
        )

        # Step: Apply discount formula (Formula) [converted]
        df_Apply_discount_formula = df_Calculate_line_metrics
        df_Apply_discount_formula = df_Apply_discount_formula.withColumn(
            "adjusted_net_revenue",
            col("net_revenue") - (col("discount_amount") * 0.05),
        )

        # Step: Normalize customer name (StringOperations) [converted]
        df_Normalize_customer_name = df_Apply_discount_formula
        df_Normalize_customer_name = df_Normalize_customer_name.withColumn(
            "customer_name_clean", upper(trim(col("customer_name").cast("string")))
        )
        df_Normalize_customer_name = df_Normalize_customer_name.withColumn(
            "product_code_sub", substring(col("product_code").cast("string"), 2, 9)
        )

        # Step: Fix product codes (ReplaceInString) [converted]
        df_Fix_product_codes = df_Normalize_customer_name.withColumn(
            "product_code_std",
            regexp_replace(col("product_code").cast("string"), r"PRD\-", "PROD-"),
        )

        # Step: Sort by order date (SortRows) [converted]
        df_Sort_by_order_date = df_Fix_product_codes.orderBy(
            col("order_date").asc_nulls_last(),
            col("product_code_std").asc_nulls_last(),
        )

        # Step: Merge join with products (MergeJoin) [converted]
        _left_df_Merge_join_with_products = df_Sort_by_order_date.orderBy(
            col("product_code_std").asc_nulls_last()
        )
        _right_df_Merge_join_with_products = df_CSV_product_prices.orderBy(
            col("product_code").asc_nulls_last()
        )
        _right_df_Merge_join_with_products = _right_df_Merge_join_with_products.select(
            *[
                col(c).alias(c if c != "product_code" else "product_code_price")
                for c in _right_df_Merge_join_with_products.columns
            ]
        )
        # Broadcast small product-price dimension (same INNER keys/predicates).
        _right_df_Merge_join_with_products = maybe_broadcast(
            _right_df_Merge_join_with_products,
            enabled=config.get("broadcast_product_prices", True),
        )
        df_Merge_join_with_products = _left_df_Merge_join_with_products.join(
            _right_df_Merge_join_with_products,
            (
                _left_df_Merge_join_with_products["product_code_std"]
                == _right_df_Merge_join_with_products["product_code_price"]
            )
            & _left_df_Merge_join_with_products["product_code_std"].isNotNull()
            & _right_df_Merge_join_with_products["product_code_price"].isNotNull(),
            how="inner",
        )

        # Step: Stream lookup regions (StreamLookup) [converted]
        _lkp_src_df_Stream_lookup_regions = df_Region_reference_grid.filter(
            col("region_code").isNotNull()
        ).dropDuplicates(["region_code"])
        _lkp_df_Stream_lookup_regions = broadcast(_lkp_src_df_Stream_lookup_regions)
        df_Stream_lookup_regions = df_Merge_join_with_products.join(
            _lkp_df_Stream_lookup_regions,
            on=["region_code"],
            how="left",
        )

        # Step: Aggregate by region (GroupBy) [converted]
        df_Aggregate_by_region = df_Stream_lookup_regions.groupBy(
            "region_name", "region_tier"
        ).agg(
            (_sum(col("adjusted_net_revenue")))
            .cast("double")
            .alias("total_net_revenue"),
            (count(col("order_id"))).cast("int").alias("order_count"),
        )
        # Cache for fan-out to Delta + audit file (identical rows to both sinks).
        df_Aggregate_by_region = cache_for_reuse(df_Aggregate_by_region)

        # Step: Table output summary (TableOutput) [converted]
        df_Table_output_summary = df_Aggregate_by_region
        table = f"{catalog}.{schema}.fact_order_summary"
        write_delta(
            df_Table_output_summary,
            table,
            mode="append",
            partition_by=config.get("partition_by")
            or ["region_tier", "region_name"],
            target_files=config.get("target_files"),
            spark=spark,
        )
        log_event(_LOG, "delta_write", table=table, mode="append")

        # Step: Text file audit log (TextFileOutput) [converted]
        df_Text_file_audit_log = df_Aggregate_by_region
        audit_path = config.get(
            "order_summary_audit",
            f"{data_dir}/order_summary_audit.txt",
        )
        (
            df_Text_file_audit_log.write.format("csv")
            .option("header", True)
            .option("sep", ",")
            .option("quote", '"')
            .option("encoding", "UTF-8")
            .mode("overwrite")
            .save(audit_path)
        )
        log_event(_LOG, "audit_write", path=audit_path)

        result = df_Aggregate_by_region
        unpersist_quiet(df_Aggregate_by_region)
        return result
