"""PySpark module migrated from Pentaho transformation: TR_Sales_Summary.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/reporting/TR_Sales_Summary.ktr
Independent module — ``run(spark, config)`` returns a DataFrame.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    length,
    lit,
    lower,
    when,
    coalesce,
    row_number,
    sum as _sum,
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_sales_summary")

from pyspark.sql.window import Window


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Sales_Summary`` step-for-step."""
    config = dict(config or {})
    apply_spark_runtime_hints(spark, config)
    log_event(_LOG, "transformation_start")
    catalog = config.get("catalog", "main")
    schema = config.get("schema", "analytics")
    data_dir = config.get("data_dir", "/Volumes/main/default/pentaho_data")

    # Parameters
    PROJECT_HOME = ''
    DATASET_PATH = ''
    RUN_ID = ''
    CURRENT_DATE = ''
    LOG_PATH = '${PROJECT_HOME}/logs'
    REJECT_PATH = '${PROJECT_HOME}/rejects'
    OUTPUT_PATH = '${PROJECT_HOME}/output'
    VAR_PATH_AUDIT = '${PROJECT_HOME}/audit'
    VAR_ETL_BATCH_ID = '${RUN_ID}'

    # Step: Read Enriched Sales Fallback (CsvInput) [converted]
    # CSV Input: Read Enriched Sales Fallback
    df_Read_Enriched_Sales_Fallback = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('order_item_id STRING, order_id STRING, product_id STRING, customer_id STRING, store_id STRING, promotion_id STRING, order_date STRING, total_revenue DOUBLE, quantity DOUBLE, converted_amount_usd DOUBLE, channel_mapped STRING, promo_code STRING, region_id STRING, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/sales_enriched_.csv')
    )

    # Step: Read Fact Sales (CsvInput) [converted]
    # CSV Input: Read Fact Sales
    df_Read_Fact_Sales = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('sales_sk INT, order_item_id STRING, order_id STRING, business_key STRING, customer_sk INT, product_sk INT, store_sk INT, employee_sk INT, promotion_sk INT, date_sk INT, sales_combo_sk INT, quantity_sold DOUBLE, unit_price DOUBLE, extended_price DOUBLE, discount_amount_calc DOUBLE, net_sales_amount DOUBLE, tax_amount_calc DOUBLE, shipping_cost_calc DOUBLE, total_revenue DOUBLE, profit DOUBLE, margin DOUBLE, converted_amount_usd DOUBLE, return_amount DOUBLE, refund_amount_calc DOUBLE, currency_code STRING, channel_mapped STRING, order_status STRING, late_delivery_flag STRING, high_value_order_flag STRING, sales_bk_checksum STRING, version_number INT, fact_action STRING, load_status STRING, is_current STRING, batch_id STRING, run_id STRING, dw_insert_ts STRING, dw_update_ts STRING, order_date STRING')
        .load(f'{data_dir}/fact_sales_.csv')
    )

    # Step: Region Revenue From Enriched (MemoryGroupBy) [converted]
    # Memory Group By: Region Revenue From Enriched
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Region_Revenue_From_Enriched = df_Read_Enriched_Sales_Fallback.groupBy('region_id').agg(_sum(col("total_revenue")).alias('revenue'))

    # Step: Prefer Fact Over Enriched (Dummy) [converted]
    # Dummy: Prefer Fact Over Enriched
    # Pass-through step - DataFrame unchanged
    df_Dummy_Prefer_Fact_Over_Enriched = df_Read_Fact_Sales

    # Step: Sort Top Regions (SortRows) [converted]
    # Sort Rows: Sort Top Regions
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Top_Regions = df_Region_Revenue_From_Enriched
    _sort_df_Sort_Top_Regions = _sort_df_Sort_Top_Regions.withColumn("_sort_ci_revenue", lower(col("revenue").cast("string")))
    df_Sort_Top_Regions = _sort_df_Sort_Top_Regions.orderBy(col("_sort_ci_revenue").asc_nulls_last())
    df_Sort_Top_Regions = df_Sort_Top_Regions.drop("_sort_ci_revenue")

    # Step: Derive Date Parts (Formula) [converted]
    # Formula: Derive Date Parts
    df_Derive_Date_Parts = df_Dummy_Prefer_Fact_Over_Enriched
    df_Derive_Date_Parts = df_Derive_Date_Parts.withColumn('formula_result', lit(None))  # empty formula

    # Step: Sample Top Regions (SampleRows) [converted]
    # Sample Rows: Sample Top Regions
    _w_sr_df_Sample_Top_Regions = Window.orderBy(monotonically_increasing_id())
    df_Sample_Top_Regions = df_Sort_Top_Regions.withColumn('_sr_rn', row_number().over(_w_sr_df_Sample_Top_Regions))
    # preserved.lines_range='1..20' ranges=[(1, 20)]
    df_Sample_Top_Regions = df_Sample_Top_Regions.filter(((col('_sr_rn') >= 1) & (col('_sr_rn') <= 20)))
    df_Sample_Top_Regions = df_Sample_Top_Regions.drop('_sr_rn')

    # Step: Add Summary Batch (Constant) [converted]
    # Add Constants: Add Summary Batch
    df_Add_Summary_Batch = df_Derive_Date_Parts
    df_Add_Summary_Batch = df_Add_Summary_Batch.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Add_Summary_Batch = df_Add_Summary_Batch.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'

    # Step: Write Top Regions (TextFileOutput) [converted]
    # Pentaho step: Write Top Regions (type: TextFileOutput)
    # Pentaho filename: /output/reporting/sales_top_regions_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='region_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Top_Regions = df_Sample_Top_Regions
    _out_df_Write_Top_Regions = df_Write_Top_Regions.select('region_id', 'revenue')
    writer = _out_df_Write_Top_Regions.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_top_regions_.csv')

    # Step: Customer Revenue Aggregate (MemoryGroupBy) [converted]
    # Memory Group By: Customer Revenue Aggregate
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Customer_Revenue_Aggregate = df_Add_Summary_Batch.groupBy('customer_sk').agg(_sum(col("total_revenue")).alias('revenue'), countDistinct(col("order_id")).alias('orders'))

    # Step: Daily Sales (MemoryGroupBy) [converted]
    # Memory Group By: Daily Sales
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Daily_Sales = df_Add_Summary_Batch.groupBy('sales_day').agg(_sum(col("total_revenue")).alias('revenue'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'))

    # Step: Monthly Sales (MemoryGroupBy) [converted]
    # Memory Group By: Monthly Sales
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Monthly_Sales = df_Add_Summary_Batch.groupBy('sales_year', 'sales_month').agg(_sum(col("total_revenue")).alias('revenue'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'))

    # Step: Product Revenue Aggregate (MemoryGroupBy) [converted]
    # Memory Group By: Product Revenue Aggregate
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Product_Revenue_Aggregate = df_Add_Summary_Batch.groupBy('product_sk').agg(_sum(col("total_revenue")).alias('revenue'), _sum(col("quantity_sold")).alias('units'))

    # Step: Promotion Effectiveness (MemoryGroupBy) [converted]
    # Memory Group By: Promotion Effectiveness
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Promotion_Effectiveness = df_Add_Summary_Batch.groupBy('promotion_sk').agg(_sum(col("total_revenue")).alias('revenue'), _sum(col("discount_amount_calc")).alias('discount_total'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'))

    # Step: Quarterly Sales (MemoryGroupBy) [converted]
    # Memory Group By: Quarterly Sales
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Quarterly_Sales = df_Add_Summary_Batch.groupBy('sales_year', 'sales_quarter').agg(_sum(col("total_revenue")).alias('revenue'), _sum(col("quantity_sold")).alias('units'))

    # Step: Store Revenue Aggregate Rpt (MemoryGroupBy) [converted]
    # Memory Group By: Store Revenue Aggregate Rpt
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Store_Revenue_Aggregate_Rpt = df_Add_Summary_Batch.groupBy('store_sk').agg(_sum(col("total_revenue")).alias('revenue'), countDistinct(col("order_id")).alias('orders'))

    # Step: Weekly Sales (MemoryGroupBy) [converted]
    # Memory Group By: Weekly Sales
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Weekly_Sales = df_Add_Summary_Batch.groupBy('sales_year', 'sales_week').agg(_sum(col("total_revenue")).alias('revenue'), _sum(col("quantity_sold")).alias('units'))

    # Step: Yearly Sales (MemoryGroupBy) [converted]
    # Memory Group By: Yearly Sales
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Yearly_Sales = df_Add_Summary_Batch.groupBy('sales_year').agg(_sum(col("total_revenue")).alias('revenue'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'))

    # Step: Sort Top Customers (SortRows) [converted]
    # Sort Rows: Sort Top Customers
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Top_Customers = df_Customer_Revenue_Aggregate
    _sort_df_Sort_Top_Customers = _sort_df_Sort_Top_Customers.withColumn("_sort_ci_revenue", lower(col("revenue").cast("string")))
    df_Sort_Top_Customers = _sort_df_Sort_Top_Customers.orderBy(col("_sort_ci_revenue").asc_nulls_last())
    df_Sort_Top_Customers = df_Sort_Top_Customers.drop("_sort_ci_revenue")

    # Step: Tag Daily Metric Stream (Constant) [converted]
    # Add Constants: Tag Daily Metric Stream
    df_Tag_Daily_Metric_Stream = df_Daily_Sales
    df_Tag_Daily_Metric_Stream = df_Tag_Daily_Metric_Stream.withColumn("metric_stream", lit('DAILY'))
    # preserved.metric_stream: length='-1', precision='-1'

    # Step: Write Daily Sales Summary (TextFileOutput) [converted]
    # Pentaho step: Write Daily Sales Summary (type: TextFileOutput)
    # Pentaho filename: /output/sales/summary/sales_daily_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='sales_day' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Daily_Sales_Summary = df_Daily_Sales
    _out_df_Write_Daily_Sales_Summary = df_Write_Daily_Sales_Summary.select('sales_day', 'revenue', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Write_Daily_Sales_Summary.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_daily_.csv')

    # Step: Seed KPI Row (Constant) [converted]
    # Add Constants: Seed KPI Row
    df_Seed_KPI_Row = df_Monthly_Sales
    df_Seed_KPI_Row = df_Seed_KPI_Row.withColumn("kpi_group", lit('SALES_PERIOD_KPIS'))
    # preserved.kpi_group: length='-1', precision='-1'
    df_Seed_KPI_Row = df_Seed_KPI_Row.withColumn("report_name", lit('SALES_SUMMARY'))
    # preserved.report_name: length='-1', precision='-1'
    df_Seed_KPI_Row = df_Seed_KPI_Row.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Seed_KPI_Row = df_Seed_KPI_Row.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'

    # Step: Write Monthly Sales Summary (TextFileOutput) [converted]
    # Pentaho step: Write Monthly Sales Summary (type: TextFileOutput)
    # Pentaho filename: /output/sales/summary/sales_monthly_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='sales_year' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sales_month' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Monthly_Sales_Summary = df_Monthly_Sales
    _out_df_Write_Monthly_Sales_Summary = df_Write_Monthly_Sales_Summary.select('sales_year', 'sales_month', 'revenue', 'units', 'orders')
    writer = _out_df_Write_Monthly_Sales_Summary.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_monthly_.csv')

    # Step: Sort Top Products (SortRows) [converted]
    # Sort Rows: Sort Top Products
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Top_Products = df_Product_Revenue_Aggregate
    _sort_df_Sort_Top_Products = _sort_df_Sort_Top_Products.withColumn("_sort_ci_revenue", lower(col("revenue").cast("string")))
    df_Sort_Top_Products = _sort_df_Sort_Top_Products.orderBy(col("_sort_ci_revenue").asc_nulls_last())
    df_Sort_Top_Products = df_Sort_Top_Products.drop("_sort_ci_revenue")

    # Step: Promo Lift Proxy (Formula) [converted]
    # Formula: Promo Lift Proxy
    df_Promo_Lift_Proxy = df_Promotion_Effectiveness
    df_Promo_Lift_Proxy = df_Promo_Lift_Proxy.withColumn('formula_result', lit(None))  # empty formula

    # Step: Write Quarterly Sales Summary (TextFileOutput) [converted]
    # Pentaho step: Write Quarterly Sales Summary (type: TextFileOutput)
    # Pentaho filename: /output/sales/summary/sales_quarterly_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='sales_year' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sales_quarter' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Quarterly_Sales_Summary = df_Quarterly_Sales
    _out_df_Write_Quarterly_Sales_Summary = df_Write_Quarterly_Sales_Summary.select('sales_year', 'sales_quarter', 'revenue', 'units')
    writer = _out_df_Write_Quarterly_Sales_Summary.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_quarterly_.csv')

    # Step: Sort Top Stores (SortRows) [converted]
    # Sort Rows: Sort Top Stores
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Top_Stores = df_Store_Revenue_Aggregate_Rpt
    _sort_df_Sort_Top_Stores = _sort_df_Sort_Top_Stores.withColumn("_sort_ci_revenue", lower(col("revenue").cast("string")))
    df_Sort_Top_Stores = _sort_df_Sort_Top_Stores.orderBy(col("_sort_ci_revenue").asc_nulls_last())
    df_Sort_Top_Stores = df_Sort_Top_Stores.drop("_sort_ci_revenue")

    # Step: Tag Weekly Metric Stream (Constant) [converted]
    # Add Constants: Tag Weekly Metric Stream
    df_Tag_Weekly_Metric_Stream = df_Weekly_Sales
    df_Tag_Weekly_Metric_Stream = df_Tag_Weekly_Metric_Stream.withColumn("metric_stream", lit('WEEKLY'))
    # preserved.metric_stream: length='-1', precision='-1'

    # Step: Write Weekly Sales Summary (TextFileOutput) [converted]
    # Pentaho step: Write Weekly Sales Summary (type: TextFileOutput)
    # Pentaho filename: /output/sales/summary/sales_weekly_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='sales_year' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sales_week' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Weekly_Sales_Summary = df_Weekly_Sales
    _out_df_Write_Weekly_Sales_Summary = df_Write_Weekly_Sales_Summary.select('sales_year', 'sales_week', 'revenue', 'units')
    writer = _out_df_Write_Weekly_Sales_Summary.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_weekly_.csv')

    # Step: Write Yearly Sales Summary (TextFileOutput) [converted]
    # Pentaho step: Write Yearly Sales Summary (type: TextFileOutput)
    # Pentaho filename: /output/sales/summary/sales_yearly_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='sales_year' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Yearly_Sales_Summary = df_Yearly_Sales
    _out_df_Write_Yearly_Sales_Summary = df_Write_Yearly_Sales_Summary.select('sales_year', 'revenue', 'units', 'orders')
    writer = _out_df_Write_Yearly_Sales_Summary.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_yearly_.csv')

    # Step: Sample Top Customers (SampleRows) [converted]
    # Sample Rows: Sample Top Customers
    _w_sr_df_Sample_Top_Customers = Window.orderBy(monotonically_increasing_id())
    df_Sample_Top_Customers = df_Sort_Top_Customers.withColumn('_sr_rn', row_number().over(_w_sr_df_Sample_Top_Customers))
    # preserved.lines_range='1..20' ranges=[(1, 20)]
    df_Sample_Top_Customers = df_Sample_Top_Customers.filter(((col('_sr_rn') >= 1) & (col('_sr_rn') <= 20)))
    df_Sample_Top_Customers = df_Sample_Top_Customers.drop('_sr_rn')

    # Step: Normalise Monthly KPIs (Normaliser) [converted]
    # Row Normaliser: Normalise Monthly KPIs
    _norm_df_Normalise_Monthly_KPIs_0 = df_Seed_KPI_Row.select(col("batch_id"), col("kpi_group"), col("report_name"), col("run_id"), col("sales_month"), col("sales_year"), lit('revenue').alias("kpi_name"), col("revenue").alias("revenue"))
    _norm_df_Normalise_Monthly_KPIs_1 = df_Seed_KPI_Row.select(col("batch_id"), col("kpi_group"), col("report_name"), col("run_id"), col("sales_month"), col("sales_year"), lit('units').alias("kpi_name"), col("units").alias("units"))
    _norm_df_Normalise_Monthly_KPIs_2 = df_Seed_KPI_Row.select(col("batch_id"), col("kpi_group"), col("report_name"), col("run_id"), col("sales_month"), col("sales_year"), lit('orders').alias("kpi_name"), col("orders").alias("orders"))
    df_Normalise_Monthly_KPIs = _norm_df_Normalise_Monthly_KPIs_0
    df_Normalise_Monthly_KPIs = df_Normalise_Monthly_KPIs.unionByName(_norm_df_Normalise_Monthly_KPIs_1, allowMissingColumns=True)
    df_Normalise_Monthly_KPIs = df_Normalise_Monthly_KPIs.unionByName(_norm_df_Normalise_Monthly_KPIs_2, allowMissingColumns=True)

    # Step: Sample Top Products (SampleRows) [converted]
    # Sample Rows: Sample Top Products
    _w_sr_df_Sample_Top_Products = Window.orderBy(monotonically_increasing_id())
    df_Sample_Top_Products = df_Sort_Top_Products.withColumn('_sr_rn', row_number().over(_w_sr_df_Sample_Top_Products))
    # preserved.lines_range='1..20' ranges=[(1, 20)]
    df_Sample_Top_Products = df_Sample_Top_Products.filter(((col('_sr_rn') >= 1) & (col('_sr_rn') <= 20)))
    df_Sample_Top_Products = df_Sample_Top_Products.drop('_sr_rn')

    # Step: Write Promotion Effectiveness (TextFileOutput) [converted]
    # Pentaho step: Write Promotion Effectiveness (type: TextFileOutput)
    # Pentaho filename: /output/reporting/sales_promo_effectiveness_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='promotion_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='discount_total' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='discount_pct_of_revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Promotion_Effectiveness = df_Promo_Lift_Proxy
    _out_df_Write_Promotion_Effectiveness = df_Write_Promotion_Effectiveness.select('promotion_sk', 'revenue', 'discount_total', 'discount_pct_of_revenue', 'units', 'orders')
    writer = _out_df_Write_Promotion_Effectiveness.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_promo_effectiveness_.csv')

    # Step: Sample Top Stores (SampleRows) [converted]
    # Sample Rows: Sample Top Stores
    _w_sr_df_Sample_Top_Stores = Window.orderBy(monotonically_increasing_id())
    df_Sample_Top_Stores = df_Sort_Top_Stores.withColumn('_sr_rn', row_number().over(_w_sr_df_Sample_Top_Stores))
    # preserved.lines_range='1..20' ranges=[(1, 20)]
    df_Sample_Top_Stores = df_Sample_Top_Stores.filter(((col('_sr_rn') >= 1) & (col('_sr_rn') <= 20)))
    df_Sample_Top_Stores = df_Sample_Top_Stores.drop('_sr_rn')

    # Step: Append Period Metric Tags (Append) [converted]
    # Append Streams: Append Period Metric Tags
    # preserved.head_name='Tag Daily Metric Stream'
    # preserved.tail_name='Tag Weekly Metric Stream'
    # preserved.stream_order=['Tag Daily Metric Stream', 'Tag Weekly Metric Stream']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_Period_Metric_Tags = df_Tag_Daily_Metric_Stream.unionByName(df_Tag_Weekly_Metric_Stream, allowMissingColumns=True)

    # Step: Write Top Customers (TextFileOutput) [converted]
    # Pentaho step: Write Top Customers (type: TextFileOutput)
    # Pentaho filename: /output/reporting/sales_top_customers_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='customer_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Top_Customers = df_Sample_Top_Customers
    _out_df_Write_Top_Customers = df_Write_Top_Customers.select('customer_sk', 'revenue', 'orders')
    writer = _out_df_Write_Top_Customers.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_top_customers_.csv')

    # Step: Denormalise Monthly KPIs (Denormaliser) [converted]
    # Row Denormaliser: Denormalise Monthly KPIs
    # preserved.target 'revenue' type='Number' format='' length='-1' precision='-1' decimal='' grouping='' currency='' null_string='' aggregation=-
    # preserved.target 'units' type='Number' format='' length='-1' precision='-1' decimal='' grouping='' currency='' null_string='' aggregation=-
    # preserved.target 'orders' type='Number' format='' length='-1' precision='-1' decimal='' grouping='' currency='' null_string='' aggregation=-
    df_Denormalise_Monthly_KPIs = df_Normalise_Monthly_KPIs.groupBy("kpi_group").agg(first(when(col("kpi_name") == lit('revenue'), col("revenue")), ignorenulls=True).alias('revenue'), first(when(col("kpi_name") == lit('units'), col("units")), ignorenulls=True).alias('units'), first(when(col("kpi_name") == lit('orders'), col("orders")), ignorenulls=True).alias('orders'))
    df_Denormalise_Monthly_KPIs = df_Denormalise_Monthly_KPIs.withColumn("revenue", col("revenue").cast("double"))
    df_Denormalise_Monthly_KPIs = df_Denormalise_Monthly_KPIs.withColumn("units", col("units").cast("double"))
    df_Denormalise_Monthly_KPIs = df_Denormalise_Monthly_KPIs.withColumn("orders", col("orders").cast("double"))

    # Step: Write Top Products (TextFileOutput) [converted]
    # Pentaho step: Write Top Products (type: TextFileOutput)
    # Pentaho filename: /output/reporting/sales_top_products_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='product_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Top_Products = df_Sample_Top_Products
    _out_df_Write_Top_Products = df_Write_Top_Products.select('product_sk', 'revenue', 'units')
    writer = _out_df_Write_Top_Products.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_top_products_.csv')

    # Step: Write Promo Effectiveness Summary Path (TextFileOutput) [converted]
    # Pentaho step: Write Promo Effectiveness Summary Path (type: TextFileOutput)
    # Pentaho filename: /output/sales/summary/sales_promo_effectiveness_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='promotion_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='discount_total' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='discount_pct_of_revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Promo_Effectiveness_Summary_Path = df_Write_Promotion_Effectiveness
    _out_df_Write_Promo_Effectiveness_Summary_Path = df_Write_Promo_Effectiveness_Summary_Path.select('promotion_sk', 'revenue', 'discount_total', 'discount_pct_of_revenue', 'units', 'orders')
    writer = _out_df_Write_Promo_Effectiveness_Summary_Path.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_promo_effectiveness_.csv')

    # Step: Write Top Stores (TextFileOutput) [converted]
    # Pentaho step: Write Top Stores (type: TextFileOutput)
    # Pentaho filename: /output/reporting/sales_top_stores_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='store_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Top_Stores = df_Sample_Top_Stores
    _out_df_Write_Top_Stores = df_Write_Top_Stores.select('store_sk', 'revenue', 'orders')
    writer = _out_df_Write_Top_Stores.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_top_stores_.csv')

    # Step: Write Consolidated Summary Metrics (TextFileOutput) [converted]
    # Pentaho step: Write Consolidated Summary Metrics (type: TextFileOutput)
    # Pentaho filename: /output/sales/summary/sales_summary_metrics_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='metric_stream' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Consolidated_Summary_Metrics = df_Append_Period_Metric_Tags
    _out_df_Write_Consolidated_Summary_Metrics = df_Write_Consolidated_Summary_Metrics.select('metric_stream', 'batch_id', 'run_id')
    writer = _out_df_Write_Consolidated_Summary_Metrics.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_summary_metrics_.csv')

    # Step: Write Reporting KPI Pivot (TextFileOutput) [converted]
    # Pentaho step: Write Reporting KPI Pivot (type: TextFileOutput)
    # Pentaho filename: /output/reporting/sales_kpi_pivot_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='kpi_group' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='report_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Reporting_KPI_Pivot = df_Denormalise_Monthly_KPIs
    _out_df_Write_Reporting_KPI_Pivot = df_Write_Reporting_KPI_Pivot.select('kpi_group', 'revenue', 'units', 'orders', 'report_name', 'batch_id', 'run_id')
    writer = _out_df_Write_Reporting_KPI_Pivot.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_kpi_pivot_.csv')

    # Step: Write Summary Audit JSON (JsonOutput) [converted]
    # Pentaho step: Write Summary Audit JSON (type: JsonOutput)
    df_Write_Summary_Audit_JSON = df_Write_Reporting_KPI_Pivot
    df_Write_Summary_Audit_JSON.write \
    .mode('overwrite') \
    .json(
        f'{data_dir}/sales_summary_.json'
    )
    # preserved.json_bloc='rows' output_value='json_blob'

    # Step: Log Summary Complete (WriteToLog) [converted]
    # Write to Log: Log Summary Complete
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=SUMMARY_COMPLETE | TRANS=TR_Sales_Summary | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Summary_Complete = logging.getLogger('pentaho.writetolog.Log_Summary_Complete')
    _log_df_Log_Summary_Complete.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Summary_Complete = df_Write_Summary_Audit_JSON
    _log_rows_df_Log_Summary_Complete = _log_df_df_Log_Summary_Complete.take(20)
    _log_df_Log_Summary_Complete.info('Log Summary Complete' + ' | columns=' + str(_log_df_df_Log_Summary_Complete.columns))
    _log_df_Log_Summary_Complete.info('AUDIT | EVENT=SUMMARY_COMPLETE | TRANS=TR_Sales_Summary | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Summary_Complete:
        _log_df_Log_Summary_Complete.info('Log Summary Complete' + ' | ' + str(_lr.asDict()))
    df_Log_Summary_Complete = df_Write_Summary_Audit_JSON

    # Step: Copy Summary To Result (RowsToResult) [converted]
    # Copy Rows to Result: Copy Summary To Result
    # preserved.result_buffer='rows'
    # preserved.preserve_order=True
    # LIMITATION: Pentaho Result rows are job-level; Databricks uses a notebook-scoped buffer (_pentaho_result_rows) for downstream hops / orchestration. Cross-job Result transfer needs Databricks Jobs task values or persisted Delta tables.
    _pentaho_result_rows = globals().setdefault('_pentaho_result_rows', {})
    _pentaho_result_files = globals().setdefault('_pentaho_result_files', [])
    # Preserve schema and relative ordering for 'Copy Summary To Result'
    _result_rows_df_Copy_Summary_To_Result = df_Log_Summary_Complete
    _pentaho_result_rows['Copy Summary To Result'] = _result_rows_df_Copy_Summary_To_Result
    _pentaho_result_rows['__latest__'] = _result_rows_df_Copy_Summary_To_Result
    df_Copy_Summary_To_Result = df_Log_Summary_Complete

    # Step: Summary Complete (Dummy) [converted]
    # Dummy: Summary Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Summary_Complete = df_Copy_Summary_To_Result

    log_event(_LOG, "transformation_end")
    return df_Dummy_Summary_Complete
