"""PySpark module migrated from Pentaho transformation: TR_Executive_Dashboard.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/reporting/TR_Executive_Dashboard.ktr
Independent module — ``run(spark, config)`` returns a DataFrame.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_date,
    length,
    lit,
    lower,
    when,
    coalesce,
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_executive_dashboard")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Executive_Dashboard`` step-for-step."""
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

    # Step: Get Dashboard Variables (GetVariable) [converted]
    # Get Variables: Get Dashboard Variables
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'current_date', 'variable': '${CURRENT_DATE}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
    # preserved.output_columns=['batch_id', 'run_id', 'current_date']
    import os
    import re as _re_var
    # Lookup order: Databricks widgets → os.environ → spark.conf (pentaho.var.*) → transformation parameters → empty string
    df_Get_Dashboard_Variables = spark.range(1).select(lit(1).alias('_row'))
    # field 'batch_id' from variable string '${VAR_ETL_BATCH_ID}'
    # preserved.field.batch_id.trim_type='none'
    # preserved.field.batch_id.type='String'
    _batch_id_resolved = None
    _dbu__batch_id_resolved = globals().get('dbutils')
    if _dbu__batch_id_resolved is not None and hasattr(_dbu__batch_id_resolved, 'widgets'):
        try:
            _batch_id_resolved = _dbu__batch_id_resolved.widgets.get('VAR_ETL_BATCH_ID')
        except Exception:
            _batch_id_resolved = None
    if _batch_id_resolved in (None, ''):
        import os as _os__batch_id_resolved
        _batch_id_resolved = _os__batch_id_resolved.environ.get('VAR_ETL_BATCH_ID')
    if _batch_id_resolved in (None, ''):
        try:
            _batch_id_resolved = spark.conf.get('pentaho.var.VAR_ETL_BATCH_ID')
        except Exception:
            _batch_id_resolved = None
    if _batch_id_resolved in (None, ''):
        _batch_id_resolved = '${RUN_ID}'
    if _batch_id_resolved is None:
        _batch_id_resolved = ''
    df_Get_Dashboard_Variables = df_Get_Dashboard_Variables.withColumn('batch_id', lit(_batch_id_resolved))
    # field 'run_id' from variable string '${RUN_ID}'
    # preserved.field.run_id.trim_type='none'
    # preserved.field.run_id.type='String'
    _run_id_resolved = None
    _dbu__run_id_resolved = globals().get('dbutils')
    if _dbu__run_id_resolved is not None and hasattr(_dbu__run_id_resolved, 'widgets'):
        try:
            _run_id_resolved = _dbu__run_id_resolved.widgets.get('RUN_ID')
        except Exception:
            _run_id_resolved = None
    if _run_id_resolved in (None, ''):
        import os as _os__run_id_resolved
        _run_id_resolved = _os__run_id_resolved.environ.get('RUN_ID')
    if _run_id_resolved in (None, ''):
        try:
            _run_id_resolved = spark.conf.get('pentaho.var.RUN_ID')
        except Exception:
            _run_id_resolved = None
    if _run_id_resolved in (None, ''):
        _run_id_resolved = ''
    if _run_id_resolved is None:
        _run_id_resolved = ''
    df_Get_Dashboard_Variables = df_Get_Dashboard_Variables.withColumn('run_id', lit(_run_id_resolved))
    # field 'current_date' from variable string '${CURRENT_DATE}'
    # preserved.field.current_date.trim_type='none'
    # preserved.field.current_date.type='String'
    _current_date_resolved = None
    _dbu__current_date_resolved = globals().get('dbutils')
    if _dbu__current_date_resolved is not None and hasattr(_dbu__current_date_resolved, 'widgets'):
        try:
            _current_date_resolved = _dbu__current_date_resolved.widgets.get('CURRENT_DATE')
        except Exception:
            _current_date_resolved = None
    if _current_date_resolved in (None, ''):
        import os as _os__current_date_resolved
        _current_date_resolved = _os__current_date_resolved.environ.get('CURRENT_DATE')
    if _current_date_resolved in (None, ''):
        try:
            _current_date_resolved = spark.conf.get('pentaho.var.CURRENT_DATE')
        except Exception:
            _current_date_resolved = None
    if _current_date_resolved in (None, ''):
        _current_date_resolved = ''
    if _current_date_resolved is None:
        _current_date_resolved = ''
    df_Get_Dashboard_Variables = df_Get_Dashboard_Variables.withColumn('current_date', lit(_current_date_resolved))

    # Step: Read Calc For Dashboard (CsvInput) [converted]
    # CSV Input: Read Calc For Dashboard
    df_Read_Calc_For_Dashboard = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('order_item_id STRING, order_id STRING, order_date STRING, sales_day STRING, sales_year INT, sales_month INT, store_sk INT, product_sk INT, customer_sk INT, promotion_sk INT, channel_mapped STRING, revenue DOUBLE, converted_revenue_usd DOUBLE, cogs DOUBLE, gross_profit DOUBLE, operating_profit DOUBLE, net_profit DOUBLE, gross_margin_pct DOUBLE, net_margin_pct DOUBLE, tax_amount DOUBLE, discount_amount DOUBLE, refund_amount DOUBLE, shipping_cost DOUBLE, inventory_cost DOUBLE, budget_variance DOUBLE, forecast_variance DOUBLE, quantity_sold DOUBLE, margin_band STRING, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/financial_calculations_.csv')
    )

    # Step: Read Prior Executive Snapshot (CsvInput) [converted]
    # CSV Input: Read Prior Executive Snapshot
    df_Read_Prior_Executive_Snapshot = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('metric_name STRING, metric_value DOUBLE, batch_id STRING')
        .load(f'{data_dir}/executive_summary_prior.csv')
    )

    # Step: Read Store KPIs Feed (CsvInput) [converted]
    # CSV Input: Read Store KPIs Feed
    df_Read_Store_KPIs_Feed = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('store_sk INT, revenue DOUBLE, gross_profit DOUBLE, net_profit DOUBLE, units DOUBLE, orders INT, avg_margin DOUBLE, refunds DOUBLE, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/store_kpis_.csv')
    )

    # Step: Category Performance (MemoryGroupBy) [converted]
    # Memory Group By: Category Performance
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Category_Performance = df_Read_Calc_For_Dashboard.groupBy('product_sk').agg(_sum(col("converted_revenue_usd")).alias('revenue'), _sum(col("gross_profit")).alias('gross_profit'), _sum(col("net_profit")).alias('net_profit'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'), _sum(col("refund_amount")).alias('refund_amount'), _sum(col("budget_variance")).alias('budget_variance'))

    # Step: Customer Segmentation (MemoryGroupBy) [converted]
    # Memory Group By: Customer Segmentation
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Customer_Segmentation = df_Read_Calc_For_Dashboard.groupBy('customer_sk', 'margin_band').agg(_sum(col("converted_revenue_usd")).alias('revenue'), _sum(col("gross_profit")).alias('gross_profit'), _sum(col("net_profit")).alias('net_profit'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'), _sum(col("refund_amount")).alias('refund_amount'), _sum(col("budget_variance")).alias('budget_variance'))

    # Step: Executive Summary Aggregate (MemoryGroupBy) [converted]
    # Memory Group By: Executive Summary Aggregate
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Executive_Summary_Aggregate = df_Read_Calc_For_Dashboard.groupBy().agg(_sum(col("converted_revenue_usd")).alias('revenue'), _sum(col("gross_profit")).alias('gross_profit'), _sum(col("operating_profit")).alias('operating_profit'), _sum(col("net_profit")).alias('net_profit'), _sum(col("cogs")).alias('cogs'), _sum(col("tax_amount")).alias('tax_amount'), _sum(col("discount_amount")).alias('discount_amount'), _sum(col("refund_amount")).alias('refund_amount'), _sum(col("shipping_cost")).alias('shipping_cost'), _sum(col("inventory_cost")).alias('inventory_cost'), _sum(col("budget_variance")).alias('budget_variance'), _sum(col("forecast_variance")).alias('forecast_variance'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'))

    # Step: Inventory Health (MemoryGroupBy) [converted]
    # Memory Group By: Inventory Health
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Inventory_Health = df_Read_Calc_For_Dashboard.groupBy('product_sk', 'store_sk').agg(_sum(col("converted_revenue_usd")).alias('revenue'), _sum(col("gross_profit")).alias('gross_profit'), _sum(col("net_profit")).alias('net_profit'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'), _sum(col("refund_amount")).alias('refund_amount'), _sum(col("budget_variance")).alias('budget_variance'))

    # Step: Product Performance (MemoryGroupBy) [converted]
    # Memory Group By: Product Performance
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Product_Performance = df_Read_Calc_For_Dashboard.groupBy('product_sk').agg(_sum(col("converted_revenue_usd")).alias('revenue'), _sum(col("gross_profit")).alias('gross_profit'), _sum(col("net_profit")).alias('net_profit'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'), _sum(col("refund_amount")).alias('refund_amount'), _sum(col("budget_variance")).alias('budget_variance'))

    # Step: Promotion Effectiveness (MemoryGroupBy) [converted]
    # Memory Group By: Promotion Effectiveness
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Promotion_Effectiveness = df_Read_Calc_For_Dashboard.groupBy('promotion_sk').agg(_sum(col("converted_revenue_usd")).alias('revenue'), _sum(col("gross_profit")).alias('gross_profit'), _sum(col("net_profit")).alias('net_profit'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'), _sum(col("refund_amount")).alias('refund_amount'), _sum(col("budget_variance")).alias('budget_variance'))

    # Step: Regional Summary (MemoryGroupBy) [converted]
    # Memory Group By: Regional Summary
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Regional_Summary = df_Read_Calc_For_Dashboard.groupBy('store_sk').agg(_sum(col("converted_revenue_usd")).alias('revenue'), _sum(col("gross_profit")).alias('gross_profit'), _sum(col("net_profit")).alias('net_profit'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'), _sum(col("refund_amount")).alias('refund_amount'), _sum(col("budget_variance")).alias('budget_variance'))

    # Step: Store Performance (MemoryGroupBy) [converted]
    # Memory Group By: Store Performance
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Store_Performance = df_Read_Calc_For_Dashboard.groupBy('store_sk').agg(_sum(col("converted_revenue_usd")).alias('revenue'), _sum(col("gross_profit")).alias('gross_profit'), _sum(col("net_profit")).alias('net_profit'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'), _sum(col("refund_amount")).alias('refund_amount'), _sum(col("budget_variance")).alias('budget_variance'))

    # Step: Sort Prior Exec Metrics (SortRows) [converted]
    # Sort Rows: Sort Prior Exec Metrics
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Prior_Exec_Metrics = df_Read_Prior_Executive_Snapshot
    _sort_df_Sort_Prior_Exec_Metrics = _sort_df_Sort_Prior_Exec_Metrics.withColumn("_sort_ci_metric_name", lower(col("metric_name").cast("string")))
    df_Sort_Prior_Exec_Metrics = _sort_df_Sort_Prior_Exec_Metrics.orderBy(col("_sort_ci_metric_name").asc_nulls_last())
    df_Sort_Prior_Exec_Metrics = df_Sort_Prior_Exec_Metrics.drop("_sort_ci_metric_name")

    # Step: Enrich Category Performance (Formula) [converted]
    # Formula: Enrich Category Performance
    df_Enrich_Category_Performance = df_Category_Performance
    df_Enrich_Category_Performance = df_Enrich_Category_Performance.withColumn('formula_result', lit(None))  # empty formula

    # Step: Enrich Customer Segmentation (Formula) [converted]
    # Formula: Enrich Customer Segmentation
    df_Enrich_Customer_Segmentation = df_Customer_Segmentation
    df_Enrich_Customer_Segmentation = df_Enrich_Customer_Segmentation.withColumn('formula_result', lit(None))  # empty formula

    # Step: Executive Margin Metrics (Formula) [converted]
    # Formula: Executive Margin Metrics
    df_Executive_Margin_Metrics = df_Executive_Summary_Aggregate
    df_Executive_Margin_Metrics = df_Executive_Margin_Metrics.withColumn('formula_result', lit(None))  # empty formula

    # Step: Enrich Inventory Health (Formula) [converted]
    # Formula: Enrich Inventory Health
    df_Enrich_Inventory_Health = df_Inventory_Health
    df_Enrich_Inventory_Health = df_Enrich_Inventory_Health.withColumn('formula_result', lit(None))  # empty formula

    # Step: Enrich Product Performance (Formula) [converted]
    # Formula: Enrich Product Performance
    df_Enrich_Product_Performance = df_Product_Performance
    df_Enrich_Product_Performance = df_Enrich_Product_Performance.withColumn('formula_result', lit(None))  # empty formula

    # Step: Enrich Promotion Effectiveness (Formula) [converted]
    # Formula: Enrich Promotion Effectiveness
    df_Enrich_Promotion_Effectiveness = df_Promotion_Effectiveness
    df_Enrich_Promotion_Effectiveness = df_Enrich_Promotion_Effectiveness.withColumn('formula_result', lit(None))  # empty formula

    # Step: Enrich Regional Summary (Formula) [converted]
    # Formula: Enrich Regional Summary
    df_Enrich_Regional_Summary = df_Regional_Summary
    df_Enrich_Regional_Summary = df_Enrich_Regional_Summary.withColumn('formula_result', lit(None))  # empty formula

    # Step: Enrich Store Performance (Formula) [converted]
    # Formula: Enrich Store Performance
    df_Enrich_Store_Performance = df_Store_Performance
    df_Enrich_Store_Performance = df_Enrich_Store_Performance.withColumn('formula_result', lit(None))  # empty formula

    # Step: Table Category Performance (TableOutput) [converted]
    # Pentaho step: Table Category Performance (type: TableOutput) (Pentaho schema: retail_rpt)
    _mapped_df_Table_Category_Performance = df_Enrich_Category_Performance.select(col('product_sk'), col('dashboard_name'), col('report_date'), col('revenue'), col('gross_profit'), col('net_profit'), col('gross_margin_pct'), col('units'), col('orders'), col('refund_amount'), col('budget_variance'), col('batch_id'), col('run_id'))
    df_Table_Category_Performance = _mapped_df_Table_Category_Performance
    write_delta(
        df_Table_Category_Performance,
        f"{catalog}.{schema}.rpt_category_performance",
        mode='overwrite',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.rpt_category_performance", mode='overwrite')

    # Step: Write Category Performance File (TextFileOutput) [converted]
    # Pentaho step: Write Category Performance File (type: TextFileOutput)
    # Pentaho filename: /output/finance/dashboard/category_performance_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='product_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='dashboard_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='report_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_margin_pct' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='refund_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='budget_variance' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Category_Performance_File = df_Enrich_Category_Performance
    _out_df_Write_Category_Performance_File = df_Write_Category_Performance_File.select('product_sk', 'dashboard_name', 'report_date', 'revenue', 'gross_profit', 'net_profit', 'gross_margin_pct', 'units', 'orders', 'refund_amount', 'budget_variance', 'batch_id', 'run_id')
    writer = _out_df_Write_Category_Performance_File.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/category_performance_.csv')

    # Step: Customer Value Tiers (NumberRange) [converted]
    # Number Range: Customer Value Tiers
    # Number Range semantics: lower_bound <= value < upper_bound (Pentaho NumberRangeRule)
    df_Customer_Value_Tiers = df_Enrich_Customer_Segmentation.withColumn('segment', when(col("revenue").isNull(), lit('Bronze')).otherwise(when((col("revenue").cast("double") >= lit(0.0)) & (col("revenue").cast("double") < lit(500.0)), lit('Bronze')).when((col("revenue").cast("double") >= lit(500.01)) & (col("revenue").cast("double") < lit(5000.0)), lit('Silver')).when((col("revenue").cast("double") >= lit(5000.01)) & (col("revenue").cast("double") < lit(25000.0)), lit('Gold')).when((col("revenue").cast("double") >= lit(25000.01)) & (col("revenue").cast("double") < lit(999999999.0)), lit('Platinum')).otherwise(lit('Bronze'))))
    # preserved.fallback='Bronze' rules=4 lower_inclusive=True upper_inclusive=False

    # Step: Table Customer Segmentation (TableOutput) [converted]
    # Pentaho step: Table Customer Segmentation (type: TableOutput) (Pentaho schema: retail_rpt)
    _mapped_df_Table_Customer_Segmentation = df_Enrich_Customer_Segmentation.select(col('customer_sk'), col('margin_band'), col('dashboard_name'), col('report_date'), col('revenue'), col('gross_profit'), col('net_profit'), col('gross_margin_pct'), col('units'), col('orders'), col('refund_amount'), col('budget_variance'), col('batch_id'), col('run_id'))
    df_Table_Customer_Segmentation = _mapped_df_Table_Customer_Segmentation
    write_delta(
        df_Table_Customer_Segmentation,
        f"{catalog}.{schema}.rpt_customer_segmentation",
        mode='overwrite',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.rpt_customer_segmentation", mode='overwrite')

    # Step: Write Customer Segmentation File (TextFileOutput) [converted]
    # Pentaho step: Write Customer Segmentation File (type: TextFileOutput)
    # Pentaho filename: /output/finance/dashboard/customer_segmentation_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='customer_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='margin_band' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='dashboard_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='report_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_margin_pct' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='refund_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='budget_variance' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Customer_Segmentation_File = df_Enrich_Customer_Segmentation
    _out_df_Write_Customer_Segmentation_File = df_Write_Customer_Segmentation_File.select('customer_sk', 'margin_band', 'dashboard_name', 'report_date', 'revenue', 'gross_profit', 'net_profit', 'gross_margin_pct', 'units', 'orders', 'refund_amount', 'budget_variance', 'batch_id', 'run_id')
    writer = _out_df_Write_Customer_Segmentation_File.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/customer_segmentation_.csv')

    # Step: Tag Executive Summary (Constant) [converted]
    # Add Constants: Tag Executive Summary
    df_Tag_Executive_Summary = df_Executive_Margin_Metrics
    df_Tag_Executive_Summary = df_Tag_Executive_Summary.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Tag_Executive_Summary = df_Tag_Executive_Summary.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'
    df_Tag_Executive_Summary = df_Tag_Executive_Summary.withColumn("period_type", lit('EXEC'))
    # preserved.period_type: length='-1', precision='-1'

    # Step: Table Inventory Health (TableOutput) [converted]
    # Pentaho step: Table Inventory Health (type: TableOutput) (Pentaho schema: retail_rpt)
    _mapped_df_Table_Inventory_Health = df_Enrich_Inventory_Health.select(col('product_sk'), col('store_sk'), col('dashboard_name'), col('report_date'), col('revenue'), col('gross_profit'), col('net_profit'), col('gross_margin_pct'), col('units'), col('orders'), col('refund_amount'), col('budget_variance'), col('batch_id'), col('run_id'))
    df_Table_Inventory_Health = _mapped_df_Table_Inventory_Health
    write_delta(
        df_Table_Inventory_Health,
        f"{catalog}.{schema}.rpt_inventory_health",
        mode='overwrite',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.rpt_inventory_health", mode='overwrite')

    # Step: Write Inventory Health File (TextFileOutput) [converted]
    # Pentaho step: Write Inventory Health File (type: TextFileOutput)
    # Pentaho filename: /output/finance/dashboard/inventory_health_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='product_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='dashboard_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='report_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_margin_pct' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='refund_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='budget_variance' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Inventory_Health_File = df_Enrich_Inventory_Health
    _out_df_Write_Inventory_Health_File = df_Write_Inventory_Health_File.select('product_sk', 'store_sk', 'dashboard_name', 'report_date', 'revenue', 'gross_profit', 'net_profit', 'gross_margin_pct', 'units', 'orders', 'refund_amount', 'budget_variance', 'batch_id', 'run_id')
    writer = _out_df_Write_Inventory_Health_File.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/inventory_health_.csv')

    # Step: Table Product Performance (TableOutput) [converted]
    # Pentaho step: Table Product Performance (type: TableOutput) (Pentaho schema: retail_rpt)
    _mapped_df_Table_Product_Performance = df_Enrich_Product_Performance.select(col('product_sk'), col('dashboard_name'), col('report_date'), col('revenue'), col('gross_profit'), col('net_profit'), col('gross_margin_pct'), col('units'), col('orders'), col('refund_amount'), col('budget_variance'), col('batch_id'), col('run_id'))
    df_Table_Product_Performance = _mapped_df_Table_Product_Performance
    write_delta(
        df_Table_Product_Performance,
        f"{catalog}.{schema}.rpt_product_performance",
        mode='overwrite',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.rpt_product_performance", mode='overwrite')

    # Step: Write Product Performance File (TextFileOutput) [converted]
    # Pentaho step: Write Product Performance File (type: TextFileOutput)
    # Pentaho filename: /output/finance/dashboard/product_performance_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='product_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='dashboard_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='report_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_margin_pct' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='refund_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='budget_variance' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Product_Performance_File = df_Enrich_Product_Performance
    _out_df_Write_Product_Performance_File = df_Write_Product_Performance_File.select('product_sk', 'dashboard_name', 'report_date', 'revenue', 'gross_profit', 'net_profit', 'gross_margin_pct', 'units', 'orders', 'refund_amount', 'budget_variance', 'batch_id', 'run_id')
    writer = _out_df_Write_Product_Performance_File.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/product_performance_.csv')

    # Step: Table Promotion Effectiveness (TableOutput) [converted]
    # Pentaho step: Table Promotion Effectiveness (type: TableOutput) (Pentaho schema: retail_rpt)
    _mapped_df_Table_Promotion_Effectiveness = df_Enrich_Promotion_Effectiveness.select(col('promotion_sk'), col('dashboard_name'), col('report_date'), col('revenue'), col('gross_profit'), col('net_profit'), col('gross_margin_pct'), col('units'), col('orders'), col('refund_amount'), col('budget_variance'), col('batch_id'), col('run_id'))
    df_Table_Promotion_Effectiveness = _mapped_df_Table_Promotion_Effectiveness
    write_delta(
        df_Table_Promotion_Effectiveness,
        f"{catalog}.{schema}.rpt_promotion_effectiveness",
        mode='overwrite',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.rpt_promotion_effectiveness", mode='overwrite')

    # Step: Write Promotion Effectiveness File (TextFileOutput) [converted]
    # Pentaho step: Write Promotion Effectiveness File (type: TextFileOutput)
    # Pentaho filename: /output/finance/dashboard/promotion_effectiveness_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='promotion_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='dashboard_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='report_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_margin_pct' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='refund_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='budget_variance' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Promotion_Effectiveness_File = df_Enrich_Promotion_Effectiveness
    _out_df_Write_Promotion_Effectiveness_File = df_Write_Promotion_Effectiveness_File.select('promotion_sk', 'dashboard_name', 'report_date', 'revenue', 'gross_profit', 'net_profit', 'gross_margin_pct', 'units', 'orders', 'refund_amount', 'budget_variance', 'batch_id', 'run_id')
    writer = _out_df_Write_Promotion_Effectiveness_File.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/promotion_effectiveness_.csv')

    # Step: Table Regional Summary (TableOutput) [converted]
    # Pentaho step: Table Regional Summary (type: TableOutput) (Pentaho schema: retail_rpt)
    _mapped_df_Table_Regional_Summary = df_Enrich_Regional_Summary.select(col('store_sk'), col('dashboard_name'), col('report_date'), col('revenue'), col('gross_profit'), col('net_profit'), col('gross_margin_pct'), col('units'), col('orders'), col('refund_amount'), col('budget_variance'), col('batch_id'), col('run_id'))
    df_Table_Regional_Summary = _mapped_df_Table_Regional_Summary
    write_delta(
        df_Table_Regional_Summary,
        f"{catalog}.{schema}.rpt_regional_summary",
        mode='overwrite',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.rpt_regional_summary", mode='overwrite')

    # Step: Write Regional Summary File (TextFileOutput) [converted]
    # Pentaho step: Write Regional Summary File (type: TextFileOutput)
    # Pentaho filename: /output/finance/dashboard/regional_summary_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='store_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='dashboard_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='report_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_margin_pct' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='refund_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='budget_variance' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Regional_Summary_File = df_Enrich_Regional_Summary
    _out_df_Write_Regional_Summary_File = df_Write_Regional_Summary_File.select('store_sk', 'dashboard_name', 'report_date', 'revenue', 'gross_profit', 'net_profit', 'gross_margin_pct', 'units', 'orders', 'refund_amount', 'budget_variance', 'batch_id', 'run_id')
    writer = _out_df_Write_Regional_Summary_File.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/regional_summary_.csv')

    # Step: Lookup Store KPI Onto Dashboard (StreamLookup) [failed]
    # Stream Lookup: Lookup Store KPI Onto Dashboard
    # StreamLookup 'Lookup Store KPI Onto Dashboard': no join keys — lookup join not generated
    df_Lookup_Store_KPI_Onto_Dashboard = df_Enrich_Store_Performance

    # Step: Table Store Performance (TableOutput) [converted]
    # Pentaho step: Table Store Performance (type: TableOutput) (Pentaho schema: retail_rpt)
    _mapped_df_Table_Store_Performance = df_Enrich_Store_Performance.select(col('store_sk'), col('dashboard_name'), col('report_date'), col('revenue'), col('gross_profit'), col('net_profit'), col('gross_margin_pct'), col('units'), col('orders'), col('refund_amount'), col('budget_variance'), col('batch_id'), col('run_id'))
    df_Table_Store_Performance = _mapped_df_Table_Store_Performance
    write_delta(
        df_Table_Store_Performance,
        f"{catalog}.{schema}.rpt_store_performance",
        mode='overwrite',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.rpt_store_performance", mode='overwrite')

    # Step: Write Store Performance File (TextFileOutput) [converted]
    # Pentaho step: Write Store Performance File (type: TextFileOutput)
    # Pentaho filename: /output/finance/dashboard/store_performance_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='store_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='dashboard_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='report_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_margin_pct' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='refund_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='budget_variance' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Store_Performance_File = df_Enrich_Store_Performance
    _out_df_Write_Store_Performance_File = df_Write_Store_Performance_File.select('store_sk', 'dashboard_name', 'report_date', 'revenue', 'gross_profit', 'net_profit', 'gross_margin_pct', 'units', 'orders', 'refund_amount', 'budget_variance', 'batch_id', 'run_id')
    writer = _out_df_Write_Store_Performance_File.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/store_performance_.csv')

    # Step: Write Customer Segments File (TextFileOutput) [converted]
    # Pentaho step: Write Customer Segments File (type: TextFileOutput)
    # Pentaho filename: /output/finance/dashboard/customer_segments_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='customer_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='margin_band' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='segment' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Customer_Segments_File = df_Customer_Value_Tiers
    _out_df_Write_Customer_Segments_File = df_Write_Customer_Segments_File.select('customer_sk', 'margin_band', 'segment', 'revenue', 'gross_profit', 'net_profit', 'batch_id', 'run_id')
    writer = _out_df_Write_Customer_Segments_File.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/customer_segments_.csv')

    # Step: Prepare Current Exec For Merge (SelectValues) [converted]
    # Select Values: Prepare Current Exec For Merge
    df_Prepare_Current_Exec_For_Merge = df_Tag_Executive_Summary.select(col("dashboard_name").alias("metric_name"), col("revenue").alias("metric_value"), col("batch_id").alias("batch_id"))

    # Step: Table Executive Summary (TableOutput) [converted]
    # Pentaho step: Table Executive Summary (type: TableOutput) (Pentaho schema: retail_rpt)
    _mapped_df_Table_Executive_Summary = df_Tag_Executive_Summary.select(col('dashboard_name'), col('report_date'), col('period_type'), col('revenue'), col('gross_profit'), col('operating_profit'), col('net_profit'), col('gross_margin_pct'), col('net_margin_pct'), col('cogs'), col('tax_amount'), col('discount_amount'), col('refund_amount'), col('shipping_cost'), col('inventory_cost'), col('budget_variance'), col('forecast_variance'), col('units'), col('orders'), col('batch_id'), col('run_id'))
    df_Table_Executive_Summary = _mapped_df_Table_Executive_Summary
    write_delta(
        df_Table_Executive_Summary,
        f"{catalog}.{schema}.rpt_executive_summary",
        mode='overwrite',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.rpt_executive_summary", mode='overwrite')

    # Step: Write Executive Summary File (TextFileOutput) [converted]
    # Pentaho step: Write Executive Summary File (type: TextFileOutput)
    # Pentaho filename: /output/finance/dashboard/executive_summary_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='dashboard_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='report_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='period_type' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='operating_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_margin_pct' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_margin_pct' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='cogs' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='tax_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='discount_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='refund_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipping_cost' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='inventory_cost' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='budget_variance' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='forecast_variance' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Executive_Summary_File = df_Tag_Executive_Summary
    _out_df_Write_Executive_Summary_File = df_Write_Executive_Summary_File.select('dashboard_name', 'report_date', 'period_type', 'revenue', 'gross_profit', 'operating_profit', 'net_profit', 'gross_margin_pct', 'net_margin_pct', 'cogs', 'tax_amount', 'discount_amount', 'refund_amount', 'shipping_cost', 'inventory_cost', 'budget_variance', 'forecast_variance', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Write_Executive_Summary_File.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/executive_summary_.csv')

    # Step: Sort Current Exec Metrics (SortRows) [converted]
    # Sort Rows: Sort Current Exec Metrics
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Current_Exec_Metrics = df_Prepare_Current_Exec_For_Merge
    _sort_df_Sort_Current_Exec_Metrics = _sort_df_Sort_Current_Exec_Metrics.withColumn("_sort_ci_metric_name", lower(col("metric_name").cast("string")))
    df_Sort_Current_Exec_Metrics = _sort_df_Sort_Current_Exec_Metrics.orderBy(col("_sort_ci_metric_name").asc_nulls_last())
    df_Sort_Current_Exec_Metrics = df_Sort_Current_Exec_Metrics.drop("_sort_ci_metric_name")

    # Step: Append Dashboard Landings A (Append) [converted]
    # Append Streams: Append Dashboard Landings A
    # preserved.head_name='Write Executive Summary File'
    # preserved.tail_name='Write Regional Summary File'
    # preserved.stream_order=['Write Executive Summary File', 'Write Regional Summary File']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_Dashboard_Landings_A = df_Write_Executive_Summary_File.unionByName(df_Write_Regional_Summary_File, allowMissingColumns=True)

    # Step: Merge Prior Vs Current Exec (MergeRows) [converted]
    # Merge Rows (Diff): Merge Prior Vs Current Exec
    # preserved.flag_field='merge_flag'
    # preserved.reference='Sort Prior Exec Metrics'
    # preserved.compare='Sort Current Exec Metrics'
    # preserved.key_fields=['metric_name']
    # preserved.value_fields=['metric_value']
    _ref_df_Merge_Prior_Vs_Current_Exec = df_Sort_Prior_Exec_Metrics.alias("r")
    _cmp_df_Merge_Prior_Vs_Current_Exec = df_Sort_Current_Exec_Metrics.alias("c")
    # WARNING: MergeRows 'Merge Prior Vs Current Exec': null join keys do not match under Spark equality; duplicate keys expand to a product within the key group
    df_Merge_Prior_Vs_Current_Exec = _ref_df_Merge_Prior_Vs_Current_Exec.join(_cmp_df_Merge_Prior_Vs_Current_Exec, (col("r.metric_name") == col("c.metric_name")), 'full_outer')
    df_Merge_Prior_Vs_Current_Exec = df_Merge_Prior_Vs_Current_Exec.withColumn('merge_flag', when(col("c.metric_name").isNull(), lit("deleted")).when(col("r.metric_name").isNull(), lit("new")).when((~col("r.metric_value").eqNullSafe(col("c.metric_value"))), lit("changed")).otherwise(lit("identical")))
    # NOTE: MergeRows 'Merge Prior Vs Current Exec': output prefers compare values (CDC-style); deleted rows keep reference values
    df_Merge_Prior_Vs_Current_Exec = df_Merge_Prior_Vs_Current_Exec.select(coalesce(col("c.metric_name"), col("r.metric_name")).alias('metric_name'), coalesce(col("c.metric_value"), col("r.metric_value")).alias('metric_value'), col('merge_flag'))
    # NOTE: MergeRows flags — deleted / new / changed / identical (requires pre-sorted inputs in PDI; Spark join does not enforce sort order)

    # Step: Append Dashboard Landings B (Append) [converted]
    # Append Streams: Append Dashboard Landings B
    # preserved.head_name='Append Dashboard Landings A'
    # preserved.tail_name='Write Store Performance File'
    # preserved.stream_order=['Append Dashboard Landings A', 'Write Store Performance File']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_Dashboard_Landings_B = df_Append_Dashboard_Landings_A.unionByName(df_Write_Store_Performance_File, allowMissingColumns=True)

    # Step: Exec Metric Changed? (FilterRows) [failed]
    # Filter Rows: Exec Metric Changed?
    df_Unchanged_Exec_Metrics = df_Merge_Prior_Vs_Current_Exec.filter((col("merge_flag") == lit('identical')))
    df_Changed_Exec_Metrics = df_Merge_Prior_Vs_Current_Exec.filter(~((col("merge_flag") == lit('identical'))))
    df_Exec_Metric_Changed? = df_Unchanged_Exec_Metrics

    # Step: Changed Exec Metrics (Dummy) [converted]
    # Dummy: Changed Exec Metrics
    # Pass-through step - DataFrame unchanged
    df_Dummy_Changed_Exec_Metrics = df_Changed_Exec_Metrics

    # Step: Unchanged Exec Metrics (Dummy) [converted]
    # Dummy: Unchanged Exec Metrics
    # Pass-through step - DataFrame unchanged
    df_Dummy_Unchanged_Exec_Metrics = df_Unchanged_Exec_Metrics

    # Step: Write Exec Change Report (TextFileOutput) [converted]
    # Pentaho step: Write Exec Change Report (type: TextFileOutput)
    # Pentaho filename: /output/finance/dashboard/executive_changes_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='metric_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='metric_value' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='merge_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Exec_Change_Report = df_Dummy_Changed_Exec_Metrics
    _out_df_Write_Exec_Change_Report = df_Write_Exec_Change_Report.select('metric_name', 'metric_value', 'merge_flag', 'batch_id')
    writer = _out_df_Write_Exec_Change_Report.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/executive_changes_.csv')

    # Step: Block Dashboard Outputs (BlockingStep) [converted]
    # Blocking Step: Block Dashboard Outputs
    # preserved.pass_all_rows=True
    # preserved.directory='%%java.io.tmpdir%%'
    # preserved.prefix='block'
    # preserved.cache_size='5000'
    # preserved.compress_files=False
    # LIMITATION: Pentaho temp-file spill (directory/prefix/compress) is replaced by Spark cache/persist.
    df_Block_Dashboard_Outputs = cache_for_reuse(df_Append_Dashboard_Landings_B)
    _ = df_Block_Dashboard_Outputs.count()  # synchronize: wait for all upstream rows

    # Step: Log Executive Dashboard (WriteToLog) [converted]
    # Write to Log: Log Executive Dashboard
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=DASHBOARD_OK | TRANS=TR_Executive_Dashboard | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Executive_Dashboard = logging.getLogger('pentaho.writetolog.Log_Executive_Dashboard')
    _log_df_Log_Executive_Dashboard.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Executive_Dashboard = df_Block_Dashboard_Outputs
    _log_rows_df_Log_Executive_Dashboard = _log_df_df_Log_Executive_Dashboard.take(20)
    _log_df_Log_Executive_Dashboard.info('Log Executive Dashboard' + ' | columns=' + str(_log_df_df_Log_Executive_Dashboard.columns))
    _log_df_Log_Executive_Dashboard.info('AUDIT | EVENT=DASHBOARD_OK | TRANS=TR_Executive_Dashboard | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Executive_Dashboard:
        _log_df_Log_Executive_Dashboard.info('Log Executive Dashboard' + ' | ' + str(_lr.asDict()))
    df_Log_Executive_Dashboard = df_Block_Dashboard_Outputs

    # Step: Dashboard Complete (Dummy) [converted]
    # Dummy: Dashboard Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Dashboard_Complete = df_Log_Executive_Dashboard

    log_event(_LOG, "transformation_end")
    return df_Dummy_Dashboard_Complete
