"""PySpark module migrated from Pentaho transformation: TR_KPI_Calculation.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/reporting/TR_KPI_Calculation.ktr
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_kpi_calculation")

from pyspark.sql.window import Window


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_KPI_Calculation`` step-for-step."""
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

    # Step: Get KPI Variables (GetVariable) [converted]
    # Get Variables: Get KPI Variables
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'top_n', 'variable': '20', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'bottom_n', 'variable': '20', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
    # preserved.output_columns=['batch_id', 'run_id', 'top_n', 'bottom_n']
    import os
    import re as _re_var
    # Lookup order: Databricks widgets → os.environ → spark.conf (pentaho.var.*) → transformation parameters → empty string
    df_Get_KPI_Variables = spark.range(1).select(lit(1).alias('_row'))
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
    df_Get_KPI_Variables = df_Get_KPI_Variables.withColumn('batch_id', lit(_batch_id_resolved))
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
    df_Get_KPI_Variables = df_Get_KPI_Variables.withColumn('run_id', lit(_run_id_resolved))
    # field 'top_n' from variable string '20'
    # preserved.field.top_n.trim_type='none'
    # preserved.field.top_n.type='String'
    _top_n_resolved = None
    _dbu__top_n_resolved = globals().get('dbutils')
    if _dbu__top_n_resolved is not None and hasattr(_dbu__top_n_resolved, 'widgets'):
        try:
            _top_n_resolved = _dbu__top_n_resolved.widgets.get('20')
        except Exception:
            _top_n_resolved = None
    if _top_n_resolved in (None, ''):
        import os as _os__top_n_resolved
        _top_n_resolved = _os__top_n_resolved.environ.get('20')
    if _top_n_resolved in (None, ''):
        try:
            _top_n_resolved = spark.conf.get('pentaho.var.20')
        except Exception:
            _top_n_resolved = None
    if _top_n_resolved in (None, ''):
        _top_n_resolved = ''
    if _top_n_resolved is None:
        _top_n_resolved = ''
    df_Get_KPI_Variables = df_Get_KPI_Variables.withColumn('top_n', lit(_top_n_resolved))
    # field 'bottom_n' from variable string '20'
    # preserved.field.bottom_n.trim_type='none'
    # preserved.field.bottom_n.type='String'
    _bottom_n_resolved = None
    _dbu__bottom_n_resolved = globals().get('dbutils')
    if _dbu__bottom_n_resolved is not None and hasattr(_dbu__bottom_n_resolved, 'widgets'):
        try:
            _bottom_n_resolved = _dbu__bottom_n_resolved.widgets.get('20')
        except Exception:
            _bottom_n_resolved = None
    if _bottom_n_resolved in (None, ''):
        import os as _os__bottom_n_resolved
        _bottom_n_resolved = _os__bottom_n_resolved.environ.get('20')
    if _bottom_n_resolved in (None, ''):
        try:
            _bottom_n_resolved = spark.conf.get('pentaho.var.20')
        except Exception:
            _bottom_n_resolved = None
    if _bottom_n_resolved in (None, ''):
        _bottom_n_resolved = ''
    if _bottom_n_resolved is None:
        _bottom_n_resolved = ''
    df_Get_KPI_Variables = df_Get_KPI_Variables.withColumn('bottom_n', lit(_bottom_n_resolved))

    # Step: Read Financial Calculations (CsvInput) [converted]
    # CSV Input: Read Financial Calculations
    df_Read_Financial_Calculations = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('order_item_id STRING, order_id STRING, order_date STRING, sales_day STRING, sales_year INT, sales_month INT, sales_quarter INT, store_sk INT, product_sk INT, customer_sk INT, employee_sk INT, promotion_sk INT, channel_mapped STRING, quantity_sold DOUBLE, revenue DOUBLE, converted_revenue_usd DOUBLE, cogs DOUBLE, gross_profit DOUBLE, net_profit DOUBLE, gross_margin_pct DOUBLE, net_margin_pct DOUBLE, tax_amount DOUBLE, discount_amount DOUBLE, refund_amount DOUBLE, shipping_cost DOUBLE, inventory_cost DOUBLE, budget_variance DOUBLE, forecast_variance DOUBLE, margin_band STRING, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/financial_calculations_.csv')
    )

    # Step: Read Inventory Staging For KPI (CsvInput) [converted]
    # CSV Input: Read Inventory Staging For KPI
    df_Read_Inventory_Staging_For_KPI = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('product_sk INT, store_sk INT, inventory_cost DOUBLE, quantity_on_hand DOUBLE, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/stg_finance_joined_.csv')
    )

    # Step: Tag KPI Batch (Constant) [converted]
    # Add Constants: Tag KPI Batch
    df_Tag_KPI_Batch = df_Read_Financial_Calculations
    df_Tag_KPI_Batch = df_Tag_KPI_Batch.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Tag_KPI_Batch = df_Tag_KPI_Batch.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'
    df_Tag_KPI_Batch = df_Tag_KPI_Batch.withColumn("kpi_module", lit('FINANCE_KPI'))
    # preserved.kpi_module: length='-1', precision='-1'

    # Step: Inventory KPIs (MemoryGroupBy) [converted]
    # Memory Group By: Inventory KPIs
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Inventory_KPIs = df_Read_Inventory_Staging_For_KPI.groupBy('product_sk', 'store_sk').agg(_sum(col("inventory_cost")).alias('inventory_cost'), _sum(col("quantity_on_hand")).alias('qty_on_hand'))

    # Step: Customer KPIs (GroupBy) [converted]
    # Group By: Customer KPIs
    df_Customer_KPIs = df_Tag_KPI_Batch.groupBy('customer_sk').agg(_sum(col("converted_revenue_usd")).alias('revenue'), _sum(col("gross_profit")).alias('gross_profit'), _sum(col("net_profit")).alias('net_profit'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'), avg(col("gross_margin_pct")).alias('avg_margin'), _sum(col("refund_amount")).alias('refunds'))

    # Step: Daily Revenue KPI (MemoryGroupBy) [converted]
    # Memory Group By: Daily Revenue KPI
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Daily_Revenue_KPI = df_Tag_KPI_Batch.groupBy('sales_day').agg(_sum(col("converted_revenue_usd")).alias('revenue'), _sum(col("gross_profit")).alias('gross_profit'), _sum(col("net_profit")).alias('net_profit'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'))

    # Step: Employee KPIs (GroupBy) [converted]
    # Group By: Employee KPIs
    df_Employee_KPIs = df_Tag_KPI_Batch.groupBy('employee_sk').agg(_sum(col("converted_revenue_usd")).alias('revenue'), _sum(col("gross_profit")).alias('gross_profit'), _sum(col("net_profit")).alias('net_profit'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'), avg(col("gross_margin_pct")).alias('avg_margin'), _sum(col("refund_amount")).alias('refunds'))

    # Step: Monthly Revenue KPI (MemoryGroupBy) [converted]
    # Memory Group By: Monthly Revenue KPI
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Monthly_Revenue_KPI = df_Tag_KPI_Batch.groupBy('sales_year', 'sales_month').agg(_sum(col("converted_revenue_usd")).alias('revenue'), _sum(col("gross_profit")).alias('gross_profit'), _sum(col("net_profit")).alias('net_profit'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'))

    # Step: Product KPIs (GroupBy) [converted]
    # Group By: Product KPIs
    df_Product_KPIs = df_Tag_KPI_Batch.groupBy('product_sk').agg(_sum(col("converted_revenue_usd")).alias('revenue'), _sum(col("gross_profit")).alias('gross_profit'), _sum(col("net_profit")).alias('net_profit'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'), avg(col("gross_margin_pct")).alias('avg_margin'), _sum(col("refund_amount")).alias('refunds'))

    # Step: Promotion KPIs (GroupBy) [converted]
    # Group By: Promotion KPIs
    df_Promotion_KPIs = df_Tag_KPI_Batch.groupBy('promotion_sk').agg(_sum(col("converted_revenue_usd")).alias('revenue'), _sum(col("gross_profit")).alias('gross_profit'), _sum(col("net_profit")).alias('net_profit'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'), avg(col("gross_margin_pct")).alias('avg_margin'), _sum(col("refund_amount")).alias('refunds'))

    # Step: Quarterly Revenue KPI (MemoryGroupBy) [converted]
    # Memory Group By: Quarterly Revenue KPI
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Quarterly_Revenue_KPI = df_Tag_KPI_Batch.groupBy('sales_year', 'sales_quarter').agg(_sum(col("converted_revenue_usd")).alias('revenue'), _sum(col("gross_profit")).alias('gross_profit'), _sum(col("net_profit")).alias('net_profit'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'))

    # Step: Region KPIs Proxy (GroupBy) [converted]
    # Group By: Region KPIs Proxy
    df_Region_KPIs_Proxy = df_Tag_KPI_Batch.groupBy('store_sk').agg(_sum(col("converted_revenue_usd")).alias('revenue'), _sum(col("gross_profit")).alias('gross_profit'), _sum(col("net_profit")).alias('net_profit'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'), avg(col("gross_margin_pct")).alias('avg_margin'), _sum(col("refund_amount")).alias('refunds'))

    # Step: Store KPIs (GroupBy) [converted]
    # Group By: Store KPIs
    df_Store_KPIs = df_Tag_KPI_Batch.groupBy('store_sk').agg(_sum(col("converted_revenue_usd")).alias('revenue'), _sum(col("gross_profit")).alias('gross_profit'), _sum(col("net_profit")).alias('net_profit'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'), avg(col("gross_margin_pct")).alias('avg_margin'), _sum(col("refund_amount")).alias('refunds'))

    # Step: Yearly Revenue KPI (MemoryGroupBy) [converted]
    # Memory Group By: Yearly Revenue KPI
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Yearly_Revenue_KPI = df_Tag_KPI_Batch.groupBy('sales_year').agg(_sum(col("converted_revenue_usd")).alias('revenue'), _sum(col("gross_profit")).alias('gross_profit'), _sum(col("net_profit")).alias('net_profit'), _sum(col("quantity_sold")).alias('units'), countDistinct(col("order_id")).alias('orders'))

    # Step: Write Customer KPIs (TextFileOutput) [converted]
    # Pentaho step: Write Customer KPIs (type: TextFileOutput)
    # Pentaho filename: /output/finance/kpi/customer_kpis_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='customer_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='avg_margin' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='refunds' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Customer_KPIs = df_Customer_KPIs
    _out_df_Write_Customer_KPIs = df_Write_Customer_KPIs.select('customer_sk', 'revenue', 'gross_profit', 'net_profit', 'units', 'orders', 'avg_margin', 'refunds', 'batch_id', 'run_id')
    writer = _out_df_Write_Customer_KPIs.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/customer_kpis_.csv')

    # Step: Mark Period Daily (Constant) [converted]
    # Add Constants: Mark Period Daily
    df_Mark_Period_Daily = df_Daily_Revenue_KPI
    df_Mark_Period_Daily = df_Mark_Period_Daily.withColumn("period_type", lit('DAILY'))
    # preserved.period_type: length='-1', precision='-1'

    # Step: Write Daily Revenue KPI (TextFileOutput) [converted]
    # Pentaho step: Write Daily Revenue KPI (type: TextFileOutput)
    # Pentaho filename: /output/finance/kpi/daily_revenue_kpi_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='sales_day' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Daily_Revenue_KPI = df_Daily_Revenue_KPI
    _out_df_Write_Daily_Revenue_KPI = df_Write_Daily_Revenue_KPI.select('sales_day', 'revenue', 'gross_profit', 'net_profit', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Write_Daily_Revenue_KPI.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/daily_revenue_kpi_.csv')

    # Step: Write Employee KPIs (TextFileOutput) [converted]
    # Pentaho step: Write Employee KPIs (type: TextFileOutput)
    # Pentaho filename: /output/finance/kpi/employee_kpis_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='employee_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='avg_margin' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='refunds' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Employee_KPIs = df_Employee_KPIs
    _out_df_Write_Employee_KPIs = df_Write_Employee_KPIs.select('employee_sk', 'revenue', 'gross_profit', 'net_profit', 'units', 'orders', 'avg_margin', 'refunds', 'batch_id', 'run_id')
    writer = _out_df_Write_Employee_KPIs.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/employee_kpis_.csv')

    # Step: Write Monthly Revenue KPI (TextFileOutput) [converted]
    # Pentaho step: Write Monthly Revenue KPI (type: TextFileOutput)
    # Pentaho filename: /output/finance/kpi/monthly_revenue_kpi_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='sales_year' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sales_month' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Monthly_Revenue_KPI = df_Monthly_Revenue_KPI
    _out_df_Write_Monthly_Revenue_KPI = df_Write_Monthly_Revenue_KPI.select('sales_year', 'sales_month', 'revenue', 'gross_profit', 'net_profit', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Write_Monthly_Revenue_KPI.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/monthly_revenue_kpi_.csv')

    # Step: Merge Product KPI With Inventory (MergeJoin) [converted]
    # Merge Join: Merge Product KPI With Inventory
    # preserved.join_type='LEFT OUTER'
    # preserved.join_keys=[{'left': 'product_sk', 'right': 'product_sk'}]
    # NOTE: PDI Merge Join requires both streams pre-sorted on join keys — Spark join() does not enforce sort order (preserve sort steps upstream if needed)
    # WARNING: MergeJoin 'Merge Product KPI With Inventory': null join keys do not match (Spark == / PDI merge semantics); duplicate keys produce a cartesian explosion within the key group; ensure key data types match across streams
    _joined_df_Merge_Product_KPI_With_Inventory = df_Product_KPIs.join(df_Inventory_KPIs, on=["product_sk"], how='left')
    # WARNING: MergeJoin 'Merge Product KPI With Inventory': column lineage unavailable — join output may contain ambiguous duplicate column names
    df_Merge_Product_KPI_With_Inventory = _joined_df_Merge_Product_KPI_With_Inventory

    # Step: Sort Bottom Products (SortRows) [converted]
    # Sort Rows: Sort Bottom Products
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Bottom_Products = df_Product_KPIs
    _sort_df_Sort_Bottom_Products = _sort_df_Sort_Bottom_Products.withColumn("_sort_ci_revenue", lower(col("revenue").cast("string")))
    df_Sort_Bottom_Products = _sort_df_Sort_Bottom_Products.orderBy(col("_sort_ci_revenue").asc_nulls_last())
    df_Sort_Bottom_Products = df_Sort_Bottom_Products.drop("_sort_ci_revenue")

    # Step: Sort Top Products (SortRows) [converted]
    # Sort Rows: Sort Top Products
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Top_Products = df_Product_KPIs
    _sort_df_Sort_Top_Products = _sort_df_Sort_Top_Products.withColumn("_sort_ci_revenue", lower(col("revenue").cast("string")))
    df_Sort_Top_Products = _sort_df_Sort_Top_Products.orderBy(col("_sort_ci_revenue").desc_nulls_last())
    df_Sort_Top_Products = df_Sort_Top_Products.drop("_sort_ci_revenue")

    # Step: Write Promotion KPIs (TextFileOutput) [converted]
    # Pentaho step: Write Promotion KPIs (type: TextFileOutput)
    # Pentaho filename: /output/finance/kpi/promotion_kpis_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='promotion_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='avg_margin' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='refunds' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Promotion_KPIs = df_Promotion_KPIs
    _out_df_Write_Promotion_KPIs = df_Write_Promotion_KPIs.select('promotion_sk', 'revenue', 'gross_profit', 'net_profit', 'units', 'orders', 'avg_margin', 'refunds', 'batch_id', 'run_id')
    writer = _out_df_Write_Promotion_KPIs.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/promotion_kpis_.csv')

    # Step: Write Quarterly Revenue KPI (TextFileOutput) [converted]
    # Pentaho step: Write Quarterly Revenue KPI (type: TextFileOutput)
    # Pentaho filename: /output/finance/kpi/quarterly_revenue_kpi_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='sales_year' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sales_quarter' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Quarterly_Revenue_KPI = df_Quarterly_Revenue_KPI
    _out_df_Write_Quarterly_Revenue_KPI = df_Write_Quarterly_Revenue_KPI.select('sales_year', 'sales_quarter', 'revenue', 'gross_profit', 'net_profit', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Write_Quarterly_Revenue_KPI.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/quarterly_revenue_kpi_.csv')

    # Step: DB Join Region For Store KPI (DBJoin) [partial]
    # Database Join: DB Join Region For Store KPI
    # preserved.connection='conn_dev_dwh'
    # preserved.sql='SELECT store_sk, region_id, region_name FROM retail_dwh.dim_store s LEFT JOIN retail_dwh.dim_region r ON s.region_id = r.region_id WHERE s.store_sk = ?'
    # preserved.outer_join=True
    # preserved.row_limit=0
    # preserved.replace_vars=True
    # preserved.parameters=[{'name': 'store_sk', 'type': 'String'}, {'name': '\n        ', 'type': ''}]
    _sql_df_DB_Join_Region_For_Store_KPI = 'SELECT store_sk, region_id, region_name FROM retail_dwh.dim_store s LEFT JOIN retail_dwh.dim_region r ON s.region_id = r.region_id WHERE s.store_sk = ?'
    # WARNING: per-row parameterized joins cannot use spark.sql with '?' placeholders; emitting JDBC prepared-statement skeleton (foreachPartition).
    # preserved.sql_template='SELECT store_sk, region_id, region_name FROM retail_dwh.dim_store s LEFT JOIN retail_dwh.dim_region r ON s.region_id = r.region_id WHERE s.store_sk = :store_sk'
    _param_fields_df_DB_Join_Region_For_Store_KPI = ['store_sk', '\n        ']
    import os
    # foreachPartition JDBC outline (wire PENTAHO_JDBC_URL / driver at runtime):
    # def _dbjoin_partition(rows):
    #     conn = <jdbc connect from os.environ['PENTAHO_JDBC_URL']>
    #     cur = conn.prepareStatement('SELECT store_sk, region_id, region_name FROM retail_dwh.dim_store s LEFT JOIN retail_dwh.dim_region r ON s.region_id = r.region_id WHERE s.store_sk = ?')
    #     for row in rows:
    #         for i, f in enumerate(_param_fields_df_DB_Join_Region_For_Store_KPI, 1):
    #             cur.setObject(i, row[f])
    #         rs = cur.executeQuery(); ... yield joined rows
    # Fallback: preserve input stream; attach empty lookup side for schema continuity
    df_DB_Join_Region_For_Store_KPI = df_Region_KPIs_Proxy
    # Join type preserved as 'left'; join keys=['store_sk', '\n        ']

    # Step: Sort Bottom Stores (SortRows) [converted]
    # Sort Rows: Sort Bottom Stores
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Bottom_Stores = df_Store_KPIs
    _sort_df_Sort_Bottom_Stores = _sort_df_Sort_Bottom_Stores.withColumn("_sort_ci_revenue", lower(col("revenue").cast("string")))
    df_Sort_Bottom_Stores = _sort_df_Sort_Bottom_Stores.orderBy(col("_sort_ci_revenue").asc_nulls_last())
    df_Sort_Bottom_Stores = df_Sort_Bottom_Stores.drop("_sort_ci_revenue")

    # Step: Sort Top Stores (SortRows) [converted]
    # Sort Rows: Sort Top Stores
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Top_Stores = df_Store_KPIs
    _sort_df_Sort_Top_Stores = _sort_df_Sort_Top_Stores.withColumn("_sort_ci_revenue", lower(col("revenue").cast("string")))
    df_Sort_Top_Stores = _sort_df_Sort_Top_Stores.orderBy(col("_sort_ci_revenue").desc_nulls_last())
    df_Sort_Top_Stores = df_Sort_Top_Stores.drop("_sort_ci_revenue")

    # Step: Write Store KPIs (TextFileOutput) [converted]
    # Pentaho step: Write Store KPIs (type: TextFileOutput)
    # Pentaho filename: /output/finance/kpi/store_kpis_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='store_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='avg_margin' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='refunds' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Store_KPIs = df_Store_KPIs
    _out_df_Write_Store_KPIs = df_Write_Store_KPIs.select('store_sk', 'revenue', 'gross_profit', 'net_profit', 'units', 'orders', 'avg_margin', 'refunds', 'batch_id', 'run_id')
    writer = _out_df_Write_Store_KPIs.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/store_kpis_.csv')

    # Step: Write Yearly Revenue KPI (TextFileOutput) [converted]
    # Pentaho step: Write Yearly Revenue KPI (type: TextFileOutput)
    # Pentaho filename: /output/finance/kpi/yearly_revenue_kpi_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='sales_year' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Yearly_Revenue_KPI = df_Yearly_Revenue_KPI
    _out_df_Write_Yearly_Revenue_KPI = df_Write_Yearly_Revenue_KPI.select('sales_year', 'revenue', 'gross_profit', 'net_profit', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Write_Yearly_Revenue_KPI.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/yearly_revenue_kpi_.csv')

    # Step: Route Period KPI Pack (SwitchCase) [converted]
    # Switch / Case: Route Period KPI Pack
    # preserved.fieldname='period_type'
    # preserved.switch_field='period_type'
    # preserved.cases=[{'value': 'DAILY', 'target_step': 'Pack Daily KPI'}, {'value': 'MONTHLY', 'target_step': 'Pack Monthly KPI'}]
    # preserved.default_target_step='Pack Other Period KPI'
    # preserved.use_contains=False
    # preserved.case_value_type='String'
    # preserved.rules=[{'value': 'DAILY', 'target_step': 'Pack Daily KPI'}, {'value': 'MONTHLY', 'target_step': 'Pack Monthly KPI'}]
    _routed_df_Route_Period_KPI_Pack = df_Mark_Period_Daily.withColumn('_route_Route_Period_KPI_Pack', when(col("period_type") == lit('DAILY'), lit('Pack Daily KPI')).when(col("period_type") == lit('MONTHLY'), lit('Pack Monthly KPI')).otherwise(lit('Pack Other Period KPI')))
    df_Pack_Daily_KPI = _routed_df_Route_Period_KPI_Pack.filter(col('_route_Route_Period_KPI_Pack') == lit('Pack Daily KPI')).drop('_route_Route_Period_KPI_Pack')
    df_Pack_Monthly_KPI = _routed_df_Route_Period_KPI_Pack.filter(col('_route_Route_Period_KPI_Pack') == lit('Pack Monthly KPI')).drop('_route_Route_Period_KPI_Pack')
    df_Pack_Other_Period_KPI = _routed_df_Route_Period_KPI_Pack.filter(col('_route_Route_Period_KPI_Pack') == lit('Pack Other Period KPI')).drop('_route_Route_Period_KPI_Pack')
    df_Route_Period_KPI_Pack = df_Pack_Daily_KPI

    # Step: Inventory Health Ratio (Formula) [converted]
    # Formula: Inventory Health Ratio
    df_Inventory_Health_Ratio = df_Merge_Product_KPI_With_Inventory
    df_Inventory_Health_Ratio = df_Inventory_Health_Ratio.withColumn('formula_result', lit(None))  # empty formula

    # Step: Sample Bottom N Products (SampleRows) [converted]
    # Sample Rows: Sample Bottom N Products
    _w_sr_df_Sample_Bottom_N_Products = Window.orderBy(monotonically_increasing_id())
    df_Sample_Bottom_N_Products = df_Sort_Bottom_Products.withColumn('_sr_rn', row_number().over(_w_sr_df_Sample_Bottom_N_Products))
    # preserved.lines_range='1..20' ranges=[(1, 20)]
    df_Sample_Bottom_N_Products = df_Sample_Bottom_N_Products.filter(((col('_sr_rn') >= 1) & (col('_sr_rn') <= 20)))
    df_Sample_Bottom_N_Products = df_Sample_Bottom_N_Products.drop('_sr_rn')

    # Step: Sample Top N Products (SampleRows) [converted]
    # Sample Rows: Sample Top N Products
    _w_sr_df_Sample_Top_N_Products = Window.orderBy(monotonically_increasing_id())
    df_Sample_Top_N_Products = df_Sort_Top_Products.withColumn('_sr_rn', row_number().over(_w_sr_df_Sample_Top_N_Products))
    # preserved.lines_range='1..20' ranges=[(1, 20)]
    df_Sample_Top_N_Products = df_Sample_Top_N_Products.filter(((col('_sr_rn') >= 1) & (col('_sr_rn') <= 20)))
    df_Sample_Top_N_Products = df_Sample_Top_N_Products.drop('_sr_rn')

    # Step: Region KPIs (MemoryGroupBy) [converted]
    # Memory Group By: Region KPIs
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Region_KPIs = df_DB_Join_Region_For_Store_KPI.groupBy('region_id').agg(_sum(col("revenue")).alias('revenue'), _sum(col("gross_profit")).alias('gross_profit'), _sum(col("net_profit")).alias('net_profit'), _sum(col("units")).alias('units'), _sum(col("orders")).alias('orders'))

    # Step: Sample Bottom N Stores (SampleRows) [converted]
    # Sample Rows: Sample Bottom N Stores
    _w_sr_df_Sample_Bottom_N_Stores = Window.orderBy(monotonically_increasing_id())
    df_Sample_Bottom_N_Stores = df_Sort_Bottom_Stores.withColumn('_sr_rn', row_number().over(_w_sr_df_Sample_Bottom_N_Stores))
    # preserved.lines_range='1..20' ranges=[(1, 20)]
    df_Sample_Bottom_N_Stores = df_Sample_Bottom_N_Stores.filter(((col('_sr_rn') >= 1) & (col('_sr_rn') <= 20)))
    df_Sample_Bottom_N_Stores = df_Sample_Bottom_N_Stores.drop('_sr_rn')

    # Step: Sample Top N Stores (SampleRows) [converted]
    # Sample Rows: Sample Top N Stores
    _w_sr_df_Sample_Top_N_Stores = Window.orderBy(monotonically_increasing_id())
    df_Sample_Top_N_Stores = df_Sort_Top_Stores.withColumn('_sr_rn', row_number().over(_w_sr_df_Sample_Top_N_Stores))
    # preserved.lines_range='1..20' ranges=[(1, 20)]
    df_Sample_Top_N_Stores = df_Sample_Top_N_Stores.filter(((col('_sr_rn') >= 1) & (col('_sr_rn') <= 20)))
    df_Sample_Top_N_Stores = df_Sample_Top_N_Stores.drop('_sr_rn')

    # Step: Pack Daily KPI (Dummy) [converted]
    # Dummy: Pack Daily KPI
    # Pass-through step - DataFrame unchanged
    df_Dummy_Pack_Daily_KPI = df_Pack_Daily_KPI

    # Step: Pack Monthly KPI (Dummy) [converted]
    # Dummy: Pack Monthly KPI
    # Pass-through step - DataFrame unchanged
    df_Dummy_Pack_Monthly_KPI = df_Pack_Monthly_KPI

    # Step: Pack Other Period KPI (Dummy) [converted]
    # Dummy: Pack Other Period KPI
    # Pass-through step - DataFrame unchanged
    df_Dummy_Pack_Other_Period_KPI = df_Pack_Other_Period_KPI

    # Step: Write Inventory KPIs (TextFileOutput) [converted]
    # Pentaho step: Write Inventory KPIs (type: TextFileOutput)
    # Pentaho filename: /output/finance/kpi/inventory_kpis_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='product_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='inventory_cost' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='qty_on_hand' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sell_through_proxy' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='inventory_kpi_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Inventory_KPIs = df_Inventory_Health_Ratio
    _out_df_Write_Inventory_KPIs = df_Write_Inventory_KPIs.select('product_sk', 'store_sk', 'revenue', 'units', 'inventory_cost', 'qty_on_hand', 'sell_through_proxy', 'inventory_kpi_status', 'batch_id', 'run_id')
    writer = _out_df_Write_Inventory_KPIs.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/inventory_kpis_.csv')

    # Step: Write Bottom N Products (TextFileOutput) [converted]
    # Pentaho step: Write Bottom N Products (type: TextFileOutput)
    # Pentaho filename: /output/finance/kpi/bottom_n_products_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='product_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Bottom_N_Products = df_Sample_Bottom_N_Products
    _out_df_Write_Bottom_N_Products = df_Write_Bottom_N_Products.select('product_sk', 'revenue', 'gross_profit', 'net_profit', 'units', 'orders')
    writer = _out_df_Write_Bottom_N_Products.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/bottom_n_products_.csv')

    # Step: Write Top N Products (TextFileOutput) [converted]
    # Pentaho step: Write Top N Products (type: TextFileOutput)
    # Pentaho filename: /output/finance/kpi/top_n_products_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='product_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Top_N_Products = df_Sample_Top_N_Products
    _out_df_Write_Top_N_Products = df_Write_Top_N_Products.select('product_sk', 'revenue', 'gross_profit', 'net_profit', 'units', 'orders')
    writer = _out_df_Write_Top_N_Products.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/top_n_products_.csv')

    # Step: Write Region KPIs (TextFileOutput) [converted]
    # Pentaho step: Write Region KPIs (type: TextFileOutput)
    # Pentaho filename: /output/finance/kpi/region_kpis_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='region_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Region_KPIs = df_Region_KPIs
    _out_df_Write_Region_KPIs = df_Write_Region_KPIs.select('region_id', 'revenue', 'gross_profit', 'net_profit', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Write_Region_KPIs.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/region_kpis_.csv')

    # Step: Write Bottom N Stores (TextFileOutput) [converted]
    # Pentaho step: Write Bottom N Stores (type: TextFileOutput)
    # Pentaho filename: /output/finance/kpi/bottom_n_stores_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='store_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Bottom_N_Stores = df_Sample_Bottom_N_Stores
    _out_df_Write_Bottom_N_Stores = df_Write_Bottom_N_Stores.select('store_sk', 'revenue', 'gross_profit', 'net_profit', 'units', 'orders')
    writer = _out_df_Write_Bottom_N_Stores.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/bottom_n_stores_.csv')

    # Step: Write Top N Stores (TextFileOutput) [converted]
    # Pentaho step: Write Top N Stores (type: TextFileOutput)
    # Pentaho filename: /output/finance/kpi/top_n_stores_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='store_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='units' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Top_N_Stores = df_Sample_Top_N_Stores
    _out_df_Write_Top_N_Stores = df_Write_Top_N_Stores.select('store_sk', 'revenue', 'gross_profit', 'net_profit', 'units', 'orders')
    writer = _out_df_Write_Top_N_Stores.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/top_n_stores_.csv')

    # Step: Append Period Packs (Append) [converted]
    # Append Streams: Append Period Packs
    # preserved.head_name='Pack Daily KPI'
    # preserved.tail_name='Pack Monthly KPI'
    # preserved.stream_order=['Pack Daily KPI', 'Pack Monthly KPI']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_Period_Packs = df_Dummy_Pack_Daily_KPI.unionByName(df_Dummy_Pack_Monthly_KPI, allowMissingColumns=True)

    # Step: KPI Revenue Positive? (FilterRows) [failed]
    # Filter Rows: KPI Revenue Positive?
    df_KPI_Mapping_Output = df_Append_Period_Packs.filter(col("revenue").isNotNull())
    df_KPI_Reject_Path = df_Append_Period_Packs.filter(~(col("revenue").isNotNull()))
    df_KPI_Revenue_Positive? = df_KPI_Mapping_Output

    # Step: KPI Mapping Output (Dummy) [converted]
    # Dummy: KPI Mapping Output
    # Pass-through step - DataFrame unchanged
    df_Dummy_KPI_Mapping_Output = df_KPI_Mapping_Output

    # Step: KPI Reject Path (Dummy) [converted]
    # Dummy: KPI Reject Path
    # Pass-through step - DataFrame unchanged
    df_Dummy_KPI_Reject_Path = df_KPI_Reject_Path

    # Step: KPI AOV Calculator (Calculator) [converted]
    # Calculator: KPI AOV Calculator
    df_KPI_AOV_Calculator = df_Dummy_KPI_Mapping_Output
    df_KPI_AOV_Calculator = df_KPI_AOV_Calculator.withColumn("aov", ((col("revenue") / col("orders"))).cast('decimal(38,4)'))

    # Step: Write KPI Rejects (TextFileOutput) [converted]
    # Pentaho step: Write KPI Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/finance/kpi_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='period_type' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_KPI_Rejects = df_Dummy_KPI_Reject_Path
    _out_df_Write_KPI_Rejects = df_Write_KPI_Rejects.select('period_type', 'revenue', 'batch_id', 'run_id')
    writer = _out_df_Write_KPI_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/kpi_rejects_.csv')

    # Step: Write KPI Summary JSON (JsonOutput) [converted]
    # Pentaho step: Write KPI Summary JSON (type: JsonOutput)
    df_Write_KPI_Summary_JSON = df_KPI_AOV_Calculator
    df_Write_KPI_Summary_JSON.write \
    .mode('overwrite') \
    .json(
        f'{data_dir}/kpi_summary_.json'
    )
    # preserved.json_bloc='rows' output_value='json_blob'

    # Step: Block KPI Complete (BlockingStep) [converted]
    # Blocking Step: Block KPI Complete
    # preserved.pass_all_rows=True
    # preserved.directory='%%java.io.tmpdir%%'
    # preserved.prefix='block'
    # preserved.cache_size='5000'
    # preserved.compress_files=False
    # LIMITATION: Pentaho temp-file spill (directory/prefix/compress) is replaced by Spark cache/persist.
    df_Block_KPI_Complete = cache_for_reuse(df_Write_Daily_Revenue_KPI)
    _ = df_Block_KPI_Complete.count()  # synchronize: wait for all upstream rows

    # Step: Log KPI Calculation (WriteToLog) [converted]
    # Write to Log: Log KPI Calculation
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=KPI_OK | TRANS=TR_KPI_Calculation | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_KPI_Calculation = logging.getLogger('pentaho.writetolog.Log_KPI_Calculation')
    _log_df_Log_KPI_Calculation.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_KPI_Calculation = df_Block_KPI_Complete
    _log_rows_df_Log_KPI_Calculation = _log_df_df_Log_KPI_Calculation.take(20)
    _log_df_Log_KPI_Calculation.info('Log KPI Calculation' + ' | columns=' + str(_log_df_df_Log_KPI_Calculation.columns))
    _log_df_Log_KPI_Calculation.info('AUDIT | EVENT=KPI_OK | TRANS=TR_KPI_Calculation | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_KPI_Calculation:
        _log_df_Log_KPI_Calculation.info('Log KPI Calculation' + ' | ' + str(_lr.asDict()))
    df_Log_KPI_Calculation = df_Block_KPI_Complete

    # Step: KPI Complete (Dummy) [converted]
    # Dummy: KPI Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_KPI_Complete = df_Log_KPI_Calculation

    log_event(_LOG, "transformation_end")
    return df_Dummy_KPI_Complete
