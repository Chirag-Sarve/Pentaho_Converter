"""PySpark module migrated from Pentaho transformation: TR_Reporting_Output.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/reporting/TR_Reporting_Output.ktr
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
    when,
    coalesce,
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_reporting_output")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Reporting_Output`` step-for-step."""
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

    # Step: Generate Report Catalog (RowGenerator) [converted]
    # Generate Rows: Generate Report Catalog
    data = [('ALL', 'CSV,EXCEL,JSON,XML,TEXT')]
    df_Generate_Report_Catalog = spark.createDataFrame(data, ['report_code', 'export_formats'])

    # Step: Get Report Variables (GetVariable) [converted]
    # Get Variables: Get Report Variables
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'output_path', 'variable': '${OUTPUT_PATH}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'current_date', 'variable': '${CURRENT_DATE}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
    # preserved.output_columns=['batch_id', 'run_id', 'output_path', 'current_date']
    import os
    import re as _re_var
    # Lookup order: Databricks widgets → os.environ → spark.conf (pentaho.var.*) → transformation parameters → empty string
    df_Get_Report_Variables = df_Generate_Report_Catalog
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
    df_Get_Report_Variables = df_Get_Report_Variables.withColumn('batch_id', lit(_batch_id_resolved))
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
    df_Get_Report_Variables = df_Get_Report_Variables.withColumn('run_id', lit(_run_id_resolved))
    # field 'output_path' from variable string '${OUTPUT_PATH}'
    # preserved.field.output_path.trim_type='none'
    # preserved.field.output_path.type='String'
    _output_path_resolved = None
    _dbu__output_path_resolved = globals().get('dbutils')
    if _dbu__output_path_resolved is not None and hasattr(_dbu__output_path_resolved, 'widgets'):
        try:
            _output_path_resolved = _dbu__output_path_resolved.widgets.get('OUTPUT_PATH')
        except Exception:
            _output_path_resolved = None
    if _output_path_resolved in (None, ''):
        import os as _os__output_path_resolved
        _output_path_resolved = _os__output_path_resolved.environ.get('OUTPUT_PATH')
    if _output_path_resolved in (None, ''):
        try:
            _output_path_resolved = spark.conf.get('pentaho.var.OUTPUT_PATH')
        except Exception:
            _output_path_resolved = None
    if _output_path_resolved in (None, ''):
        _output_path_resolved = '${PROJECT_HOME}/output'
    if _output_path_resolved is None:
        _output_path_resolved = ''
    df_Get_Report_Variables = df_Get_Report_Variables.withColumn('output_path', lit(_output_path_resolved))
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
    df_Get_Report_Variables = df_Get_Report_Variables.withColumn('current_date', lit(_current_date_resolved))

    # Step: Read Customer Dashboard (CsvInput) [converted]
    # CSV Input: Read Customer Dashboard
    df_Read_Customer_Dashboard = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('dashboard_name STRING, report_date STRING, revenue DOUBLE, gross_profit DOUBLE, net_profit DOUBLE, gross_margin_pct DOUBLE, units DOUBLE, orders INT, refund_amount DOUBLE, budget_variance DOUBLE, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/customer_segmentation_.csv')
    )

    # Step: Read Daily KPIs (CsvInput) [converted]
    # CSV Input: Read Daily KPIs
    df_Read_Daily_KPIs = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('sales_day STRING, sales_year INT, sales_month INT, sales_quarter INT, revenue DOUBLE, gross_profit DOUBLE, net_profit DOUBLE, units DOUBLE, orders INT, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/daily_revenue_kpi_.csv')
    )

    # Step: Read Executive Dashboard Feed (CsvInput) [converted]
    # CSV Input: Read Executive Dashboard Feed
    df_Read_Executive_Dashboard_Feed = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('dashboard_name STRING, report_date STRING, revenue DOUBLE, gross_profit DOUBLE, net_profit DOUBLE, gross_margin_pct DOUBLE, units DOUBLE, orders INT, refund_amount DOUBLE, budget_variance DOUBLE, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/executive_summary_.csv')
    )

    # Step: Read Financial Summary (CsvInput) [converted]
    # CSV Input: Read Financial Summary
    df_Read_Financial_Summary = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('dashboard_name STRING, report_date STRING, revenue DOUBLE, gross_profit DOUBLE, net_profit DOUBLE, gross_margin_pct DOUBLE, units DOUBLE, orders INT, refund_amount DOUBLE, budget_variance DOUBLE, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/executive_summary_.csv')
    )

    # Step: Read Inventory Dashboard (CsvInput) [converted]
    # CSV Input: Read Inventory Dashboard
    df_Read_Inventory_Dashboard = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('dashboard_name STRING, report_date STRING, revenue DOUBLE, gross_profit DOUBLE, net_profit DOUBLE, gross_margin_pct DOUBLE, units DOUBLE, orders INT, refund_amount DOUBLE, budget_variance DOUBLE, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/inventory_health_.csv')
    )

    # Step: Read Monthly KPIs (CsvInput) [converted]
    # CSV Input: Read Monthly KPIs
    df_Read_Monthly_KPIs = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('sales_day STRING, sales_year INT, sales_month INT, sales_quarter INT, revenue DOUBLE, gross_profit DOUBLE, net_profit DOUBLE, units DOUBLE, orders INT, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/monthly_revenue_kpi_.csv')
    )

    # Step: Read P&L Executive (CsvInput) [failed]
    # CSV Input: Read P&L Executive
    df_Read_P&L_Executive = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('dashboard_name STRING, report_date STRING, revenue DOUBLE, gross_profit DOUBLE, net_profit DOUBLE, gross_margin_pct DOUBLE, units DOUBLE, orders INT, refund_amount DOUBLE, budget_variance DOUBLE, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/executive_summary_.csv')
    )

    # Step: Read Promotion Dashboard (CsvInput) [converted]
    # CSV Input: Read Promotion Dashboard
    df_Read_Promotion_Dashboard = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('dashboard_name STRING, report_date STRING, revenue DOUBLE, gross_profit DOUBLE, net_profit DOUBLE, gross_margin_pct DOUBLE, units DOUBLE, orders INT, refund_amount DOUBLE, budget_variance DOUBLE, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/promotion_effectiveness_.csv')
    )

    # Step: Read Quarterly KPIs (CsvInput) [converted]
    # CSV Input: Read Quarterly KPIs
    df_Read_Quarterly_KPIs = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('sales_day STRING, sales_year INT, sales_month INT, sales_quarter INT, revenue DOUBLE, gross_profit DOUBLE, net_profit DOUBLE, units DOUBLE, orders INT, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/quarterly_revenue_kpi_.csv')
    )

    # Step: Read Regional Dashboard (CsvInput) [converted]
    # CSV Input: Read Regional Dashboard
    df_Read_Regional_Dashboard = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('dashboard_name STRING, report_date STRING, revenue DOUBLE, gross_profit DOUBLE, net_profit DOUBLE, gross_margin_pct DOUBLE, units DOUBLE, orders INT, refund_amount DOUBLE, budget_variance DOUBLE, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/regional_summary_.csv')
    )

    # Step: Read Sales Dashboard (CsvInput) [converted]
    # CSV Input: Read Sales Dashboard
    df_Read_Sales_Dashboard = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('dashboard_name STRING, report_date STRING, revenue DOUBLE, gross_profit DOUBLE, net_profit DOUBLE, gross_margin_pct DOUBLE, units DOUBLE, orders INT, refund_amount DOUBLE, budget_variance DOUBLE, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/store_performance_.csv')
    )

    # Step: Read Store Dashboard (CsvInput) [converted]
    # CSV Input: Read Store Dashboard
    df_Read_Store_Dashboard = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('dashboard_name STRING, report_date STRING, revenue DOUBLE, gross_profit DOUBLE, net_profit DOUBLE, gross_margin_pct DOUBLE, units DOUBLE, orders INT, refund_amount DOUBLE, budget_variance DOUBLE, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/store_performance_.csv')
    )

    # Step: Read Yearly KPIs (CsvInput) [converted]
    # CSV Input: Read Yearly KPIs
    df_Read_Yearly_KPIs = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('sales_day STRING, sales_year INT, sales_month INT, sales_quarter INT, revenue DOUBLE, gross_profit DOUBLE, net_profit DOUBLE, units DOUBLE, orders INT, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/yearly_revenue_kpi_.csv')
    )

    # Step: Unify Report Metadata (Constant) [failed]
    # Add Constants: Unify Report Metadata
    df_Unify_Report_Metadata = df_Read_P&L_Executive
    df_Unify_Report_Metadata = df_Unify_Report_Metadata.withColumn("period_type", lit('REPORT'))
    # preserved.period_type: length='-1', precision='-1'
    df_Unify_Report_Metadata = df_Unify_Report_Metadata.withColumn("operating_profit", lit(0.0))
    # preserved.operating_profit: length='-1', precision='-1'
    df_Unify_Report_Metadata = df_Unify_Report_Metadata.withColumn("net_margin_pct", lit(0.0))
    # preserved.net_margin_pct: length='-1', precision='-1'
    df_Unify_Report_Metadata = df_Unify_Report_Metadata.withColumn("cogs", lit(0.0))
    # preserved.cogs: length='-1', precision='-1'
    df_Unify_Report_Metadata = df_Unify_Report_Metadata.withColumn("tax_amount", lit(0.0))
    # preserved.tax_amount: length='-1', precision='-1'
    df_Unify_Report_Metadata = df_Unify_Report_Metadata.withColumn("discount_amount", lit(0.0))
    # preserved.discount_amount: length='-1', precision='-1'
    df_Unify_Report_Metadata = df_Unify_Report_Metadata.withColumn("shipping_cost", lit(0.0))
    # preserved.shipping_cost: length='-1', precision='-1'
    df_Unify_Report_Metadata = df_Unify_Report_Metadata.withColumn("inventory_cost", lit(0.0))
    # preserved.inventory_cost: length='-1', precision='-1'
    df_Unify_Report_Metadata = df_Unify_Report_Metadata.withColumn("forecast_variance", lit(0.0))
    # preserved.forecast_variance: length='-1', precision='-1'
    df_Unify_Report_Metadata = df_Unify_Report_Metadata.withColumn("report_date", lit('${CURRENT_DATE}'))
    # preserved.report_date: length='-1', precision='-1'

    # Step: Normalize Report Row (Formula) [converted]
    # Formula: Normalize Report Row
    df_Normalize_Report_Row = df_Unify_Report_Metadata
    df_Normalize_Report_Row = df_Normalize_Report_Row.withColumn('formula_result', lit(None))  # empty formula

    # Step: Select P&L Fields (SelectValues) [failed]
    # Select Values: Select P&L Fields
    df_Select_P&L_Fields = df_Normalize_Report_Row.select(col("dashboard_name").alias("dashboard_name"), col("report_date").alias("report_date"), col("period_type").alias("period_type"), col("revenue").alias("revenue"), col("gross_profit").alias("gross_profit"), col("operating_profit").alias("operating_profit"), col("net_profit").alias("net_profit"), col("gross_margin_pct").alias("gross_margin_pct"), col("net_margin_pct").alias("net_margin_pct"), col("cogs").alias("cogs"), col("tax_amount").alias("tax_amount"), col("discount_amount").alias("discount_amount"), col("refund_amount").alias("refund_amount"), col("shipping_cost").alias("shipping_cost"), col("inventory_cost").alias("inventory_cost"), col("budget_variance").alias("budget_variance"), col("forecast_variance").alias("forecast_variance"), col("units").alias("units"), col("orders").alias("orders"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"))

    # Step: Report Has Revenue? (FilterRows) [failed]
    # Filter Rows: Report Has Revenue?
    df_Report_Mapping_Output = df_Select_P&L_Fields.filter(col("revenue").isNotNull())
    df_Abort_Empty_Report = df_Select_P&L_Fields.filter(~(col("revenue").isNotNull()))
    df_Report_Has_Revenue? = df_Report_Mapping_Output

    # Step: Abort Empty Report (Abort) [converted]
    # Abort: Abort Empty Report
    # preserved.row_threshold=0
    # preserved.message='Reporting output stream empty. RUN_ID=${RUN_ID}'
    # preserved.always_log_rows=True
    # preserved.row_threshold_raw='0'
    # Abort operates on its own failure/branch stream df_Abort_Empty_Report (already assigned by upstream Filter/Switch; not overwritten)
    print('Abort sample for', 'Abort Empty Report', df_Abort_Empty_Report.limit(100).collect())  # always_log_rows
    _abort_count_df_Abort_Empty_Report = df_Abort_Empty_Report.count()
    if _abort_count_df_Abort_Empty_Report > 0:  # Abort when any row reaches this step (threshold<=0)
        raise RuntimeError('Reporting output stream empty. RUN_ID=${RUN_ID}')

    # Step: Report Mapping Output (Dummy) [converted]
    # Dummy: Report Mapping Output
    # Pass-through step - DataFrame unchanged
    df_Dummy_Report_Mapping_Output = df_Report_Mapping_Output

    # Step: Write Report Exception Log (TextFileOutput) [converted]
    # Pentaho step: Write Report Exception Log (type: TextFileOutput)
    # Pentaho filename: /rejects/exception_reports/finance/reporting_exceptions_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='dashboard_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Report_Exception_Log = df_Abort_Empty_Report
    _out_df_Write_Report_Exception_Log = df_Write_Report_Exception_Log.select('dashboard_name', 'revenue', 'batch_id', 'run_id')
    writer = _out_df_Write_Report_Exception_Log.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/reporting_exceptions_.log')

    # Step: Clone For Multi Format Export (CloneRow) [failed]
    # Clone Row: Clone For Multi Format Export
    # preserved.nr_clones=5
    # preserved.nr_clone_in_field=False
    # preserved.add_clone_flag=False
    # preserved.clone_flag_field='cloneflag'
    # preserved.add_clone_num=False
    # preserved.clone_num_field='clonenum'
    # preserved.nr_clones_raw='5'
    _clone_parts_df_Clone_For_Multi_Format_Export = []
    _base_df_Clone_For_Multi_Format_Export = df_Select_P&L_Fields
    _orig_df_Clone_For_Multi_Format_Export = _base_df_Clone_For_Multi_Format_Export
    _clone_parts_df_Clone_For_Multi_Format_Export.append(_orig_df_Clone_For_Multi_Format_Export)
    for _ci in range(1, 5 + 1):
        _c = _base_df_Clone_For_Multi_Format_Export
        _clone_parts_df_Clone_For_Multi_Format_Export.append(_c)
    df_Clone_For_Multi_Format_Export = _clone_parts_df_Clone_For_Multi_Format_Export[0]
    for _part in _clone_parts_df_Clone_For_Multi_Format_Export[1:]:
        df_Clone_For_Multi_Format_Export = df_Clone_For_Multi_Format_Export.unionByName(_part, allowMissingColumns=True)

    # Step: CSV Output Reports (TextFileOutput) [converted]
    # Pentaho step: CSV Output Reports (type: TextFileOutput)
    # Pentaho filename: /output/reports/csv/financial_reports_
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
    df_CSV_Output_Reports = df_Clone_For_Multi_Format_Export
    _out_df_CSV_Output_Reports = df_CSV_Output_Reports.select('dashboard_name', 'report_date', 'period_type', 'revenue', 'gross_profit', 'operating_profit', 'net_profit', 'gross_margin_pct', 'net_margin_pct', 'cogs', 'tax_amount', 'discount_amount', 'refund_amount', 'shipping_cost', 'inventory_cost', 'budget_variance', 'forecast_variance', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_CSV_Output_Reports.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/financial_reports_.csv')

    # Step: Excel Output Reports (ExcelOutput) [partial]
    # Microsoft Excel Output (Deprecated): Excel Output Reports
    # Requires spark-excel (com.crealytics.spark.excel) on the cluster classpath.
    # PARTIAL: fonts, templates, sheet protection, and per-cell formats have no 1:1 Spark mapping.
    _excel_df_Excel_Output_Reports = df_Clone_For_Multi_Format_Export.select('dashboard_name', 'report_date', 'period_type', 'revenue', 'gross_profit', 'operating_profit', 'net_profit', 'gross_margin_pct', 'net_margin_pct', 'cogs', 'tax_amount', 'discount_amount', 'refund_amount', 'shipping_cost', 'inventory_cost', 'budget_variance', 'forecast_variance', 'units', 'orders', 'batch_id', 'run_id')
    # NOTE: Field ordering follows Pentaho Excel field list; runtime schema mismatch will fail the select.
    df_Excel_Output_Reports = df_Clone_For_Multi_Format_Export
    (
        _excel_df_Excel_Output_Reports.write.format('com.crealytics.spark.excel')
        .option('dataAddress', "'FinanceReports'!A1")
        .option('header', 'true')
        .mode('overwrite')
        .save(f'{data_dir}/financial_reports_.xls')
    )
    # preserved.filename='${PROJECT_HOME}/output/reports/excel/financial_reports_.xls'
    # preserved.sheetname='FinanceReports'
    # preserved.extension='xls'
    # preserved.header='Y'
    # preserved.append='N'
    # preserved.starting_cell='A1'
    # preserved.autosizecolums='Y'
    # WARNING: Auto-size columns not mapped to spark-excel options.
    # preserved.password_set=True  # value redacted
    # NOTE: create_parent_folder=Y — object stores typically create parents.
    # preserved.add_to_result_filenames='Y'
    # WARNING: Add filenames to result has no Carte/result-file equivalent on Databricks.

    # Step: Export Customer Dashboard CSV (TextFileOutput) [converted]
    # Pentaho step: Export Customer Dashboard CSV (type: TextFileOutput)
    # Pentaho filename: /output/reports/csv/customer_dashboard_
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
    df_Export_Customer_Dashboard_CSV = df_Clone_For_Multi_Format_Export
    _out_df_Export_Customer_Dashboard_CSV = df_Export_Customer_Dashboard_CSV.select('dashboard_name', 'report_date', 'period_type', 'revenue', 'gross_profit', 'operating_profit', 'net_profit', 'gross_margin_pct', 'net_margin_pct', 'cogs', 'tax_amount', 'discount_amount', 'refund_amount', 'shipping_cost', 'inventory_cost', 'budget_variance', 'forecast_variance', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Export_Customer_Dashboard_CSV.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/customer_dashboard_.csv')

    # Step: Export Daily KPIs CSV (TextFileOutput) [converted]
    # Pentaho step: Export Daily KPIs CSV (type: TextFileOutput)
    # Pentaho filename: /output/reports/csv/daily_kpis_
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
    df_Export_Daily_KPIs_CSV = df_Clone_For_Multi_Format_Export
    _out_df_Export_Daily_KPIs_CSV = df_Export_Daily_KPIs_CSV.select('dashboard_name', 'report_date', 'period_type', 'revenue', 'gross_profit', 'operating_profit', 'net_profit', 'gross_margin_pct', 'net_margin_pct', 'cogs', 'tax_amount', 'discount_amount', 'refund_amount', 'shipping_cost', 'inventory_cost', 'budget_variance', 'forecast_variance', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Export_Daily_KPIs_CSV.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/daily_kpis_.csv')

    # Step: Export Executive Dashboard CSV (TextFileOutput) [converted]
    # Pentaho step: Export Executive Dashboard CSV (type: TextFileOutput)
    # Pentaho filename: /output/reports/csv/executive_dashboard_
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
    df_Export_Executive_Dashboard_CSV = df_Clone_For_Multi_Format_Export
    _out_df_Export_Executive_Dashboard_CSV = df_Export_Executive_Dashboard_CSV.select('dashboard_name', 'report_date', 'period_type', 'revenue', 'gross_profit', 'operating_profit', 'net_profit', 'gross_margin_pct', 'net_margin_pct', 'cogs', 'tax_amount', 'discount_amount', 'refund_amount', 'shipping_cost', 'inventory_cost', 'budget_variance', 'forecast_variance', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Export_Executive_Dashboard_CSV.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/executive_dashboard_.csv')

    # Step: Export Financial Summary CSV (TextFileOutput) [converted]
    # Pentaho step: Export Financial Summary CSV (type: TextFileOutput)
    # Pentaho filename: /output/reports/csv/financial_summary_
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
    df_Export_Financial_Summary_CSV = df_Clone_For_Multi_Format_Export
    _out_df_Export_Financial_Summary_CSV = df_Export_Financial_Summary_CSV.select('dashboard_name', 'report_date', 'period_type', 'revenue', 'gross_profit', 'operating_profit', 'net_profit', 'gross_margin_pct', 'net_margin_pct', 'cogs', 'tax_amount', 'discount_amount', 'refund_amount', 'shipping_cost', 'inventory_cost', 'budget_variance', 'forecast_variance', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Export_Financial_Summary_CSV.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/financial_summary_.csv')

    # Step: Export Inventory Dashboard CSV (TextFileOutput) [converted]
    # Pentaho step: Export Inventory Dashboard CSV (type: TextFileOutput)
    # Pentaho filename: /output/reports/csv/inventory_dashboard_
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
    df_Export_Inventory_Dashboard_CSV = df_Clone_For_Multi_Format_Export
    _out_df_Export_Inventory_Dashboard_CSV = df_Export_Inventory_Dashboard_CSV.select('dashboard_name', 'report_date', 'period_type', 'revenue', 'gross_profit', 'operating_profit', 'net_profit', 'gross_margin_pct', 'net_margin_pct', 'cogs', 'tax_amount', 'discount_amount', 'refund_amount', 'shipping_cost', 'inventory_cost', 'budget_variance', 'forecast_variance', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Export_Inventory_Dashboard_CSV.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/inventory_dashboard_.csv')

    # Step: Export Monthly KPIs CSV (TextFileOutput) [converted]
    # Pentaho step: Export Monthly KPIs CSV (type: TextFileOutput)
    # Pentaho filename: /output/reports/csv/monthly_kpis_
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
    df_Export_Monthly_KPIs_CSV = df_Clone_For_Multi_Format_Export
    _out_df_Export_Monthly_KPIs_CSV = df_Export_Monthly_KPIs_CSV.select('dashboard_name', 'report_date', 'period_type', 'revenue', 'gross_profit', 'operating_profit', 'net_profit', 'gross_margin_pct', 'net_margin_pct', 'cogs', 'tax_amount', 'discount_amount', 'refund_amount', 'shipping_cost', 'inventory_cost', 'budget_variance', 'forecast_variance', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Export_Monthly_KPIs_CSV.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/monthly_kpis_.csv')

    # Step: Export Profit And Loss CSV (TextFileOutput) [converted]
    # Pentaho step: Export Profit And Loss CSV (type: TextFileOutput)
    # Pentaho filename: /output/reports/csv/profit_and_loss_
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
    df_Export_Profit_And_Loss_CSV = df_Clone_For_Multi_Format_Export
    _out_df_Export_Profit_And_Loss_CSV = df_Export_Profit_And_Loss_CSV.select('dashboard_name', 'report_date', 'period_type', 'revenue', 'gross_profit', 'operating_profit', 'net_profit', 'gross_margin_pct', 'net_margin_pct', 'cogs', 'tax_amount', 'discount_amount', 'refund_amount', 'shipping_cost', 'inventory_cost', 'budget_variance', 'forecast_variance', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Export_Profit_And_Loss_CSV.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/profit_and_loss_.csv')

    # Step: Export Promotion Dashboard CSV (TextFileOutput) [converted]
    # Pentaho step: Export Promotion Dashboard CSV (type: TextFileOutput)
    # Pentaho filename: /output/reports/csv/promotion_dashboard_
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
    df_Export_Promotion_Dashboard_CSV = df_Clone_For_Multi_Format_Export
    _out_df_Export_Promotion_Dashboard_CSV = df_Export_Promotion_Dashboard_CSV.select('dashboard_name', 'report_date', 'period_type', 'revenue', 'gross_profit', 'operating_profit', 'net_profit', 'gross_margin_pct', 'net_margin_pct', 'cogs', 'tax_amount', 'discount_amount', 'refund_amount', 'shipping_cost', 'inventory_cost', 'budget_variance', 'forecast_variance', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Export_Promotion_Dashboard_CSV.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/promotion_dashboard_.csv')

    # Step: Export Quarterly KPIs CSV (TextFileOutput) [converted]
    # Pentaho step: Export Quarterly KPIs CSV (type: TextFileOutput)
    # Pentaho filename: /output/reports/csv/quarterly_kpis_
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
    df_Export_Quarterly_KPIs_CSV = df_Clone_For_Multi_Format_Export
    _out_df_Export_Quarterly_KPIs_CSV = df_Export_Quarterly_KPIs_CSV.select('dashboard_name', 'report_date', 'period_type', 'revenue', 'gross_profit', 'operating_profit', 'net_profit', 'gross_margin_pct', 'net_margin_pct', 'cogs', 'tax_amount', 'discount_amount', 'refund_amount', 'shipping_cost', 'inventory_cost', 'budget_variance', 'forecast_variance', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Export_Quarterly_KPIs_CSV.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/quarterly_kpis_.csv')

    # Step: Export Regional Dashboard CSV (TextFileOutput) [converted]
    # Pentaho step: Export Regional Dashboard CSV (type: TextFileOutput)
    # Pentaho filename: /output/reports/csv/regional_dashboard_
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
    df_Export_Regional_Dashboard_CSV = df_Clone_For_Multi_Format_Export
    _out_df_Export_Regional_Dashboard_CSV = df_Export_Regional_Dashboard_CSV.select('dashboard_name', 'report_date', 'period_type', 'revenue', 'gross_profit', 'operating_profit', 'net_profit', 'gross_margin_pct', 'net_margin_pct', 'cogs', 'tax_amount', 'discount_amount', 'refund_amount', 'shipping_cost', 'inventory_cost', 'budget_variance', 'forecast_variance', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Export_Regional_Dashboard_CSV.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/regional_dashboard_.csv')

    # Step: Export Sales Dashboard CSV (TextFileOutput) [converted]
    # Pentaho step: Export Sales Dashboard CSV (type: TextFileOutput)
    # Pentaho filename: /output/reports/csv/sales_dashboard_
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
    df_Export_Sales_Dashboard_CSV = df_Clone_For_Multi_Format_Export
    _out_df_Export_Sales_Dashboard_CSV = df_Export_Sales_Dashboard_CSV.select('dashboard_name', 'report_date', 'period_type', 'revenue', 'gross_profit', 'operating_profit', 'net_profit', 'gross_margin_pct', 'net_margin_pct', 'cogs', 'tax_amount', 'discount_amount', 'refund_amount', 'shipping_cost', 'inventory_cost', 'budget_variance', 'forecast_variance', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Export_Sales_Dashboard_CSV.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_dashboard_.csv')

    # Step: Export Store Dashboard CSV (TextFileOutput) [converted]
    # Pentaho step: Export Store Dashboard CSV (type: TextFileOutput)
    # Pentaho filename: /output/reports/csv/store_dashboard_
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
    df_Export_Store_Dashboard_CSV = df_Clone_For_Multi_Format_Export
    _out_df_Export_Store_Dashboard_CSV = df_Export_Store_Dashboard_CSV.select('dashboard_name', 'report_date', 'period_type', 'revenue', 'gross_profit', 'operating_profit', 'net_profit', 'gross_margin_pct', 'net_margin_pct', 'cogs', 'tax_amount', 'discount_amount', 'refund_amount', 'shipping_cost', 'inventory_cost', 'budget_variance', 'forecast_variance', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Export_Store_Dashboard_CSV.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/store_dashboard_.csv')

    # Step: Export Yearly KPIs CSV (TextFileOutput) [converted]
    # Pentaho step: Export Yearly KPIs CSV (type: TextFileOutput)
    # Pentaho filename: /output/reports/csv/yearly_kpis_
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
    df_Export_Yearly_KPIs_CSV = df_Clone_For_Multi_Format_Export
    _out_df_Export_Yearly_KPIs_CSV = df_Export_Yearly_KPIs_CSV.select('dashboard_name', 'report_date', 'period_type', 'revenue', 'gross_profit', 'operating_profit', 'net_profit', 'gross_margin_pct', 'net_margin_pct', 'cogs', 'tax_amount', 'discount_amount', 'refund_amount', 'shipping_cost', 'inventory_cost', 'budget_variance', 'forecast_variance', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Export_Yearly_KPIs_CSV.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/yearly_kpis_.csv')

    # Step: JSON Output Reports (JsonOutput) [converted]
    # Pentaho step: JSON Output Reports (type: JsonOutput)
    df_JSON_Output_Reports = df_Clone_For_Multi_Format_Export
    df_JSON_Output_Reports.write \
    .mode('overwrite') \
    .json(
        f'{data_dir}/financial_reports_.json'
    )
    # preserved.json_bloc='rows' output_value='json_blob'

    # Step: Text File Output Reports (TextFileOutput) [converted]
    # Pentaho step: Text File Output Reports (type: TextFileOutput)
    # Pentaho filename: /output/reports/text/financial_reports_
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
    df_Text_File_Output_Reports = df_Clone_For_Multi_Format_Export
    _out_df_Text_File_Output_Reports = df_Text_File_Output_Reports.select('dashboard_name', 'report_date', 'period_type', 'revenue', 'gross_profit', 'operating_profit', 'net_profit', 'gross_margin_pct', 'net_margin_pct', 'cogs', 'tax_amount', 'discount_amount', 'refund_amount', 'shipping_cost', 'inventory_cost', 'budget_variance', 'forecast_variance', 'units', 'orders', 'batch_id', 'run_id')
    writer = _out_df_Text_File_Output_Reports.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/financial_reports_.txt')

    # Step: XML Output Reports (XMLOutput) [converted]
    # XML Output: XML Output Reports
    # Requires spark-xml (com.databricks.spark.xml) on the cluster classpath.
    df_XML_Output_Reports = df_Clone_For_Multi_Format_Export
    (
        df_XML_Output_Reports.write.format('xml')
        .option('rootTag', 'rows')
        .option('rowTag', 'row')
        .mode('overwrite')
        .save(f'{data_dir}/financial_reports_')
    )

    # Step: Block Report Exports (BlockingStep) [converted]
    # Blocking Step: Block Report Exports
    # preserved.pass_all_rows=True
    # preserved.directory='%%java.io.tmpdir%%'
    # preserved.prefix='block'
    # preserved.cache_size='5000'
    # preserved.compress_files=False
    # LIMITATION: Pentaho temp-file spill (directory/prefix/compress) is replaced by Spark cache/persist.
    df_Block_Report_Exports = cache_for_reuse(df_CSV_Output_Reports)
    _ = df_Block_Report_Exports.count()  # synchronize: wait for all upstream rows

    # Step: Log Reporting Output (WriteToLog) [converted]
    # Write to Log: Log Reporting Output
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=REPORT_EXPORT_OK | TRANS=TR_Reporting_Output | RUN_ID=${RUN_ID} | FORMATS=CSV,EXCEL,JSON,XML,TEXT'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Reporting_Output = logging.getLogger('pentaho.writetolog.Log_Reporting_Output')
    _log_df_Log_Reporting_Output.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Reporting_Output = df_Block_Report_Exports
    _log_rows_df_Log_Reporting_Output = _log_df_df_Log_Reporting_Output.take(20)
    _log_df_Log_Reporting_Output.info('Log Reporting Output' + ' | columns=' + str(_log_df_df_Log_Reporting_Output.columns))
    _log_df_Log_Reporting_Output.info('AUDIT | EVENT=REPORT_EXPORT_OK | TRANS=TR_Reporting_Output | RUN_ID=${RUN_ID} | FORMATS=CSV,EXCEL,JSON,XML,TEXT')
    for _lr in _log_rows_df_Log_Reporting_Output:
        _log_df_Log_Reporting_Output.info('Log Reporting Output' + ' | ' + str(_lr.asDict()))
    df_Log_Reporting_Output = df_Block_Report_Exports

    # Step: Reporting Output Complete (Dummy) [converted]
    # Dummy: Reporting Output Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Reporting_Output_Complete = df_Log_Reporting_Output

    log_event(_LOG, "transformation_end")
    return df_Dummy_Reporting_Output_Complete
