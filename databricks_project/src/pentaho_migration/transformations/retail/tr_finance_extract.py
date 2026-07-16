"""PySpark module migrated from Pentaho transformation: TR_Finance_Extract.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/staging/TR_Finance_Extract.ktr
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
    substring,
    trim,
    when,
    coalesce,
    row_number,
    md5,
    concat,
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_finance_extract")

from pyspark.sql.window import Window


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Finance_Extract`` step-for-step."""
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

    # Step: Generate Extract Control Row (RowGenerator) [converted]
    # Generate Rows: Generate Extract Control Row
    data = [('FINANCE_REPORTING', 'FULL', 'USD')]
    df_Generate_Extract_Control_Row = spark.createDataFrame(data, ['extract_module', 'extract_mode', 'reporting_currency'])

    # Step: Get Variables (GetVariable) [converted]
    # Get Variables: Get Variables
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'dataset_path', 'variable': '${DATASET_PATH}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'project_home', 'variable': '${PROJECT_HOME}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'output_path', 'variable': '${OUTPUT_PATH}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'current_date', 'variable': '${CURRENT_DATE}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
    # preserved.output_columns=['batch_id', 'run_id', 'dataset_path', 'project_home', 'output_path', 'current_date']
    import os
    import re as _re_var
    # Lookup order: Databricks widgets → os.environ → spark.conf (pentaho.var.*) → transformation parameters → empty string
    df_Get_Variables = df_Generate_Extract_Control_Row
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
    df_Get_Variables = df_Get_Variables.withColumn('batch_id', lit(_batch_id_resolved))
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
    df_Get_Variables = df_Get_Variables.withColumn('run_id', lit(_run_id_resolved))
    # field 'dataset_path' from variable string '${DATASET_PATH}'
    # preserved.field.dataset_path.trim_type='none'
    # preserved.field.dataset_path.type='String'
    _dataset_path_resolved = None
    _dbu__dataset_path_resolved = globals().get('dbutils')
    if _dbu__dataset_path_resolved is not None and hasattr(_dbu__dataset_path_resolved, 'widgets'):
        try:
            _dataset_path_resolved = _dbu__dataset_path_resolved.widgets.get('DATASET_PATH')
        except Exception:
            _dataset_path_resolved = None
    if _dataset_path_resolved in (None, ''):
        import os as _os__dataset_path_resolved
        _dataset_path_resolved = _os__dataset_path_resolved.environ.get('DATASET_PATH')
    if _dataset_path_resolved in (None, ''):
        try:
            _dataset_path_resolved = spark.conf.get('pentaho.var.DATASET_PATH')
        except Exception:
            _dataset_path_resolved = None
    if _dataset_path_resolved in (None, ''):
        _dataset_path_resolved = ''
    if _dataset_path_resolved is None:
        _dataset_path_resolved = ''
    df_Get_Variables = df_Get_Variables.withColumn('dataset_path', lit(_dataset_path_resolved))
    # field 'project_home' from variable string '${PROJECT_HOME}'
    # preserved.field.project_home.trim_type='none'
    # preserved.field.project_home.type='String'
    _project_home_resolved = None
    _dbu__project_home_resolved = globals().get('dbutils')
    if _dbu__project_home_resolved is not None and hasattr(_dbu__project_home_resolved, 'widgets'):
        try:
            _project_home_resolved = _dbu__project_home_resolved.widgets.get('PROJECT_HOME')
        except Exception:
            _project_home_resolved = None
    if _project_home_resolved in (None, ''):
        import os as _os__project_home_resolved
        _project_home_resolved = _os__project_home_resolved.environ.get('PROJECT_HOME')
    if _project_home_resolved in (None, ''):
        try:
            _project_home_resolved = spark.conf.get('pentaho.var.PROJECT_HOME')
        except Exception:
            _project_home_resolved = None
    if _project_home_resolved in (None, ''):
        _project_home_resolved = ''
    if _project_home_resolved is None:
        _project_home_resolved = ''
    df_Get_Variables = df_Get_Variables.withColumn('project_home', lit(_project_home_resolved))
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
    df_Get_Variables = df_Get_Variables.withColumn('output_path', lit(_output_path_resolved))
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
    df_Get_Variables = df_Get_Variables.withColumn('current_date', lit(_current_date_resolved))

    # Step: Validate FactSales Dump (GetFileNames) [converted]
    # Get File Names: Validate FactSales Dump
    _list_path_df_Validate_FactSales_Dump = f'{data_dir}/fact'
    try:
        _fs_entries_df_Validate_FactSales_Dump = dbutils.fs.ls(_list_path_df_Validate_FactSales_Dump)
        df_Validate_FactSales_Dump = spark.createDataFrame(
            [(e.path, e.name, e.size, e.modificationTime) for e in _fs_entries_df_Validate_FactSales_Dump],
            ['filename', 'short_filename', 'size', 'last_modified']
        )
    except Exception:
        # Fallback: Hadoop FileSystem listing via SparkContext
        _jpath = spark._jvm.org.apache.hadoop.fs.Path(_list_path_df_Validate_FactSales_Dump)
        _fs = _jpath.getFileSystem(spark._jsc.hadoopConfiguration())
        _statuses = _fs.listStatus(_jpath)
        df_Validate_FactSales_Dump = spark.createDataFrame(
            [(s.getPath().toString(), s.getPath().getName(), s.getLen(), s.getModificationTime())
             for s in _statuses if s.isFile()],
            ['filename', 'short_filename', 'size', 'last_modified']
        )

    # Step: Validate daily_sales_targets.csv (GetFileNames) [failed]
    # Get File Names: Validate daily_sales_targets.csv
    _list_path_df_Validate_daily_sales_targets.csv = '/'
    try:
        _fs_entries_df_Validate_daily_sales_targets.csv = dbutils.fs.ls(_list_path_df_Validate_daily_sales_targets.csv)
        df_Validate_daily_sales_targets.csv = spark.createDataFrame(
            [(e.path, e.name, e.size, e.modificationTime) for e in _fs_entries_df_Validate_daily_sales_targets.csv],
            ['filename', 'short_filename', 'size', 'last_modified']
        )
    except Exception:
        # Fallback: Hadoop FileSystem listing via SparkContext
        _jpath = spark._jvm.org.apache.hadoop.fs.Path(_list_path_df_Validate_daily_sales_targets.csv)
        _fs = _jpath.getFileSystem(spark._jsc.hadoopConfiguration())
        _statuses = _fs.listStatus(_jpath)
        df_Validate_daily_sales_targets.csv = spark.createDataFrame(
            [(s.getPath().toString(), s.getPath().getName(), s.getLen(), s.getModificationTime())
             for s in _statuses if s.isFile()],
            ['filename', 'short_filename', 'size', 'last_modified']
        )

    # Step: Validate exchange_rates.csv (GetFileNames) [failed]
    # Get File Names: Validate exchange_rates.csv
    _list_path_df_Validate_exchange_rates.csv = '/'
    try:
        _fs_entries_df_Validate_exchange_rates.csv = dbutils.fs.ls(_list_path_df_Validate_exchange_rates.csv)
        df_Validate_exchange_rates.csv = spark.createDataFrame(
            [(e.path, e.name, e.size, e.modificationTime) for e in _fs_entries_df_Validate_exchange_rates.csv],
            ['filename', 'short_filename', 'size', 'last_modified']
        )
    except Exception:
        # Fallback: Hadoop FileSystem listing via SparkContext
        _jpath = spark._jvm.org.apache.hadoop.fs.Path(_list_path_df_Validate_exchange_rates.csv)
        _fs = _jpath.getFileSystem(spark._jsc.hadoopConfiguration())
        _statuses = _fs.listStatus(_jpath)
        df_Validate_exchange_rates.csv = spark.createDataFrame(
            [(s.getPath().toString(), s.getPath().getName(), s.getLen(), s.getModificationTime())
             for s in _statuses if s.isFile()],
            ['filename', 'short_filename', 'size', 'last_modified']
        )

    # Step: Validate payments.csv (GetFileNames) [failed]
    # Get File Names: Validate payments.csv
    _list_path_df_Validate_payments.csv = '/'
    try:
        _fs_entries_df_Validate_payments.csv = dbutils.fs.ls(_list_path_df_Validate_payments.csv)
        df_Validate_payments.csv = spark.createDataFrame(
            [(e.path, e.name, e.size, e.modificationTime) for e in _fs_entries_df_Validate_payments.csv],
            ['filename', 'short_filename', 'size', 'last_modified']
        )
    except Exception:
        # Fallback: Hadoop FileSystem listing via SparkContext
        _jpath = spark._jvm.org.apache.hadoop.fs.Path(_list_path_df_Validate_payments.csv)
        _fs = _jpath.getFileSystem(spark._jsc.hadoopConfiguration())
        _statuses = _fs.listStatus(_jpath)
        df_Validate_payments.csv = spark.createDataFrame(
            [(s.getPath().toString(), s.getPath().getName(), s.getLen(), s.getModificationTime())
             for s in _statuses if s.isFile()],
            ['filename', 'short_filename', 'size', 'last_modified']
        )

    # Step: Validate returns.csv (GetFileNames) [failed]
    # Get File Names: Validate returns.csv
    _list_path_df_Validate_returns.csv = '/'
    try:
        _fs_entries_df_Validate_returns.csv = dbutils.fs.ls(_list_path_df_Validate_returns.csv)
        df_Validate_returns.csv = spark.createDataFrame(
            [(e.path, e.name, e.size, e.modificationTime) for e in _fs_entries_df_Validate_returns.csv],
            ['filename', 'short_filename', 'size', 'last_modified']
        )
    except Exception:
        # Fallback: Hadoop FileSystem listing via SparkContext
        _jpath = spark._jvm.org.apache.hadoop.fs.Path(_list_path_df_Validate_returns.csv)
        _fs = _jpath.getFileSystem(spark._jsc.hadoopConfiguration())
        _statuses = _fs.listStatus(_jpath)
        df_Validate_returns.csv = spark.createDataFrame(
            [(s.getPath().toString(), s.getPath().getName(), s.getLen(), s.getModificationTime())
             for s in _statuses if s.isFile()],
            ['filename', 'short_filename', 'size', 'last_modified']
        )

    # Step: Append Source Checks A (Append) [converted]
    # Append Streams: Append Source Checks A
    # preserved.head_name='Validate FactSales Dump'
    # preserved.tail_name='Validate payments.csv'
    # preserved.stream_order=['Validate FactSales Dump', 'Validate payments.csv']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_Source_Checks_A = df_Validate_FactSales_Dump.unionByName(df_Validate_payments.csv, allowMissingColumns=True)

    # Step: Append Source Checks B (Append) [converted]
    # Append Streams: Append Source Checks B
    # preserved.head_name='Append Source Checks A'
    # preserved.tail_name='Validate returns.csv'
    # preserved.stream_order=['Append Source Checks A', 'Validate returns.csv']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_Source_Checks_B = df_Append_Source_Checks_A.unionByName(df_Validate_returns.csv, allowMissingColumns=True)

    # Step: Append Source Checks C (Append) [converted]
    # Append Streams: Append Source Checks C
    # preserved.head_name='Append Source Checks B'
    # preserved.tail_name='Validate exchange_rates.csv'
    # preserved.stream_order=['Append Source Checks B', 'Validate exchange_rates.csv']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_Source_Checks_C = df_Append_Source_Checks_B.unionByName(df_Validate_exchange_rates.csv, allowMissingColumns=True)

    # Step: Append Source Checks D (Append) [converted]
    # Append Streams: Append Source Checks D
    # preserved.head_name='Append Source Checks C'
    # preserved.tail_name='Validate daily_sales_targets.csv'
    # preserved.stream_order=['Append Source Checks C', 'Validate daily_sales_targets.csv']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_Source_Checks_D = df_Append_Source_Checks_C.unionByName(df_Validate_daily_sales_targets.csv, allowMissingColumns=True)

    # Step: Required Finance Files Found? (FilterRows) [failed]
    # Filter Rows: Required Finance Files Found?
    df_Log_Files_Ready = df_Append_Source_Checks_D.filter((col("file_exists") == lit('Y')))
    df_Abort_Missing_Finance_Source = df_Append_Source_Checks_D.filter(~((col("file_exists") == lit('Y'))))
    df_Required_Finance_Files_Found? = df_Log_Files_Ready

    # Step: Abort Missing Finance Source (Abort) [converted]
    # Abort: Abort Missing Finance Source
    # preserved.row_threshold=0
    # preserved.message='Required Finance/Reporting source missing. RUN_ID=${RUN_ID}'
    # preserved.always_log_rows=True
    # preserved.row_threshold_raw='0'
    # Abort operates on its own failure/branch stream df_Abort_Missing_Finance_Source (already assigned by upstream Filter/Switch; not overwritten)
    print('Abort sample for', 'Abort Missing Finance Source', df_Abort_Missing_Finance_Source.limit(100).collect())  # always_log_rows
    _abort_count_df_Abort_Missing_Finance_Source = df_Abort_Missing_Finance_Source.count()
    if _abort_count_df_Abort_Missing_Finance_Source > 0:  # Abort when any row reaches this step (threshold<=0)
        raise RuntimeError('Required Finance/Reporting source missing. RUN_ID=${RUN_ID}')

    # Step: Log Files Ready (WriteToLog) [failed]
    # Write to Log: Log Files Ready
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=EXTRACT_FILES_OK | TRANS=TR_Finance_Extract | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Files_Ready = logging.getLogger('pentaho.writetolog.Log_Files_Ready')
    _log_df_Log_Files_Ready.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Files_Ready = df_Required_Finance_Files_Found?
    _log_rows_df_Log_Files_Ready = _log_df_df_Log_Files_Ready.take(20)
    _log_df_Log_Files_Ready.info('Log Files Ready' + ' | columns=' + str(_log_df_df_Log_Files_Ready.columns))
    _log_df_Log_Files_Ready.info('AUDIT | EVENT=EXTRACT_FILES_OK | TRANS=TR_Finance_Extract | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Files_Ready:
        _log_df_Log_Files_Ready.info('Log Files Ready' + ' | ' + str(_lr.asDict()))
    df_Log_Files_Ready = df_Required_Finance_Files_Found?

    # Step: Optional Excel Targets Overlay (ExcelInput) [converted]
    # Excel Input: Optional Excel Targets Overlay
    df_Optional_Excel_Targets_Overlay = (
        spark.read.format('com.crealytics.spark.excel')
        .option('sheetName', 'Sheet1')
        .option('header', 'true')
        .load('/metadata/rules/finance_targets_overlay.xlsx')
    )

    # Step: Read Daily Targets (CsvInput) [converted]
    # CSV Input: Read Daily Targets
    df_Read_Daily_Targets = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('target_id STRING, store_id STRING, region_id STRING, target_date STRING, sales_target_amount STRING, orders_target STRING, currency_code STRING, channel STRING, created_by STRING')
        .load('/daily_sales_targets.csv')
    )

    # Step: Read Exchange Rates (CsvInput) [converted]
    # CSV Input: Read Exchange Rates
    df_Read_Exchange_Rates = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('rate_id STRING, rate_date STRING, base_currency STRING, quote_currency STRING, exchange_rate STRING, source STRING')
        .load('/exchange_rates.csv')
    )

    # Step: Read FactInventory (CsvInput) [converted]
    # CSV Input: Read FactInventory
    df_Read_FactInventory = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('inventory_sk INT, inventory_id STRING, product_sk INT, store_sk INT, supplier_sk INT, date_sk INT, quantity_on_hand DOUBLE, quantity_reserved DOUBLE, stock_value DOUBLE, unit_cost DOUBLE, reorder_level DOUBLE, product_id STRING, store_id STRING, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/fact_inventory_.csv')
    )

    # Step: Read FactPayments (CsvInput) [converted]
    # CSV Input: Read FactPayments
    df_Read_FactPayments = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('payment_id STRING, order_id STRING, payment_method STRING, payment_status STRING, payment_date STRING, amount STRING, currency_code STRING, card_brand STRING, transaction_ref STRING, gateway STRING')
        .load('/payments.csv')
    )

    # Step: Read FactReturns (TextFileInput) [converted]
    # Pentaho step: Read FactReturns (type: TextFileInput)
    # INFO: preserved Legacy Text File Input option: date_format_lenient='Y'
    # Pentaho filename: /returns.csv
    # NOTE: Spark CSV outputs are directories — load the same path written by Text File Output (not an individual part-*.csv file)
    # NOTE: missing/empty/corrupt files fail or yield empty DataFrames at Spark runtime (use PERMISSIVE mode / upstream path checks as needed)
    df_Read_FactReturns = (
        spark.read
        .option("header", True)
        .option("sep", ',')
        .option("quote", '"')
        .option("encoding", 'UTF-8')
        .option("inferSchema", False)
        .schema('return_id STRING, order_item_id STRING, order_id STRING, product_id STRING, return_date STRING, return_reason STRING, return_status STRING, quantity_returned STRING, refund_amount STRING, restocking_fee STRING, notes STRING')
        .csv(f'{data_dir}/returns.csv')
    )
    # INFO: preserved.field_format name='return_id' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='order_item_id' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='order_id' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='product_id' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='return_date' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='return_reason' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='return_status' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='quantity_returned' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='refund_amount' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='restocking_fee' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='notes' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    df_Read_FactReturns = df_Read_FactReturns.select(col('return_id').alias('return_id'), col('order_item_id').alias('order_item_id'), col('order_id').alias('order_id'), col('product_id').alias('product_id'), col('return_date').alias('return_date'), col('return_reason').alias('return_reason'), col('return_status').alias('return_status'), col('quantity_returned').alias('quantity_returned'), col('refund_amount').alias('refund_amount'), col('restocking_fee').alias('restocking_fee'), col('notes').alias('notes'))
    df_Read_FactReturns = df_Read_FactReturns.filter(~((col('return_id').isNull() | (length(trim(col('return_id').cast('string'))) == 0)) & (col('order_item_id').isNull() | (length(trim(col('order_item_id').cast('string'))) == 0)) & (col('order_id').isNull() | (length(trim(col('order_id').cast('string'))) == 0)) & (col('product_id').isNull() | (length(trim(col('product_id').cast('string'))) == 0)) & (col('return_date').isNull() | (length(trim(col('return_date').cast('string'))) == 0)) & (col('return_reason').isNull() | (length(trim(col('return_reason').cast('string'))) == 0)) & (col('return_status').isNull() | (length(trim(col('return_status').cast('string'))) == 0)) & (col('quantity_returned').isNull() | (length(trim(col('quantity_returned').cast('string'))) == 0)) & (col('refund_amount').isNull() | (length(trim(col('refund_amount').cast('string'))) == 0)) & (col('restocking_fee').isNull() | (length(trim(col('restocking_fee').cast('string'))) == 0)) & (col('notes').isNull() | (length(trim(col('notes').cast('string'))) == 0))))
    df_Read_FactReturns = df_Read_FactReturns.withColumn('source_row_num', monotonically_increasing_id())

    # Step: Read FactSales (CsvInput) [converted]
    # CSV Input: Read FactSales
    df_Read_FactSales = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('sales_sk INT, order_item_id STRING, order_id STRING, customer_sk INT, product_sk INT, store_sk INT, employee_sk INT, promotion_sk INT, date_sk INT, quantity_sold DOUBLE, unit_price DOUBLE, extended_price DOUBLE, discount_amount_calc DOUBLE, net_sales_amount DOUBLE, tax_amount_calc DOUBLE, shipping_cost_calc DOUBLE, total_revenue DOUBLE, profit DOUBLE, margin DOUBLE, converted_amount_usd DOUBLE, return_amount DOUBLE, refund_amount_calc DOUBLE, currency_code STRING, channel_mapped STRING, order_date STRING, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/fact_sales_.csv')
    )

    # Step: Read Finance Policy JSON (JsonInput) [converted]
    # JSON Input: Read Finance Policy JSON
    df_Read_Finance_Policy_JSON = spark.read.format('json').option('multiline', 'true').load('${PROJECT_HOME}/metadata/rules/finance_reporting_policy.json')

    # Step: Read Finance Validation Rules XML (getXMLData) [converted]
    # XML Input: Read Finance Validation Rules XML
    df_Read_Finance_Validation_Rules_XML = spark.read.format('xml').option('rowTag', 'row').load('${PROJECT_HOME}/metadata/rules/finance_validation_rules.xml')

    # Step: Read Fixed FX Reference (FixedInput) [partial]
    # Fixed File Input: Read Fixed FX Reference
    # Pentaho buffer_size='50000' (no Spark CSV/text reader equivalent)
    # Pentaho lazy conversion enabled (applied at Pentaho runtime, not in Spark reader)
    # Pentaho filename: /metadata/rules/finance_fx_fixed.txt
    # NOTE: Spark CSV outputs are directories — load the same path written by Text File Output (not an individual part-*.csv file)
    # NOTE: missing/empty/corrupt files fail or yield empty DataFrames at Spark runtime (use PERMISSIVE mode / upstream path checks as needed)
    # TODO: Fixed-width input approximated via substring parsing.
    _tfi_raw_df_Read_Fixed_FX_Reference = (
        spark.read.format("text")
        .option("encoding", 'UTF-8')
        .load(f'{data_dir}/finance_fx_fixed.txt')
    )
    # WARNING: Fixed-width field 'fx_base' missing length — using full line.
    # WARNING: Fixed-width field 'fx_quote' missing length — using full line.
    # WARNING: Fixed-width field 'fx_rate' missing length — using full line.
    # WARNING: Fixed-width field 'fx_source' missing length — using full line.
    df_Read_Fixed_FX_Reference = _tfi_raw_df_Read_Fixed_FX_Reference.select(col("value").alias('fx_base'), col("value").alias('fx_quote'), col("value").alias('fx_rate'), col("value").alias('fx_source'))
    df_Read_Fixed_FX_Reference = df_Read_Fixed_FX_Reference.select(col('fx_base').alias('fx_base'), col('fx_quote').alias('fx_quote'), col('fx_rate').alias('fx_rate'), col('fx_source').alias('fx_source'))

    # Step: Prepare Target Lookup Keys (SelectValues) [converted]
    # Select Values: Prepare Target Lookup Keys
    df_Prepare_Target_Lookup_Keys = df_Read_Daily_Targets.select(col("store_id").alias("tgt_store_id"), col("region_id").alias("region_id"), col("target_date").alias("target_date"), col("sales_target_amount").alias("sales_target_amount"), col("orders_target").alias("orders_target"), col("currency_code").alias("tgt_currency"), col("channel").alias("tgt_channel"))

    # Step: Prepare FX Lookup Keys (SelectValues) [converted]
    # Select Values: Prepare FX Lookup Keys
    df_Prepare_FX_Lookup_Keys = df_Read_Exchange_Rates.select(col("quote_currency").alias("fx_currency"), col("exchange_rate").alias("exchange_rate"), col("rate_date").alias("rate_date"), col("base_currency").alias("base_currency"), col("source").alias("fx_source"))

    # Step: Prepare Inventory Cost Lookup (SelectValues) [converted]
    # Select Values: Prepare Inventory Cost Lookup
    df_Prepare_Inventory_Cost_Lookup = df_Read_FactInventory.select(col("product_sk").alias("inv_product_sk"), col("store_sk").alias("inv_store_sk"), col("stock_value").alias("inventory_cost"), col("quantity_on_hand").alias("quantity_on_hand"), col("unit_cost").alias("unit_cost"))

    # Step: Prepare Payments Lookup (SelectValues) [converted]
    # Select Values: Prepare Payments Lookup
    df_Prepare_Payments_Lookup = df_Read_FactPayments.select(col("order_id").alias("pay_order_id"), col("amount").alias("payment_amount"), col("payment_status").alias("payment_status"), col("payment_method").alias("payment_method"), col("currency_code").alias("payment_currency"))

    # Step: Prepare Returns Lookup (SelectValues) [converted]
    # Select Values: Prepare Returns Lookup
    df_Prepare_Returns_Lookup = df_Read_FactReturns.select(col("order_item_id").alias("ret_order_item_id"), col("refund_amount").alias("refund_amount_src"), col("quantity_returned").alias("quantity_returned"), col("return_status").alias("return_status"), col("return_date").alias("return_date"))

    # Step: Sample FactSales Peek (SampleRows) [converted]
    # Sample Rows: Sample FactSales Peek
    _w_sr_df_Sample_FactSales_Peek = Window.orderBy(monotonically_increasing_id())
    df_Sample_FactSales_Peek = df_Read_FactSales.withColumn('_sr_rn', row_number().over(_w_sr_df_Sample_FactSales_Peek))
    # preserved.lines_range='1..5' ranges=[(1, 5)]
    df_Sample_FactSales_Peek = df_Sample_FactSales_Peek.filter(((col('_sr_rn') >= 1) & (col('_sr_rn') <= 5)))
    df_Sample_FactSales_Peek = df_Sample_FactSales_Peek.drop('_sr_rn')

    # Step: Prep Target Merge Keys (SelectValues) [converted]
    # Select Values: Prep Target Merge Keys
    df_Prep_Target_Merge_Keys = df_Prepare_Target_Lookup_Keys.select(col("sales_target_amount").alias("sales_target_amount"), col("tgt_store_id").alias("store_sk"), col("region_id").alias("region_id"), col("target_date").alias("target_date"))

    # Step: Capture Extract Timestamp (SystemInfo) [converted]
    # System Info: Capture Extract Timestamp
    df_Capture_Extract_Timestamp = df_Sample_FactSales_Peek
    df_Capture_Extract_Timestamp = df_Capture_Extract_Timestamp.withColumn("extract_ts", current_date())
    df_Capture_Extract_Timestamp = df_Capture_Extract_Timestamp.withColumn("extract_start", current_date())

    # Step: Sort Targets For Merge (SortRows) [converted]
    # Sort Rows: Sort Targets For Merge
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Targets_For_Merge = df_Prep_Target_Merge_Keys
    _sort_df_Sort_Targets_For_Merge = _sort_df_Sort_Targets_For_Merge.withColumn("_sort_ci_store_sk", lower(col("store_sk").cast("string")))
    df_Sort_Targets_For_Merge = _sort_df_Sort_Targets_For_Merge.orderBy(col("_sort_ci_store_sk").asc_nulls_last())
    df_Sort_Targets_For_Merge = df_Sort_Targets_For_Merge.drop("_sort_ci_store_sk")

    # Step: Tag Finance Batch Metadata (Constant) [converted]
    # Add Constants: Tag Finance Batch Metadata
    df_Tag_Finance_Batch_Metadata = df_Capture_Extract_Timestamp
    df_Tag_Finance_Batch_Metadata = df_Tag_Finance_Batch_Metadata.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Tag_Finance_Batch_Metadata = df_Tag_Finance_Batch_Metadata.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'
    df_Tag_Finance_Batch_Metadata = df_Tag_Finance_Batch_Metadata.withColumn("etl_layer", lit('FINANCE_EXTRACT'))
    # preserved.etl_layer: length='-1', precision='-1'
    df_Tag_Finance_Batch_Metadata = df_Tag_Finance_Batch_Metadata.withColumn("source_system", lit('RETAIL_DWH'))
    # preserved.source_system: length='-1', precision='-1'

    # Step: Clone Sales For Dual Land (CloneRow) [converted]
    # Clone Row: Clone Sales For Dual Land
    # preserved.nr_clones=2
    # preserved.nr_clone_in_field=False
    # preserved.add_clone_flag=False
    # preserved.clone_flag_field='cloneflag'
    # preserved.add_clone_num=False
    # preserved.clone_num_field='clonenum'
    # preserved.nr_clones_raw='2'
    _clone_parts_df_Clone_Sales_For_Dual_Land = []
    _base_df_Clone_Sales_For_Dual_Land = df_Tag_Finance_Batch_Metadata
    _orig_df_Clone_Sales_For_Dual_Land = _base_df_Clone_Sales_For_Dual_Land
    _clone_parts_df_Clone_Sales_For_Dual_Land.append(_orig_df_Clone_Sales_For_Dual_Land)
    for _ci in range(1, 2 + 1):
        _c = _base_df_Clone_Sales_For_Dual_Land
        _clone_parts_df_Clone_Sales_For_Dual_Land.append(_c)
    df_Clone_Sales_For_Dual_Land = _clone_parts_df_Clone_Sales_For_Dual_Land[0]
    for _part in _clone_parts_df_Clone_Sales_For_Dual_Land[1:]:
        df_Clone_Sales_For_Dual_Land = df_Clone_Sales_For_Dual_Land.unionByName(_part, allowMissingColumns=True)

    # Step: Write FX Staging (TextFileOutput) [converted]
    # Pentaho step: Write FX Staging (type: TextFileOutput)
    # Pentaho filename: /output/finance/staging/stg_exchange_rates_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='rate_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rate_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='base_currency' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quote_currency' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='exchange_rate' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='source' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_FX_Staging = df_Tag_Finance_Batch_Metadata
    _out_df_Write_FX_Staging = df_Write_FX_Staging.select('rate_id', 'rate_date', 'base_currency', 'quote_currency', 'exchange_rate', 'source', 'batch_id', 'run_id')
    writer = _out_df_Write_FX_Staging.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_exchange_rates_.csv')

    # Step: Write Payments Staging (TextFileOutput) [converted]
    # Pentaho step: Write Payments Staging (type: TextFileOutput)
    # Pentaho filename: /output/finance/staging/stg_fact_payments_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='payment_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_method' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='currency_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Payments_Staging = df_Tag_Finance_Batch_Metadata
    _out_df_Write_Payments_Staging = df_Write_Payments_Staging.select('payment_id', 'order_id', 'payment_method', 'payment_status', 'payment_date', 'amount', 'currency_code', 'batch_id', 'run_id')
    writer = _out_df_Write_Payments_Staging.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_fact_payments_.csv')

    # Step: Write Returns Staging (TextFileOutput) [converted]
    # Pentaho step: Write Returns Staging (type: TextFileOutput)
    # Pentaho filename: /output/finance/staging/stg_fact_returns_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='return_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_item_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='return_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='refund_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quantity_returned' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Returns_Staging = df_Tag_Finance_Batch_Metadata
    _out_df_Write_Returns_Staging = df_Write_Returns_Staging.select('return_id', 'order_item_id', 'order_id', 'product_id', 'return_date', 'refund_amount', 'quantity_returned', 'batch_id', 'run_id')
    writer = _out_df_Write_Returns_Staging.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_fact_returns_.csv')

    # Step: Write Targets Staging (TextFileOutput) [converted]
    # Pentaho step: Write Targets Staging (type: TextFileOutput)
    # Pentaho filename: /output/finance/staging/stg_daily_targets_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='target_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='region_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='target_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sales_target_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='orders_target' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='currency_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='channel' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Targets_Staging = df_Tag_Finance_Batch_Metadata
    _out_df_Write_Targets_Staging = df_Write_Targets_Staging.select('target_id', 'store_id', 'region_id', 'target_date', 'sales_target_amount', 'orders_target', 'currency_code', 'channel', 'batch_id', 'run_id')
    writer = _out_df_Write_Targets_Staging.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_daily_targets_.csv')

    # Step: Prepare Sales Staging Fields (SelectValues) [converted]
    # Select Values: Prepare Sales Staging Fields
    df_Prepare_Sales_Staging_Fields = df_Clone_Sales_For_Dual_Land.select(col("order_item_id").alias("order_item_id"), col("order_id").alias("order_id"), col("order_date").alias("order_date"), col("store_sk").alias("store_sk"), col("product_sk").alias("product_sk"), col("customer_sk").alias("customer_sk"), col("employee_sk").alias("employee_sk"), col("promotion_sk").alias("promotion_sk"), col("channel_mapped").alias("channel_mapped"), col("currency_code").alias("currency_code"), col("quantity_sold").alias("quantity_sold"), col("extended_price").alias("extended_price"), col("discount_amount_calc").alias("discount_amount_calc"), col("net_sales_amount").alias("net_sales_amount"), col("tax_amount_calc").alias("tax_amount_calc"), col("shipping_cost_calc").alias("shipping_cost_calc"), col("total_revenue").alias("total_revenue"), col("profit").alias("profit"), col("converted_amount_usd").alias("converted_amount_usd"), col("refund_amount_calc").alias("refund_amount_calc"), col("return_amount").alias("return_amount"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"), col("extract_ts").alias("extract_ts"))

    # Step: Lookup Inventory Cost (StreamLookup) [failed]
    # Stream Lookup: Lookup Inventory Cost
    # StreamLookup 'Lookup Inventory Cost': no join keys — lookup join not generated
    df_Lookup_Inventory_Cost = df_Prepare_Sales_Staging_Fields

    # Step: Lookup Return Refund (StreamLookup) [failed]
    # Stream Lookup: Lookup Return Refund
    # StreamLookup 'Lookup Return Refund': no join keys — lookup join not generated
    df_Lookup_Return_Refund = df_Lookup_Inventory_Cost

    # Step: Lookup Payment Amount (StreamLookup) [failed]
    # Stream Lookup: Lookup Payment Amount
    # StreamLookup 'Lookup Payment Amount': no join keys — lookup join not generated
    df_Lookup_Payment_Amount = df_Lookup_Return_Refund

    # Step: Lookup FX Rate (StreamLookup) [failed]
    # Stream Lookup: Lookup FX Rate
    # StreamLookup 'Lookup FX Rate': no join keys — lookup join not generated
    df_Lookup_FX_Rate = df_Lookup_Payment_Amount

    # Step: Sort Sales For Target Merge (SortRows) [converted]
    # Sort Rows: Sort Sales For Target Merge
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Sales_For_Target_Merge = df_Lookup_FX_Rate
    _sort_df_Sort_Sales_For_Target_Merge = _sort_df_Sort_Sales_For_Target_Merge.withColumn("_sort_ci_store_sk", lower(col("store_sk").cast("string")))
    df_Sort_Sales_For_Target_Merge = _sort_df_Sort_Sales_For_Target_Merge.orderBy(col("_sort_ci_store_sk").asc_nulls_last())
    df_Sort_Sales_For_Target_Merge = df_Sort_Sales_For_Target_Merge.drop("_sort_ci_store_sk")

    # Step: Merge Sales With Targets Context (MergeJoin) [converted]
    # Merge Join: Merge Sales With Targets Context
    # preserved.join_type='LEFT OUTER'
    # preserved.join_keys=[{'left': 'store_sk', 'right': 'store_sk'}]
    # NOTE: PDI Merge Join requires both streams pre-sorted on join keys — Spark join() does not enforce sort order (preserve sort steps upstream if needed)
    # WARNING: MergeJoin 'Merge Sales With Targets Context': null join keys do not match (Spark == / PDI merge semantics); duplicate keys produce a cartesian explosion within the key group; ensure key data types match across streams
    _joined_df_Merge_Sales_With_Targets_Context = df_Sort_Sales_For_Target_Merge.join(df_Sort_Targets_For_Merge, on=["store_sk"], how='left')
    # WARNING: MergeJoin 'Merge Sales With Targets Context': column lineage unavailable — join output may contain ambiguous duplicate column names
    df_Merge_Sales_With_Targets_Context = _joined_df_Merge_Sales_With_Targets_Context

    # Step: Default Missing Financials (IfNull) [converted]
    # If Field Value Is Null: Default Missing Financials
    df_Default_Missing_Financials = df_Merge_Sales_With_Targets_Context
    df_Default_Missing_Financials = df_Default_Missing_Financials.withColumn('inventory_cost', when(col('inventory_cost').isNull(), lit(0)).otherwise(col('inventory_cost')))
    df_Default_Missing_Financials = df_Default_Missing_Financials.withColumn('refund_amount_src', when(col('refund_amount_src').isNull(), lit(0)).otherwise(col('refund_amount_src')))
    df_Default_Missing_Financials = df_Default_Missing_Financials.withColumn('payment_amount', when(col('payment_amount').isNull(), lit(0)).otherwise(col('payment_amount')))
    df_Default_Missing_Financials = df_Default_Missing_Financials.withColumn('exchange_rate', when(col('exchange_rate').isNull(), lit(1)).otherwise(col('exchange_rate')))
    df_Default_Missing_Financials = df_Default_Missing_Financials.withColumn('sales_target_amount', when(col('sales_target_amount').isNull(), lit(0)).otherwise(col('sales_target_amount')))

    # Step: Normalize Extract Measures (Formula) [converted]
    # Formula: Normalize Extract Measures
    df_Normalize_Extract_Measures = df_Default_Missing_Financials
    df_Normalize_Extract_Measures = df_Normalize_Extract_Measures.withColumn('formula_result', lit(None))  # empty formula

    # Step: Hash Unique Finance Lines (UniqueRowsByHashSet) [converted]
    # Unique Rows (HashSet): Hash Unique Finance Lines
    # preserved.reject_duplicate_row=N error_description=''
    # preserved.store_values=True
    # preserved.count_rows=False count_field='count' compare_fields=['order_item_id', 'order_id']
    df_Hash_Unique_Finance_Lines = df_Normalize_Extract_Measures.dropDuplicates(["order_item_id", "order_id"])

    # Step: MD5 Finance Extract Fingerprint (CheckSum) [converted]
    # Add a Checksum: MD5 Finance Extract Fingerprint
    df_MD5_Finance_Extract_Fingerprint = df_Hash_Unique_Finance_Lines
    df_MD5_Finance_Extract_Fingerprint = df_MD5_Finance_Extract_Fingerprint.withColumn("extract_checksum", md5(concat(coalesce(col("order_item_id").cast("string"), lit("")), coalesce(col("order_id").cast("string"), lit("")), coalesce(col("revenue_usd").cast("string"), lit("")), coalesce(col("batch_id").cast("string"), lit("")))))
    # preserved.checksumtype='MD5' resultType='hexadecimal' fields=['order_item_id', 'order_id', 'revenue_usd', 'batch_id']

    # Step: Reject Invalid Currency? (FilterRows) [failed]
    # Filter Rows: Reject Invalid Currency?
    df_Write_Extract_Rejects = df_MD5_Finance_Extract_Fingerprint.filter(col("currency_code").isNull())
    df_Finance_Mapping_Output = df_MD5_Finance_Extract_Fingerprint.filter(~(col("currency_code").isNull()))
    df_Reject_Invalid_Currency? = df_Write_Extract_Rejects

    # Step: Finance Mapping Output (Dummy) [converted]
    # Dummy: Finance Mapping Output
    # Pass-through step - DataFrame unchanged
    df_Dummy_Finance_Mapping_Output = df_Finance_Mapping_Output

    # Step: Write Extract Rejects (TextFileOutput) [failed]
    # Pentaho step: Write Extract Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/finance/finance_extract_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='order_item_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='currency_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Extract_Rejects = df_Reject_Invalid_Currency?
    _out_df_Write_Extract_Rejects = df_Write_Extract_Rejects.select('order_item_id', 'order_id', 'currency_code', 'batch_id', 'run_id')
    writer = _out_df_Write_Extract_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/finance_extract_rejects_.csv')

    # Step: Detect Empty Finance Stream (DetectEmptyStream) [converted]
    # Detect Empty Stream: Detect Empty Finance Stream
    _empty_flag_df_Detect_Empty_Finance_Stream = df_Dummy_Finance_Mapping_Output.limit(1).count() == 0
    # Pentaho semantics: if empty → one null row with input schema; else → empty DataFrame (no rows forwarded)
    if _empty_flag_df_Detect_Empty_Finance_Stream:
        _schema_df_Detect_Empty_Finance_Stream = df_Dummy_Finance_Mapping_Output.schema
        if len(df_Dummy_Finance_Mapping_Output.columns) == 0:
            df_Detect_Empty_Finance_Stream = spark.createDataFrame([], _schema_df_Detect_Empty_Finance_Stream)
        else:
            df_Detect_Empty_Finance_Stream = spark.createDataFrame([tuple(None for _ in df_Dummy_Finance_Mapping_Output.columns)], _schema_df_Detect_Empty_Finance_Stream)
    else:
        df_Detect_Empty_Finance_Stream = df_Dummy_Finance_Mapping_Output.limit(0)
    # Downstream hops receive this single output stream (empty-detection row or zero rows).

    # Step: Write Finance Staging Joined (TextFileOutput) [converted]
    # Pentaho step: Write Finance Staging Joined (type: TextFileOutput)
    # Pentaho filename: /output/finance/staging/stg_finance_joined_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='order_item_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='customer_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='employee_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='promotion_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='channel_mapped' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='currency_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quantity_sold' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='extended_price' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='discount_amount_calc' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_sales_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='tax_amount_calc' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipping_cost_calc' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='total_revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='converted_amount_usd' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue_usd' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='refund_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='inventory_cost' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unit_cost' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quantity_on_hand' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='exchange_rate' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sales_target_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='extract_checksum' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='extract_ts' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='extract_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Finance_Staging_Joined = df_Dummy_Finance_Mapping_Output
    _out_df_Write_Finance_Staging_Joined = df_Write_Finance_Staging_Joined.select('order_item_id', 'order_id', 'order_date', 'store_sk', 'product_sk', 'customer_sk', 'employee_sk', 'promotion_sk', 'channel_mapped', 'currency_code', 'quantity_sold', 'extended_price', 'discount_amount_calc', 'net_sales_amount', 'tax_amount_calc', 'shipping_cost_calc', 'total_revenue', 'profit', 'converted_amount_usd', 'revenue_usd', 'refund_amount', 'inventory_cost', 'unit_cost', 'quantity_on_hand', 'payment_amount', 'payment_status', 'exchange_rate', 'sales_target_amount', 'extract_checksum', 'batch_id', 'run_id', 'extract_ts', 'extract_status')
    writer = _out_df_Write_Finance_Staging_Joined.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_finance_joined_.csv')

    # Step: Abort Empty Finance Extract (Abort) [converted]
    # Abort: Abort Empty Finance Extract
    # preserved.row_threshold=0
    # preserved.message='Finance extract produced zero rows. RUN_ID=${RUN_ID}'
    # preserved.always_log_rows=True
    # preserved.row_threshold_raw='0'
    df_Abort_Empty_Finance_Extract = df_Detect_Empty_Finance_Stream
    print('Abort sample for', 'Abort Empty Finance Extract', df_Abort_Empty_Finance_Extract.limit(100).collect())  # always_log_rows
    _abort_count_df_Abort_Empty_Finance_Extract = df_Abort_Empty_Finance_Extract.count()
    if _abort_count_df_Abort_Empty_Finance_Extract > 0:  # Abort when any row reaches this step (threshold<=0)
        raise RuntimeError('Finance extract produced zero rows. RUN_ID=${RUN_ID}')

    # Step: Count Extracted Finance Rows (MemoryGroupBy) [partial]
    # Memory Group By: Count Extracted Finance Rows
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Extracted_Finance_Rows = df_Write_Finance_Staging_Joined.groupBy().agg(count(lit(1)).alias('rows_extracted'), _sum(col("revenue_usd")).alias('revenue_sum'))

    # Step: Tag Extraction Log (Constant) [converted]
    # Add Constants: Tag Extraction Log
    df_Tag_Extraction_Log = df_Count_Extracted_Finance_Rows
    df_Tag_Extraction_Log = df_Tag_Extraction_Log.withColumn("object_name", lit('stg_finance_joined'))
    # preserved.object_name: length='-1', precision='-1'
    df_Tag_Extraction_Log = df_Tag_Extraction_Log.withColumn("layer", lit('FINANCE_EXTRACT'))
    # preserved.layer: length='-1', precision='-1'
    df_Tag_Extraction_Log = df_Tag_Extraction_Log.withColumn("status", lit('SUCCESS'))
    # preserved.status: length='-1', precision='-1'
    df_Tag_Extraction_Log = df_Tag_Extraction_Log.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Tag_Extraction_Log = df_Tag_Extraction_Log.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'

    # Step: Write Extraction Log (TextFileOutput) [converted]
    # Pentaho step: Write Extraction Log (type: TextFileOutput)
    # Pentaho filename: /logs/execution/finance/TR_Finance_Extract_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='object_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='layer' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_extracted' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue_sum' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Extraction_Log = df_Tag_Extraction_Log
    _out_df_Write_Extraction_Log = df_Write_Extraction_Log.select('object_name', 'layer', 'rows_extracted', 'revenue_sum', 'status', 'batch_id', 'run_id')
    writer = _out_df_Write_Extraction_Log.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/TR_Finance_Extract_.log')

    # Step: Write Extraction Metrics JSON (JsonOutput) [converted]
    # Pentaho step: Write Extraction Metrics JSON (type: JsonOutput)
    df_Write_Extraction_Metrics_JSON = df_Tag_Extraction_Log
    df_Write_Extraction_Metrics_JSON.write \
    .mode('overwrite') \
    .json(
        f'{data_dir}/finance_extract_.json'
    )
    # preserved.json_bloc='rows' output_value='json_blob'

    # Step: Block Until Extract Landed (BlockingStep) [converted]
    # Blocking Step: Block Until Extract Landed
    # preserved.pass_all_rows=True
    # preserved.directory='%%java.io.tmpdir%%'
    # preserved.prefix='block'
    # preserved.cache_size='5000'
    # preserved.compress_files=False
    # LIMITATION: Pentaho temp-file spill (directory/prefix/compress) is replaced by Spark cache/persist.
    df_Block_Until_Extract_Landed = cache_for_reuse(df_Write_Extraction_Log)
    _ = df_Block_Until_Extract_Landed.count()  # synchronize: wait for all upstream rows

    # Step: Log Finance Extract Complete (WriteToLog) [converted]
    # Write to Log: Log Finance Extract Complete
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=EXTRACT_OK | TRANS=TR_Finance_Extract | RUN_ID=${RUN_ID} | ROWS_PROCESSED=OK'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Finance_Extract_Complete = logging.getLogger('pentaho.writetolog.Log_Finance_Extract_Complete')
    _log_df_Log_Finance_Extract_Complete.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Finance_Extract_Complete = df_Block_Until_Extract_Landed
    _log_rows_df_Log_Finance_Extract_Complete = _log_df_df_Log_Finance_Extract_Complete.take(20)
    _log_df_Log_Finance_Extract_Complete.info('Log Finance Extract Complete' + ' | columns=' + str(_log_df_df_Log_Finance_Extract_Complete.columns))
    _log_df_Log_Finance_Extract_Complete.info('AUDIT | EVENT=EXTRACT_OK | TRANS=TR_Finance_Extract | RUN_ID=${RUN_ID} | ROWS_PROCESSED=OK')
    for _lr in _log_rows_df_Log_Finance_Extract_Complete:
        _log_df_Log_Finance_Extract_Complete.info('Log Finance Extract Complete' + ' | ' + str(_lr.asDict()))
    df_Log_Finance_Extract_Complete = df_Block_Until_Extract_Landed

    # Step: Extract Complete (Dummy) [converted]
    # Dummy: Extract Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Extract_Complete = df_Log_Finance_Extract_Complete

    log_event(_LOG, "transformation_end")
    return df_Dummy_Extract_Complete
