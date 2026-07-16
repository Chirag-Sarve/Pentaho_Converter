"""PySpark module migrated from Pentaho transformation: TR_Customer_Extract.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/staging/TR_Customer_Extract.ktr
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_customer_extract")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Customer_Extract`` step-for-step."""
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

    # Step: Get Variables (GetVariable) [converted]
    # Get Variables: Get Variables
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'dataset_path', 'variable': '${DATASET_PATH}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'project_home', 'variable': '${PROJECT_HOME}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'current_date', 'variable': '${CURRENT_DATE}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
    # preserved.output_columns=['batch_id', 'run_id', 'dataset_path', 'project_home', 'current_date']
    import os
    import re as _re_var
    # Lookup order: Databricks widgets → os.environ → spark.conf (pentaho.var.*) → transformation parameters → empty string
    df_Get_Variables = spark.range(1).select(lit(1).alias('_row'))
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

    # Step: Read customers.csv (CsvInput) [converted]
    # CSV Input: Read customers.csv
    df_Read_customers.csv = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('customer_id STRING, first_name STRING, last_name STRING, email STRING, phone STRING, address_line1 STRING, address_line2 STRING, city STRING, state_province STRING, postal_code STRING, country_code STRING, country_name STRING, preferred_currency STRING, loyalty_tier STRING, registration_date STRING, date_of_birth STRING, is_active STRING')
        .load('/customers.csv')
    )

    # Step: Write Extract Rejects (TextFileOutput) [converted]
    # Pentaho step: Write Extract Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/customer/customers_extract_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='customer_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='first_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='last_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='email' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='phone' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='address_line1' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='address_line2' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='city' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='state_province' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='postal_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='country_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='country_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='preferred_currency' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='loyalty_tier' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='registration_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='date_of_birth' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_active' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_CODE' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_DESC' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_FIELDS' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Extract_Rejects = df_Write_Extract_Rejects
    _out_df_Write_Extract_Rejects = df_Write_Extract_Rejects.select('customer_id', 'first_name', 'last_name', 'email', 'phone', 'address_line1', 'address_line2', 'city', 'state_province', 'postal_code', 'country_code', 'country_name', 'preferred_currency', 'loyalty_tier', 'registration_date', 'date_of_birth', 'is_active', 'ERR_CODE', 'ERR_DESC', 'ERR_FIELDS', 'batch_id', 'run_id')
    writer = _out_df_Write_Extract_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/customers_extract_rejects_.csv')

    # Step: Validate File Exists (GetFileNames) [converted]
    # Get File Names: Validate File Exists
    _list_path_df_Validate_File_Exists = '/'
    try:
        _fs_entries_df_Validate_File_Exists = dbutils.fs.ls(_list_path_df_Validate_File_Exists)
        df_Validate_File_Exists = spark.createDataFrame(
            [(e.path, e.name, e.size, e.modificationTime) for e in _fs_entries_df_Validate_File_Exists],
            ['filename', 'short_filename', 'size', 'last_modified']
        )
    except Exception:
        # Fallback: Hadoop FileSystem listing via SparkContext
        _jpath = spark._jvm.org.apache.hadoop.fs.Path(_list_path_df_Validate_File_Exists)
        _fs = _jpath.getFileSystem(spark._jsc.hadoopConfiguration())
        _statuses = _fs.listStatus(_jpath)
        df_Validate_File_Exists = spark.createDataFrame(
            [(s.getPath().toString(), s.getPath().getName(), s.getLen(), s.getModificationTime())
             for s in _statuses if s.isFile()],
            ['filename', 'short_filename', 'size', 'last_modified']
        )

    # Step: Capture Extract Timestamp (SystemInfo) [converted]
    # System Info: Capture Extract Timestamp
    df_Capture_Extract_Timestamp = df_Read_customers.csv
    df_Capture_Extract_Timestamp = df_Capture_Extract_Timestamp.withColumn("extract_ts", current_date())
    df_Capture_Extract_Timestamp = df_Capture_Extract_Timestamp.withColumn("extract_start", current_date())

    # Step: File Found? (FilterRows) [failed]
    # Filter Rows: File Found?
    df_Log_File_Ready = df_Validate_File_Exists.filter((col("file_exists") == lit('Y')))
    df_Abort_Missing_File = df_Validate_File_Exists.filter(~((col("file_exists") == lit('Y'))))
    df_File_Found? = df_Log_File_Ready

    # Step: Tag Batch Metadata (Constant) [converted]
    # Add Constants: Tag Batch Metadata
    df_Tag_Batch_Metadata = df_Capture_Extract_Timestamp
    df_Tag_Batch_Metadata = df_Tag_Batch_Metadata.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Tag_Batch_Metadata = df_Tag_Batch_Metadata.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'
    df_Tag_Batch_Metadata = df_Tag_Batch_Metadata.withColumn("etl_layer", lit('EXTRACT'))
    # preserved.etl_layer: length='-1', precision='-1'

    # Step: Abort Missing File (Abort) [converted]
    # Abort: Abort Missing File
    # preserved.row_threshold=0
    # preserved.message='customers.csv not found under ${DATASET_PATH}. RUN_ID=${RUN_ID}'
    # preserved.always_log_rows=True
    # preserved.row_threshold_raw='0'
    # Abort operates on its own failure/branch stream df_Abort_Missing_File (already assigned by upstream Filter/Switch; not overwritten)
    print('Abort sample for', 'Abort Missing File', df_Abort_Missing_File.limit(100).collect())  # always_log_rows
    _abort_count_df_Abort_Missing_File = df_Abort_Missing_File.count()
    if _abort_count_df_Abort_Missing_File > 0:  # Abort when any row reaches this step (threshold<=0)
        raise RuntimeError('customers.csv not found under ${DATASET_PATH}. RUN_ID=${RUN_ID}')

    # Step: Log File Ready (WriteToLog) [failed]
    # Write to Log: Log File Ready
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=FILE_OK | TRANS=TR_Customer_Extract | FILE=${DATASET_PATH}/customers.csv | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_File_Ready = logging.getLogger('pentaho.writetolog.Log_File_Ready')
    _log_df_Log_File_Ready.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_File_Ready = df_File_Found?
    _log_rows_df_Log_File_Ready = _log_df_df_Log_File_Ready.take(20)
    _log_df_Log_File_Ready.info('Log File Ready' + ' | columns=' + str(_log_df_df_Log_File_Ready.columns))
    _log_df_Log_File_Ready.info('AUDIT | EVENT=FILE_OK | TRANS=TR_Customer_Extract | FILE=${DATASET_PATH}/customers.csv | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_File_Ready:
        _log_df_Log_File_Ready.info('Log File Ready' + ' | ' + str(_lr.asDict()))
    df_Log_File_Ready = df_File_Found?

    # Step: Select Extract Columns (SelectValues) [converted]
    # Select Values: Select Extract Columns
    df_Select_Extract_Columns = df_Tag_Batch_Metadata.select(col("customer_id").alias("customer_id"), col("first_name").alias("first_name"), col("last_name").alias("last_name"), col("email").alias("email"), col("phone").alias("phone"), col("address_line1").alias("address_line1"), col("address_line2").alias("address_line2"), col("city").alias("city"), col("state_province").alias("state_province"), col("postal_code").alias("postal_code"), col("country_code").alias("country_code"), col("country_name").alias("country_name"), col("preferred_currency").alias("preferred_currency"), col("loyalty_tier").alias("loyalty_tier"), col("registration_date").alias("registration_date"), col("date_of_birth").alias("date_of_birth"), col("is_active").alias("is_active"), col("source_row_num").alias("source_row_num"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"), col("etl_layer").alias("etl_layer"), col("extract_ts").alias("extract_ts"))

    # Step: Add Extract Constants (Constant) [converted]
    # Add Constants: Add Extract Constants
    df_Add_Extract_Constants = df_Log_File_Ready
    df_Add_Extract_Constants = df_Add_Extract_Constants.withColumn("etl_layer", lit('EXTRACT'))
    # preserved.etl_layer: length='-1', precision='-1'
    df_Add_Extract_Constants = df_Add_Extract_Constants.withColumn("source_entity", lit('customers'))
    # preserved.source_entity: length='-1', precision='-1'
    df_Add_Extract_Constants = df_Add_Extract_Constants.withColumn("source_file_name", lit('customers.csv'))
    # preserved.source_file_name: length='-1', precision='-1'

    # Step: Count Extracted Rows (MemoryGroupBy) [converted]
    # Memory Group By: Count Extracted Rows
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Extracted_Rows = df_Select_Extract_Columns.groupBy().agg(count(lit(1)).alias('rows_extracted'))

    # Step: Log Row Extracted (WriteToLog) [converted]
    # Write to Log: Log Row Extracted
    # preserved.log_level='Basic'
    # preserved.log_message='EXTRACT | customer_id=${customer_id} | row=${source_row_num} | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Row_Extracted = logging.getLogger('pentaho.writetolog.Log_Row_Extracted')
    _log_df_Log_Row_Extracted.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Row_Extracted = df_Select_Extract_Columns
    _log_rows_df_Log_Row_Extracted = _log_df_df_Log_Row_Extracted.take(20)
    _log_df_Log_Row_Extracted.info('Log Row Extracted' + ' | columns=' + str(_log_df_df_Log_Row_Extracted.columns))
    _log_df_Log_Row_Extracted.info('EXTRACT | customer_id=${customer_id} | row=${source_row_num} | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Row_Extracted:
        _log_df_Log_Row_Extracted.info('Log Row Extracted' + ' | ' + str(_lr.asDict()))
    df_Log_Row_Extracted = df_Select_Extract_Columns

    # Step: Write Staging Land File (TextFileOutput) [converted]
    # Pentaho step: Write Staging Land File (type: TextFileOutput)
    # Pentaho filename: /output/customer/staging/stg_raw_customers_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='customer_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='first_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='last_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='email' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='phone' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='address_line1' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='address_line2' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='city' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='state_province' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='postal_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='country_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='country_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='preferred_currency' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='loyalty_tier' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='registration_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='date_of_birth' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_active' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='source_row_num' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='etl_layer' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='extract_ts' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Staging_Land_File = df_Select_Extract_Columns
    _out_df_Write_Staging_Land_File = df_Write_Staging_Land_File.select('customer_id', 'first_name', 'last_name', 'email', 'phone', 'address_line1', 'address_line2', 'city', 'state_province', 'postal_code', 'country_code', 'country_name', 'preferred_currency', 'loyalty_tier', 'registration_date', 'date_of_birth', 'is_active', 'source_row_num', 'batch_id', 'run_id', 'etl_layer', 'extract_ts')
    writer = _out_df_Write_Staging_Land_File.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_raw_customers_.csv')

    # Step: Add Extract Audit Fields (Constant) [converted]
    # Add Constants: Add Extract Audit Fields
    df_Add_Extract_Audit_Fields = df_Count_Extracted_Rows
    df_Add_Extract_Audit_Fields = df_Add_Extract_Audit_Fields.withColumn("object_name", lit('customers'))
    # preserved.object_name: length='-1', precision='-1'
    df_Add_Extract_Audit_Fields = df_Add_Extract_Audit_Fields.withColumn("layer", lit('EXTRACT'))
    # preserved.layer: length='-1', precision='-1'
    df_Add_Extract_Audit_Fields = df_Add_Extract_Audit_Fields.withColumn("status", lit('SUCCESS'))
    # preserved.status: length='-1', precision='-1'
    df_Add_Extract_Audit_Fields = df_Add_Extract_Audit_Fields.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Add_Extract_Audit_Fields = df_Add_Extract_Audit_Fields.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'

    # Step: Write Staging Table (TableOutput) [converted]
    # Pentaho step: Write Staging Table (type: TableOutput) (Pentaho schema: retail_stg)
    # Mode: append (Pentaho truncate=N)
    _mapped_df_Write_Staging_Table = df_Write_Staging_Land_File.select(col('customer_id'), col('first_name'), col('last_name'), col('email'), col('phone'), col('address_line1'), col('address_line2'), col('city'), col('state_province'), col('postal_code'), col('country_code'), col('country_name'), col('preferred_currency'), col('loyalty_tier'), col('registration_date'), col('date_of_birth'), col('is_active'), col('source_row_num'), col('batch_id'), col('run_id'), col('etl_layer'))
    df_Write_Staging_Table = _mapped_df_Write_Staging_Table
    write_delta(
        df_Write_Staging_Table,
        f"{catalog}.{schema}.stg_raw_customers",
        mode='append',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.stg_raw_customers", mode='append')

    # Step: Write Extraction Log (TextFileOutput) [converted]
    # Pentaho step: Write Extraction Log (type: TextFileOutput)
    # Pentaho filename: /logs/execution/customer/TR_Customer_Extract_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='object_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='layer' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_extracted' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Extraction_Log = df_Add_Extract_Audit_Fields
    _out_df_Write_Extraction_Log = df_Write_Extraction_Log.select('object_name', 'layer', 'rows_extracted', 'status', 'batch_id', 'run_id')
    writer = _out_df_Write_Extraction_Log.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/TR_Customer_Extract_.log')

    # Step: Block Until Extract Complete (BlockingStep) [converted]
    # Blocking Step: Block Until Extract Complete
    # preserved.pass_all_rows=True
    # preserved.directory='%%java.io.tmpdir%%'
    # preserved.prefix='block'
    # preserved.cache_size='5000'
    # preserved.compress_files=False
    # LIMITATION: Pentaho temp-file spill (directory/prefix/compress) is replaced by Spark cache/persist.
    df_Block_Until_Extract_Complete = cache_for_reuse(df_Write_Staging_Table)
    _ = df_Block_Until_Extract_Complete.count()  # synchronize: wait for all upstream rows

    # Step: Write Extraction Audit JSON (JsonOutput) [converted]
    # Pentaho step: Write Extraction Audit JSON (type: JsonOutput)
    df_Write_Extraction_Audit_JSON = df_Write_Extraction_Log
    df_Write_Extraction_Audit_JSON.write \
    .mode('overwrite') \
    .json(
        f'{data_dir}/customer_extract_.json'
    )
    # preserved.json_bloc='rows' output_value='json_blob'

    # Step: Extract Complete (Dummy) [converted]
    # Dummy: Extract Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Extract_Complete = df_Block_Until_Extract_Complete

    # Step: Copy Extract Metrics To Result (RowsToResult) [converted]
    # Copy Rows to Result: Copy Extract Metrics To Result
    # preserved.result_buffer='rows'
    # preserved.preserve_order=True
    # LIMITATION: Pentaho Result rows are job-level; Databricks uses a notebook-scoped buffer (_pentaho_result_rows) for downstream hops / orchestration. Cross-job Result transfer needs Databricks Jobs task values or persisted Delta tables.
    _pentaho_result_rows = globals().setdefault('_pentaho_result_rows', {})
    _pentaho_result_files = globals().setdefault('_pentaho_result_files', [])
    # Preserve schema and relative ordering for 'Copy Extract Metrics To Result'
    _result_rows_df_Copy_Extract_Metrics_To_Result = df_Write_Extraction_Audit_JSON
    _pentaho_result_rows['Copy Extract Metrics To Result'] = _result_rows_df_Copy_Extract_Metrics_To_Result
    _pentaho_result_rows['__latest__'] = _result_rows_df_Copy_Extract_Metrics_To_Result
    df_Copy_Extract_Metrics_To_Result = df_Write_Extraction_Audit_JSON

    log_event(_LOG, "transformation_end")
    return df_Copy_Extract_Metrics_To_Result
