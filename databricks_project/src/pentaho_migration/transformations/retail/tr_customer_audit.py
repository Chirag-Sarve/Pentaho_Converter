"""PySpark module migrated from Pentaho transformation: TR_Customer_Audit.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/utilities/TR_Customer_Audit.ktr
Independent module — ``run(spark, config)`` returns a DataFrame.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_date,
    current_timestamp,
    length,
    lit,
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_customer_audit")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Customer_Audit`` step-for-step."""
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

    # Step: Get Audit Variables (GetVariable) [converted]
    # Get Variables: Get Audit Variables
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'project_home', 'variable': '${PROJECT_HOME}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'current_date', 'variable': '${CURRENT_DATE}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'module_name', 'variable': 'Load_Customer_Data', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
    # preserved.output_columns=['batch_id', 'run_id', 'project_home', 'current_date', 'module_name']
    import os
    import re as _re_var
    # Lookup order: Databricks widgets → os.environ → spark.conf (pentaho.var.*) → transformation parameters → empty string
    df_Get_Audit_Variables = spark.range(1).select(lit(1).alias('_row'))
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
    df_Get_Audit_Variables = df_Get_Audit_Variables.withColumn('batch_id', lit(_batch_id_resolved))
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
    df_Get_Audit_Variables = df_Get_Audit_Variables.withColumn('run_id', lit(_run_id_resolved))
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
    df_Get_Audit_Variables = df_Get_Audit_Variables.withColumn('project_home', lit(_project_home_resolved))
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
    df_Get_Audit_Variables = df_Get_Audit_Variables.withColumn('current_date', lit(_current_date_resolved))
    # field 'module_name' from variable string 'Load_Customer_Data'
    # preserved.field.module_name.trim_type='none'
    # preserved.field.module_name.type='String'
    _module_name_resolved = None
    _dbu__module_name_resolved = globals().get('dbutils')
    if _dbu__module_name_resolved is not None and hasattr(_dbu__module_name_resolved, 'widgets'):
        try:
            _module_name_resolved = _dbu__module_name_resolved.widgets.get('Load_Customer_Data')
        except Exception:
            _module_name_resolved = None
    if _module_name_resolved in (None, ''):
        import os as _os__module_name_resolved
        _module_name_resolved = _os__module_name_resolved.environ.get('Load_Customer_Data')
    if _module_name_resolved in (None, ''):
        try:
            _module_name_resolved = spark.conf.get('pentaho.var.Load_Customer_Data')
        except Exception:
            _module_name_resolved = None
    if _module_name_resolved in (None, ''):
        _module_name_resolved = ''
    if _module_name_resolved is None:
        _module_name_resolved = ''
    df_Get_Audit_Variables = df_Get_Audit_Variables.withColumn('module_name', lit(_module_name_resolved))

    # Step: Read Dimension Stats (CsvInput) [converted]
    # CSV Input: Read Dimension Stats
    df_Read_Dimension_Stats = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('scd_action STRING, action_count INT')
        .load(f'{data_dir}/customer_dimension_stats_.csv')
    )

    # Step: Read Extract Metrics (CsvInput) [converted]
    # CSV Input: Read Extract Metrics
    df_Read_Extract_Metrics = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('object_name STRING, layer STRING, rows_extracted INT, status STRING, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/TR_Customer_Extract_.log')
    )

    # Step: Read Reject File Counts (CsvInput) [converted]
    # CSV Input: Read Reject File Counts
    df_Read_Reject_File_Counts = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('customer_id STRING, _reject_code STRING')
        .load(f'{data_dir}/customers_rejects_.csv')
    )

    # Step: Read Validation Metrics (CsvInput) [converted]
    # CSV Input: Read Validation Metrics
    df_Read_Validation_Metrics = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('validation_status STRING, row_count INT')
        .load(f'{data_dir}/TR_Customer_Validation_.log')
    )

    # Step: Write Audit Rejects (TextFileOutput) [converted]
    # Pentaho step: Write Audit Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/exception_reports/customer_audit_errors_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='ERR_CODE' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_DESC' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Audit_Rejects = df_Write_Audit_Rejects
    _out_df_Write_Audit_Rejects = df_Write_Audit_Rejects.select('ERR_CODE', 'ERR_DESC', 'batch_id', 'run_id')
    writer = _out_df_Write_Audit_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/customer_audit_errors_.csv')

    # Step: Capture Audit Timestamps (SystemInfo) [converted]
    # System Info: Capture Audit Timestamps
    df_Capture_Audit_Timestamps = df_Get_Audit_Variables
    df_Capture_Audit_Timestamps = df_Capture_Audit_Timestamps.withColumn("audit_start_ts", current_date())
    df_Capture_Audit_Timestamps = df_Capture_Audit_Timestamps.withColumn("audit_end_ts", current_date())
    df_Capture_Audit_Timestamps = df_Capture_Audit_Timestamps.withColumn("execution_time_ms", current_timestamp())

    # Step: Sum Dimension Actions (MemoryGroupBy) [converted]
    # Memory Group By: Sum Dimension Actions
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Sum_Dimension_Actions = df_Read_Dimension_Stats.groupBy().agg(_sum(col("action_count")).alias('rows_inserted'), _sum(col("action_count")).alias('rows_updated'))

    # Step: Count Rejects (MemoryGroupBy) [converted]
    # Memory Group By: Count Rejects
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Rejects = df_Read_Reject_File_Counts.groupBy().agg(count(lit(1)).alias('rows_rejected'))

    # Step: Seed Audit Record (Constant) [converted]
    # Add Constants: Seed Audit Record
    df_Seed_Audit_Record = df_Capture_Audit_Timestamps
    df_Seed_Audit_Record = df_Seed_Audit_Record.withColumn("object_name", lit('dim_customer'))
    # preserved.object_name: length='-1', precision='-1'
    df_Seed_Audit_Record = df_Seed_Audit_Record.withColumn("layer", lit('CUSTOMER_ETL'))
    # preserved.layer: length='-1', precision='-1'
    df_Seed_Audit_Record = df_Seed_Audit_Record.withColumn("rows_read", lit('0'))
    # preserved.rows_read: length='-1', precision='-1'
    df_Seed_Audit_Record = df_Seed_Audit_Record.withColumn("rows_rejected", lit('0'))
    # preserved.rows_rejected: length='-1', precision='-1'
    df_Seed_Audit_Record = df_Seed_Audit_Record.withColumn("rows_inserted", lit('0'))
    # preserved.rows_inserted: length='-1', precision='-1'
    df_Seed_Audit_Record = df_Seed_Audit_Record.withColumn("rows_updated", lit('0'))
    # preserved.rows_updated: length='-1', precision='-1'
    df_Seed_Audit_Record = df_Seed_Audit_Record.withColumn("error_count", lit('0'))
    # preserved.error_count: length='-1', precision='-1'
    df_Seed_Audit_Record = df_Seed_Audit_Record.withColumn("status", lit('SUCCESS'))
    # preserved.status: length='-1', precision='-1'
    df_Seed_Audit_Record = df_Seed_Audit_Record.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Seed_Audit_Record = df_Seed_Audit_Record.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'

    # Step: Combine Audit Streams (Dummy) [converted]
    # Dummy: Combine Audit Streams
    # Pass-through step - DataFrame unchanged
    df_Dummy_Combine_Audit_Streams = df_Read_Extract_Metrics

    # Step: Compute Audit Totals (Calculator) [converted]
    # Calculator: Compute Audit Totals
    df_Compute_Audit_Totals = df_Dummy_Combine_Audit_Streams
    df_Compute_Audit_Totals = df_Compute_Audit_Totals.withColumn("rows_read_num", (lit(0)).cast('int'))

    # Step: Finalize Audit Metrics (Formula) [converted]
    # Formula: Finalize Audit Metrics
    df_Finalize_Audit_Metrics = df_Compute_Audit_Totals
    df_Finalize_Audit_Metrics = df_Finalize_Audit_Metrics.withColumn('formula_result', lit(None))  # empty formula

    # Step: Select Audit Columns (SelectValues) [converted]
    # Select Values: Select Audit Columns
    df_Select_Audit_Columns = df_Finalize_Audit_Metrics.select(col("object_name").alias("object_name"), col("layer").alias("layer"), col("rows_read").alias("rows_read"), col("rows_rejected").alias("rows_rejected"), col("rows_inserted").alias("rows_inserted"), col("rows_updated").alias("rows_updated"), col("execution_time").alias("execution_time"), col("error_count").alias("error_count"), col("status").alias("status"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"), col("audit_end_ts").alias("end_ts"))

    # Step: Write Customer Audit File (TextFileOutput) [converted]
    # Pentaho step: Write Customer Audit File (type: TextFileOutput)
    # Pentaho filename: /audit/load_audit/Customer_Audit_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='object_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='layer' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_read' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_rejected' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_inserted' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_updated' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='execution_time' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='error_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='end_ts' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Customer_Audit_File = df_Select_Audit_Columns
    _out_df_Write_Customer_Audit_File = df_Write_Customer_Audit_File.select('object_name', 'layer', 'rows_read', 'rows_rejected', 'rows_inserted', 'rows_updated', 'execution_time', 'error_count', 'status', 'batch_id', 'run_id', 'end_ts')
    writer = _out_df_Write_Customer_Audit_File.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/Customer_Audit_.csv')

    # Step: Write Customer Audit Table (TableOutput) [converted]
    # Pentaho step: Write Customer Audit Table (type: TableOutput) (Pentaho schema: retail_audit)
    # Mode: append (Pentaho truncate=N)
    _mapped_df_Write_Customer_Audit_Table = df_Select_Audit_Columns.select(col('object_name'), col('layer'), col('rows_read'), col('rows_rejected'), col('rows_inserted'), col('rows_updated'), col('execution_time'), col('error_count'), col('status'), col('batch_id'), col('run_id'), col('end_ts'))
    df_Write_Customer_Audit_Table = _mapped_df_Write_Customer_Audit_Table
    write_delta(
        df_Write_Customer_Audit_Table,
        f"{catalog}.{schema}.audit_customer_load",
        mode='append',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.audit_customer_load", mode='append')

    # Step: Write Execution Log (TextFileOutput) [converted]
    # Pentaho step: Write Execution Log (type: TextFileOutput)
    # Pentaho filename: /logs/execution/customer/TR_Customer_Audit_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='object_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='layer' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_read' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_rejected' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_inserted' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_updated' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='execution_time' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='error_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Execution_Log = df_Select_Audit_Columns
    _out_df_Write_Execution_Log = df_Write_Execution_Log.select('object_name', 'layer', 'rows_read', 'rows_rejected', 'rows_inserted', 'rows_updated', 'execution_time', 'error_count', 'status', 'run_id')
    writer = _out_df_Write_Execution_Log.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/TR_Customer_Audit_.log')

    # Step: Write Audit JSON (JsonOutput) [converted]
    # Pentaho step: Write Audit JSON (type: JsonOutput)
    df_Write_Audit_JSON = df_Write_Customer_Audit_File
    df_Write_Audit_JSON.write \
    .mode('overwrite') \
    .json(
        f'{data_dir}/Customer_Audit_.json'
    )
    # preserved.json_bloc='rows' output_value='json_blob'

    # Step: Log Audit Summary (WriteToLog) [converted]
    # Write to Log: Log Audit Summary
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | TRANS=TR_Customer_Audit | RUN_ID=${RUN_ID} | READ=${rows_read} | REJECT=${rows_rejected} | INSERT=${rows_inserted} | UPDATE=${rows_updated} | ERRORS=${error_count} | TIME=${execution_time}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Audit_Summary = logging.getLogger('pentaho.writetolog.Log_Audit_Summary')
    _log_df_Log_Audit_Summary.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Audit_Summary = df_Write_Execution_Log
    _log_rows_df_Log_Audit_Summary = _log_df_df_Log_Audit_Summary.take(20)
    _log_df_Log_Audit_Summary.info('Log Audit Summary' + ' | columns=' + str(_log_df_df_Log_Audit_Summary.columns))
    _log_df_Log_Audit_Summary.info('AUDIT | TRANS=TR_Customer_Audit | RUN_ID=${RUN_ID} | READ=${rows_read} | REJECT=${rows_rejected} | INSERT=${rows_inserted} | UPDATE=${rows_updated} | ERRORS=${error_count} | TIME=${execution_time}')
    for _lr in _log_rows_df_Log_Audit_Summary:
        _log_df_Log_Audit_Summary.info('Log Audit Summary' + ' | ' + str(_lr.asDict()))
    df_Log_Audit_Summary = df_Write_Execution_Log

    # Step: Block Audit Complete (BlockingStep) [converted]
    # Blocking Step: Block Audit Complete
    # preserved.pass_all_rows=True
    # preserved.directory='%%java.io.tmpdir%%'
    # preserved.prefix='block'
    # preserved.cache_size='5000'
    # preserved.compress_files=False
    # LIMITATION: Pentaho temp-file spill (directory/prefix/compress) is replaced by Spark cache/persist.
    df_Block_Audit_Complete = cache_for_reuse(df_Write_Customer_Audit_Table)
    _ = df_Block_Audit_Complete.count()  # synchronize: wait for all upstream rows

    # Step: Copy Audit To Result (RowsToResult) [converted]
    # Copy Rows to Result: Copy Audit To Result
    # preserved.result_buffer='rows'
    # preserved.preserve_order=True
    # LIMITATION: Pentaho Result rows are job-level; Databricks uses a notebook-scoped buffer (_pentaho_result_rows) for downstream hops / orchestration. Cross-job Result transfer needs Databricks Jobs task values or persisted Delta tables.
    _pentaho_result_rows = globals().setdefault('_pentaho_result_rows', {})
    _pentaho_result_files = globals().setdefault('_pentaho_result_files', [])
    # Preserve schema and relative ordering for 'Copy Audit To Result'
    _result_rows_df_Copy_Audit_To_Result = df_Block_Audit_Complete
    _pentaho_result_rows['Copy Audit To Result'] = _result_rows_df_Copy_Audit_To_Result
    _pentaho_result_rows['__latest__'] = _result_rows_df_Copy_Audit_To_Result
    df_Copy_Audit_To_Result = df_Block_Audit_Complete

    # Step: Audit Complete (Dummy) [converted]
    # Dummy: Audit Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Audit_Complete = df_Copy_Audit_To_Result

    log_event(_LOG, "transformation_end")
    return df_Dummy_Audit_Complete
