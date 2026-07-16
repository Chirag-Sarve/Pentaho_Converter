"""PySpark module migrated from Pentaho transformation: TR_Product_Audit.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/utilities/TR_Product_Audit.ktr
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_product_audit")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Product_Audit`` step-for-step."""
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

    # Step: Generate Audit Seed (RowGenerator) [converted]
    # Generate Rows: Generate Audit Seed
    data = [('dim_product', 'PRODUCT_ETL', 'SUCCESS')]
    df_Generate_Audit_Seed = spark.createDataFrame(data, ['object_name', 'layer', 'status'])

    # Step: Read Dimension Stats (CsvInput) [converted]
    # CSV Input: Read Dimension Stats
    df_Read_Dimension_Stats = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('scd_action STRING, action_count INT')
        .load(f'{data_dir}/product_dimension_stats_.csv')
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
        .load(f'{data_dir}/TR_Product_Extract_.log')
    )

    # Step: Read Reject File (CsvInput) [converted]
    # CSV Input: Read Reject File
    df_Read_Reject_File = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('product_id STRING, reject_reason STRING')
        .load(f'{data_dir}/products_rejects_.csv')
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
        .load(f'{data_dir}/TR_Product_Validation_.log')
    )

    # Step: Get Audit Variables (GetVariable) [converted]
    # Get Variables: Get Audit Variables
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'project_home', 'variable': '${PROJECT_HOME}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'current_date', 'variable': '${CURRENT_DATE}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'module_name', 'variable': 'Load_Product_Data', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
    # preserved.output_columns=['batch_id', 'run_id', 'project_home', 'current_date', 'module_name']
    import os
    import re as _re_var
    # Lookup order: Databricks widgets → os.environ → spark.conf (pentaho.var.*) → transformation parameters → empty string
    df_Get_Audit_Variables = df_Generate_Audit_Seed
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
    # field 'module_name' from variable string 'Load_Product_Data'
    # preserved.field.module_name.trim_type='none'
    # preserved.field.module_name.type='String'
    _module_name_resolved = None
    _dbu__module_name_resolved = globals().get('dbutils')
    if _dbu__module_name_resolved is not None and hasattr(_dbu__module_name_resolved, 'widgets'):
        try:
            _module_name_resolved = _dbu__module_name_resolved.widgets.get('Load_Product_Data')
        except Exception:
            _module_name_resolved = None
    if _module_name_resolved in (None, ''):
        import os as _os__module_name_resolved
        _module_name_resolved = _os__module_name_resolved.environ.get('Load_Product_Data')
    if _module_name_resolved in (None, ''):
        try:
            _module_name_resolved = spark.conf.get('pentaho.var.Load_Product_Data')
        except Exception:
            _module_name_resolved = None
    if _module_name_resolved in (None, ''):
        _module_name_resolved = ''
    if _module_name_resolved is None:
        _module_name_resolved = ''
    df_Get_Audit_Variables = df_Get_Audit_Variables.withColumn('module_name', lit(_module_name_resolved))

    # Step: Inserted Actions? (FilterRows) [failed]
    # Filter Rows: Inserted Actions?
    df_Sum_Inserts = df_Read_Dimension_Stats.filter((col("scd_action") == lit('INSERT')))
    df_Non_Insert_Stats = df_Read_Dimension_Stats.filter(~((col("scd_action") == lit('INSERT'))))
    df_Inserted_Actions? = df_Sum_Inserts

    # Step: Sum All Dimension Actions (MemoryGroupBy) [converted]
    # Memory Group By: Sum All Dimension Actions
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Sum_All_Dimension_Actions = df_Read_Dimension_Stats.groupBy().agg(_sum(col("action_count")).alias('rows_inserted_all'), _sum(col("action_count")).alias('rows_updated_all'))

    # Step: Updated Actions? (FilterRows) [failed]
    # Filter Rows: Updated Actions?
    df_Sum_Updates = df_Read_Dimension_Stats.filter((col("scd_action") == lit('UPDATE')))
    df_Skip_Non_Updates = df_Read_Dimension_Stats.filter(~((col("scd_action") == lit('UPDATE'))))
    df_Updated_Actions? = df_Sum_Updates

    # Step: Count Rejects (MemoryGroupBy) [converted]
    # Memory Group By: Count Rejects
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Rejects = df_Read_Reject_File.groupBy().agg(count(lit(1)).alias('rows_rejected'))

    # Step: Capture Audit Timestamps (SystemInfo) [converted]
    # System Info: Capture Audit Timestamps
    df_Capture_Audit_Timestamps = df_Get_Audit_Variables
    df_Capture_Audit_Timestamps = df_Capture_Audit_Timestamps.withColumn("audit_start_ts", current_date())
    df_Capture_Audit_Timestamps = df_Capture_Audit_Timestamps.withColumn("audit_end_ts", current_date())
    df_Capture_Audit_Timestamps = df_Capture_Audit_Timestamps.withColumn("execution_time_ms", current_timestamp())

    # Step: Non Insert Stats (Dummy) [converted]
    # Dummy: Non Insert Stats
    # Pass-through step - DataFrame unchanged
    df_Dummy_Non_Insert_Stats = df_Non_Insert_Stats

    # Step: Sum Inserts (MemoryGroupBy) [failed]
    # Memory Group By: Sum Inserts
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Sum_Inserts = df_Inserted_Actions?.groupBy().agg(_sum(col("action_count")).alias('rows_inserted'))

    # Step: Skip Non Updates (Dummy) [converted]
    # Dummy: Skip Non Updates
    # Pass-through step - DataFrame unchanged
    df_Dummy_Skip_Non_Updates = df_Skip_Non_Updates

    # Step: Sum Updates (MemoryGroupBy) [failed]
    # Memory Group By: Sum Updates
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Sum_Updates = df_Updated_Actions?.groupBy().agg(_sum(col("action_count")).alias('rows_updated'))

    # Step: Seed Audit Record (Constant) [converted]
    # Add Constants: Seed Audit Record
    df_Seed_Audit_Record = df_Capture_Audit_Timestamps
    df_Seed_Audit_Record = df_Seed_Audit_Record.withColumn("object_name", lit('dim_product'))
    # preserved.object_name: length='-1', precision='-1'
    df_Seed_Audit_Record = df_Seed_Audit_Record.withColumn("layer", lit('PRODUCT_ETL'))
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

    # Step: Finalize Audit Metrics (Formula) [converted]
    # Formula: Finalize Audit Metrics
    df_Finalize_Audit_Metrics = df_Dummy_Combine_Audit_Streams
    df_Finalize_Audit_Metrics = df_Finalize_Audit_Metrics.withColumn('formula_result', lit(None))  # empty formula

    # Step: Audit Checksum Fingerprint (CheckSum) [converted]
    # Add a Checksum: Audit Checksum Fingerprint
    df_Audit_Checksum_Fingerprint = df_Finalize_Audit_Metrics
    df_Audit_Checksum_Fingerprint = df_Audit_Checksum_Fingerprint.withColumn("audit_checksum", md5(concat(coalesce(col("run_id").cast("string"), lit("")), coalesce(col("batch_id").cast("string"), lit("")), coalesce(col("object_name").cast("string"), lit("")))))
    # preserved.checksumtype='MD5' resultType='hexadecimal' fields=['run_id', 'batch_id', 'object_name']

    # Step: Write Audit Table (TableOutput) [converted]
    # Pentaho step: Write Audit Table (type: TableOutput) (Pentaho schema: retail_audit)
    # Mode: append (Pentaho truncate=N)
    _mapped_df_Write_Audit_Table = df_Audit_Checksum_Fingerprint.select(col('object_name'), col('layer'), col('rows_read'), col('rows_rejected'), col('rows_inserted'), col('rows_updated'), col('execution_time'), col('error_count'), col('status'), col('batch_id'), col('run_id'), col('audit_start_ts'), col('audit_end_ts'), col('audit_checksum'))
    df_Write_Audit_Table = _mapped_df_Write_Audit_Table
    write_delta(
        df_Write_Audit_Table,
        f"{catalog}.{schema}.etl_audit_product",
        mode='append',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.etl_audit_product", mode='append')

    # Step: Write Audit CSV (TextFileOutput) [converted]
    # Pentaho step: Write Audit CSV (type: TextFileOutput)
    # Pentaho filename: /audit/load_audit/product_audit_
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
    # INFO: preserved.field_format name='audit_start_ts' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='audit_end_ts' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='audit_checksum' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Audit_CSV = df_Write_Audit_Table
    _out_df_Write_Audit_CSV = df_Write_Audit_CSV.select('object_name', 'layer', 'rows_read', 'rows_rejected', 'rows_inserted', 'rows_updated', 'execution_time', 'error_count', 'status', 'batch_id', 'run_id', 'audit_start_ts', 'audit_end_ts', 'audit_checksum')
    writer = _out_df_Write_Audit_CSV.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/product_audit_.csv')

    # Step: Write Execution Log (TextFileOutput) [converted]
    # Pentaho step: Write Execution Log (type: TextFileOutput)
    # Pentaho filename: /logs/execution/product/TR_Product_Audit_
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
    # INFO: preserved.field_format name='audit_start_ts' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='audit_end_ts' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='audit_checksum' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Execution_Log = df_Write_Audit_CSV
    _out_df_Write_Execution_Log = df_Write_Execution_Log.select('object_name', 'layer', 'rows_read', 'rows_rejected', 'rows_inserted', 'rows_updated', 'execution_time', 'error_count', 'status', 'batch_id', 'run_id', 'audit_start_ts', 'audit_end_ts', 'audit_checksum')
    writer = _out_df_Write_Execution_Log.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/TR_Product_Audit_.log')

    # Step: Write Execution Audit JSON (JsonOutput) [converted]
    # Pentaho step: Write Execution Audit JSON (type: JsonOutput)
    df_Write_Execution_Audit_JSON = df_Write_Execution_Log
    df_Write_Execution_Audit_JSON.write \
    .mode('overwrite') \
    .json(
        f'{data_dir}/product_audit__exec.json'
    )
    # preserved.json_bloc='rows' output_value='json_blob'

    # Step: Log Audit Complete (WriteToLog) [converted]
    # Write to Log: Log Audit Complete
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=AUDIT_COMPLETE | TRANS=TR_Product_Audit | ROWS_READ=${rows_read} | ROWS_REJECTED=${rows_rejected} | ROWS_INSERTED=${rows_inserted} | ROWS_UPDATED=${rows_updated} | EXEC_MS=${execution_time} | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Audit_Complete = logging.getLogger('pentaho.writetolog.Log_Audit_Complete')
    _log_df_Log_Audit_Complete.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Audit_Complete = df_Write_Execution_Audit_JSON
    _log_rows_df_Log_Audit_Complete = _log_df_df_Log_Audit_Complete.take(20)
    _log_df_Log_Audit_Complete.info('Log Audit Complete' + ' | columns=' + str(_log_df_df_Log_Audit_Complete.columns))
    _log_df_Log_Audit_Complete.info('AUDIT | EVENT=AUDIT_COMPLETE | TRANS=TR_Product_Audit | ROWS_READ=${rows_read} | ROWS_REJECTED=${rows_rejected} | ROWS_INSERTED=${rows_inserted} | ROWS_UPDATED=${rows_updated} | EXEC_MS=${execution_time} | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Audit_Complete:
        _log_df_Log_Audit_Complete.info('Log Audit Complete' + ' | ' + str(_lr.asDict()))
    df_Log_Audit_Complete = df_Write_Execution_Audit_JSON

    # Step: Copy Audit Metrics To Result (RowsToResult) [converted]
    # Copy Rows to Result: Copy Audit Metrics To Result
    # preserved.result_buffer='rows'
    # preserved.preserve_order=True
    # LIMITATION: Pentaho Result rows are job-level; Databricks uses a notebook-scoped buffer (_pentaho_result_rows) for downstream hops / orchestration. Cross-job Result transfer needs Databricks Jobs task values or persisted Delta tables.
    _pentaho_result_rows = globals().setdefault('_pentaho_result_rows', {})
    _pentaho_result_files = globals().setdefault('_pentaho_result_files', [])
    # Preserve schema and relative ordering for 'Copy Audit Metrics To Result'
    _result_rows_df_Copy_Audit_Metrics_To_Result = df_Log_Audit_Complete
    _pentaho_result_rows['Copy Audit Metrics To Result'] = _result_rows_df_Copy_Audit_Metrics_To_Result
    _pentaho_result_rows['__latest__'] = _result_rows_df_Copy_Audit_Metrics_To_Result
    df_Copy_Audit_Metrics_To_Result = df_Log_Audit_Complete

    # Step: Audit Complete (Dummy) [converted]
    # Dummy: Audit Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Audit_Complete = df_Copy_Audit_Metrics_To_Result

    log_event(_LOG, "transformation_end")
    return df_Dummy_Audit_Complete
