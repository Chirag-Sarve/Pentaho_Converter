"""PySpark module migrated from Pentaho transformation: TR_Inventory_Audit.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/utilities/TR_Inventory_Audit.ktr
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_inventory_audit")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Inventory_Audit`` step-for-step."""
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
    data = [('fact_inventory', 'INVENTORY_ETL', 'SUCCESS')]
    df_Generate_Audit_Seed = spark.createDataFrame(data, ['object_name', 'layer', 'status'])

    # Step: Read Extract Metrics (CsvInput) [converted]
    # CSV Input: Read Extract Metrics
    df_Read_Extract_Metrics = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('object_name STRING, layer STRING, rows_extracted INT, status STRING, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/TR_Inventory_Extract_.log')
    )

    # Step: Read Fact Metrics (CsvInput) [converted]
    # CSV Input: Read Fact Metrics
    df_Read_Fact_Metrics = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('fact_action STRING, action_count INT')
        .load(f'{data_dir}/TR_FactInventory_Load_.log')
    )

    # Step: Read Reject File (CsvInput) [converted]
    # CSV Input: Read Reject File
    df_Read_Reject_File = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('inventory_id STRING, reject_reason STRING')
        .load(f'{data_dir}/inventory_rejects_.csv')
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
        .load(f'{data_dir}/TR_Inventory_Validation_.log')
    )

    # Step: Get Audit Variables (GetVariable) [converted]
    # Get Variables: Get Audit Variables
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'project_home', 'variable': '${PROJECT_HOME}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'current_date', 'variable': '${CURRENT_DATE}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'module_name', 'variable': 'Load_Inventory_Data', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
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
    # field 'module_name' from variable string 'Load_Inventory_Data'
    # preserved.field.module_name.trim_type='none'
    # preserved.field.module_name.type='String'
    _module_name_resolved = None
    _dbu__module_name_resolved = globals().get('dbutils')
    if _dbu__module_name_resolved is not None and hasattr(_dbu__module_name_resolved, 'widgets'):
        try:
            _module_name_resolved = _dbu__module_name_resolved.widgets.get('Load_Inventory_Data')
        except Exception:
            _module_name_resolved = None
    if _module_name_resolved in (None, ''):
        import os as _os__module_name_resolved
        _module_name_resolved = _os__module_name_resolved.environ.get('Load_Inventory_Data')
    if _module_name_resolved in (None, ''):
        try:
            _module_name_resolved = spark.conf.get('pentaho.var.Load_Inventory_Data')
        except Exception:
            _module_name_resolved = None
    if _module_name_resolved in (None, ''):
        _module_name_resolved = ''
    if _module_name_resolved is None:
        _module_name_resolved = ''
    df_Get_Audit_Variables = df_Get_Audit_Variables.withColumn('module_name', lit(_module_name_resolved))

    # Step: Sum Extracted Rows (MemoryGroupBy) [converted]
    # Memory Group By: Sum Extracted Rows
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Sum_Extracted_Rows = df_Read_Extract_Metrics.groupBy().agg(_sum(col("rows_extracted")).alias('rows_read'))

    # Step: Inserted Actions? (FilterRows) [failed]
    # Filter Rows: Inserted Actions?
    df_Sum_Inserts = df_Read_Fact_Metrics.filter((col("fact_action") == lit('INSERT')))
    df_Non_Insert_Stats = df_Read_Fact_Metrics.filter(~((col("fact_action") == lit('INSERT'))))
    df_Inserted_Actions? = df_Sum_Inserts

    # Step: Sum All Fact Actions (MemoryGroupBy) [converted]
    # Memory Group By: Sum All Fact Actions
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Sum_All_Fact_Actions = df_Read_Fact_Metrics.groupBy().agg(_sum(col("action_count")).alias('rows_written_all'))

    # Step: Updated Actions? (FilterRows) [failed]
    # Filter Rows: Updated Actions?
    df_Sum_Updates = df_Read_Fact_Metrics.filter((col("fact_action") == lit('UPDATE')))
    df_Skip_Non_Updates = df_Read_Fact_Metrics.filter(~((col("fact_action") == lit('UPDATE'))))
    df_Updated_Actions? = df_Sum_Updates

    # Step: Count Rejects (MemoryGroupBy) [converted]
    # Memory Group By: Count Rejects
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Rejects = df_Read_Reject_File.groupBy().agg(count(lit(1)).alias('rows_rejected'))

    # Step: Sum Validated Rows (MemoryGroupBy) [converted]
    # Memory Group By: Sum Validated Rows
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Sum_Validated_Rows = df_Read_Validation_Metrics.groupBy().agg(_sum(col("row_count")).alias('rows_validated'))

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

    # Step: Compose Audit Record (Constant) [converted]
    # Add Constants: Compose Audit Record
    df_Compose_Audit_Record = df_Capture_Audit_Timestamps
    df_Compose_Audit_Record = df_Compose_Audit_Record.withColumn("object_name", lit('fact_inventory'))
    # preserved.object_name: length='-1', precision='-1'
    df_Compose_Audit_Record = df_Compose_Audit_Record.withColumn("layer", lit('INVENTORY_ETL'))
    # preserved.layer: length='-1', precision='-1'
    df_Compose_Audit_Record = df_Compose_Audit_Record.withColumn("status", lit('SUCCESS'))
    # preserved.status: length='-1', precision='-1'
    df_Compose_Audit_Record = df_Compose_Audit_Record.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Compose_Audit_Record = df_Compose_Audit_Record.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'
    df_Compose_Audit_Record = df_Compose_Audit_Record.withColumn("rows_written", lit(0))
    # preserved.rows_written: length='-1', precision='-1'
    df_Compose_Audit_Record = df_Compose_Audit_Record.withColumn("error_count", lit(0))
    # preserved.error_count: length='-1', precision='-1'

    # Step: Finalize Audit Metrics (Formula) [converted]
    # Formula: Finalize Audit Metrics
    df_Finalize_Audit_Metrics = df_Compose_Audit_Record
    df_Finalize_Audit_Metrics = df_Finalize_Audit_Metrics.withColumn('formula_result', lit(None))  # empty formula

    # Step: Write Inventory Audit JSON (JsonOutput) [converted]
    # Pentaho step: Write Inventory Audit JSON (type: JsonOutput)
    df_Write_Inventory_Audit_JSON = df_Finalize_Audit_Metrics
    df_Write_Inventory_Audit_JSON.write \
    .mode('overwrite') \
    .json(
        f'{data_dir}/inventory_audit_.json'
    )
    # preserved.json_bloc='rows' output_value='json_blob'

    # Step: Write Inventory Audit Table (TableOutput) [converted]
    # Pentaho step: Write Inventory Audit Table (type: TableOutput) (Pentaho schema: retail_audit)
    # Mode: append (Pentaho truncate=N)
    _mapped_df_Write_Inventory_Audit_Table = df_Finalize_Audit_Metrics.select(col('object_name'), col('layer'), col('batch_id'), col('run_id'), col('rows_read'), col('rows_written'), col('rows_rejected'), col('error_count'), col('status'), col('execution_time_ms'))
    df_Write_Inventory_Audit_Table = _mapped_df_Write_Inventory_Audit_Table
    write_delta(
        df_Write_Inventory_Audit_Table,
        f"{catalog}.{schema}.etl_execution_audit",
        mode='append',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.etl_execution_audit", mode='append')

    # Step: Write Inventory Execution Log (TextFileOutput) [converted]
    # Pentaho step: Write Inventory Execution Log (type: TextFileOutput)
    # Pentaho filename: /logs/execution/inventory/TR_Inventory_Audit_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='object_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='layer' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_read' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_written' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_rejected' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='error_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='execution_time_ms' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Inventory_Execution_Log = df_Finalize_Audit_Metrics
    _out_df_Write_Inventory_Execution_Log = df_Write_Inventory_Execution_Log.select('object_name', 'layer', 'rows_read', 'rows_written', 'rows_rejected', 'error_count', 'execution_time_ms', 'status', 'batch_id', 'run_id')
    writer = _out_df_Write_Inventory_Execution_Log.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/TR_Inventory_Audit_.log')

    # Step: Copy Audit Metrics To Result (RowsToResult) [converted]
    # Copy Rows to Result: Copy Audit Metrics To Result
    # preserved.result_buffer='rows'
    # preserved.preserve_order=True
    # LIMITATION: Pentaho Result rows are job-level; Databricks uses a notebook-scoped buffer (_pentaho_result_rows) for downstream hops / orchestration. Cross-job Result transfer needs Databricks Jobs task values or persisted Delta tables.
    _pentaho_result_rows = globals().setdefault('_pentaho_result_rows', {})
    _pentaho_result_files = globals().setdefault('_pentaho_result_files', [])
    # Preserve schema and relative ordering for 'Copy Audit Metrics To Result'
    _result_rows_df_Copy_Audit_Metrics_To_Result = df_Write_Inventory_Audit_JSON
    _pentaho_result_rows['Copy Audit Metrics To Result'] = _result_rows_df_Copy_Audit_Metrics_To_Result
    _pentaho_result_rows['__latest__'] = _result_rows_df_Copy_Audit_Metrics_To_Result
    df_Copy_Audit_Metrics_To_Result = df_Write_Inventory_Audit_JSON

    # Step: Log Inventory Audit (WriteToLog) [converted]
    # Write to Log: Log Inventory Audit
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=AUDIT_OK | TRANS=TR_Inventory_Audit | RUN_ID=${RUN_ID} | ROWS_READ/WRITE/REJECT logged'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Inventory_Audit = logging.getLogger('pentaho.writetolog.Log_Inventory_Audit')
    _log_df_Log_Inventory_Audit.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Inventory_Audit = df_Copy_Audit_Metrics_To_Result
    _log_rows_df_Log_Inventory_Audit = _log_df_df_Log_Inventory_Audit.take(20)
    _log_df_Log_Inventory_Audit.info('Log Inventory Audit' + ' | columns=' + str(_log_df_df_Log_Inventory_Audit.columns))
    _log_df_Log_Inventory_Audit.info('AUDIT | EVENT=AUDIT_OK | TRANS=TR_Inventory_Audit | RUN_ID=${RUN_ID} | ROWS_READ/WRITE/REJECT logged')
    for _lr in _log_rows_df_Log_Inventory_Audit:
        _log_df_Log_Inventory_Audit.info('Log Inventory Audit' + ' | ' + str(_lr.asDict()))
    df_Log_Inventory_Audit = df_Copy_Audit_Metrics_To_Result

    # Step: Audit Complete (Dummy) [converted]
    # Dummy: Audit Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Audit_Complete = df_Log_Inventory_Audit

    log_event(_LOG, "transformation_end")
    return df_Dummy_Audit_Complete
