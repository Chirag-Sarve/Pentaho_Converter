"""PySpark module migrated from Pentaho transformation: TR_Finance_Audit.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/utilities/TR_Finance_Audit.ktr
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
    when,
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_finance_audit")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Finance_Audit`` step-for-step."""
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
    data = [('finance_reporting', 'FINANCE_REPORTING_ETL', 'SUCCESS')]
    df_Generate_Audit_Seed = spark.createDataFrame(data, ['object_name', 'layer', 'status'])

    # Step: Get Audit Variables (GetVariable) [converted]
    # Get Variables: Get Audit Variables
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'project_home', 'variable': '${PROJECT_HOME}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'current_date', 'variable': '${CURRENT_DATE}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'module_name', 'variable': 'Load_Finance_Reporting', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
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
    # field 'module_name' from variable string 'Load_Finance_Reporting'
    # preserved.field.module_name.trim_type='none'
    # preserved.field.module_name.type='String'
    _module_name_resolved = None
    _dbu__module_name_resolved = globals().get('dbutils')
    if _dbu__module_name_resolved is not None and hasattr(_dbu__module_name_resolved, 'widgets'):
        try:
            _module_name_resolved = _dbu__module_name_resolved.widgets.get('Load_Finance_Reporting')
        except Exception:
            _module_name_resolved = None
    if _module_name_resolved in (None, ''):
        import os as _os__module_name_resolved
        _module_name_resolved = _os__module_name_resolved.environ.get('Load_Finance_Reporting')
    if _module_name_resolved in (None, ''):
        try:
            _module_name_resolved = spark.conf.get('pentaho.var.Load_Finance_Reporting')
        except Exception:
            _module_name_resolved = None
    if _module_name_resolved in (None, ''):
        _module_name_resolved = ''
    if _module_name_resolved is None:
        _module_name_resolved = ''
    df_Get_Audit_Variables = df_Get_Audit_Variables.withColumn('module_name', lit(_module_name_resolved))

    # Step: Capture Audit Timestamps (SystemInfo) [converted]
    # System Info: Capture Audit Timestamps
    df_Capture_Audit_Timestamps = df_Get_Audit_Variables
    df_Capture_Audit_Timestamps = df_Capture_Audit_Timestamps.withColumn("audit_start_ts", current_date())
    df_Capture_Audit_Timestamps = df_Capture_Audit_Timestamps.withColumn("audit_end_ts", current_date())
    df_Capture_Audit_Timestamps = df_Capture_Audit_Timestamps.withColumn("execution_time_ms", current_timestamp())

    # Step: Read Calc Metrics (CsvInput) [converted]
    # CSV Input: Read Calc Metrics
    df_Read_Calc_Metrics = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('rows_calculated INT, revenue_total DOUBLE, gross_profit_total DOUBLE, net_profit_total DOUBLE, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/TR_Financial_Calculations_.log')
    )

    # Step: Read Calc Rejects (CsvInput) [converted]
    # CSV Input: Read Calc Rejects
    df_Read_Calc_Rejects = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('order_item_id STRING, order_id STRING, order_id_valid STRING, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/finance_calc_rejects_.csv')
    )

    # Step: Read Extract Metrics (CsvInput) [converted]
    # CSV Input: Read Extract Metrics
    df_Read_Extract_Metrics = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('object_name STRING, layer STRING, rows_extracted INT, revenue_sum DOUBLE, status STRING, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/TR_Finance_Extract_.log')
    )

    # Step: Read Extract Rejects (CsvInput) [converted]
    # CSV Input: Read Extract Rejects
    df_Read_Extract_Rejects = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('order_item_id STRING, order_id STRING, currency_code STRING, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/finance_extract_rejects_.csv')
    )

    # Step: Read KPI Rejects (CsvInput) [converted]
    # CSV Input: Read KPI Rejects
    df_Read_KPI_Rejects = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('period_type STRING, revenue DOUBLE, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/kpi_rejects_.csv')
    )

    # Step: Sum Calculated Rows (MemoryGroupBy) [converted]
    # Memory Group By: Sum Calculated Rows
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Sum_Calculated_Rows = df_Read_Calc_Metrics.groupBy().agg(_sum(col("rows_calculated")).alias('rows_written'))

    # Step: Count Calc Rejects (MemoryGroupBy) [converted]
    # Memory Group By: Count Calc Rejects
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Calc_Rejects = df_Read_Calc_Rejects.groupBy().agg(count(lit(1)).alias('calc_rejects'))

    # Step: Sum Extracted Rows (MemoryGroupBy) [converted]
    # Memory Group By: Sum Extracted Rows
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Sum_Extracted_Rows = df_Read_Extract_Metrics.groupBy().agg(_sum(col("rows_extracted")).alias('rows_read'))

    # Step: Count Extract Rejects (MemoryGroupBy) [converted]
    # Memory Group By: Count Extract Rejects
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Extract_Rejects = df_Read_Extract_Rejects.groupBy().agg(count(lit(1)).alias('extract_rejects'))

    # Step: Count KPI Rejects (MemoryGroupBy) [converted]
    # Memory Group By: Count KPI Rejects
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_KPI_Rejects = df_Read_KPI_Rejects.groupBy().agg(count(lit(1)).alias('kpi_rejects'))

    # Step: Compose Audit Record (Constant) [converted]
    # Add Constants: Compose Audit Record
    df_Compose_Audit_Record = df_Capture_Audit_Timestamps
    df_Compose_Audit_Record = df_Compose_Audit_Record.withColumn("object_name", lit('finance_reporting'))
    # preserved.object_name: length='-1', precision='-1'
    df_Compose_Audit_Record = df_Compose_Audit_Record.withColumn("layer", lit('FINANCE_REPORTING_ETL'))
    # preserved.layer: length='-1', precision='-1'
    df_Compose_Audit_Record = df_Compose_Audit_Record.withColumn("status", lit('SUCCESS'))
    # preserved.status: length='-1', precision='-1'
    df_Compose_Audit_Record = df_Compose_Audit_Record.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Compose_Audit_Record = df_Compose_Audit_Record.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'
    df_Compose_Audit_Record = df_Compose_Audit_Record.withColumn("rows_written_default", lit(0))
    # preserved.rows_written_default: length='-1', precision='-1'
    df_Compose_Audit_Record = df_Compose_Audit_Record.withColumn("error_count_default", lit(0))
    # preserved.error_count_default: length='-1', precision='-1'

    # Step: Finalize Audit Metrics (Formula) [converted]
    # Formula: Finalize Audit Metrics
    df_Finalize_Audit_Metrics = df_Compose_Audit_Record
    df_Finalize_Audit_Metrics = df_Finalize_Audit_Metrics.withColumn('formula_result', lit(None))  # empty formula

    # Step: Audit Healthy? (FilterRows) [failed]
    # Filter Rows: Audit Healthy?
    df_Checksum_Audit_Payload = df_Finalize_Audit_Metrics.filter((col("status") == lit('SUCCESS')))
    df_Abort_Audit_Failure = df_Finalize_Audit_Metrics.filter(~((col("status") == lit('SUCCESS'))))
    df_Audit_Healthy? = df_Checksum_Audit_Payload

    # Step: Abort Audit Failure (Abort) [converted]
    # Abort: Abort Audit Failure
    # preserved.row_threshold=0
    # preserved.message='Finance audit recorded FAILURE status. RUN_ID=${RUN_ID}'
    # preserved.always_log_rows=True
    # preserved.row_threshold_raw='0'
    # Abort operates on its own failure/branch stream df_Abort_Audit_Failure (already assigned by upstream Filter/Switch; not overwritten)
    print('Abort sample for', 'Abort Audit Failure', df_Abort_Audit_Failure.limit(100).collect())  # always_log_rows
    _abort_count_df_Abort_Audit_Failure = df_Abort_Audit_Failure.count()
    if _abort_count_df_Abort_Audit_Failure > 0:  # Abort when any row reaches this step (threshold<=0)
        raise RuntimeError('Finance audit recorded FAILURE status. RUN_ID=${RUN_ID}')

    # Step: Checksum Audit Payload (CheckSum) [failed]
    # Add a Checksum: Checksum Audit Payload
    df_Checksum_Audit_Payload = df_Audit_Healthy?
    df_Checksum_Audit_Payload = df_Checksum_Audit_Payload.withColumn("audit_md5", md5(concat(coalesce(col("object_name").cast("string"), lit("")), coalesce(col("batch_id").cast("string"), lit("")), coalesce(col("run_id").cast("string"), lit("")), coalesce(col("rows_read").cast("string"), lit("")), coalesce(col("rows_written").cast("string"), lit("")), coalesce(col("rows_rejected").cast("string"), lit("")))))
    # preserved.checksumtype='MD5' resultType='hexadecimal' fields=['object_name', 'batch_id', 'run_id', 'rows_read', 'rows_written', 'rows_rejected']

    # Step: Write Error Log (TextFileOutput) [converted]
    # Pentaho step: Write Error Log (type: TextFileOutput)
    # Pentaho filename: /logs/execution/finance/TR_Finance_Error_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='object_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_rejected' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='error_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='audit_md5' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Error_Log = df_Checksum_Audit_Payload
    _out_df_Write_Error_Log = df_Write_Error_Log.select('object_name', 'rows_rejected', 'error_count', 'status', 'batch_id', 'run_id', 'audit_md5')
    writer = _out_df_Write_Error_Log.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/TR_Finance_Error_.log')

    # Step: Write Execution Statistics Report (TextFileOutput) [converted]
    # Pentaho step: Write Execution Statistics Report (type: TextFileOutput)
    # Pentaho filename: /audit/load_audit/finance_execution_stats_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='rows_read' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_written' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_rejected' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='error_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='execution_time_ms' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='performance_rows_per_sec' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Execution_Statistics_Report = df_Checksum_Audit_Payload
    _out_df_Write_Execution_Statistics_Report = df_Write_Execution_Statistics_Report.select('rows_read', 'rows_written', 'rows_rejected', 'error_count', 'execution_time_ms', 'performance_rows_per_sec', 'batch_id', 'run_id')
    writer = _out_df_Write_Execution_Statistics_Report.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/finance_execution_stats_.csv')

    # Step: Write Finance Audit JSON (JsonOutput) [converted]
    # Pentaho step: Write Finance Audit JSON (type: JsonOutput)
    df_Write_Finance_Audit_JSON = df_Checksum_Audit_Payload
    df_Write_Finance_Audit_JSON.write \
    .mode('overwrite') \
    .json(
        f'{data_dir}/finance_audit_.json'
    )
    # preserved.json_bloc='rows' output_value='json_blob'

    # Step: Write Finance Audit Table (TableOutput) [converted]
    # Pentaho step: Write Finance Audit Table (type: TableOutput) (Pentaho schema: retail_audit)
    # Mode: append (Pentaho truncate=N)
    _mapped_df_Write_Finance_Audit_Table = df_Checksum_Audit_Payload.select(col('object_name'), col('layer'), col('batch_id'), col('run_id'), col('rows_read'), col('rows_written'), col('rows_rejected'), col('error_count'), col('status'), col('execution_time_ms'), col('performance_rows_per_sec'), col('audit_md5'))
    df_Write_Finance_Audit_Table = _mapped_df_Write_Finance_Audit_Table
    write_delta(
        df_Write_Finance_Audit_Table,
        f"{catalog}.{schema}.etl_execution_audit",
        mode='append',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.etl_execution_audit", mode='append')

    # Step: Write Performance Metrics Report (TextFileOutput) [converted]
    # Pentaho step: Write Performance Metrics Report (type: TextFileOutput)
    # Pentaho filename: /audit/load_audit/finance_performance_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='performance_rows_per_sec' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='execution_time_ms' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_written' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Performance_Metrics_Report = df_Checksum_Audit_Payload
    _out_df_Write_Performance_Metrics_Report = df_Write_Performance_Metrics_Report.select('performance_rows_per_sec', 'execution_time_ms', 'rows_written', 'batch_id', 'run_id')
    writer = _out_df_Write_Performance_Metrics_Report.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/finance_performance_.csv')

    # Step: Write Transformation Log (TextFileOutput) [converted]
    # Pentaho step: Write Transformation Log (type: TextFileOutput)
    # Pentaho filename: /logs/execution/finance/TR_Finance_Audit_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='object_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='layer' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_read' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_written' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_rejected' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='error_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='execution_time_ms' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='performance_rows_per_sec' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='audit_md5' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Transformation_Log = df_Checksum_Audit_Payload
    _out_df_Write_Transformation_Log = df_Write_Transformation_Log.select('object_name', 'layer', 'batch_id', 'run_id', 'rows_read', 'rows_written', 'rows_rejected', 'error_count', 'status', 'execution_time_ms', 'performance_rows_per_sec', 'audit_md5')
    writer = _out_df_Write_Transformation_Log.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/TR_Finance_Audit_.log')

    # Step: Copy Audit Metrics To Result (RowsToResult) [converted]
    # Copy Rows to Result: Copy Audit Metrics To Result
    # preserved.result_buffer='rows'
    # preserved.preserve_order=True
    # LIMITATION: Pentaho Result rows are job-level; Databricks uses a notebook-scoped buffer (_pentaho_result_rows) for downstream hops / orchestration. Cross-job Result transfer needs Databricks Jobs task values or persisted Delta tables.
    _pentaho_result_rows = globals().setdefault('_pentaho_result_rows', {})
    _pentaho_result_files = globals().setdefault('_pentaho_result_files', [])
    # Preserve schema and relative ordering for 'Copy Audit Metrics To Result'
    _result_rows_df_Copy_Audit_Metrics_To_Result = df_Write_Finance_Audit_JSON
    _pentaho_result_rows['Copy Audit Metrics To Result'] = _result_rows_df_Copy_Audit_Metrics_To_Result
    _pentaho_result_rows['__latest__'] = _result_rows_df_Copy_Audit_Metrics_To_Result
    df_Copy_Audit_Metrics_To_Result = df_Write_Finance_Audit_JSON

    # Step: Log Finance Audit (WriteToLog) [converted]
    # Write to Log: Log Finance Audit
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=AUDIT_OK | TRANS=TR_Finance_Audit | RUN_ID=${RUN_ID} | ROWS_READ/WRITE/REJECT logged'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Finance_Audit = logging.getLogger('pentaho.writetolog.Log_Finance_Audit')
    _log_df_Log_Finance_Audit.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Finance_Audit = df_Copy_Audit_Metrics_To_Result
    _log_rows_df_Log_Finance_Audit = _log_df_df_Log_Finance_Audit.take(20)
    _log_df_Log_Finance_Audit.info('Log Finance Audit' + ' | columns=' + str(_log_df_df_Log_Finance_Audit.columns))
    _log_df_Log_Finance_Audit.info('AUDIT | EVENT=AUDIT_OK | TRANS=TR_Finance_Audit | RUN_ID=${RUN_ID} | ROWS_READ/WRITE/REJECT logged')
    for _lr in _log_rows_df_Log_Finance_Audit:
        _log_df_Log_Finance_Audit.info('Log Finance Audit' + ' | ' + str(_lr.asDict()))
    df_Log_Finance_Audit = df_Copy_Audit_Metrics_To_Result

    # Step: Audit Complete (Dummy) [converted]
    # Dummy: Audit Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Audit_Complete = df_Log_Finance_Audit

    log_event(_LOG, "transformation_end")
    return df_Dummy_Audit_Complete
