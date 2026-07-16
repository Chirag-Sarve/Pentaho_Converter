"""PySpark module migrated from Pentaho transformation: TR_Customer_Profile.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/staging/TR_Customer_Profile.ktr
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_customer_profile")

from pyspark.sql.window import Window


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Customer_Profile`` step-for-step."""
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
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
    # preserved.output_columns=['batch_id', 'run_id']
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

    # Step: Write Profile Rejects (TextFileOutput) [converted]
    # Pentaho step: Write Profile Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/customer/customers_profile_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='customer_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_CODE' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_DESC' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_FIELDS' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Profile_Rejects = df_Write_Profile_Rejects
    _out_df_Write_Profile_Rejects = df_Write_Profile_Rejects.select('customer_id', 'ERR_CODE', 'ERR_DESC', 'ERR_FIELDS', 'batch_id')
    writer = _out_df_Write_Profile_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/customers_profile_rejects_.csv')

    # Step: Empty String To Null (NullIf) [converted]
    # Null If: Empty String To Null
    # preserved.fields=[{'name': 'customer_id', 'value': '', 'type': ''}, {'name': 'first_name', 'value': '', 'type': ''}, {'name': 'last_name', 'value': '', 'type': ''}, {'name': 'email', 'value': '', 'type': ''}, {'name': 'phone', 'value': '', 'type': ''}, {'name': 'address_line1', 'value': '', 'type': ''}, {'name': 'address_line2', 'value': '', 'type': ''}, {'name': 'city', 'value': '', 'type': ''}, {'name': 'state_province', 'value': '', 'type': ''}, {'name': 'postal_code', 'value': '', 'type': ''}, {'name': 'country_code', 'value': '', 'type': ''}, {'name': 'country_name', 'value': '', 'type': ''}, {'name': 'preferred_currency', 'value': '', 'type': ''}, {'name': 'loyalty_tier', 'value': '', 'type': ''}, {'name': 'registration_date', 'value': '', 'type': ''}, {'name': 'date_of_birth', 'value': '', 'type': ''}, {'name': 'is_active', 'value': '', 'type': ''}]
    df_Empty_String_To_Null = df_Read_customers.csv
    df_Empty_String_To_Null = df_Empty_String_To_Null.withColumn('customer_id', when((col('customer_id').isNull() | (col('customer_id').cast('string') == lit(''))), lit(None)).otherwise(col('customer_id')))
    df_Empty_String_To_Null = df_Empty_String_To_Null.withColumn('first_name', when((col('first_name').isNull() | (col('first_name').cast('string') == lit(''))), lit(None)).otherwise(col('first_name')))
    df_Empty_String_To_Null = df_Empty_String_To_Null.withColumn('last_name', when((col('last_name').isNull() | (col('last_name').cast('string') == lit(''))), lit(None)).otherwise(col('last_name')))
    df_Empty_String_To_Null = df_Empty_String_To_Null.withColumn('email', when((col('email').isNull() | (col('email').cast('string') == lit(''))), lit(None)).otherwise(col('email')))
    df_Empty_String_To_Null = df_Empty_String_To_Null.withColumn('phone', when((col('phone').isNull() | (col('phone').cast('string') == lit(''))), lit(None)).otherwise(col('phone')))
    df_Empty_String_To_Null = df_Empty_String_To_Null.withColumn('address_line1', when((col('address_line1').isNull() | (col('address_line1').cast('string') == lit(''))), lit(None)).otherwise(col('address_line1')))
    df_Empty_String_To_Null = df_Empty_String_To_Null.withColumn('address_line2', when((col('address_line2').isNull() | (col('address_line2').cast('string') == lit(''))), lit(None)).otherwise(col('address_line2')))
    df_Empty_String_To_Null = df_Empty_String_To_Null.withColumn('city', when((col('city').isNull() | (col('city').cast('string') == lit(''))), lit(None)).otherwise(col('city')))
    df_Empty_String_To_Null = df_Empty_String_To_Null.withColumn('state_province', when((col('state_province').isNull() | (col('state_province').cast('string') == lit(''))), lit(None)).otherwise(col('state_province')))
    df_Empty_String_To_Null = df_Empty_String_To_Null.withColumn('postal_code', when((col('postal_code').isNull() | (col('postal_code').cast('string') == lit(''))), lit(None)).otherwise(col('postal_code')))
    df_Empty_String_To_Null = df_Empty_String_To_Null.withColumn('country_code', when((col('country_code').isNull() | (col('country_code').cast('string') == lit(''))), lit(None)).otherwise(col('country_code')))
    df_Empty_String_To_Null = df_Empty_String_To_Null.withColumn('country_name', when((col('country_name').isNull() | (col('country_name').cast('string') == lit(''))), lit(None)).otherwise(col('country_name')))
    df_Empty_String_To_Null = df_Empty_String_To_Null.withColumn('preferred_currency', when((col('preferred_currency').isNull() | (col('preferred_currency').cast('string') == lit(''))), lit(None)).otherwise(col('preferred_currency')))
    df_Empty_String_To_Null = df_Empty_String_To_Null.withColumn('loyalty_tier', when((col('loyalty_tier').isNull() | (col('loyalty_tier').cast('string') == lit(''))), lit(None)).otherwise(col('loyalty_tier')))
    df_Empty_String_To_Null = df_Empty_String_To_Null.withColumn('registration_date', when((col('registration_date').isNull() | (col('registration_date').cast('string') == lit(''))), lit(None)).otherwise(col('registration_date')))
    df_Empty_String_To_Null = df_Empty_String_To_Null.withColumn('date_of_birth', when((col('date_of_birth').isNull() | (col('date_of_birth').cast('string') == lit(''))), lit(None)).otherwise(col('date_of_birth')))
    df_Empty_String_To_Null = df_Empty_String_To_Null.withColumn('is_active', when((col('is_active').isNull() | (col('is_active').cast('string') == lit(''))), lit(None)).otherwise(col('is_active')))

    # Step: Add Profile Constants (Constant) [converted]
    # Add Constants: Add Profile Constants
    df_Add_Profile_Constants = df_Empty_String_To_Null
    df_Add_Profile_Constants = df_Add_Profile_Constants.withColumn("row_flag", lit('1'))
    # preserved.row_flag: length='-1', precision='-1'
    df_Add_Profile_Constants = df_Add_Profile_Constants.withColumn("profile_entity", lit('customers'))
    # preserved.profile_entity: length='-1', precision='-1'
    df_Add_Profile_Constants = df_Add_Profile_Constants.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'

    # Step: Derive Profile Dimensions (Formula) [converted]
    # Formula: Derive Profile Dimensions
    df_Derive_Profile_Dimensions = df_Add_Profile_Constants
    df_Derive_Profile_Dimensions = df_Derive_Profile_Dimensions.withColumn('formula_result', lit(None))  # empty formula

    # Step: Calculate Null Indicators (Calculator) [converted]
    # Calculator: Calculate Null Indicators
    df_Calculate_Null_Indicators = df_Derive_Profile_Dimensions
    df_Calculate_Null_Indicators = df_Calculate_Null_Indicators.withColumn("null_score", ((col("email_is_null") + col("phone_is_null"))).cast('int'))

    # Step: Sort By Customer ID (SortRows) [converted]
    # Sort Rows: Sort By Customer ID
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_By_Customer_ID = df_Calculate_Null_Indicators
    _sort_df_Sort_By_Customer_ID = _sort_df_Sort_By_Customer_ID.withColumn("_sort_ci_customer_id", lower(col("customer_id").cast("string")))
    df_Sort_By_Customer_ID = _sort_df_Sort_By_Customer_ID.orderBy(col("_sort_ci_customer_id").asc_nulls_last())
    df_Sort_By_Customer_ID = df_Sort_By_Customer_ID.drop("_sort_ci_customer_id")

    # Step: Detect Duplicate Keys (Unique) [converted]
    # Unique Rows: Detect Duplicate Keys
    # preserved.reject_duplicate_row=N error_description=''
    # Unique Rows expects sorted input in Pentaho; Spark dropDuplicates is order-independent
    # preserved.count_rows=True count_field='customer_id_dup_count' compare_fields=['customer_id']
    df_Detect_Duplicate_Keys = df_Sort_By_Customer_ID
    _w_cnt_df_Detect_Duplicate_Keys = Window.partitionBy(col("customer_id"))
    df_Detect_Duplicate_Keys = df_Detect_Duplicate_Keys.withColumn("customer_id_dup_count", count(lit(1)).over(_w_cnt_df_Detect_Duplicate_Keys))
    _w_rn_df_Detect_Duplicate_Keys = Window.partitionBy(col("customer_id")).orderBy(monotonically_increasing_id())
    df_Detect_Duplicate_Keys = df_Detect_Duplicate_Keys.withColumn('_uniq_rn', row_number().over(_w_rn_df_Detect_Duplicate_Keys))
    df_Detect_Duplicate_Keys = df_Detect_Duplicate_Keys.filter(col('_uniq_rn') == 1).drop('_uniq_rn')

    # Step: Log Profiling Progress (WriteToLog) [converted]
    # Write to Log: Log Profiling Progress
    # preserved.log_level='Basic'
    # preserved.log_message='PROFILE | TRANS=TR_Customer_Profile | RUN_ID=${RUN_ID} | customer_id=${customer_id}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Profiling_Progress = logging.getLogger('pentaho.writetolog.Log_Profiling_Progress')
    _log_df_Log_Profiling_Progress.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Profiling_Progress = df_Detect_Duplicate_Keys
    _log_rows_df_Log_Profiling_Progress = _log_df_df_Log_Profiling_Progress.take(20)
    _log_df_Log_Profiling_Progress.info('Log Profiling Progress' + ' | columns=' + str(_log_df_df_Log_Profiling_Progress.columns))
    _log_df_Log_Profiling_Progress.info('PROFILE | TRANS=TR_Customer_Profile | RUN_ID=${RUN_ID} | customer_id=${customer_id}')
    for _lr in _log_rows_df_Log_Profiling_Progress:
        _log_df_Log_Profiling_Progress.info('Log Profiling Progress' + ' | ' + str(_lr.asDict()))
    df_Log_Profiling_Progress = df_Detect_Duplicate_Keys

    # Step: Overall Null Statistics (MemoryGroupBy) [partial]
    # Memory Group By: Overall Null Statistics
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Overall_Null_Statistics = df_Detect_Duplicate_Keys.groupBy().agg(count(lit(1)).alias('rows_profiled'), _sum(col("email_is_null")).alias('null_email_count'), _sum(col("phone_is_null")).alias('null_phone_count'), _sum(col("country_is_null")).alias('null_country_count'), _sum(col("dob_is_null")).alias('null_dob_count'), _sum(col("customer_id_dup_count")).alias('duplicate_key_rows'))

    # Step: Sort By Country (SortRows) [converted]
    # Sort Rows: Sort By Country
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_By_Country = df_Detect_Duplicate_Keys
    _sort_df_Sort_By_Country = _sort_df_Sort_By_Country.withColumn("_sort_ci_country_code", lower(col("country_code").cast("string")))
    df_Sort_By_Country = _sort_df_Sort_By_Country.orderBy(col("_sort_ci_country_code").asc_nulls_last())
    df_Sort_By_Country = df_Sort_By_Country.drop("_sort_ci_country_code")

    # Step: Sort By Customer Type (SortRows) [converted]
    # Sort Rows: Sort By Customer Type
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_By_Customer_Type = df_Detect_Duplicate_Keys
    _sort_df_Sort_By_Customer_Type = _sort_df_Sort_By_Customer_Type.withColumn("_sort_ci_customer_type", lower(col("customer_type").cast("string")))
    df_Sort_By_Customer_Type = _sort_df_Sort_By_Customer_Type.orderBy(col("_sort_ci_customer_type").asc_nulls_last())
    df_Sort_By_Customer_Type = df_Sort_By_Customer_Type.drop("_sort_ci_customer_type")

    # Step: Sort By Gender (SortRows) [converted]
    # Sort Rows: Sort By Gender
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_By_Gender = df_Detect_Duplicate_Keys
    _sort_df_Sort_By_Gender = _sort_df_Sort_By_Gender.withColumn("_sort_ci_gender_proxy", lower(col("gender_proxy").cast("string")))
    df_Sort_By_Gender = _sort_df_Sort_By_Gender.orderBy(col("_sort_ci_gender_proxy").asc_nulls_last())
    df_Sort_By_Gender = df_Sort_By_Gender.drop("_sort_ci_gender_proxy")

    # Step: Tag Profile Report (Constant) [converted]
    # Add Constants: Tag Profile Report
    df_Tag_Profile_Report = df_Overall_Null_Statistics
    df_Tag_Profile_Report = df_Tag_Profile_Report.withColumn("report_name", lit('Customer_Profiling_Report'))
    # preserved.report_name: length='-1', precision='-1'
    df_Tag_Profile_Report = df_Tag_Profile_Report.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'
    df_Tag_Profile_Report = df_Tag_Profile_Report.withColumn("status", lit('SUCCESS'))
    # preserved.status: length='-1', precision='-1'

    # Step: Country Distribution (GroupBy) [converted]
    # Group By: Country Distribution
    df_Country_Distribution = df_Sort_By_Country.groupBy('country_code').agg(count(lit(1)).alias('country_customer_count'))

    # Step: Customer Type Distribution (GroupBy) [converted]
    # Group By: Customer Type Distribution
    df_Customer_Type_Distribution = df_Sort_By_Customer_Type.groupBy('customer_type').agg(count(lit(1)).alias('type_customer_count'))

    # Step: Gender Distribution (GroupBy) [converted]
    # Group By: Gender Distribution
    df_Gender_Distribution = df_Sort_By_Gender.groupBy('gender_proxy').agg(count(lit(1)).alias('gender_customer_count'))

    # Step: Write Profiling Report (TextFileOutput) [converted]
    # Pentaho step: Write Profiling Report (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/Customer_Profiling_Report_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='report_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_profiled' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='null_email_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='null_phone_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='null_country_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='null_dob_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='duplicate_key_rows' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Profiling_Report = df_Tag_Profile_Report
    _out_df_Write_Profiling_Report = df_Write_Profiling_Report.select('report_name', 'rows_profiled', 'null_email_count', 'null_phone_count', 'null_country_count', 'null_dob_count', 'duplicate_key_rows', 'run_id', 'status')
    writer = _out_df_Write_Profiling_Report.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/Customer_Profiling_Report_.csv')

    # Step: Write Country Distribution (TextFileOutput) [converted]
    # Pentaho step: Write Country Distribution (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/customer_profile_country_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='country_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='country_customer_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Country_Distribution = df_Country_Distribution
    _out_df_Write_Country_Distribution = df_Write_Country_Distribution.select('country_code', 'country_customer_count')
    writer = _out_df_Write_Country_Distribution.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/customer_profile_country_.csv')

    # Step: Write Customer Type Distribution (TextFileOutput) [converted]
    # Pentaho step: Write Customer Type Distribution (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/customer_profile_type_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='customer_type' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='type_customer_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Customer_Type_Distribution = df_Customer_Type_Distribution
    _out_df_Write_Customer_Type_Distribution = df_Write_Customer_Type_Distribution.select('customer_type', 'type_customer_count')
    writer = _out_df_Write_Customer_Type_Distribution.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/customer_profile_type_.csv')

    # Step: Write Gender Distribution (TextFileOutput) [converted]
    # Pentaho step: Write Gender Distribution (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/customer_profile_gender_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='gender_proxy' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gender_customer_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Gender_Distribution = df_Gender_Distribution
    _out_df_Write_Gender_Distribution = df_Write_Gender_Distribution.select('gender_proxy', 'gender_customer_count')
    writer = _out_df_Write_Gender_Distribution.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/customer_profile_gender_.csv')

    # Step: Write Profiling JSON (JsonOutput) [converted]
    # Pentaho step: Write Profiling JSON (type: JsonOutput)
    df_Write_Profiling_JSON = df_Write_Profiling_Report
    df_Write_Profiling_JSON.write \
    .mode('overwrite') \
    .json(
        f'{data_dir}/Customer_Profiling_Report_.json'
    )
    # preserved.json_bloc='rows' output_value='json_blob'

    # Step: Profile Complete (Dummy) [converted]
    # Dummy: Profile Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Profile_Complete = df_Write_Profiling_JSON

    log_event(_LOG, "transformation_end")
    return df_Dummy_Profile_Complete
