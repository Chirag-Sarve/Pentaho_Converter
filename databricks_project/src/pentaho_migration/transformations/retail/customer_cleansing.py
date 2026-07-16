"""PySpark module migrated from Pentaho transformation: TR_Customer_Cleansing.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/cleansing/Customer_Cleansing.ktr
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
    trim,
    upper,
    when,
    coalesce,
    row_number,
    max as _max,
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

_LOG = get_logger("pentaho_migration.transformations.retail.customer_cleansing")

from pyspark.sql.window import Window


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Customer_Cleansing`` step-for-step."""
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

    # Step: Read Validated Customers (CsvInput) [converted]
    # CSV Input: Read Validated Customers
    df_Read_Validated_Customers = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('customer_id STRING, first_name STRING, last_name STRING, email STRING, phone STRING, address_line1 STRING, address_line2 STRING, city STRING, state_province STRING, postal_code STRING, country_code STRING, country_name STRING, preferred_currency STRING, loyalty_tier STRING, registration_date STRING, date_of_birth STRING, is_active STRING, source_row_num INT, batch_id STRING, run_id STRING, validation_status STRING')
        .load(f'{data_dir}/customers_validated_.csv')
    )

    # Step: Read customers.csv Fallback (CsvInput) [converted]
    # CSV Input: Read customers.csv Fallback
    df_Read_customers.csv_Fallback = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('customer_id STRING, first_name STRING, last_name STRING, email STRING, phone STRING, address_line1 STRING, address_line2 STRING, city STRING, state_province STRING, postal_code STRING, country_code STRING, country_name STRING, preferred_currency STRING, loyalty_tier STRING, registration_date STRING, date_of_birth STRING, is_active STRING')
        .load('/customers.csv')
    )

    # Step: Unify Cleanse Input (Dummy) [converted]
    # Dummy: Unify Cleanse Input
    # Pass-through step - DataFrame unchanged
    df_Dummy_Unify_Cleanse_Input = df_Read_Validated_Customers

    # Step: Trim Spaces (StringOperations) [converted]
    # String Operations: Trim Spaces
    df_Trim_Spaces = df_Dummy_Unify_Cleanse_Input
    df_Trim_Spaces = df_Trim_Spaces.withColumn("customer_id", trim(col("customer_id").cast("string")))
    df_Trim_Spaces = df_Trim_Spaces.withColumn("first_name", trim(col("first_name").cast("string")))
    df_Trim_Spaces = df_Trim_Spaces.withColumn("last_name", trim(col("last_name").cast("string")))
    df_Trim_Spaces = df_Trim_Spaces.withColumn("email", trim(col("email").cast("string")))
    df_Trim_Spaces = df_Trim_Spaces.withColumn("phone", trim(col("phone").cast("string")))
    df_Trim_Spaces = df_Trim_Spaces.withColumn("address_line1", trim(col("address_line1").cast("string")))
    df_Trim_Spaces = df_Trim_Spaces.withColumn("address_line2", trim(col("address_line2").cast("string")))
    df_Trim_Spaces = df_Trim_Spaces.withColumn("city", trim(col("city").cast("string")))
    df_Trim_Spaces = df_Trim_Spaces.withColumn("state_province", trim(col("state_province").cast("string")))
    df_Trim_Spaces = df_Trim_Spaces.withColumn("postal_code", trim(col("postal_code").cast("string")))
    df_Trim_Spaces = df_Trim_Spaces.withColumn("country_code", trim(col("country_code").cast("string")))
    df_Trim_Spaces = df_Trim_Spaces.withColumn("country_name", trim(col("country_name").cast("string")))
    df_Trim_Spaces = df_Trim_Spaces.withColumn("preferred_currency", trim(col("preferred_currency").cast("string")))
    df_Trim_Spaces = df_Trim_Spaces.withColumn("loyalty_tier", trim(col("loyalty_tier").cast("string")))
    df_Trim_Spaces = df_Trim_Spaces.withColumn("registration_date", trim(col("registration_date").cast("string")))
    df_Trim_Spaces = df_Trim_Spaces.withColumn("date_of_birth", trim(col("date_of_birth").cast("string")))

    # Step: Uppercase Names (StringOperations) [converted]
    # String Operations: Uppercase Names
    df_Uppercase_Names = df_Trim_Spaces
    df_Uppercase_Names = df_Uppercase_Names.withColumn("first_name", upper(col("first_name").cast("string")))
    df_Uppercase_Names = df_Uppercase_Names.withColumn("last_name", upper(col("last_name").cast("string")))
    df_Uppercase_Names = df_Uppercase_Names.withColumn("city", upper(col("city").cast("string")))
    df_Uppercase_Names = df_Uppercase_Names.withColumn("country_name", upper(col("country_name").cast("string")))
    df_Uppercase_Names = df_Uppercase_Names.withColumn("state_province", upper(col("state_province").cast("string")))

    # Step: Lowercase Emails (StringOperations) [converted]
    # String Operations: Lowercase Emails
    df_Lowercase_Emails = df_Uppercase_Names
    df_Lowercase_Emails = df_Lowercase_Emails.withColumn("email", lower(col("email").cast("string")))

    # Step: Normalize Phone Numbers (ReplaceString) [partial]
    # ReplaceString: Normalize Phone Numbers
    df_Normalize_Phone_Numbers = df_Lowercase_Emails

    # Step: Empty Strings To Null (NullIf) [converted]
    # Null If: Empty Strings To Null
    # preserved.fields=[{'name': 'address_line2', 'value': '', 'type': ''}, {'name': 'state_province', 'value': '', 'type': ''}, {'name': 'phone', 'value': '', 'type': ''}]
    df_Empty_Strings_To_Null = df_Normalize_Phone_Numbers
    df_Empty_Strings_To_Null = df_Empty_Strings_To_Null.withColumn('address_line2', when((col('address_line2').isNull() | (col('address_line2').cast('string') == lit(''))), lit(None)).otherwise(col('address_line2')))
    df_Empty_Strings_To_Null = df_Empty_Strings_To_Null.withColumn('state_province', when((col('state_province').isNull() | (col('state_province').cast('string') == lit(''))), lit(None)).otherwise(col('state_province')))
    df_Empty_Strings_To_Null = df_Empty_Strings_To_Null.withColumn('phone', when((col('phone').isNull() | (col('phone').cast('string') == lit(''))), lit(None)).otherwise(col('phone')))

    # Step: Replace NULLs (IfNull) [converted]
    # If Field Value Is Null: Replace NULLs
    df_Replace_NULLs = df_Empty_Strings_To_Null
    df_Replace_NULLs = df_Replace_NULLs.withColumn('address_line2', when(col('address_line2').isNull(), lit('N/A')).otherwise(col('address_line2')))
    df_Replace_NULLs = df_Replace_NULLs.withColumn('state_province', when(col('state_province').isNull(), lit('UNKNOWN')).otherwise(col('state_province')))
    df_Replace_NULLs = df_Replace_NULLs.withColumn('phone', when(col('phone').isNull(), lit(0)).otherwise(col('phone')))
    df_Replace_NULLs = df_Replace_NULLs.withColumn('preferred_currency', when(col('preferred_currency').isNull(), lit('USD')).otherwise(col('preferred_currency')))
    df_Replace_NULLs = df_Replace_NULLs.withColumn('loyalty_tier', when(col('loyalty_tier').isNull(), lit('Bronze')).otherwise(col('loyalty_tier')))

    # Step: Standardize Countries (ValueMapper) [converted]
    # Value Mapper: Standardize Countries
    df_Standardize_Countries = df_Replace_NULLs.withColumn("country_code_std", when((lower(col("country_code")) == lower(lit('USA'))), lit('US')).when((lower(col("country_code")) == lower(lit('UK'))), lit('GB')).when((lower(col("country_code")) == lower(lit('GBR'))), lit('GB')).when((lower(col("country_code")) == lower(lit('DEU'))), lit('DE')).when((lower(col("country_code")) == lower(lit('FRA'))), lit('FR')).when((col("country_code").isNull() | (col("country_code") == lit(''))), col("country_code")).otherwise(lit(None)))
    # preserved.case_sensitive=False mappings=5 default=''

    # Step: Apply Country Standardization (Formula) [converted]
    # Formula: Apply Country Standardization
    df_Apply_Country_Standardization = df_Standardize_Countries
    df_Apply_Country_Standardization = df_Apply_Country_Standardization.withColumn('formula_result', lit(None))  # empty formula

    # Step: Remove Invalid Characters (ReplaceString) [partial]
    # ReplaceString: Remove Invalid Characters
    df_Remove_Invalid_Characters = df_Apply_Country_Standardization

    # Step: Rename Cleansed City (SelectValues) [converted]
    # Select Values: Rename Cleansed City
    df_Rename_Cleansed_City = df_Remove_Invalid_Characters.select(col("city_std").alias("city"), col("customer_id").alias("customer_id"), col("first_name").alias("first_name"), col("last_name").alias("last_name"), col("email").alias("email"), col("phone").alias("phone"), col("address_line1").alias("address_line1"), col("address_line2").alias("address_line2"), col("state_province").alias("state_province"), col("postal_code").alias("postal_code"), col("country_code").alias("country_code"), col("country_name").alias("country_name"), col("preferred_currency").alias("preferred_currency"), col("loyalty_tier").alias("loyalty_tier"), col("registration_date").alias("registration_date"), col("date_of_birth").alias("date_of_birth"), col("is_active").alias("is_active"), col("source_row_num").alias("source_row_num"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"))

    # Step: Sort For Dedupe (SortRows) [converted]
    # Sort Rows: Sort For Dedupe
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_For_Dedupe = df_Rename_Cleansed_City
    _sort_df_Sort_For_Dedupe = _sort_df_Sort_For_Dedupe.withColumn("_sort_ci_customer_id", lower(col("customer_id").cast("string")))
    _sort_df_Sort_For_Dedupe = _sort_df_Sort_For_Dedupe.withColumn("_sort_ci_email", lower(col("email").cast("string")))
    df_Sort_For_Dedupe = _sort_df_Sort_For_Dedupe.orderBy(col("_sort_ci_customer_id").asc_nulls_last(), col("_sort_ci_email").asc_nulls_last())
    df_Sort_For_Dedupe = df_Sort_For_Dedupe.drop("_sort_ci_customer_id", "_sort_ci_email")

    # Step: Remove Duplicate Customers (Unique) [converted]
    # Unique Rows: Remove Duplicate Customers
    # preserved.reject_duplicate_row=N error_description=''
    # Unique Rows expects sorted input in Pentaho; Spark dropDuplicates is order-independent
    # preserved.count_rows=True count_field='dup_count' compare_fields=['customer_id']
    df_Remove_Duplicate_Customers = df_Sort_For_Dedupe
    _w_cnt_df_Remove_Duplicate_Customers = Window.partitionBy(col("customer_id"))
    df_Remove_Duplicate_Customers = df_Remove_Duplicate_Customers.withColumn("dup_count", count(lit(1)).over(_w_cnt_df_Remove_Duplicate_Customers))
    _w_rn_df_Remove_Duplicate_Customers = Window.partitionBy(col("customer_id")).orderBy(monotonically_increasing_id())
    df_Remove_Duplicate_Customers = df_Remove_Duplicate_Customers.withColumn('_uniq_rn', row_number().over(_w_rn_df_Remove_Duplicate_Customers))
    df_Remove_Duplicate_Customers = df_Remove_Duplicate_Customers.filter(col('_uniq_rn') == 1).drop('_uniq_rn')

    # Step: Cleansing Statistics (MemoryGroupBy) [converted]
    # Memory Group By: Cleansing Statistics
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Cleansing_Statistics = df_Remove_Duplicate_Customers.groupBy().agg(count(lit(1)).alias('rows_cleansed'), _max(col("dup_count")).alias('max_dup_count'))

    # Step: Write Cleansed Customers (TextFileOutput) [converted]
    # Pentaho step: Write Cleansed Customers (type: TextFileOutput)
    # Pentaho filename: /output/customer/cleansed/customers_cleansed_
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
    # INFO: preserved.field_format name='dup_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Cleansed_Customers = df_Remove_Duplicate_Customers
    _out_df_Write_Cleansed_Customers = df_Write_Cleansed_Customers.select('customer_id', 'first_name', 'last_name', 'email', 'phone', 'address_line1', 'address_line2', 'city', 'state_province', 'postal_code', 'country_code', 'country_name', 'preferred_currency', 'loyalty_tier', 'registration_date', 'date_of_birth', 'is_active', 'source_row_num', 'batch_id', 'run_id', 'dup_count')
    writer = _out_df_Write_Cleansed_Customers.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/customers_cleansed_.csv')

    # Step: Write Deduped Rejects (TextFileOutput) [converted]
    # Pentaho step: Write Deduped Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/customer/customers_dedupe_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='customer_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='email' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='dup_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Deduped_Rejects = df_Remove_Duplicate_Customers
    _out_df_Write_Deduped_Rejects = df_Write_Deduped_Rejects.select('customer_id', 'email', 'dup_count', 'batch_id', 'run_id')
    writer = _out_df_Write_Deduped_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/customers_dedupe_rejects_.csv')

    # Step: Tag Cleansing Report (Constant) [converted]
    # Add Constants: Tag Cleansing Report
    df_Tag_Cleansing_Report = df_Cleansing_Statistics
    df_Tag_Cleansing_Report = df_Tag_Cleansing_Report.withColumn("report_name", lit('Customer_Cleansing_Report'))
    # preserved.report_name: length='-1', precision='-1'
    df_Tag_Cleansing_Report = df_Tag_Cleansing_Report.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'
    df_Tag_Cleansing_Report = df_Tag_Cleansing_Report.withColumn("status", lit('SUCCESS'))
    # preserved.status: length='-1', precision='-1'

    # Step: Write Cleansed Staging Table (TableOutput) [converted]
    # Pentaho step: Write Cleansed Staging Table (type: TableOutput) (Pentaho schema: retail_stg)
    # Mode: append (Pentaho truncate=N)
    _mapped_df_Write_Cleansed_Staging_Table = df_Write_Cleansed_Customers.select(col('customer_id'), col('first_name'), col('last_name'), col('email'), col('phone'), col('address_line1'), col('address_line2'), col('city'), col('state_province'), col('postal_code'), col('country_code'), col('country_name'), col('preferred_currency'), col('loyalty_tier'), col('registration_date'), col('date_of_birth'), col('is_active'), col('batch_id'), col('run_id'))
    df_Write_Cleansed_Staging_Table = _mapped_df_Write_Cleansed_Staging_Table
    write_delta(
        df_Write_Cleansed_Staging_Table,
        f"{catalog}.{schema}.stg_cln_customers",
        mode='append',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.stg_cln_customers", mode='append')

    # Step: Write Cleansing Report (TextFileOutput) [converted]
    # Pentaho step: Write Cleansing Report (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/Customer_Cleansing_Report_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='report_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_cleansed' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='max_dup_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Cleansing_Report = df_Tag_Cleansing_Report
    _out_df_Write_Cleansing_Report = df_Write_Cleansing_Report.select('report_name', 'rows_cleansed', 'max_dup_count', 'run_id', 'status')
    writer = _out_df_Write_Cleansing_Report.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/Customer_Cleansing_Report_.csv')

    # Step: Cleansing Complete (Dummy) [converted]
    # Dummy: Cleansing Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Cleansing_Complete = df_Write_Cleansed_Staging_Table

    # Step: Log Cleansing Complete (WriteToLog) [converted]
    # Write to Log: Log Cleansing Complete
    # preserved.log_level='Basic'
    # preserved.log_message='CLEANSE | TRANS=TR_Customer_Cleansing | RUN_ID=${RUN_ID} | STATUS=SUCCESS'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Cleansing_Complete = logging.getLogger('pentaho.writetolog.Log_Cleansing_Complete')
    _log_df_Log_Cleansing_Complete.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Cleansing_Complete = df_Write_Cleansing_Report
    _log_rows_df_Log_Cleansing_Complete = _log_df_df_Log_Cleansing_Complete.take(20)
    _log_df_Log_Cleansing_Complete.info('Log Cleansing Complete' + ' | columns=' + str(_log_df_df_Log_Cleansing_Complete.columns))
    _log_df_Log_Cleansing_Complete.info('CLEANSE | TRANS=TR_Customer_Cleansing | RUN_ID=${RUN_ID} | STATUS=SUCCESS')
    for _lr in _log_rows_df_Log_Cleansing_Complete:
        _log_df_Log_Cleansing_Complete.info('Log Cleansing Complete' + ' | ' + str(_lr.asDict()))
    df_Log_Cleansing_Complete = df_Write_Cleansing_Report

    log_event(_LOG, "transformation_end")
    return df_Log_Cleansing_Complete
