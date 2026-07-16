"""PySpark module migrated from Pentaho transformation: TR_Customer_Validation.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/validation/TR_Customer_Validation.ktr
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_customer_validation")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Customer_Validation`` step-for-step."""
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
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'reject_path', 'variable': '${REJECT_PATH}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
    # preserved.output_columns=['batch_id', 'run_id', 'reject_path']
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
    # field 'reject_path' from variable string '${REJECT_PATH}'
    # preserved.field.reject_path.trim_type='none'
    # preserved.field.reject_path.type='String'
    _reject_path_resolved = None
    _dbu__reject_path_resolved = globals().get('dbutils')
    if _dbu__reject_path_resolved is not None and hasattr(_dbu__reject_path_resolved, 'widgets'):
        try:
            _reject_path_resolved = _dbu__reject_path_resolved.widgets.get('REJECT_PATH')
        except Exception:
            _reject_path_resolved = None
    if _reject_path_resolved in (None, ''):
        import os as _os__reject_path_resolved
        _reject_path_resolved = _os__reject_path_resolved.environ.get('REJECT_PATH')
    if _reject_path_resolved in (None, ''):
        try:
            _reject_path_resolved = spark.conf.get('pentaho.var.REJECT_PATH')
        except Exception:
            _reject_path_resolved = None
    if _reject_path_resolved in (None, ''):
        _reject_path_resolved = '${PROJECT_HOME}/rejects'
    if _reject_path_resolved is None:
        _reject_path_resolved = ''
    df_Get_Variables = df_Get_Variables.withColumn('reject_path', lit(_reject_path_resolved))

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

    # Step: Add Validation Batch (Constant) [converted]
    # Add Constants: Add Validation Batch
    df_Add_Validation_Batch = df_Read_customers.csv
    df_Add_Validation_Batch = df_Add_Validation_Batch.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Add_Validation_Batch = df_Add_Validation_Batch.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'
    df_Add_Validation_Batch = df_Add_Validation_Batch.withColumn("validation_status", lit('PENDING'))
    # preserved.validation_status: length='-1', precision='-1'

    # Step: Trim Key Fields (StringOperations) [converted]
    # String Operations: Trim Key Fields
    df_Trim_Key_Fields = df_Add_Validation_Batch
    df_Trim_Key_Fields = df_Trim_Key_Fields.withColumn("customer_id", trim(col("customer_id").cast("string")))
    df_Trim_Key_Fields = df_Trim_Key_Fields.withColumn("email", trim(col("email").cast("string")))
    df_Trim_Key_Fields = df_Trim_Key_Fields.withColumn("phone", trim(col("phone").cast("string")))
    df_Trim_Key_Fields = df_Trim_Key_Fields.withColumn("country_code", upper(trim(col("country_code").cast("string"))))
    df_Trim_Key_Fields = df_Trim_Key_Fields.withColumn("postal_code", trim(col("postal_code").cast("string")))

    # Step: Validate Customer ID Pattern (RegexEval) [converted]
    # Regex Evaluation: Validate Customer ID Pattern
    # preserved.matcher='customer_id'
    # preserved.pattern='^CUS[0-9]{6}$'
    # preserved.result_field='result'
    # preserved.use_variable_interpolation=False
    # preserved.allow_capture_groups=False
    # preserved.replace_fields=True
    # preserved.case_insensitive=False
    # preserved.canon_eq=False
    # preserved.comment=False
    # preserved.dotall=False
    # preserved.multiline=False
    # preserved.unicode=False
    # preserved.unix_lines=False
    # NOTE: replacefields=Y — capture groups overwrite same-named inbound columns (Spark withColumn always overwrites by name)
    df_Validate_Customer_ID_Pattern = df_Trim_Key_Fields
    df_Validate_Customer_ID_Pattern = df_Validate_Customer_ID_Pattern.withColumn('result', when(col('customer_id').rlike('^CUS[0-9]{6}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Email Pattern (RegexEval) [converted]
    # Regex Evaluation: Validate Email Pattern
    # preserved.matcher='email'
    # preserved.pattern='^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
    # preserved.result_field='result'
    # preserved.use_variable_interpolation=False
    # preserved.allow_capture_groups=False
    # preserved.replace_fields=True
    # preserved.case_insensitive=False
    # preserved.canon_eq=False
    # preserved.comment=False
    # preserved.dotall=False
    # preserved.multiline=False
    # preserved.unicode=False
    # preserved.unix_lines=False
    # NOTE: replacefields=Y — capture groups overwrite same-named inbound columns (Spark withColumn always overwrites by name)
    df_Validate_Email_Pattern = df_Validate_Customer_ID_Pattern
    df_Validate_Email_Pattern = df_Validate_Email_Pattern.withColumn('result', when(col('email').rlike('^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Phone Pattern (RegexEval) [converted]
    # Regex Evaluation: Validate Phone Pattern
    # preserved.matcher='phone'
    # preserved.pattern='^\\+?[0-9\\-\\s()]{7,20}$'
    # preserved.result_field='result'
    # preserved.use_variable_interpolation=False
    # preserved.allow_capture_groups=False
    # preserved.replace_fields=True
    # preserved.case_insensitive=False
    # preserved.canon_eq=False
    # preserved.comment=False
    # preserved.dotall=False
    # preserved.multiline=False
    # preserved.unicode=False
    # preserved.unix_lines=False
    # NOTE: replacefields=Y — capture groups overwrite same-named inbound columns (Spark withColumn always overwrites by name)
    df_Validate_Phone_Pattern = df_Validate_Email_Pattern
    df_Validate_Phone_Pattern = df_Validate_Phone_Pattern.withColumn('result', when(col('phone').rlike('^\\+?[0-9\\-\\s()]{7,20}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Country Code (RegexEval) [converted]
    # Regex Evaluation: Validate Country Code
    # preserved.matcher='country_code'
    # preserved.pattern='^[A-Z]{2}$'
    # preserved.result_field='result'
    # preserved.use_variable_interpolation=False
    # preserved.allow_capture_groups=False
    # preserved.replace_fields=True
    # preserved.case_insensitive=False
    # preserved.canon_eq=False
    # preserved.comment=False
    # preserved.dotall=False
    # preserved.multiline=False
    # preserved.unicode=False
    # preserved.unix_lines=False
    # NOTE: replacefields=Y — capture groups overwrite same-named inbound columns (Spark withColumn always overwrites by name)
    df_Validate_Country_Code = df_Validate_Phone_Pattern
    df_Validate_Country_Code = df_Validate_Country_Code.withColumn('result', when(col('country_code').rlike('^[A-Z]{2}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Postal Code (RegexEval) [converted]
    # Regex Evaluation: Validate Postal Code
    # preserved.matcher='postal_code'
    # preserved.pattern='^[A-Za-z0-9 \\-]{3,12}$'
    # preserved.result_field='result'
    # preserved.use_variable_interpolation=False
    # preserved.allow_capture_groups=False
    # preserved.replace_fields=True
    # preserved.case_insensitive=False
    # preserved.canon_eq=False
    # preserved.comment=False
    # preserved.dotall=False
    # preserved.multiline=False
    # preserved.unicode=False
    # preserved.unix_lines=False
    # NOTE: replacefields=Y — capture groups overwrite same-named inbound columns (Spark withColumn always overwrites by name)
    df_Validate_Postal_Code = df_Validate_Country_Code
    df_Validate_Postal_Code = df_Validate_Postal_Code.withColumn('result', when(col('postal_code').rlike('^[A-Za-z0-9 \\-]{3,12}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Birth Date (RegexEval) [converted]
    # Regex Evaluation: Validate Birth Date
    # preserved.matcher='date_of_birth'
    # preserved.pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    # preserved.result_field='result'
    # preserved.use_variable_interpolation=False
    # preserved.allow_capture_groups=False
    # preserved.replace_fields=True
    # preserved.case_insensitive=False
    # preserved.canon_eq=False
    # preserved.comment=False
    # preserved.dotall=False
    # preserved.multiline=False
    # preserved.unicode=False
    # preserved.unix_lines=False
    # NOTE: replacefields=Y — capture groups overwrite same-named inbound columns (Spark withColumn always overwrites by name)
    df_Validate_Birth_Date = df_Validate_Postal_Code
    df_Validate_Birth_Date = df_Validate_Birth_Date.withColumn('result', when(col('date_of_birth').rlike('^[0-9]{4}-[0-9]{2}-[0-9]{2}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Registration Date (RegexEval) [converted]
    # Regex Evaluation: Validate Registration Date
    # preserved.matcher='registration_date'
    # preserved.pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    # preserved.result_field='result'
    # preserved.use_variable_interpolation=False
    # preserved.allow_capture_groups=False
    # preserved.replace_fields=True
    # preserved.case_insensitive=False
    # preserved.canon_eq=False
    # preserved.comment=False
    # preserved.dotall=False
    # preserved.multiline=False
    # preserved.unicode=False
    # preserved.unix_lines=False
    # NOTE: replacefields=Y — capture groups overwrite same-named inbound columns (Spark withColumn always overwrites by name)
    df_Validate_Registration_Date = df_Validate_Birth_Date
    df_Validate_Registration_Date = df_Validate_Registration_Date.withColumn('result', when(col('registration_date').rlike('^[0-9]{4}-[0-9]{2}-[0-9]{2}$'), lit("Y")).otherwise(lit("N")))

    # Step: Data Validator Core Rules (Validator) [converted]
    # Data Validator: Data Validator Core Rules
    # preserved.validate_all=True
    # preserved.concat_errors=True
    # preserved.concat_separator='|'
    # WARNING: Data Validator has no validator_field rules
    df_Data_Validator_Core_Rules = df_Validate_Registration_Date

    # Step: Compose Validation Flag (Formula) [converted]
    # Formula: Compose Validation Flag
    df_Compose_Validation_Flag = df_Data_Validator_Core_Rules
    df_Compose_Validation_Flag = df_Compose_Validation_Flag.withColumn('formula_result', lit(None))  # empty formula

    # Step: Valid Row? (FilterRows) [failed]
    # Filter Rows: Valid Row?
    df_Mark_Valid = df_Compose_Validation_Flag.filter((col("is_valid_row") == lit('Y')))
    df_Mark_Reject = df_Compose_Validation_Flag.filter(~((col("is_valid_row") == lit('Y'))))
    df_Valid_Row? = df_Mark_Valid

    # Step: Mark Reject (Constant) [failed]
    # Add Constants: Mark Reject
    df_Mark_Reject = df_Valid_Row?
    df_Mark_Reject = df_Mark_Reject.withColumn("validation_status", lit('REJECTED'))
    # preserved.validation_status: length='-1', precision='-1'

    # Step: Mark Valid (Constant) [failed]
    # Add Constants: Mark Valid
    df_Mark_Valid = df_Valid_Row?
    df_Mark_Valid = df_Mark_Valid.withColumn("validation_status", lit('VALID'))
    # preserved.validation_status: length='-1', precision='-1'
    df_Mark_Valid = df_Mark_Valid.withColumn("reject_code", lit(None))
    # preserved.reject_code: length='-1', precision='-1'

    # Step: Prepare Reject Stream (SelectValues) [converted]
    # Select Values: Prepare Reject Stream
    df_Prepare_Reject_Stream = df_Mark_Reject.select(col("customer_id").alias("customer_id"), col("email").alias("email"), col("phone").alias("phone"), col("country_code").alias("country_code"), col("postal_code").alias("postal_code"), col("date_of_birth").alias("date_of_birth"), col("registration_date").alias("registration_date"), col("reject_reason").alias("_reject_reason"), col("batch_id").alias("_batch_id"), col("run_id").alias("run_id"), col("source_row_num").alias("_source_row_num"))

    # Step: Count Validation Outcomes (MemoryGroupBy) [converted]
    # Memory Group By: Count Validation Outcomes
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Validation_Outcomes = df_Mark_Valid.groupBy('validation_status').agg(count(lit(1)).alias('row_count'))

    # Step: Prepare Valid Stream (SelectValues) [converted]
    # Select Values: Prepare Valid Stream
    df_Prepare_Valid_Stream = df_Mark_Valid.select(col("customer_id").alias("customer_id"), col("first_name").alias("first_name"), col("last_name").alias("last_name"), col("email").alias("email"), col("phone").alias("phone"), col("address_line1").alias("address_line1"), col("address_line2").alias("address_line2"), col("city").alias("city"), col("state_province").alias("state_province"), col("postal_code").alias("postal_code"), col("country_code").alias("country_code"), col("country_name").alias("country_name"), col("preferred_currency").alias("preferred_currency"), col("loyalty_tier").alias("loyalty_tier"), col("registration_date").alias("registration_date"), col("date_of_birth").alias("date_of_birth"), col("is_active").alias("is_active"), col("source_row_num").alias("source_row_num"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"), col("validation_status").alias("validation_status"))

    # Step: Map Reject Codes (ValueMapper) [converted]
    # Value Mapper: Map Reject Codes
    df_Map_Reject_Codes = df_Prepare_Reject_Stream.withColumn("_reject_code", when((lower(col("reject_reason")) == lower(lit('RK_NULL_BK'))), lit('RK_NULL_BK')).when((lower(col("reject_reason")) == lower(lit('RK_BAD_EMAIL'))), lit('RK_BAD_EMAIL')).when((lower(col("reject_reason")) == lower(lit('RK_BAD_PHONE'))), lit('RK_BAD_PHONE')).when((lower(col("reject_reason")) == lower(lit('RK_BAD_ENUM:country'))), lit('RK_BAD_ENUM')).when((lower(col("reject_reason")) == lower(lit('RK_BAD_POSTAL'))), lit('RK_BAD_TYPE')).when((lower(col("reject_reason")) == lower(lit('RK_BAD_DATE:birth'))), lit('RK_BAD_DATE')).when((lower(col("reject_reason")) == lower(lit('RK_BAD_DATE:registration'))), lit('RK_BAD_DATE')).when((col("reject_reason").isNull() | (col("reject_reason") == lit(''))), col("reject_reason")).otherwise(lit('RK_INVALID')))
    # preserved.case_sensitive=False mappings=7 default='RK_INVALID'

    # Step: Write Validation Log (TextFileOutput) [converted]
    # Pentaho step: Write Validation Log (type: TextFileOutput)
    # Pentaho filename: /logs/execution/customer/TR_Customer_Validation_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='validation_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='row_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Validation_Log = df_Count_Validation_Outcomes
    _out_df_Write_Validation_Log = df_Write_Validation_Log.select('validation_status', 'row_count')
    writer = _out_df_Write_Validation_Log.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/TR_Customer_Validation_.log')

    # Step: Write Validated Customers (TextFileOutput) [converted]
    # Pentaho step: Write Validated Customers (type: TextFileOutput)
    # Pentaho filename: /output/customer/validated/customers_validated_
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
    # INFO: preserved.field_format name='validation_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Validated_Customers = df_Prepare_Valid_Stream
    _out_df_Write_Validated_Customers = df_Write_Validated_Customers.select('customer_id', 'first_name', 'last_name', 'email', 'phone', 'address_line1', 'address_line2', 'city', 'state_province', 'postal_code', 'country_code', 'country_name', 'preferred_currency', 'loyalty_tier', 'registration_date', 'date_of_birth', 'is_active', 'source_row_num', 'batch_id', 'run_id', 'validation_status')
    writer = _out_df_Write_Validated_Customers.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/customers_validated_.csv')

    # Step: Write Customer Reject File (TextFileOutput) [converted]
    # Pentaho step: Write Customer Reject File (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/customer/customers_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='customer_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='email' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='phone' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='country_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='postal_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='date_of_birth' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='registration_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='_reject_reason' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='_reject_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='_source_row_num' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='_batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Customer_Reject_File = df_Map_Reject_Codes
    _out_df_Write_Customer_Reject_File = df_Write_Customer_Reject_File.select('customer_id', 'email', 'phone', 'country_code', 'postal_code', 'date_of_birth', 'registration_date', '_reject_reason', '_reject_code', '_source_row_num', '_batch_id')
    writer = _out_df_Write_Customer_Reject_File.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/customers_rejects_.csv')

    # Step: Write Valid Staging Table (TableOutput) [converted]
    # Pentaho step: Write Valid Staging Table (type: TableOutput) (Pentaho schema: retail_stg)
    # Mode: append (Pentaho truncate=N)
    _mapped_df_Write_Valid_Staging_Table = df_Write_Validated_Customers.select(col('customer_id'), col('first_name'), col('last_name'), col('email'), col('phone'), col('address_line1'), col('address_line2'), col('city'), col('state_province'), col('postal_code'), col('country_code'), col('country_name'), col('preferred_currency'), col('loyalty_tier'), col('registration_date'), col('date_of_birth'), col('is_active'), col('source_row_num'), col('batch_id'), col('run_id'), col('validation_status'))
    df_Write_Valid_Staging_Table = _mapped_df_Write_Valid_Staging_Table
    write_delta(
        df_Write_Valid_Staging_Table,
        f"{catalog}.{schema}.stg_val_customers",
        mode='append',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.stg_val_customers", mode='append')

    # Step: Log Rejected Row (WriteToLog) [converted]
    # Write to Log: Log Rejected Row
    # preserved.log_level='Error'
    # preserved.log_message='REJECT | TRANS=TR_Customer_Validation | customer_id=${customer_id} | reason=${reject_reason} | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Rejected_Row = logging.getLogger('pentaho.writetolog.Log_Rejected_Row')
    _log_df_Log_Rejected_Row.setLevel(logging.ERROR)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Rejected_Row = df_Write_Customer_Reject_File
    _log_rows_df_Log_Rejected_Row = _log_df_df_Log_Rejected_Row.take(20)
    _log_df_Log_Rejected_Row.error('Log Rejected Row' + ' | columns=' + str(_log_df_df_Log_Rejected_Row.columns))
    _log_df_Log_Rejected_Row.error('REJECT | TRANS=TR_Customer_Validation | customer_id=${customer_id} | reason=${reject_reason} | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Rejected_Row:
        _log_df_Log_Rejected_Row.error('Log Rejected Row' + ' | ' + str(_lr.asDict()))
    df_Log_Rejected_Row = df_Write_Customer_Reject_File

    # Step: Validation Complete (Dummy) [converted]
    # Dummy: Validation Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Validation_Complete = df_Write_Valid_Staging_Table

    log_event(_LOG, "transformation_end")
    return df_Dummy_Validation_Complete
