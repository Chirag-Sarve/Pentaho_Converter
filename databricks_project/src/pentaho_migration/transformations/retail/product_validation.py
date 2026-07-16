"""PySpark module migrated from Pentaho transformation: TR_Product_Validation.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/validation/Product_Validation.ktr
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

_LOG = get_logger("pentaho_migration.transformations.retail.product_validation")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Product_Validation`` step-for-step."""
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

    # Step: Read Staged Products (CsvInput) [converted]
    # CSV Input: Read Staged Products
    df_Read_Staged_Products = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('product_id STRING, sku STRING, product_name STRING, category_id STRING, supplier_id STRING, brand STRING, unit_cost STRING, unit_price STRING, currency_code STRING, weight_kg STRING, is_active STRING, created_date STRING, description STRING, upc STRING, barcode STRING, length_cm STRING, width_cm STRING, height_cm STRING, status STRING, source_row_num INT, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/stg_raw_products_.csv')
    )

    # Step: Read Validation Policy JSON (JsonInput) [converted]
    # JSON Input: Read Validation Policy JSON
    df_Read_Validation_Policy_JSON = spark.read.format('json').option('multiline', 'true').load('${PROJECT_HOME}/metadata/rules/product_validation_policy.json')

    # Step: Read Validation Rules XML (getXMLData) [converted]
    # XML Input: Read Validation Rules XML
    df_Read_Validation_Rules_XML = spark.read.format('xml').option('rowTag', 'row').load('${PROJECT_HOME}/metadata/rules/product_validation_rules.xml')

    # Step: Write Validator Error Rejects (TextFileOutput) [converted]
    # Pentaho step: Write Validator Error Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/product/products_validator_errs_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sku' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_CODE' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_DESC' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_FIELDS' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Validator_Error_Rejects = df_Write_Validator_Error_Rejects
    _out_df_Write_Validator_Error_Rejects = df_Write_Validator_Error_Rejects.select('product_id', 'sku', 'ERR_CODE', 'ERR_DESC', 'ERR_FIELDS', 'batch_id', 'run_id')
    writer = _out_df_Write_Validator_Error_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/products_validator_errs_.csv')

    # Step: Add Validation Batch (Constant) [converted]
    # Add Constants: Add Validation Batch
    df_Add_Validation_Batch = df_Read_Staged_Products
    df_Add_Validation_Batch = df_Add_Validation_Batch.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Add_Validation_Batch = df_Add_Validation_Batch.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'
    df_Add_Validation_Batch = df_Add_Validation_Batch.withColumn("validation_status", lit('PENDING'))
    # preserved.validation_status: length='-1', precision='-1'

    # Step: Trim Validation Keys (StringOperations) [converted]
    # String Operations: Trim Validation Keys
    df_Trim_Validation_Keys = df_Add_Validation_Batch
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("product_id", upper(trim(col("product_id").cast("string"))))
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("sku", upper(trim(col("sku").cast("string"))))
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("category_id", upper(trim(col("category_id").cast("string"))))
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("supplier_id", upper(trim(col("supplier_id").cast("string"))))
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("upc", trim(col("upc").cast("string")))
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("barcode", upper(trim(col("barcode").cast("string"))))
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("status", upper(trim(col("status").cast("string"))))
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("currency_code", upper(trim(col("currency_code").cast("string"))))

    # Step: Validate Product ID (RegexEval) [converted]
    # Regex Evaluation: Validate Product ID
    # preserved.matcher='product_id'
    # preserved.pattern='^PRD[0-9]{5}$'
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
    df_Validate_Product_ID = df_Trim_Validation_Keys
    df_Validate_Product_ID = df_Validate_Product_ID.withColumn('result', when(col('product_id').rlike('^PRD[0-9]{5}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Category ID (RegexEval) [converted]
    # Regex Evaluation: Validate Category ID
    # preserved.matcher='category_id'
    # preserved.pattern='^CAT[0-9]{3}$'
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
    df_Validate_Category_ID = df_Validate_Product_ID
    df_Validate_Category_ID = df_Validate_Category_ID.withColumn('result', when(col('category_id').rlike('^CAT[0-9]{3}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Supplier ID (RegexEval) [converted]
    # Regex Evaluation: Validate Supplier ID
    # preserved.matcher='supplier_id'
    # preserved.pattern='^SUP[0-9]{4}$'
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
    df_Validate_Supplier_ID = df_Validate_Category_ID
    df_Validate_Supplier_ID = df_Validate_Supplier_ID.withColumn('result', when(col('supplier_id').rlike('^SUP[0-9]{4}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate SKU (RegexEval) [converted]
    # Regex Evaluation: Validate SKU
    # preserved.matcher='sku'
    # preserved.pattern='^SKU-[A-Z]{3}-[0-9]{5}$'
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
    df_Validate_SKU = df_Validate_Supplier_ID
    df_Validate_SKU = df_Validate_SKU.withColumn('result', when(col('sku').rlike('^SKU-[A-Z]{3}-[0-9]{5}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate UPC (RegexEval) [converted]
    # Regex Evaluation: Validate UPC
    # preserved.matcher='upc'
    # preserved.pattern='^[0-9]{12}$'
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
    df_Validate_UPC = df_Validate_SKU
    df_Validate_UPC = df_Validate_UPC.withColumn('result', when(col('upc').rlike('^[0-9]{12}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Barcode (RegexEval) [converted]
    # Regex Evaluation: Validate Barcode
    # preserved.matcher='barcode'
    # preserved.pattern='^BC-SKU-[A-Z]{3}-[0-9]{5}$'
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
    df_Validate_Barcode = df_Validate_UPC
    df_Validate_Barcode = df_Validate_Barcode.withColumn('result', when(col('barcode').rlike('^BC-SKU-[A-Z]{3}-[0-9]{5}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Status (RegexEval) [converted]
    # Regex Evaluation: Validate Status
    # preserved.matcher='status'
    # preserved.pattern='^(ACTIVE|INACTIVE)$'
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
    df_Validate_Status = df_Validate_Barcode
    df_Validate_Status = df_Validate_Status.withColumn('result', when(col('status').rlike('^(ACTIVE|INACTIVE)$'), lit("Y")).otherwise(lit("N")))

    # Step: Cast Numeric Validation Fields (SelectValues) [converted]
    # Select Values: Cast Numeric Validation Fields
    df_Cast_Numeric_Validation_Fields = df_Validate_Status.select(col("product_id").alias("product_id"), col("sku").alias("sku"), col("category_id").alias("category_id"), col("supplier_id").alias("supplier_id"), col("unit_price").alias("unit_price"), col("unit_cost").alias("unit_cost"), col("weight_kg").alias("weight_kg"), col("length_cm").alias("length_cm"), col("width_cm").alias("width_cm"), col("height_cm").alias("height_cm"), col("upc").alias("upc"), col("barcode").alias("barcode"), col("status").alias("status"), col("product_id_valid").alias("product_id_valid"), col("category_id_valid").alias("category_id_valid"), col("supplier_id_valid").alias("supplier_id_valid"), col("sku_valid").alias("sku_valid"), col("upc_valid").alias("upc_valid"), col("barcode_valid").alias("barcode_valid"), col("status_valid").alias("status_valid"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"), col("source_row_num").alias("source_row_num"), col("product_name").alias("product_name"), col("brand").alias("brand"), col("currency_code").alias("currency_code"), col("is_active").alias("is_active"), col("created_date").alias("created_date"), col("description").alias("description"))

    # Step: Validate Price Range Bucket (NumberRange) [converted]
    # Number Range: Validate Price Range Bucket
    # Number Range semantics: lower_bound <= value < upper_bound (Pentaho NumberRangeRule)
    df_Validate_Price_Range_Bucket = df_Cast_Numeric_Validation_Fields.withColumn('price_range_ok', when(col("unit_price").isNull(), lit('N')).otherwise(when((col("unit_price").cast("double") >= lit(0.01)) & (col("unit_price").cast("double") < lit(1000000.0)), lit('Y')).otherwise(lit('N'))))
    # preserved.fallback='N' rules=1 lower_inclusive=True upper_inclusive=False

    # Step: Validate Weight Range Bucket (NumberRange) [converted]
    # Number Range: Validate Weight Range Bucket
    # Number Range semantics: lower_bound <= value < upper_bound (Pentaho NumberRangeRule)
    df_Validate_Weight_Range_Bucket = df_Validate_Price_Range_Bucket.withColumn('weight_range_ok', when(col("weight_kg").isNull(), lit('N')).otherwise(when((col("weight_kg").cast("double") >= lit(0.001)) & (col("weight_kg").cast("double") < lit(5000.0)), lit('Y')).otherwise(lit('N'))))
    # preserved.fallback='N' rules=1 lower_inclusive=True upper_inclusive=False

    # Step: Data Validator Product Rules (Validator) [converted]
    # Data Validator: Data Validator Product Rules
    # preserved.validate_all=True
    # preserved.concat_errors=True
    # preserved.concat_separator='|'
    # WARNING: Data Validator has no validator_field rules
    df_Data_Validator_Product_Rules = df_Validate_Weight_Range_Bucket

    # Step: Compose Validation Flag (Formula) [converted]
    # Formula: Compose Validation Flag
    df_Compose_Validation_Flag = df_Data_Validator_Product_Rules
    df_Compose_Validation_Flag = df_Compose_Validation_Flag.withColumn('formula_result', lit(None))  # empty formula

    # Step: Valid Product Row? (FilterRows) [failed]
    # Filter Rows: Valid Product Row?
    df_Write_Valid_Products = df_Compose_Validation_Flag.filter((col("is_valid_row") == lit('Y')))
    df_Route_Rejects = df_Compose_Validation_Flag.filter(~((col("is_valid_row") == lit('Y'))))
    df_Valid_Product_Row? = df_Write_Valid_Products

    # Step: Route Rejects (Constant) [failed]
    # Add Constants: Route Rejects
    df_Route_Rejects = df_Valid_Product_Row?
    df_Route_Rejects = df_Route_Rejects.withColumn("reject_layer", lit('VALIDATION'))
    # preserved.reject_layer: length='-1', precision='-1'

    # Step: Write Valid Products (TextFileOutput) [failed]
    # Pentaho step: Write Valid Products (type: TextFileOutput)
    # Pentaho filename: /output/product/validated/products_valid_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sku' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='category_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='brand' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unit_cost' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unit_price' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='currency_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='weight_kg' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_active' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='created_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='description' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='upc' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='barcode' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='length_cm' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='width_cm' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='height_cm' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='source_row_num' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Valid_Products = df_Valid_Product_Row?
    _out_df_Write_Valid_Products = df_Write_Valid_Products.select('product_id', 'sku', 'product_name', 'category_id', 'supplier_id', 'brand', 'unit_cost', 'unit_price', 'currency_code', 'weight_kg', 'is_active', 'created_date', 'description', 'upc', 'barcode', 'length_cm', 'width_cm', 'height_cm', 'status', 'batch_id', 'run_id', 'source_row_num')
    writer = _out_df_Write_Valid_Products.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/products_valid_.csv')

    # Step: Write Product Reject File (TextFileOutput) [converted]
    # Pentaho step: Write Product Reject File (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/product/products_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sku' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='category_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='brand' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unit_cost' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unit_price' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='currency_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='weight_kg' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_active' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='created_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='description' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='upc' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='barcode' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='length_cm' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='width_cm' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='height_cm' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='reject_reason' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_valid_row' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='source_row_num' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Product_Reject_File = df_Route_Rejects
    _out_df_Write_Product_Reject_File = df_Write_Product_Reject_File.select('product_id', 'sku', 'product_name', 'category_id', 'supplier_id', 'brand', 'unit_cost', 'unit_price', 'currency_code', 'weight_kg', 'is_active', 'created_date', 'description', 'upc', 'barcode', 'length_cm', 'width_cm', 'height_cm', 'status', 'reject_reason', 'is_valid_row', 'batch_id', 'run_id', 'source_row_num')
    writer = _out_df_Write_Product_Reject_File.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/products_rejects_.csv')

    # Step: Count Valid Rows (MemoryGroupBy) [converted]
    # Memory Group By: Count Valid Rows
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Valid_Rows = df_Write_Valid_Products.groupBy().agg(count(lit(1)).alias('row_count'))

    # Step: Count Reject Rows (MemoryGroupBy) [converted]
    # Memory Group By: Count Reject Rows
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Reject_Rows = df_Write_Product_Reject_File.groupBy().agg(count(lit(1)).alias('row_count'))

    # Step: Tag Valid Metrics (Constant) [converted]
    # Add Constants: Tag Valid Metrics
    df_Tag_Valid_Metrics = df_Count_Valid_Rows
    df_Tag_Valid_Metrics = df_Tag_Valid_Metrics.withColumn("validation_status", lit('VALID'))
    # preserved.validation_status: length='-1', precision='-1'

    # Step: Tag Reject Metrics (Constant) [converted]
    # Add Constants: Tag Reject Metrics
    df_Tag_Reject_Metrics = df_Count_Reject_Rows
    df_Tag_Reject_Metrics = df_Tag_Reject_Metrics.withColumn("validation_status", lit('REJECTED'))
    # preserved.validation_status: length='-1', precision='-1'

    # Step: Append Validation Metrics (Append) [converted]
    # Append Streams: Append Validation Metrics
    # preserved.head_name='Tag Valid Metrics'
    # preserved.tail_name='Tag Reject Metrics'
    # preserved.stream_order=['Tag Valid Metrics', 'Tag Reject Metrics']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_Validation_Metrics = df_Tag_Valid_Metrics.unionByName(df_Tag_Reject_Metrics, allowMissingColumns=True)

    # Step: Write Validation Log (TextFileOutput) [converted]
    # Pentaho step: Write Validation Log (type: TextFileOutput)
    # Pentaho filename: /logs/execution/product/TR_Product_Validation_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='validation_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='row_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Validation_Log = df_Append_Validation_Metrics
    _out_df_Write_Validation_Log = df_Write_Validation_Log.select('validation_status', 'row_count')
    writer = _out_df_Write_Validation_Log.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/TR_Product_Validation_.log')

    # Step: Log Validation Complete (WriteToLog) [converted]
    # Write to Log: Log Validation Complete
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=VALIDATION_COMPLETE | TRANS=TR_Product_Validation | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Validation_Complete = logging.getLogger('pentaho.writetolog.Log_Validation_Complete')
    _log_df_Log_Validation_Complete.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Validation_Complete = df_Write_Validation_Log
    _log_rows_df_Log_Validation_Complete = _log_df_df_Log_Validation_Complete.take(20)
    _log_df_Log_Validation_Complete.info('Log Validation Complete' + ' | columns=' + str(_log_df_df_Log_Validation_Complete.columns))
    _log_df_Log_Validation_Complete.info('AUDIT | EVENT=VALIDATION_COMPLETE | TRANS=TR_Product_Validation | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Validation_Complete:
        _log_df_Log_Validation_Complete.info('Log Validation Complete' + ' | ' + str(_lr.asDict()))
    df_Log_Validation_Complete = df_Write_Validation_Log

    # Step: Validation Complete (Dummy) [converted]
    # Dummy: Validation Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Validation_Complete = df_Log_Validation_Complete

    log_event(_LOG, "transformation_end")
    return df_Dummy_Validation_Complete
