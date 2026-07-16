"""PySpark module migrated from Pentaho transformation: TR_Inventory_Validation.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/validation/Inventory_Validation.ktr
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

_LOG = get_logger("pentaho_migration.transformations.retail.inventory_validation")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Inventory_Validation`` step-for-step."""
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

    # Step: Read Validation Policy JSON (JsonInput) [converted]
    # JSON Input: Read Validation Policy JSON
    df_Read_Validation_Policy_JSON = spark.read.format('json').option('multiline', 'true').load('${PROJECT_HOME}/metadata/rules/inventory_validation_policy.json')

    # Step: Read Validation Rules XML (getXMLData) [converted]
    # XML Input: Read Validation Rules XML
    df_Read_Validation_Rules_XML = spark.read.format('xml').option('rowTag', 'row').load('${PROJECT_HOME}/metadata/rules/inventory_validation_rules.xml')

    # Step: Read Staged Joined Inventory (CsvInput) [converted]
    # CSV Input: Read Staged Joined Inventory
    df_Read_Staged_Joined_Inventory = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('inventory_id STRING, store_id STRING, product_id STRING, quantity_on_hand STRING, quantity_reserved STRING, reorder_level STRING, reorder_quantity STRING, last_stocktake_date STRING, bin_location STRING, is_low_stock STRING, sku STRING, product_name STRING, category_id STRING, supplier_id STRING, brand STRING, unit_cost STRING, unit_price STRING, currency_code STRING, weight_kg STRING, is_active STRING, store_name STRING, store_type STRING, region_id STRING, square_footage STRING, supplier_name STRING, lead_time_days STRING, supplier_active STRING, warehouse_code STRING, bin_slot STRING, batch_number STRING, expiry_date STRING, maximum_stock STRING, minimum_stock STRING, quantity STRING, available_qty STRING, batch_id STRING, run_id STRING, extract_ts STRING, source_row_num STRING')
        .load(f'{data_dir}/stg_joined_inventory_.csv')
    )

    # Step: JSON Policy Consumed (Dummy) [converted]
    # Dummy: JSON Policy Consumed
    # Pass-through step - DataFrame unchanged
    df_Dummy_JSON_Policy_Consumed = df_Read_Validation_Policy_JSON

    # Step: XML Rules Consumed (Dummy) [converted]
    # Dummy: XML Rules Consumed
    # Pass-through step - DataFrame unchanged
    df_Dummy_XML_Rules_Consumed = df_Get_Variables

    # Step: Detect Empty Inventory Stream (DetectEmptyStream) [converted]
    # Detect Empty Stream: Detect Empty Inventory Stream
    _empty_flag_df_Detect_Empty_Inventory_Stream = df_Read_Staged_Joined_Inventory.limit(1).count() == 0
    # Pentaho semantics: if empty → one null row with input schema; else → empty DataFrame (no rows forwarded)
    if _empty_flag_df_Detect_Empty_Inventory_Stream:
        _schema_df_Detect_Empty_Inventory_Stream = df_Read_Staged_Joined_Inventory.schema
        if len(df_Read_Staged_Joined_Inventory.columns) == 0:
            df_Detect_Empty_Inventory_Stream = spark.createDataFrame([], _schema_df_Detect_Empty_Inventory_Stream)
        else:
            df_Detect_Empty_Inventory_Stream = spark.createDataFrame([tuple(None for _ in df_Read_Staged_Joined_Inventory.columns)], _schema_df_Detect_Empty_Inventory_Stream)
    else:
        df_Detect_Empty_Inventory_Stream = df_Read_Staged_Joined_Inventory.limit(0)
    # Downstream hops receive this single output stream (empty-detection row or zero rows).

    # Step: Empty Stream Guard? (FilterRows) [failed]
    # Filter Rows: Empty Stream Guard?
    df_Add_Validation_Batch = df_Detect_Empty_Inventory_Stream.filter(col("inventory_id").isNotNull())
    df_Abort_Empty_Inventory_Stream = df_Detect_Empty_Inventory_Stream.filter(~(col("inventory_id").isNotNull()))
    df_Empty_Stream_Guard? = df_Add_Validation_Batch

    # Step: Abort Empty Inventory Stream (Abort) [converted]
    # Abort: Abort Empty Inventory Stream
    # preserved.row_threshold=0
    # preserved.message='Catastrophic empty inventory stream. RUN_ID=${RUN_ID}'
    # preserved.always_log_rows=True
    # preserved.row_threshold_raw='0'
    # Abort operates on its own failure/branch stream df_Abort_Empty_Inventory_Stream (already assigned by upstream Filter/Switch; not overwritten)
    print('Abort sample for', 'Abort Empty Inventory Stream', df_Abort_Empty_Inventory_Stream.limit(100).collect())  # always_log_rows
    _abort_count_df_Abort_Empty_Inventory_Stream = df_Abort_Empty_Inventory_Stream.count()
    if _abort_count_df_Abort_Empty_Inventory_Stream > 0:  # Abort when any row reaches this step (threshold<=0)
        raise RuntimeError('Catastrophic empty inventory stream. RUN_ID=${RUN_ID}')

    # Step: Add Validation Batch (Constant) [failed]
    # Add Constants: Add Validation Batch
    df_Add_Validation_Batch = df_Empty_Stream_Guard?
    df_Add_Validation_Batch = df_Add_Validation_Batch.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Add_Validation_Batch = df_Add_Validation_Batch.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'
    df_Add_Validation_Batch = df_Add_Validation_Batch.withColumn("validation_status", lit('PENDING'))
    # preserved.validation_status: length='-1', precision='-1'

    # Step: Trim Validation Keys (StringOperations) [converted]
    # String Operations: Trim Validation Keys
    df_Trim_Validation_Keys = df_Add_Validation_Batch
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("inventory_id", upper(trim(col("inventory_id").cast("string"))))
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("product_id", upper(trim(col("product_id").cast("string"))))
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("store_id", upper(trim(col("store_id").cast("string"))))
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("supplier_id", upper(trim(col("supplier_id").cast("string"))))
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("warehouse_code", upper(trim(col("warehouse_code").cast("string"))))
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("batch_number", upper(trim(col("batch_number").cast("string"))))

    # Step: Validate Inventory ID (RegexEval) [converted]
    # Regex Evaluation: Validate Inventory ID
    # preserved.matcher='inventory_id'
    # preserved.pattern='^INV[0-9]{6}$|^[A-Z0-9_-]{6,}$'
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
    df_Validate_Inventory_ID = df_Trim_Validation_Keys
    df_Validate_Inventory_ID = df_Validate_Inventory_ID.withColumn('result', when(col('inventory_id').rlike('^INV[0-9]{6}$|^[A-Z0-9_-]{6,}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Product ID (RegexEval) [converted]
    # Regex Evaluation: Validate Product ID
    # preserved.matcher='product_id'
    # preserved.pattern='^PRD[0-9]{5}$|^[A-Z0-9_-]{4,}$'
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
    df_Validate_Product_ID = df_Validate_Inventory_ID
    df_Validate_Product_ID = df_Validate_Product_ID.withColumn('result', when(col('product_id').rlike('^PRD[0-9]{5}$|^[A-Z0-9_-]{4,}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Store ID (RegexEval) [converted]
    # Regex Evaluation: Validate Store ID
    # preserved.matcher='store_id'
    # preserved.pattern='^STR[0-9]{3,}$|^[A-Z0-9_-]{3,}$'
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
    df_Validate_Store_ID = df_Validate_Product_ID
    df_Validate_Store_ID = df_Validate_Store_ID.withColumn('result', when(col('store_id').rlike('^STR[0-9]{3,}$|^[A-Z0-9_-]{3,}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Supplier ID (RegexEval) [converted]
    # Regex Evaluation: Validate Supplier ID
    # preserved.matcher='supplier_id'
    # preserved.pattern='^SUP[0-9]{4}$|^[A-Z0-9_-]{4,}$'
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
    df_Validate_Supplier_ID = df_Validate_Store_ID
    df_Validate_Supplier_ID = df_Validate_Supplier_ID.withColumn('result', when(col('supplier_id').rlike('^SUP[0-9]{4}$|^[A-Z0-9_-]{4,}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Warehouse Code (RegexEval) [converted]
    # Regex Evaluation: Validate Warehouse Code
    # preserved.matcher='warehouse_code'
    # preserved.pattern='^[A-Z0-9]{1,12}$'
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
    df_Validate_Warehouse_Code = df_Validate_Supplier_ID
    df_Validate_Warehouse_Code = df_Validate_Warehouse_Code.withColumn('result', when(col('warehouse_code').rlike('^[A-Z0-9]{1,12}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Batch Number (RegexEval) [converted]
    # Regex Evaluation: Validate Batch Number
    # preserved.matcher='batch_number'
    # preserved.pattern='^[A-Z0-9_-]{4,64}$'
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
    df_Validate_Batch_Number = df_Validate_Warehouse_Code
    df_Validate_Batch_Number = df_Validate_Batch_Number.withColumn('result', when(col('batch_number').rlike('^[A-Z0-9_-]{4,64}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Expiry Date (RegexEval) [converted]
    # Regex Evaluation: Validate Expiry Date
    # preserved.matcher='expiry_date'
    # preserved.pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}'
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
    df_Validate_Expiry_Date = df_Validate_Batch_Number
    df_Validate_Expiry_Date = df_Validate_Expiry_Date.withColumn('result', when(col('expiry_date').rlike('^[0-9]{4}-[0-9]{2}-[0-9]{2}'), lit("Y")).otherwise(lit("N")))

    # Step: Cast Validation Numerics (SelectValues) [converted]
    # Select Values: Cast Validation Numerics
    df_Cast_Validation_Numerics = df_Validate_Expiry_Date.select(col("inventory_id").alias("inventory_id"), col("store_id").alias("store_id"), col("product_id").alias("product_id"), col("quantity_on_hand").alias("quantity_on_hand"), col("quantity_reserved").alias("quantity_reserved"), col("reorder_level").alias("reorder_level"), col("reorder_quantity").alias("reorder_quantity"), col("last_stocktake_date").alias("last_stocktake_date"), col("bin_location").alias("bin_location"), col("is_low_stock").alias("is_low_stock"), col("sku").alias("sku"), col("product_name").alias("product_name"), col("category_id").alias("category_id"), col("supplier_id").alias("supplier_id"), col("brand").alias("brand"), col("unit_cost").alias("unit_cost"), col("unit_price").alias("unit_price"), col("currency_code").alias("currency_code"), col("weight_kg").alias("weight_kg"), col("is_active").alias("is_active"), col("store_name").alias("store_name"), col("store_type").alias("store_type"), col("region_id").alias("region_id"), col("square_footage").alias("square_footage"), col("supplier_name").alias("supplier_name"), col("lead_time_days").alias("lead_time_days"), col("supplier_active").alias("supplier_active"), col("warehouse_code").alias("warehouse_code"), col("bin_slot").alias("bin_slot"), col("batch_number").alias("batch_number"), col("expiry_date").alias("expiry_date"), col("maximum_stock").alias("maximum_stock"), col("minimum_stock").alias("minimum_stock"), col("quantity").alias("quantity"), col("available_qty").alias("available_qty"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"), col("extract_ts").alias("extract_ts"), col("source_row_num").alias("source_row_num"), col("inventory_id_valid").alias("inventory_id_valid"), col("product_id_valid").alias("product_id_valid"), col("store_id_valid").alias("store_id_valid"), col("supplier_id_valid").alias("supplier_id_valid"), col("warehouse_valid").alias("warehouse_valid"), col("batch_valid").alias("batch_valid"), col("expiry_valid").alias("expiry_valid"), col("validation_status").alias("validation_status"))

    # Step: Soft Lookup DimProduct (DBJoin) [partial]
    # Database Join: Soft Lookup DimProduct
    # preserved.connection='conn_dev_dwh'
    # preserved.sql="SELECT product_sk FROM retail_dwh.dim_product WHERE product_id = ? AND is_current = 'Y'"
    # preserved.outer_join=True
    # preserved.row_limit=0
    # preserved.replace_vars=True
    # preserved.parameters=[{'name': 'product_id', 'type': 'String'}, {'name': '\n        ', 'type': ''}]
    _sql_df_Soft_Lookup_DimProduct = "SELECT product_sk FROM retail_dwh.dim_product WHERE product_id = ? AND is_current = 'Y'"
    # WARNING: per-row parameterized joins cannot use spark.sql with '?' placeholders; emitting JDBC prepared-statement skeleton (foreachPartition).
    # preserved.sql_template="SELECT product_sk FROM retail_dwh.dim_product WHERE product_id = :product_id AND is_current = 'Y'"
    _param_fields_df_Soft_Lookup_DimProduct = ['product_id', '\n        ']
    import os
    # foreachPartition JDBC outline (wire PENTAHO_JDBC_URL / driver at runtime):
    # def _dbjoin_partition(rows):
    #     conn = <jdbc connect from os.environ['PENTAHO_JDBC_URL']>
    #     cur = conn.prepareStatement("SELECT product_sk FROM retail_dwh.dim_product WHERE product_id = ? AND is_current = 'Y'")
    #     for row in rows:
    #         for i, f in enumerate(_param_fields_df_Soft_Lookup_DimProduct, 1):
    #             cur.setObject(i, row[f])
    #         rs = cur.executeQuery(); ... yield joined rows
    # Fallback: preserve input stream; attach empty lookup side for schema continuity
    df_Soft_Lookup_DimProduct = df_Cast_Validation_Numerics
    # Join type preserved as 'left'; join keys=['product_id', '\n        ']

    # Step: Soft Lookup DimStore (DBJoin) [partial]
    # Database Join: Soft Lookup DimStore
    # preserved.connection='conn_dev_dwh'
    # preserved.sql="SELECT store_sk FROM retail_dwh.dim_store WHERE store_id = ? AND is_current = 'Y'"
    # preserved.outer_join=True
    # preserved.row_limit=0
    # preserved.replace_vars=True
    # preserved.parameters=[{'name': 'store_id', 'type': 'String'}, {'name': '\n        ', 'type': ''}]
    _sql_df_Soft_Lookup_DimStore = "SELECT store_sk FROM retail_dwh.dim_store WHERE store_id = ? AND is_current = 'Y'"
    # WARNING: per-row parameterized joins cannot use spark.sql with '?' placeholders; emitting JDBC prepared-statement skeleton (foreachPartition).
    # preserved.sql_template="SELECT store_sk FROM retail_dwh.dim_store WHERE store_id = :store_id AND is_current = 'Y'"
    _param_fields_df_Soft_Lookup_DimStore = ['store_id', '\n        ']
    import os
    # foreachPartition JDBC outline (wire PENTAHO_JDBC_URL / driver at runtime):
    # def _dbjoin_partition(rows):
    #     conn = <jdbc connect from os.environ['PENTAHO_JDBC_URL']>
    #     cur = conn.prepareStatement("SELECT store_sk FROM retail_dwh.dim_store WHERE store_id = ? AND is_current = 'Y'")
    #     for row in rows:
    #         for i, f in enumerate(_param_fields_df_Soft_Lookup_DimStore, 1):
    #             cur.setObject(i, row[f])
    #         rs = cur.executeQuery(); ... yield joined rows
    # Fallback: preserve input stream; attach empty lookup side for schema continuity
    df_Soft_Lookup_DimStore = df_Soft_Lookup_DimProduct
    # Join type preserved as 'left'; join keys=['store_id', '\n        ']

    # Step: Soft Lookup DimSupplier (DBJoin) [partial]
    # Database Join: Soft Lookup DimSupplier
    # preserved.connection='conn_dev_dwh'
    # preserved.sql="SELECT supplier_sk FROM retail_dwh.dim_supplier WHERE supplier_id = ? AND is_current = 'Y'"
    # preserved.outer_join=True
    # preserved.row_limit=0
    # preserved.replace_vars=True
    # preserved.parameters=[{'name': 'supplier_id', 'type': 'String'}, {'name': '\n        ', 'type': ''}]
    _sql_df_Soft_Lookup_DimSupplier = "SELECT supplier_sk FROM retail_dwh.dim_supplier WHERE supplier_id = ? AND is_current = 'Y'"
    # WARNING: per-row parameterized joins cannot use spark.sql with '?' placeholders; emitting JDBC prepared-statement skeleton (foreachPartition).
    # preserved.sql_template="SELECT supplier_sk FROM retail_dwh.dim_supplier WHERE supplier_id = :supplier_id AND is_current = 'Y'"
    _param_fields_df_Soft_Lookup_DimSupplier = ['supplier_id', '\n        ']
    import os
    # foreachPartition JDBC outline (wire PENTAHO_JDBC_URL / driver at runtime):
    # def _dbjoin_partition(rows):
    #     conn = <jdbc connect from os.environ['PENTAHO_JDBC_URL']>
    #     cur = conn.prepareStatement("SELECT supplier_sk FROM retail_dwh.dim_supplier WHERE supplier_id = ? AND is_current = 'Y'")
    #     for row in rows:
    #         for i, f in enumerate(_param_fields_df_Soft_Lookup_DimSupplier, 1):
    #             cur.setObject(i, row[f])
    #         rs = cur.executeQuery(); ... yield joined rows
    # Fallback: preserve input stream; attach empty lookup side for schema continuity
    df_Soft_Lookup_DimSupplier = df_Soft_Lookup_DimStore
    # Join type preserved as 'left'; join keys=['supplier_id', '\n        ']

    # Step: Data Validator Inventory Rules (Validator) [converted]
    # Data Validator: Data Validator Inventory Rules
    # preserved.validate_all=True
    # preserved.concat_errors=True
    # preserved.concat_separator='|'
    # WARNING: Data Validator has no validator_field rules
    df_Data_Validator_Inventory_Rules = df_Soft_Lookup_DimSupplier

    # Step: Compute Validation Flags (Formula) [converted]
    # Formula: Compute Validation Flags
    df_Compute_Validation_Flags = df_Data_Validator_Inventory_Rules
    df_Compute_Validation_Flags = df_Compute_Validation_Flags.withColumn('formula_result', lit(None))  # empty formula

    # Step: Valid Inventory? (FilterRows) [failed]
    # Filter Rows: Valid Inventory?
    df_Mark_Valid_Inventory = df_Compute_Validation_Flags.filter((col("reject_reason") == lit('OK')))
    df_Bucket_Reject_Reason = df_Compute_Validation_Flags.filter(~((col("reject_reason") == lit('OK'))))
    df_Valid_Inventory? = df_Mark_Valid_Inventory

    # Step: Bucket Reject Reason (SwitchCase) [failed]
    # Switch / Case: Bucket Reject Reason
    # preserved.fieldname='reject_reason'
    # preserved.switch_field='reject_reason'
    # preserved.cases=[{'value': 'FORMAT', 'target_step': 'Write Format Rejects'}, {'value': 'RANGE', 'target_step': 'Write Range Rejects'}]
    # preserved.default_target_step='Write Format Rejects'
    # preserved.use_contains=False
    # preserved.case_value_type='String'
    # preserved.rules=[{'value': 'FORMAT', 'target_step': 'Write Format Rejects'}, {'value': 'RANGE', 'target_step': 'Write Range Rejects'}]
    _routed_df_Bucket_Reject_Reason = df_Valid_Inventory?.withColumn('_route_Bucket_Reject_Reason', when(col("reject_reason") == lit('FORMAT'), lit('Write Format Rejects')).when(col("reject_reason") == lit('RANGE'), lit('Write Range Rejects')).otherwise(lit('Write Format Rejects')))
    df_Write_Format_Rejects = _routed_df_Bucket_Reject_Reason.filter(col('_route_Bucket_Reject_Reason') == lit('Write Format Rejects')).drop('_route_Bucket_Reject_Reason')
    df_Write_Range_Rejects = _routed_df_Bucket_Reject_Reason.filter(col('_route_Bucket_Reject_Reason') == lit('Write Range Rejects')).drop('_route_Bucket_Reject_Reason')
    df_Bucket_Reject_Reason = df_Write_Format_Rejects

    # Step: Mark Valid Inventory (Constant) [failed]
    # Add Constants: Mark Valid Inventory
    df_Mark_Valid_Inventory = df_Valid_Inventory?
    df_Mark_Valid_Inventory = df_Mark_Valid_Inventory.withColumn("validation_status", lit('VALID'))
    # preserved.validation_status: length='-1', precision='-1'

    # Step: Write Format Rejects (TextFileOutput) [converted]
    # Pentaho step: Write Format Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/inventory/inventory_format_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='inventory_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='reject_reason' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Format_Rejects = df_Bucket_Reject_Reason
    _out_df_Write_Format_Rejects = df_Write_Format_Rejects.select('inventory_id', 'product_id', 'store_id', 'reject_reason', 'batch_id', 'run_id')
    writer = _out_df_Write_Format_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/inventory_format_rejects_.csv')

    # Step: Write Range Rejects (TextFileOutput) [converted]
    # Pentaho step: Write Range Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/inventory/inventory_range_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='inventory_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quantity' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='maximum_stock' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='minimum_stock' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='reject_reason' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Range_Rejects = df_Bucket_Reject_Reason
    _out_df_Write_Range_Rejects = df_Write_Range_Rejects.select('inventory_id', 'quantity', 'maximum_stock', 'minimum_stock', 'reject_reason', 'batch_id', 'run_id')
    writer = _out_df_Write_Range_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/inventory_range_rejects_.csv')

    # Step: Write Valid Inventory (TextFileOutput) [converted]
    # Pentaho step: Write Valid Inventory (type: TextFileOutput)
    # Pentaho filename: /output/inventory/validated/inventory_valid_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='inventory_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quantity_on_hand' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quantity_reserved' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='reorder_level' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='reorder_quantity' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='last_stocktake_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='bin_location' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_low_stock' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
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
    # INFO: preserved.field_format name='store_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_type' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='region_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='square_footage' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='lead_time_days' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_active' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='warehouse_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='bin_slot' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_number' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='expiry_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='maximum_stock' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='minimum_stock' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quantity' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='available_qty' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='extract_ts' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='source_row_num' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='validation_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Valid_Inventory = df_Mark_Valid_Inventory
    _out_df_Write_Valid_Inventory = df_Write_Valid_Inventory.select('inventory_id', 'store_id', 'product_id', 'quantity_on_hand', 'quantity_reserved', 'reorder_level', 'reorder_quantity', 'last_stocktake_date', 'bin_location', 'is_low_stock', 'sku', 'product_name', 'category_id', 'supplier_id', 'brand', 'unit_cost', 'unit_price', 'currency_code', 'weight_kg', 'is_active', 'store_name', 'store_type', 'region_id', 'square_footage', 'supplier_name', 'lead_time_days', 'supplier_active', 'warehouse_code', 'bin_slot', 'batch_number', 'expiry_date', 'maximum_stock', 'minimum_stock', 'quantity', 'available_qty', 'batch_id', 'run_id', 'extract_ts', 'source_row_num', 'validation_status', 'batch_id', 'run_id')
    writer = _out_df_Write_Valid_Inventory.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/inventory_valid_.csv')

    # Step: Write Reject Report (TextFileOutput) [converted]
    # Pentaho step: Write Reject Report (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/inventory/inventory_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='inventory_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='reject_reason' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Reject_Report = df_Write_Format_Rejects
    _out_df_Write_Reject_Report = df_Write_Reject_Report.select('inventory_id', 'reject_reason', 'batch_id', 'run_id')
    writer = _out_df_Write_Reject_Report.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/inventory_rejects_.csv')

    # Step: Count Valid Rows (MemoryGroupBy) [converted]
    # Memory Group By: Count Valid Rows
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Valid_Rows = df_Write_Valid_Inventory.groupBy().agg(count(lit(1)).alias('row_count'), first(col("validation_status"), ignorenulls=True).alias('validation_status'))

    # Step: Write Validation Log (TextFileOutput) [converted]
    # Pentaho step: Write Validation Log (type: TextFileOutput)
    # Pentaho filename: /logs/execution/inventory/TR_Inventory_Validation_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='validation_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='row_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Validation_Log = df_Count_Valid_Rows
    _out_df_Write_Validation_Log = df_Write_Validation_Log.select('validation_status', 'row_count')
    writer = _out_df_Write_Validation_Log.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/TR_Inventory_Validation_.log')

    # Step: Log Validation Complete (WriteToLog) [converted]
    # Write to Log: Log Validation Complete
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=VALIDATION_OK | TRANS=TR_Inventory_Validation | RUN_ID=${RUN_ID}'
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
    _log_df_Log_Validation_Complete.info('AUDIT | EVENT=VALIDATION_OK | TRANS=TR_Inventory_Validation | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Validation_Complete:
        _log_df_Log_Validation_Complete.info('Log Validation Complete' + ' | ' + str(_lr.asDict()))
    df_Log_Validation_Complete = df_Write_Validation_Log

    # Step: Validation Complete (Dummy) [converted]
    # Dummy: Validation Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Validation_Complete = df_Log_Validation_Complete

    log_event(_LOG, "transformation_end")
    return df_Dummy_Validation_Complete
