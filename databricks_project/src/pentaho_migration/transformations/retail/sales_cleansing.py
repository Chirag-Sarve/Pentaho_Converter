"""PySpark module migrated from Pentaho transformation: TR_Sales_Validation.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/cleansing/Sales_Cleansing.ktr
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

_LOG = get_logger("pentaho_migration.transformations.retail.sales_cleansing")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Sales_Validation`` step-for-step."""
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

    # Step: Read Staged Joined Sales (CsvInput) [converted]
    # CSV Input: Read Staged Joined Sales
    df_Read_Staged_Joined_Sales = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('order_item_id STRING, order_id STRING, product_id STRING, promotion_id STRING, quantity STRING, unit_price STRING, discount_amount STRING, line_total STRING, currency_code STRING, customer_id STRING, store_id STRING, employee_id STRING, order_date STRING, order_status STRING, channel STRING, order_currency STRING, subtotal_amount STRING, order_tax_amount STRING, shipping_amount STRING, order_discount_amount STRING, total_amount STRING, promo_code STRING, payment_id STRING, payment_method STRING, payment_status STRING, payment_amount STRING, shipment_id STRING, shipment_status STRING, shipped_date STRING, estimated_delivery_date STRING, actual_delivery_date STRING, shipping_cost STRING, return_qty STRING, return_refund_amount STRING, exchange_rate STRING, batch_id STRING, run_id STRING, etl_layer STRING, extract_ts STRING, source_row_num INT')
        .load(f'{data_dir}/stg_joined_sales_.csv')
    )

    # Step: Read Validation Policy JSON (JsonInput) [converted]
    # JSON Input: Read Validation Policy JSON
    df_Read_Validation_Policy_JSON = spark.read.format('json').option('multiline', 'true').load('${PROJECT_HOME}/metadata/rules/sales_validation_policy.json')

    # Step: Read Validation Rules XML (getXMLData) [converted]
    # XML Input: Read Validation Rules XML
    df_Read_Validation_Rules_XML = spark.read.format('xml').option('rowTag', 'row').load('${PROJECT_HOME}/metadata/rules/sales_validation_rules.xml')

    # Step: Write Validator Error Rejects (TextFileOutput) [converted]
    # Pentaho step: Write Validator Error Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/sales/sales_validator_errs_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='order_item_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_CODE' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_DESC' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_FIELDS' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Validator_Error_Rejects = df_Write_Validator_Error_Rejects
    _out_df_Write_Validator_Error_Rejects = df_Write_Validator_Error_Rejects.select('order_item_id', 'order_id', 'ERR_CODE', 'ERR_DESC', 'ERR_FIELDS', 'batch_id', 'run_id')
    writer = _out_df_Write_Validator_Error_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_validator_errs_.csv')

    # Step: Detect Empty Sales Stream (DetectEmptyStream) [converted]
    # Detect Empty Stream: Detect Empty Sales Stream
    _empty_flag_df_Detect_Empty_Sales_Stream = df_Read_Staged_Joined_Sales.limit(1).count() == 0
    # Pentaho semantics: if empty → one null row with input schema; else → empty DataFrame (no rows forwarded)
    if _empty_flag_df_Detect_Empty_Sales_Stream:
        _schema_df_Detect_Empty_Sales_Stream = df_Read_Staged_Joined_Sales.schema
        if len(df_Read_Staged_Joined_Sales.columns) == 0:
            df_Detect_Empty_Sales_Stream = spark.createDataFrame([], _schema_df_Detect_Empty_Sales_Stream)
        else:
            df_Detect_Empty_Sales_Stream = spark.createDataFrame([tuple(None for _ in df_Read_Staged_Joined_Sales.columns)], _schema_df_Detect_Empty_Sales_Stream)
    else:
        df_Detect_Empty_Sales_Stream = df_Read_Staged_Joined_Sales.limit(0)
    # Downstream hops receive this single output stream (empty-detection row or zero rows).

    # Step: Get Variables (GetVariable) [converted]
    # Get Variables: Get Variables
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'reject_path', 'variable': '${REJECT_PATH}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
    # preserved.output_columns=['batch_id', 'run_id', 'reject_path']
    import os
    import re as _re_var
    # Lookup order: Databricks widgets → os.environ → spark.conf (pentaho.var.*) → transformation parameters → empty string
    df_Get_Variables = df_Read_Validation_Rules_XML
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

    # Step: Empty Stream Guard? (FilterRows) [failed]
    # Filter Rows: Empty Stream Guard?
    df_Add_Validation_Batch = df_Detect_Empty_Sales_Stream.filter(col("order_item_id").isNotNull())
    df_Abort_Empty_Sales_Stream = df_Detect_Empty_Sales_Stream.filter(~(col("order_item_id").isNotNull()))
    df_Empty_Stream_Guard? = df_Add_Validation_Batch

    # Step: Abort Empty Sales Stream (Abort) [converted]
    # Abort: Abort Empty Sales Stream
    # preserved.row_threshold=0
    # preserved.message='Catastrophic empty sales stream — no staged rows. RUN_ID=${RUN_ID}'
    # preserved.always_log_rows=True
    # preserved.row_threshold_raw='0'
    # Abort operates on its own failure/branch stream df_Abort_Empty_Sales_Stream (already assigned by upstream Filter/Switch; not overwritten)
    print('Abort sample for', 'Abort Empty Sales Stream', df_Abort_Empty_Sales_Stream.limit(100).collect())  # always_log_rows
    _abort_count_df_Abort_Empty_Sales_Stream = df_Abort_Empty_Sales_Stream.count()
    if _abort_count_df_Abort_Empty_Sales_Stream > 0:  # Abort when any row reaches this step (threshold<=0)
        raise RuntimeError('Catastrophic empty sales stream — no staged rows. RUN_ID=${RUN_ID}')

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
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("order_id", upper(trim(col("order_id").cast("string"))))
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("order_item_id", upper(trim(col("order_item_id").cast("string"))))
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("product_id", upper(trim(col("product_id").cast("string"))))
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("customer_id", upper(trim(col("customer_id").cast("string"))))
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("store_id", upper(trim(col("store_id").cast("string"))))
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("payment_id", upper(trim(col("payment_id").cast("string"))))
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("shipment_id", upper(trim(col("shipment_id").cast("string"))))
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("currency_code", upper(trim(col("currency_code").cast("string"))))
    df_Trim_Validation_Keys = df_Trim_Validation_Keys.withColumn("order_status", upper(trim(col("order_status").cast("string"))))

    # Step: Validate Order ID (RegexEval) [converted]
    # Regex Evaluation: Validate Order ID
    # preserved.matcher='order_id'
    # preserved.pattern='^ORD[0-9]{6,}$|^[A-Z0-9_-]{6,}$'
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
    df_Validate_Order_ID = df_Trim_Validation_Keys
    df_Validate_Order_ID = df_Validate_Order_ID.withColumn('result', when(col('order_id').rlike('^ORD[0-9]{6,}$|^[A-Z0-9_-]{6,}$'), lit("Y")).otherwise(lit("N")))

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
    df_Validate_Product_ID = df_Validate_Order_ID
    df_Validate_Product_ID = df_Validate_Product_ID.withColumn('result', when(col('product_id').rlike('^PRD[0-9]{5}$|^[A-Z0-9_-]{4,}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Customer ID (RegexEval) [converted]
    # Regex Evaluation: Validate Customer ID
    # preserved.matcher='customer_id'
    # preserved.pattern='^CUS[0-9]{5}$|^[A-Z0-9_-]{4,}$'
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
    df_Validate_Customer_ID = df_Validate_Product_ID
    df_Validate_Customer_ID = df_Validate_Customer_ID.withColumn('result', when(col('customer_id').rlike('^CUS[0-9]{5}$|^[A-Z0-9_-]{4,}$'), lit("Y")).otherwise(lit("N")))

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
    df_Validate_Store_ID = df_Validate_Customer_ID
    df_Validate_Store_ID = df_Validate_Store_ID.withColumn('result', when(col('store_id').rlike('^STR[0-9]{3,}$|^[A-Z0-9_-]{3,}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Payment ID (RegexEval) [converted]
    # Regex Evaluation: Validate Payment ID
    # preserved.matcher='payment_id'
    # preserved.pattern='^(|PAY[0-9A-Z_-]{4,})$'
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
    df_Validate_Payment_ID = df_Validate_Store_ID
    df_Validate_Payment_ID = df_Validate_Payment_ID.withColumn('result', when(col('payment_id').rlike('^(|PAY[0-9A-Z_-]{4,})$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Shipment ID (RegexEval) [converted]
    # Regex Evaluation: Validate Shipment ID
    # preserved.matcher='shipment_id'
    # preserved.pattern='^(|SHP[0-9A-Z_-]{4,})$'
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
    df_Validate_Shipment_ID = df_Validate_Payment_ID
    df_Validate_Shipment_ID = df_Validate_Shipment_ID.withColumn('result', when(col('shipment_id').rlike('^(|SHP[0-9A-Z_-]{4,})$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Currency (RegexEval) [converted]
    # Regex Evaluation: Validate Currency
    # preserved.matcher='currency_code'
    # preserved.pattern='^[A-Z]{3}$'
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
    df_Validate_Currency = df_Validate_Shipment_ID
    df_Validate_Currency = df_Validate_Currency.withColumn('result', when(col('currency_code').rlike('^[A-Z]{3}$'), lit("Y")).otherwise(lit("N")))

    # Step: Validate Order Date (RegexEval) [converted]
    # Regex Evaluation: Validate Order Date
    # preserved.matcher='order_date'
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
    df_Validate_Order_Date = df_Validate_Currency
    df_Validate_Order_Date = df_Validate_Order_Date.withColumn('result', when(col('order_date').rlike('^[0-9]{4}-[0-9]{2}-[0-9]{2}'), lit("Y")).otherwise(lit("N")))

    # Step: Cast Numeric Validation Fields (SelectValues) [converted]
    # Select Values: Cast Numeric Validation Fields
    df_Cast_Numeric_Validation_Fields = df_Validate_Order_Date.select(col("order_item_id").alias("order_item_id"), col("order_id").alias("order_id"), col("product_id").alias("product_id"), col("customer_id").alias("customer_id"), col("store_id").alias("store_id"), col("employee_id").alias("employee_id"), col("promotion_id").alias("promotion_id"), col("quantity").alias("quantity"), col("unit_price").alias("unit_price"), col("discount_amount").alias("discount_amount"), col("line_total").alias("line_total"), col("currency_code").alias("currency_code"), col("order_date").alias("order_date"), col("order_status").alias("order_status"), col("channel").alias("channel"), col("order_tax_amount").alias("order_tax_amount"), col("shipping_amount").alias("shipping_amount"), col("total_amount").alias("total_amount"), col("payment_id").alias("payment_id"), col("payment_method").alias("payment_method"), col("payment_status").alias("payment_status"), col("payment_amount").alias("payment_amount"), col("shipment_id").alias("shipment_id"), col("shipment_status").alias("shipment_status"), col("shipped_date").alias("shipped_date"), col("estimated_delivery_date").alias("estimated_delivery_date"), col("actual_delivery_date").alias("actual_delivery_date"), col("shipping_cost").alias("shipping_cost"), col("return_qty").alias("return_qty"), col("return_refund_amount").alias("return_refund_amount"), col("exchange_rate").alias("exchange_rate"), col("promo_code").alias("promo_code"), col("subtotal_amount").alias("subtotal_amount"), col("order_discount_amount").alias("order_discount_amount"), col("order_currency").alias("order_currency"), col("order_id_valid").alias("order_id_valid"), col("product_id_valid").alias("product_id_valid"), col("customer_id_valid").alias("customer_id_valid"), col("store_id_valid").alias("store_id_valid"), col("payment_id_valid").alias("payment_id_valid"), col("shipment_id_valid").alias("shipment_id_valid"), col("currency_valid").alias("currency_valid"), col("order_date_valid").alias("order_date_valid"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"), col("source_row_num").alias("source_row_num"))

    # Step: Soft Lookup DimCustomer (DBJoin) [partial]
    # Database Join: Soft Lookup DimCustomer
    # preserved.connection='conn_dev_dwh'
    # preserved.sql="SELECT customer_sk FROM retail_dwh.dim_customer WHERE customer_id = ? AND is_current = 'Y'"
    # preserved.outer_join=True
    # preserved.row_limit=0
    # preserved.replace_vars=True
    # preserved.parameters=[{'name': 'customer_id', 'type': 'String'}, {'name': '\n        ', 'type': ''}]
    _sql_df_Soft_Lookup_DimCustomer = "SELECT customer_sk FROM retail_dwh.dim_customer WHERE customer_id = ? AND is_current = 'Y'"
    # WARNING: per-row parameterized joins cannot use spark.sql with '?' placeholders; emitting JDBC prepared-statement skeleton (foreachPartition).
    # preserved.sql_template="SELECT customer_sk FROM retail_dwh.dim_customer WHERE customer_id = :customer_id AND is_current = 'Y'"
    _param_fields_df_Soft_Lookup_DimCustomer = ['customer_id', '\n        ']
    import os
    # foreachPartition JDBC outline (wire PENTAHO_JDBC_URL / driver at runtime):
    # def _dbjoin_partition(rows):
    #     conn = <jdbc connect from os.environ['PENTAHO_JDBC_URL']>
    #     cur = conn.prepareStatement("SELECT customer_sk FROM retail_dwh.dim_customer WHERE customer_id = ? AND is_current = 'Y'")
    #     for row in rows:
    #         for i, f in enumerate(_param_fields_df_Soft_Lookup_DimCustomer, 1):
    #             cur.setObject(i, row[f])
    #         rs = cur.executeQuery(); ... yield joined rows
    # Fallback: preserve input stream; attach empty lookup side for schema continuity
    df_Soft_Lookup_DimCustomer = df_Cast_Numeric_Validation_Fields
    # Join type preserved as 'left'; join keys=['customer_id', '\n        ']

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
    df_Soft_Lookup_DimProduct = df_Soft_Lookup_DimCustomer
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

    # Step: Data Validator Sales Rules (Validator) [converted]
    # Data Validator: Data Validator Sales Rules
    # preserved.validate_all=True
    # preserved.concat_errors=True
    # preserved.concat_separator='|'
    # WARNING: Data Validator has no validator_field rules
    df_Data_Validator_Sales_Rules = df_Soft_Lookup_DimStore

    # Step: Compose Validation Flag (Formula) [converted]
    # Formula: Compose Validation Flag
    df_Compose_Validation_Flag = df_Data_Validator_Sales_Rules
    df_Compose_Validation_Flag = df_Compose_Validation_Flag.withColumn('formula_result', lit(None))  # empty formula

    # Step: Valid Sales Row? (FilterRows) [failed]
    # Filter Rows: Valid Sales Row?
    df_Write_Valid_Sales = df_Compose_Validation_Flag.filter((col("is_valid_row") == lit('Y')))
    df_Route_Rejects = df_Compose_Validation_Flag.filter(~((col("is_valid_row") == lit('Y'))))
    df_Valid_Sales_Row? = df_Write_Valid_Sales

    # Step: Route Rejects (Constant) [failed]
    # Add Constants: Route Rejects
    df_Route_Rejects = df_Valid_Sales_Row?
    df_Route_Rejects = df_Route_Rejects.withColumn("reject_layer", lit('VALIDATION'))
    # preserved.reject_layer: length='-1', precision='-1'

    # Step: Write Valid Sales (TextFileOutput) [failed]
    # Pentaho step: Write Valid Sales (type: TextFileOutput)
    # Pentaho filename: /output/sales/validated/sales_valid_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='order_item_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='promotion_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quantity' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unit_price' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='discount_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='line_total' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='currency_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='customer_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='employee_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='channel' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_currency' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='subtotal_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_tax_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipping_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_discount_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='total_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='promo_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_method' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipment_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipment_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipped_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='estimated_delivery_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='actual_delivery_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipping_cost' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='return_qty' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='return_refund_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='exchange_rate' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='source_row_num' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Valid_Sales = df_Valid_Sales_Row?
    _out_df_Write_Valid_Sales = df_Write_Valid_Sales.select('order_item_id', 'order_id', 'product_id', 'promotion_id', 'quantity', 'unit_price', 'discount_amount', 'line_total', 'currency_code', 'customer_id', 'store_id', 'employee_id', 'order_date', 'order_status', 'channel', 'order_currency', 'subtotal_amount', 'order_tax_amount', 'shipping_amount', 'order_discount_amount', 'total_amount', 'promo_code', 'payment_id', 'payment_method', 'payment_status', 'payment_amount', 'shipment_id', 'shipment_status', 'shipped_date', 'estimated_delivery_date', 'actual_delivery_date', 'shipping_cost', 'return_qty', 'return_refund_amount', 'exchange_rate', 'batch_id', 'run_id', 'source_row_num')
    writer = _out_df_Write_Valid_Sales.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_valid_.csv')

    # Step: Write Sales Reject File (TextFileOutput) [converted]
    # Pentaho step: Write Sales Reject File (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/sales/sales_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='order_item_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='promotion_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quantity' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unit_price' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='discount_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='line_total' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='currency_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='customer_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='employee_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='channel' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_currency' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='subtotal_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_tax_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipping_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_discount_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='total_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='promo_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_method' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipment_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipment_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipped_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='estimated_delivery_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='actual_delivery_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipping_cost' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='return_qty' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='return_refund_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='exchange_rate' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='source_row_num' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='reject_reason' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_valid_row' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Sales_Reject_File = df_Route_Rejects
    _out_df_Write_Sales_Reject_File = df_Write_Sales_Reject_File.select('order_item_id', 'order_id', 'product_id', 'promotion_id', 'quantity', 'unit_price', 'discount_amount', 'line_total', 'currency_code', 'customer_id', 'store_id', 'employee_id', 'order_date', 'order_status', 'channel', 'order_currency', 'subtotal_amount', 'order_tax_amount', 'shipping_amount', 'order_discount_amount', 'total_amount', 'promo_code', 'payment_id', 'payment_method', 'payment_status', 'payment_amount', 'shipment_id', 'shipment_status', 'shipped_date', 'estimated_delivery_date', 'actual_delivery_date', 'shipping_cost', 'return_qty', 'return_refund_amount', 'exchange_rate', 'batch_id', 'run_id', 'source_row_num', 'reject_reason', 'is_valid_row')
    writer = _out_df_Write_Sales_Reject_File.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_rejects_.csv')

    # Step: Count Valid Rows (MemoryGroupBy) [converted]
    # Memory Group By: Count Valid Rows
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Valid_Rows = df_Write_Valid_Sales.groupBy().agg(count(lit(1)).alias('row_count'))

    # Step: Count Reject Rows (MemoryGroupBy) [converted]
    # Memory Group By: Count Reject Rows
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Reject_Rows = df_Write_Sales_Reject_File.groupBy().agg(count(lit(1)).alias('row_count'))

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
    # Pentaho filename: /logs/execution/sales/TR_Sales_Validation_
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
    writer.mode('overwrite').save(f'{data_dir}/TR_Sales_Validation_.log')

    # Step: Log Validation Complete (WriteToLog) [converted]
    # Write to Log: Log Validation Complete
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=VALIDATION_COMPLETE | TRANS=TR_Sales_Validation | RUN_ID=${RUN_ID}'
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
    _log_df_Log_Validation_Complete.info('AUDIT | EVENT=VALIDATION_COMPLETE | TRANS=TR_Sales_Validation | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Validation_Complete:
        _log_df_Log_Validation_Complete.info('Log Validation Complete' + ' | ' + str(_lr.asDict()))
    df_Log_Validation_Complete = df_Write_Validation_Log

    # Step: Validation Complete (Dummy) [converted]
    # Dummy: Validation Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Validation_Complete = df_Log_Validation_Complete

    log_event(_LOG, "transformation_end")
    return df_Dummy_Validation_Complete
