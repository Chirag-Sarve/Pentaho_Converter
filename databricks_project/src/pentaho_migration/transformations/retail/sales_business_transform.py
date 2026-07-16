"""PySpark module migrated from Pentaho transformation: TR_Sales_Business_Rules.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/cleansing/Sales_Business_Transform.ktr
Independent module — ``run(spark, config)`` returns a DataFrame.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_date,
    length,
    lit,
    lower,
    trim,
    upper,
    when,
    coalesce,
    md5,
    concat,
    split,
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

_LOG = get_logger("pentaho_migration.transformations.retail.sales_business_transform")



def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Sales_Business_Rules`` step-for-step."""
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

    # Step: Get Business Rule Vars (GetVariable) [converted]
    # Get Variables: Get Business Rule Vars
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'current_date', 'variable': '${CURRENT_DATE}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
    # preserved.output_columns=['batch_id', 'run_id', 'current_date']
    import os
    import re as _re_var
    # Lookup order: Databricks widgets → os.environ → spark.conf (pentaho.var.*) → transformation parameters → empty string
    df_Get_Business_Rule_Vars = spark.range(1).select(lit(1).alias('_row'))
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
    df_Get_Business_Rule_Vars = df_Get_Business_Rule_Vars.withColumn('batch_id', lit(_batch_id_resolved))
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
    df_Get_Business_Rule_Vars = df_Get_Business_Rule_Vars.withColumn('run_id', lit(_run_id_resolved))
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
    df_Get_Business_Rule_Vars = df_Get_Business_Rule_Vars.withColumn('current_date', lit(_current_date_resolved))

    # Step: Read Holidays For Flags (JsonInput) [converted]
    # JSON Input: Read Holidays For Flags
    df_Read_Holidays_For_Flags = spark.read.format('json').option('multiline', 'true').load('${PROJECT_HOME}/metadata/rules/sales_holidays.json')

    # Step: Read Valid Sales (CsvInput) [converted]
    # CSV Input: Read Valid Sales
    df_Read_Valid_Sales = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('order_item_id STRING, order_id STRING, product_id STRING, promotion_id STRING, quantity STRING, unit_price STRING, discount_amount STRING, line_total STRING, currency_code STRING, customer_id STRING, store_id STRING, employee_id STRING, order_date STRING, order_status STRING, channel STRING, order_currency STRING, subtotal_amount STRING, order_tax_amount STRING, shipping_amount STRING, order_discount_amount STRING, total_amount STRING, promo_code STRING, payment_id STRING, payment_method STRING, payment_status STRING, payment_amount STRING, shipment_id STRING, shipment_status STRING, shipped_date STRING, estimated_delivery_date STRING, actual_delivery_date STRING, shipping_cost STRING, return_qty STRING, return_refund_amount STRING, exchange_rate STRING, batch_id STRING, run_id STRING, source_row_num STRING')
        .load(f'{data_dir}/sales_valid_.csv')
    )

    # Step: Write Business Rules Rejects (TextFileOutput) [converted]
    # Pentaho step: Write Business Rules Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/sales/sales_business_rules_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='order_item_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_CODE' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_DESC' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Business_Rules_Rejects = df_Write_Business_Rules_Rejects
    _out_df_Write_Business_Rules_Rejects = df_Write_Business_Rules_Rejects.select('order_item_id', 'order_id', 'ERR_CODE', 'ERR_DESC', 'batch_id', 'run_id')
    writer = _out_df_Write_Business_Rules_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_business_rules_rejects_.csv')

    # Step: Prepare Holiday Lookup (SelectValues) [converted]
    # Select Values: Prepare Holiday Lookup
    df_Prepare_Holiday_Lookup = df_Read_Holidays_For_Flags.select(col("holiday_date").alias("hol_date"), col("holiday_name").alias("holiday_name"))

    # Step: Cast Business Rule Numerics (SelectValues) [converted]
    # Select Values: Cast Business Rule Numerics
    df_Cast_Business_Rule_Numerics = df_Read_Valid_Sales.select(col("order_item_id").alias("order_item_id"), col("order_id").alias("order_id"), col("product_id").alias("product_id"), col("promotion_id").alias("promotion_id"), col("quantity").alias("quantity"), col("unit_price").alias("unit_price"), col("discount_amount").alias("discount_amount"), col("line_total").alias("line_total"), col("currency_code").alias("currency_code"), col("customer_id").alias("customer_id"), col("store_id").alias("store_id"), col("employee_id").alias("employee_id"), col("order_date").alias("order_date"), col("order_status").alias("order_status"), col("channel").alias("channel"), col("order_currency").alias("order_currency"), col("subtotal_amount").alias("subtotal_amount"), col("order_tax_amount").alias("order_tax_amount"), col("shipping_amount").alias("shipping_amount"), col("order_discount_amount").alias("order_discount_amount"), col("total_amount").alias("total_amount"), col("promo_code").alias("promo_code"), col("payment_id").alias("payment_id"), col("payment_method").alias("payment_method"), col("payment_status").alias("payment_status"), col("payment_amount").alias("payment_amount"), col("shipment_id").alias("shipment_id"), col("shipment_status").alias("shipment_status"), col("shipped_date").alias("shipped_date"), col("estimated_delivery_date").alias("estimated_delivery_date"), col("actual_delivery_date").alias("actual_delivery_date"), col("shipping_cost").alias("shipping_cost"), col("return_qty").alias("return_qty"), col("return_refund_amount").alias("return_refund_amount"), col("exchange_rate").alias("exchange_rate"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"), col("source_row_num").alias("source_row_num"))

    # Step: Standardize Channel Text (StringOperations) [converted]
    # String Operations: Standardize Channel Text
    df_Standardize_Channel_Text = df_Cast_Business_Rule_Numerics
    df_Standardize_Channel_Text = df_Standardize_Channel_Text.withColumn("channel", upper(trim(col("channel").cast("string"))))
    df_Standardize_Channel_Text = df_Standardize_Channel_Text.withColumn("order_status", upper(trim(col("order_status").cast("string"))))

    # Step: Normalize Promo Code (ReplaceString) [partial]
    # ReplaceString: Normalize Promo Code
    df_Normalize_Promo_Code = df_Standardize_Channel_Text

    # Step: Default Null Amounts (IfNull) [converted]
    # If Field Value Is Null: Default Null Amounts
    df_Default_Null_Amounts = df_Normalize_Promo_Code
    df_Default_Null_Amounts = df_Default_Null_Amounts.withColumn('discount_amount', when(col('discount_amount').isNull(), lit(0)).otherwise(col('discount_amount')))
    df_Default_Null_Amounts = df_Default_Null_Amounts.withColumn('order_tax_amount', when(col('order_tax_amount').isNull(), lit(0)).otherwise(col('order_tax_amount')))
    df_Default_Null_Amounts = df_Default_Null_Amounts.withColumn('shipping_amount', when(col('shipping_amount').isNull(), lit(0)).otherwise(col('shipping_amount')))
    df_Default_Null_Amounts = df_Default_Null_Amounts.withColumn('shipping_cost', when(col('shipping_cost').isNull(), lit(0)).otherwise(col('shipping_cost')))
    df_Default_Null_Amounts = df_Default_Null_Amounts.withColumn('exchange_rate', when(col('exchange_rate').isNull(), lit(1)).otherwise(col('exchange_rate')))
    df_Default_Null_Amounts = df_Default_Null_Amounts.withColumn('return_qty', when(col('return_qty').isNull(), lit(0)).otherwise(col('return_qty')))
    df_Default_Null_Amounts = df_Default_Null_Amounts.withColumn('return_refund_amount', when(col('return_refund_amount').isNull(), lit(0)).otherwise(col('return_refund_amount')))

    # Step: Null If Unknown Promo (NullIf) [converted]
    # Null If: Null If Unknown Promo
    # preserved.fields=[{'name': 'promo_code', 'value': 'UNKNOWN', 'type': ''}]
    df_Null_If_Unknown_Promo = df_Default_Null_Amounts
    df_Null_If_Unknown_Promo = df_Null_If_Unknown_Promo.withColumn('promo_code', when((col('promo_code') == lit('UNKNOWN')), lit(None)).otherwise(col('promo_code')))

    # Step: Map Channel Codes (ValueMapper) [converted]
    # Value Mapper: Map Channel Codes
    df_Map_Channel_Codes = df_Null_If_Unknown_Promo.withColumn("channel_mapped", when((lower(col("channel")) == lower(lit('ONLINE'))), lit('ECOMMERCE')).when((lower(col("channel")) == lower(lit('WEB'))), lit('ECOMMERCE')).when((lower(col("channel")) == lower(lit('STORE'))), lit('RETAIL')).when((lower(col("channel")) == lower(lit('POS'))), lit('RETAIL')).when((lower(col("channel")) == lower(lit('MOBILE'))), lit('MOBILE_APP')).when((lower(col("channel")) == lower(lit('APP'))), lit('MOBILE_APP')).when((col("channel").isNull() | (col("channel") == lit(''))), col("channel")).otherwise(lit('OTHER')))
    # preserved.case_sensitive=False mappings=6 default='OTHER'

    # Step: Calculate Gross Net Shipping (Calculator) [converted]
    # Calculator: Calculate Gross Net Shipping
    df_Calculate_Gross_Net_Shipping = df_Map_Channel_Codes
    df_Calculate_Gross_Net_Shipping = df_Calculate_Gross_Net_Shipping.withColumn("gross_amount", ((col("quantity") * col("unit_price"))).cast('decimal(38,4)'))
    df_Calculate_Gross_Net_Shipping = df_Calculate_Gross_Net_Shipping.withColumn("net_amount", ((col("gross_amount") - col("discount_amount"))).cast('decimal(38,4)'))
    df_Calculate_Gross_Net_Shipping = df_Calculate_Gross_Net_Shipping.withColumn("shipping_cost_calc", (col("shipping_cost")).cast('decimal(38,4)'))

    # Step: Calculate Commercial Metrics (Formula) [converted]
    # Formula: Calculate Commercial Metrics
    df_Calculate_Commercial_Metrics = df_Calculate_Gross_Net_Shipping
    df_Calculate_Commercial_Metrics = df_Calculate_Commercial_Metrics.withColumn('formula_result', lit(None))  # empty formula

    # Step: Lookup Holiday Flag (StreamLookup) [failed]
    # Stream Lookup: Lookup Holiday Flag
    # StreamLookup 'Lookup Holiday Flag': no join keys — lookup join not generated
    df_Lookup_Holiday_Flag = df_Calculate_Commercial_Metrics

    # Step: Set Holiday Flag (Formula) [converted]
    # Formula: Set Holiday Flag
    df_Set_Holiday_Flag = df_Lookup_Holiday_Flag
    df_Set_Holiday_Flag = df_Set_Holiday_Flag.withColumn('formula_result', lit(None))  # empty formula

    # Step: Order Value Bands (NumberRange) [converted]
    # Number Range: Order Value Bands
    # Number Range semantics: lower_bound <= value < upper_bound (Pentaho NumberRangeRule)
    df_Order_Value_Bands = df_Set_Holiday_Flag.withColumn('order_value_band', when(col("total_revenue").isNull(), lit('UNKNOWN')).otherwise(when((col("total_revenue").cast("double") >= lit(0.0)) & (col("total_revenue").cast("double") < lit(50.0)), lit('MICRO')).when((col("total_revenue").cast("double") >= lit(50.0)) & (col("total_revenue").cast("double") < lit(200.0)), lit('SMALL')).when((col("total_revenue").cast("double") >= lit(200.0)) & (col("total_revenue").cast("double") < lit(1000.0)), lit('MEDIUM')).when((col("total_revenue").cast("double") >= lit(1000.0)) & (col("total_revenue").cast("double") < lit(9999999.0)), lit('LARGE')).otherwise(lit('UNKNOWN'))))
    # preserved.fallback='UNKNOWN' rules=4 lower_inclusive=True upper_inclusive=False

    # Step: Build Sales BK Hash Payload (ConcatFields) [converted]
    # Concat Fields: Build Sales BK Hash Payload
    df_Build_Sales_BK_Hash_Payload = df_Order_Value_Bands
    df_Build_Sales_BK_Hash_Payload = df_Build_Sales_BK_Hash_Payload.withColumn("sales_bk_payload", concat(concat(lit('"'), coalesce(col("order_item_id").cast("string"), lit("")), lit('"')), lit('|'), concat(lit('"'), coalesce(col("order_id").cast("string"), lit("")), lit('"')), lit('|'), concat(lit('"'), coalesce(col("product_id").cast("string"), lit("")), lit('"')), lit('|'), concat(lit('"'), coalesce(col("customer_id").cast("string"), lit("")), lit('"'))))
    # preserved.encoding='UTF-8'

    # Step: MD5 Sales BK Checksum (CheckSum) [converted]
    # Add a Checksum: MD5 Sales BK Checksum
    df_MD5_Sales_BK_Checksum = df_Build_Sales_BK_Hash_Payload
    df_MD5_Sales_BK_Checksum = df_MD5_Sales_BK_Checksum.withColumn("sales_bk_checksum", md5(coalesce(col("sales_bk_payload").cast("string"), lit(""))))
    # preserved.checksumtype='MD5' resultType='hexadecimal' fields=['sales_bk_payload']

    # Step: Split BK Payload Parts (FieldSplitter) [converted]
    # Split Fields: Split BK Payload Parts
    df_Split_BK_Payload_Parts = df_MD5_Sales_BK_Checksum
    df_Split_BK_Payload_Parts = df_Split_BK_Payload_Parts.withColumn("_parts_df_Split_BK_Payload_Parts", split(col("sales_bk_payload").cast("string"), '\\|'))
    # preserved.field 'bk_order_item' id='' idrem=False type='String' format='' group='' decimal='' currency='' length='-1' precision='-1' nullif='' ifnull='' trimtype='both'
    df_Split_BK_Payload_Parts = df_Split_BK_Payload_Parts.withColumn("bk_order_item", trim(element_at(col("_parts_df_Split_BK_Payload_Parts"), 1)))
    # preserved.field 'bk_order' id='' idrem=False type='String' format='' group='' decimal='' currency='' length='-1' precision='-1' nullif='' ifnull='' trimtype='both'
    df_Split_BK_Payload_Parts = df_Split_BK_Payload_Parts.withColumn("bk_order", trim(element_at(col("_parts_df_Split_BK_Payload_Parts"), 2)))
    # preserved.field 'bk_product' id='' idrem=False type='String' format='' group='' decimal='' currency='' length='-1' precision='-1' nullif='' ifnull='' trimtype='both'
    df_Split_BK_Payload_Parts = df_Split_BK_Payload_Parts.withColumn("bk_product", trim(element_at(col("_parts_df_Split_BK_Payload_Parts"), 3)))
    # preserved.field 'bk_customer' id='' idrem=False type='String' format='' group='' decimal='' currency='' length='-1' precision='-1' nullif='' ifnull='' trimtype='both'
    df_Split_BK_Payload_Parts = df_Split_BK_Payload_Parts.withColumn("bk_customer", trim(element_at(col("_parts_df_Split_BK_Payload_Parts"), 4)))
    df_Split_BK_Payload_Parts = df_Split_BK_Payload_Parts.drop("_parts_df_Split_BK_Payload_Parts", "sales_bk_payload")

    # Step: Unique Sales Hash Set (UniqueRowsByHashSet) [converted]
    # Unique Rows (HashSet): Unique Sales Hash Set
    # preserved.reject_duplicate_row=N error_description=''
    # preserved.store_values=True
    # preserved.count_rows=False count_field='count' compare_fields=['order_item_id', 'sales_bk_checksum']
    df_Unique_Sales_Hash_Set = df_Split_BK_Payload_Parts.dropDuplicates(["order_item_id", "sales_bk_checksum"])

    # Step: Clone Dual Currency Path (CloneRow) [converted]
    # Clone Row: Clone Dual Currency Path
    # preserved.nr_clones=1
    # preserved.nr_clone_in_field=False
    # preserved.add_clone_flag=False
    # preserved.clone_flag_field='cloneflag'
    # preserved.add_clone_num=False
    # preserved.clone_num_field='clonenum'
    # preserved.nr_clones_raw='1'
    _clone_parts_df_Clone_Dual_Currency_Path = []
    _base_df_Clone_Dual_Currency_Path = df_Unique_Sales_Hash_Set
    _orig_df_Clone_Dual_Currency_Path = _base_df_Clone_Dual_Currency_Path
    _clone_parts_df_Clone_Dual_Currency_Path.append(_orig_df_Clone_Dual_Currency_Path)
    for _ci in range(1, 1 + 1):
        _c = _base_df_Clone_Dual_Currency_Path
        _clone_parts_df_Clone_Dual_Currency_Path.append(_c)
    df_Clone_Dual_Currency_Path = _clone_parts_df_Clone_Dual_Currency_Path[0]
    for _part in _clone_parts_df_Clone_Dual_Currency_Path[1:]:
        df_Clone_Dual_Currency_Path = df_Clone_Dual_Currency_Path.unionByName(_part, allowMissingColumns=True)

    # Step: Route By Order Status (SwitchCase) [converted]
    # Switch / Case: Route By Order Status
    # preserved.fieldname='order_status'
    # preserved.switch_field='order_status'
    # preserved.cases=[{'value': 'COMPLETED', 'target_step': 'Tag Status Completed'}, {'value': 'SHIPPED', 'target_step': 'Tag Status Shipped'}, {'value': 'CANCELLED', 'target_step': 'Tag Status Cancelled'}, {'value': 'RETURNED', 'target_step': 'Tag Status Returned'}]
    # preserved.default_target_step='Tag Status Other'
    # preserved.use_contains=False
    # preserved.case_value_type='String'
    # preserved.rules=[{'value': 'COMPLETED', 'target_step': 'Tag Status Completed'}, {'value': 'SHIPPED', 'target_step': 'Tag Status Shipped'}, {'value': 'CANCELLED', 'target_step': 'Tag Status Cancelled'}, {'value': 'RETURNED', 'target_step': 'Tag Status Returned'}]
    _routed_df_Route_By_Order_Status = df_Clone_Dual_Currency_Path.withColumn('_route_Route_By_Order_Status', when(col("order_status") == lit('COMPLETED'), lit('Tag Status Completed')).when(col("order_status") == lit('SHIPPED'), lit('Tag Status Shipped')).when(col("order_status") == lit('CANCELLED'), lit('Tag Status Cancelled')).when(col("order_status") == lit('RETURNED'), lit('Tag Status Returned')).otherwise(lit('Tag Status Other')))
    df_Tag_Status_Completed = _routed_df_Route_By_Order_Status.filter(col('_route_Route_By_Order_Status') == lit('Tag Status Completed')).drop('_route_Route_By_Order_Status')
    df_Tag_Status_Shipped = _routed_df_Route_By_Order_Status.filter(col('_route_Route_By_Order_Status') == lit('Tag Status Shipped')).drop('_route_Route_By_Order_Status')
    df_Tag_Status_Cancelled = _routed_df_Route_By_Order_Status.filter(col('_route_Route_By_Order_Status') == lit('Tag Status Cancelled')).drop('_route_Route_By_Order_Status')
    df_Tag_Status_Returned = _routed_df_Route_By_Order_Status.filter(col('_route_Route_By_Order_Status') == lit('Tag Status Returned')).drop('_route_Route_By_Order_Status')
    df_Tag_Status_Other = _routed_df_Route_By_Order_Status.filter(col('_route_Route_By_Order_Status') == lit('Tag Status Other')).drop('_route_Route_By_Order_Status')
    df_Route_By_Order_Status = df_Tag_Status_Completed

    # Step: Tag Status Cancelled (Constant) [converted]
    # Add Constants: Tag Status Cancelled
    df_Tag_Status_Cancelled = df_Route_By_Order_Status
    df_Tag_Status_Cancelled = df_Tag_Status_Cancelled.withColumn("status_bucket", lit('CANCELLED'))
    # preserved.status_bucket: length='-1', precision='-1'

    # Step: Tag Status Completed (Constant) [converted]
    # Add Constants: Tag Status Completed
    df_Tag_Status_Completed = df_Route_By_Order_Status
    df_Tag_Status_Completed = df_Tag_Status_Completed.withColumn("status_bucket", lit('FULFILLED'))
    # preserved.status_bucket: length='-1', precision='-1'

    # Step: Tag Status Other (Constant) [converted]
    # Add Constants: Tag Status Other
    df_Tag_Status_Other = df_Route_By_Order_Status
    df_Tag_Status_Other = df_Tag_Status_Other.withColumn("status_bucket", lit('OPEN'))
    # preserved.status_bucket: length='-1', precision='-1'

    # Step: Tag Status Returned (Constant) [converted]
    # Add Constants: Tag Status Returned
    df_Tag_Status_Returned = df_Route_By_Order_Status
    df_Tag_Status_Returned = df_Tag_Status_Returned.withColumn("status_bucket", lit('RETURNED'))
    # preserved.status_bucket: length='-1', precision='-1'

    # Step: Tag Status Shipped (Constant) [converted]
    # Add Constants: Tag Status Shipped
    df_Tag_Status_Shipped = df_Route_By_Order_Status
    df_Tag_Status_Shipped = df_Tag_Status_Shipped.withColumn("status_bucket", lit('IN_TRANSIT'))
    # preserved.status_bucket: length='-1', precision='-1'

    # Step: Unify Status Routes (Dummy) [converted]
    # Dummy: Unify Status Routes
    # Pass-through step - DataFrame unchanged
    df_Dummy_Unify_Status_Routes = df_Tag_Status_Completed

    # Step: Average Basket Metrics (MemoryGroupBy) [partial]
    # Memory Group By: Average Basket Metrics
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Average_Basket_Metrics = df_Dummy_Unify_Status_Routes.groupBy().agg(avg(col("quantity")).alias('avg_basket_size'), avg(col("total_revenue")).alias('avg_order_value'))

    # Step: Cancelled For Aggregate? (FilterRows) [failed]
    # Filter Rows: Cancelled For Aggregate?
    df_Sum_Cancelled_Revenue = df_Dummy_Unify_Status_Routes.filter((col("order_status") == lit('CANCELLED')))
    df_Skip_Cancel_Aggregate = df_Dummy_Unify_Status_Routes.filter(~((col("order_status") == lit('CANCELLED'))))
    df_Cancelled_For_Aggregate? = df_Sum_Cancelled_Revenue

    # Step: Customer Lifetime Revenue (MemoryGroupBy) [partial]
    # Memory Group By: Customer Lifetime Revenue
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Customer_Lifetime_Revenue = df_Dummy_Unify_Status_Routes.groupBy('customer_id').agg(_sum(col("total_revenue")).alias('customer_lifetime_revenue'), countDistinct(col("order_id")).alias('customer_order_count'))

    # Step: Employee Sales Aggregate (MemoryGroupBy) [partial]
    # Memory Group By: Employee Sales Aggregate
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Employee_Sales_Aggregate = df_Dummy_Unify_Status_Routes.groupBy('employee_id').agg(_sum(col("total_revenue")).alias('employee_sales'), count(lit(1)).alias('employee_lines'))

    # Step: Returned Revenue Aggregate (MemoryGroupBy) [partial]
    # Memory Group By: Returned Revenue Aggregate
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Returned_Revenue_Aggregate = df_Dummy_Unify_Status_Routes.groupBy().agg(_sum(col("return_amount")).alias('returned_revenue'), _sum(col("total_revenue")).alias('cancelled_revenue'))

    # Step: Shipping Performance Aggregate (MemoryGroupBy) [converted]
    # Memory Group By: Shipping Performance Aggregate
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Shipping_Performance_Aggregate = df_Dummy_Unify_Status_Routes.groupBy('late_delivery_flag').agg(count(lit(1)).alias('shipment_count'))

    # Step: Store Revenue Aggregate (MemoryGroupBy) [partial]
    # Memory Group By: Store Revenue Aggregate
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Store_Revenue_Aggregate = df_Dummy_Unify_Status_Routes.groupBy('store_id').agg(_sum(col("total_revenue")).alias('store_revenue'), countDistinct(col("order_id")).alias('store_orders'))

    # Step: Write Basket Metrics (TextFileOutput) [converted]
    # Pentaho step: Write Basket Metrics (type: TextFileOutput)
    # Pentaho filename: /output/sales/enriched/sales_basket_metrics_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='avg_basket_size' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='avg_order_value' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Basket_Metrics = df_Average_Basket_Metrics
    _out_df_Write_Basket_Metrics = df_Write_Basket_Metrics.select('avg_basket_size', 'avg_order_value')
    writer = _out_df_Write_Basket_Metrics.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_basket_metrics_.csv')

    # Step: Skip Cancel Aggregate (Dummy) [converted]
    # Dummy: Skip Cancel Aggregate
    # Pass-through step - DataFrame unchanged
    df_Dummy_Skip_Cancel_Aggregate = df_Skip_Cancel_Aggregate

    # Step: Sum Cancelled Revenue (MemoryGroupBy) [failed]
    # Memory Group By: Sum Cancelled Revenue
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Sum_Cancelled_Revenue = df_Cancelled_For_Aggregate?.groupBy().agg(_sum(col("total_revenue")).alias('cancelled_revenue'))

    # Step: Attach Customer LTV (StreamLookup) [failed]
    # Stream Lookup: Attach Customer LTV
    # StreamLookup 'Attach Customer LTV': no join keys — lookup join not generated
    df_Attach_Customer_LTV = df_Dummy_Unify_Status_Routes

    # Step: Write CLTV Aggregate (TextFileOutput) [converted]
    # Pentaho step: Write CLTV Aggregate (type: TextFileOutput)
    # Pentaho filename: /output/sales/enriched/sales_customer_ltv_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='customer_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='customer_lifetime_revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='customer_order_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_CLTV_Aggregate = df_Customer_Lifetime_Revenue
    _out_df_Write_CLTV_Aggregate = df_Write_CLTV_Aggregate.select('customer_id', 'customer_lifetime_revenue', 'customer_order_count')
    writer = _out_df_Write_CLTV_Aggregate.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_customer_ltv_.csv')

    # Step: Write Employee Sales Aggregate (TextFileOutput) [converted]
    # Pentaho step: Write Employee Sales Aggregate (type: TextFileOutput)
    # Pentaho filename: /output/sales/enriched/sales_employee_sales_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='employee_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='employee_sales' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='employee_lines' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Employee_Sales_Aggregate = df_Employee_Sales_Aggregate
    _out_df_Write_Employee_Sales_Aggregate = df_Write_Employee_Sales_Aggregate.select('employee_id', 'employee_sales', 'employee_lines')
    writer = _out_df_Write_Employee_Sales_Aggregate.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_employee_sales_.csv')

    # Step: Write Store Revenue Aggregate (TextFileOutput) [converted]
    # Pentaho step: Write Store Revenue Aggregate (type: TextFileOutput)
    # Pentaho filename: /output/sales/enriched/sales_store_revenue_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='store_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Store_Revenue_Aggregate = df_Store_Revenue_Aggregate
    _out_df_Write_Store_Revenue_Aggregate = df_Write_Store_Revenue_Aggregate.select('store_id', 'store_revenue', 'store_orders')
    writer = _out_df_Write_Store_Revenue_Aggregate.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_store_revenue_.csv')

    # Step: Attach Store Revenue (StreamLookup) [failed]
    # Stream Lookup: Attach Store Revenue
    # StreamLookup 'Attach Store Revenue': no join keys — lookup join not generated
    df_Attach_Store_Revenue = df_Store_Revenue_Aggregate

    # Step: Attach Employee Sales (StreamLookup) [failed]
    # Stream Lookup: Attach Employee Sales
    # StreamLookup 'Attach Employee Sales': no join keys — lookup join not generated
    df_Attach_Employee_Sales = df_Employee_Sales_Aggregate

    # Step: Regional Revenue Proxy (Formula) [converted]
    # Formula: Regional Revenue Proxy
    df_Regional_Revenue_Proxy = df_Attach_Employee_Sales
    df_Regional_Revenue_Proxy = df_Regional_Revenue_Proxy.withColumn('formula_result', lit(None))  # empty formula

    # Step: Write Enriched Sales (TextFileOutput) [converted]
    # Pentaho step: Write Enriched Sales (type: TextFileOutput)
    # Pentaho filename: /output/sales/enriched/sales_enriched_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='order_item_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='promotion_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='customer_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='employee_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='status_bucket' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='channel' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='channel_mapped' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='currency_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quantity' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unit_price' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gross_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='discount_amount_calc' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='tax_amount_calc' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipping_cost_calc' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='total_revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='margin' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='fx_rate' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='converted_amount_usd' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='return_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='refund_amount_calc' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='late_delivery_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='high_value_order_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='weekend_order_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='holiday_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_value_band' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sales_bk_checksum' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='customer_lifetime_revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='regional_revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='employee_sales' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_method' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipment_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='promo_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Enriched_Sales = df_Regional_Revenue_Proxy
    _out_df_Write_Enriched_Sales = df_Write_Enriched_Sales.select('order_item_id', 'order_id', 'product_id', 'promotion_id', 'customer_id', 'store_id', 'employee_id', 'order_date', 'order_status', 'status_bucket', 'channel', 'channel_mapped', 'currency_code', 'quantity', 'unit_price', 'gross_amount', 'discount_amount_calc', 'net_amount', 'tax_amount_calc', 'shipping_cost_calc', 'total_revenue', 'profit', 'margin', 'fx_rate', 'converted_amount_usd', 'return_amount', 'refund_amount_calc', 'late_delivery_flag', 'high_value_order_flag', 'weekend_order_flag', 'holiday_flag', 'order_value_band', 'sales_bk_checksum', 'customer_lifetime_revenue', 'store_revenue', 'regional_revenue', 'employee_sales', 'payment_id', 'payment_method', 'shipment_id', 'promo_code', 'batch_id', 'run_id')
    writer = _out_df_Write_Enriched_Sales.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_enriched_.csv')

    # Step: Log Business Rules Complete (WriteToLog) [converted]
    # Write to Log: Log Business Rules Complete
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=BUSINESS_RULES_COMPLETE | TRANS=TR_Sales_Business_Rules | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Business_Rules_Complete = logging.getLogger('pentaho.writetolog.Log_Business_Rules_Complete')
    _log_df_Log_Business_Rules_Complete.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Business_Rules_Complete = df_Write_Enriched_Sales
    _log_rows_df_Log_Business_Rules_Complete = _log_df_df_Log_Business_Rules_Complete.take(20)
    _log_df_Log_Business_Rules_Complete.info('Log Business Rules Complete' + ' | columns=' + str(_log_df_df_Log_Business_Rules_Complete.columns))
    _log_df_Log_Business_Rules_Complete.info('AUDIT | EVENT=BUSINESS_RULES_COMPLETE | TRANS=TR_Sales_Business_Rules | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Business_Rules_Complete:
        _log_df_Log_Business_Rules_Complete.info('Log Business Rules Complete' + ' | ' + str(_lr.asDict()))
    df_Log_Business_Rules_Complete = df_Write_Enriched_Sales

    # Step: Copy Enriched To Result (RowsToResult) [converted]
    # Copy Rows to Result: Copy Enriched To Result
    # preserved.result_buffer='rows'
    # preserved.preserve_order=True
    # LIMITATION: Pentaho Result rows are job-level; Databricks uses a notebook-scoped buffer (_pentaho_result_rows) for downstream hops / orchestration. Cross-job Result transfer needs Databricks Jobs task values or persisted Delta tables.
    _pentaho_result_rows = globals().setdefault('_pentaho_result_rows', {})
    _pentaho_result_files = globals().setdefault('_pentaho_result_files', [])
    # Preserve schema and relative ordering for 'Copy Enriched To Result'
    _result_rows_df_Copy_Enriched_To_Result = df_Log_Business_Rules_Complete
    _pentaho_result_rows['Copy Enriched To Result'] = _result_rows_df_Copy_Enriched_To_Result
    _pentaho_result_rows['__latest__'] = _result_rows_df_Copy_Enriched_To_Result
    df_Copy_Enriched_To_Result = df_Log_Business_Rules_Complete

    # Step: Business Rules Complete (Dummy) [converted]
    # Dummy: Business Rules Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Business_Rules_Complete = df_Returned_Revenue_Aggregate

    log_event(_LOG, "transformation_end")
    return df_Dummy_Business_Rules_Complete
