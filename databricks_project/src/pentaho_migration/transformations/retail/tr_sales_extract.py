"""PySpark module migrated from Pentaho transformation: TR_Sales_Extract.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/staging/TR_Sales_Extract.ktr
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_sales_extract")

from pyspark.sql.window import Window


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Sales_Extract`` step-for-step."""
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

    # Step: Generate Extract Control Row (RowGenerator) [converted]
    # Generate Rows: Generate Extract Control Row
    data = [('SALES', 'FULL')]
    df_Generate_Extract_Control_Row = spark.createDataFrame(data, ['extract_module', 'extract_mode'])

    # Step: Read exchange_rates.csv (CsvInput) [converted]
    # CSV Input: Read exchange_rates.csv
    df_Read_exchange_rates.csv = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('rate_id STRING, rate_date STRING, base_currency STRING, quote_currency STRING, exchange_rate STRING, source STRING')
        .load('/exchange_rates.csv')
    )

    # Step: Read order_items.csv (CsvInput) [converted]
    # CSV Input: Read order_items.csv
    df_Read_order_items.csv = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('order_item_id STRING, order_id STRING, product_id STRING, promotion_id STRING, quantity STRING, unit_price STRING, discount_amount STRING, line_total STRING, currency_code STRING')
        .load('/order_items.csv')
    )

    # Step: Read orders.csv (CsvInput) [converted]
    # CSV Input: Read orders.csv
    df_Read_orders.csv = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('order_id STRING, customer_id STRING, store_id STRING, employee_id STRING, order_date STRING, order_status STRING, channel STRING, currency_code STRING, subtotal_amount STRING, tax_amount STRING, shipping_amount STRING, discount_amount STRING, total_amount STRING, promo_code STRING')
        .load('/orders.csv')
    )

    # Step: Read payments.csv (CsvInput) [converted]
    # CSV Input: Read payments.csv
    df_Read_payments.csv = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('payment_id STRING, order_id STRING, payment_method STRING, payment_status STRING, payment_date STRING, amount STRING, currency_code STRING, card_brand STRING, transaction_ref STRING, gateway STRING')
        .load('/payments.csv')
    )

    # Step: Read returns.csv (CsvInput) [converted]
    # CSV Input: Read returns.csv
    df_Read_returns.csv = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('return_id STRING, order_item_id STRING, order_id STRING, product_id STRING, return_date STRING, return_reason STRING, return_status STRING, quantity_returned STRING, refund_amount STRING, restocking_fee STRING, notes STRING')
        .load('/returns.csv')
    )

    # Step: Read shipments.csv (TextFileInput) [converted]
    # Pentaho step: Read shipments.csv (type: TextFileInput)
    # INFO: preserved Legacy Text File Input option: date_format_lenient='Y'
    # Pentaho filename: /shipments.csv
    # NOTE: Spark CSV outputs are directories — load the same path written by Text File Output (not an individual part-*.csv file)
    # NOTE: missing/empty/corrupt files fail or yield empty DataFrames at Spark runtime (use PERMISSIVE mode / upstream path checks as needed)
    df_Read_shipments.csv = (
        spark.read
        .option("header", True)
        .option("sep", ',')
        .option("quote", '"')
        .option("encoding", 'UTF-8')
        .option("inferSchema", False)
        .schema('shipment_id STRING, order_id STRING, shipping_company STRING, shipping_method STRING, tracking_number STRING, shipment_status STRING, shipped_date STRING, estimated_delivery_date STRING, actual_delivery_date STRING, shipping_cost STRING, origin_warehouse STRING, destination_postal_code STRING')
        .csv(f'{data_dir}/shipments.csv')
    )
    # INFO: preserved.field_format name='shipment_id' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='order_id' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='shipping_company' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='shipping_method' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='tracking_number' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='shipment_status' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='shipped_date' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='estimated_delivery_date' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='actual_delivery_date' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='shipping_cost' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='origin_warehouse' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    # INFO: preserved.field_format name='destination_postal_code' options={'precision': -1, 'position': '-1', 'repeat': 'N'}
    df_Read_shipments.csv = df_Read_shipments.csv.select(col('shipment_id').alias('shipment_id'), col('order_id').alias('order_id'), col('shipping_company').alias('shipping_company'), col('shipping_method').alias('shipping_method'), col('tracking_number').alias('tracking_number'), col('shipment_status').alias('shipment_status'), col('shipped_date').alias('shipped_date'), col('estimated_delivery_date').alias('estimated_delivery_date'), col('actual_delivery_date').alias('actual_delivery_date'), col('shipping_cost').alias('shipping_cost'), col('origin_warehouse').alias('origin_warehouse'), col('destination_postal_code').alias('destination_postal_code'))
    df_Read_shipments.csv = df_Read_shipments.csv.filter(~((col('shipment_id').isNull() | (length(trim(col('shipment_id').cast('string'))) == 0)) & (col('order_id').isNull() | (length(trim(col('order_id').cast('string'))) == 0)) & (col('shipping_company').isNull() | (length(trim(col('shipping_company').cast('string'))) == 0)) & (col('shipping_method').isNull() | (length(trim(col('shipping_method').cast('string'))) == 0)) & (col('tracking_number').isNull() | (length(trim(col('tracking_number').cast('string'))) == 0)) & (col('shipment_status').isNull() | (length(trim(col('shipment_status').cast('string'))) == 0)) & (col('shipped_date').isNull() | (length(trim(col('shipped_date').cast('string'))) == 0)) & (col('estimated_delivery_date').isNull() | (length(trim(col('estimated_delivery_date').cast('string'))) == 0)) & (col('actual_delivery_date').isNull() | (length(trim(col('actual_delivery_date').cast('string'))) == 0)) & (col('shipping_cost').isNull() | (length(trim(col('shipping_cost').cast('string'))) == 0)) & (col('origin_warehouse').isNull() | (length(trim(col('origin_warehouse').cast('string'))) == 0)) & (col('destination_postal_code').isNull() | (length(trim(col('destination_postal_code').cast('string'))) == 0))))
    df_Read_shipments.csv = df_Read_shipments.csv.withColumn('source_row_num', monotonically_increasing_id())

    # Step: Write Extract Rejects (TextFileOutput) [converted]
    # Pentaho step: Write Extract Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/sales/sales_extract_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='order_item_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_CODE' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_DESC' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_FIELDS' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Extract_Rejects = df_Write_Extract_Rejects
    _out_df_Write_Extract_Rejects = df_Write_Extract_Rejects.select('order_item_id', 'order_id', 'product_id', 'ERR_CODE', 'ERR_DESC', 'ERR_FIELDS', 'batch_id', 'run_id')
    writer = _out_df_Write_Extract_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_extract_rejects_.csv')

    # Step: Get Variables (GetVariable) [converted]
    # Get Variables: Get Variables
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'dataset_path', 'variable': '${DATASET_PATH}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'project_home', 'variable': '${PROJECT_HOME}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'current_date', 'variable': '${CURRENT_DATE}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
    # preserved.output_columns=['batch_id', 'run_id', 'dataset_path', 'project_home', 'current_date']
    import os
    import re as _re_var
    # Lookup order: Databricks widgets → os.environ → spark.conf (pentaho.var.*) → transformation parameters → empty string
    df_Get_Variables = df_Generate_Extract_Control_Row
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
    # field 'dataset_path' from variable string '${DATASET_PATH}'
    # preserved.field.dataset_path.trim_type='none'
    # preserved.field.dataset_path.type='String'
    _dataset_path_resolved = None
    _dbu__dataset_path_resolved = globals().get('dbutils')
    if _dbu__dataset_path_resolved is not None and hasattr(_dbu__dataset_path_resolved, 'widgets'):
        try:
            _dataset_path_resolved = _dbu__dataset_path_resolved.widgets.get('DATASET_PATH')
        except Exception:
            _dataset_path_resolved = None
    if _dataset_path_resolved in (None, ''):
        import os as _os__dataset_path_resolved
        _dataset_path_resolved = _os__dataset_path_resolved.environ.get('DATASET_PATH')
    if _dataset_path_resolved in (None, ''):
        try:
            _dataset_path_resolved = spark.conf.get('pentaho.var.DATASET_PATH')
        except Exception:
            _dataset_path_resolved = None
    if _dataset_path_resolved in (None, ''):
        _dataset_path_resolved = ''
    if _dataset_path_resolved is None:
        _dataset_path_resolved = ''
    df_Get_Variables = df_Get_Variables.withColumn('dataset_path', lit(_dataset_path_resolved))
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
    df_Get_Variables = df_Get_Variables.withColumn('project_home', lit(_project_home_resolved))
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
    df_Get_Variables = df_Get_Variables.withColumn('current_date', lit(_current_date_resolved))

    # Step: Prepare FX Lookup (SelectValues) [converted]
    # Select Values: Prepare FX Lookup
    df_Prepare_FX_Lookup = df_Read_exchange_rates.csv.select(col("base_currency").alias("fx_base"), col("quote_currency").alias("fx_quote"), col("exchange_rate").alias("exchange_rate"), col("rate_date").alias("rate_date"))

    # Step: Write Staging Land FX (TextFileOutput) [converted]
    # Pentaho step: Write Staging Land FX (type: TextFileOutput)
    # Pentaho filename: /output/sales/staging/stg_raw_exchange_rates_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='rate_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rate_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='base_currency' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quote_currency' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='exchange_rate' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='source' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Staging_Land_FX = df_Read_exchange_rates.csv
    _out_df_Write_Staging_Land_FX = df_Write_Staging_Land_FX.select('rate_id', 'rate_date', 'base_currency', 'quote_currency', 'exchange_rate', 'source')
    writer = _out_df_Write_Staging_Land_FX.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_raw_exchange_rates_.csv')

    # Step: Sample Order Items Peek (SampleRows) [converted]
    # Sample Rows: Sample Order Items Peek
    _w_sr_df_Sample_Order_Items_Peek = Window.orderBy(monotonically_increasing_id())
    df_Sample_Order_Items_Peek = df_Read_order_items.csv.withColumn('_sr_rn', row_number().over(_w_sr_df_Sample_Order_Items_Peek))
    # preserved.lines_range='1..5' ranges=[(1, 5)]
    df_Sample_Order_Items_Peek = df_Sample_Order_Items_Peek.filter(((col('_sr_rn') >= 1) & (col('_sr_rn') <= 5)))
    df_Sample_Order_Items_Peek = df_Sample_Order_Items_Peek.drop('_sr_rn')

    # Step: Write Staging Land Order Items (TextFileOutput) [converted]
    # Pentaho step: Write Staging Land Order Items (type: TextFileOutput)
    # Pentaho filename: /output/sales/staging/stg_raw_order_items_
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
    df_Write_Staging_Land_Order_Items = df_Read_order_items.csv
    _out_df_Write_Staging_Land_Order_Items = df_Write_Staging_Land_Order_Items.select('order_item_id', 'order_id', 'product_id', 'promotion_id', 'quantity', 'unit_price', 'discount_amount', 'line_total', 'currency_code')
    writer = _out_df_Write_Staging_Land_Order_Items.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_raw_order_items_.csv')

    # Step: Prepare Orders Join Keys (SelectValues) [converted]
    # Select Values: Prepare Orders Join Keys
    df_Prepare_Orders_Join_Keys = df_Read_orders.csv.select(col("order_id").alias("ord_order_id"), col("customer_id").alias("customer_id"), col("store_id").alias("store_id"), col("employee_id").alias("employee_id"), col("order_date").alias("order_date"), col("order_status").alias("order_status"), col("channel").alias("channel"), col("currency_code").alias("order_currency"), col("subtotal_amount").alias("subtotal_amount"), col("tax_amount").alias("order_tax_amount"), col("shipping_amount").alias("shipping_amount"), col("discount_amount").alias("order_discount_amount"), col("total_amount").alias("total_amount"), col("promo_code").alias("promo_code"))

    # Step: Write Staging Land Orders (TextFileOutput) [converted]
    # Pentaho step: Write Staging Land Orders (type: TextFileOutput)
    # Pentaho filename: /output/sales/staging/stg_raw_orders_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='customer_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='employee_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='channel' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='currency_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='subtotal_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='tax_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipping_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='discount_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='total_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='promo_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Staging_Land_Orders = df_Read_orders.csv
    _out_df_Write_Staging_Land_Orders = df_Write_Staging_Land_Orders.select('order_id', 'customer_id', 'store_id', 'employee_id', 'order_date', 'order_status', 'channel', 'currency_code', 'subtotal_amount', 'tax_amount', 'shipping_amount', 'discount_amount', 'total_amount', 'promo_code')
    writer = _out_df_Write_Staging_Land_Orders.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_raw_orders_.csv')

    # Step: Prepare Payments Lookup (SelectValues) [converted]
    # Select Values: Prepare Payments Lookup
    df_Prepare_Payments_Lookup = df_Read_payments.csv.select(col("order_id").alias("pay_order_id"), col("payment_id").alias("payment_id"), col("payment_method").alias("payment_method"), col("payment_status").alias("payment_status"), col("amount").alias("payment_amount"))

    # Step: Write Staging Land Payments (TextFileOutput) [converted]
    # Pentaho step: Write Staging Land Payments (type: TextFileOutput)
    # Pentaho filename: /output/sales/staging/stg_raw_payments_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='payment_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_method' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='currency_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='card_brand' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='transaction_ref' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='gateway' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Staging_Land_Payments = df_Read_payments.csv
    _out_df_Write_Staging_Land_Payments = df_Write_Staging_Land_Payments.select('payment_id', 'order_id', 'payment_method', 'payment_status', 'payment_date', 'amount', 'currency_code', 'card_brand', 'transaction_ref', 'gateway')
    writer = _out_df_Write_Staging_Land_Payments.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_raw_payments_.csv')

    # Step: Aggregate Returns By Item (MemoryGroupBy) [converted]
    # Memory Group By: Aggregate Returns By Item
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Aggregate_Returns_By_Item = df_Read_returns.csv.groupBy('order_item_id').agg(_sum(col("quantity_returned")).alias('return_qty'), _sum(col("refund_amount")).alias('return_refund_amount'), count(lit(1)).alias('return_count'))

    # Step: Write Staging Land Returns (TextFileOutput) [converted]
    # Pentaho step: Write Staging Land Returns (type: TextFileOutput)
    # Pentaho filename: /output/sales/staging/stg_raw_returns_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='return_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_item_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='return_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='return_reason' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='return_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quantity_returned' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='refund_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='restocking_fee' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='notes' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Staging_Land_Returns = df_Read_returns.csv
    _out_df_Write_Staging_Land_Returns = df_Write_Staging_Land_Returns.select('return_id', 'order_item_id', 'order_id', 'product_id', 'return_date', 'return_reason', 'return_status', 'quantity_returned', 'refund_amount', 'restocking_fee', 'notes')
    writer = _out_df_Write_Staging_Land_Returns.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_raw_returns_.csv')

    # Step: Prepare Shipments Lookup (SelectValues) [converted]
    # Select Values: Prepare Shipments Lookup
    df_Prepare_Shipments_Lookup = df_Read_shipments.csv.select(col("order_id").alias("ship_order_id"), col("shipment_id").alias("shipment_id"), col("shipment_status").alias("shipment_status"), col("shipped_date").alias("shipped_date"), col("estimated_delivery_date").alias("estimated_delivery_date"), col("actual_delivery_date").alias("actual_delivery_date"), col("shipping_cost").alias("shipping_cost"))

    # Step: Write Staging Land Shipments (TextFileOutput) [converted]
    # Pentaho step: Write Staging Land Shipments (type: TextFileOutput)
    # Pentaho filename: /output/sales/staging/stg_raw_shipments_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='shipment_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipping_company' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipping_method' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='tracking_number' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipment_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipped_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='estimated_delivery_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='actual_delivery_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipping_cost' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='origin_warehouse' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='destination_postal_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='source_row_num' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Staging_Land_Shipments = df_Read_shipments.csv
    _out_df_Write_Staging_Land_Shipments = df_Write_Staging_Land_Shipments.select('shipment_id', 'order_id', 'shipping_company', 'shipping_method', 'tracking_number', 'shipment_status', 'shipped_date', 'estimated_delivery_date', 'actual_delivery_date', 'shipping_cost', 'origin_warehouse', 'destination_postal_code', 'source_row_num')
    writer = _out_df_Write_Staging_Land_Shipments.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_raw_shipments_.csv')

    # Step: Validate exchange_rates.csv (GetFileNames) [failed]
    # Get File Names: Validate exchange_rates.csv
    _list_path_df_Validate_exchange_rates.csv = '/'
    try:
        _fs_entries_df_Validate_exchange_rates.csv = dbutils.fs.ls(_list_path_df_Validate_exchange_rates.csv)
        df_Validate_exchange_rates.csv = spark.createDataFrame(
            [(e.path, e.name, e.size, e.modificationTime) for e in _fs_entries_df_Validate_exchange_rates.csv],
            ['filename', 'short_filename', 'size', 'last_modified']
        )
    except Exception:
        # Fallback: Hadoop FileSystem listing via SparkContext
        _jpath = spark._jvm.org.apache.hadoop.fs.Path(_list_path_df_Validate_exchange_rates.csv)
        _fs = _jpath.getFileSystem(spark._jsc.hadoopConfiguration())
        _statuses = _fs.listStatus(_jpath)
        df_Validate_exchange_rates.csv = spark.createDataFrame(
            [(s.getPath().toString(), s.getPath().getName(), s.getLen(), s.getModificationTime())
             for s in _statuses if s.isFile()],
            ['filename', 'short_filename', 'size', 'last_modified']
        )

    # Step: Validate order_items.csv (GetFileNames) [failed]
    # Get File Names: Validate order_items.csv
    _list_path_df_Validate_order_items.csv = '/'
    try:
        _fs_entries_df_Validate_order_items.csv = dbutils.fs.ls(_list_path_df_Validate_order_items.csv)
        df_Validate_order_items.csv = spark.createDataFrame(
            [(e.path, e.name, e.size, e.modificationTime) for e in _fs_entries_df_Validate_order_items.csv],
            ['filename', 'short_filename', 'size', 'last_modified']
        )
    except Exception:
        # Fallback: Hadoop FileSystem listing via SparkContext
        _jpath = spark._jvm.org.apache.hadoop.fs.Path(_list_path_df_Validate_order_items.csv)
        _fs = _jpath.getFileSystem(spark._jsc.hadoopConfiguration())
        _statuses = _fs.listStatus(_jpath)
        df_Validate_order_items.csv = spark.createDataFrame(
            [(s.getPath().toString(), s.getPath().getName(), s.getLen(), s.getModificationTime())
             for s in _statuses if s.isFile()],
            ['filename', 'short_filename', 'size', 'last_modified']
        )

    # Step: Validate orders.csv (GetFileNames) [failed]
    # Get File Names: Validate orders.csv
    _list_path_df_Validate_orders.csv = '/'
    try:
        _fs_entries_df_Validate_orders.csv = dbutils.fs.ls(_list_path_df_Validate_orders.csv)
        df_Validate_orders.csv = spark.createDataFrame(
            [(e.path, e.name, e.size, e.modificationTime) for e in _fs_entries_df_Validate_orders.csv],
            ['filename', 'short_filename', 'size', 'last_modified']
        )
    except Exception:
        # Fallback: Hadoop FileSystem listing via SparkContext
        _jpath = spark._jvm.org.apache.hadoop.fs.Path(_list_path_df_Validate_orders.csv)
        _fs = _jpath.getFileSystem(spark._jsc.hadoopConfiguration())
        _statuses = _fs.listStatus(_jpath)
        df_Validate_orders.csv = spark.createDataFrame(
            [(s.getPath().toString(), s.getPath().getName(), s.getLen(), s.getModificationTime())
             for s in _statuses if s.isFile()],
            ['filename', 'short_filename', 'size', 'last_modified']
        )

    # Step: Validate payments.csv (GetFileNames) [failed]
    # Get File Names: Validate payments.csv
    _list_path_df_Validate_payments.csv = '/'
    try:
        _fs_entries_df_Validate_payments.csv = dbutils.fs.ls(_list_path_df_Validate_payments.csv)
        df_Validate_payments.csv = spark.createDataFrame(
            [(e.path, e.name, e.size, e.modificationTime) for e in _fs_entries_df_Validate_payments.csv],
            ['filename', 'short_filename', 'size', 'last_modified']
        )
    except Exception:
        # Fallback: Hadoop FileSystem listing via SparkContext
        _jpath = spark._jvm.org.apache.hadoop.fs.Path(_list_path_df_Validate_payments.csv)
        _fs = _jpath.getFileSystem(spark._jsc.hadoopConfiguration())
        _statuses = _fs.listStatus(_jpath)
        df_Validate_payments.csv = spark.createDataFrame(
            [(s.getPath().toString(), s.getPath().getName(), s.getLen(), s.getModificationTime())
             for s in _statuses if s.isFile()],
            ['filename', 'short_filename', 'size', 'last_modified']
        )

    # Step: Validate returns.csv (GetFileNames) [failed]
    # Get File Names: Validate returns.csv
    _list_path_df_Validate_returns.csv = '/'
    try:
        _fs_entries_df_Validate_returns.csv = dbutils.fs.ls(_list_path_df_Validate_returns.csv)
        df_Validate_returns.csv = spark.createDataFrame(
            [(e.path, e.name, e.size, e.modificationTime) for e in _fs_entries_df_Validate_returns.csv],
            ['filename', 'short_filename', 'size', 'last_modified']
        )
    except Exception:
        # Fallback: Hadoop FileSystem listing via SparkContext
        _jpath = spark._jvm.org.apache.hadoop.fs.Path(_list_path_df_Validate_returns.csv)
        _fs = _jpath.getFileSystem(spark._jsc.hadoopConfiguration())
        _statuses = _fs.listStatus(_jpath)
        df_Validate_returns.csv = spark.createDataFrame(
            [(s.getPath().toString(), s.getPath().getName(), s.getLen(), s.getModificationTime())
             for s in _statuses if s.isFile()],
            ['filename', 'short_filename', 'size', 'last_modified']
        )

    # Step: Validate shipments.csv (GetFileNames) [failed]
    # Get File Names: Validate shipments.csv
    _list_path_df_Validate_shipments.csv = '/'
    try:
        _fs_entries_df_Validate_shipments.csv = dbutils.fs.ls(_list_path_df_Validate_shipments.csv)
        df_Validate_shipments.csv = spark.createDataFrame(
            [(e.path, e.name, e.size, e.modificationTime) for e in _fs_entries_df_Validate_shipments.csv],
            ['filename', 'short_filename', 'size', 'last_modified']
        )
    except Exception:
        # Fallback: Hadoop FileSystem listing via SparkContext
        _jpath = spark._jvm.org.apache.hadoop.fs.Path(_list_path_df_Validate_shipments.csv)
        _fs = _jpath.getFileSystem(spark._jsc.hadoopConfiguration())
        _statuses = _fs.listStatus(_jpath)
        df_Validate_shipments.csv = spark.createDataFrame(
            [(s.getPath().toString(), s.getPath().getName(), s.getLen(), s.getModificationTime())
             for s in _statuses if s.isFile()],
            ['filename', 'short_filename', 'size', 'last_modified']
        )

    # Step: Capture Extract Timestamp (SystemInfo) [converted]
    # System Info: Capture Extract Timestamp
    df_Capture_Extract_Timestamp = df_Sample_Order_Items_Peek
    df_Capture_Extract_Timestamp = df_Capture_Extract_Timestamp.withColumn("extract_ts", current_date())
    df_Capture_Extract_Timestamp = df_Capture_Extract_Timestamp.withColumn("extract_start", current_date())

    # Step: Sort Orders By Order ID (SortRows) [converted]
    # Sort Rows: Sort Orders By Order ID
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Orders_By_Order_ID = df_Prepare_Orders_Join_Keys
    _sort_df_Sort_Orders_By_Order_ID = _sort_df_Sort_Orders_By_Order_ID.withColumn("_sort_ci_ord_order_id", lower(col("ord_order_id").cast("string")))
    df_Sort_Orders_By_Order_ID = _sort_df_Sort_Orders_By_Order_ID.orderBy(col("_sort_ci_ord_order_id").asc_nulls_last())
    df_Sort_Orders_By_Order_ID = df_Sort_Orders_By_Order_ID.drop("_sort_ci_ord_order_id")

    # Step: Prepare Returns Lookup (SelectValues) [converted]
    # Select Values: Prepare Returns Lookup
    df_Prepare_Returns_Lookup = df_Aggregate_Returns_By_Item.select(col("order_item_id").alias("ret_order_item_id"), col("return_qty").alias("return_qty"), col("return_refund_amount").alias("return_refund_amount"), col("return_count").alias("return_count"))

    # Step: Tag FX File Check (SelectValues) [converted]
    # Select Values: Tag FX File Check
    df_Tag_FX_File_Check = df_Validate_exchange_rates.csv.select(col("short_filename").alias("short_filename"), col("file_exists").alias("file_exists"), col("file_size").alias("file_size"))

    # Step: Tag Items File Check (SelectValues) [converted]
    # Select Values: Tag Items File Check
    df_Tag_Items_File_Check = df_Validate_order_items.csv.select(col("short_filename").alias("short_filename"), col("file_exists").alias("file_exists"), col("file_size").alias("file_size"))

    # Step: Tag Orders File Check (SelectValues) [converted]
    # Select Values: Tag Orders File Check
    df_Tag_Orders_File_Check = df_Validate_orders.csv.select(col("short_filename").alias("short_filename"), col("file_exists").alias("file_exists"), col("file_size").alias("file_size"))

    # Step: Tag Payments File Check (SelectValues) [converted]
    # Select Values: Tag Payments File Check
    df_Tag_Payments_File_Check = df_Validate_payments.csv.select(col("short_filename").alias("short_filename"), col("file_exists").alias("file_exists"), col("file_size").alias("file_size"))

    # Step: Tag Returns File Check (SelectValues) [converted]
    # Select Values: Tag Returns File Check
    df_Tag_Returns_File_Check = df_Validate_returns.csv.select(col("short_filename").alias("short_filename"), col("file_exists").alias("file_exists"), col("file_size").alias("file_size"))

    # Step: Tag Shipments File Check (SelectValues) [converted]
    # Select Values: Tag Shipments File Check
    df_Tag_Shipments_File_Check = df_Validate_shipments.csv.select(col("short_filename").alias("short_filename"), col("file_exists").alias("file_exists"), col("file_size").alias("file_size"))

    # Step: Tag Sales Batch Metadata (Constant) [converted]
    # Add Constants: Tag Sales Batch Metadata
    df_Tag_Sales_Batch_Metadata = df_Capture_Extract_Timestamp
    df_Tag_Sales_Batch_Metadata = df_Tag_Sales_Batch_Metadata.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Tag_Sales_Batch_Metadata = df_Tag_Sales_Batch_Metadata.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'
    df_Tag_Sales_Batch_Metadata = df_Tag_Sales_Batch_Metadata.withColumn("etl_layer", lit('EXTRACT'))
    # preserved.etl_layer: length='-1', precision='-1'
    df_Tag_Sales_Batch_Metadata = df_Tag_Sales_Batch_Metadata.withColumn("source_entity", lit('sales'))
    # preserved.source_entity: length='-1', precision='-1'

    # Step: Append File Checks A (Append) [converted]
    # Append Streams: Append File Checks A
    # preserved.head_name='Tag Orders File Check'
    # preserved.tail_name='Tag Items File Check'
    # preserved.stream_order=['Tag Orders File Check', 'Tag Items File Check']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_File_Checks_A = df_Tag_Orders_File_Check.unionByName(df_Tag_Items_File_Check, allowMissingColumns=True)

    # Step: Sort Order Items By Order ID (SortRows) [converted]
    # Sort Rows: Sort Order Items By Order ID
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Order_Items_By_Order_ID = df_Tag_Sales_Batch_Metadata
    _sort_df_Sort_Order_Items_By_Order_ID = _sort_df_Sort_Order_Items_By_Order_ID.withColumn("_sort_ci_order_id", lower(col("order_id").cast("string")))
    df_Sort_Order_Items_By_Order_ID = _sort_df_Sort_Order_Items_By_Order_ID.orderBy(col("_sort_ci_order_id").asc_nulls_last())
    df_Sort_Order_Items_By_Order_ID = df_Sort_Order_Items_By_Order_ID.drop("_sort_ci_order_id")

    # Step: Append File Checks B (Append) [converted]
    # Append Streams: Append File Checks B
    # preserved.head_name='Append File Checks A'
    # preserved.tail_name='Tag Payments File Check'
    # preserved.stream_order=['Append File Checks A', 'Tag Payments File Check']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_File_Checks_B = df_Append_File_Checks_A.unionByName(df_Tag_Payments_File_Check, allowMissingColumns=True)

    # Step: Merge Join Items To Orders (MergeJoin) [converted]
    # Merge Join: Merge Join Items To Orders
    # preserved.join_type='LEFT OUTER'
    # preserved.join_keys=[{'left': 'order_id', 'right': 'ord_order_id'}]
    # NOTE: PDI Merge Join requires both streams pre-sorted on join keys — Spark join() does not enforce sort order (preserve sort steps upstream if needed)
    # WARNING: MergeJoin 'Merge Join Items To Orders': null join keys do not match (Spark == / PDI merge semantics); duplicate keys produce a cartesian explosion within the key group; ensure key data types match across streams
    _joined_df_Merge_Join_Items_To_Orders = df_Sort_Order_Items_By_Order_ID.join(df_Sort_Orders_By_Order_ID, (df_Sort_Order_Items_By_Order_ID["order_id"] == df_Sort_Orders_By_Order_ID["ord_order_id"]), how='left')
    # WARNING: MergeJoin 'Merge Join Items To Orders': column lineage unavailable — cannot disambiguate join output columns
    df_Merge_Join_Items_To_Orders = _joined_df_Merge_Join_Items_To_Orders

    # Step: Append File Checks C (Append) [converted]
    # Append Streams: Append File Checks C
    # preserved.head_name='Append File Checks B'
    # preserved.tail_name='Tag Shipments File Check'
    # preserved.stream_order=['Append File Checks B', 'Tag Shipments File Check']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_File_Checks_C = df_Append_File_Checks_B.unionByName(df_Tag_Shipments_File_Check, allowMissingColumns=True)

    # Step: Stream Lookup Payments (StreamLookup) [failed]
    # Stream Lookup: Stream Lookup Payments
    # StreamLookup 'Stream Lookup Payments': no join keys — lookup join not generated
    df_Stream_Lookup_Payments = df_Merge_Join_Items_To_Orders

    # Step: Append File Checks D (Append) [converted]
    # Append Streams: Append File Checks D
    # preserved.head_name='Append File Checks C'
    # preserved.tail_name='Tag Returns File Check'
    # preserved.stream_order=['Append File Checks C', 'Tag Returns File Check']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_File_Checks_D = df_Append_File_Checks_C.unionByName(df_Tag_Returns_File_Check, allowMissingColumns=True)

    # Step: Stream Lookup Shipments (StreamLookup) [failed]
    # Stream Lookup: Stream Lookup Shipments
    # StreamLookup 'Stream Lookup Shipments': no join keys — lookup join not generated
    df_Stream_Lookup_Shipments = df_Stream_Lookup_Payments

    # Step: Append File Checks E (Append) [converted]
    # Append Streams: Append File Checks E
    # preserved.head_name='Append File Checks D'
    # preserved.tail_name='Tag FX File Check'
    # preserved.stream_order=['Append File Checks D', 'Tag FX File Check']
    # Stream order preserved: head then tail (schema mismatch uses allowMissingColumns)
    df_Append_File_Checks_E = df_Append_File_Checks_D.unionByName(df_Tag_FX_File_Check, allowMissingColumns=True)

    # Step: Stream Lookup Returns (StreamLookup) [failed]
    # Stream Lookup: Stream Lookup Returns
    # StreamLookup 'Stream Lookup Returns': no join keys — lookup join not generated
    df_Stream_Lookup_Returns = df_Stream_Lookup_Shipments

    # Step: All Files Found? (FilterRows) [failed]
    # Filter Rows: All Files Found?
    df_Log_Files_Ready = df_Append_File_Checks_E.filter((col("file_exists") == lit('Y')))
    df_Abort_Missing_Source_File = df_Append_File_Checks_E.filter(~((col("file_exists") == lit('Y'))))
    df_All_Files_Found? = df_Log_Files_Ready

    # Step: Stream Lookup FX Rates (StreamLookup) [failed]
    # Stream Lookup: Stream Lookup FX Rates
    # StreamLookup 'Stream Lookup FX Rates': no join keys — lookup join not generated
    df_Stream_Lookup_FX_Rates = df_Stream_Lookup_Returns

    # Step: Abort Missing Source File (Abort) [converted]
    # Abort: Abort Missing Source File
    # preserved.row_threshold=0
    # preserved.message='Required Sales source file missing under ${DATASET_PATH}. RUN_ID=${RUN_ID}'
    # preserved.always_log_rows=True
    # preserved.row_threshold_raw='0'
    # Abort operates on its own failure/branch stream df_Abort_Missing_Source_File (already assigned by upstream Filter/Switch; not overwritten)
    print('Abort sample for', 'Abort Missing Source File', df_Abort_Missing_Source_File.limit(100).collect())  # always_log_rows
    _abort_count_df_Abort_Missing_Source_File = df_Abort_Missing_Source_File.count()
    if _abort_count_df_Abort_Missing_Source_File > 0:  # Abort when any row reaches this step (threshold<=0)
        raise RuntimeError('Required Sales source file missing under ${DATASET_PATH}. RUN_ID=${RUN_ID}')

    # Step: Log Files Ready (WriteToLog) [failed]
    # Write to Log: Log Files Ready
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=FILES_OK | TRANS=TR_Sales_Extract | FILES=orders,order_items,payments,shipments,returns,exchange_rates | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Files_Ready = logging.getLogger('pentaho.writetolog.Log_Files_Ready')
    _log_df_Log_Files_Ready.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Files_Ready = df_All_Files_Found?
    _log_rows_df_Log_Files_Ready = _log_df_df_Log_Files_Ready.take(20)
    _log_df_Log_Files_Ready.info('Log Files Ready' + ' | columns=' + str(_log_df_df_Log_Files_Ready.columns))
    _log_df_Log_Files_Ready.info('AUDIT | EVENT=FILES_OK | TRANS=TR_Sales_Extract | FILES=orders,order_items,payments,shipments,returns,exchange_rates | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Files_Ready:
        _log_df_Log_Files_Ready.info('Log Files Ready' + ' | ' + str(_lr.asDict()))
    df_Log_Files_Ready = df_All_Files_Found?

    # Step: Select Joined Sales Columns (SelectValues) [converted]
    # Select Values: Select Joined Sales Columns
    df_Select_Joined_Sales_Columns = df_Stream_Lookup_FX_Rates.select(col("order_item_id").alias("order_item_id"), col("order_id").alias("order_id"), col("product_id").alias("product_id"), col("promotion_id").alias("promotion_id"), col("quantity").alias("quantity"), col("unit_price").alias("unit_price"), col("discount_amount").alias("discount_amount"), col("line_total").alias("line_total"), col("currency_code").alias("currency_code"), col("customer_id").alias("customer_id"), col("store_id").alias("store_id"), col("employee_id").alias("employee_id"), col("order_date").alias("order_date"), col("order_status").alias("order_status"), col("channel").alias("channel"), col("order_currency").alias("order_currency"), col("subtotal_amount").alias("subtotal_amount"), col("order_tax_amount").alias("order_tax_amount"), col("shipping_amount").alias("shipping_amount"), col("order_discount_amount").alias("order_discount_amount"), col("total_amount").alias("total_amount"), col("promo_code").alias("promo_code"), col("payment_id").alias("payment_id"), col("payment_method").alias("payment_method"), col("payment_status").alias("payment_status"), col("payment_amount").alias("payment_amount"), col("shipment_id").alias("shipment_id"), col("shipment_status").alias("shipment_status"), col("shipped_date").alias("shipped_date"), col("estimated_delivery_date").alias("estimated_delivery_date"), col("actual_delivery_date").alias("actual_delivery_date"), col("shipping_cost").alias("shipping_cost"), col("return_qty").alias("return_qty"), col("return_refund_amount").alias("return_refund_amount"), col("exchange_rate").alias("exchange_rate"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"), col("etl_layer").alias("etl_layer"), col("extract_ts").alias("extract_ts"), col("source_row_num").alias("source_row_num"))

    # Step: Optional Excel Sales Overlay (ExcelInput) [converted]
    # Excel Input: Optional Excel Sales Overlay
    df_Optional_Excel_Sales_Overlay = (
        spark.read.format('com.crealytics.spark.excel')
        .option('sheetName', 'Sheet1')
        .option('header', 'true')
        .load('/sales_attributes.xlsx')
    )

    # Step: Count Extracted Orders (GroupBy) [converted]
    # Group By: Count Extracted Orders
    df_Count_Extracted_Orders = df_Select_Joined_Sales_Columns.groupBy('order_id').agg(count(lit(1)).alias('order_lines'))

    # Step: Count Extracted Sales Lines (MemoryGroupBy) [converted]
    # Memory Group By: Count Extracted Sales Lines
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Extracted_Sales_Lines = df_Select_Joined_Sales_Columns.groupBy().agg(count(lit(1)).alias('rows_extracted'))

    # Step: Write Staging Joined Sales (TextFileOutput) [converted]
    # Pentaho step: Write Staging Joined Sales (type: TextFileOutput)
    # Pentaho filename: /output/sales/staging/stg_joined_sales_
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
    # INFO: preserved.field_format name='etl_layer' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='extract_ts' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='source_row_num' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Staging_Joined_Sales = df_Select_Joined_Sales_Columns
    _out_df_Write_Staging_Joined_Sales = df_Write_Staging_Joined_Sales.select('order_item_id', 'order_id', 'product_id', 'promotion_id', 'quantity', 'unit_price', 'discount_amount', 'line_total', 'currency_code', 'customer_id', 'store_id', 'employee_id', 'order_date', 'order_status', 'channel', 'order_currency', 'subtotal_amount', 'order_tax_amount', 'shipping_amount', 'order_discount_amount', 'total_amount', 'promo_code', 'payment_id', 'payment_method', 'payment_status', 'payment_amount', 'shipment_id', 'shipment_status', 'shipped_date', 'estimated_delivery_date', 'actual_delivery_date', 'shipping_cost', 'return_qty', 'return_refund_amount', 'exchange_rate', 'batch_id', 'run_id', 'etl_layer', 'extract_ts', 'source_row_num')
    writer = _out_df_Write_Staging_Joined_Sales.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/stg_joined_sales_.csv')

    # Step: Read Holidays JSON (JsonInput) [converted]
    # JSON Input: Read Holidays JSON
    df_Read_Holidays_JSON = spark.read.format('json').option('multiline', 'true').load('${PROJECT_HOME}/metadata/rules/sales_holidays.json')

    # Step: Add Extract Audit Fields (Constant) [converted]
    # Add Constants: Add Extract Audit Fields
    df_Add_Extract_Audit_Fields = df_Count_Extracted_Sales_Lines
    df_Add_Extract_Audit_Fields = df_Add_Extract_Audit_Fields.withColumn("object_name", lit('sales'))
    # preserved.object_name: length='-1', precision='-1'
    df_Add_Extract_Audit_Fields = df_Add_Extract_Audit_Fields.withColumn("layer", lit('EXTRACT'))
    # preserved.layer: length='-1', precision='-1'
    df_Add_Extract_Audit_Fields = df_Add_Extract_Audit_Fields.withColumn("status", lit('SUCCESS'))
    # preserved.status: length='-1', precision='-1'
    df_Add_Extract_Audit_Fields = df_Add_Extract_Audit_Fields.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Add_Extract_Audit_Fields = df_Add_Extract_Audit_Fields.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'

    # Step: Write Staging Sales Table (TableOutput) [converted]
    # Pentaho step: Write Staging Sales Table (type: TableOutput) (Pentaho schema: retail_stg)
    # Mode: append (Pentaho truncate=N)
    _mapped_df_Write_Staging_Sales_Table = df_Write_Staging_Joined_Sales.select(col('order_item_id'), col('order_id'), col('product_id'), col('customer_id'), col('store_id'), col('quantity'), col('unit_price'), col('line_total'), col('order_date'), col('order_status'), col('payment_id'), col('shipment_id'), col('batch_id'), col('run_id'), col('etl_layer'))
    df_Write_Staging_Sales_Table = _mapped_df_Write_Staging_Sales_Table
    write_delta(
        df_Write_Staging_Sales_Table,
        f"{catalog}.{schema}.stg_joined_sales",
        mode='append',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.stg_joined_sales", mode='append')

    # Step: Read Sales Validation Rules XML (getXMLData) [converted]
    # XML Input: Read Sales Validation Rules XML
    df_Read_Sales_Validation_Rules_XML = spark.read.format('xml').option('rowTag', 'row').load('${PROJECT_HOME}/metadata/rules/sales_validation_rules.xml')

    # Step: Write Extraction Log (TextFileOutput) [converted]
    # Pentaho step: Write Extraction Log (type: TextFileOutput)
    # Pentaho filename: /logs/execution/sales/TR_Sales_Extract_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='object_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='layer' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='rows_extracted' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Extraction_Log = df_Add_Extract_Audit_Fields
    _out_df_Write_Extraction_Log = df_Write_Extraction_Log.select('object_name', 'layer', 'rows_extracted', 'status', 'batch_id', 'run_id')
    writer = _out_df_Write_Extraction_Log.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/TR_Sales_Extract_.log')

    # Step: Block Until Extract Complete (BlockingStep) [converted]
    # Blocking Step: Block Until Extract Complete
    # preserved.pass_all_rows=True
    # preserved.directory='%%java.io.tmpdir%%'
    # preserved.prefix='block'
    # preserved.cache_size='5000'
    # preserved.compress_files=False
    # LIMITATION: Pentaho temp-file spill (directory/prefix/compress) is replaced by Spark cache/persist.
    df_Block_Until_Extract_Complete = cache_for_reuse(df_Write_Staging_Sales_Table)
    _ = df_Block_Until_Extract_Complete.count()  # synchronize: wait for all upstream rows

    # Step: Write Extraction Audit JSON (JsonOutput) [converted]
    # Pentaho step: Write Extraction Audit JSON (type: JsonOutput)
    df_Write_Extraction_Audit_JSON = df_Write_Extraction_Log
    df_Write_Extraction_Audit_JSON.write \
    .mode('overwrite') \
    .json(
        f'{data_dir}/sales_extract_.json'
    )
    # preserved.json_bloc='rows' output_value='json_blob'

    # Step: Extract Complete (Dummy) [converted]
    # Dummy: Extract Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Extract_Complete = df_Block_Until_Extract_Complete

    # Step: Copy Extract Metrics To Result (RowsToResult) [converted]
    # Copy Rows to Result: Copy Extract Metrics To Result
    # preserved.result_buffer='rows'
    # preserved.preserve_order=True
    # LIMITATION: Pentaho Result rows are job-level; Databricks uses a notebook-scoped buffer (_pentaho_result_rows) for downstream hops / orchestration. Cross-job Result transfer needs Databricks Jobs task values or persisted Delta tables.
    _pentaho_result_rows = globals().setdefault('_pentaho_result_rows', {})
    _pentaho_result_files = globals().setdefault('_pentaho_result_files', [])
    # Preserve schema and relative ordering for 'Copy Extract Metrics To Result'
    _result_rows_df_Copy_Extract_Metrics_To_Result = df_Write_Extraction_Audit_JSON
    _pentaho_result_rows['Copy Extract Metrics To Result'] = _result_rows_df_Copy_Extract_Metrics_To_Result
    _pentaho_result_rows['__latest__'] = _result_rows_df_Copy_Extract_Metrics_To_Result
    df_Copy_Extract_Metrics_To_Result = df_Write_Extraction_Audit_JSON

    log_event(_LOG, "transformation_end")
    return df_Copy_Extract_Metrics_To_Result
