"""PySpark module migrated from Pentaho transformation: TR_Sales_Profile.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/staging/TR_Sales_Profile.ktr
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_sales_profile")

from pyspark.sql.window import Window


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Sales_Profile`` step-for-step."""
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

    # Step: Add Profile Batch (Constant) [converted]
    # Add Constants: Add Profile Batch
    df_Add_Profile_Batch = df_Read_Staged_Joined_Sales
    df_Add_Profile_Batch = df_Add_Profile_Batch.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Add_Profile_Batch = df_Add_Profile_Batch.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'

    # Step: Cast Profile Numerics (SelectValues) [converted]
    # Select Values: Cast Profile Numerics
    df_Cast_Profile_Numerics = df_Add_Profile_Batch.select(col("order_item_id").alias("order_item_id"), col("order_id").alias("order_id"), col("product_id").alias("product_id"), col("promotion_id").alias("promotion_id"), col("quantity").alias("quantity"), col("unit_price").alias("unit_price"), col("discount_amount").alias("discount_amount"), col("line_total").alias("line_total"), col("currency_code").alias("currency_code"), col("customer_id").alias("customer_id"), col("store_id").alias("store_id"), col("employee_id").alias("employee_id"), col("order_date").alias("order_date"), col("order_status").alias("order_status"), col("channel").alias("channel"), col("order_currency").alias("order_currency"), col("subtotal_amount").alias("subtotal_amount"), col("order_tax_amount").alias("order_tax_amount"), col("shipping_amount").alias("shipping_amount"), col("order_discount_amount").alias("order_discount_amount"), col("total_amount").alias("total_amount"), col("promo_code").alias("promo_code"), col("payment_id").alias("payment_id"), col("payment_method").alias("payment_method"), col("payment_status").alias("payment_status"), col("payment_amount").alias("payment_amount"), col("shipment_id").alias("shipment_id"), col("shipment_status").alias("shipment_status"), col("shipped_date").alias("shipped_date"), col("estimated_delivery_date").alias("estimated_delivery_date"), col("actual_delivery_date").alias("actual_delivery_date"), col("shipping_cost").alias("shipping_cost"), col("return_qty").alias("return_qty"), col("return_refund_amount").alias("return_refund_amount"), col("exchange_rate").alias("exchange_rate"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"), col("etl_layer").alias("etl_layer"), col("extract_ts").alias("extract_ts"), col("source_row_num").alias("source_row_num"))

    # Step: Cancelled Orders? (FilterRows) [failed]
    # Filter Rows: Cancelled Orders?
    df_Count_Cancelled_Orders = df_Cast_Profile_Numerics.filter((col("order_status") == lit('CANCELLED')))
    df_Non_Cancelled_Path = df_Cast_Profile_Numerics.filter(~((col("order_status") == lit('CANCELLED'))))
    df_Cancelled_Orders? = df_Count_Cancelled_Orders

    # Step: Currency Distribution (MemoryGroupBy) [converted]
    # Memory Group By: Currency Distribution
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Currency_Distribution = df_Cast_Profile_Numerics.groupBy('currency_code').agg(count(lit(1)).alias('line_count'), _sum(col("line_total")).alias('revenue'))

    # Step: Customer Distribution (MemoryGroupBy) [converted]
    # Memory Group By: Customer Distribution
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Customer_Distribution = df_Cast_Profile_Numerics.groupBy('customer_id').agg(countDistinct(col("order_id")).alias('order_count'), _sum(col("line_total")).alias('revenue'))

    # Step: Flag Late Shipments (Formula) [converted]
    # Formula: Flag Late Shipments
    df_Flag_Late_Shipments = df_Cast_Profile_Numerics
    df_Flag_Late_Shipments = df_Flag_Late_Shipments.withColumn('formula_result', lit(None))  # empty formula

    # Step: Flag Missing Payments (Formula) [converted]
    # Formula: Flag Missing Payments
    df_Flag_Missing_Payments = df_Cast_Profile_Numerics
    df_Flag_Missing_Payments = df_Flag_Missing_Payments.withColumn('formula_result', lit(None))  # empty formula

    # Step: Flag Returned Lines (Formula) [converted]
    # Formula: Flag Returned Lines
    df_Flag_Returned_Lines = df_Cast_Profile_Numerics
    df_Flag_Returned_Lines = df_Flag_Returned_Lines.withColumn('formula_result', lit(None))  # empty formula

    # Step: Map Payment Method Labels (ValueMapper) [converted]
    # Value Mapper: Map Payment Method Labels
    df_Map_Payment_Method_Labels = df_Cast_Profile_Numerics.withColumn("payment_method_label", when((lower(col("payment_method")) == lower(lit('CARD'))), lit('Card')).when((lower(col("payment_method")) == lower(lit('CASH'))), lit('Cash')).when((lower(col("payment_method")) == lower(lit('UPI'))), lit('UPI')).when((lower(col("payment_method")) == lower(lit('WALLET'))), lit('Wallet')).when((lower(col("payment_method")) == lower(lit('COD'))), lit('Cash on Delivery')).when((lower(col("payment_method")) == lower(lit('UNKNOWN'))), lit('Unknown')).when((lower(col("payment_method")) == lower(lit('MISSING'))), lit('Missing')).when((col("payment_method").isNull() | (col("payment_method") == lit(''))), col("payment_method")).otherwise(lit('Other')))
    # preserved.case_sensitive=False mappings=7 default='Other'

    # Step: Order Count Aggregate (MemoryGroupBy) [converted]
    # Memory Group By: Order Count Aggregate
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Order_Count_Aggregate = df_Cast_Profile_Numerics.groupBy().agg(countDistinct(col("order_id")).alias('order_count'), count(lit(1)).alias('line_count'), countDistinct(col("customer_id")).alias('customer_count'))

    # Step: Order Value Bands Profile (NumberRange) [converted]
    # Number Range: Order Value Bands Profile
    # Number Range semantics: lower_bound <= value < upper_bound (Pentaho NumberRangeRule)
    df_Order_Value_Bands_Profile = df_Cast_Profile_Numerics.withColumn('value_band', when(col("line_total").isNull(), lit('OTHER')).otherwise(when((col("line_total").cast("double") >= lit(0.0)) & (col("line_total").cast("double") < lit(50.0)), lit('LOW')).when((col("line_total").cast("double") >= lit(50.0)) & (col("line_total").cast("double") < lit(200.0)), lit('MEDIUM')).when((col("line_total").cast("double") >= lit(200.0)) & (col("line_total").cast("double") < lit(1000.0)), lit('HIGH')).when((col("line_total").cast("double") >= lit(1000.0)) & (col("line_total").cast("double") < lit(9999999.0)), lit('VIP')).otherwise(lit('OTHER'))))
    # preserved.fallback='OTHER' rules=4 lower_inclusive=True upper_inclusive=False

    # Step: Route By Channel Profile (SwitchCase) [converted]
    # Switch / Case: Route By Channel Profile
    # preserved.fieldname='channel'
    # preserved.switch_field='channel'
    # preserved.cases=[{'value': 'ONLINE', 'target_step': 'Tag Channel Online'}, {'value': 'STORE', 'target_step': 'Tag Channel Store'}, {'value': 'MOBILE', 'target_step': 'Tag Channel Mobile'}]
    # preserved.default_target_step='Tag Channel Other'
    # preserved.use_contains=False
    # preserved.case_value_type='String'
    # preserved.rules=[{'value': 'ONLINE', 'target_step': 'Tag Channel Online'}, {'value': 'STORE', 'target_step': 'Tag Channel Store'}, {'value': 'MOBILE', 'target_step': 'Tag Channel Mobile'}]
    _routed_df_Route_By_Channel_Profile = df_Cast_Profile_Numerics.withColumn('_route_Route_By_Channel_Profile', when(col("channel") == lit('ONLINE'), lit('Tag Channel Online')).when(col("channel") == lit('STORE'), lit('Tag Channel Store')).when(col("channel") == lit('MOBILE'), lit('Tag Channel Mobile')).otherwise(lit('Tag Channel Other')))
    df_Tag_Channel_Online = _routed_df_Route_By_Channel_Profile.filter(col('_route_Route_By_Channel_Profile') == lit('Tag Channel Online')).drop('_route_Route_By_Channel_Profile')
    df_Tag_Channel_Store = _routed_df_Route_By_Channel_Profile.filter(col('_route_Route_By_Channel_Profile') == lit('Tag Channel Store')).drop('_route_Route_By_Channel_Profile')
    df_Tag_Channel_Mobile = _routed_df_Route_By_Channel_Profile.filter(col('_route_Route_By_Channel_Profile') == lit('Tag Channel Mobile')).drop('_route_Route_By_Channel_Profile')
    df_Tag_Channel_Other = _routed_df_Route_By_Channel_Profile.filter(col('_route_Route_By_Channel_Profile') == lit('Tag Channel Other')).drop('_route_Route_By_Channel_Profile')
    df_Route_By_Channel_Profile = df_Tag_Channel_Online

    # Step: Sample Profile Peek (SampleRows) [converted]
    # Sample Rows: Sample Profile Peek
    _w_sr_df_Sample_Profile_Peek = Window.orderBy(monotonically_increasing_id())
    df_Sample_Profile_Peek = df_Cast_Profile_Numerics.withColumn('_sr_rn', row_number().over(_w_sr_df_Sample_Profile_Peek))
    # preserved.lines_range='1..10' ranges=[(1, 10)]
    df_Sample_Profile_Peek = df_Sample_Profile_Peek.filter(((col('_sr_rn') >= 1) & (col('_sr_rn') <= 10)))
    df_Sample_Profile_Peek = df_Sample_Profile_Peek.drop('_sr_rn')

    # Step: Sort For Duplicate Order Check (SortRows) [converted]
    # Sort Rows: Sort For Duplicate Order Check
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_For_Duplicate_Order_Check = df_Cast_Profile_Numerics
    _sort_df_Sort_For_Duplicate_Order_Check = _sort_df_Sort_For_Duplicate_Order_Check.withColumn("_sort_ci_order_id", lower(col("order_id").cast("string")))
    _sort_df_Sort_For_Duplicate_Order_Check = _sort_df_Sort_For_Duplicate_Order_Check.withColumn("_sort_ci_order_item_id", lower(col("order_item_id").cast("string")))
    df_Sort_For_Duplicate_Order_Check = _sort_df_Sort_For_Duplicate_Order_Check.orderBy(col("_sort_ci_order_id").asc_nulls_last(), col("_sort_ci_order_item_id").asc_nulls_last())
    df_Sort_For_Duplicate_Order_Check = df_Sort_For_Duplicate_Order_Check.drop("_sort_ci_order_id", "_sort_ci_order_item_id")

    # Step: Store Distribution (MemoryGroupBy) [converted]
    # Memory Group By: Store Distribution
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Store_Distribution = df_Cast_Profile_Numerics.groupBy('store_id').agg(countDistinct(col("order_id")).alias('order_count'), _sum(col("line_total")).alias('revenue'))

    # Step: Count Cancelled Orders (MemoryGroupBy) [failed]
    # Memory Group By: Count Cancelled Orders
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Cancelled_Orders = df_Cancelled_Orders?.groupBy().agg(countDistinct(col("order_id")).alias('cancelled_orders'), _sum(col("line_total")).alias('cancelled_revenue'))

    # Step: Non Cancelled Path (Dummy) [converted]
    # Dummy: Non Cancelled Path
    # Pass-through step - DataFrame unchanged
    df_Dummy_Non_Cancelled_Path = df_Non_Cancelled_Path

    # Step: Write Currency Distribution (TextFileOutput) [converted]
    # Pentaho step: Write Currency Distribution (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/sales_currency_dist_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='currency_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='line_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Currency_Distribution = df_Currency_Distribution
    _out_df_Write_Currency_Distribution = df_Write_Currency_Distribution.select('currency_code', 'line_count', 'revenue')
    writer = _out_df_Write_Currency_Distribution.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_currency_dist_.csv')

    # Step: Sort Customer Revenue (SortRows) [converted]
    # Sort Rows: Sort Customer Revenue
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Customer_Revenue = df_Customer_Distribution
    _sort_df_Sort_Customer_Revenue = _sort_df_Sort_Customer_Revenue.withColumn("_sort_ci_revenue", lower(col("revenue").cast("string")))
    df_Sort_Customer_Revenue = _sort_df_Sort_Customer_Revenue.orderBy(col("_sort_ci_revenue").asc_nulls_last())
    df_Sort_Customer_Revenue = df_Sort_Customer_Revenue.drop("_sort_ci_revenue")

    # Step: Late Shipments? (FilterRows) [failed]
    # Filter Rows: Late Shipments?
    df_Write_Late_Shipments = df_Flag_Late_Shipments.filter((col("is_late_shipment") == lit('Y')))
    df_OnTime_Path = df_Flag_Late_Shipments.filter(~((col("is_late_shipment") == lit('Y'))))
    df_Late_Shipments? = df_Write_Late_Shipments

    # Step: Missing Payments? (FilterRows) [failed]
    # Filter Rows: Missing Payments?
    df_Write_Missing_Payments = df_Flag_Missing_Payments.filter((col("missing_payment") == lit('Y')))
    df_Payment_OK_Path = df_Flag_Missing_Payments.filter(~((col("missing_payment") == lit('Y'))))
    df_Missing_Payments? = df_Write_Missing_Payments

    # Step: Returned Lines? (FilterRows) [failed]
    # Filter Rows: Returned Lines?
    df_Count_Returned_Orders = df_Flag_Returned_Lines.filter((col("is_returned") == lit('Y')))
    df_Non_Returned_Path = df_Flag_Returned_Lines.filter(~((col("is_returned") == lit('Y'))))
    df_Returned_Lines? = df_Count_Returned_Orders

    # Step: Payment Method Distribution (MemoryGroupBy) [converted]
    # Memory Group By: Payment Method Distribution
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Payment_Method_Distribution = df_Map_Payment_Method_Labels.groupBy('payment_method_label').agg(countDistinct(col("order_id")).alias('order_count'), _sum(col("payment_amount")).alias('pay_amount'))

    # Step: Calculate Avg Basket Metric (Calculator) [converted]
    # Calculator: Calculate Avg Basket Metric
    df_Calculate_Avg_Basket_Metric = df_Order_Count_Aggregate
    df_Calculate_Avg_Basket_Metric = df_Calculate_Avg_Basket_Metric.withColumn("avg_lines_per_order", ((col("line_count") / col("order_count"))).cast('decimal(38,4)'))

    # Step: Tag Profile Pivot Key (Constant) [converted]
    # Add Constants: Tag Profile Pivot Key
    df_Tag_Profile_Pivot_Key = df_Order_Count_Aggregate
    df_Tag_Profile_Pivot_Key = df_Tag_Profile_Pivot_Key.withColumn("profile_group", lit('SALES_VOLUME_PROFILE'))
    # preserved.profile_group: length='-1', precision='-1'

    # Step: Write Order Count Profile (TextFileOutput) [converted]
    # Pentaho step: Write Order Count Profile (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/sales_order_count_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='order_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='line_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='customer_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Order_Count_Profile = df_Order_Count_Aggregate
    _out_df_Write_Order_Count_Profile = df_Write_Order_Count_Profile.select('order_count', 'line_count', 'customer_count', 'batch_id', 'run_id')
    writer = _out_df_Write_Order_Count_Profile.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_order_count_.csv')

    # Step: Value Band Distribution (MemoryGroupBy) [converted]
    # Memory Group By: Value Band Distribution
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Value_Band_Distribution = df_Order_Value_Bands_Profile.groupBy('value_band').agg(count(lit(1)).alias('line_count'), _sum(col("line_total")).alias('revenue'))

    # Step: Tag Channel Mobile (Constant) [converted]
    # Add Constants: Tag Channel Mobile
    df_Tag_Channel_Mobile = df_Route_By_Channel_Profile
    df_Tag_Channel_Mobile = df_Tag_Channel_Mobile.withColumn("channel_group", lit('DIGITAL'))
    # preserved.channel_group: length='-1', precision='-1'

    # Step: Tag Channel Online (Constant) [converted]
    # Add Constants: Tag Channel Online
    df_Tag_Channel_Online = df_Route_By_Channel_Profile
    df_Tag_Channel_Online = df_Tag_Channel_Online.withColumn("channel_group", lit('DIGITAL'))
    # preserved.channel_group: length='-1', precision='-1'

    # Step: Tag Channel Other (Constant) [converted]
    # Add Constants: Tag Channel Other
    df_Tag_Channel_Other = df_Route_By_Channel_Profile
    df_Tag_Channel_Other = df_Tag_Channel_Other.withColumn("channel_group", lit('OTHER'))
    # preserved.channel_group: length='-1', precision='-1'

    # Step: Tag Channel Store (Constant) [converted]
    # Add Constants: Tag Channel Store
    df_Tag_Channel_Store = df_Route_By_Channel_Profile
    df_Tag_Channel_Store = df_Tag_Channel_Store.withColumn("channel_group", lit('RETAIL'))
    # preserved.channel_group: length='-1', precision='-1'

    # Step: Unique Order Item Keys (Unique) [converted]
    # Unique Rows: Unique Order Item Keys
    # preserved.reject_duplicate_row=N error_description=''
    # Unique Rows expects sorted input in Pentaho; Spark dropDuplicates is order-independent
    # preserved.count_rows=False count_field='count' compare_fields=['order_item_id']
    df_Unique_Order_Item_Keys = df_Sort_For_Duplicate_Order_Check.dropDuplicates(["order_item_id"])

    # Step: Write Store Distribution (TextFileOutput) [converted]
    # Pentaho step: Write Store Distribution (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/sales_store_dist_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='store_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Store_Distribution = df_Store_Distribution
    _out_df_Write_Store_Distribution = df_Write_Store_Distribution.select('store_id', 'order_count', 'revenue')
    writer = _out_df_Write_Store_Distribution.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_store_dist_.csv')

    # Step: Write Cancelled Orders (TextFileOutput) [converted]
    # Pentaho step: Write Cancelled Orders (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/sales_cancelled_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='cancelled_orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='cancelled_revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Cancelled_Orders = df_Count_Cancelled_Orders
    _out_df_Write_Cancelled_Orders = df_Write_Cancelled_Orders.select('cancelled_orders', 'cancelled_revenue', 'batch_id', 'run_id')
    writer = _out_df_Write_Cancelled_Orders.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_cancelled_.csv')

    # Step: Write Customer Distribution (TextFileOutput) [converted]
    # Pentaho step: Write Customer Distribution (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/sales_customer_dist_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='customer_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Customer_Distribution = df_Sort_Customer_Revenue
    _out_df_Write_Customer_Distribution = df_Write_Customer_Distribution.select('customer_id', 'order_count', 'revenue')
    writer = _out_df_Write_Customer_Distribution.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_customer_dist_.csv')

    # Step: OnTime Path (Dummy) [converted]
    # Dummy: OnTime Path
    # Pass-through step - DataFrame unchanged
    df_Dummy_OnTime_Path = df_OnTime_Path

    # Step: Write Late Shipments (TextFileOutput) [failed]
    # Pentaho step: Write Late Shipments (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/sales_late_shipments_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipment_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='estimated_delivery_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='actual_delivery_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_late_shipment' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Late_Shipments = df_Late_Shipments?
    _out_df_Write_Late_Shipments = df_Write_Late_Shipments.select('order_id', 'shipment_id', 'estimated_delivery_date', 'actual_delivery_date', 'is_late_shipment')
    writer = _out_df_Write_Late_Shipments.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_late_shipments_.csv')

    # Step: Payment OK Path (Dummy) [converted]
    # Dummy: Payment OK Path
    # Pass-through step - DataFrame unchanged
    df_Dummy_Payment_OK_Path = df_Payment_OK_Path

    # Step: Write Missing Payments (TextFileOutput) [failed]
    # Pentaho step: Write Missing Payments (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/sales_missing_payments_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='missing_payment' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Missing_Payments = df_Missing_Payments?
    _out_df_Write_Missing_Payments = df_Write_Missing_Payments.select('order_id', 'payment_id', 'payment_status', 'missing_payment')
    writer = _out_df_Write_Missing_Payments.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_missing_payments_.csv')

    # Step: Count Returned Orders (MemoryGroupBy) [failed]
    # Memory Group By: Count Returned Orders
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Returned_Orders = df_Returned_Lines?.groupBy().agg(countDistinct(col("order_id")).alias('returned_orders'), _sum(col("return_qty")).alias('returned_qty'), _sum(col("return_refund_amount")).alias('refund_total'))

    # Step: Non Returned Path (Dummy) [converted]
    # Dummy: Non Returned Path
    # Pass-through step - DataFrame unchanged
    df_Dummy_Non_Returned_Path = df_Non_Returned_Path

    # Step: Write Payment Method Dist (TextFileOutput) [converted]
    # Pentaho step: Write Payment Method Dist (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/sales_payment_method_dist_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='payment_method_label' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='pay_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Payment_Method_Dist = df_Payment_Method_Distribution
    _out_df_Write_Payment_Method_Dist = df_Write_Payment_Method_Dist.select('payment_method_label', 'order_count', 'pay_amount')
    writer = _out_df_Write_Payment_Method_Dist.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_payment_method_dist_.csv')

    # Step: Seed Profile Report Header (Constant) [converted]
    # Add Constants: Seed Profile Report Header
    df_Seed_Profile_Report_Header = df_Order_Count_Aggregate
    df_Seed_Profile_Report_Header = df_Seed_Profile_Report_Header.withColumn("report_name", lit('SALES_PROFILING'))
    # preserved.report_name: length='-1', precision='-1'
    df_Seed_Profile_Report_Header = df_Seed_Profile_Report_Header.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Seed_Profile_Report_Header = df_Seed_Profile_Report_Header.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'

    # Step: Normalise Order Count Metrics (Normaliser) [converted]
    # Row Normaliser: Normalise Order Count Metrics
    _norm_df_Normalise_Order_Count_Metrics_0 = df_Tag_Profile_Pivot_Key.select(col("profile_group"), lit('order_count').alias("metric_name"), col("order_count").alias("order_count"))
    _norm_df_Normalise_Order_Count_Metrics_1 = df_Tag_Profile_Pivot_Key.select(col("profile_group"), lit('line_count').alias("metric_name"), col("line_count").alias("line_count"))
    _norm_df_Normalise_Order_Count_Metrics_2 = df_Tag_Profile_Pivot_Key.select(col("profile_group"), lit('customer_count').alias("metric_name"), col("customer_count").alias("customer_count"))
    df_Normalise_Order_Count_Metrics = _norm_df_Normalise_Order_Count_Metrics_0
    df_Normalise_Order_Count_Metrics = df_Normalise_Order_Count_Metrics.unionByName(_norm_df_Normalise_Order_Count_Metrics_1, allowMissingColumns=True)
    df_Normalise_Order_Count_Metrics = df_Normalise_Order_Count_Metrics.unionByName(_norm_df_Normalise_Order_Count_Metrics_2, allowMissingColumns=True)

    # Step: Unify Channel Routes (Dummy) [converted]
    # Dummy: Unify Channel Routes
    # Pass-through step - DataFrame unchanged
    df_Dummy_Unify_Channel_Routes = df_Tag_Channel_Online

    # Step: Duplicate Order Item Analysis (GroupBy) [converted]
    # Group By: Duplicate Order Item Analysis
    df_Duplicate_Order_Item_Analysis = df_Unique_Order_Item_Keys.groupBy('order_item_id').agg(count(lit(1)).alias('dup_count'))

    # Step: Write Returned Profile (TextFileOutput) [converted]
    # Pentaho step: Write Returned Profile (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/sales_returned_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='returned_orders' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='returned_qty' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='refund_total' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Returned_Profile = df_Count_Returned_Orders
    _out_df_Write_Returned_Profile = df_Write_Returned_Profile.select('returned_orders', 'returned_qty', 'refund_total')
    writer = _out_df_Write_Returned_Profile.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_returned_.csv')

    # Step: Write Profiling Report JSON (JsonOutput) [converted]
    # Pentaho step: Write Profiling Report JSON (type: JsonOutput)
    df_Write_Profiling_Report_JSON = df_Seed_Profile_Report_Header
    df_Write_Profiling_Report_JSON.write \
    .mode('overwrite') \
    .json(
        f'{data_dir}/sales_profiling_report__summary.json'
    )
    # preserved.json_bloc='rows' output_value='json_blob'

    # Step: Denormalise Volume Metrics (Denormaliser) [converted]
    # Row Denormaliser: Denormalise Volume Metrics
    # preserved.target 'order_count' type='Number' format='' length='-1' precision='-1' decimal='' grouping='' currency='' null_string='' aggregation=-
    # preserved.target 'line_count' type='Number' format='' length='-1' precision='-1' decimal='' grouping='' currency='' null_string='' aggregation=-
    # preserved.target 'customer_count' type='Number' format='' length='-1' precision='-1' decimal='' grouping='' currency='' null_string='' aggregation=-
    df_Denormalise_Volume_Metrics = df_Normalise_Order_Count_Metrics.groupBy("profile_group").agg(first(when(col("metric_name") == lit('order_count'), col("order_count")), ignorenulls=True).alias('order_count'), first(when(col("metric_name") == lit('line_count'), col("line_count")), ignorenulls=True).alias('line_count'), first(when(col("metric_name") == lit('customer_count'), col("customer_count")), ignorenulls=True).alias('customer_count'))
    df_Denormalise_Volume_Metrics = df_Denormalise_Volume_Metrics.withColumn("order_count", col("order_count").cast("double"))
    df_Denormalise_Volume_Metrics = df_Denormalise_Volume_Metrics.withColumn("line_count", col("line_count").cast("double"))
    df_Denormalise_Volume_Metrics = df_Denormalise_Volume_Metrics.withColumn("customer_count", col("customer_count").cast("double"))

    # Step: Write Profiling Report (TextFileOutput) [converted]
    # Pentaho step: Write Profiling Report (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/sales_profiling_report_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='report_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='metric_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Profiling_Report = df_Normalise_Order_Count_Metrics
    _out_df_Write_Profiling_Report = df_Write_Profiling_Report.select('report_name', 'metric_name', 'order_count', 'batch_id', 'run_id')
    writer = _out_df_Write_Profiling_Report.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_profiling_report_.csv')

    # Step: Flag Duplicate Order Items (Formula) [converted]
    # Formula: Flag Duplicate Order Items
    df_Flag_Duplicate_Order_Items = df_Duplicate_Order_Item_Analysis
    df_Flag_Duplicate_Order_Items = df_Flag_Duplicate_Order_Items.withColumn('formula_result', lit(None))  # empty formula

    # Step: Log Profiling Complete (WriteToLog) [converted]
    # Write to Log: Log Profiling Complete
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=PROFILE_COMPLETE | TRANS=TR_Sales_Profile | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Profiling_Complete = logging.getLogger('pentaho.writetolog.Log_Profiling_Complete')
    _log_df_Log_Profiling_Complete.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Profiling_Complete = df_Write_Profiling_Report
    _log_rows_df_Log_Profiling_Complete = _log_df_df_Log_Profiling_Complete.take(20)
    _log_df_Log_Profiling_Complete.info('Log Profiling Complete' + ' | columns=' + str(_log_df_df_Log_Profiling_Complete.columns))
    _log_df_Log_Profiling_Complete.info('AUDIT | EVENT=PROFILE_COMPLETE | TRANS=TR_Sales_Profile | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Profiling_Complete:
        _log_df_Log_Profiling_Complete.info('Log Profiling Complete' + ' | ' + str(_lr.asDict()))
    df_Log_Profiling_Complete = df_Write_Profiling_Report

    # Step: Keep Duplicate Order Items (FilterRows) [converted]
    # Filter Rows: Keep Duplicate Order Items
    df_Write_Duplicate_Orders = df_Flag_Duplicate_Order_Items.filter((col("is_duplicate") == lit('Y')))
    df_Discard_Non_Dupes = df_Flag_Duplicate_Order_Items.filter(~((col("is_duplicate") == lit('Y'))))
    df_Keep_Duplicate_Order_Items = df_Write_Duplicate_Orders

    # Step: Profile Complete (Dummy) [converted]
    # Dummy: Profile Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Profile_Complete = df_Log_Profiling_Complete

    # Step: Discard Non Dupes (Dummy) [converted]
    # Dummy: Discard Non Dupes
    # Pass-through step - DataFrame unchanged
    df_Dummy_Discard_Non_Dupes = df_Discard_Non_Dupes

    # Step: Write Duplicate Orders (TextFileOutput) [converted]
    # Pentaho step: Write Duplicate Orders (type: TextFileOutput)
    # Pentaho filename: /audit/data_quality/sales_duplicate_orders_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='order_item_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='dup_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Duplicate_Orders = df_Keep_Duplicate_Order_Items
    _out_df_Write_Duplicate_Orders = df_Write_Duplicate_Orders.select('order_item_id', 'order_id', 'dup_count')
    writer = _out_df_Write_Duplicate_Orders.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_duplicate_orders_.csv')

    log_event(_LOG, "transformation_end")
    return df_Write_Duplicate_Orders
