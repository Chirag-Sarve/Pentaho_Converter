"""PySpark module migrated from Pentaho transformation: TR_Sales_Dimension_Lookups.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/lookup/TR_Sales_Dimension_Lookups.ktr
Independent module — ``run(spark, config)`` returns a DataFrame.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    count,
    current_timestamp,
    length,
    lit,
    when,
    coalesce,
    row_number,
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_sales_dimension_lookups")

from pyspark.sql.window import Window


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Sales_Dimension_Lookups`` step-for-step."""
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

    # Step: Read DimCustomer Current (CsvInput) [converted]
    # CSV Input: Read DimCustomer Current
    df_Read_DimCustomer_Current = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('customer_sk INT, customer_id STRING, is_current STRING')
        .load(f'{data_dir}/dim_customer_current.csv')
    )

    # Step: Read DimDate (CsvInput) [converted]
    # CSV Input: Read DimDate
    df_Read_DimDate = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('date_sk INT, full_date STRING, calendar_year INT, calendar_month INT')
        .load(f'{data_dir}/dim_date.csv')
    )

    # Step: Read DimEmployee Current (CsvInput) [converted]
    # CSV Input: Read DimEmployee Current
    df_Read_DimEmployee_Current = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('employee_sk INT, employee_id STRING, is_current STRING')
        .load(f'{data_dir}/dim_employee_current.csv')
    )

    # Step: Read DimProduct Current (CsvInput) [converted]
    # CSV Input: Read DimProduct Current
    df_Read_DimProduct_Current = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('product_sk INT, product_id STRING, is_current STRING')
        .load(f'{data_dir}/dim_product_current.csv')
    )

    # Step: Read DimPromotion Current (CsvInput) [converted]
    # CSV Input: Read DimPromotion Current
    df_Read_DimPromotion_Current = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('promotion_sk INT, promotion_id STRING, is_current STRING')
        .load(f'{data_dir}/dim_promotion_current.csv')
    )

    # Step: Read DimStore Current (CsvInput) [converted]
    # CSV Input: Read DimStore Current
    df_Read_DimStore_Current = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('store_sk INT, store_id STRING, region_id STRING, is_current STRING')
        .load(f'{data_dir}/dim_store_current.csv')
    )

    # Step: Read Enriched Sales (CsvInput) [converted]
    # CSV Input: Read Enriched Sales
    df_Read_Enriched_Sales = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('order_item_id STRING, order_id STRING, product_id STRING, promotion_id STRING, customer_id STRING, store_id STRING, employee_id STRING, order_date STRING, order_status STRING, status_bucket STRING, channel STRING, channel_mapped STRING, currency_code STRING, quantity DOUBLE, unit_price DOUBLE, gross_amount DOUBLE, discount_amount_calc DOUBLE, net_amount DOUBLE, tax_amount_calc DOUBLE, shipping_cost_calc DOUBLE, total_revenue DOUBLE, profit DOUBLE, margin DOUBLE, fx_rate DOUBLE, converted_amount_usd DOUBLE, return_amount DOUBLE, refund_amount_calc DOUBLE, late_delivery_flag STRING, high_value_order_flag STRING, weekend_order_flag STRING, holiday_flag STRING, order_value_band STRING, sales_bk_checksum STRING, customer_lifetime_revenue DOUBLE, store_revenue DOUBLE, regional_revenue DOUBLE, employee_sales DOUBLE, payment_id STRING, payment_method STRING, shipment_id STRING, promo_code STRING, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/sales_enriched_.csv')
    )

    # Step: Write Lookup Rejects (TextFileOutput) [converted]
    # Pentaho step: Write Lookup Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/sales/sales_lookup_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='order_item_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='customer_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_CODE' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_DESC' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Lookup_Rejects = df_Write_Lookup_Rejects
    _out_df_Write_Lookup_Rejects = df_Write_Lookup_Rejects.select('order_item_id', 'customer_id', 'product_id', 'ERR_CODE', 'ERR_DESC', 'batch_id', 'run_id')
    writer = _out_df_Write_Lookup_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_lookup_rejects_.csv')

    # Step: Prepare Customer Dim Keys (SelectValues) [converted]
    # Select Values: Prepare Customer Dim Keys
    df_Prepare_Customer_Dim_Keys = df_Read_DimCustomer_Current.select(col("customer_sk").alias("lkp_customer_sk"), col("customer_id").alias("lkp_customer_id"))

    # Step: Prepare Date Dim Keys (SelectValues) [converted]
    # Select Values: Prepare Date Dim Keys
    df_Prepare_Date_Dim_Keys = df_Read_DimDate.select(col("date_sk").alias("lkp_date_sk"), col("full_date").alias("lkp_full_date"))

    # Step: Prepare Employee Dim Keys (SelectValues) [converted]
    # Select Values: Prepare Employee Dim Keys
    df_Prepare_Employee_Dim_Keys = df_Read_DimEmployee_Current.select(col("employee_sk").alias("lkp_employee_sk"), col("employee_id").alias("lkp_employee_id"))

    # Step: Prepare Product Dim Keys (SelectValues) [converted]
    # Select Values: Prepare Product Dim Keys
    df_Prepare_Product_Dim_Keys = df_Read_DimProduct_Current.select(col("product_sk").alias("lkp_product_sk"), col("product_id").alias("lkp_product_id"))

    # Step: Prepare Promotion Dim Keys (SelectValues) [converted]
    # Select Values: Prepare Promotion Dim Keys
    df_Prepare_Promotion_Dim_Keys = df_Read_DimPromotion_Current.select(col("promotion_sk").alias("lkp_promotion_sk"), col("promotion_id").alias("lkp_promotion_id"))

    # Step: Prepare Store Dim Keys (SelectValues) [converted]
    # Select Values: Prepare Store Dim Keys
    df_Prepare_Store_Dim_Keys = df_Read_DimStore_Current.select(col("store_sk").alias("lkp_store_sk"), col("store_id").alias("lkp_store_id"), col("region_id").alias("lkp_region_id"))

    # Step: Stream Lookup Customer SK (StreamLookup) [failed]
    # Stream Lookup: Stream Lookup Customer SK
    # StreamLookup 'Stream Lookup Customer SK': no join keys — lookup join not generated
    df_Stream_Lookup_Customer_SK = df_Read_Enriched_Sales

    # Step: Stream Lookup Product SK (StreamLookup) [failed]
    # Stream Lookup: Stream Lookup Product SK
    # StreamLookup 'Stream Lookup Product SK': no join keys — lookup join not generated
    df_Stream_Lookup_Product_SK = df_Stream_Lookup_Customer_SK

    # Step: Stream Lookup Store SK (StreamLookup) [failed]
    # Stream Lookup: Stream Lookup Store SK
    # StreamLookup 'Stream Lookup Store SK': no join keys — lookup join not generated
    df_Stream_Lookup_Store_SK = df_Stream_Lookup_Product_SK

    # Step: Stream Lookup Employee SK (StreamLookup) [failed]
    # Stream Lookup: Stream Lookup Employee SK
    # StreamLookup 'Stream Lookup Employee SK': no join keys — lookup join not generated
    df_Stream_Lookup_Employee_SK = df_Stream_Lookup_Store_SK

    # Step: Stream Lookup Promotion SK (StreamLookup) [failed]
    # Stream Lookup: Stream Lookup Promotion SK
    # StreamLookup 'Stream Lookup Promotion SK': no join keys — lookup join not generated
    df_Stream_Lookup_Promotion_SK = df_Stream_Lookup_Employee_SK

    # Step: Stream Lookup Date SK (StreamLookup) [failed]
    # Stream Lookup: Stream Lookup Date SK
    # StreamLookup 'Stream Lookup Date SK': no join keys — lookup join not generated
    df_Stream_Lookup_Date_SK = df_Stream_Lookup_Promotion_SK

    # Step: DB Join Customer Soft (DBJoin) [partial]
    # Database Join: DB Join Customer Soft
    # preserved.connection='conn_dev_dwh'
    # preserved.sql="SELECT customer_sk AS db_customer_sk FROM retail_dwh.dim_customer WHERE customer_id = ? AND is_current = 'Y'"
    # preserved.outer_join=True
    # preserved.row_limit=0
    # preserved.replace_vars=True
    # preserved.parameters=[{'name': 'customer_id', 'type': 'String'}, {'name': '\n        ', 'type': ''}]
    _sql_df_DB_Join_Customer_Soft = "SELECT customer_sk AS db_customer_sk FROM retail_dwh.dim_customer WHERE customer_id = ? AND is_current = 'Y'"
    # WARNING: per-row parameterized joins cannot use spark.sql with '?' placeholders; emitting JDBC prepared-statement skeleton (foreachPartition).
    # preserved.sql_template="SELECT customer_sk AS db_customer_sk FROM retail_dwh.dim_customer WHERE customer_id = :customer_id AND is_current = 'Y'"
    _param_fields_df_DB_Join_Customer_Soft = ['customer_id', '\n        ']
    import os
    # foreachPartition JDBC outline (wire PENTAHO_JDBC_URL / driver at runtime):
    # def _dbjoin_partition(rows):
    #     conn = <jdbc connect from os.environ['PENTAHO_JDBC_URL']>
    #     cur = conn.prepareStatement("SELECT customer_sk AS db_customer_sk FROM retail_dwh.dim_customer WHERE customer_id = ? AND is_current = 'Y'")
    #     for row in rows:
    #         for i, f in enumerate(_param_fields_df_DB_Join_Customer_Soft, 1):
    #             cur.setObject(i, row[f])
    #         rs = cur.executeQuery(); ... yield joined rows
    # Fallback: preserve input stream; attach empty lookup side for schema continuity
    df_DB_Join_Customer_Soft = df_Stream_Lookup_Date_SK
    # Join type preserved as 'left'; join keys=['customer_id', '\n        ']

    # Step: DB Join Product Soft (DBJoin) [partial]
    # Database Join: DB Join Product Soft
    # preserved.connection='conn_dev_dwh'
    # preserved.sql="SELECT product_sk AS db_product_sk FROM retail_dwh.dim_product WHERE product_id = ? AND is_current = 'Y'"
    # preserved.outer_join=True
    # preserved.row_limit=0
    # preserved.replace_vars=True
    # preserved.parameters=[{'name': 'product_id', 'type': 'String'}, {'name': '\n        ', 'type': ''}]
    _sql_df_DB_Join_Product_Soft = "SELECT product_sk AS db_product_sk FROM retail_dwh.dim_product WHERE product_id = ? AND is_current = 'Y'"
    # WARNING: per-row parameterized joins cannot use spark.sql with '?' placeholders; emitting JDBC prepared-statement skeleton (foreachPartition).
    # preserved.sql_template="SELECT product_sk AS db_product_sk FROM retail_dwh.dim_product WHERE product_id = :product_id AND is_current = 'Y'"
    _param_fields_df_DB_Join_Product_Soft = ['product_id', '\n        ']
    import os
    # foreachPartition JDBC outline (wire PENTAHO_JDBC_URL / driver at runtime):
    # def _dbjoin_partition(rows):
    #     conn = <jdbc connect from os.environ['PENTAHO_JDBC_URL']>
    #     cur = conn.prepareStatement("SELECT product_sk AS db_product_sk FROM retail_dwh.dim_product WHERE product_id = ? AND is_current = 'Y'")
    #     for row in rows:
    #         for i, f in enumerate(_param_fields_df_DB_Join_Product_Soft, 1):
    #             cur.setObject(i, row[f])
    #         rs = cur.executeQuery(); ... yield joined rows
    # Fallback: preserve input stream; attach empty lookup side for schema continuity
    df_DB_Join_Product_Soft = df_DB_Join_Customer_Soft
    # Join type preserved as 'left'; join keys=['product_id', '\n        ']

    # Step: Dimension Lookup Date Only (DimensionLookup) [converted]
    # Dimension Lookup/Update: Dimension Lookup Date Only
    # preserved.connection='conn_dev_dwh'
    # WARNING: DimensionLookup 'Dimension Lookup Date Only': connection 'conn_dev_dwh' mapped to Spark/UC table 'main.retail_dwh.dim_date' (not JDBC).
    # preserved.commit_size=100
    # preserved.cache_size=5000
    # preserved.preload_cache=True
    # preserved.use_start_date_alternative=False
    # preserved.start_date_alternative='none'
    # preserved.use_batch=True
    # preserved.min_year=1900
    # preserved.max_year=2199
    # SCD mode: lookup-only; Type1=0 Type2=0 PunchThrough=0 technical=0
    # Surrogate key strategy: tablemax (MAX(tk)+row_number) for 'Dimension Lookup Date Only'
    # Optional: ALTER TABLE ... CHANGE COLUMN tk GENERATED BY DEFAULT AS IDENTITY — then omit tk from INSERT values
    # Optional: spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")  # additive columns only
    # Post-load tip: partition pruning on effective_start_date/effective_end_date; OPTIMIZE ... ZORDER BY (order_date)
    _dim_df_Dimension_Lookup_Date_Only = spark.table('main.retail_dwh.dim_date').select("date_sk_dim", "order_date", "effective_start_date", "effective_end_date", "version_number")
    # Cache: broadcast join approximates Pentaho preload/cache
    _dim_df_Dimension_Lookup_Date_Only = broadcast(_dim_df_Dimension_Lookup_Date_Only)
    # Effective dating: order_date between effective_start_date and effective_end_date
    _dim_active_df_Dimension_Lookup_Date_Only = _dim_df_Dimension_Lookup_Date_Only
    # Late-arriving / expired / overlap: filter to version covering stream date
    _dim_joined = df_DB_Join_Product_Soft.join(_dim_active_df_Dimension_Lookup_Date_Only, on=((df_DB_Join_Product_Soft["order_date"] == _dim_active_df_Dimension_Lookup_Date_Only["order_date"]) & (df_DB_Join_Product_Soft["order_date"] >= _dim_active_df_Dimension_Lookup_Date_Only["effective_start_date"]) & (df_DB_Join_Product_Soft["order_date"] < _dim_active_df_Dimension_Lookup_Date_Only["effective_end_date"])), how='left')
    df_Dimension_Lookup_Date_Only = _dim_joined
    # Lookup-only: null 'date_sk_dim' indicates cache miss / unknown BK

    # Step: Fill Unknown Member Defaults (IfNull) [converted]
    # If Field Value Is Null: Fill Unknown Member Defaults
    df_Fill_Unknown_Member_Defaults = df_Dimension_Lookup_Date_Only
    df_Fill_Unknown_Member_Defaults = df_Fill_Unknown_Member_Defaults.withColumn('customer_sk', when(col('customer_sk').isNull(), lit(-1)).otherwise(col('customer_sk')))
    df_Fill_Unknown_Member_Defaults = df_Fill_Unknown_Member_Defaults.withColumn('product_sk', when(col('product_sk').isNull(), lit(-1)).otherwise(col('product_sk')))
    df_Fill_Unknown_Member_Defaults = df_Fill_Unknown_Member_Defaults.withColumn('store_sk', when(col('store_sk').isNull(), lit(-1)).otherwise(col('store_sk')))
    df_Fill_Unknown_Member_Defaults = df_Fill_Unknown_Member_Defaults.withColumn('employee_sk', when(col('employee_sk').isNull(), lit(-1)).otherwise(col('employee_sk')))
    df_Fill_Unknown_Member_Defaults = df_Fill_Unknown_Member_Defaults.withColumn('promotion_sk', when(col('promotion_sk').isNull(), lit(-1)).otherwise(col('promotion_sk')))
    df_Fill_Unknown_Member_Defaults = df_Fill_Unknown_Member_Defaults.withColumn('date_sk', when(col('date_sk').isNull(), lit(-1)).otherwise(col('date_sk')))
    df_Fill_Unknown_Member_Defaults = df_Fill_Unknown_Member_Defaults.withColumn('region_id', when(col('region_id').isNull(), lit('UNK')).otherwise(col('region_id')))

    # Step: Resolve Missing Dimensions (Formula) [converted]
    # Formula: Resolve Missing Dimensions
    df_Resolve_Missing_Dimensions = df_Fill_Unknown_Member_Defaults
    df_Resolve_Missing_Dimensions = df_Resolve_Missing_Dimensions.withColumn('formula_result', lit(None))  # empty formula

    # Step: Generate Fallback Sales Line SK (Sequence) [converted]
    # Add Sequence: Generate Fallback Sales Line SK
    # preserved.use_counter=True counter_name='sales_line_sk_counter'
    _w_seq_df_Generate_Fallback_Sales_Line_SK = Window.orderBy(monotonically_increasing_id())
    # preserved.max_value=999999999 — wrap to start (Pentaho counter)
    df_Generate_Fallback_Sales_Line_SK = df_Resolve_Missing_Dimensions.withColumn("sales_line_sk", lit(1) + ((row_number().over(_w_seq_df_Generate_Fallback_Sales_Line_SK) - lit(1)) % greatest(((lit(999999999) - lit(1)) // lit(1)) + lit(1), lit(1))) * lit(1))
    # WARNING: Spark row_number over monotonically_increasing_id is order-based; sort upstream if deterministic sequencing across partitions is required

    # Step: Force Unknown SK Sentinel (SetValueField) [converted]
    # Set Field Value: Force Unknown SK Sentinel
    df_Force_Unknown_SK_Sentinel = df_Generate_Fallback_Sales_Line_SK
    df_Force_Unknown_SK_Sentinel = df_Force_Unknown_SK_Sentinel.withColumn("unknown_member_sk", col("-1"))

    # Step: Combination Lookup Sales Junk Key (CombinationLookup) [converted]
    # Combination Lookup/Update: Combination Lookup Sales Junk Key
    # preserved.connection='conn_dev_dwh'
    # WARNING: CombinationLookup 'Combination Lookup Sales Junk Key': connection 'conn_dev_dwh' mapped to Spark/UC table 'main.retail_dwh.dim_sales_combo' (not JDBC).
    # preserved.commit_size=100
    # preserved.cache_size=9999
    # preserved.preload_cache=True
    # preserved.use_hash=True
    # preserved.hash_field='sales_bk_crc'
    # preserved.last_update_field='last_seen_ts'
    # WARNING: CombinationLookup 'Combination Lookup Sales Junk Key': CRC/hash cache ('sales_bk_crc') is database-specific — business-key equi-join used instead; metadata preserved.
    # Surrogate key strategy: tablemax (MAX(tk)+row_number) for 'Combination Lookup Sales Junk Key'
    # Optional: ALTER TABLE ... CHANGE COLUMN tk GENERATED BY DEFAULT AS IDENTITY — then omit tk from INSERT values
    # Optional: spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")  # additive columns only
    # Edge cases: null business keys skipped from insert; duplicate combinations deduplicated before MERGE
    _combo_dim_df_Combination_Lookup_Sales_Junk_Key = spark.table('main.retail_dwh.dim_sales_combo').select("technical_key", "order_item_id", "order_id", "product_id")
    # Cache: broadcast join approximates Pentaho preload/cache
    _combo_dim_df_Combination_Lookup_Sales_Junk_Key = broadcast(_combo_dim_df_Combination_Lookup_Sales_Junk_Key)
    _combo_dim_df_Combination_Lookup_Sales_Junk_Key = _combo_dim_df_Combination_Lookup_Sales_Junk_Key.select(col("order_item_id"), col("order_id"), col("product_id"), col("technical_key"))
    _combo_joined = df_Force_Unknown_SK_Sentinel.join(_combo_dim_df_Combination_Lookup_Sales_Junk_Key, on=["order_item_id", "order_id", "product_id"], how='left')
    _combo_miss_df_Combination_Lookup_Sales_Junk_Key = _combo_joined.filter(col("technical_key").isNull() & ~(col("order_item_id").isNull() | col("order_id").isNull() | col("product_id").isNull()))
    _combo_miss_df_Combination_Lookup_Sales_Junk_Key = _combo_miss_df_Combination_Lookup_Sales_Junk_Key.dropDuplicates(["order_item_id", "order_id", "product_id"])
    if 'last_seen_ts' in _combo_miss_df_Combination_Lookup_Sales_Junk_Key.columns:
        _combo_ins_df_Combination_Lookup_Sales_Junk_Key = _combo_miss_df_Combination_Lookup_Sales_Junk_Key.select(col("order_item_id").alias("order_item_id"), col("order_id").alias("order_id"), col("product_id").alias("product_id"), col('last_seen_ts').alias('last_seen_ts'))
    else:
        _combo_ins_df_Combination_Lookup_Sales_Junk_Key = _combo_miss_df_Combination_Lookup_Sales_Junk_Key.select(col("order_item_id").alias("order_item_id"), col("order_id").alias("order_id"), col("product_id").alias("product_id"), current_timestamp().alias('last_seen_ts'))
    # tablemax + row_number (IDENTITY would omit tk from INSERT below)
    _max_tk = spark.sql("SELECT COALESCE(MAX(`technical_key`), 0) AS m FROM main.retail_dwh.dim_sales_combo").collect()[0][0]
    from pyspark.sql.window import Window as _DWWindow
    _combo_ins_df_Combination_Lookup_Sales_Junk_Key = _combo_ins_df_Combination_Lookup_Sales_Junk_Key.withColumn("_dw_rn", row_number().over(_DWWindow.orderBy(lit(1))))
    _combo_ins_df_Combination_Lookup_Sales_Junk_Key = _combo_ins_df_Combination_Lookup_Sales_Junk_Key.withColumn("technical_key", (lit(_max_tk) + col("_dw_rn")).cast("long")).drop("_dw_rn")
    _combo_ins_df_Combination_Lookup_Sales_Junk_Key.createOrReplaceTempView('_combo_insert_src_df_Combination_Lookup_Sales_Junk_Key')
    from delta.tables import DeltaTable
    (
        DeltaTable.forName(spark, 'main.retail_dwh.dim_sales_combo').alias("t")
        .merge(spark.table('_combo_insert_src_df_Combination_Lookup_Sales_Junk_Key').alias("s"), 't.`order_item_id` <=> s.`order_item_id` AND t.`order_id` <=> s.`order_id` AND t.`product_id` <=> s.`product_id`')
        .whenNotMatchedInsert(values={'order_item_id': 's.`order_item_id`', 'order_id': 's.`order_id`', 'product_id': 's.`product_id`', 'last_seen_ts': 's.`last_seen_ts`', 'technical_key': 's.`technical_key`'})
        .execute()
    )
    # Delta transaction: each DeltaTable.merge().execute() is one atomic transaction
    # Attach TK without re-scanning dimension: union prior keys with inserts (map table fields back to stream names)
    _combo_new_keys = _combo_ins_df_Combination_Lookup_Sales_Junk_Key.select(col("order_item_id"), col("order_id"), col("product_id"), col("technical_key"))
    _combo_dim_df_Combination_Lookup_Sales_Junk_Key = _combo_dim_df_Combination_Lookup_Sales_Junk_Key.unionByName(_combo_new_keys)
    _combo_dim_df_Combination_Lookup_Sales_Junk_Key = broadcast(_combo_dim_df_Combination_Lookup_Sales_Junk_Key)
    df_Combination_Lookup_Sales_Junk_Key = df_Force_Unknown_SK_Sentinel.join(_combo_dim_df_Combination_Lookup_Sales_Junk_Key, on=["order_item_id", "order_id", "product_id"], how='left')
    # Null surrogate keys after MERGE indicate unresolved/null business keys

    # Step: Finalize Surrogate Keys (Formula) [converted]
    # Formula: Finalize Surrogate Keys
    df_Finalize_Surrogate_Keys = df_Combination_Lookup_Sales_Junk_Key
    df_Finalize_Surrogate_Keys = df_Finalize_Surrogate_Keys.withColumn('formula_result', lit(None))  # empty formula

    # Step: Count Unknown Members (MemoryGroupBy) [converted]
    # Memory Group By: Count Unknown Members
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Count_Unknown_Members = df_Finalize_Surrogate_Keys.groupBy('unknown_customer_flag', 'unknown_product_flag', 'unknown_store_flag').agg(count(lit(1)).alias('row_count'))

    # Step: Write Lookuped Sales (TextFileOutput) [converted]
    # Pentaho step: Write Lookuped Sales (type: TextFileOutput)
    # Pentaho filename: /output/sales/enriched/sales_lookuped_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='order_item_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='customer_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='employee_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='promotion_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='customer_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='employee_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='promotion_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='date_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sales_combo_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sales_line_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='region_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unknown_customer_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unknown_product_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unknown_store_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
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
    # INFO: preserved.field_format name='payment_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='payment_method' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipment_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='promo_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Lookuped_Sales = df_Finalize_Surrogate_Keys
    _out_df_Write_Lookuped_Sales = df_Write_Lookuped_Sales.select('order_item_id', 'order_id', 'product_id', 'customer_id', 'store_id', 'employee_id', 'promotion_id', 'order_date', 'customer_sk', 'product_sk', 'store_sk', 'employee_sk', 'promotion_sk', 'date_sk', 'sales_combo_sk', 'sales_line_sk', 'region_id', 'unknown_customer_flag', 'unknown_product_flag', 'unknown_store_flag', 'order_status', 'status_bucket', 'channel', 'channel_mapped', 'currency_code', 'quantity', 'unit_price', 'gross_amount', 'discount_amount_calc', 'net_amount', 'tax_amount_calc', 'shipping_cost_calc', 'total_revenue', 'profit', 'margin', 'fx_rate', 'converted_amount_usd', 'return_amount', 'refund_amount_calc', 'late_delivery_flag', 'high_value_order_flag', 'weekend_order_flag', 'holiday_flag', 'order_value_band', 'sales_bk_checksum', 'payment_id', 'payment_method', 'shipment_id', 'promo_code', 'batch_id', 'run_id')
    writer = _out_df_Write_Lookuped_Sales.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_lookuped_.csv')

    # Step: Write Unknown Member Stats (TextFileOutput) [converted]
    # Pentaho step: Write Unknown Member Stats (type: TextFileOutput)
    # Pentaho filename: /audit/load_audit/sales_unknown_dim_stats_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='unknown_customer_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unknown_product_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unknown_store_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='row_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Unknown_Member_Stats = df_Count_Unknown_Members
    _out_df_Write_Unknown_Member_Stats = df_Write_Unknown_Member_Stats.select('unknown_customer_flag', 'unknown_product_flag', 'unknown_store_flag', 'row_count')
    writer = _out_df_Write_Unknown_Member_Stats.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_unknown_dim_stats_.csv')

    # Step: Log Dimension Lookups Complete (WriteToLog) [converted]
    # Write to Log: Log Dimension Lookups Complete
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=DIM_LOOKUP_COMPLETE | TRANS=TR_Sales_Dimension_Lookups | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Dimension_Lookups_Complete = logging.getLogger('pentaho.writetolog.Log_Dimension_Lookups_Complete')
    _log_df_Log_Dimension_Lookups_Complete.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Dimension_Lookups_Complete = df_Write_Lookuped_Sales
    _log_rows_df_Log_Dimension_Lookups_Complete = _log_df_df_Log_Dimension_Lookups_Complete.take(20)
    _log_df_Log_Dimension_Lookups_Complete.info('Log Dimension Lookups Complete' + ' | columns=' + str(_log_df_df_Log_Dimension_Lookups_Complete.columns))
    _log_df_Log_Dimension_Lookups_Complete.info('AUDIT | EVENT=DIM_LOOKUP_COMPLETE | TRANS=TR_Sales_Dimension_Lookups | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Dimension_Lookups_Complete:
        _log_df_Log_Dimension_Lookups_Complete.info('Log Dimension Lookups Complete' + ' | ' + str(_lr.asDict()))
    df_Log_Dimension_Lookups_Complete = df_Write_Lookuped_Sales

    # Step: Block Until Lookups Done (BlockingStep) [converted]
    # Blocking Step: Block Until Lookups Done
    # preserved.pass_all_rows=True
    # preserved.directory='%%java.io.tmpdir%%'
    # preserved.prefix='block'
    # preserved.cache_size='5000'
    # preserved.compress_files=False
    # LIMITATION: Pentaho temp-file spill (directory/prefix/compress) is replaced by Spark cache/persist.
    df_Block_Until_Lookups_Done = cache_for_reuse(df_Log_Dimension_Lookups_Complete)
    _ = df_Block_Until_Lookups_Done.count()  # synchronize: wait for all upstream rows

    # Step: Dimension Lookups Complete (Dummy) [converted]
    # Dummy: Dimension Lookups Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Dimension_Lookups_Complete = df_Block_Until_Lookups_Done

    log_event(_LOG, "transformation_end")
    return df_Dummy_Dimension_Lookups_Complete
