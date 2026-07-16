"""PySpark module migrated from Pentaho transformation: TR_FactSales_Load.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/fact/TR_FactSales_Load.ktr
Independent module — ``run(spark, config)`` returns a DataFrame.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    count,
    current_date,
    current_timestamp,
    length,
    lit,
    lower,
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_fact_sales_load")

from pyspark.sql.window import Window


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_FactSales_Load`` step-for-step."""
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

    # Step: Get Fact Load Variables (GetVariable) [converted]
    # Get Variables: Get Fact Load Variables
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'current_date', 'variable': '${CURRENT_DATE}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
    # preserved.output_columns=['batch_id', 'run_id', 'current_date']
    import os
    import re as _re_var
    # Lookup order: Databricks widgets → os.environ → spark.conf (pentaho.var.*) → transformation parameters → empty string
    df_Get_Fact_Load_Variables = spark.range(1).select(lit(1).alias('_row'))
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
    df_Get_Fact_Load_Variables = df_Get_Fact_Load_Variables.withColumn('batch_id', lit(_batch_id_resolved))
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
    df_Get_Fact_Load_Variables = df_Get_Fact_Load_Variables.withColumn('run_id', lit(_run_id_resolved))
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
    df_Get_Fact_Load_Variables = df_Get_Fact_Load_Variables.withColumn('current_date', lit(_current_date_resolved))

    # Step: Read Existing FactSales (CsvInput) [converted]
    # CSV Input: Read Existing FactSales
    df_Read_Existing_FactSales = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('sales_sk INT, order_item_id STRING, sales_bk_checksum STRING, version_number INT, total_revenue DOUBLE')
        .load(f'{data_dir}/fact_sales_current.csv')
    )

    # Step: Read Lookuped Sales (CsvInput) [converted]
    # CSV Input: Read Lookuped Sales
    df_Read_Lookuped_Sales = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('order_item_id STRING, order_id STRING, product_id STRING, customer_id STRING, store_id STRING, employee_id STRING, promotion_id STRING, order_date STRING, customer_sk INT, product_sk INT, store_sk INT, employee_sk INT, promotion_sk INT, date_sk INT, sales_combo_sk INT, sales_line_sk INT, region_id STRING, unknown_customer_flag STRING, unknown_product_flag STRING, unknown_store_flag STRING, order_status STRING, status_bucket STRING, channel STRING, channel_mapped STRING, currency_code STRING, quantity DOUBLE, unit_price DOUBLE, gross_amount DOUBLE, discount_amount_calc DOUBLE, net_amount DOUBLE, tax_amount_calc DOUBLE, shipping_cost_calc DOUBLE, total_revenue DOUBLE, profit DOUBLE, margin DOUBLE, fx_rate DOUBLE, converted_amount_usd DOUBLE, return_amount DOUBLE, refund_amount_calc DOUBLE, late_delivery_flag STRING, high_value_order_flag STRING, weekend_order_flag STRING, holiday_flag STRING, order_value_band STRING, sales_bk_checksum STRING, payment_id STRING, payment_method STRING, shipment_id STRING, promo_code STRING, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/sales_lookuped_.csv')
    )

    # Step: Write Fact Rejects (TextFileOutput) [converted]
    # Pentaho step: Write Fact Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/sales/sales_fact_rejects_
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
    df_Write_Fact_Rejects = df_Write_Fact_Rejects
    _out_df_Write_Fact_Rejects = df_Write_Fact_Rejects.select('order_item_id', 'order_id', 'ERR_CODE', 'ERR_DESC', 'batch_id', 'run_id')
    writer = _out_df_Write_Fact_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/sales_fact_rejects_.csv')

    # Step: Prepare Existing Fact Keys (SelectValues) [converted]
    # Select Values: Prepare Existing Fact Keys
    df_Prepare_Existing_Fact_Keys = df_Read_Existing_FactSales.select(col("sales_sk").alias("existing_sales_sk"), col("order_item_id").alias("existing_order_item_id"), col("sales_bk_checksum").alias("existing_checksum"), col("version_number").alias("existing_version"))

    # Step: Inject Current Date For CDC (Constant) [converted]
    # Add Constants: Inject Current Date For CDC
    df_Inject_Current_Date_For_CDC = df_Get_Fact_Load_Variables
    df_Inject_Current_Date_For_CDC = df_Inject_Current_Date_For_CDC.withColumn("current_date", lit('${CURRENT_DATE}'))
    # preserved.current_date: length='-1', precision='-1'
    df_Inject_Current_Date_For_CDC = df_Inject_Current_Date_For_CDC.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Inject_Current_Date_For_CDC = df_Inject_Current_Date_For_CDC.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'

    # Step: CDC Order Date Filter? (FilterRows) [failed]
    # Filter Rows: CDC Order Date Filter?
    df_Apply_CDC_Compare = df_Inject_Current_Date_For_CDC.filter(col("order_date").isNotNull())
    df_Skip_CDC_Empty_Date = df_Inject_Current_Date_For_CDC.filter(~(col("order_date").isNotNull()))
    df_CDC_Order_Date_Filter? = df_Apply_CDC_Compare

    # Step: Apply CDC Compare (Formula) [failed]
    # Formula: Apply CDC Compare
    df_Apply_CDC_Compare = df_CDC_Order_Date_Filter?
    df_Apply_CDC_Compare = df_Apply_CDC_Compare.withColumn('formula_result', lit(None))  # empty formula

    # Step: Skip CDC Empty Date (Dummy) [converted]
    # Dummy: Skip CDC Empty Date
    # Pass-through step - DataFrame unchanged
    df_Dummy_Skip_CDC_Empty_Date = df_Skip_CDC_Empty_Date

    # Step: Keep CDC Rows? (FilterRows) [failed]
    # Filter Rows: Keep CDC Rows?
    df_Lookup_Existing_Fact = df_Apply_CDC_Compare.filter((col("cdc_include") == lit('Y')))
    df_CDC_Excluded_Path = df_Apply_CDC_Compare.filter(~((col("cdc_include") == lit('Y'))))
    df_Keep_CDC_Rows? = df_Lookup_Existing_Fact

    # Step: CDC Excluded Path (Dummy) [converted]
    # Dummy: CDC Excluded Path
    # Pass-through step - DataFrame unchanged
    df_Dummy_CDC_Excluded_Path = df_CDC_Excluded_Path

    # Step: Lookup Existing Fact (StreamLookup) [failed]
    # Stream Lookup: Lookup Existing Fact
    # StreamLookup 'Lookup Existing Fact': no join keys — lookup join not generated
    df_Lookup_Existing_Fact = df_Keep_CDC_Rows?

    # Step: Combination Lookup Fact BK (CombinationLookup) [converted]
    # Combination Lookup/Update: Combination Lookup Fact BK
    # preserved.connection='conn_dev_dwh'
    # WARNING: CombinationLookup 'Combination Lookup Fact BK': connection 'conn_dev_dwh' mapped to Spark/UC table 'main.retail_dwh.dim_sales_combo' (not JDBC).
    # preserved.commit_size=100
    # preserved.cache_size=9999
    # preserved.preload_cache=True
    # preserved.use_hash=True
    # preserved.hash_field='sales_bk_crc'
    # preserved.last_update_field='last_seen_ts'
    # WARNING: CombinationLookup 'Combination Lookup Fact BK': CRC/hash cache ('sales_bk_crc') is database-specific — business-key equi-join used instead; metadata preserved.
    # Surrogate key strategy: tablemax (MAX(tk)+row_number) for 'Combination Lookup Fact BK'
    # Optional: ALTER TABLE ... CHANGE COLUMN tk GENERATED BY DEFAULT AS IDENTITY — then omit tk from INSERT values
    # Optional: spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")  # additive columns only
    # Edge cases: null business keys skipped from insert; duplicate combinations deduplicated before MERGE
    _combo_dim_df_Combination_Lookup_Fact_BK = spark.table('main.retail_dwh.dim_sales_combo').select("technical_key", "order_item_id", "order_id", "product_id")
    # Cache: broadcast join approximates Pentaho preload/cache
    _combo_dim_df_Combination_Lookup_Fact_BK = broadcast(_combo_dim_df_Combination_Lookup_Fact_BK)
    _combo_dim_df_Combination_Lookup_Fact_BK = _combo_dim_df_Combination_Lookup_Fact_BK.select(col("order_item_id"), col("order_id"), col("product_id"), col("technical_key"))
    _combo_joined = df_Lookup_Existing_Fact.join(_combo_dim_df_Combination_Lookup_Fact_BK, on=["order_item_id", "order_id", "product_id"], how='left')
    _combo_miss_df_Combination_Lookup_Fact_BK = _combo_joined.filter(col("technical_key").isNull() & ~(col("order_item_id").isNull() | col("order_id").isNull() | col("product_id").isNull()))
    _combo_miss_df_Combination_Lookup_Fact_BK = _combo_miss_df_Combination_Lookup_Fact_BK.dropDuplicates(["order_item_id", "order_id", "product_id"])
    if 'last_seen_ts' in _combo_miss_df_Combination_Lookup_Fact_BK.columns:
        _combo_ins_df_Combination_Lookup_Fact_BK = _combo_miss_df_Combination_Lookup_Fact_BK.select(col("order_item_id").alias("order_item_id"), col("order_id").alias("order_id"), col("product_id").alias("product_id"), col('last_seen_ts').alias('last_seen_ts'))
    else:
        _combo_ins_df_Combination_Lookup_Fact_BK = _combo_miss_df_Combination_Lookup_Fact_BK.select(col("order_item_id").alias("order_item_id"), col("order_id").alias("order_id"), col("product_id").alias("product_id"), current_timestamp().alias('last_seen_ts'))
    # tablemax + row_number (IDENTITY would omit tk from INSERT below)
    _max_tk = spark.sql("SELECT COALESCE(MAX(`technical_key`), 0) AS m FROM main.retail_dwh.dim_sales_combo").collect()[0][0]
    from pyspark.sql.window import Window as _DWWindow
    _combo_ins_df_Combination_Lookup_Fact_BK = _combo_ins_df_Combination_Lookup_Fact_BK.withColumn("_dw_rn", row_number().over(_DWWindow.orderBy(lit(1))))
    _combo_ins_df_Combination_Lookup_Fact_BK = _combo_ins_df_Combination_Lookup_Fact_BK.withColumn("technical_key", (lit(_max_tk) + col("_dw_rn")).cast("long")).drop("_dw_rn")
    _combo_ins_df_Combination_Lookup_Fact_BK.createOrReplaceTempView('_combo_insert_src_df_Combination_Lookup_Fact_BK')
    from delta.tables import DeltaTable
    (
        DeltaTable.forName(spark, 'main.retail_dwh.dim_sales_combo').alias("t")
        .merge(spark.table('_combo_insert_src_df_Combination_Lookup_Fact_BK').alias("s"), 't.`order_item_id` <=> s.`order_item_id` AND t.`order_id` <=> s.`order_id` AND t.`product_id` <=> s.`product_id`')
        .whenNotMatchedInsert(values={'order_item_id': 's.`order_item_id`', 'order_id': 's.`order_id`', 'product_id': 's.`product_id`', 'last_seen_ts': 's.`last_seen_ts`', 'technical_key': 's.`technical_key`'})
        .execute()
    )
    # Delta transaction: each DeltaTable.merge().execute() is one atomic transaction
    # Attach TK without re-scanning dimension: union prior keys with inserts (map table fields back to stream names)
    _combo_new_keys = _combo_ins_df_Combination_Lookup_Fact_BK.select(col("order_item_id"), col("order_id"), col("product_id"), col("technical_key"))
    _combo_dim_df_Combination_Lookup_Fact_BK = _combo_dim_df_Combination_Lookup_Fact_BK.unionByName(_combo_new_keys)
    _combo_dim_df_Combination_Lookup_Fact_BK = broadcast(_combo_dim_df_Combination_Lookup_Fact_BK)
    df_Combination_Lookup_Fact_BK = df_Lookup_Existing_Fact.join(_combo_dim_df_Combination_Lookup_Fact_BK, on=["order_item_id", "order_id", "product_id"], how='left')
    # Null surrogate keys after MERGE indicate unresolved/null business keys

    # Step: Optional DimDate For Fact (DimensionLookup) [converted]
    # Dimension Lookup/Update: Optional DimDate For Fact
    # preserved.connection='conn_dev_dwh'
    # WARNING: DimensionLookup 'Optional DimDate For Fact': connection 'conn_dev_dwh' mapped to Spark/UC table 'main.retail_dwh.dim_date' (not JDBC).
    # preserved.commit_size=100
    # preserved.cache_size=5000
    # preserved.preload_cache=True
    # preserved.use_start_date_alternative=False
    # preserved.start_date_alternative='none'
    # preserved.use_batch=True
    # preserved.min_year=1900
    # preserved.max_year=2199
    # SCD mode: lookup-only; Type1=0 Type2=0 PunchThrough=0 technical=0
    # Surrogate key strategy: tablemax (MAX(tk)+row_number) for 'Optional DimDate For Fact'
    # Optional: ALTER TABLE ... CHANGE COLUMN tk GENERATED BY DEFAULT AS IDENTITY — then omit tk from INSERT values
    # Optional: spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")  # additive columns only
    # Post-load tip: partition pruning on effective_start_date/effective_end_date; OPTIMIZE ... ZORDER BY (order_date)
    _dim_df_Optional_DimDate_For_Fact = spark.table('main.retail_dwh.dim_date').select("date_sk", "order_date", "effective_start_date", "effective_end_date", "version_number")
    # Cache: broadcast join approximates Pentaho preload/cache
    _dim_df_Optional_DimDate_For_Fact = broadcast(_dim_df_Optional_DimDate_For_Fact)
    # Effective dating: order_date between effective_start_date and effective_end_date
    _dim_active_df_Optional_DimDate_For_Fact = _dim_df_Optional_DimDate_For_Fact
    # Late-arriving / expired / overlap: filter to version covering stream date
    _dim_joined = df_Combination_Lookup_Fact_BK.join(_dim_active_df_Optional_DimDate_For_Fact, on=((df_Combination_Lookup_Fact_BK["order_date"] == _dim_active_df_Optional_DimDate_For_Fact["order_date"]) & (df_Combination_Lookup_Fact_BK["order_date"] >= _dim_active_df_Optional_DimDate_For_Fact["effective_start_date"]) & (df_Combination_Lookup_Fact_BK["order_date"] < _dim_active_df_Optional_DimDate_For_Fact["effective_end_date"])), how='left')
    df_Optional_DimDate_For_Fact = _dim_joined
    # Lookup-only: null 'date_sk' indicates cache miss / unknown BK

    # Step: Classify Fact Action (Formula) [converted]
    # Formula: Classify Fact Action
    df_Classify_Fact_Action = df_Optional_DimDate_For_Fact
    df_Classify_Fact_Action = df_Classify_Fact_Action.withColumn('formula_result', lit(None))  # empty formula

    # Step: Route Fact Action (SwitchCase) [converted]
    # Switch / Case: Route Fact Action
    # preserved.fieldname='fact_action'
    # preserved.switch_field='fact_action'
    # preserved.cases=[{'value': 'INSERT', 'target_step': 'Prepare Fact Insert'}, {'value': 'UPDATE', 'target_step': 'Prepare Fact Update'}, {'value': 'UNCHANGED', 'target_step': 'Skip Fact Unchanged'}]
    # preserved.default_target_step='Skip Fact Unchanged'
    # preserved.use_contains=False
    # preserved.case_value_type='String'
    # preserved.rules=[{'value': 'INSERT', 'target_step': 'Prepare Fact Insert'}, {'value': 'UPDATE', 'target_step': 'Prepare Fact Update'}, {'value': 'UNCHANGED', 'target_step': 'Skip Fact Unchanged'}]
    _routed_df_Route_Fact_Action = df_Classify_Fact_Action.withColumn('_route_Route_Fact_Action', when(col("fact_action") == lit('INSERT'), lit('Prepare Fact Insert')).when(col("fact_action") == lit('UPDATE'), lit('Prepare Fact Update')).when(col("fact_action") == lit('UNCHANGED'), lit('Skip Fact Unchanged')).otherwise(lit('Skip Fact Unchanged')))
    df_Prepare_Fact_Insert = _routed_df_Route_Fact_Action.filter(col('_route_Route_Fact_Action') == lit('Prepare Fact Insert')).drop('_route_Route_Fact_Action')
    df_Prepare_Fact_Update = _routed_df_Route_Fact_Action.filter(col('_route_Route_Fact_Action') == lit('Prepare Fact Update')).drop('_route_Route_Fact_Action')
    df_Skip_Fact_Unchanged = _routed_df_Route_Fact_Action.filter(col('_route_Route_Fact_Action') == lit('Skip Fact Unchanged')).drop('_route_Route_Fact_Action')
    df_Route_Fact_Action = df_Prepare_Fact_Insert

    # Step: Prepare Fact Insert (Constant) [converted]
    # Add Constants: Prepare Fact Insert
    df_Prepare_Fact_Insert = df_Route_Fact_Action
    df_Prepare_Fact_Insert = df_Prepare_Fact_Insert.withColumn("load_status", lit('INSERTED'))
    # preserved.load_status: length='-1', precision='-1'
    df_Prepare_Fact_Insert = df_Prepare_Fact_Insert.withColumn("is_current", lit('Y'))
    # preserved.is_current: length='-1', precision='-1'

    # Step: Prepare Fact Update (Constant) [converted]
    # Add Constants: Prepare Fact Update
    df_Prepare_Fact_Update = df_Route_Fact_Action
    df_Prepare_Fact_Update = df_Prepare_Fact_Update.withColumn("load_status", lit('UPDATED'))
    # preserved.load_status: length='-1', precision='-1'
    df_Prepare_Fact_Update = df_Prepare_Fact_Update.withColumn("is_current", lit('Y'))
    # preserved.is_current: length='-1', precision='-1'

    # Step: Skip Fact Unchanged (Constant) [converted]
    # Add Constants: Skip Fact Unchanged
    df_Skip_Fact_Unchanged = df_Route_Fact_Action
    df_Skip_Fact_Unchanged = df_Skip_Fact_Unchanged.withColumn("load_status", lit('SKIPPED'))
    # preserved.load_status: length='-1', precision='-1'
    df_Skip_Fact_Unchanged = df_Skip_Fact_Unchanged.withColumn("is_current", lit('Y'))
    # preserved.is_current: length='-1', precision='-1'

    # Step: Generate Sales SK (Sequence) [converted]
    # Add Sequence: Generate Sales SK
    # preserved.use_counter=True counter_name='sales_sk_counter'
    _w_seq_df_Generate_Sales_SK = Window.orderBy(monotonically_increasing_id())
    # preserved.max_value=999999999 — wrap to start (Pentaho counter)
    df_Generate_Sales_SK = df_Prepare_Fact_Insert.withColumn("sales_sk", lit(1) + ((row_number().over(_w_seq_df_Generate_Sales_SK) - lit(1)) % greatest(((lit(999999999) - lit(1)) // lit(1)) + lit(1), lit(1))) * lit(1))
    # WARNING: Spark row_number over monotonically_increasing_id is order-based; sort upstream if deterministic sequencing across partitions is required

    # Step: Sort Insert Fact Path (SortRows) [converted]
    # Sort Rows: Sort Insert Fact Path
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Insert_Fact_Path = df_Prepare_Fact_Insert
    _sort_df_Sort_Insert_Fact_Path = _sort_df_Sort_Insert_Fact_Path.withColumn("_sort_ci_order_item_id", lower(col("order_item_id").cast("string")))
    df_Sort_Insert_Fact_Path = _sort_df_Sort_Insert_Fact_Path.orderBy(col("_sort_ci_order_item_id").asc_nulls_last())
    df_Sort_Insert_Fact_Path = df_Sort_Insert_Fact_Path.drop("_sort_ci_order_item_id")

    # Step: Sort Update Fact Path (SortRows) [converted]
    # Sort Rows: Sort Update Fact Path
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Update_Fact_Path = df_Prepare_Fact_Update
    _sort_df_Sort_Update_Fact_Path = _sort_df_Sort_Update_Fact_Path.withColumn("_sort_ci_order_item_id", lower(col("order_item_id").cast("string")))
    df_Sort_Update_Fact_Path = _sort_df_Sort_Update_Fact_Path.orderBy(col("_sort_ci_order_item_id").asc_nulls_last())
    df_Sort_Update_Fact_Path = df_Sort_Update_Fact_Path.drop("_sort_ci_order_item_id")

    # Step: Unify Fact Routes (Dummy) [converted]
    # Dummy: Unify Fact Routes
    # Pass-through step - DataFrame unchanged
    df_Dummy_Unify_Fact_Routes = df_Generate_Sales_SK

    # Step: Merge Rows Fact Diff (MergeRows) [converted]
    # Merge Rows (Diff): Merge Rows Fact Diff
    # preserved.flag_field='merge_flag'
    # preserved.reference='Sort Insert Fact Path'
    # preserved.compare='Sort Update Fact Path'
    # preserved.key_fields=['order_item_id']
    # preserved.value_fields=['sales_bk_checksum', 'total_revenue', 'version_number']
    _ref_df_Merge_Rows_Fact_Diff = df_Sort_Insert_Fact_Path.alias("r")
    _cmp_df_Merge_Rows_Fact_Diff = df_Sort_Update_Fact_Path.alias("c")
    # WARNING: MergeRows 'Merge Rows Fact Diff': null join keys do not match under Spark equality; duplicate keys expand to a product within the key group
    df_Merge_Rows_Fact_Diff = _ref_df_Merge_Rows_Fact_Diff.join(_cmp_df_Merge_Rows_Fact_Diff, (col("r.order_item_id") == col("c.order_item_id")), 'full_outer')
    df_Merge_Rows_Fact_Diff = df_Merge_Rows_Fact_Diff.withColumn('merge_flag', when(col("c.order_item_id").isNull(), lit("deleted")).when(col("r.order_item_id").isNull(), lit("new")).when((~col("r.sales_bk_checksum").eqNullSafe(col("c.sales_bk_checksum"))) | (~col("r.total_revenue").eqNullSafe(col("c.total_revenue"))) | (~col("r.version_number").eqNullSafe(col("c.version_number"))), lit("changed")).otherwise(lit("identical")))
    # NOTE: MergeRows 'Merge Rows Fact Diff': output prefers compare values (CDC-style); deleted rows keep reference values
    df_Merge_Rows_Fact_Diff = df_Merge_Rows_Fact_Diff.select(coalesce(col("c.order_item_id"), col("r.order_item_id")).alias('order_item_id'), coalesce(col("c.sales_bk_checksum"), col("r.sales_bk_checksum")).alias('sales_bk_checksum'), coalesce(col("c.total_revenue"), col("r.total_revenue")).alias('total_revenue'), coalesce(col("c.version_number"), col("r.version_number")).alias('version_number'), col('merge_flag'))
    # NOTE: MergeRows flags — deleted / new / changed / identical (requires pre-sorted inputs in PDI; Spark join does not enforce sort order)

    # Step: Stamp Fact Audit Timestamps (SystemInfo) [converted]
    # System Info: Stamp Fact Audit Timestamps
    df_Stamp_Fact_Audit_Timestamps = df_Dummy_Unify_Fact_Routes
    df_Stamp_Fact_Audit_Timestamps = df_Stamp_Fact_Audit_Timestamps.withColumn("dw_insert_ts", current_date())
    df_Stamp_Fact_Audit_Timestamps = df_Stamp_Fact_Audit_Timestamps.withColumn("dw_update_ts", current_date())

    # Step: Calculate Fact Measures (Calculator) [converted]
    # Calculator: Calculate Fact Measures
    df_Calculate_Fact_Measures = df_Stamp_Fact_Audit_Timestamps
    df_Calculate_Fact_Measures = df_Calculate_Fact_Measures.withColumn("quantity_sold", (col("quantity")).cast('decimal(38,4)'))
    df_Calculate_Fact_Measures = df_Calculate_Fact_Measures.withColumn("extended_price", (col("gross_amount")).cast('decimal(38,4)'))
    df_Calculate_Fact_Measures = df_Calculate_Fact_Measures.withColumn("net_sales_amount", (col("net_amount")).cast('decimal(38,4)'))

    # Step: Select Fact Columns Heavy (SelectValues) [converted]
    # Select Values: Select Fact Columns Heavy
    df_Select_Fact_Columns_Heavy = df_Calculate_Fact_Measures.select(col("sales_sk").alias("sales_sk"), col("order_item_id").alias("order_item_id"), col("order_id").alias("order_id"), col("business_key").alias("business_key"), col("customer_sk").alias("customer_sk"), col("product_sk").alias("product_sk"), col("store_sk").alias("store_sk"), col("employee_sk").alias("employee_sk"), col("promotion_sk").alias("promotion_sk"), col("date_sk").alias("date_sk"), col("sales_combo_sk").alias("sales_combo_sk"), col("quantity_sold").alias("quantity_sold"), col("unit_price").alias("unit_price"), col("extended_price").alias("extended_price"), col("discount_amount_calc").alias("discount_amount_calc"), col("net_sales_amount").alias("net_sales_amount"), col("tax_amount_calc").alias("tax_amount_calc"), col("shipping_cost_calc").alias("shipping_cost_calc"), col("total_revenue").alias("total_revenue"), col("profit").alias("profit"), col("margin").alias("margin"), col("converted_amount_usd").alias("converted_amount_usd"), col("return_amount").alias("return_amount"), col("refund_amount_calc").alias("refund_amount_calc"), col("currency_code").alias("currency_code"), col("channel_mapped").alias("channel_mapped"), col("order_status").alias("order_status"), col("late_delivery_flag").alias("late_delivery_flag"), col("high_value_order_flag").alias("high_value_order_flag"), col("sales_bk_checksum").alias("sales_bk_checksum"), col("version_number").alias("version_number"), col("fact_action").alias("fact_action"), col("load_status").alias("load_status"), col("is_current").alias("is_current"), col("batch_id").alias("batch_id"), col("run_id").alias("run_id"), col("dw_insert_ts").alias("dw_insert_ts"), col("dw_update_ts").alias("dw_update_ts"), col("order_date").alias("order_date"))

    # Step: Fact Load Stats (MemoryGroupBy) [converted]
    # Memory Group By: Fact Load Stats
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Fact_Load_Stats = df_Select_Fact_Columns_Heavy.groupBy('fact_action').agg(count(lit(1)).alias('action_count'))

    # Step: Write Fact Sales Dump (TextFileOutput) [converted]
    # Pentaho step: Write Fact Sales Dump (type: TextFileOutput)
    # Pentaho filename: /output/sales/fact/fact_sales_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='sales_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_item_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='business_key' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='customer_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='employee_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='promotion_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='date_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sales_combo_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quantity_sold' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unit_price' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='extended_price' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='discount_amount_calc' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='net_sales_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='tax_amount_calc' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shipping_cost_calc' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='total_revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='profit' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='margin' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='converted_amount_usd' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='return_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='refund_amount_calc' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='currency_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='channel_mapped' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='late_delivery_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='high_value_order_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sales_bk_checksum' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='version_number' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='fact_action' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='load_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_current' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='dw_insert_ts' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='dw_update_ts' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Fact_Sales_Dump = df_Select_Fact_Columns_Heavy
    _out_df_Write_Fact_Sales_Dump = df_Write_Fact_Sales_Dump.select('sales_sk', 'order_item_id', 'order_id', 'business_key', 'customer_sk', 'product_sk', 'store_sk', 'employee_sk', 'promotion_sk', 'date_sk', 'sales_combo_sk', 'quantity_sold', 'unit_price', 'extended_price', 'discount_amount_calc', 'net_sales_amount', 'tax_amount_calc', 'shipping_cost_calc', 'total_revenue', 'profit', 'margin', 'converted_amount_usd', 'return_amount', 'refund_amount_calc', 'currency_code', 'channel_mapped', 'order_status', 'late_delivery_flag', 'high_value_order_flag', 'sales_bk_checksum', 'version_number', 'fact_action', 'load_status', 'is_current', 'batch_id', 'run_id', 'dw_insert_ts', 'dw_update_ts', 'order_date')
    writer = _out_df_Write_Fact_Sales_Dump.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/fact_sales_.csv')

    # Step: Write Fact Metrics Log (TextFileOutput) [converted]
    # Pentaho step: Write Fact Metrics Log (type: TextFileOutput)
    # Pentaho filename: /logs/execution/sales/TR_FactSales_Load_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='fact_action' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='action_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Fact_Metrics_Log = df_Fact_Load_Stats
    _out_df_Write_Fact_Metrics_Log = df_Write_Fact_Metrics_Log.select('fact_action', 'action_count')
    writer = _out_df_Write_Fact_Metrics_Log.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/TR_FactSales_Load_.log')

    # Step: Write Fact Sales Current (TextFileOutput) [converted]
    # Pentaho step: Write Fact Sales Current (type: TextFileOutput)
    # Pentaho filename: /output/sales/fact/fact_sales_current
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='sales_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='order_item_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sales_bk_checksum' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='version_number' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='total_revenue' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Fact_Sales_Current = df_Write_Fact_Sales_Dump
    _out_df_Write_Fact_Sales_Current = df_Write_Fact_Sales_Current.select('sales_sk', 'order_item_id', 'sales_bk_checksum', 'version_number', 'total_revenue')
    writer = _out_df_Write_Fact_Sales_Current.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/fact_sales_current.csv')

    # Step: Write Fact Metrics Audit JSON (JsonOutput) [converted]
    # Pentaho step: Write Fact Metrics Audit JSON (type: JsonOutput)
    df_Write_Fact_Metrics_Audit_JSON = df_Write_Fact_Metrics_Log
    df_Write_Fact_Metrics_Audit_JSON.write \
    .mode('overwrite') \
    .json(
        f'{data_dir}/sales_fact_stats_.json'
    )
    # preserved.json_bloc='rows' output_value='json_blob'

    # Step: Insert Update FactSales (InsertUpdate) [converted]
    # Insert/Update: Insert Update FactSales
    _upsert_src = df_Write_Fact_Sales_Current
    _upsert_src.createOrReplaceTempView('_upsert_src')
    spark.sql('''MERGE INTO main.retail_dwh.fact_sales t USING _upsert_src s ON t.`order_item_id` = s.`order_item_id` WHEN MATCHED THEN UPDATE SET t.`sales_sk` = s.`sales_sk`, t.`order_id` = s.`order_id`, t.`customer_sk` = s.`customer_sk`, t.`product_sk` = s.`product_sk`, t.`store_sk` = s.`store_sk`, t.`employee_sk` = s.`employee_sk`, t.`promotion_sk` = s.`promotion_sk`, t.`date_sk` = s.`date_sk`, t.`quantity_sold` = s.`quantity_sold`, t.`unit_price` = s.`unit_price`, t.`extended_price` = s.`extended_price`, t.`discount_amount` = s.`discount_amount_calc`, t.`net_sales_amount` = s.`net_sales_amount`, t.`tax_amount` = s.`tax_amount_calc`, t.`shipping_cost` = s.`shipping_cost_calc`, t.`total_revenue` = s.`total_revenue`, t.`profit` = s.`profit`, t.`margin` = s.`margin`, t.`converted_amount_usd` = s.`converted_amount_usd`, t.`version_number` = s.`version_number`, t.`sales_bk_checksum` = s.`sales_bk_checksum`, t.`batch_id` = s.`batch_id`, t.`dw_update_ts` = s.`dw_update_ts` WHEN NOT MATCHED THEN INSERT (`sales_sk`, `order_id`, `customer_sk`, `product_sk`, `store_sk`, `employee_sk`, `promotion_sk`, `date_sk`, `quantity_sold`, `unit_price`, `extended_price`, `discount_amount`, `net_sales_amount`, `tax_amount`, `shipping_cost`, `total_revenue`, `profit`, `margin`, `converted_amount_usd`, `version_number`, `sales_bk_checksum`, `batch_id`, `dw_insert_ts`, `dw_update_ts`) VALUES (s.`sales_sk`, s.`order_id`, s.`customer_sk`, s.`product_sk`, s.`store_sk`, s.`employee_sk`, s.`promotion_sk`, s.`date_sk`, s.`quantity_sold`, s.`unit_price`, s.`extended_price`, s.`discount_amount_calc`, s.`net_sales_amount`, s.`tax_amount_calc`, s.`shipping_cost_calc`, s.`total_revenue`, s.`profit`, s.`margin`, s.`converted_amount_usd`, s.`version_number`, s.`sales_bk_checksum`, s.`batch_id`, s.`dw_insert_ts`, s.`dw_update_ts`)''')
    df_Insert_Update_FactSales = df_Write_Fact_Sales_Current

    # Step: Table Output FactSales (TableOutput) [converted]
    # Pentaho step: Table Output FactSales (type: TableOutput) (Pentaho schema: retail_dwh)
    # Mode: append (Pentaho truncate=N)
    _mapped_df_Table_Output_FactSales = df_Insert_Update_FactSales.select(col('sales_sk'), col('order_item_id'), col('order_id'), col('customer_sk'), col('product_sk'), col('store_sk'), col('employee_sk'), col('promotion_sk'), col('date_sk'), col('quantity_sold'), col('unit_price'), col('extended_price'), col('net_sales_amount'), col('total_revenue'), col('profit'), col('margin'), col('converted_amount_usd'), col('version_number'), col('batch_id'), col('dw_insert_ts'), col('dw_update_ts'))
    df_Table_Output_FactSales = _mapped_df_Table_Output_FactSales
    write_delta(
        df_Table_Output_FactSales,
        f"{catalog}.{schema}.fact_sales",
        mode='append',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.fact_sales", mode='append')

    # Step: Log Fact Load Complete (WriteToLog) [converted]
    # Write to Log: Log Fact Load Complete
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=FACT_LOAD_COMPLETE | TRANS=TR_FactSales_Load | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Fact_Load_Complete = logging.getLogger('pentaho.writetolog.Log_Fact_Load_Complete')
    _log_df_Log_Fact_Load_Complete.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Fact_Load_Complete = df_Table_Output_FactSales
    _log_rows_df_Log_Fact_Load_Complete = _log_df_df_Log_Fact_Load_Complete.take(20)
    _log_df_Log_Fact_Load_Complete.info('Log Fact Load Complete' + ' | columns=' + str(_log_df_df_Log_Fact_Load_Complete.columns))
    _log_df_Log_Fact_Load_Complete.info('AUDIT | EVENT=FACT_LOAD_COMPLETE | TRANS=TR_FactSales_Load | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Fact_Load_Complete:
        _log_df_Log_Fact_Load_Complete.info('Log Fact Load Complete' + ' | ' + str(_lr.asDict()))
    df_Log_Fact_Load_Complete = df_Table_Output_FactSales

    # Step: Block Until Fact Load Done (BlockingStep) [converted]
    # Blocking Step: Block Until Fact Load Done
    # preserved.pass_all_rows=True
    # preserved.directory='%%java.io.tmpdir%%'
    # preserved.prefix='block'
    # preserved.cache_size='5000'
    # preserved.compress_files=False
    # LIMITATION: Pentaho temp-file spill (directory/prefix/compress) is replaced by Spark cache/persist.
    df_Block_Until_Fact_Load_Done = cache_for_reuse(df_Log_Fact_Load_Complete)
    _ = df_Block_Until_Fact_Load_Done.count()  # synchronize: wait for all upstream rows

    # Step: Fact Load Complete (Dummy) [converted]
    # Dummy: Fact Load Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Fact_Load_Complete = df_Block_Until_Fact_Load_Done

    log_event(_LOG, "transformation_end")
    return df_Dummy_Fact_Load_Complete
