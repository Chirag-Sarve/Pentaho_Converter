"""PySpark module migrated from Pentaho transformation: TR_Inventory_Lookups.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/lookup/TR_Inventory_Lookups.ktr
Independent module — ``run(spark, config)`` returns a DataFrame.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    count,
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

_LOG = get_logger("pentaho_migration.transformations.retail.tr_inventory_lookups")

from pyspark.sql.window import Window


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Inventory_Lookups`` step-for-step."""
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

    # Step: Get Lookup Variables (GetVariable) [converted]
    # Get Variables: Get Lookup Variables
    # preserved.fields=[{'name': 'batch_id', 'variable': '${VAR_ETL_BATCH_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}, {'name': 'run_id', 'variable': '${RUN_ID}', 'type': 'String', 'type_name': 'String', 'format': '', 'currency': '', 'decimal': '', 'group': '', 'length': -1, 'precision': -1, 'trim_type': 'none'}]
    # preserved.output_columns=['batch_id', 'run_id']
    import os
    import re as _re_var
    # Lookup order: Databricks widgets → os.environ → spark.conf (pentaho.var.*) → transformation parameters → empty string
    df_Get_Lookup_Variables = spark.range(1).select(lit(1).alias('_row'))
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
    df_Get_Lookup_Variables = df_Get_Lookup_Variables.withColumn('batch_id', lit(_batch_id_resolved))
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
    df_Get_Lookup_Variables = df_Get_Lookup_Variables.withColumn('run_id', lit(_run_id_resolved))

    # Step: Get Rows From Result Seed (RowsFromResult) [converted]
    # Get Rows from Result: Get Rows From Result Seed
    # preserved.result_buffer='rows'
    # LIMITATION: reads from notebook-scoped _pentaho_result_rows (populated by Copy Rows to Result / job orchestration).
    _pentaho_result_rows = globals().setdefault('_pentaho_result_rows', {})
    _pentaho_result_files = globals().setdefault('_pentaho_result_files', [])
    _src_rows_df_Get_Rows_From_Result_Seed = _pentaho_result_rows.get('__latest__')
    if _src_rows_df_Get_Rows_From_Result_Seed is None:
        _src_rows_df_Get_Rows_From_Result_Seed = next(iter(_pentaho_result_rows.values()), None) if _pentaho_result_rows else None
    if _src_rows_df_Get_Rows_From_Result_Seed is None:
        # Empty result / missing prior Copy Rows to Result
        df_Get_Rows_From_Result_Seed = spark.createDataFrame([], '_result_rows STRING')
    else:
        df_Get_Rows_From_Result_Seed = _src_rows_df_Get_Rows_From_Result_Seed
    # WARNING: Get Rows from Result ignores upstream hop 'df_Get_Lookup_Variables' (PDI expects this as a source step)

    # Step: Read Enriched Inventory (CsvInput) [converted]
    # CSV Input: Read Enriched Inventory
    df_Read_Enriched_Inventory = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('inventory_id STRING, product_id STRING, store_id STRING, supplier_id STRING, region_id STRING, last_stocktake_date STRING, warehouse_code STRING, quantity_clean STRING, available_qty_clean STRING, stock_value STRING, reorder_flag STRING, abc_classification STRING, xyz_classification STRING, safety_stock STRING, economic_order_quantity STRING, dead_stock_flag STRING, excess_stock_flag STRING, shortage_flag STRING, inventory_bk_checksum STRING, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/inventory_enriched_.csv')
    )

    # Step: Result Seed Consumed (Dummy) [converted]
    # Dummy: Result Seed Consumed
    # Pass-through step - DataFrame unchanged
    df_Dummy_Result_Seed_Consumed = df_Get_Rows_From_Result_Seed

    # Step: Inject Lookup Batch (Constant) [converted]
    # Add Constants: Inject Lookup Batch
    df_Inject_Lookup_Batch = df_Read_Enriched_Inventory
    df_Inject_Lookup_Batch = df_Inject_Lookup_Batch.withColumn("batch_id", lit('${VAR_ETL_BATCH_ID}'))
    # preserved.batch_id: length='-1', precision='-1'
    df_Inject_Lookup_Batch = df_Inject_Lookup_Batch.withColumn("run_id", lit('${RUN_ID}'))
    # preserved.run_id: length='-1', precision='-1'
    df_Inject_Lookup_Batch = df_Inject_Lookup_Batch.withColumn("unknown_member_sk", lit(-1))
    # preserved.unknown_member_sk: length='-1', precision='-1'

    # Step: Lookup Product Dimension (DimensionLookup) [converted]
    # Dimension Lookup/Update: Lookup Product Dimension
    # preserved.connection='conn_dev_dwh'
    # WARNING: DimensionLookup 'Lookup Product Dimension': connection 'conn_dev_dwh' mapped to Spark/UC table 'main.retail_dwh.dim_product' (not JDBC).
    # preserved.commit_size=100
    # preserved.cache_size=5000
    # preserved.preload_cache=True
    # preserved.use_start_date_alternative=False
    # preserved.start_date_alternative='none'
    # preserved.use_batch=True
    # preserved.min_year=1900
    # preserved.max_year=2199
    # SCD mode: lookup-only; Type1=0 Type2=0 PunchThrough=0 technical=0
    # Surrogate key strategy: tablemax (MAX(tk)+row_number) for 'Lookup Product Dimension'
    # Optional: ALTER TABLE ... CHANGE COLUMN tk GENERATED BY DEFAULT AS IDENTITY — then omit tk from INSERT values
    # Optional: spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")  # additive columns only
    # Post-load tip: partition pruning on effective_start_date/effective_end_date; OPTIMIZE ... ZORDER BY (product_id)
    _dim_df_Lookup_Product_Dimension = spark.table('main.retail_dwh.dim_product').select("product_sk", "product_id", "effective_start_date", "effective_end_date", "version_number")
    # Cache: broadcast join approximates Pentaho preload/cache
    _dim_df_Lookup_Product_Dimension = broadcast(_dim_df_Lookup_Product_Dimension)
    # Effective dating: last_stocktake_date between effective_start_date and effective_end_date
    _dim_active_df_Lookup_Product_Dimension = _dim_df_Lookup_Product_Dimension
    # Late-arriving / expired / overlap: filter to version covering stream date
    _dim_joined = df_Inject_Lookup_Batch.join(_dim_active_df_Lookup_Product_Dimension, on=((df_Inject_Lookup_Batch["product_id"] == _dim_active_df_Lookup_Product_Dimension["product_id"]) & (df_Inject_Lookup_Batch["last_stocktake_date"] >= _dim_active_df_Lookup_Product_Dimension["effective_start_date"]) & (df_Inject_Lookup_Batch["last_stocktake_date"] < _dim_active_df_Lookup_Product_Dimension["effective_end_date"])), how='left')
    df_Lookup_Product_Dimension = _dim_joined
    # Lookup-only: null 'product_sk' indicates cache miss / unknown BK

    # Step: Lookup Supplier Dimension (DimensionLookup) [converted]
    # Dimension Lookup/Update: Lookup Supplier Dimension
    # preserved.connection='conn_dev_dwh'
    # WARNING: DimensionLookup 'Lookup Supplier Dimension': connection 'conn_dev_dwh' mapped to Spark/UC table 'main.retail_dwh.dim_supplier' (not JDBC).
    # preserved.commit_size=100
    # preserved.cache_size=5000
    # preserved.preload_cache=True
    # preserved.use_start_date_alternative=False
    # preserved.start_date_alternative='none'
    # preserved.use_batch=True
    # preserved.min_year=1900
    # preserved.max_year=2199
    # SCD mode: lookup-only; Type1=0 Type2=0 PunchThrough=0 technical=0
    # Surrogate key strategy: tablemax (MAX(tk)+row_number) for 'Lookup Supplier Dimension'
    # Optional: ALTER TABLE ... CHANGE COLUMN tk GENERATED BY DEFAULT AS IDENTITY — then omit tk from INSERT values
    # Optional: spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")  # additive columns only
    # Post-load tip: partition pruning on effective_start_date/effective_end_date; OPTIMIZE ... ZORDER BY (supplier_id)
    _dim_df_Lookup_Supplier_Dimension = spark.table('main.retail_dwh.dim_supplier').select("supplier_sk", "supplier_id", "effective_start_date", "effective_end_date", "version_number")
    # Cache: broadcast join approximates Pentaho preload/cache
    _dim_df_Lookup_Supplier_Dimension = broadcast(_dim_df_Lookup_Supplier_Dimension)
    # Effective dating: last_stocktake_date between effective_start_date and effective_end_date
    _dim_active_df_Lookup_Supplier_Dimension = _dim_df_Lookup_Supplier_Dimension
    # Late-arriving / expired / overlap: filter to version covering stream date
    _dim_joined = df_Lookup_Product_Dimension.join(_dim_active_df_Lookup_Supplier_Dimension, on=((df_Lookup_Product_Dimension["supplier_id"] == _dim_active_df_Lookup_Supplier_Dimension["supplier_id"]) & (df_Lookup_Product_Dimension["last_stocktake_date"] >= _dim_active_df_Lookup_Supplier_Dimension["effective_start_date"]) & (df_Lookup_Product_Dimension["last_stocktake_date"] < _dim_active_df_Lookup_Supplier_Dimension["effective_end_date"])), how='left')
    df_Lookup_Supplier_Dimension = _dim_joined
    # Lookup-only: null 'supplier_sk' indicates cache miss / unknown BK

    # Step: Lookup Store Dimension (DimensionLookup) [converted]
    # Dimension Lookup/Update: Lookup Store Dimension
    # preserved.connection='conn_dev_dwh'
    # WARNING: DimensionLookup 'Lookup Store Dimension': connection 'conn_dev_dwh' mapped to Spark/UC table 'main.retail_dwh.dim_store' (not JDBC).
    # preserved.commit_size=100
    # preserved.cache_size=5000
    # preserved.preload_cache=True
    # preserved.use_start_date_alternative=False
    # preserved.start_date_alternative='none'
    # preserved.use_batch=True
    # preserved.min_year=1900
    # preserved.max_year=2199
    # SCD mode: lookup-only; Type1=0 Type2=0 PunchThrough=0 technical=0
    # Surrogate key strategy: tablemax (MAX(tk)+row_number) for 'Lookup Store Dimension'
    # Optional: ALTER TABLE ... CHANGE COLUMN tk GENERATED BY DEFAULT AS IDENTITY — then omit tk from INSERT values
    # Optional: spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")  # additive columns only
    # Post-load tip: partition pruning on effective_start_date/effective_end_date; OPTIMIZE ... ZORDER BY (store_id)
    _dim_df_Lookup_Store_Dimension = spark.table('main.retail_dwh.dim_store').select("store_sk", "store_id", "effective_start_date", "effective_end_date", "version_number")
    # Cache: broadcast join approximates Pentaho preload/cache
    _dim_df_Lookup_Store_Dimension = broadcast(_dim_df_Lookup_Store_Dimension)
    # Effective dating: last_stocktake_date between effective_start_date and effective_end_date
    _dim_active_df_Lookup_Store_Dimension = _dim_df_Lookup_Store_Dimension
    # Late-arriving / expired / overlap: filter to version covering stream date
    _dim_joined = df_Lookup_Supplier_Dimension.join(_dim_active_df_Lookup_Store_Dimension, on=((df_Lookup_Supplier_Dimension["store_id"] == _dim_active_df_Lookup_Store_Dimension["store_id"]) & (df_Lookup_Supplier_Dimension["last_stocktake_date"] >= _dim_active_df_Lookup_Store_Dimension["effective_start_date"]) & (df_Lookup_Supplier_Dimension["last_stocktake_date"] < _dim_active_df_Lookup_Store_Dimension["effective_end_date"])), how='left')
    df_Lookup_Store_Dimension = _dim_joined
    # Lookup-only: null 'store_sk' indicates cache miss / unknown BK

    # Step: Lookup Region Dimension (DimensionLookup) [converted]
    # Dimension Lookup/Update: Lookup Region Dimension
    # preserved.connection='conn_dev_dwh'
    # WARNING: DimensionLookup 'Lookup Region Dimension': connection 'conn_dev_dwh' mapped to Spark/UC table 'main.retail_dwh.dim_region' (not JDBC).
    # preserved.commit_size=100
    # preserved.cache_size=5000
    # preserved.preload_cache=True
    # preserved.use_start_date_alternative=False
    # preserved.start_date_alternative='none'
    # preserved.use_batch=True
    # preserved.min_year=1900
    # preserved.max_year=2199
    # SCD mode: lookup-only; Type1=0 Type2=0 PunchThrough=0 technical=0
    # Surrogate key strategy: tablemax (MAX(tk)+row_number) for 'Lookup Region Dimension'
    # Optional: ALTER TABLE ... CHANGE COLUMN tk GENERATED BY DEFAULT AS IDENTITY — then omit tk from INSERT values
    # Optional: spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")  # additive columns only
    # Post-load tip: partition pruning on effective_start_date/effective_end_date; OPTIMIZE ... ZORDER BY (region_id)
    _dim_df_Lookup_Region_Dimension = spark.table('main.retail_dwh.dim_region').select("region_sk", "region_id", "effective_start_date", "effective_end_date", "version_number")
    # Cache: broadcast join approximates Pentaho preload/cache
    _dim_df_Lookup_Region_Dimension = broadcast(_dim_df_Lookup_Region_Dimension)
    # Effective dating: last_stocktake_date between effective_start_date and effective_end_date
    _dim_active_df_Lookup_Region_Dimension = _dim_df_Lookup_Region_Dimension
    # Late-arriving / expired / overlap: filter to version covering stream date
    _dim_joined = df_Lookup_Store_Dimension.join(_dim_active_df_Lookup_Region_Dimension, on=((df_Lookup_Store_Dimension["region_id"] == _dim_active_df_Lookup_Region_Dimension["region_id"]) & (df_Lookup_Store_Dimension["last_stocktake_date"] >= _dim_active_df_Lookup_Region_Dimension["effective_start_date"]) & (df_Lookup_Store_Dimension["last_stocktake_date"] < _dim_active_df_Lookup_Region_Dimension["effective_end_date"])), how='left')
    df_Lookup_Region_Dimension = _dim_joined
    # Lookup-only: null 'region_sk' indicates cache miss / unknown BK

    # Step: Lookup Date Dimension (DimensionLookup) [converted]
    # Dimension Lookup/Update: Lookup Date Dimension
    # preserved.connection='conn_dev_dwh'
    # WARNING: DimensionLookup 'Lookup Date Dimension': connection 'conn_dev_dwh' mapped to Spark/UC table 'main.retail_dwh.dim_date' (not JDBC).
    # preserved.commit_size=100
    # preserved.cache_size=5000
    # preserved.preload_cache=True
    # preserved.use_start_date_alternative=False
    # preserved.start_date_alternative='none'
    # preserved.use_batch=True
    # preserved.min_year=1900
    # preserved.max_year=2199
    # SCD mode: lookup-only; Type1=0 Type2=0 PunchThrough=0 technical=0
    # Surrogate key strategy: tablemax (MAX(tk)+row_number) for 'Lookup Date Dimension'
    # Optional: ALTER TABLE ... CHANGE COLUMN tk GENERATED BY DEFAULT AS IDENTITY — then omit tk from INSERT values
    # Optional: spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")  # additive columns only
    # Post-load tip: partition pruning on effective_start_date/effective_end_date; OPTIMIZE ... ZORDER BY (last_stocktake_date)
    _dim_df_Lookup_Date_Dimension = spark.table('main.retail_dwh.dim_date').select("date_sk", "last_stocktake_date", "effective_start_date", "effective_end_date", "version_number")
    # Cache: broadcast join approximates Pentaho preload/cache
    _dim_df_Lookup_Date_Dimension = broadcast(_dim_df_Lookup_Date_Dimension)
    # Effective dating: last_stocktake_date between effective_start_date and effective_end_date
    _dim_active_df_Lookup_Date_Dimension = _dim_df_Lookup_Date_Dimension
    # Late-arriving / expired / overlap: filter to version covering stream date
    _dim_joined = df_Lookup_Region_Dimension.join(_dim_active_df_Lookup_Date_Dimension, on=((df_Lookup_Region_Dimension["last_stocktake_date"] == _dim_active_df_Lookup_Date_Dimension["last_stocktake_date"]) & (df_Lookup_Region_Dimension["last_stocktake_date"] >= _dim_active_df_Lookup_Date_Dimension["effective_start_date"]) & (df_Lookup_Region_Dimension["last_stocktake_date"] < _dim_active_df_Lookup_Date_Dimension["effective_end_date"])), how='left')
    df_Lookup_Date_Dimension = _dim_joined
    # Lookup-only: null 'date_sk' indicates cache miss / unknown BK

    # Step: Resolve Unknown Members (IfNull) [converted]
    # If Field Value Is Null: Resolve Unknown Members
    df_Resolve_Unknown_Members = df_Lookup_Date_Dimension
    df_Resolve_Unknown_Members = df_Resolve_Unknown_Members.withColumn('product_sk', when(col('product_sk').isNull(), lit(-1)).otherwise(col('product_sk')))
    df_Resolve_Unknown_Members = df_Resolve_Unknown_Members.withColumn('supplier_sk', when(col('supplier_sk').isNull(), lit(-1)).otherwise(col('supplier_sk')))
    df_Resolve_Unknown_Members = df_Resolve_Unknown_Members.withColumn('store_sk', when(col('store_sk').isNull(), lit(-1)).otherwise(col('store_sk')))
    df_Resolve_Unknown_Members = df_Resolve_Unknown_Members.withColumn('region_sk', when(col('region_sk').isNull(), lit(-1)).otherwise(col('region_sk')))
    df_Resolve_Unknown_Members = df_Resolve_Unknown_Members.withColumn('date_sk', when(col('date_sk').isNull(), lit(-1)).otherwise(col('date_sk')))

    # Step: Force Unknown SK Sentinel (SetValueField) [converted]
    # Set Field Value: Force Unknown SK Sentinel
    df_Force_Unknown_SK_Sentinel = df_Resolve_Unknown_Members
    df_Force_Unknown_SK_Sentinel = df_Force_Unknown_SK_Sentinel.withColumn("unknown_member_sk", col("-1"))

    # Step: Combination Lookup Inventory (CombinationLookup) [converted]
    # Combination Lookup/Update: Combination Lookup Inventory
    # preserved.connection='conn_dev_dwh'
    # WARNING: CombinationLookup 'Combination Lookup Inventory': connection 'conn_dev_dwh' mapped to Spark/UC table 'main.retail_dwh.dim_inventory_combo' (not JDBC).
    # preserved.commit_size=100
    # preserved.cache_size=9999
    # preserved.preload_cache=True
    # preserved.use_hash=True
    # preserved.hash_field='inventory_bk_crc'
    # WARNING: CombinationLookup 'Combination Lookup Inventory': CRC/hash cache ('inventory_bk_crc') is database-specific — business-key equi-join used instead; metadata preserved.
    # Surrogate key strategy: tablemax (MAX(tk)+row_number) for 'Combination Lookup Inventory'
    # Optional: ALTER TABLE ... CHANGE COLUMN tk GENERATED BY DEFAULT AS IDENTITY — then omit tk from INSERT values
    # Optional: spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")  # additive columns only
    # Edge cases: null business keys skipped from insert; duplicate combinations deduplicated before MERGE
    _combo_dim_df_Combination_Lookup_Inventory = spark.table('main.retail_dwh.dim_inventory_combo').select("technical_key", "inventory_id", "product_id", "store_id")
    # Cache: broadcast join approximates Pentaho preload/cache
    _combo_dim_df_Combination_Lookup_Inventory = broadcast(_combo_dim_df_Combination_Lookup_Inventory)
    _combo_dim_df_Combination_Lookup_Inventory = _combo_dim_df_Combination_Lookup_Inventory.select(col("inventory_id"), col("product_id"), col("store_id"), col("technical_key"))
    _combo_joined = df_Force_Unknown_SK_Sentinel.join(_combo_dim_df_Combination_Lookup_Inventory, on=["inventory_id", "product_id", "store_id"], how='left')
    _combo_miss_df_Combination_Lookup_Inventory = _combo_joined.filter(col("technical_key").isNull() & ~(col("inventory_id").isNull() | col("product_id").isNull() | col("store_id").isNull()))
    _combo_miss_df_Combination_Lookup_Inventory = _combo_miss_df_Combination_Lookup_Inventory.dropDuplicates(["inventory_id", "product_id", "store_id"])
    _combo_ins_df_Combination_Lookup_Inventory = _combo_miss_df_Combination_Lookup_Inventory.select(col("inventory_id").alias("inventory_id"), col("product_id").alias("product_id"), col("store_id").alias("store_id"))
    # tablemax + row_number (IDENTITY would omit tk from INSERT below)
    _max_tk = spark.sql("SELECT COALESCE(MAX(`technical_key`), 0) AS m FROM main.retail_dwh.dim_inventory_combo").collect()[0][0]
    from pyspark.sql.window import Window as _DWWindow
    _combo_ins_df_Combination_Lookup_Inventory = _combo_ins_df_Combination_Lookup_Inventory.withColumn("_dw_rn", row_number().over(_DWWindow.orderBy(lit(1))))
    _combo_ins_df_Combination_Lookup_Inventory = _combo_ins_df_Combination_Lookup_Inventory.withColumn("technical_key", (lit(_max_tk) + col("_dw_rn")).cast("long")).drop("_dw_rn")
    _combo_ins_df_Combination_Lookup_Inventory.createOrReplaceTempView('_combo_insert_src_df_Combination_Lookup_Inventory')
    from delta.tables import DeltaTable
    (
        DeltaTable.forName(spark, 'main.retail_dwh.dim_inventory_combo').alias("t")
        .merge(spark.table('_combo_insert_src_df_Combination_Lookup_Inventory').alias("s"), 't.`inventory_id` <=> s.`inventory_id` AND t.`product_id` <=> s.`product_id` AND t.`store_id` <=> s.`store_id`')
        .whenNotMatchedInsert(values={'inventory_id': 's.`inventory_id`', 'product_id': 's.`product_id`', 'store_id': 's.`store_id`', 'technical_key': 's.`technical_key`'})
        .execute()
    )
    # Delta transaction: each DeltaTable.merge().execute() is one atomic transaction
    # Attach TK without re-scanning dimension: union prior keys with inserts (map table fields back to stream names)
    _combo_new_keys = _combo_ins_df_Combination_Lookup_Inventory.select(col("inventory_id"), col("product_id"), col("store_id"), col("technical_key"))
    _combo_dim_df_Combination_Lookup_Inventory = _combo_dim_df_Combination_Lookup_Inventory.unionByName(_combo_new_keys)
    _combo_dim_df_Combination_Lookup_Inventory = broadcast(_combo_dim_df_Combination_Lookup_Inventory)
    df_Combination_Lookup_Inventory = df_Force_Unknown_SK_Sentinel.join(_combo_dim_df_Combination_Lookup_Inventory, on=["inventory_id", "product_id", "store_id"], how='left')
    # Null surrogate keys after MERGE indicate unresolved/null business keys

    # Step: Generate Inventory Surrogate (Sequence) [converted]
    # Add Sequence: Generate Inventory Surrogate
    # preserved.use_counter=True counter_name='customer_sk_counter'
    _w_seq_df_Generate_Inventory_Surrogate = Window.orderBy(monotonically_increasing_id())
    # preserved.max_value=999999999 — wrap to start (Pentaho counter)
    df_Generate_Inventory_Surrogate = df_Combination_Lookup_Inventory.withColumn("inventory_sk", lit(1) + ((row_number().over(_w_seq_df_Generate_Inventory_Surrogate) - lit(1)) % greatest(((lit(999999999) - lit(1)) // lit(1)) + lit(1), lit(1))) * lit(1))
    # WARNING: Spark row_number over monotonically_increasing_id is order-based; sort upstream if deterministic sequencing across partitions is required

    # Step: Optional Existing Fact Check (DBJoin) [partial]
    # Database Join: Optional Existing Fact Check
    # preserved.connection='conn_dev_dwh'
    # preserved.sql='SELECT inventory_sk AS existing_inventory_sk FROM retail_dwh.fact_inventory WHERE inventory_id = ?'
    # preserved.outer_join=True
    # preserved.row_limit=0
    # preserved.replace_vars=True
    # preserved.parameters=[{'name': 'inventory_id', 'type': 'String'}, {'name': '\n        ', 'type': ''}]
    _sql_df_Optional_Existing_Fact_Check = 'SELECT inventory_sk AS existing_inventory_sk FROM retail_dwh.fact_inventory WHERE inventory_id = ?'
    # WARNING: per-row parameterized joins cannot use spark.sql with '?' placeholders; emitting JDBC prepared-statement skeleton (foreachPartition).
    # preserved.sql_template='SELECT inventory_sk AS existing_inventory_sk FROM retail_dwh.fact_inventory WHERE inventory_id = :inventory_id'
    _param_fields_df_Optional_Existing_Fact_Check = ['inventory_id', '\n        ']
    import os
    # foreachPartition JDBC outline (wire PENTAHO_JDBC_URL / driver at runtime):
    # def _dbjoin_partition(rows):
    #     conn = <jdbc connect from os.environ['PENTAHO_JDBC_URL']>
    #     cur = conn.prepareStatement('SELECT inventory_sk AS existing_inventory_sk FROM retail_dwh.fact_inventory WHERE inventory_id = ?')
    #     for row in rows:
    #         for i, f in enumerate(_param_fields_df_Optional_Existing_Fact_Check, 1):
    #             cur.setObject(i, row[f])
    #         rs = cur.executeQuery(); ... yield joined rows
    # Fallback: preserve input stream; attach empty lookup side for schema continuity
    df_Optional_Existing_Fact_Check = df_Generate_Inventory_Surrogate
    # Join type preserved as 'left'; join keys=['inventory_id', '\n        ']

    # Step: Unknown Member Flags (Formula) [converted]
    # Formula: Unknown Member Flags
    df_Unknown_Member_Flags = df_Optional_Existing_Fact_Check
    df_Unknown_Member_Flags = df_Unknown_Member_Flags.withColumn('formula_result', lit(None))  # empty formula

    # Step: Write Lookuped Inventory (TextFileOutput) [converted]
    # Pentaho step: Write Lookuped Inventory (type: TextFileOutput)
    # Pentaho filename: /output/inventory/lookup/inventory_lookuped_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='inventory_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='region_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='last_stocktake_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='warehouse_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='quantity_clean' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='available_qty_clean' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='stock_value' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='reorder_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='abc_classification' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='xyz_classification' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='safety_stock' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='economic_order_quantity' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='dead_stock_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='excess_stock_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='shortage_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='inventory_bk_checksum' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='store_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='region_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='date_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='inventory_combo_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='inventory_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unknown_product_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unknown_store_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unknown_supplier_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unknown_member_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Lookuped_Inventory = df_Unknown_Member_Flags
    _out_df_Write_Lookuped_Inventory = df_Write_Lookuped_Inventory.select('inventory_id', 'product_id', 'store_id', 'supplier_id', 'region_id', 'last_stocktake_date', 'warehouse_code', 'quantity_clean', 'available_qty_clean', 'stock_value', 'reorder_flag', 'abc_classification', 'xyz_classification', 'safety_stock', 'economic_order_quantity', 'dead_stock_flag', 'excess_stock_flag', 'shortage_flag', 'inventory_bk_checksum', 'batch_id', 'run_id', 'product_sk', 'supplier_sk', 'store_sk', 'region_sk', 'date_sk', 'inventory_combo_sk', 'inventory_sk', 'unknown_product_flag', 'unknown_store_flag', 'unknown_supplier_flag', 'unknown_member_sk')
    writer = _out_df_Write_Lookuped_Inventory.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/inventory_lookuped_.csv')

    # Step: Copy Lookup Rows To Result (RowsToResult) [converted]
    # Copy Rows to Result: Copy Lookup Rows To Result
    # preserved.result_buffer='rows'
    # preserved.preserve_order=True
    # LIMITATION: Pentaho Result rows are job-level; Databricks uses a notebook-scoped buffer (_pentaho_result_rows) for downstream hops / orchestration. Cross-job Result transfer needs Databricks Jobs task values or persisted Delta tables.
    _pentaho_result_rows = globals().setdefault('_pentaho_result_rows', {})
    _pentaho_result_files = globals().setdefault('_pentaho_result_files', [])
    # Preserve schema and relative ordering for 'Copy Lookup Rows To Result'
    _result_rows_df_Copy_Lookup_Rows_To_Result = df_Write_Lookuped_Inventory
    _pentaho_result_rows['Copy Lookup Rows To Result'] = _result_rows_df_Copy_Lookup_Rows_To_Result
    _pentaho_result_rows['__latest__'] = _result_rows_df_Copy_Lookup_Rows_To_Result
    df_Copy_Lookup_Rows_To_Result = df_Write_Lookuped_Inventory

    # Step: Log Lookups Complete (WriteToLog) [converted]
    # Write to Log: Log Lookups Complete
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=LOOKUPS_OK | TRANS=TR_Inventory_Lookups | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Lookups_Complete = logging.getLogger('pentaho.writetolog.Log_Lookups_Complete')
    _log_df_Log_Lookups_Complete.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Lookups_Complete = df_Copy_Lookup_Rows_To_Result
    _log_rows_df_Log_Lookups_Complete = _log_df_df_Log_Lookups_Complete.take(20)
    _log_df_Log_Lookups_Complete.info('Log Lookups Complete' + ' | columns=' + str(_log_df_df_Log_Lookups_Complete.columns))
    _log_df_Log_Lookups_Complete.info('AUDIT | EVENT=LOOKUPS_OK | TRANS=TR_Inventory_Lookups | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Lookups_Complete:
        _log_df_Log_Lookups_Complete.info('Log Lookups Complete' + ' | ' + str(_lr.asDict()))
    df_Log_Lookups_Complete = df_Copy_Lookup_Rows_To_Result

    # Step: Lookups Complete (Dummy) [converted]
    # Dummy: Lookups Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Lookups_Complete = df_Dummy_Result_Seed_Consumed

    log_event(_LOG, "transformation_end")
    return df_Dummy_Lookups_Complete
