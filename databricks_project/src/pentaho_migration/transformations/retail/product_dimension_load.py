"""PySpark module migrated from Pentaho transformation: TR_Product_Dimension_Load.

Source: C:/Users/Prateek.Kotian/Desktop/Pentaho/Retail & E-commerce/Retail_ETL_Project/transformations/dimension/Product_Dimension_Load.ktr
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
    md5,
    concat,
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

_LOG = get_logger("pentaho_migration.transformations.retail.product_dimension_load")

from pyspark.sql.window import Window


def run(spark: SparkSession, config: Mapping[str, Any] | None = None) -> DataFrame:
    """Execute Pentaho transformation ``TR_Product_Dimension_Load`` step-for-step."""
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

    # Step: Read Enriched Products (CsvInput) [converted]
    # CSV Input: Read Enriched Products
    df_Read_Enriched_Products = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('product_id STRING, sku STRING, product_name STRING, category_id STRING, category_name STRING, parent_category_id STRING, supplier_id STRING, supplier_name STRING, supplier_country STRING, supplier_lead_time_days STRING, brand STRING, unit_cost DOUBLE, unit_price DOUBLE, margin_amount DOUBLE, profit_pct DOUBLE, discount_pct DOUBLE, selling_price DOUBLE, tax_amount DOUBLE, volume_cm3 DOUBLE, product_age_years INT, lifecycle_stage STRING, lifecycle_label STRING, abc_class STRING, abc_priority STRING, price_band STRING, currency_code STRING, weight_kg DOUBLE, length_cm DOUBLE, width_cm DOUBLE, height_cm DOUBLE, upc STRING, barcode STRING, is_active STRING, status STRING, created_date STRING, description STRING, product_bk_checksum STRING, batch_id STRING, run_id STRING')
        .load(f'{data_dir}/products_enriched_.csv')
    )

    # Step: Read Existing DimProduct (CsvInput) [converted]
    # CSV Input: Read Existing DimProduct
    df_Read_Existing_DimProduct = (
        spark.read.format('csv')
        .option('header', True)
        .option('sep', ',')
        .option('quote', '"')
        .option('inferSchema', False)
        .schema('product_sk INT, product_id STRING, sku STRING, product_name STRING, category_id STRING, supplier_id STRING, unit_price DOUBLE, is_active STRING, is_current STRING, version_number INT, scd_hash STRING, effective_start_date STRING, effective_end_date STRING, current_flag STRING')
        .load(f'{data_dir}/dim_product_current.csv')
    )

    # Step: Write Dimension Rejects (TextFileOutput) [converted]
    # Pentaho step: Write Dimension Rejects (type: TextFileOutput)
    # Pentaho filename: /rejects/rejected_rows/product/products_dim_rejects_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sku' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_CODE' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='ERR_DESC' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Dimension_Rejects = df_Write_Dimension_Rejects
    _out_df_Write_Dimension_Rejects = df_Write_Dimension_Rejects.select('product_id', 'sku', 'ERR_CODE', 'ERR_DESC', 'batch_id', 'run_id')
    writer = _out_df_Write_Dimension_Rejects.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/products_dim_rejects_.csv')

    # Step: Build SCD Hash Payload (ConcatFields) [converted]
    # Concat Fields: Build SCD Hash Payload
    df_Build_SCD_Hash_Payload = df_Read_Enriched_Products
    df_Build_SCD_Hash_Payload = df_Build_SCD_Hash_Payload.withColumn("scd_hash_payload", concat(concat(lit('"'), coalesce(col("product_name").cast("string"), lit("")), lit('"')), lit('|'), concat(lit('"'), coalesce(col("category_id").cast("string"), lit("")), lit('"')), lit('|'), concat(lit('"'), coalesce(col("supplier_id").cast("string"), lit("")), lit('"')), lit('|'), concat(lit('"'), coalesce(col("brand").cast("string"), lit("")), lit('"')), lit('|'), concat(lit('"'), coalesce(col("unit_cost").cast("string"), lit("")), lit('"')), lit('|'), concat(lit('"'), coalesce(col("unit_price").cast("string"), lit("")), lit('"')), lit('|'), concat(lit('"'), coalesce(col("selling_price").cast("string"), lit("")), lit('"')), lit('|'), concat(lit('"'), coalesce(col("is_active").cast("string"), lit("")), lit('"')), lit('|'), concat(lit('"'), coalesce(col("abc_class").cast("string"), lit("")), lit('"')), lit('|'), concat(lit('"'), coalesce(col("lifecycle_stage").cast("string"), lit("")), lit('"')), lit('|'), concat(lit('"'), coalesce(col("price_band").cast("string"), lit("")), lit('"'))))
    # preserved.encoding='UTF-8'

    # Step: Prepare Existing Dimension Keys (SelectValues) [converted]
    # Select Values: Prepare Existing Dimension Keys
    df_Prepare_Existing_Dimension_Keys = df_Read_Existing_DimProduct.select(col("product_sk").alias("existing_product_sk"), col("product_id").alias("existing_product_id"), col("scd_hash").alias("existing_scd_hash"), col("is_current").alias("existing_is_current"), col("version_number").alias("existing_version_number"), col("current_flag").alias("existing_current_flag"))

    # Step: MD5 SCD Hash (CheckSum) [converted]
    # Add a Checksum: MD5 SCD Hash
    df_MD5_SCD_Hash = df_Build_SCD_Hash_Payload
    df_MD5_SCD_Hash = df_MD5_SCD_Hash.withColumn("scd_hash", md5(coalesce(col("scd_hash_payload").cast("string"), lit(""))))
    # preserved.checksumtype='MD5' resultType='hexadecimal' fields=['scd_hash_payload']

    # Step: Detect Existing Products (StreamLookup) [failed]
    # Stream Lookup: Detect Existing Products
    # StreamLookup 'Detect Existing Products': no join keys — lookup join not generated
    df_Detect_Existing_Products = df_MD5_SCD_Hash

    # Step: Classify SCD Action (Formula) [converted]
    # Formula: Classify SCD Action
    df_Classify_SCD_Action = df_Detect_Existing_Products
    df_Classify_SCD_Action = df_Classify_SCD_Action.withColumn('formula_result', lit(None))  # empty formula

    # Step: Combination Lookup Product BK (CombinationLookup) [converted]
    # Combination Lookup/Update: Combination Lookup Product BK
    # preserved.connection='conn_dev_dwh'
    # WARNING: CombinationLookup 'Combination Lookup Product BK': connection 'conn_dev_dwh' mapped to Spark/UC table 'main.retail_dwh.dim_product_key_bridge' (not JDBC).
    # preserved.commit_size=100
    # preserved.cache_size=9999
    # preserved.preload_cache=True
    # preserved.use_hash=True
    # preserved.hash_field='product_bk_crc'
    # preserved.last_update_field='last_seen_ts'
    # WARNING: CombinationLookup 'Combination Lookup Product BK': CRC/hash cache ('product_bk_crc') is database-specific — business-key equi-join used instead; metadata preserved.
    # Surrogate key strategy: tablemax (MAX(tk)+row_number) for 'Combination Lookup Product BK'
    # Optional: ALTER TABLE ... CHANGE COLUMN tk GENERATED BY DEFAULT AS IDENTITY — then omit tk from INSERT values
    # Optional: spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")  # additive columns only
    # Edge cases: null business keys skipped from insert; duplicate combinations deduplicated before MERGE
    _combo_dim_df_Combination_Lookup_Product_BK = spark.table('main.retail_dwh.dim_product_key_bridge').select("technical_key", "product_id", "sku")
    # Cache: broadcast join approximates Pentaho preload/cache
    _combo_dim_df_Combination_Lookup_Product_BK = broadcast(_combo_dim_df_Combination_Lookup_Product_BK)
    _combo_dim_df_Combination_Lookup_Product_BK = _combo_dim_df_Combination_Lookup_Product_BK.select(col("product_id"), col("sku"), col("technical_key"))
    _combo_joined = df_Classify_SCD_Action.join(_combo_dim_df_Combination_Lookup_Product_BK, on=["product_id", "sku"], how='left')
    _combo_miss_df_Combination_Lookup_Product_BK = _combo_joined.filter(col("technical_key").isNull() & ~(col("product_id").isNull() | col("sku").isNull()))
    _combo_miss_df_Combination_Lookup_Product_BK = _combo_miss_df_Combination_Lookup_Product_BK.dropDuplicates(["product_id", "sku"])
    if 'last_seen_ts' in _combo_miss_df_Combination_Lookup_Product_BK.columns:
        _combo_ins_df_Combination_Lookup_Product_BK = _combo_miss_df_Combination_Lookup_Product_BK.select(col("product_id").alias("product_id"), col("sku").alias("sku"), col('last_seen_ts').alias('last_seen_ts'))
    else:
        _combo_ins_df_Combination_Lookup_Product_BK = _combo_miss_df_Combination_Lookup_Product_BK.select(col("product_id").alias("product_id"), col("sku").alias("sku"), current_timestamp().alias('last_seen_ts'))
    # tablemax + row_number (IDENTITY would omit tk from INSERT below)
    _max_tk = spark.sql("SELECT COALESCE(MAX(`technical_key`), 0) AS m FROM main.retail_dwh.dim_product_key_bridge").collect()[0][0]
    from pyspark.sql.window import Window as _DWWindow
    _combo_ins_df_Combination_Lookup_Product_BK = _combo_ins_df_Combination_Lookup_Product_BK.withColumn("_dw_rn", row_number().over(_DWWindow.orderBy(lit(1))))
    _combo_ins_df_Combination_Lookup_Product_BK = _combo_ins_df_Combination_Lookup_Product_BK.withColumn("technical_key", (lit(_max_tk) + col("_dw_rn")).cast("long")).drop("_dw_rn")
    _combo_ins_df_Combination_Lookup_Product_BK.createOrReplaceTempView('_combo_insert_src_df_Combination_Lookup_Product_BK')
    from delta.tables import DeltaTable
    (
        DeltaTable.forName(spark, 'main.retail_dwh.dim_product_key_bridge').alias("t")
        .merge(spark.table('_combo_insert_src_df_Combination_Lookup_Product_BK').alias("s"), 't.`product_id` <=> s.`product_id` AND t.`sku` <=> s.`sku`')
        .whenNotMatchedInsert(values={'product_id': 's.`product_id`', 'sku': 's.`sku`', 'last_seen_ts': 's.`last_seen_ts`', 'technical_key': 's.`technical_key`'})
        .execute()
    )
    # Delta transaction: each DeltaTable.merge().execute() is one atomic transaction
    # Attach TK without re-scanning dimension: union prior keys with inserts (map table fields back to stream names)
    _combo_new_keys = _combo_ins_df_Combination_Lookup_Product_BK.select(col("product_id"), col("sku"), col("technical_key"))
    _combo_dim_df_Combination_Lookup_Product_BK = _combo_dim_df_Combination_Lookup_Product_BK.unionByName(_combo_new_keys)
    _combo_dim_df_Combination_Lookup_Product_BK = broadcast(_combo_dim_df_Combination_Lookup_Product_BK)
    df_Combination_Lookup_Product_BK = df_Classify_SCD_Action.join(_combo_dim_df_Combination_Lookup_Product_BK, on=["product_id", "sku"], how='left')
    # Null surrogate keys after MERGE indicate unresolved/null business keys

    # Step: Route SCD Action (SwitchCase) [converted]
    # Switch / Case: Route SCD Action
    # preserved.fieldname='scd_action'
    # preserved.switch_field='scd_action'
    # preserved.cases=[{'value': 'INSERT', 'target_step': 'Prepare Insert New'}, {'value': 'UPDATE', 'target_step': 'Prepare Update History'}, {'value': 'UNCHANGED', 'target_step': 'Skip Unchanged'}]
    # preserved.default_target_step='Skip Unchanged'
    # preserved.use_contains=False
    # preserved.case_value_type='String'
    # preserved.rules=[{'value': 'INSERT', 'target_step': 'Prepare Insert New'}, {'value': 'UPDATE', 'target_step': 'Prepare Update History'}, {'value': 'UNCHANGED', 'target_step': 'Skip Unchanged'}]
    _routed_df_Route_SCD_Action = df_Combination_Lookup_Product_BK.withColumn('_route_Route_SCD_Action', when(col("scd_action") == lit('INSERT'), lit('Prepare Insert New')).when(col("scd_action") == lit('UPDATE'), lit('Prepare Update History')).when(col("scd_action") == lit('UNCHANGED'), lit('Skip Unchanged')).otherwise(lit('Skip Unchanged')))
    df_Prepare_Insert_New = _routed_df_Route_SCD_Action.filter(col('_route_Route_SCD_Action') == lit('Prepare Insert New')).drop('_route_Route_SCD_Action')
    df_Prepare_Update_History = _routed_df_Route_SCD_Action.filter(col('_route_Route_SCD_Action') == lit('Prepare Update History')).drop('_route_Route_SCD_Action')
    df_Skip_Unchanged = _routed_df_Route_SCD_Action.filter(col('_route_Route_SCD_Action') == lit('Skip Unchanged')).drop('_route_Route_SCD_Action')
    df_Route_SCD_Action = df_Prepare_Insert_New

    # Step: Prepare Insert New (Constant) [converted]
    # Add Constants: Prepare Insert New
    df_Prepare_Insert_New = df_Route_SCD_Action
    df_Prepare_Insert_New = df_Prepare_Insert_New.withColumn("is_current", lit('Y'))
    # preserved.is_current: length='-1', precision='-1'
    df_Prepare_Insert_New = df_Prepare_Insert_New.withColumn("current_flag", lit('Y'))
    # preserved.current_flag: length='-1', precision='-1'
    df_Prepare_Insert_New = df_Prepare_Insert_New.withColumn("effective_end_date", lit('9999-12-31'))
    # preserved.effective_end_date: length='-1', precision='-1'
    df_Prepare_Insert_New = df_Prepare_Insert_New.withColumn("load_status", lit('INSERTED'))
    # preserved.load_status: length='-1', precision='-1'

    # Step: Prepare Update History (Constant) [converted]
    # Add Constants: Prepare Update History
    df_Prepare_Update_History = df_Route_SCD_Action
    df_Prepare_Update_History = df_Prepare_Update_History.withColumn("is_current_old", lit('N'))
    # preserved.is_current_old: length='-1', precision='-1'
    df_Prepare_Update_History = df_Prepare_Update_History.withColumn("current_flag_old", lit('N'))
    # preserved.current_flag_old: length='-1', precision='-1'
    df_Prepare_Update_History = df_Prepare_Update_History.withColumn("history_flag", lit('Y'))
    # preserved.history_flag: length='-1', precision='-1'
    df_Prepare_Update_History = df_Prepare_Update_History.withColumn("load_status", lit('UPDATED'))
    # preserved.load_status: length='-1', precision='-1'

    # Step: Skip Unchanged (Constant) [converted]
    # Add Constants: Skip Unchanged
    df_Skip_Unchanged = df_Route_SCD_Action
    df_Skip_Unchanged = df_Skip_Unchanged.withColumn("load_status", lit('SKIPPED'))
    # preserved.load_status: length='-1', precision='-1'
    df_Skip_Unchanged = df_Skip_Unchanged.withColumn("is_current", lit('Y'))
    # preserved.is_current: length='-1', precision='-1'
    df_Skip_Unchanged = df_Skip_Unchanged.withColumn("current_flag", lit('Y'))
    # preserved.current_flag: length='-1', precision='-1'

    # Step: Generate Surrogate Key (Sequence) [converted]
    # Add Sequence: Generate Surrogate Key
    # preserved.use_counter=True counter_name='product_sk_counter'
    _w_seq_df_Generate_Surrogate_Key = Window.orderBy(monotonically_increasing_id())
    # preserved.max_value=999999999 — wrap to start (Pentaho counter)
    df_Generate_Surrogate_Key = df_Prepare_Insert_New.withColumn("product_sk", lit(1) + ((row_number().over(_w_seq_df_Generate_Surrogate_Key) - lit(1)) % greatest(((lit(999999999) - lit(1)) // lit(1)) + lit(1), lit(1))) * lit(1))
    # WARNING: Spark row_number over monotonically_increasing_id is order-based; sort upstream if deterministic sequencing across partitions is required

    # Step: Clone For History Maintain (CloneRow) [converted]
    # Clone Row: Clone For History Maintain
    # preserved.nr_clones=1
    # preserved.nr_clone_in_field=False
    # preserved.add_clone_flag=False
    # preserved.clone_flag_field='cloneflag'
    # preserved.add_clone_num=False
    # preserved.clone_num_field='clonenum'
    # preserved.nr_clones_raw='1'
    _clone_parts_df_Clone_For_History_Maintain = []
    _base_df_Clone_For_History_Maintain = df_Prepare_Update_History
    _orig_df_Clone_For_History_Maintain = _base_df_Clone_For_History_Maintain
    _clone_parts_df_Clone_For_History_Maintain.append(_orig_df_Clone_For_History_Maintain)
    for _ci in range(1, 1 + 1):
        _c = _base_df_Clone_For_History_Maintain
        _clone_parts_df_Clone_For_History_Maintain.append(_c)
    df_Clone_For_History_Maintain = _clone_parts_df_Clone_For_History_Maintain[0]
    for _part in _clone_parts_df_Clone_For_History_Maintain[1:]:
        df_Clone_For_History_Maintain = df_Clone_For_History_Maintain.unionByName(_part, allowMissingColumns=True)

    # Step: Insert Path Ready (Dummy) [converted]
    # Dummy: Insert Path Ready
    # Pass-through step - DataFrame unchanged
    df_Dummy_Insert_Path_Ready = df_Generate_Surrogate_Key

    # Step: Sort History Path (SortRows) [converted]
    # Sort Rows: Sort History Path
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_History_Path = df_Clone_For_History_Maintain
    _sort_df_Sort_History_Path = _sort_df_Sort_History_Path.withColumn("_sort_ci_product_id", lower(col("product_id").cast("string")))
    df_Sort_History_Path = _sort_df_Sort_History_Path.orderBy(col("_sort_ci_product_id").asc_nulls_last())
    df_Sort_History_Path = df_Sort_History_Path.drop("_sort_ci_product_id")

    # Step: Sort Insert Path (SortRows) [converted]
    # Sort Rows: Sort Insert Path
    # preserved.directory='%%java.io.tmpdir%%' prefix='out' sort_size='1000000' free_memory='' compress=False compress_variable=''
    _sort_df_Sort_Insert_Path = df_Dummy_Insert_Path_Ready
    _sort_df_Sort_Insert_Path = _sort_df_Sort_Insert_Path.withColumn("_sort_ci_product_id", lower(col("product_id").cast("string")))
    df_Sort_Insert_Path = _sort_df_Sort_Insert_Path.orderBy(col("_sort_ci_product_id").asc_nulls_last())
    df_Sort_Insert_Path = df_Sort_Insert_Path.drop("_sort_ci_product_id")

    # Step: Stamp Effective Dates (SystemInfo) [converted]
    # System Info: Stamp Effective Dates
    df_Stamp_Effective_Dates = df_Dummy_Insert_Path_Ready
    df_Stamp_Effective_Dates = df_Stamp_Effective_Dates.withColumn("effective_start_date", current_date())
    df_Stamp_Effective_Dates = df_Stamp_Effective_Dates.withColumn("dw_insert_ts", current_date())
    df_Stamp_Effective_Dates = df_Stamp_Effective_Dates.withColumn("dw_update_ts", current_date())

    # Step: Merge Rows SCD Delta (MergeRows) [converted]
    # Merge Rows (Diff): Merge Rows SCD Delta
    # preserved.flag_field='merge_flag'
    # preserved.reference='Sort Insert Path'
    # preserved.compare='Sort History Path'
    # preserved.key_fields=['product_id']
    # preserved.value_fields=['scd_hash', 'unit_price', 'is_active']
    _ref_df_Merge_Rows_SCD_Delta = df_Sort_Insert_Path.alias("r")
    _cmp_df_Merge_Rows_SCD_Delta = df_Sort_History_Path.alias("c")
    # WARNING: MergeRows 'Merge Rows SCD Delta': null join keys do not match under Spark equality; duplicate keys expand to a product within the key group
    df_Merge_Rows_SCD_Delta = _ref_df_Merge_Rows_SCD_Delta.join(_cmp_df_Merge_Rows_SCD_Delta, (col("r.product_id") == col("c.product_id")), 'full_outer')
    df_Merge_Rows_SCD_Delta = df_Merge_Rows_SCD_Delta.withColumn('merge_flag', when(col("c.product_id").isNull(), lit("deleted")).when(col("r.product_id").isNull(), lit("new")).when((~col("r.scd_hash").eqNullSafe(col("c.scd_hash"))) | (~col("r.unit_price").eqNullSafe(col("c.unit_price"))) | (~col("r.is_active").eqNullSafe(col("c.is_active"))), lit("changed")).otherwise(lit("identical")))
    # NOTE: MergeRows 'Merge Rows SCD Delta': output prefers compare values (CDC-style); deleted rows keep reference values
    df_Merge_Rows_SCD_Delta = df_Merge_Rows_SCD_Delta.select(coalesce(col("c.product_id"), col("r.product_id")).alias('product_id'), coalesce(col("c.scd_hash"), col("r.scd_hash")).alias('scd_hash'), coalesce(col("c.unit_price"), col("r.unit_price")).alias('unit_price'), coalesce(col("c.is_active"), col("r.is_active")).alias('is_active'), col('merge_flag'))
    # NOTE: MergeRows flags — deleted / new / changed / identical (requires pre-sorted inputs in PDI; Spark join does not enforce sort order)

    # Step: SCD2 Dimension Lookup Update (DimensionLookup) [converted]
    # Dimension Lookup/Update: SCD2 Dimension Lookup Update
    # preserved.connection='conn_dev_dwh'
    # WARNING: DimensionLookup 'SCD2 Dimension Lookup Update': connection 'conn_dev_dwh' mapped to Spark/UC table 'main.retail_dwh.dim_product' (not JDBC).
    # preserved.commit_size=100
    # preserved.cache_size=5000
    # preserved.preload_cache=True
    # preserved.use_start_date_alternative=False
    # preserved.start_date_alternative='none'
    # preserved.use_batch=True
    # preserved.min_year=1900
    # preserved.max_year=2199
    # SCD mode: update; Type1=0 Type2=22 PunchThrough=1 technical=0
    # Surrogate key strategy: tablemax (MAX(tk)+row_number) for 'SCD2 Dimension Lookup Update'
    # Optional: ALTER TABLE ... CHANGE COLUMN tk GENERATED BY DEFAULT AS IDENTITY — then omit tk from INSERT values
    # Optional: spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")  # additive columns only
    # Post-load tip: partition pruning on effective_start_date/effective_end_date; OPTIMIZE ... ZORDER BY (product_id)
    # Update mode: skip pre-MERGE lookup join; re-join after MERGEs below
    _scd_max_ts = lit("2199-12-31 23:59:59.999").cast("timestamp")
    _scd_min_ts = lit("1900-01-01 00:00:00").cast("timestamp")
    # preserved.scd_min_ts / scd_max_ts for open-ended version bounds
    _dim_src_df_SCD2_Dimension_Lookup_Update = df_Stamp_Effective_Dates.withColumn("_scd_effective", col("scd_change_date"))
    _dim_src_df_SCD2_Dimension_Lookup_Update = _dim_src_df_SCD2_Dimension_Lookup_Update.filter(~(col("product_id").isNull())).dropDuplicates(["product_id"])
    # Null business keys skipped; duplicate BK rows deduplicated before MERGE
    _dim_src_df_SCD2_Dimension_Lookup_Update.createOrReplaceTempView('_dim_scd_src_df_SCD2_Dimension_Lookup_Update')
    # Active version predicate approximated via date_to >= max_year boundary
    _dim_cmp_active = spark.table('main.retail_dwh.dim_product').select("product_sk", "product_id", "version_number", "sku", "product_name", "category_id", "category_name", "supplier_id", "supplier_name", "brand", "unit_cost", "unit_price", "selling_price", "currency_code", "weight_kg", "volume_cm3", "margin_amount", "profit_pct", "abc_class", "lifecycle_stage", "price_band", "is_active", "created_date", "scd_hash", "batch_id")
    _dim_cmp_active = _dim_cmp_active.filter(col('effective_end_date') >= _scd_max_ts)
    # Cache: broadcast join approximates Pentaho preload/cache
    _dim_cmp_active = broadcast(_dim_cmp_active)
    _dim_cmp_active = _dim_cmp_active.select(col("product_id"), col("product_sk").alias("_prior_tk"), col("version_number").alias("_prior_version"), col("sku").alias("_prior_sku"), col("product_name").alias("_prior_product_name"), col("category_id").alias("_prior_category_id"), col("category_name").alias("_prior_category_name"), col("supplier_id").alias("_prior_supplier_id"), col("supplier_name").alias("_prior_supplier_name"), col("brand").alias("_prior_brand"), col("unit_cost").alias("_prior_unit_cost"), col("unit_price").alias("_prior_unit_price"), col("selling_price").alias("_prior_selling_price"), col("currency_code").alias("_prior_currency_code"), col("weight_kg").alias("_prior_weight_kg"), col("volume_cm3").alias("_prior_volume_cm3"), col("margin_amount").alias("_prior_margin_amount"), col("profit_pct").alias("_prior_profit_pct"), col("abc_class").alias("_prior_abc_class"), col("lifecycle_stage").alias("_prior_lifecycle_stage"), col("price_band").alias("_prior_price_band"), col("is_active").alias("_prior_is_active"), col("created_date").alias("_prior_created_date"), col("scd_hash").alias("_prior_scd_hash"), col("batch_id").alias("_prior_batch_id"))
    _dim_cmp_df_SCD2_Dimension_Lookup_Update = _dim_src_df_SCD2_Dimension_Lookup_Update.join(_dim_cmp_active, on='product_id', how='left')
    # PunchThrough (1 fields): UPDATE all historical versions
    from delta.tables import DeltaTable
    (
        DeltaTable.forName(spark, 'main.retail_dwh.dim_product').alias("t")
        .merge(spark.table('_dim_scd_src_df_SCD2_Dimension_Lookup_Update').alias("s"), 't.`product_id` <=> s.`product_id`')
        .whenMatchedUpdate(set={'description': 's.`description`'})
        .execute()
    )
    # Delta transaction: each DeltaTable.merge().execute() is one atomic transaction
    # Version field preserved: version_number
    # Type 2 expire active version when attributes change
    (
        DeltaTable.forName(spark, 'main.retail_dwh.dim_product').alias("t")
        .merge(spark.table('_dim_scd_src_df_SCD2_Dimension_Lookup_Update').alias("s"), "t.`product_id` <=> s.`product_id` AND t.`effective_end_date` >= TIMESTAMP '2199-12-31 23:59:59.999'")
        .whenMatchedUpdate(condition='(NOT (t.`sku` <=> s.`sku`) OR NOT (t.`product_name` <=> s.`product_name`) OR NOT (t.`category_id` <=> s.`category_id`) OR NOT (t.`category_name` <=> s.`category_name`) OR NOT (t.`supplier_id` <=> s.`supplier_id`) OR NOT (t.`supplier_name` <=> s.`supplier_name`) OR NOT (t.`brand` <=> s.`brand`) OR NOT (t.`unit_cost` <=> s.`unit_cost`) OR NOT (t.`unit_price` <=> s.`unit_price`) OR NOT (t.`selling_price` <=> s.`selling_price`) OR NOT (t.`currency_code` <=> s.`currency_code`) OR NOT (t.`weight_kg` <=> s.`weight_kg`) OR NOT (t.`volume_cm3` <=> s.`volume_cm3`) OR NOT (t.`margin_amount` <=> s.`margin_amount`) OR NOT (t.`profit_pct` <=> s.`profit_pct`) OR NOT (t.`abc_class` <=> s.`abc_class`) OR NOT (t.`lifecycle_stage` <=> s.`lifecycle_stage`) OR NOT (t.`price_band` <=> s.`price_band`) OR NOT (t.`is_active` <=> s.`is_active`) OR NOT (t.`created_date` <=> s.`created_date`) OR NOT (t.`scd_hash` <=> s.`scd_hash`) OR NOT (t.`batch_id` <=> s.`batch_id`))', set={'effective_end_date': 's.`_scd_effective`'})
        .execute()
    )
    # Delta transaction: each DeltaTable.merge().execute() is one atomic transaction
    # Build insert candidates (new BK and/or Type 2 attribute changes)
    _dim_new_df_SCD2_Dimension_Lookup_Update = _dim_cmp_df_SCD2_Dimension_Lookup_Update.filter(col("_prior_tk").isNull() | ((~col("_prior_sku").eqNullSafe(col("sku"))) | (~col("_prior_product_name").eqNullSafe(col("product_name"))) | (~col("_prior_category_id").eqNullSafe(col("category_id"))) | (~col("_prior_category_name").eqNullSafe(col("category_name"))) | (~col("_prior_supplier_id").eqNullSafe(col("supplier_id"))) | (~col("_prior_supplier_name").eqNullSafe(col("supplier_name"))) | (~col("_prior_brand").eqNullSafe(col("brand"))) | (~col("_prior_unit_cost").eqNullSafe(col("unit_cost"))) | (~col("_prior_unit_price").eqNullSafe(col("unit_price"))) | (~col("_prior_selling_price").eqNullSafe(col("selling_price"))) | (~col("_prior_currency_code").eqNullSafe(col("currency_code"))) | (~col("_prior_weight_kg").eqNullSafe(col("weight_kg"))) | (~col("_prior_volume_cm3").eqNullSafe(col("volume_cm3"))) | (~col("_prior_margin_amount").eqNullSafe(col("margin_amount"))) | (~col("_prior_profit_pct").eqNullSafe(col("profit_pct"))) | (~col("_prior_abc_class").eqNullSafe(col("abc_class"))) | (~col("_prior_lifecycle_stage").eqNullSafe(col("lifecycle_stage"))) | (~col("_prior_price_band").eqNullSafe(col("price_band"))) | (~col("_prior_is_active").eqNullSafe(col("is_active"))) | (~col("_prior_created_date").eqNullSafe(col("created_date"))) | (~col("_prior_scd_hash").eqNullSafe(col("scd_hash"))) | (~col("_prior_batch_id").eqNullSafe(col("batch_id")))) | (col("_prior_tk").isNotNull() & ((~col("_prior_sku").eqNullSafe(col("sku"))) | (~col("_prior_product_name").eqNullSafe(col("product_name"))) | (~col("_prior_category_id").eqNullSafe(col("category_id"))) | (~col("_prior_category_name").eqNullSafe(col("category_name"))) | (~col("_prior_supplier_id").eqNullSafe(col("supplier_id"))) | (~col("_prior_supplier_name").eqNullSafe(col("supplier_name"))) | (~col("_prior_brand").eqNullSafe(col("brand"))) | (~col("_prior_unit_cost").eqNullSafe(col("unit_cost"))) | (~col("_prior_unit_price").eqNullSafe(col("unit_price"))) | (~col("_prior_selling_price").eqNullSafe(col("selling_price"))) | (~col("_prior_currency_code").eqNullSafe(col("currency_code"))) | (~col("_prior_weight_kg").eqNullSafe(col("weight_kg"))) | (~col("_prior_volume_cm3").eqNullSafe(col("volume_cm3"))) | (~col("_prior_margin_amount").eqNullSafe(col("margin_amount"))) | (~col("_prior_profit_pct").eqNullSafe(col("profit_pct"))) | (~col("_prior_abc_class").eqNullSafe(col("abc_class"))) | (~col("_prior_lifecycle_stage").eqNullSafe(col("lifecycle_stage"))) | (~col("_prior_price_band").eqNullSafe(col("price_band"))) | (~col("_prior_is_active").eqNullSafe(col("is_active"))) | (~col("_prior_created_date").eqNullSafe(col("created_date"))) | (~col("_prior_scd_hash").eqNullSafe(col("scd_hash"))) | (~col("_prior_batch_id").eqNullSafe(col("batch_id"))))))
    _dim_new_df_SCD2_Dimension_Lookup_Update = _dim_new_df_SCD2_Dimension_Lookup_Update.withColumn('version_number', (coalesce(col("_prior_version"), lit(0)) + lit(1)).cast("long"))
    _dim_new_df_SCD2_Dimension_Lookup_Update = _dim_new_df_SCD2_Dimension_Lookup_Update.drop('_prior_tk', '_prior_version', '_prior_sku', '_prior_product_name', '_prior_category_id', '_prior_category_name', '_prior_supplier_id', '_prior_supplier_name', '_prior_brand', '_prior_unit_cost', '_prior_unit_price', '_prior_selling_price', '_prior_currency_code', '_prior_weight_kg', '_prior_volume_cm3', '_prior_margin_amount', '_prior_profit_pct', '_prior_abc_class', '_prior_lifecycle_stage', '_prior_price_band', '_prior_is_active', '_prior_created_date', '_prior_scd_hash', '_prior_batch_id')
    # Assign surrogate keys for new / Type2 rows (tablemax)
    # tablemax + row_number (IDENTITY would omit tk from INSERT below)
    _max_tk = spark.sql("SELECT COALESCE(MAX(`product_sk`), 0) AS m FROM main.retail_dwh.dim_product").collect()[0][0]
    from pyspark.sql.window import Window as _DWWindow
    _dim_new_df_SCD2_Dimension_Lookup_Update = _dim_new_df_SCD2_Dimension_Lookup_Update.withColumn("_dw_rn", row_number().over(_DWWindow.orderBy(lit(1))))
    _dim_new_df_SCD2_Dimension_Lookup_Update = _dim_new_df_SCD2_Dimension_Lookup_Update.withColumn("product_sk", (lit(_max_tk) + col("_dw_rn")).cast("long")).drop("_dw_rn")
    _dim_new_df_SCD2_Dimension_Lookup_Update.createOrReplaceTempView('_dim_scd_new_df_SCD2_Dimension_Lookup_Update')
    # Single MERGE INSERT on technical key (new SK never matches existing)
    (
        DeltaTable.forName(spark, 'main.retail_dwh.dim_product').alias("t")
        .merge(spark.table('_dim_scd_new_df_SCD2_Dimension_Lookup_Update').alias("s"), 't.`product_sk` <=> s.`product_sk`')
        .whenNotMatchedInsert(values={'product_sk': 's.`product_sk`', 'product_id': 's.`product_id`', 'sku': 's.`sku`', 'product_name': 's.`product_name`', 'category_id': 's.`category_id`', 'category_name': 's.`category_name`', 'supplier_id': 's.`supplier_id`', 'supplier_name': 's.`supplier_name`', 'brand': 's.`brand`', 'unit_cost': 's.`unit_cost`', 'unit_price': 's.`unit_price`', 'selling_price': 's.`selling_price`', 'currency_code': 's.`currency_code`', 'weight_kg': 's.`weight_kg`', 'volume_cm3': 's.`volume_cm3`', 'margin_amount': 's.`margin_amount`', 'profit_pct': 's.`profit_pct`', 'abc_class': 's.`abc_class`', 'lifecycle_stage': 's.`lifecycle_stage`', 'price_band': 's.`price_band`', 'is_active': 's.`is_active`', 'created_date': 's.`created_date`', 'description': 's.`description`', 'scd_hash': 's.`scd_hash`', 'batch_id': 's.`batch_id`', 'effective_start_date': 's.`_scd_effective`', 'effective_end_date': "TIMESTAMP '2199-12-31 23:59:59.999'", 'version_number': 's.`version_number`'})
        .execute()
    )
    # Delta transaction: each DeltaTable.merge().execute() is one atomic transaction
    # Schema evolution: add new attribute columns with ALTER TABLE before MERGE
    # Effective date overlaps / multiple actives: enforce with constraints or dedupe window on (business_keys, date_from)
    # Cache: broadcast join approximates Pentaho preload/cache
    _dim_df_SCD2_Dimension_Lookup_Update = spark.table('main.retail_dwh.dim_product').select("product_sk", "product_id", "effective_start_date", "effective_end_date", "version_number", "sku", "product_name", "category_id", "category_name", "supplier_id", "supplier_name", "brand", "unit_cost", "unit_price", "selling_price", "currency_code", "weight_kg", "volume_cm3", "margin_amount", "profit_pct", "abc_class", "lifecycle_stage", "price_band", "is_active", "created_date", "scd_hash", "batch_id", "description")
    _dim_df_SCD2_Dimension_Lookup_Update = broadcast(_dim_df_SCD2_Dimension_Lookup_Update)
    df_SCD2_Dimension_Lookup_Update = df_Stamp_Effective_Dates.join(_dim_df_SCD2_Dimension_Lookup_Update, on=((df_Stamp_Effective_Dates["product_id"] == _dim_df_SCD2_Dimension_Lookup_Update["product_id"]) & (df_Stamp_Effective_Dates["scd_change_date"] >= _dim_df_SCD2_Dimension_Lookup_Update["effective_start_date"]) & (df_Stamp_Effective_Dates["scd_change_date"] < _dim_df_SCD2_Dimension_Lookup_Update["effective_end_date"])), how='left')
    # Null 'product_sk' after update indicates unresolved BK / null keys

    # Step: Dimension Load Stats (MemoryGroupBy) [converted]
    # Memory Group By: Dimension Load Stats
    # NOTE: Pentaho Memory Group By aggregates entirely in JVM heap; Spark uses distributed groupBy().agg() — memory pressure shifts to executors, and result ordering / early-partial-agg timing may differ.
    df_Dimension_Load_Stats = df_SCD2_Dimension_Lookup_Update.groupBy('scd_action').agg(count(lit(1)).alias('action_count'))

    # Step: Write Product Dimension File (TextFileOutput) [converted]
    # Pentaho step: Write Product Dimension File (type: TextFileOutput)
    # Pentaho filename: /output/product/dimension/dim_product_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='product_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='business_key' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sku' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='category_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='category_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='brand' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unit_cost' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unit_price' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='selling_price' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='margin_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='profit_pct' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='tax_amount' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='discount_pct' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='volume_cm3' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='weight_kg' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='currency_code' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='abc_class' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='lifecycle_stage' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='price_band' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_active' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='created_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='description' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='scd_hash' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='version_number' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='effective_start_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='effective_end_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_current' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='current_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='scd_action' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='load_status' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='batch_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='run_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='dw_insert_ts' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='dw_update_ts' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Product_Dimension_File = df_SCD2_Dimension_Lookup_Update
    _out_df_Write_Product_Dimension_File = df_Write_Product_Dimension_File.select('product_sk', 'product_id', 'business_key', 'sku', 'product_name', 'category_id', 'category_name', 'supplier_id', 'supplier_name', 'brand', 'unit_cost', 'unit_price', 'selling_price', 'margin_amount', 'profit_pct', 'tax_amount', 'discount_pct', 'volume_cm3', 'weight_kg', 'currency_code', 'abc_class', 'lifecycle_stage', 'price_band', 'is_active', 'status', 'created_date', 'description', 'scd_hash', 'version_number', 'effective_start_date', 'effective_end_date', 'is_current', 'current_flag', 'scd_action', 'load_status', 'batch_id', 'run_id', 'dw_insert_ts', 'dw_update_ts')
    writer = _out_df_Write_Product_Dimension_File.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/dim_product_.csv')

    # Step: Write Dimension Stats (TextFileOutput) [converted]
    # Pentaho step: Write Dimension Stats (type: TextFileOutput)
    # Pentaho filename: /audit/load_audit/product_dimension_stats_
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='scd_action' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='action_count' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Dimension_Stats = df_Dimension_Load_Stats
    _out_df_Write_Dimension_Stats = df_Write_Dimension_Stats.select('scd_action', 'action_count')
    writer = _out_df_Write_Dimension_Stats.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/product_dimension_stats_.csv')

    # Step: Write Product Dimension Current (TextFileOutput) [converted]
    # Pentaho step: Write Product Dimension Current (type: TextFileOutput)
    # Pentaho filename: /output/product/dimension/dim_product_current
    # NOTE: Spark CSV/text writers create a directory at this path (not a single flat file); subsequent Text File Input must load the same path
    # NOTE: empty input DataFrames write zero data files; missing parent paths are usually created by the filesystem
    # INFO: preserved split_every='0' — use repartition/coalesce to control output file count
    # INFO: create_parent_folder=Y — Spark/DBFS typically creates parent folders
    # INFO: preserved.field_format name='product_sk' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='sku' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='product_name' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='category_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='supplier_id' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='unit_price' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_active' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='is_current' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='version_number' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='scd_hash' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='effective_start_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='effective_end_date' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    # INFO: preserved.field_format name='current_flag' options={'trim_type': 'none', 'length': '-1', 'precision': '-1'}
    df_Write_Product_Dimension_Current = df_Write_Product_Dimension_File
    _out_df_Write_Product_Dimension_Current = df_Write_Product_Dimension_Current.select('product_sk', 'product_id', 'sku', 'product_name', 'category_id', 'supplier_id', 'unit_price', 'is_active', 'is_current', 'version_number', 'scd_hash', 'effective_start_date', 'effective_end_date', 'current_flag')
    writer = _out_df_Write_Product_Dimension_Current.write.format("csv")
    writer = writer.option("header", True)
    writer = writer.option("sep", ',')
    writer = writer.option("quote", '"')
    writer = writer.option("encoding", 'UTF-8')
    writer.mode('overwrite').save(f'{data_dir}/dim_product_current.csv')

    # Step: Load DimProduct Table (TableOutput) [converted]
    # Pentaho step: Load DimProduct Table (type: TableOutput) (Pentaho schema: retail_dwh)
    # Mode: append (Pentaho truncate=N)
    _mapped_df_Load_DimProduct_Table = df_Write_Product_Dimension_Current.select(col('product_sk'), col('product_id'), col('sku'), col('product_name'), col('category_id'), col('supplier_id'), col('brand'), col('unit_cost'), col('unit_price'), col('currency_code'), col('weight_kg'), col('is_active'), col('created_date'), col('description'), col('effective_start_date'), col('effective_end_date'), col('is_current'), col('version_number'), col('batch_id'))
    df_Load_DimProduct_Table = _mapped_df_Load_DimProduct_Table
    write_delta(
        df_Load_DimProduct_Table,
        f"{catalog}.{schema}.dim_product",
        mode='append',
        partition_by=config.get('partition_by') or [],
        target_files=config.get('target_files'),
        spark=spark,
    )
    log_event(_LOG, "delta_write", table=f"{catalog}.{schema}.dim_product", mode='append')

    # Step: Log Dimension Load Complete (WriteToLog) [converted]
    # Write to Log: Log Dimension Load Complete
    # preserved.log_level='Basic'
    # preserved.log_message='AUDIT | EVENT=DIM_LOAD_COMPLETE | TRANS=TR_Product_Dimension_Load | RUN_ID=${RUN_ID}'
    # preserved.display_header=True
    # preserved.limit_rows=False
    # preserved.limit_rows_number=0
    import logging
    _log_df_Log_Dimension_Load_Complete = logging.getLogger('pentaho.writetolog.Log_Dimension_Load_Complete')
    _log_df_Log_Dimension_Load_Complete.setLevel(logging.INFO)
    # NOTE: sampling up to 20 rows for logging (avoid collect() on full partitions)
    _log_df_df_Log_Dimension_Load_Complete = df_Load_DimProduct_Table
    _log_rows_df_Log_Dimension_Load_Complete = _log_df_df_Log_Dimension_Load_Complete.take(20)
    _log_df_Log_Dimension_Load_Complete.info('Log Dimension Load Complete' + ' | columns=' + str(_log_df_df_Log_Dimension_Load_Complete.columns))
    _log_df_Log_Dimension_Load_Complete.info('AUDIT | EVENT=DIM_LOAD_COMPLETE | TRANS=TR_Product_Dimension_Load | RUN_ID=${RUN_ID}')
    for _lr in _log_rows_df_Log_Dimension_Load_Complete:
        _log_df_Log_Dimension_Load_Complete.info('Log Dimension Load Complete' + ' | ' + str(_lr.asDict()))
    df_Log_Dimension_Load_Complete = df_Load_DimProduct_Table

    # Step: Block Until Dim Load Done (BlockingStep) [converted]
    # Blocking Step: Block Until Dim Load Done
    # preserved.pass_all_rows=True
    # preserved.directory='%%java.io.tmpdir%%'
    # preserved.prefix='block'
    # preserved.cache_size='5000'
    # preserved.compress_files=False
    # LIMITATION: Pentaho temp-file spill (directory/prefix/compress) is replaced by Spark cache/persist.
    df_Block_Until_Dim_Load_Done = cache_for_reuse(df_Log_Dimension_Load_Complete)
    _ = df_Block_Until_Dim_Load_Done.count()  # synchronize: wait for all upstream rows

    # Step: Dimension Load Complete (Dummy) [converted]
    # Dummy: Dimension Load Complete
    # Pass-through step - DataFrame unchanged
    df_Dummy_Dimension_Load_Complete = df_Block_Until_Dim_Load_Done

    log_event(_LOG, "transformation_end")
    return df_Dummy_Dimension_Load_Complete
